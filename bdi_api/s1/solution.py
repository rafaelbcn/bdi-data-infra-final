import gzip
import json
import logging
import multiprocessing
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from os.path import join
from random import randint
from typing import Tuple

import duckdb
import requests
from fastapi import APIRouter, HTTPException, status
from opentelemetry import trace

from bdi_api.settigns import Settings

settings = Settings()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class ExtractFiles(HTMLParser):
    files: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[Tuple[str, str | None]]):
        if tag == "a":
            href_attr = [attr[1] for attr in attrs if attr[0] == "href" and attr[1] and attr[1].endswith(".json.gz")]
            if href_attr:
                self.files.append(href_attr[0])


s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["solution1"],
)


def download_file(file_name: str) -> None:
    url = BASE_URL + file_name
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    local_file = os.path.join(download_dir, file_name)

    response = requests.get(url, stream=True)
    with open(local_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=128 * 1024):
            f.write(chunk)


def read_prepared_sql() -> str:
    dir = join(settings.prepared_dir, "*", "*.parquet")
    return f"read_parquet('{dir}', hive_partitioning = 1, hive_types_autocast = 0)"


@s1.post("/aircraft/download")
def download_data() -> str:
    """Downloads the files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.
    """
    index = requests.get(BASE_URL)
    parser = ExtractFiles()
    parser.feed(index.text)
    files = parser.files[:1000]

    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    os.makedirs(download_dir, exist_ok=True)

    assert len(files) == 1000
    # We use parallelization to avoid wasting time
    with ThreadPoolExecutor(max_workers=12) as executor:
        executor.map(download_file, files)
    return "OK"


@tracer.start_as_current_span("compress_files")
def compress_files(files: list[str], target_file: str) -> None:
    def extract_row(ac: dict, ts: float) -> dict:
        altitude = ac.get("alt_baro")
        return {
            "ts": ts,
            "hex": ac.get("hex"),
            "reg": ac.get("r"),
            "alt_baro": altitude if isinstance(altitude, int) else 0,
            "speed": ac.get("gs"),
            "flight": ac.get("flight"),
            "aircraft_type": ac.get("t"),
            "lat": ac.get("lat", ac.get("lastPosition", {}).get("lat")),
            "lon": ac.get("lon", ac.get("lastPosition", {}).get("lon")),
            "emergency": ac.get("emergency"),
        }

    with open(target_file, "a", encoding="utf8") as write_f:
        for name in files:
            with gzip.open(name, mode="rt", encoding="utf8") as f:
                data = json.load(f)
                ts = data["now"]
                for ac in data["aircraft"]:
                    json.dump(extract_row(ac, ts), write_f)
                    write_f.write("\n")


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.
    """

    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    tmp_dir = settings.prepared_dir + "_tmp"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)

    files = []
    for _, _, filenames in os.walk(raw_dir):
        files.extend(join(raw_dir, name) for name in filenames)

    batch_size = 50
    batches = [
        (files[i : i + batch_size], join(tmp_dir, f"ac_{randint(0, 99999):05d}.json"))
        for i in range(0, len(files), batch_size)
    ]
    with multiprocessing.Pool(4) as pool:
        pool.starmap(compress_files, batches)
    logging.info("Compression ended")

    @tracer.start_as_current_span("to_parquet")
    def to_parquet() -> None:
        duckdb.sql("INSTALL parquet;")
        logging.info("Reading from tmp dir")
        if os.path.exists(settings.prepared_dir):
            shutil.rmtree(settings.prepared_dir)
        duckdb.sql(
            f"""SET memory_limit = '5GB';SET threads TO 4;COPY (SELECT *, hex[:1] AS hex_start
        FROM read_json_auto('{tmp_dir}/*') ORDER BY hex, ts
        ) TO '{settings.prepared_dir}'
        (FORMAT PARQUET, partition_by (hex_start))
        """
        )
        logger.info("Ended")

    to_parquet()
    # Cleanup
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    duckdb.sql("LOAD parquet;")
    res = duckdb.sql(
        f"""
    SELECT DISTINCT
      hex AS icao
      , reg AS registration
      , aircraft_type AS type
    FROM {read_prepared_sql()}
    ORDER BY icao LIMIT {num_results} OFFSET {num_results * page}
    """,
    )
    return [{"icao": hex, "registration": reg, "type": y} for hex, reg, y in res.fetchall()]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    duckdb.sql("LOAD parquet;")
    res = duckdb.sql(
        f"""
    SELECT
      ts AS timestamp
      , lat
      , lon
    FROM {read_prepared_sql()}
    WHERE hex_start = '{icao[:1]}' AND hex = '{icao}'
    ORDER BY ts LIMIT {num_results} OFFSET {num_results * page}
    """,
    )
    rows = res.fetchall()
    if not rows:
        exists = duckdb.sql(
            f"SELECT 1 FROM {read_prepared_sql()} WHERE hex_start = '{icao[:1]}' AND hex = '{icao}' LIMIT 1",
        )
        if exists.fetchall():
            return []
        else:
            return []  # Statement requested empty return.
            # raise HTTPException(status_code=404, detail="Aircraft not found")
    return [{"timestamp": ts, "lat": lat, "lon": lon} for ts, lat, lon in res.fetchall()]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    duckdb.sql("LOAD parquet;")
    res = duckdb.sql(
        f"""
    SELECT
      MAX(alt_baro) AS max_altitude
      , MAX(speed) AS max_ground_speed
      , MAX(emergency is not NULL and emergency != 'none') AS  had_emergency
    FROM {read_prepared_sql()}
    WHERE hex_start = '{icao[:1]}' AND hex = '{icao}'
    GROUP BY hex
    """,
    )
    rows = res.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="Aircraft not found")
    alt, gs, em = rows[0]
    return {"max_altitude_baro": int(alt), "max_ground_speed": gs, "had_emergency": em}
