from fastapi import APIRouter, status

from bdi_api.settigns import Settings

settings = Settings()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data() -> str:
    """ Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`

    NOTE: you can change that value via the environment variable `BDI_S3_BUCKET`
    """
    # BASE_URL
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    # TODO

    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO
    return "OK"
