import pytest
import time
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bdi_api.s1.exercise import s1
from bdi_api.app import app as real_app


@pytest.fixture(scope="class")
def app() -> FastAPI:
    """In case you want to test only a part"""
    return real_app


@pytest.fixture(scope="class")
def client(app: FastAPI) -> TestClient:
    """We include our router for the examples"""
    yield TestClient(app)

class TestEvaluateS1:

    def test_download(self, client: TestClient, json_metadata, request) -> None:
        # Given
        start = int(round(time.time() * 1000))
        # When
        with client as client:  # Always do this
            response = client.post(f"/api/s1/aircraft/download")
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert response.status_code == 200

    def test_preparation(self, client: TestClient, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        # When
        with client as client:  # Always do this
            response = client.post(f"/api/s1/aircraft/prepare")
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert response.status_code == 200

    def test_aircrafts_schema(self, client: TestClient, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        # When
        with client as client:  # Always do this
            response = client.get(f"/api/s1/aircraft")
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert response.status_code == 200
        resp = response.json()
        assert isinstance(resp, list)
        assert resp
        assert set(resp[0].keys()) == {"icao", "registration", "type"}

    def test_all_aircrafts_count(self, client, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        # When
        results = []

        with client as client:  # Always do this
            page = 0
            r = client.get(f"/api/s1/aircraft/?num_results=500&page={page}")
            assert r.status_code == 200
            response = r.json()
            results.extend(a["icao"] for a in response)

            while response and page < 100:
                page += 1
                r = client.get(f"/api/s1/aircraft/?num_results=500&page={page}")
                assert r.status_code == 200
                response = r.json()
                results.extend(a["icao"] for a in response)

        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert len(results) == 36807
        assert len(results) == len(set(results))

    @pytest.mark.parametrize("num_results", [138, 120, 93, 20])
    def test_aircraft_paging(self, client, num_results, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        # When
        with client as client:  # Always do this
            response = client.get(f"/api/s1/aircraft/?num_results={num_results}")
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert len(response.json()) == num_results

    @pytest.mark.parametrize(
        "icao,expected",
        [
            ("040014", {"max_altitude_baro": 9475, "max_ground_speed": 460, "had_emergency": False}),
            ("a095f0", {"max_altitude_baro": 9975, "max_ground_speed": 392.6, "had_emergency": False}),
            ("a972d3", {"max_altitude_baro": 9850, "max_ground_speed": 401.5, "had_emergency": True}),
        ]
    )
    def test_statistics(self, client, icao, expected, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        # When
        with client as client:  # Always do this
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert response.json() == expected

    def test_aircraft_stats_schema(self, client: TestClient, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        icao = "a095f0"
        # When
        with client as client:  # Always do this
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert response.status_code == 200
        resp = response.json()
        assert isinstance(resp, dict)
        assert resp
        assert set(resp.keys()) == {"max_altitude_baro", "max_ground_speed", "had_emergency"}

    @pytest.mark.parametrize(
        "icao",
        ["a972d3"]
    )
    def test_positions_ordered_asc(self, client, icao, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        # When
        results = []
        with client as client:  # Always do this
            page = 0
            r = client.get(f"/api/s1/aircraft/{icao}/positions?num_results=1000&page={page}")
            assert r.status_code == 200
            response = r.json()
            results.extend(a["timestamp"] for a in response)
            while response and page < 100:
                page += 1
                r = client.get(f"/api/s1/aircraft/{icao}/positions?num_results=1000&page={page}")
                assert r.status_code == 200
                response = r.json()
                results.extend(a["timestamp"] for a in response)
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert len(results) > 1
        assert sorted(results.copy()) == results

    def test_non_existent_position(self, client, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        icao = "do_not_exist"
        # When
        results = []
        with client as client:  # Always do this
            page = 0
            r = client.get(f"/api/s1/aircraft/{icao}/positions?num_results=1000&page={page}")
            assert r.status_code == 200
            response = r.json()
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert len(response) == 0

    def test_aircraft_position_schema(self, client: TestClient, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        icao = "a095f0"
        # When
        with client as client:  # Always do this
            response = client.get(f"/api/s1/aircraft/{icao}/positions")
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert response.status_code == 200
        resp = response.json()
        assert isinstance(resp, list)
        assert resp
        assert set(resp[0].keys()) == {"timestamp", "lat", "lon"}

    @pytest.mark.parametrize(
        "icao,expected",
        [
            ("040014", 199),
            ("a095f0", 922),
            ("a972d3", 968),
        ]
    )
    def test_positions(self, client, icao, expected, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        # When
        results = []
        with client as client:  # Always do this
            page = 0
            r = client.get(f"/api/s1/aircraft/{icao}/positions?num_results=1000&page={page}")
            assert r.status_code == 200
            response = r.json()
            results.extend(a["timestamp"] for a in response)

            while response and page < 100:
                page += 1
                r = client.get(f"/api/s1/aircraft/{icao}/positions?num_results=1000&page={page}")
                assert r.status_code == 200
                response = r.json()
                results.extend(a["timestamp"] for a in response)
        elapsed = int(round(time.time() * 1000)) - start
        json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2
        # Then
        assert len(results) == expected

    @pytest.mark.parametrize(
        "num",
        [10, 102, 683, 720]
    )
    def test_positions_num_results(self, client, num, json_metadata, request) -> None:
        start = int(round(time.time() * 1000))
        # Given
        icao = "a972d3"
        # When
        with client as client:  # Always do this
            page = 0
            r = client.get(f"/api/s1/aircraft/{icao}/positions?num_results={num}&page={page}")
            assert r.status_code == 200
            response = r.json()
            assert len(response) == num
            elapsed = int(round(time.time() * 1000)) - start
            json_metadata[request.node.originalname] = (json_metadata.get(request.node.originalname, 0) + elapsed) / 2