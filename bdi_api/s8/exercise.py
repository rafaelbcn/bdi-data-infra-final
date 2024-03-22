from typing import Optional
from fastapi import APIRouter, status
from pydantic import BaseModel
from bdi_api.settings import DBCredentials, Settings
import asyncpg


settings = Settings()
db_credentials = DBCredentials()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)


async def get_db_connection():
    return await asyncpg.connect(user=db_credentials.user,
                                 password=db_credentials.password,
                                 database=db_credentials.database,
                                 host=db_credentials.host,
                                 port=db_credentials.port)


#start coding here:
class AircraftReturn(BaseModel):
    # DO NOT MODIFY IT
    icao: str
    registration: Optional[str]
    type: Optional[str]
    owner: Optional[str]
    manufacturer: Optional[str]
    model: Optional[str]

@s8.get("/aircraft/", response_model=list[AircraftReturn])
async def list_aircraft(num_results: int = 100, page: int = 0):
    try:
        connection = await get_db_connection()
        offset = page * num_results
        query = """
        SELECT 
          ad.icao, 
          ad.registration, 
          ad.aircraft_type AS type, 
          abi.ownop AS owner, 
          abi.manufacturer, 
          abi.model
        FROM 
          aircraft_data ad
        INNER JOIN 
          aircraft_basic_info abi ON ad.icao = abi.icao
        ORDER BY 
          ad.icao ASC
        LIMIT $1 OFFSET $2
        """
        records = await connection.fetch(query, num_results, offset)
    except Exception as e:
        return JSONResponse(status_code=400, content={"message": str(e)})
    finally:
        if connection:
            await connection.close()
    return [AircraftReturn.parse_obj(dict(record)) for record in records]

class AircraftCO2(BaseModel):
    # DO NOT MODIFY IT
    icao: str
    hours_flown: float
    """Co2 tons generated"""
    co2: Optional[float]

# Do you code after this

import random
from fastapi import HTTPException
from fastapi.responses import JSONResponse

# Assuming your endpoint and models are set up as before...

@s8.get("/aircraft/{icao}/co2", response_model=AircraftCO2)
async def get_aircraft_co2(icao: str, day: Optional[str] = None) -> AircraftCO2:
    connection = None
    try:
        connection = await get_db_connection()  # Replace with your actual database connection function

        # New query logic
        query = """
        SELECT
            aircraft_data.icao,
            SUM((fuel_consumption.galph * 3.04) * 3.15 / 907.185) AS co2_tons,
            COUNT(*) * 5 / 3600 AS hours_flown -- Each row represents 5 seconds, convert total seconds to hours
        FROM
            aircraft_data
        LEFT JOIN fuel_consumption ON aircraft_data.aircraft_type = fuel_consumption.icao
        WHERE
            aircraft_data.icao = $1
        GROUP BY
            aircraft_data.icao;
        """

        # Execute the query with the provided icao code
        result = await connection.fetchrow(query, icao)

        if result:
            # If there's a result, use it; otherwise, default to 0 or None
            hours_flown = result['hours_flown'] or 0
            co2_tons = result['co2_tons']
        else:
            hours_flown = 0
            co2_tons = None

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if connection:
            await connection.close()

    # Return an instance of AircraftCO2 with the fetched data
    return AircraftCO2(icao=icao, hours_flown=hours_flown, co2=co2_tons)