import sys
import datetime as dt

from loguru import logger
from etl.extractor import CityExtractor
from etl.transformer import CityTransformer
from etl.loader import CityLoader
from etl.air_polluition import AirPollutionData


PROGRAM_NAME = "Air Quality Data Cities in PH"
if __name__ == "__main__":

    start_time = dt.datetime.now()

    logger.remove()
    logger.add("logs/std_out.log", rotation="10 MB", level="INFO")
    logger.add("logs/std_err.log", rotation="10 MB", level="ERROR")
    logger.add(sys.stdout, level="INFO")
    logger.add(sys.stderr, level="ERROR")

    logger.info(f"{PROGRAM_NAME} has started")

    URL = "http://bulk.openweathermap.org/sample/city.list.json.gz"
    GZ_PATH = "./data/city.list.json.gz"
    CITY_CSV = "PH_cities.csv"
    API_KEY = "5b94b4efbe7a57a8ec624d6d9321ad24"
    COUNTRY_CODE = "PH"
    START_TIMESTAMP = 1735689600
    END_TIMESTAMP = 1759334400

    # EXTRACT
    CityExtractor.download_gz(URL, GZ_PATH)
    raw_data = CityExtractor.load_city_json(GZ_PATH)

    # TRANSFORM
    df_cities = CityTransformer.filter_by_country(raw_data, COUNTRY_CODE)

    # LOAD
    CityLoader.save_to_csv(df_cities, CITY_CSV)

    # Execute City List
    air_data = AirPollutionData(API_KEY)
    for _, row in df_cities.iterrows():
        city = row['name']
        lat = row['lat']
        lon = row['lon']

        df = air_data.run_pipeline(
            lat, lon, START_TIMESTAMP, END_TIMESTAMP, city)
