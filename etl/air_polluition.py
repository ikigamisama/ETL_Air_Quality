import json
import requests
import pandas as pd
from datetime import datetime
from loguru import logger


class AirPollutionData:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.endpoint = "http://api.openweathermap.org/data/2.5/air_pollution/history"

    def extract(self, lat: float, lon: float, start: int, end: int) -> list[dict]:
        params = {
            "lat": lat,
            "lon": lon,
            "start": start,
            "end": end,
            "appid": self.api_key
        }

        logger.info(
            f"Requesting data for lat={lat}, lon={lon}, start={start}, end={end}")
        response = requests.get(self.endpoint, params=params)

        if response.status_code == 200:
            data = response.json()
            raw_list = data.get("list", [])
            logger.info(
                f"Successfully retrieved {len(raw_list)} air pollution records")

            if raw_list:
                logger.debug(f"First record: {raw_list[0]}")

            return raw_list
        else:
            logger.error(
                f"API request failed with status {response.status_code}: {response.text}")
            return []

    def transform(self, raw_data: list[dict]) -> pd.DataFrame:
        logger.info(
            f"Transform called with {len(raw_data) if raw_data else 0} records")
        logger.debug(f"Raw data type: {type(raw_data)}")

        if not raw_data:
            logger.warning("No data to transform - returning empty DataFrame")
            return pd.DataFrame()

        if raw_data:
            logger.debug(f"First item type: {type(raw_data[0])}")
            logger.debug(f"First item: {raw_data[0]}")

        structured = []
        for i, d in enumerate(raw_data):
            try:
                if isinstance(d, str):
                    logger.warning(
                        f"Record {i} is a string, attempting to parse as JSON")
                    d = json.loads(d)

                if not isinstance(d, dict):
                    logger.error(
                        f"Record {i} is not a dictionary: {type(d)} - {d}")
                    continue

                if 'dt' not in d:
                    logger.error(f"Record {i} missing 'dt' field: {d}")
                    continue

                if 'components' not in d:
                    logger.error(f"Record {i} missing 'components' field: {d}")
                    continue

                entry = {
                    'Date': datetime.fromtimestamp(d.get('dt', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                    'co': d.get('components', {}).get('co'),
                    'no': d.get('components', {}).get('no'),
                    'no2': d.get('components', {}).get('no2'),
                    'o3': d.get('components', {}).get('o3'),
                    'so2': d.get('components', {}).get('so2'),
                    'pm2_5': d.get('components', {}).get('pm2_5'),
                    'pm10': d.get('components', {}).get('pm10'),
                    'nh3': d.get('components', {}).get('nh3')
                }
                structured.append(entry)

            except Exception as e:
                logger.error(f"Error processing record {i}: {e}")
                logger.error(f"Problematic record: {d}")
                continue

        if not structured:
            logger.warning(
                "No valid records processed - returning empty DataFrame")
            return pd.DataFrame()

        air_data_df = pd.DataFrame(structured)
        air_data_df['Date'] = pd.to_datetime(air_data_df['Date'])

        logger.info(f"Successfully transformed {len(air_data_df)} records")
        return air_data_df

    def load_to_csv(self, df: pd.DataFrame, filename: str):
        if df.empty:
            logger.warning("DataFrame is empty - not saving to CSV")
            return

        file = f"./data/city/{filename}.csv"
        df.to_csv(file, index=False)
        logger.info(f"Saved AQI data to: {file}")

    def run_pipeline(self, lat: float, lon: float, start: int, end: int, filename: str):
        logger.info("Starting air pollution data pipeline")

        # Extract
        raw_data = self.extract(lat, lon, start, end)
        if not raw_data:
            logger.error("No data extracted - pipeline stopped")
            return None

        # Transform
        df = self.transform(raw_data)
        if df.empty:
            logger.error("No data transformed - pipeline stopped")
            return None

        # Load
        self.load_to_csv(df, filename)
        return df
