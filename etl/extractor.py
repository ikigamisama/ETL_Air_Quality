import json
import gzip
import requests

from loguru import logger


class CityExtractor:
    @staticmethod
    def download_gz(url: str, dest_path: str):
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded: {dest_path}")

    @staticmethod
    def load_city_json(gz_path: str):
        with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
            return json.load(f)
