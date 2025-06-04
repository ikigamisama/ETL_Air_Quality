from loguru import logger


class CityLoader:
    @staticmethod
    def save_to_csv(df, path: str):
        file = f"./data/{path}"
        df.to_csv(file, index=False)
        logger.info(f"Saved CSV: {file}")
