import pandas as pd


class CityTransformer:
    @staticmethod
    def filter_by_country(city_list: list, country_code: str) -> pd.DataFrame:
        filtered = [
            city for city in city_list if city['country'] == country_code]
        df = pd.DataFrame(filtered)

        # Flatten 'coord'
        df['lat'] = df['coord'].apply(lambda x: x['lat'])
        df['lon'] = df['coord'].apply(lambda x: x['lon'])
        df = df.drop(columns=['coord', 'state'])
        df = df.drop_duplicates(subset=['name'])

        return df
