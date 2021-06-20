import requests
import pandas as pd


def save_to_parquet(data_dict):
    df = pd.DataFrame.from_dict(data_dict, orient="index")
    df.to_parquet('data.parguet.snp', compression='snappy')

if __name__ == '__main__':

    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=ER49QYN4BVCI9UML'
    r = requests.get(url)
    data = r.json()

    #print(data)
    save_to_parquet(data)