import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{}.csv.gz'

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    dfs = []
    for month in range(10, 13):
        url = base_url.format(month)
        df = pd.read_csv(url, sep=',', compression="gzip", parse_dates=parse_dates)
        dfs.append(df)

    # Concatenate the dataframes for the three months
    final_quarter_data = pd.concat(dfs, ignore_index=True)

    return final_quarter_data
    #return pd.read_csv(url, sep=',', compression = "gzip")


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
