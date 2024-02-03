import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API for a range of months in a specific year
    """
    data_frames = []

    year = 2022
    month_start=1
    month_end=12

    # Generate URLs for each month in the specified range
    for month in range(month_start, month_end + 1):
        # Format month to have leading zero if needed

        formatted_month = f"{month:02d}"
        
        # Construct the URL for the specific month
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{formatted_month}.parquet'

        # Load data for the current month and append to the list
        month_data = pd.read_parquet(url, engine='pyarrow')
        
        # Add 'year_month' column to the loaded data
        month_data['year_month'] = f'{year}-{formatted_month}'

        # Append the data frame with the new column to the list
        data_frames.append(month_data)
    


    # Concatenate data frames for all months into a single data frame
    result_df = pd.concat(data_frames, ignore_index=True)

    return result_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
