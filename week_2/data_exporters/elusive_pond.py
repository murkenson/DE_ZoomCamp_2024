import pyarrow as pa  
import pyarrow.parquet as pq  
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# Set the path to the JSON file containing Google Cloud service credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/ny_taxi.json"
bucket_name = 'mage-data-zoom-camp-marfanyan'
project_id = 'ny-rides-marfanyan'

table_name = 'green_ny_taxi'

# Compose the root path for saving data in the bucket
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    # Convert Pandas DataFrame to a PyArrow table
    table = pa.Table.from_pandas(data)

    # Create a PyArrow GCS (Google Cloud Storage) file system object
    gcs = pa.fs.GcsFileSystem()

    # Write the PyArrow table to a Parquet dataset in Google Cloud Storage
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )






"""
@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    # Извлечение уникальных дат из столбца lpep_pickup_date
    unique_dates = df['lpep_pickup_date'].unique()

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'mage-data-zoom-camp-marfanyan'

    for lpep_pickup_date in unique_dates:
        # Преобразование даты в строку
        now_fpath = lpep_pickup_date.strftime("%Y/%m/%d")

        object_key = f'green_taxi/{now_fpath}/green_dayli_trips.parquet'
        
        print(object_key)

        # Фильтрация DataFrame по текущей дате
        df_filtered = df[df['lpep_pickup_date'] == lpep_pickup_date]

        GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
            df_filtered,
            bucket_name,
            object_key,
        )
"""

