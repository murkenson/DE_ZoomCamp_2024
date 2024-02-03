import pyarrow as pa # Импорт PyArrow для работы с таблицами и форматом данных Parquet
import pyarrow.parquet as pq # Импорт PyArrow для работы с форматом данных Parquet
import os # Импорт модуля для взаимодействия с операционной системой



if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# Установка пути к файлу JSON учетных данных службы Google Cloud

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/ny_taxi.json"
bucket_name = 'mage-data-zoom-camp-marfanyan'
project_id = 'ny-rides-marfanyan'

table_name = 'ny_green_taxi_data'

# Составление корневого пути для сохранения данных в бакете

root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data(data, *args, **kwargs):

    # Преобразование Pandas DataFrame в таблицу PyArrow
    table = pa.Table.from_pandas(data)

    # Создание объекта файловой системы PyArrow GCS (Google Cloud Storage)
    gcs = pa.fs.GcsFileSystem()



    # Запись таблицы PyArrow в набор данных Parquet в Google Cloud Storage
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['year_month'],
        filesystem=gcs
            )





