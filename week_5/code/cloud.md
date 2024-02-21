## Running Spark in the Cloud

### Connecting to Google Cloud Storage 

Uploading data to GCS:

```bash
gsutil -m cp -r pq/ gs://mage-data-zoom-camp-marfanyan/pq
```

Download the jar for connecting to GCS to any location (e.g. the `lib` folder):

```bash
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar
```

See the notebook with configuration in [09_spark_gcs.ipynb](09_spark_gcs.ipynb)

(Thanks Alvin Do for the instructions!)


### Local Cluster and Spark-Submit

Creating a stand-alone cluster ([docs](https://spark.apache.org/docs/latest/spark-standalone.html)):

```bash
./sbin/start-master.sh
```

Creating a worker:

```bash
URL="spark://de-zoomcamp.europe-west1-b.c.ny-rides-marfanyan.internal:7077"
./sbin/start-slave.sh ${URL}

# for newer versions of spark use that:
#./sbin/start-worker.sh ${URL}
```

Turn the notebook into a script:

```bash
jupyter nbconvert --to=script 070_PySpark.ipynb
```

Edit the script and then run it:

```bash 
python  071_PySpark_to_gcs.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020
```

Use `spark-submit` for running the script on the cluster

```bash
URL="spark://de-zoomcamp.europe-west1-b.c.ny-rides-marfanyan.internal:7077"

spark-submit \
    --master="${URL}" \
    071_PySpark_to_gcs.py \
        --input_green=data/pq/green/2020/*/ \
        --input_yellow=data/pq/yellow/2020/*/ \
        --output=data/report-2020
```

### Data Proc

Upload the script to GCS:

```bash
TODO
```

Params for the job:

* `--input_green=gs://mage-data-zoom-camp-marfanyan/pq/green/2020/*/`
* `--input_yellow=gs://mage-data-zoom-camp-marfanyan/pq/yellow/2021/*/`
* `--output=gs://mage-data-zoom-camp-marfanyan/report-2021`


Using Google Cloud SDK for submitting to dataproc
([link](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud))

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-central1 \
    gs://mage-data-zoom-camp-marfanyan/code/071_PySpark_to_gcs.py \
    -- \
        --input_green=gs://mage-data-zoom-camp-marfanyan/pq/green/2020/*/ \
        --input_yellow=gs://mage-data-zoom-camp-marfanyan/pq/yellow/2020/*/ \
        --output=gs://mage-data-zoom-camp-marfanyan/report-2020
```

### Big Query

Upload the script to GCS:

```bash
TODO
```

Write results to big query ([docs](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark)):

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://mage-data-zoom-camp-marfanyan/code/072_PySpark_to_big_query.py \
    -- \
        --input_green=gs://mage-data-zoom-camp-marfanyan/pq/green/2020/*/ \
        --input_yellow=gs://mage-data-zoom-camp-marfanyan/pq/yellow/2020/*/ \
        --output=ny_taxi.reports_2020
```

