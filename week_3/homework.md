## Week 3 Homework

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>


In this case, I am using `Mage AI` to load [Green Taxi Trip Records 2022](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) into Google Cloud Storage (GCS).

![bigquery_1](/week_3/static/mage_pipeline.png)
![bigquery_1](/week_3/static/bigquery_1.png)

>Command:
```sql
--Creating external table refering to gcs path
CREATE OR REPLACE EXTERNAL TABLE
  ny-rides-marfanyan.ny_taxi.external_green_data_2022_tripdata OPTIONS ( format = 'parquet',
    uris = ['gs://mage-data-zoom-camp-marfanyan/ny-green_taxi_data/year_month=2022-*']);

--Creating a non partitinoned table from external table
CREATE TABLE
  ny_taxi.green_data_taxi_2022 AS
SELECT
  *
FROM
  ny-rides-marfanyan.ny_taxi.external_green_data_2022_tripdata; -- external table
```


For more details and additional functions, refer to the [BigQuery Standard SQL Dates and Times](https://count.co/sql-resources/bigquery-standard-sql/dates-and-times#the-basics) documentation.


## Question 1.

Question 1: What is count of records for the **2022 Green Taxi Data**??

- 65,623,481
- 840,402
- 1,936,423
- 253,647

>Answer:
```
840,402
```

>Command:
```sql
SELECT
  COUNT(VendorID)
FROM
  ny-rides-marfanyan.ny_taxi.green_taxi_2022
```

![bigquery_1](/week_3/static/bigquery_2.png)


## Question 2.

Write a query to count the distinct number of `PULocationIDs` for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

>Answer:
```
- 0 MB for the External Table and 6.41MB for the Materialized Table
```

>Command:
```sql
SELECT
  COUNT(DISTINCT PULocationID)
FROM
  ny-rides-marfanyan.ny_taxi.external_green_data_2022_tripdata;

SELECT
  COUNT(DISTINCT PULocationID)
FROM
  ny-rides-marfanyan.ny_taxi.green_taxi_2022;
```  

![bigquery_1](/week_3/static/bigquery_3.png)
![bigquery_1](/week_3/static/bigquery_4.png)


## Question 3. 

How many records have a fare_amount of 0?

- 12,488
- 128,219
- 112
- 1,622

>Answer:
```
1,622
```

>Command:
```sql
SELECT
  COUNT(1)
FROM
  ny-rides-marfanyan.ny_taxi.green_data_taxi_2022
WHERE
  fare_amount = 0
```

![bigquery_1](/week_3/static/bigquery_8.png)


## Question 4. 

What is the best strategy to make an optimized table in Big Query if your query will always order the results by `PUlocationID` and filter based on `lpep_pickup_datetime`? (Create a new table with this strategy)

- Cluster on `lpep_pickup_datetime` Partition by `PUlocationID`
- Partition by `lpep_pickup_datetime`  Cluster on `PUlocationID`
- Partition by `lpep_pickup_datetime` and Partition by `PUlocationID`
- Cluster on by `lpep_pickup_datetime` and Cluster on `PUlocationID`

>Answer:
```
Partition by `lpep_pickup_datetime`  Cluster on `PUlocationID`
```

>Command:
```sql
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE
  ny-rides-marfanyan.ny_taxi.green_data_taxi_2022_partition
PARTITION BY
  lpep_pickup_datetime
CLUSTER BY
  PUlocationID AS
SELECT
  VendorID,
  DATE(TIMESTAMP_SECONDS(CAST(lpep_pickup_datetime / 1000000000 AS INT64))) AS lpep_pickup_datetime,
  lpep_dropoff_datetime,
  store_and_fwd_flag,
  RatecodeID,
  PULocationID,
  DOLocationID,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge
FROM
  ny-rides-marfanyan.ny_taxi.green_data_taxi_2022;
```

## Question 5. 

Write a query to retrieve the distinct `PULocationID` between `lpep_pickup_datetime` `06/01/2022` and `06/30/2022` (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

>Answer:
```
12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
```

>Command:
```sql
SELECT
  COUNT(DISTINCT PULocationID)
FROM
  ny-rides-marfanyan.ny_taxi.green_data_taxi_2022 --non-partitioned 
WHERE
  DATE(TIMESTAMP_SECONDS(CAST(lpep_pickup_datetime / 1000000000 AS INT64))) BETWEEN DATE('2022-06-01')
  AND DATE('2022-06-30');


SELECT
  COUNT(DISTINCT PULocationID)
FROM
  ny-rides-marfanyan.ny_taxi.green_data_taxi_2022_partition --partitioned 
WHERE
  lpep_pickup_datetime BETWEEN DATE('2022-06-01')
  AND DATE('2022-06-30');
```


![bigquery_1](/week_3/static/bigquery_5.png)
![bigquery_1](/week_3/static/bigquery_6.png)


## Question 6.

Where is the data stored in the External Table you created?


- Big Query
- GCP Bucket
- Big Table
- Container Registry


>Answer:
```
GCP Bucket
```

## Question 7.

It is best practice in Big Query to always cluster your data:

- True
- False

>Answer:
```
False 
```

No increase of efficiency if data size less than 1GB

## (Bonus: Not worth points) Question 8:

No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?




1. **Materialized Table (Internal):** The estimated read size of 114.11 MB indicates the amount of data that would be read if you were to query all columns from the internal storage system of BigQuery where the table physically resides.

2. **External Table (Google Cloud Storage):** The 0 MB estimate for the external table suggests that the data is stored externally in Google Cloud Storage. In this case, the estimated read size is not applicable within the internal system of BigQuery since the data is accessed directly from Google Cloud Storage when needed, and BigQuery doesn't need to read the entire dataset into its internal storage.

![bigquery_1](/week_3/static/bigquery_9.png)

>Command:
```sql
SELECT
  *
FROM
  `ny-rides-marfanyan.ny_taxi.green_data_taxi_2022`  


SELECT
  *
FROM
  `ny-rides-marfanyan.ny_taxi.external_green_data_2022_tripdata`;
```

## Submitting the solutions

* Form for submitting: TBD
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: TBD