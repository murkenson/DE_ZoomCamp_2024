## Week 2 Homework

## Let's get started 

The project is started by cloning the repo:

```bash
git clone https://github.com/mage-ai/mage-zoomcamp.git mage-zoomcamp
```
The details of setting up the containers can be found in the [github repo of mage for DE Zoomcamp 2024](https://github.com/mage-ai/mage-zoomcamp).

## Using Mage.AI as the workflow orchestration tools &#128295;
 
- Testing of block in real-time has cut down significant amount of time in switching platforms.
- Crafting an ETL pipeline from API to Google Cloud Storage as .csv file or partitioned parquet file using pyarrow. 
- Ease of deployment of Mage through Terraform and Google Cloud.
- Harnessing the paramerizated execution and backfill capability of Mage.


![Pipeline](/week_2/static/mage_pipeline.png)




## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

* 266,855 rows x 20 columns
* 544,898 rows x 18 columns
* 544,898 rows x 20 columns
* 133,744 rows x 20 columns

>Answer:
```
266,855 rows x 20 columns
```

![Data Loading](/week_2/static/mage_data_loader.png)



## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is equal to 0 _or_ the trip distance is equal to zero, how many rows are left?

* 544,897 rows
* 266,855 rows
* 139,370 rows
* 266,856 rows

>Answer:
```
139,370 rows
```


![Transformer](/week_2/static/mage_transformer.png)


## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

* data = data['lpep_pickup_datetime'].date
* data('lpep_pickup_date') = data['lpep_pickup_datetime'].date
* data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
* data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()

## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

* 1, 2, or 3
* 1 or 2
* 1, 2, 3, 4
* 1

>Answer:
```
1 or 2
```

## Question 5. Data Transformation

How many columns need to be renamed to snake case?

* 3
* 6
* 2
* 4

>Answer:
```
4
```

## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

* 96
* 56
* 67
* 108


>Answer:
```
96
```

## Submitting the solutions

* Form for submitting: TBA 

Deadline: TBA

## Solution

Will be added after the due date
