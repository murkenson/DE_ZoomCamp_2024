## Week 1 Homework

In this homework we'll prepare the environment and practice with Docker and SQL


## Question 1. Knowing docker tags

Which tag has the following text? - *Automatically remove the container when it exits* 


>Answer:
```
--rm
```



## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

>Answer:
```
0.42.0
```
>Full output of command
```
root@b71b3e0cec6e:/# pip list
Package    Version
---------- -------
pip        23.0.1
setuptools 58.1.0
wheel      0.42.0
```



## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?


>Command:

```sql
SELECT COUNT(*) as total_trips
FROM public.green_tripdata
WHERE lpep_pickup_datetime::date = '2019-09-18';
```

>Output:
```

count|
-----+
15767|
```

>Answer:
```
15767
```

## Question 4. Average

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

>Command:

```sql
SELECT cast(lpep_pickup_datetime AS DATE)
FROM public.green_tripdata
ORDER BY trip_distance DESC limit 1;
```

>Output:

```
lpep_pickup_datetime|
--------------------+
          2019-09-26|
```

>Answer:

```
2019-09-26
```


## Question 5. Most popular destination

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

>Command:

```sql
SELECT tzl."Borough"
FROM public.green_tripdata AS gt
LEFT JOIN public.zones AS tzl ON gt."PULocationID" = tzl."LocationID"
WHERE lpep_pickup_datetime BETWEEN '2019-09-18 00:00:00'
		AND '2019-09-18 23:59:59'
GROUP BY tzl."Borough"
HAVING sum(gt.total_amount) > 50000
ORDER BY sum(gt.total_amount) DESC limit 3
```

>Output:

```
Borough  |
---------+
Brooklyn |
Manhattan|
Queens   |
```

>Answer:

```
Brooklyn, Manhattan, Queens
```


## Question 6. 

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

>Command:

```sql
SELECT tzl2."Zone" AS "DOZone"
FROM public.green_tripdata AS gt
INNER JOIN public.zones AS tzl ON gt."PULocationID" = tzl."LocationID"
	AND tzl."Zone" = 'Astoria'
LEFT JOIN public.zones AS tzl2 ON gt."DOLocationID" = tzl2."LocationID"
WHERE gt.lpep_pickup_datetime BETWEEN '2019-09-01 00:00:00'
		AND '2019-09-30 23:59:59'
ORDER BY gt.tip_amount DESC limit 1;
```

>Output:

```
DOZone     |
-----------+
JFK Airport|
```

>Answer:

```
JFK Airport
```

## Terraform


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:



```


--
(base) marfanyan@de-zoomcamp:~/data-engineering-zoomcamp/01-docker-terraform/1_terraform_gcp/terraform/terraform_with_variables$ terraform apply

Terraform used the selected providers to generate the following execution plan. Resource actions are
indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be created
  + resource "google_bigquery_dataset" "demo_dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "demo_dataset_thrd"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "US"
      + max_time_travel_hours      = (known after apply)
      + project                    = "ny-rides-marfanyan"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

  # google_storage_bucket.demo-bucket will be created
  + resource "google_storage_bucket" "demo-bucket" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "US"
      + name                        = "terraform-demo-terra-bucket_thrd"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "AbortIncompleteMultipartUpload"
            }
          + condition {
              + age                   = 1
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/ny-rides-marfanyan/datasets/demo_dataset_thrd]
google_storage_bucket.demo-bucket: Creation complete after 2s [id=terraform-demo-terra-bucket_thrd]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
(base) marfanyan@de-zoomcamp:~/data-engineering-zoomcamp/01-docker-terraform/1_terraform_gcp/terraform/terraform_with_variables$
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET