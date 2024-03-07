<p align="center">
  <picture>
    <source srcset="https://github.com/risingwavelabs/risingwave/blob/main/.github/RisingWave-logo-dark.svg" width="500px" media="(prefers-color-scheme: dark)">
    <img src="https://github.com/risingwavelabs/risingwave/blob/main/.github/RisingWave-logo-light.svg" width="500px">
  </picture>
</p>


</div>

<p align="center">
  <a
    href="https://docs.risingwave.com/"
    target="_blank"
  ><b>Documentation</b></a>&nbsp;&nbsp;&nbsp;📑&nbsp;&nbsp;&nbsp;
  <a
    href="https://tutorials.risingwave.com/"
    target="_blank"
  ><b>Hands-on Tutorials</b></a>&nbsp;&nbsp;&nbsp;🎯&nbsp;&nbsp;&nbsp;
  <a
    href="https://cloud.risingwave.com/"
    target="_blank"
  ><b>RisingWave Cloud</b></a>&nbsp;&nbsp;&nbsp;🚀&nbsp;&nbsp;&nbsp;
  <a
    href="https://risingwave.com/slack"
    target="_blank"
  >
    <b>Get Instant Help</b>
  </a>
</p>
<div align="center">
  <a
    href="https://risingwave.com/slack"
    target="_blank"
  >
    <img alt="Slack" src="https://badgen.net/badge/Slack/Join%20RisingWave/0abd59?icon=slack" />
  </a>
  <a
    href="https://twitter.com/risingwavelabs"
    target="_blank"
  >
    <img alt="X" src="https://img.shields.io/twitter/follow/risingwavelabs" />
  </a>
  <a
    href="https://www.youtube.com/@risingwave-labs"
    target="_blank"
  >
    <img alt="YouTube" src="https://img.shields.io/youtube/channel/views/UCsHwdyBRxBpmkA5RRd0YNEA" />
  </a>
</div>

## Stream processing with RisingWave

In this hands-on workshop, we’ll learn how to process real-time streaming data using SQL in RisingWave. The system we’ll use is [RisingWave](https://github.com/risingwavelabs/risingwave), an open-source SQL database for processing and managing streaming data. You may not feel unfamiliar with RisingWave’s user experience, as it’s fully wire compatible with PostgreSQL.

![RisingWave](https://raw.githubusercontent.com/risingwavelabs/risingwave-docs/main/docs/images/new_archi_grey.png)

We’ll cover the following topics in this Workshop: 

- Why Stream Processing?
- Stateless computation (Filters, Projections)
- Stateful Computation (Aggregations, Joins)
- Data Ingestion and Delivery

RisingWave in 10 Minutes:
https://tutorials.risingwave.com/docs/intro

[Project Repository](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04)

## Homework

**Please setup the environment in [Getting Started](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04?tab=readme-ov-file#getting-started) and for the [Homework](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04/blob/main/homework.md#setting-up) first.**


## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the pick up taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

### Question 1

Create a materialized view to compute the average, min and max trip time between each taxi zone.

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

Options:
1. Yorkville East, Steinway
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

>Answer:
```
Yorkville East, Steinway
```

>Full output of command:
```
dev=> CREATE MATERIALIZED VIEW avg_min_max_trip_data AS
dev->
dev-> with t as (
dev(>     SELECT pickup_zone.zone as pickup_taxi_zone, dropoff_zone.zone as dropoff_taxi_zone, t.tpep_pickup_datetime, t.tpep_dropoff_datetime, t.tpep_dropoff_datetime-t.tpep_pickup_datetime as trip_time
dev(>     FROM trip_data as t
dev(>
dev(>     inner join taxi_zone  as pickup_zone
dev(>         ON t.pulocationid = pickup_zone.location_id
dev(>     inner join taxi_zone as dropoff_zone
dev(>         ON t.dolocationid = dropoff_zone.location_id
dev(>         where t.pulocationid <> t.dolocationid
dev(>
dev(>
dev(>         )
dev->
dev-> select    pickup_taxi_zone, dropoff_taxi_zone, min(trip_time) as min_trip_time, max(trip_time) as max_trip_time, avg(trip_time) as avg_trip_time
dev->  from t
dev->
dev->         group by pickup_taxi_zone, dropoff_taxi_zone;
CREATE_MATERIALIZED_VIEW



dev=> WITH max_profit AS (SELECT max(avg_trip_time) max FROM avg_min_max_trip_data)
SELECT pickup_taxi_zone, dropoff_taxi_zone, avg_trip_time, max FROM avg_min_max_trip_data, max_profit
WHERE avg_trip_time >= max;
 pickup_taxi_zone | dropoff_taxi_zone | avg_trip_time |   max
------------------+-------------------+---------------+----------
 Yorkville East   | Steinway          | 23:59:33      | 23:59:33
(1 row)
```

### Question 2

Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

Options:
1. 5
2. 3
3. 10
4. 1

>Answer:
```
1
```

>Full output of command:
```
dev=> drop materialized view avg_min_max_trip_data;
DROP_MATERIALIZED_VIEW
dev=> CREATE  MATERIALIZED VIEW avg_min_max_trip_data AS with t as (
dev(>   SELECT
dev(>     pickup_zone.zone as pickup_taxi_zone,
dev(>     dropoff_zone.zone as dropoff_taxi_zone,
dev(>     t.tpep_pickup_datetime,
dev(>     t.tpep_dropoff_datetime,
dev(>     t.tpep_dropoff_datetime - t.tpep_pickup_datetime as trip_time
dev(>   FROM
dev(>     trip_data as t
dev(>     inner join taxi_zone as pickup_zone ON t.pulocationid = pickup_zone.location_id
dev(>     inner join taxi_zone as dropoff_zone ON t.dolocationid = dropoff_zone.location_id
dev(>   where
dev(>     t.pulocationid <> t.dolocationid
dev(> )
dev-> select
dev->   pickup_taxi_zone,
dev->   dropoff_taxi_zone,
dev->   min(trip_time) as min_trip_time,
dev->   max(trip_time) as max_trip_time,
dev->   avg(trip_time) as avg_trip_time,
dev->   count(*) as cnt
dev-> from
dev->   t
dev-> group by
dev->   pickup_taxi_zone,
dev->   dropoff_taxi_zone;
CREATE_MATERIALIZED_VIEW

dev=> WITH max_profit AS (SELECT max(avg_trip_time) max FROM avg_min_max_trip_data)
dev-> SELECT * from avg_min_max_trip_data, max_profit
dev-> WHERE avg_trip_time >= max;
 pickup_taxi_zone | dropoff_taxi_zone | min_trip_time | max_trip_time | avg_trip_time | cnt |   max
------------------+-------------------+---------------+---------------+---------------+-----+----------
 Yorkville East   | Steinway          | 23:59:33      | 23:59:33      | 23:59:33      |   1 | 23:59:33
(1 row)
```

### Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 12:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 11:00:00 to 2020-01-01 12:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

>Answer:
```
LaGuardia Airport, Lincoln Square East, JFK Airport
```

>Full output of command:

```
dev=> CREATE MATERIALIZED VIEW latest_pickup_time AS
SELECT tpep_pickup_datetime AS pickup_time
FROM trip_data
WHERE tpep_pickup_datetime=(SELECT MAX(tpep_pickup_datetime) FROM trip_data);
CREATE_MATERIALIZED_VIEW
dev=> SELECT
dev->   taxi_zone.Zone AS pickup_zone,
dev->   COUNT(*) AS num_rides
dev-> FROM
dev->   trip_data
dev->   JOIN taxi_zone ON taxi_zone.location_id = trip_data.pulocationid
dev-> WHERE
dev->   trip_data.tpep_pickup_datetime >= (
dev(>     SELECT
dev(>       pickup_time - INTERVAL '17 HOURS'
dev(>     FROM
dev(>       latest_pickup_time
dev(>   )
dev-> GROUP BY
dev->   pickup_zone
dev-> ORDER BY
dev->   num_rides DESC
dev-> LIMIT
dev->   3;

     pickup_zone     | num_rides
---------------------+-----------
 LaGuardia Airport   |        19
 Lincoln Square East |        17
 JFK Airport         |        17
(3 rows)

```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/workshop2
- Deadline: 11 March (Monday), 23:00 CET 

## Rewards 🥳

Everyone who completes the homework will get a pen and a sticker, and 5 lucky winners will receive a Tshirt and other secret surprises!
We encourage you to share your achievements with this workshop on your socials and look forward to your submissions 😁

- Follow us on **LinkedIn**: https://www.linkedin.com/company/risingwave
- Follow us on **GitHub**: https://github.com/risingwavelabs/risingwave
- Join us on **Slack**: https://risingwave-labs.com/slack

See you around!

