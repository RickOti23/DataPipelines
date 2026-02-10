SELECT COUNT(*) AS short_trips
FROM public.green_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;


SELECT 
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS longest_trip_distance
FROM public.green_trips
WHERE trip_distance < 100
GROUP BY pickup_day
ORDER BY longest_trip_distance DESC
LIMIT 1;

-- Date warehouse results
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-486911.rides.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ricky_debucket/rides_dataset/rides/yellow_tripdata_2024_*.parquet'
  ]
);

-- Creating a regular table
CREATE TABLE de-zoomcamp-486911.rides.yellow_data AS (
SELECT *
FROM de-zoomcamp-486911.rides.external_yellow_tripdata);


-- Question 1: Count of records for the 2024
SELECT
 COUNT(*)
FROM `de-zoomcamp-486911.rides.yellow_data`
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024; -- answer [ 20,332,057]

-- Question 2: Data read estimation 
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT 
 COUNT(DISTINCT(pu_location_id)) as unique1
FROM `de-zoomcamp-486911.rides.yellow_data`;

SELECT 
 COUNT(DISTINCT(pu_location_id))  as unique2
FROM `de-zoomcamp-486911.rides.external_yellow_tripdata`; -- Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

-- Question 3. Understanding columnar storage. 
-- Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.
-- Why are the estimated number of Bytes different?
SELECT
  pu_location_id
FROM `de-zoomcamp-486911.rides.yellow_data`;

SELECT
  pu_location_id,
  do_location_id
FROM `de-zoomcamp-486911.rides.yellow_data`; -- Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


-- Question 4. Counting zero fare trips: How many records have a fare_amount of 0?
SELECT
 COUNT(*)
FROM `de-zoomcamp-486911.rides.yellow_data`
WHERE fare_amount = 0; -- Answer: 8,333

-- Question 5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

CREATE TABLE de-zoomcamp-486911.rides.yellow_trips_partioned
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY vendor_id 
AS (
SELECT *
FROM `de-zoomcamp-486911.rides.yellow_data`
); -- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID


-- Question 6. Partition benefits: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
-- Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
SELECT DISTINCT vendor_id
FROM `de-zoomcamp-486911.rides.yellow_data`
WHERE DATE(tpep_dropoff_datetime)
BETWEEN DATE('2024-03-01') AND DATE('2024-03-15');

SELECT DISTINCT vendor_id
FROM `de-zoomcamp-486911.rides.yellow_trips_partioned`
WHERE DATE(tpep_dropoff_datetime)
BETWEEN DATE('2024-03-01') AND DATE('2024-03-15'); -- Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


-- Question 7. External table storage. Where is the data stored in the External Table you created?
-- Answer: GCP Bucket

-- Question 8. Clustering best practices
-- Answer: True

-- Question 9
SELECT count(*)
FROM `de-zoomcamp-486911.rides.yellow_data`
