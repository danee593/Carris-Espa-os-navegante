/*

bigquery does allow a clear syntax to generate a sequence of numbers,
GENERATE_ARRAY imposis a hard limit on 1048575 -> 2^20 - 1
https://stackoverflow.com/questions/38884708/how-to-generate-series-in-bigquery-standard-sql/39082506


5 years is 2,628,000

*/

CREATE OR REPLACE TABLE `carris-416110.Carris_Warehouse.dim_time` AS
SELECT *
FROM(


WITH generate_series AS(
SELECT num1 * 1000000 + num2 AS num
FROM UNNEST(GENERATE_ARRAY(0, 9)) AS num1
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, 999999)) AS num2
WHERE (num1 * 1000000 + num2) <= 2628000
)
, dates_seconds AS(
SELECT
  DATETIME_ADD(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2024-03-04 08:00:00'), INTERVAL gs.num MINUTE) AS full_date
FROM generate_series AS gs
)

SELECT
  ROW_NUMBER() OVER(ORDER BY ds.full_date) AS time_id
  ,ds.full_date
  ,EXTRACT(DAYOFWEEK FROM ds.full_date) AS day_of_week
  ,EXTRACT(HOUR FROM ds.full_date) AS hour
  ,EXTRACT(DAY FROM ds.full_date) AS day_number_in_month
  ,EXTRACT(WEEK FROM ds.full_date) AS week_number_in_year
  ,EXTRACT(MONTH FROM ds.full_date) AS month_number
  ,CASE
  WHEN EXTRACT(DAYOFWEEK FROM ds.full_date) IN (1,6) THEN FALSE
  ELSE TRUE
  END AS weekday_flag
  ,FALSE AS holiday_flag
FROM dates_seconds AS ds
);

ALTER TABLE `carris-416110.Carris_Warehouse.dim_time`
ADD PRIMARY KEY (time_id) NOT ENFORCED;