/* Scheduled query to insert data in a daily manner to the fact table */


DECLARE today_date DATE DEFAULT CURRENT_DATE();
DECLARE yesterday_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

INSERT INTO `carris-416110.Carris_Warehouse.fact_customers_queue` (espaco_navegante_id,
  time_id,
  customers_currently_waiting,
  expected_wait_time,
  active_counters,
  is_open)

SELECT *
FROM(

WITH cte AS(
SELECT
  CAST(carris.id AS INT64) AS espaco_navegante_id
  ,DATETIME_TRUNC(PARSE_DATETIME('%y/%m/%d %H:%M:%S', carris.time), MINUTE) AS time_format -- note that year is %y instead of %Y.
  ,SAFE_CAST(carris.currently_waiting AS INT) AS customers_currently_waiting
  ,SAFE_CAST(carris.expected_wait_time AS INT) AS expected_wait_time
  ,SAFE_CAST(carris.active_counters AS INT) AS active_counters
  ,SAFE_CAST(carris.is_open AS BOOL) AS is_open
FROM `carris-416110.Carris_Lake.espacos_navegante` AS carris
WHERE PARSE_DATETIME('%y/%m/%d %H:%M:%S', carris.time) >= yesterday_date
  AND PARSE_DATETIME('%y/%m/%d %H:%M:%S', carris.time) < today_date
)

SELECT
  c.espaco_navegante_id
  ,dt.time_id
  ,c.customers_currently_waiting
  ,c.expected_wait_time
  ,c.active_counters
  ,c.is_open
FROM cte AS c
  INNER JOIN `carris-416110.Carris_Warehouse.dim_time` AS dt
    ON c.time_format = dt.full_date
ORDER BY dt.time_id
);
