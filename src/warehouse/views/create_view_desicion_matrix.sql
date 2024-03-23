/* This script creates a View that contains a desicion matrix, with the following columns:

  espaco_navegante_id
  average_expected_wait_time_in_rush_hour
  number_of_opening_hours_per_week
  number_of_bus_stops
  weighted_average_rating_on_google_reviews

View Structure
The view is constructed using a series of Common Table Expressions (CTEs) to perform data manipulation and aggregation. The final SELECT statement combines the results of these CTEs to produce the final output.

CTEs
fill_nas: This CTE fills missing expected wait times (expected_wait_time) by calculating the average expected wait time for rows with the same number of customers currently waiting (customers_currently_waiting). This is done using a window function to partition the data by customers_currently_waiting and calculate the average.
max_customers_per_date: Identifies the maximum number of customers waiting for each espaco_navegante on a given day and hour.
avg_customers_day_hour: Calculates the average maximum number of customers waiting for each espaco_navegante on a given day and hour.
ranked_hours: Ranks the hours for each espaco_navegante based on the average maximum number of customers waiting, identifying the rush hours.
rush_hours: Filters the rush hours identified in the previous CTE.
max_wait_time_rush_hour: Determines the maximum expected wait time during rush hours for each espaco_navegante.
espaco_navegante_wait_rush_hour: Calculates the average maximum wait time during rush hours for each espaco_navegante.
scores_weights: Calculates the weighted average review score for each espaco_navegante, taking into account the number of upvotes for each review.
weighted_avg_score: Aggregates the weighted average review scores for each espaco_navegante.

Final SELECT Statement
The final SELECT statement combines data from the dim_espaço_navegante table with the results from the CTEs to produce the decision matrix. This includes:

espaco_navegante_id
avg_max_wait_time_rush_hour
number_of_open_hours_per_week
number_of_bus_stops
weighted_average_review_score
*/

CREATE OR REPLACE VIEW `carris-416110.Carris_Warehouse.view_desicion_matrix` AS
SELECT *
FROM(

WITH fill_nas AS( -- fill nas by looking at the average expected wait time of those with same number of customers currently waiting.
                  -- Exmpl: if NA is encoutered in a row where customers_currently_waiting is 50 it will get filled with the avg wait time of those that also
                  -- had 50 customers waiting.
SELECT
  c.espaco_navegante_id
  ,DATETIME_TRUNC(t.full_date, DAY) AS cday -- this will prove useful as the aggreagation is per DAY.
  ,t.day_of_week
  ,t.hour
  ,c.customers_currently_waiting
  ,CASE WHEN 
    c.expected_wait_time IS NULL THEN AVG(c.expected_wait_time/60) OVER(PARTITION BY c.customers_currently_waiting)
    ELSE c.expected_wait_time/60 
    END AS expected_wait_time_imputed
FROM `carris-416110.Carris_Warehouse.fact_customers_queue` AS c
  INNER JOIN `carris-416110.Carris_Warehouse.dim_time` AS t
    ON c.time_id = t.time_id
WHERE c.is_open IS TRUE
)
, max_customers_per_date AS( -- get the max number of customers waiting for a given date, day and hour per espaco_navegante
SELECT
  f.espaco_navegante_id
  ,f.day_of_week
  ,f.hour
  ,MAX(f.customers_currently_waiting) AS max_cust_wait
FROM fill_nas AS f
GROUP BY f.cday, f.espaco_navegante_id, f.day_of_week, f.hour
)
, avg_customers_day_hour AS( -- with the max number of cust waiting get the avg for a given day and hour per espaco_navegante
SELECT
  mc.espaco_navegante_id
  ,mc.day_of_week
  ,mc.hour
  ,AVG(max_cust_wait) AS avg_max_cust_wait
FROM max_customers_per_date AS mc
GROUP BY mc.espaco_navegante_id, mc.day_of_week, mc.hour
)
, ranked_hours AS( -- assign a rank (if repeated, highly likely just let the interpreted decide) ordered by the highest avg_max_customer
                  -- for a given espaco navegante and day of week. This will rank the highest hour of avg max customers waiting giving the rush hour per day
                  -- per espaco navegante.
SELECT
  ac.espaco_navegante_id
  ,ac.day_of_week
  ,ac.hour
  ,ac.avg_max_cust_wait
  ,ROW_NUMBER() OVER(PARTITION BY ac.espaco_navegante_id, ac.day_of_week ORDER BY ac.avg_max_cust_wait DESC) AS ranked_rush
FROM avg_customers_day_hour AS ac
)
, rush_hours AS( -- simply filter out the highest ranked, as this are the rush hours per day per espaco navegante.
SELECT
  rh.espaco_navegante_id
  ,rh.day_of_week
  ,rh.hour
FROM ranked_hours AS rh
WHERE ranked_rush = 1
)
, max_wait_time_rush_hour AS( -- get the max number of wait time for a given date, day and rush hour per espaco_navegante
                              -- rush hour here is key, look at the inner join, you filter out every observation that's non in rush hour
SELECT
  fn.espaco_navegante_id
  ,fn.cday
  ,fn.day_of_week
  ,fn.hour
  ,MAX(expected_wait_time_imputed) AS max_wait_time_in_rush_hour
FROM fill_nas AS fn
  INNER JOIN rush_hours AS rh
    ON fn.espaco_navegante_id = rh.espaco_navegante_id
      AND fn.day_of_week = rh.day_of_week
      AND fn.hour = rh.hour
GROUP BY fn.espaco_navegante_id, fn.cday, fn.day_of_week, fn.hour
)
, espaco_navegante_wait_rush_hour AS( -- get the average max wait time per espaco navegante in rush hours (these differ acrross days and espaco navegante)
SELECT 
  mrh.espaco_navegante_id
  ,CAST(CEIL(AVG(mrh.max_wait_time_in_rush_hour)) AS INT) AS avg_max_wait_time_rush_hour -- notice that it's been rounded to the nearest int
FROM max_wait_time_rush_hour AS mrh
GROUP BY espaco_navegante_id
)
-- _________________ END OF RUSH HOUR BEGINING OF WEIGHTED AVERAGE SCORE REVIEW ________________________
, scores_weights AS(
SELECT
  cr.espaco_navegante_id
  ,cr.review_score
  ,cr.number_upvotes_review + 1 AS score_weight -- most of the comments does not have a upvote, therefore 0. This will be weighted as 1.
                                                -- however, if a score has a 3 upvotes, this will count as 4 because 4 people reacted, 1 that wrote, 3 reacted.
  ,cr.review_score * (cr.number_upvotes_review + 1) AS score_times_weight
FROM `carris-416110.Carris_Warehouse.fact_customers_reviews` AS cr
)
, weighted_avg_score AS(
SELECT
  sw.espaco_navegante_id
  ,ROUND(SUM(sw.score_times_weight) / SUM(sw.score_weight),2) AS weighted_average_review_score
FROM scores_weights AS sw
GROUP BY sw.espaco_navegante_id
)
-- _______ END OF DATA MANUPULATION PUTTING THE DESICION MATRIX TOGHETHER ___________
SELECT
  den.espaco_navegante_id
  ,rh.avg_max_wait_time_rush_hour AS average_expected_wait_time_in_rush_hour
  ,den.number_of_open_hours_per_week AS number_of_opening_hours_per_week
  ,den.number_of_bus_stops
  ,was.weighted_average_review_score AS weighted_average_rating_on_google_reviews
FROM `carris-416110.Carris_Warehouse.dim_espaço_navegante` AS den
  INNER JOIN espaco_navegante_wait_rush_hour AS rh
    ON den.espaco_navegante_id = rh.espaco_navegante_id
  INNER JOIN weighted_avg_score AS was
    ON den.espaco_navegante_id = was.espaco_navegante_id

);