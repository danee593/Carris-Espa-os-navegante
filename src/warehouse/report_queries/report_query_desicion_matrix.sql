/*

-- This SQL script calculates a performance score for each `espaco_navegante` (bus stop)
-- in the `carris-416110.Carris_Warehouse.view_desicion_matrix` view.

-- The script performs the following steps:

1. **Declares weights** for different factors (expected wait time, opening hours, bus stops, rating)
   - These weights control the relative importance of each factor in the final score.

2. **Normalizes data** within the decision matrix view:
   - Finds minimum and maximum values for each factor (wait time, opening hours, bus stops, rating)
   - Calculates a normalized value for each factor by dividing the actual value by its respective maximum.

3. **Calculates weighted normalized scores** for each factor:
   - Multiplies the normalized value of each factor by its corresponding weight.

4. **Joins the normalized data with the `dim_espaço_navegante` table:**
   - Retrieves additional information about bus stops (name, location).

5. **Calculates a performance score** by summing the weighted scores for each factor.

6. **Orders the results** by performance score in descending order (highest score first).

7. **Removes trademark symbol** from bus stop names using `REGEXP_REPLACE`.

**Important Notes:**

- Make sure the sum of all weights (`weight_expected_wait` + `weight_number_opening_hours` +
  `weight_number_bus_stops` + `weight_average_rating`) equals 1.
- Adjust the weights according to your specific priorities for evaluating bus stops.


This is the structure of the desicion matrix, in the view the data needs to be normalized (all units in same scale),
add the weights, as shown in this ascii table:

+----------------------------------+---------+----------------+
| Criterion                        | Weight  | Type           |
+----------------------------------+---------+----------------+
| Expected wait time in rush hour  | 0.45    | Non Beneficial |
+----------------------------------+---------+----------------+
| Number of opening hours per week | 0.15    | Beneficial     |
+----------------------------------+---------+----------------+
| Number of bus stops              | 0.15    | Beneficial     |
+----------------------------------+---------+----------------+
| Average rating on Google Reviews | 0.25    | Beneficial     |
+----------------------------------+---------+----------------+

*/

-- Make sure that this adds to 1!

DECLARE weight_expected_wait FLOAT64 DEFAULT 0.45;
DECLARE weight_number_opening_hours FLOAT64 DEFAULT 0.15;
DECLARE weight_number_bus_stops FLOAT64 DEFAULT 0.15;
DECLARE weight_average_rating FLOAT64 DEFAULT 0.25;

BEGIN
WITH normalize_constants AS(
SELECT
  dm.espaco_navegante_id
  ,dm.average_expected_wait_time_in_rush_hour
  ,MIN(dm.average_expected_wait_time_in_rush_hour) OVER() AS min_average_wait
  ,dm.number_of_opening_hours_per_week
  ,MAX(dm.number_of_opening_hours_per_week) OVER() AS max_number_open_hours
  ,dm.number_of_bus_stops
  ,MAX(dm.number_of_bus_stops) OVER() AS max_number_bus_stops
  ,dm.weighted_average_rating_on_google_reviews
  ,MAX(dm.weighted_average_rating_on_google_reviews) OVER() AS max_wighted_rating
FROM `carris-416110.Carris_Warehouse.view_desicion_matrix` AS dm
)
, normalized_matrix AS(
SELECT
  nc.espaco_navegante_id
  ,nc.min_average_wait/nc.average_expected_wait_time_in_rush_hour AS normalized_average_expected_wait_time_in_rush_hour
  ,nc.number_of_opening_hours_per_week/nc.max_number_open_hours AS normalized_number_of_opnening_hours_per_week
  ,nc.number_of_bus_stops/nc.max_number_bus_stops AS normalized_number_of_bus_stops
  ,nc.weighted_average_rating_on_google_reviews/nc.max_wighted_rating AS normalized_weighted_average_rating_on_google_reviews
FROM normalize_constants nc
)
, normalized_times_weights AS(
SELECT
  nm.espaco_navegante_id
  ,nm.normalized_average_expected_wait_time_in_rush_hour * weight_expected_wait AS expected_wait_time_in_rush_hour
  ,nm.normalized_number_of_opnening_hours_per_week * weight_number_opening_hours AS number_of_opnening_hours_per_week
  ,nm.normalized_number_of_bus_stops * weight_number_bus_stops AS number_of_bus_stops
  ,nm.normalized_weighted_average_rating_on_google_reviews * weight_average_rating AS average_rating_on_google_reviews
FROM normalized_matrix AS nm
)
SELECT
  ntw.espaco_navegante_id
  ,REGEXP_REPLACE(en.name, '® Carris Metropolitana', '') AS name
  ,en.location
  ,ntw.expected_wait_time_in_rush_hour
  ,ntw.number_of_opnening_hours_per_week
  ,ntw.number_of_bus_stops
  ,ntw.average_rating_on_google_reviews
  ,ntw.expected_wait_time_in_rush_hour + ntw.number_of_opnening_hours_per_week + ntw.number_of_bus_stops + ntw.           average_rating_on_google_reviews AS performance_score
FROM normalized_times_weights AS ntw
  INNER JOIN `carris-416110.Carris_Warehouse.dim_espaço_navegante` AS en
    ON ntw.espaco_navegante_id = en.espaco_navegante_id
ORDER BY performance_score DESC;
END;
