/* 
Probably there is a dynamic way to do this that is much easier but this is beyond my sql skills, in python this would be a breeze.
basically just get the distinct combinations of id, name, lat, long, etc.
make an equivalent of a table value constructor of the following pattern to join the two ctes and get the final result.

All of the schedules follows this pattern
['08:00-19:00'] -> 11 * 5 = 55
['08:00-21:00'] -> 13 * 7 = 91
['09:30-20:00'] -> 10.5 * 7 = 73.5
['09:00-20:00'] -> 11 * 5 = 55
['09:00-12:30', '13:30-18:00'] -> 8 * 5 = 40
['08:00-12:00', '14:00-18:00'] -> 8 * 5 = 40
all are from mon-friday but ['08:00-21:00'] AND ['09:30-20:00'] these are every day
but are easily identifiable
*/

CREATE OR REPLACE TABLE `carris-416110.Carris_Warehouse.dim_espaço_navegante` AS
SELECT *
FROM(
WITH cte AS(
SELECT DISTINCT
  CAST(espacos.id AS INT64) AS espaco_navegante_id
  ,espacos.name
  ,CAST(espacos.lon AS FLOAT64) AS longitude
  ,CAST(espacos.lat AS FLOAT64) AS latitude
  ,espacos.phone
  ,espacos.address
  ,espacos.postal_code
  ,espacos.municipality_name
  ,espacos.district_name
  ,espacos.stops
  ,espacos.hours_monday
FROM `carris-416110.Carris_Lake.espacos_navegante` AS espacos
), schedule_constructor AS( -- GoogleSQL does not have a table value constructor like T-SQL so this is the most understable way here.
  SELECT 
    '[\'08:00-19:00\']' AS schedule
    ,55 AS hours_week
  UNION ALL
  SELECT 
    '[\'08:00-21:00\']' AS schedule
    ,91 AS hours_week
  UNION ALL
  SELECT
    '[\'09:30-20:00\']' AS schedule
    ,73.5 AS hours_week
  UNION ALL
  SELECT
    '[\'09:00-20:00\']' AS schedule
    ,55 AS hours_week
  UNION ALL
  SELECT
    '[\'09:00-12:30\', \'13:30-18:00\']' AS schedule
    ,40 AS hours_week
  UNION ALL
  SELECT
    '[\'08:00-12:00\', \'14:00-18:00\']' AS schedule
    ,40 AS hours_week
)

SELECT
  c.espaco_navegante_id
  ,c.name
  ,ST_GEOGPOINT(c.longitude , c.latitude) AS location
  ,c.phone
  ,c.address
  ,c.postal_code
  ,c.municipality_name
  ,c.district_name
  ,CASE WHEN -- just create the shift with the monday hours.
    c.hours_monday IN ('[\'08:00-21:00\']', '[\'09:30-20:00\']') THEN CONCAT("Mon-Sun ", c.hours_monday)
    ELSE CONCAT("Mon-Fri ", c.hours_monday) END AS shift
  ,sc.hours_week AS number_of_open_hours_per_week
  ,LENGTH(c.stops) - LENGTH(REGEXP_REPLACE(c.stops, r',', '')) + 1 AS number_of_bus_stops -- thanks https://stackoverflow.com/questions/23609273/count-the-number-of-occurences-of-a-character-in-a-string-bigquery
FROM cte AS c
  INNER JOIN schedule_constructor AS sc
    ON c.hours_monday = sc.schedule
);

ALTER TABLE `carris-416110.Carris_Warehouse.dim_espaço_navegante`
ADD PRIMARY KEY (espaco_navegante_id) NOT ENFORCED;
