CREATE OR REPLACE TABLE `carris-416110.Carris_Warehouse.fact_customers_reviews` AS
SELECT *
FROM(
WITH cte AS(
SELECT
  ROW_NUMBER() OVER() AS review_id
  ,g.id_bq AS espaco_navegante_id
  ,TIMESTAMP_SECONDS(CAST(g.Date / 1000000000 AS INT64)) AS timstm
  ,g.Rating AS review_score
  ,g.Helpful_count AS number_upvotes_review
  ,g.Review AS full_review
FROM `carris-416110.Carris_Lake.google_reviews` AS g
), cte2 AS(
SELECT
  c.review_id
  ,c.espaco_navegante_id
  ,DATETIME_TRUNC(EXTRACT(DATETIME FROM c.timstm), MINUTE) AS tim
  ,c.review_score
  ,c.number_upvotes_review
  ,c.full_review
FROM cte AS c
)
SELECT
  c2.review_id
  ,c2.espaco_navegante_id
  ,t.time_id
  ,c2.review_score
  ,c2.number_upvotes_review
  ,c2.full_review
FROM cte2 AS c2
  INNER JOIN `carris-416110.Carris_Warehouse.dim_time` AS t
    ON c2.tim = t.full_date
);


ALTER TABLE `carris-416110.Carris_Warehouse.fact_customers_reviews`
ADD PRIMARY KEY (review_id) NOT ENFORCED,
ADD FOREIGN KEY (espaco_navegante_id) REFERENCES `carris-416110.Carris_Warehouse.dim_espaço_navegante` (espaco_navegante_id) NOT ENFORCED,
ADD FOREIGN KEY (time_id) REFERENCES `carris-416110.Carris_Warehouse.dim_time` (time_id) NOT ENFORCED;
