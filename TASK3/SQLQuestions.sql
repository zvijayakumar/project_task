-- interactions definition

CREATE TABLE IF NOT EXISTS "interactions" (
"interaction_id" INTEGER PRIMARY KEY,
  "user_id" INTEGER,
  "product_id" INTEGER,
  "action" TEXT,
  "timestamp" TIMESTAMP,
  "interaction_count_user" INTEGER,
  "interaction_count_user_product" INTEGER
);

--     1. Total number of interactions per day.

SELECT DATE(timestamp) AS interaction_date, COUNT(*) AS total_interactions
FROM interactions
GROUP BY DATE(timestamp)
ORDER BY interaction_date;

--     2. Top 5 users by the number of interactions.

SELECT user_id, COUNT(*) AS total_interactions
FROM interactions
GROUP BY user_id
ORDER BY total_interactions DESC
LIMIT 5;


--3. Most interacted products based on the number of interactions.

SELECT product_id, COUNT(*) AS total_interactions
FROM interactions
GROUP BY product_id
ORDER BY total_interactions DESC;

--3. Optimization Technique 
 
--Creating Index on the frequently used column to speed up the query retrival time
--If the data is going to grow expoentialy use the columnar based datawarehouse like redshift / snowflake for faster analytical query
--Partition the data in data lake for efficient storage and retrival


