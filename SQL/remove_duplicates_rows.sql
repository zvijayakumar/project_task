--You are given a table city_distances that stores information about the distances between pairs of cities. 
--Each route is represented twice in the table: once in each direction (e.g., Mumbai to Kolkata and Kolkata to Mumbai).
--Your task is to write a SQL query that removes these duplicates and ensures only one record is retained for each route, irrespective of direction. 
--The resulting table should list the routes in ascending order of city names.

--answer 2 
select distinct case when cd.start_city < cd.end_city then cd.start_city else end_city end as start_city,
case when cd.start_city < cd.end_city then cd.end_city else start_city end as end_city,cd.distance
from city_distances cd 



--answer 3
SELECT 
    c1.start_city,
    c1.end_city,
    c1.distance,
    c2.start_city,
    c2.end_city,
    c2.distance
FROM 
    city_distances c1
LEFT JOIN 
    city_distances c2
ON 
    c1.start_city = c2.end_city
    AND c1.end_city = c2.start_city
    AND c1.start_city < c2.start_city
    where c2.start_city is null 
    
    
    --answer 1
SELECT 
    LEAST(start_city, end_city) AS start_city,
    GREATEST(start_city, end_city) AS end_city,
    distance
FROM 
    city_distances
GROUP BY 
    LEAST(start_city, end_city), 
    GREATEST(start_city, end_city), 
    distance;
