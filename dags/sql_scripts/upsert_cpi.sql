BEGIN;

-- Upsert cpi table
UPDATE movies.cpi 
SET consumer_price_index = sc.consumer_price_index
FROM movies.stage_cpi sc
WHERE movies.cpi.date_cd= sc.date_cd; 


INSERT INTO movies.cpi
SELECT sc.* FROM movies.stage_cpi sc LEFT JOIN movies.cpi 
ON sc.date_cd = movies.cpi.date_cd
WHERE movies.cpi.date_cd IS NULL;

DROP TABLE IF EXISTS movies.stage_cpi;

END;