BEGIN;

-- Upsert date table
UPDATE movies.date 
SET day = md.day, week = md.week, month = md.month,
quarter = md.quarter, year = md.year
FROM movies.stage_date md
WHERE movies.date.release_date = md.release_date; 

INSERT INTO movies.date
SELECT md.* FROM movies.stage_date md LEFT JOIN movies.date
ON md.release_date = movies.date.release_date
WHERE movies.date.release_date IS NULL;

DROP TABLE IF EXISTS movies.stage_date;

END;