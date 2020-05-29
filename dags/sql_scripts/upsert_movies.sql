-- Upsert movies table
UPDATE movies 
SET is_adult = mm.is_adult, budget = mm.budget, original_language = mm.original_language,
title = mm.title, popularity = mm.popularity, release_date = mm.release_date,
revenue = mm.revenue, vote_count = mm.vote_count, vote_average = mm.vote_average
FROM stage_movies mm
WHERE movies.movie_id = mm.movie_id; 


INSERT INTO movies 
SELECT mm.* FROM stage_movies mm LEFT JOIN movies 
ON mm.movie_id = movies.movie_id
WHERE movies.movie_id IS NULL;

DROP TABLE stage_movies;

-- Update movie_genre table
INSERT INTO movies.movie_genre 
SELECT mg.* FROM movies.stage_movie_genre mg LEFT JOIN movies.movie_genre
ON mg.movie_id = movies.movie_genre.movie_id AND mg.genre_id = movies.movie_genre.genre_id
WHERE movies.movie_genre.movie_id IS NULL;

DROP TABLE movies.stage_movie_genre;

-- Upsert genre table
UPDATE movies.genre 
SET genre_name = mg.genre_name
FROM movies.stage_genre mg
WHERE movies.genre.genre_id = mg.genre_id; 


INSERT INTO movies.genre
SELECT mg.* FROM movies.stage_genre mg LEFT JOIN movies.genre
ON mg.genre_id = movies.genre.genre_id
WHERE movies.genre.genre_id IS NULL;

DROP TABLE movies.stage_genre;

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

DROP TABLE movies.stage_date;
