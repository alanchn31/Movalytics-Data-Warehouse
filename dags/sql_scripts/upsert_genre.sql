BEGIN;

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

END;