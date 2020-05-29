CREATE SCHEMA IF NOT EXISTS movies;


CREATE TABLE IF NOT EXISTS movies.stage_ratings (
    user_movie_id BIGINT IDENTITY(0,1),
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating NUMERIC,
    primary key (user_movie_id)
) diststyle key distkey(movie_id);

CREATE TABLE IF NOT EXISTS movies.ratings (
    user_movie_id INT IDENTITY(0,1),
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating NUMERIC,
    primary key (user_movie_id)
) diststyle key distkey(movie_id);

CREATE TABLE IF NOT EXISTS movies.stage_movies (
    movie_id INT NOT NULL,
    is_adult BOOLEAN NOT NULL,
    budget BIGINT NOT NULL,
    original_language CHAR(2) NOT NULL,
    title VARCHAR(30) NOT NULL,
    popularity FLOAT,
    release_date DATE,
    revenue BIGINT NOT NULL SORTKEY,
    vote_count INT,
    vote_average FLOAT,
    primary key (movie_id)
) diststyle key distkey(movie_id);

CREATE TABLE IF NOT EXISTS movies.movies (
    movie_id INT NOT NULL,
    is_adult BOOLEAN NOT NULL,
    budget BIGINT NOT NULL,
    original_language CHAR(2) NOT NULL,
    title VARCHAR(30) NOT NULL,
    popularity FLOAT,
    release_date DATE,
    revenue BIGINT NOT NULL SORTKEY,
    vote_count INT,
    vote_average FLOAT,
    primary key (movie_id)
) diststyle key distkey(movie_id);

CREATE TABLE IF NOT EXISTS movies.stage_movie_genre (
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    primary key (movie_id, genre_id)
)
diststyle key distkey(movie_id);

CREATE TABLE IF NOT EXISTS movies.movie_genre (
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    primary key (movie_id, genre_id)
)
diststyle key distkey(movie_id);

CREATE TABLE IF NOT EXISTS movies.stage_genre (
    genre_id INT NOT NULL,
    genre_name VARCHAR(20),
    primary key (genre_id)
)
diststyle all;

CREATE TABLE IF NOT EXISTS movies.genre (
    genre_id INT NOT NULL,
    genre_name VARCHAR(20),
    primary key (genre_id)
)
diststyle all;

CREATE TABLE IF NOT EXISTS movies.stage_date (
    release_date DATE NOT NULL SORTKEY,
    day INT,
    week INT,
    month INT,
    quarter INT,
    year INT,
    primary key (release_date)
)
diststyle all;

CREATE TABLE IF NOT EXISTS movies.date (
    release_date DATE NOT NULL SORTKEY,
    day INT,
    week INT,
    month INT,
    quarter INT,
    year INT,
    primary key (release_date)
)
diststyle all;