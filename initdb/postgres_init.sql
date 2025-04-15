-- Tạo database
CREATE DATABASE airflow;
-- Tạo table
-- Main movies table
CREATE TABLE movies (
    id INTEGER PRIMARY KEY,
    adult BOOLEAN,
    backdrop_path TEXT,
    title TEXT NOT NULL,
    original_language VARCHAR(10),
    original_title TEXT,
    overview TEXT,
    poster_path TEXT,
    media_type TEXT,
    popularity FLOAT,
    release_date DATE,
    video BOOLEAN,
    vote_average FLOAT,
    vote_count INTEGER
);

-- Genre mapping table (for array of genre_ids)
CREATE TABLE movie_genres (
    movie_id INTEGER REFERENCES movies(id),
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id)
);

-- Other result types (empty in your schema but available for future use)
CREATE TABLE person_results (
    id SERIAL PRIMARY KEY,
    -- Add fields when structure is defined
    movie_id INTEGER REFERENCES movies(id)
);

CREATE TABLE tv_results (
    id SERIAL PRIMARY KEY,
    -- Add fields when structure is defined
    movie_id INTEGER REFERENCES movies(id)
);