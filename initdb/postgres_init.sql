-- Tạo database
CREATE DATABASE movies_db;
CREATE DATABASE airflow;

-- Tạo table
CREATE TABLE IF NOT EXISTS movies (
    id INTEGER PRIMARY KEY,
    adult BOOLEAN,
    backdrop_path TEXT,
    title TEXT NOT NULL,
    original_language VARCHAR(10),
    original_title TEXT,
    overview TEXT,
    poster_path TEXT,
    media_type VARCHAR(20),
    genre_ids INTEGER[],
    popularity FLOAT,
    release_date DATE,
    video BOOLEAN,
    vote_average FLOAT,
    vote_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

