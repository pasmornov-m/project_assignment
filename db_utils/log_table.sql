-- Active: 1750861427417@@127.0.0.1@5432@project
CREATE TABLE IF NOT EXISTS logs.etl_log (
    id SERIAL PRIMARY KEY,
    operation_name TEXT,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_sec INT
);
