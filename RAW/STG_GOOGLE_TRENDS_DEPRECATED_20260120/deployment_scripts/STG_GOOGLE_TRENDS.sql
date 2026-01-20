/*
██████  ██████  ██      
██   ██ ██   ██ ██      
██   ██ ██   ██ ██      
██   ██ ██   ██ ██      
██████  ██████  ███████ 
*/

create schema google_trends_temp;
create schema google_trends;


-- Create schema
CREATE SCHEMA IF NOT EXISTS dwh_dimensions;

-- Create dim_date table
CREATE TABLE IF NOT EXISTS dwh_dimensions.dim_date (
    date_key INTEGER PRIMARY KEY,           -- YYYYMMDD format
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,           -- 1=Monday, 7=Sunday
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_leap_year BOOLEAN NOT NULL,
    year_month VARCHAR(7) NOT NULL,         -- YYYY-MM
    year_quarter VARCHAR(7) NOT NULL        -- YYYY-Q1
)
;

-- Populate with dates from 2010 to 2040
INSERT INTO dwh_dimensions.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(WEEK FROM d)::INTEGER AS week_of_year,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(ISODOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend,
    (EXTRACT(YEAR FROM d)::INTEGER % 4 = 0 
        AND (EXTRACT(YEAR FROM d)::INTEGER % 100 != 0 
            OR EXTRACT(YEAR FROM d)::INTEGER % 400 = 0)) AS is_leap_year,
    TO_CHAR(d, 'YYYY-MM') AS year_month,
    TO_CHAR(d, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM d) AS year_quarter
FROM generate_series('2010-01-01'::DATE, '2040-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING
;
/*
GRANT USAGE ON SCHEMA X to .......
+ table/view creation,
*/
create table google_trends_temp.raw_trends_search_temp
	(
	raw_json_entry varchar(max)
	)
;

create table google_trends_temp.raw_trends_search__values_temp(
	 raw_json_entry varchar(max)
	)
;



create table google_trends_temp.trends_search_temp
	(
	google_trends_id varchar(30)
	,date varchar(30)
	,timestamp bigint
	,source_file_name varchar(100)
	)
;

create table google_trends_temp.trends_search__values_temp(
	 query varchar(300)
	 ,value varchar(300)
	 ,extracted_value int
	 ,google_trends_id varchar(30)
	 ,google_trends__values_id varchar(30)
	 ,source_file_name varchar(100)
	)
;

create table google_trends.trends_search
	(
	google_trends_id varchar(30)
	,date varchar(30)
	,timestamp bigint
	,source_file_name varchar(100)
	,job_run_id int
	)
;

create table google_trends.trends_search__values(
	 query varchar(300)
	 ,value varchar(300)
	 ,extracted_value int
	 ,google_trends_id varchar(30)
	 ,google_trends__values_id varchar(30)
	 ,source_file_name varchar(100)
	 ,job_run_id int
	)
;
/*
GRANT SELECT ...... ON TABLE TO
*/

