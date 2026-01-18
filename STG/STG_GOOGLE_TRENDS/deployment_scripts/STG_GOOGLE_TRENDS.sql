/*
██████  ██████  ██      
██   ██ ██   ██ ██      
██   ██ ██   ██ ██      
██   ██ ██   ██ ██      
██████  ██████  ███████ 
*/
create schema job_info;
CREATE SEQUENCE job_info.seq_job_variables_id
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    NO CYCLE
    CACHE 1
;
CREATE SEQUENCE job_info.seq_job_run_id
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    NO CYCLE
    CACHE 1
;
    
create table job_info.job_variables(
	job_variable_id int default nextval('job_info.seq_job_variables_id')
	,job_name varchar(60) not null
	,datasource varchar(60) not null
	,variable_name varchar(60) not null
	,variable_value varchar(100)
    ,updated_at_timestamp timestamp default now()
);
create table job_info.job_runs(
	job_run_id int default nextval('job_info.seq_job_run_id')
    ,airflow_dag_id varchar(30)
    ,airflow_dag_run_id varchar(30)
	,job_name varchar(60) not null
	,datasource varchar(60) not null
	,start_timestamp timestamp not null
	,end_timestamp timestamp 
	,job_status varchar(30) not null
	,status_details varchar(300)
);

create schema google_trends_temp;
create schema google_trends;
/*
GRANT USAGE ON SCHEMA X to .......
+ table/view creation,
*/

create table google_trends_temp.trends_search_temp
	(
	google_trends_id int
	,date varchar(30)
	,timestamp bigint
	,source_file_name
	)
;

create table google_trends_temp.trends_search__values_temp(
	 query varchar(300)
	 ,value varchar(300)
	 ,extracted_value int
	 ,google_trends int
	 ,google_trends_id int
	 ,source_file_name
	)
;

create table google_trends.trends_search
	(
	google_trends_id int
	,date varchar(30)
	,timestamp bigint
	,source_file_name
	,job_run_id
	)
;

create table google_trends.trends_search__values(
	 query varchar(300)
	 ,value varchar(300)
	 ,extracted_value int
	 ,google_trends int
	 ,google_trends_id int
	 ,source_file_name
	 ,job_run_id
	)
;
/*
GRANT SELECT ...... ON TABLE TO
*/

