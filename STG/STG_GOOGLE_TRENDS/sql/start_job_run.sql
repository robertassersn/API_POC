insert into ${SCHEMA_JOB_INFO}.job_runs(
    job_run_id
	,job_name 
	,datasource 
	,start_timestamp 
	,end_timestamp 
	,job_status 
) values (
    '${JOB_RUN_ID}'
    ,'STG_GOOGLE_TRENDS'
    ,'GOOGLE'
    ,now()
    ,null
    ,'${JOB_STATUS_STARTED}'
)
;
commit