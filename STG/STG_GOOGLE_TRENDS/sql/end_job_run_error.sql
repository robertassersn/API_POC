update ${SCHEMA_JOB_INFO}.job_runs
set 
    job_status = '${JOB_STATUS_ERROR}'
    ,end_timestamp = now()
where 
    job_run_id = ${JOB_RUN_ID}
    and job_name = 'STG_GOOGLE_TRENDS'
    and datasource = 'GOOGLE'
;
commit