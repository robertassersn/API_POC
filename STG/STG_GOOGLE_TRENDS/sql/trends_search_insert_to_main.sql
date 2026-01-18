delete from ${SCHEMA_GOOGLE_TRENDS}.trends_search
where 
    google_trends_id in(
        select 
            google_trends_id
        from ${SCHEMA_GOOGLE_TRENDS_TEMP}.trends_search_temp
    )
;
insert into ${SCHEMA_GOOGLE_TRENDS}.trends_search(
    google_trends_id
	,date 
	,timestamp
	,source_file_name
	,job_run_id
)
select 
    google_trends_id
	,"date"
	,timestamp 
	,source_file_name
	,${JOB_RUN_ID} as job_run_id
from ${SCHEMA_GOOGLE_TRENDS_TEMP}.trends_search_temp
;
commit