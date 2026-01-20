with 
	duplicate_validation as (
		select 
			google_trends__values_id 
			,count(1) as "n"
			,count(1)-1 as duplicate_count
		from ${SCHEMA_GOOGLE_TRENDS_TEMP}.trends_search__values_temp
		group by 1 
		having count(1) > 1
	) 
	,expected_number_of_queries_per_day as (
		select 
			count(distinct query) as expected_number_of_queries
		from ${SCHEMA_GOOGLE_TRENDS_TEMP}.trends_search__values_temp 
	)
	,expected_entries_qa as (
		/*
		 its reasonable that number of entries for such validation may change over time
		 for such validation to work, would need to make it incremental 
		 and e.g pass timestamp from which to do specific QA 
		 
		 */
		select 
			t.google_trends_id  
			,tt.expected_number_of_queries
			,count(1) as affected_entries
		from ${SCHEMA_GOOGLE_TRENDS_TEMP}.trends_search__values_temp t 
		inner join expected_number_of_queries_per_day tt on 1=1 
		group by 1,2
		having count(1) <> tt.expected_number_of_queries
	)
select 
	'ACCEPTED_VALUES_VALIDATION' as verification
	,count(1) as affected_entries
from ${SCHEMA_GOOGLE_TRENDS_TEMP}.trends_search__values_temp t 
where 
    /*
    THIS NEEDS TO BE PASSED AS A VARIABLE FROM CONFIGURATION
    meaning t.query values
    */
	t.query not in( 
		'vpn'
		,'antivirus'
		,'ad blocker'
		,'password manager'
	)
group by 1 
having 
	count(1) > 0
union all 
select 
	'DUPLICATES_FOUND' as verification 
	,coalesce(
		sum(duplicate_count)
		,0
	) as affected_entries
from duplicate_validation t
group by 1
having 
	coalesce(
		sum(duplicate_count)
		,0
	) > 0
UNION ALL
select 
	'EXPECTED_NUMBER_OF_ENTRIES_PER_DAY' as verification 
	,coalesce(
		sum(affected_entries)
		,0
	) as affected_entries
from expected_entries_qa t
group by 1
having 
	coalesce(
		sum(affected_entries)
		,0
	) > 0 
UNION ALL
select 
	'EXPECTED_NUMBER_OF_ENTRIES_PER_DAY' as verification 
	,coalesce(
		sum(affected_entries)
		,0
	) as affected_entries
from expected_entries_qa t
group by 1
having 
	coalesce(
		sum(affected_entries)
		,0
	) > 0 
UNION ALL 
select 
	'NULL VALUES' as verification
	,count(1) as affected_entries
from ${SCHEMA_GOOGLE_TRENDS_TEMP}.trends_search__values_temp
where 
	query is null 
	or value is null 
	or extracted_value is null
	or google_trends_id is null 
	or google_trends__values_id is null 
	or source_file_name is null
group by 1 
having count(1) > 1 
;
