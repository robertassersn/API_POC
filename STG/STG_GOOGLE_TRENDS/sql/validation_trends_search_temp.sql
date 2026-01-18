with 
	table_date_range as 
		(select 
			min(date::date) as first_entry
			,max(date::date) as last_entry
		from google_trends_temp.trends_search_temp
		)
	,date_list as
		(select 
			dd.full_date 
		from dwh_dimensions.dim_date as dd
		inner join table_date_range as tt on 1=1  
		where 
			dd.full_date >= tt.first_entry 
			and dd.full_date < tt.last_entry
		)
	,duplicate_validation as (
		select 
			google_trends_id 
			,count(1) as "n"
			,count(1)-1 as duplicate_count
		from ${SCHEMA_GOOGLE_TRENDS_TEMP}.trends_search_temp
		group by 1 
		having count(1) > 1
	) 
select 
	'DATE_VERIFICATION' as verification
	,count(1) as affected_entries
from date_list dl 
left join google_trends_temp.trends_search_temp t1 on t1.date::date = dl.full_date 
where 
	t1.date is null 
group by 1 
having 
	count(1) > 0
UNION ALL 
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

;