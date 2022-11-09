{{ config(materialized='table') }}

WITH cte as (select visitstarttime ,rownum_int



from dbt_dalejandrorobledo.ga_360_raw_b_clean_final

where visitstarttime not like '%visit%'

) 
,cte_b as (select CAST(visitstarttime as int) as time_cast ,rownum_int from cte

) ,cte_c as (

select timestamp 'epoch' + time_cast * interval '1 second' AS created_at_utc_cast ,rownum_int
from cte_b

) select 

dbt_dalejandrorobledo.ga_360_raw_b_clean_final.rownum_int
,dbt_dalejandrorobledo.ga_360_raw_b_clean_final.transactions_int
,dbt_dalejandrorobledo.ga_360_raw_b_clean_final.session_id
,dbt_dalejandrorobledo.ga_360_raw_b_clean_final.user_key
,dbt_dalejandrorobledo.ga_360_raw_b_clean_final.created_at_utc
,dbt_dalejandrorobledo.ga_360_raw_b_clean_final.pageviews_int
,cte_c.created_at_utc_cast

from dbt_dalejandrorobledo.ga_360_raw_b_clean_final

join cte_c on dbt_dalejandrorobledo.ga_360_raw_b_clean_final.rownum_int = cte_c.rownum_int

order by created_at_utc_cast desc


-- do all this cast in the main_clean final

--select * from dbt_dalejandrorobledo.ga_360_raw_b_clean_final