
select 
    transactions_int
    ,CAST(rownum as int) as session_id
    ,browser
    ,fullvisitorId as user_key
    ,CAST(visitstarttime as decimal) as created_at_utc

from dbt_dalejandrorobledo.start_time_cast


