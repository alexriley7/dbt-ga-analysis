select 

session_id,

    CASE WHEN 
           created_at_utc like '%1%' or
           created_at_utc like '%2%' or
           created_at_utc like '%3%' or
           created_at_utc like '%4%' or
           created_at_utc like '%5%' or
           created_at_utc like '%6%' or
           created_at_utc like '%7%' or
           created_at_utc like '%8%' or
           created_at_utc like '%9%' or
           created_at_utc like '%0%'   THEN CAST(created_at_utc as int)

          WHEN 
           created_at_utc not like '%1%' or
           created_at_utc not like '%2%' or
           created_at_utc not like '%3%' or
           created_at_utc not like '%4%' or
           created_at_utc not like '%5%' or
           created_at_utc not like '%6%' or
           created_at_utc not like '%7%' or
           created_at_utc not like '%8%' or
           created_at_utc not like '%9%' or
           created_at_utc not like '%0%' THEN 0

              END AS created_at_utc_int

from dbt_dalejandrorobledo.v_sessions_b
