{{ config(materialized='table') }}


select 

transactions_int
,rownum AS session_id
,browser
,fullvisitorId AS user_key
,Cast( date as date ) as castedDate
,created_at_utc as wep

from dbt_dalejandrorobledo.first_query_b

--WHERE created_at between 2017-08-01 and 2017-08-03

--ORDER BY session_id ASC

