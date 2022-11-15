{{ config(materialized='table') }}

select 

CAST(session_id as int) as session_id
,transactions_int
,browser
,user_key
,casteddate AS created_at
,wep AS created_at_utc


from dbt_dalejandrorobledo.to_cast_date

--ORDER BY session_id DESC