{{ config(materialized='table') }}


WITH cte AS (

select *
from {{ ref('stg__ga360_20170701_raw__transactions_cast') }}

) select

    transactions_int
    ,cte.rownum
    ,browser
    ,fullvisitorId
    ,date
    ,CAST(visitStartTime as decimal) as created_at_utc

    --,CASE WHEN transactions_int > THEN randbetween ?

    FROM cte JOIN dbt_dalejandrorobledo.ga360_20170701_raw ON cte.rownum = dbt_dalejandrorobledo.ga360_20170701_raw.rownum

    WHERE date LIKE '%20170801%'
    AND CAST(visitStartTime as decimal) NOT LIKE '%visitstarttime%'

    --ORDER BY CAST(visitStartTime as decimal)

    

-- check sessions id and unique user id

   