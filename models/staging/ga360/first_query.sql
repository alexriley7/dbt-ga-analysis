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

    FROM cte JOIN dbt_dalejandrorobledo.ga360_20170701_raw ON cte.rownum = dbt_dalejandrorobledo.ga360_20170701_raw.rownum

    
-- create "session with orders" as s key