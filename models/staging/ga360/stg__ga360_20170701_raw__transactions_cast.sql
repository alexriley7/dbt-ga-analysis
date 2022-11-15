{{ config(materialized='table') }}

WITH stg__ga360_20170701_raw__transactions_cast AS (SELECT 

    rownum,

    CASE WHEN 
           transactions like '%1%' or
           transactions like '%2%' or
           transactions like '%3%' or
           transactions like '%4%' or
           transactions like '%5%' or
           transactions like '%6%' or
           transactions like '%7%' or
           transactions like '%8%' or
           transactions like '%9%' or
           transactions like '%0%'   THEN CAST(transactions as decimal)

          WHEN 
           transactions not like '%1%' or
           transactions not like '%2%' or
           transactions not like '%3%' or
           transactions not like '%4%' or
           transactions not like '%5%' or
           transactions not like '%6%' or
           transactions not like '%7%' or
           transactions not like '%8%' or
           transactions not like '%9%' or
           transactions not like '%0%' THEN 0

              END AS transactions_int

    FROM dbt_dalejandrorobledo.ga360_20170701_raw


) SELECT * FROM stg__ga360_20170701_raw__transactions_cast


