
{{ config(materialized='view' , schema='dev') }}


with source_data as (

    select * from {{ ref('stg_ga360__created_at_cast_sessions') }}

) SELECT 

    fullvisitorid
    ,visitid
    ,pageviews
    ,transactions
    ,transactionrevenue
    ,hits
    ,created_at_utc_cast


        FROM source_data

        ORDER BY created_at_utc_cast DESC