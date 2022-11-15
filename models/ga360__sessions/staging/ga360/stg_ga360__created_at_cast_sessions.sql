{{ config(materialized='view' , schema='dev_stg') }}


with source_data as (

    select * from {{ ref('stg_ga360__casted_sessions') }}

)

, source_data_final AS (

SELECT  
            Id
            ,fullvisitorId
            ,visitId
            ,pageviews
            ,transactions
            ,transactionrevenue
            ,hits

            ,timestamp 'epoch' + visitstarttime* interval '1 second' AS created_at_utc_cast 
--from cte_b

from source_data

) SELECT * FROM source_data_final