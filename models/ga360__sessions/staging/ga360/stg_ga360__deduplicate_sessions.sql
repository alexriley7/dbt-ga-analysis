{{ config(materialized='view' , schema='dev_stg') }}


with source_data as (

    select * from {{ ref('stg_ga360__casted_sessions') }}

)

,source_data_deduplicated AS (

    SELECT * FROM source_data


) 

,source_data_inserted AS (


SELECT 

    Id
    ,fullvisitorId
    ,visitId
    ,pageviews
    ,transactions
    ,transactionrevenue
    ,hits
    ,current_timestamp AS insertion_timestamp

FROM source_data_deduplicated

)

,unique_source AS (

SELECT * ,
                    ROW_NUMBER() OVER (PARTITION BY Id) AS ROW_NUMBER
                    FROM source_data_inserted

) SELECT * FROM unique_source

    WHERE row_number = 1

