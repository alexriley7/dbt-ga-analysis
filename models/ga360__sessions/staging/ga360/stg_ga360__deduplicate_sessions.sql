{{ config(materialized='view' , schema='dev_stg') }}


with source_data as (

    select * from {{ ref('stg_ga360__casted_sessions') }}

)

,source_data_deduplicated AS (

    SELECT * FROM source_data


) SELECT * FROM source_data_deduplicated