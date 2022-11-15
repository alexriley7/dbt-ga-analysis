
{{ config(materialized='view' , schema='dev') }}


with source_data as (

    select * from {{ ref('stg_ga360__created_at_cast_sessions') }}

) SELECT * FROM source_data