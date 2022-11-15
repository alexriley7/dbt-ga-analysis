{{ config(materialized='view' , schema='dev') }}

with source_data as (

    select * from {{ source('dbt_dalejandrorobledo', 'ga360_20170707__raw') }}

)

select *
from source_data