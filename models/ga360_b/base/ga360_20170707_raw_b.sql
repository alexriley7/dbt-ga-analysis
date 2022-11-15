{{ config(materialized='table') }}

WITH source_data as (

SELECT * FROM dbt_dalejandrorobledo.ga360_20170707_raw_b

) select * from source_data