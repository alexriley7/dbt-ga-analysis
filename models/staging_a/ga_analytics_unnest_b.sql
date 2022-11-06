{{ config(materialized='table', alias= 'simple_model', schema='_dev' )}}

WITH test_source_e AS (

    SELECT firstcolumn, totals
    
     from {{ source('dbt_dalejandrorobledo', 'summary_ga360_20170601') }}
)

SELECT * FROM test_source_e