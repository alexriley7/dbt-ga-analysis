{{ config(materialized='table', alias= 'third_model', schema='_dev' )}}

WITH test_source_a AS (

    SELECT * from {{ source('dbt_dalejandrorobledo', 'summary_ga360_20170601') }}
)

SELECT * FROM test_source_a