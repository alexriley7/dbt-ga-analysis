{{ config(materialized='table', alias= 'second_model', schema='ga_analytics_test_ofi' )}}

WITH test_source_a AS (

    SELECT * from {{ source('dbt_dalejandrorobledo', 'summary_ga360_20170601') }}
)

SELECT * FROM test_source_a