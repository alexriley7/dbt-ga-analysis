WITH test_source AS (

    SELECT * from {{ source('dbt_dalejandrorobledo', 'summary_ga_2017_july') }}
)

SELECT * FROM test_source