{{ config(materialized='table', alias= 'numbers', schema='_dev' )}}

{{ dbt_utils.generate_series(upper_bound=1000) }}