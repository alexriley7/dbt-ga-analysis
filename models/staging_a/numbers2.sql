{{ config(materialized='table', alias= 'numbersb', schema='_dev' )}}


select generated_number as ordinal from dbt_dalejandrorobledo__dev.numbers