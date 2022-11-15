WITH cte as (SELECT 

    transactions

    ,CASE WHEN transactions = '' THEN 'x'

    ELSE 'y'
    END AS wep

    FROM dbt_dalejandrorobledo.ga360_20170701_raw )

SELECT * FROM cte

--tira un join lol