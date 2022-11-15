WITH cte AS (
SELECT 

    transactions_int,
    CASE WHEN 
           rownum like '%1%' or
           rownum like '%2%' or
           rownum like '%3%' or
           rownum like '%4%' or
           rownum like '%5%' or
           rownum like '%6%' or
           rownum like '%7%' or
           rownum like '%8%' or
           rownum like '%9%' or
           rownum like '%0%'   THEN CAST(rownum as decimal)

          WHEN 
           rownum not like '%1%' or
           rownum not like '%2%' or
           rownum not like '%3%' or
           rownum not like '%4%' or
           rownum not like '%5%' or
           rownum not like '%6%' or
           rownum not like '%7%' or
           rownum not like '%8%' or
           rownum not like '%9%' or
           rownum not like '%0%' THEN 0 END AS rownum_int

from dbt_dalejandrorobledo.ga_360_raw_b_clean_transaction_int

) select rownum_int
         ,transactions_int

         FROM cte
         