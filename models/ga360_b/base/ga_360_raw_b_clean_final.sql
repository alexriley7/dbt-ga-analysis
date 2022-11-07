
SELECT 

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

           ,CASE WHEN 
           transactions like '%1%' or
           transactions like '%2%' or
           transactions like '%3%' or
           transactions like '%4%' or
           transactions like '%5%' or
           transactions like '%6%' or
           transactions like '%7%' or
           transactions like '%8%' or
           transactions like '%9%' or
           transactions like '%0%'   THEN CAST(transactions as decimal)

          WHEN 
           transactions not like '%1%' or
           transactions not like '%2%' or
           transactions not like '%3%' or
           transactions not like '%4%' or
           transactions not like '%5%' or
           transactions not like '%6%' or
           transactions not like '%7%' or
           transactions not like '%8%' or
           transactions not like '%9%' or
           transactions not like '%0%' THEN 0 END AS transactions_int

            ,visitId AS session_id
            ,fullvisitorId AS user_key
            ,CASE WHEN visitStartTime not like '%visitStartTime%' THEN CAST(visitStartTime as decimal) 
             WHEN visitStartTime like '%visitStartTime' THEN 0 END AS created_at_utc

 from dbt_dalejandrorobledo.ga360_20170707_raw_b