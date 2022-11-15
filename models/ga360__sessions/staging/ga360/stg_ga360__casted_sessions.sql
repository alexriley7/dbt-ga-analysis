
{{ config(materialized='view' , schema='dev_stg') }}


with source_data as (

    select * from {{ ref('stg_ga360__sessions') }}

)

,source_data_filtered AS (
-- In this step maybe we can setup a test

 SELECT 
    
           Id
           ,visitStartTime
           ,visitId
           ,fullvisitorId
           ,hits
           ,pageviews
          ,transactions
          ,transactionrevenue

         FROM source_data

           WHERE 
           
           Id <> 'Id'
           AND visitStartTime <> 'visitStartTime'
           AND visitId <> 'visitId'
           AND hits <> 'hits'
           AND pageviews <> 'pageviews'
           AND fullvisitorId <> 'fullvisitorId'
           AND transactions <> 'transactions'
           AND transactionrevenue <> 'transactionrevenue'
)

,source_data_null_removed AS (

SELECT 

        Id
        ,CASE WHEN pageviews = ' ' THEN '0' ELSE pageviews END AS pageviews
        ,CASE WHEN fullvisitorId = ' ' THEN '0' ELSE fullvisitorId END AS fullvisitorId
        ,CASE WHEN transactions = ' ' THEN '0' ELSE transactions END AS transactions
        ,CASE WHEN transactionrevenue = ' ' THEN '0' ELSE transactionrevenue END AS transactionrevenue
        ,CASE WHEN hits = ' ' THEN '0' ELSE hits END AS hits
        ,CASE WHEN visitId = ' ' THEN '0' ELSE visitId END AS visitId
        ,CASE WHEN visitStartTime = ' ' THEN '0' ELSE visitStartTime END AS visitStartTime

FROM source_data_filtered

) 

, source_data_casted AS (

SELECT 

    Id
    ,fullvisitorId
    ,visitId
    ,CAST(pageviews AS decimal) AS pageviews
    ,CAST(transactions AS decimal) AS transactions
    ,CAST(transactionrevenue AS decimal) AS transactionrevenue
     ,CAST(hits AS decimal) AS hits
     ,CAST(visitStartTime AS int) AS visitStartTime

FROM source_data_null_removed

--ORDER BY transactionrevenue_c DESC

)

, source_data_final AS (

    SELECT * FROM source_data_casted
)
        SELECT * FROM source_data_final

   -- order by transactionrevenue desc





--, source_data_casted as ( 

  --  select 

    --    CAST(visitStartTime AS decimal) AS visitStartTime_int
      --      ,CAST(Id AS INT) AS id_int 
        --    ,CAST(visitId AS INT) AS visitId_int 
          -- ,CAST(fullvisitorId AS INT) AS fullvisitorid_int 
          --  ,CAST(hits AS INT) AS hits 
           -- ,pageviews
          --  ,CAST(transactions AS INT) AS transactions 
          --  ,CAST(transactionrevenue AS decimal) AS transactionrevenue 
    
    
    
     --from source_data_filtered

     --ORDER BY id_int DESC

--) select *  from source_data_casted