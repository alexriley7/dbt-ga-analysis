create table dbt_dalejandrorobledo.json_test (
    RowNum int, 
    totals varchar(1000)
    )
;

insert into dbt_dalejandrorobledo.json_test
  (RowNum, totals) 
values
  (1, '{  
     "items":[  
        {  
           "visits":"1",
           "hits":2,
           "pageviews":"2",
           "timeOnSite": "60",
           "bounces": null,
           "transactions": null,
           "transactionRevenue": null,
           "newVisits": "1",
           "screenviews": null,
           "uniqueScreenviews": null,
           "timeOnScreen": null,
           "totalTransactionRevenue": null,
           "sessionQualityDim": "1"
        }
     
     ]}')
;