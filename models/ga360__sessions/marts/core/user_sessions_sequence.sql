
{{ config(materialized='view' , schema='dev') }}


with source_data as (

    select * from {{ ref('int_ga360__sessions') }}

) 

, sessions_sequence AS (SELECT
    v_sessions_w.visitId AS session_id
    ,v_sessions_w.fullvisitorId AS user_key
    ,v_sessions_w.created_at_utc_cast AS session_timestamp
    --,transactions_int
    ,v_sessions_w.pageviews AS sum_pageviews
    --,SUM(pageviews_int) OVER() AS sum_pageviews
    ,SUM(sum_pageviews) OVER (PARTITION BY user_key ORDER BY session_timestamp rows between unbounded preceding and current row) AS cumulated_pageviews

    

    ,ROW_NUMBER() OVER (PARTITION BY user_key ORDER BY session_timestamp) AS client_session_sequence
    ,ROW_NUMBER() OVER (PARTITION BY user_key ORDER BY session_timestamp) = 1 AS is_client_first_session
    ,LAG(session_id) OVER (PARTITION BY user_key ORDER BY session_timestamp) AS client_previous_session_id
    ,LEAD(session_id) OVER (PARTITION BY user_key ORDER BY session_timestamp) AS client_next_order_id
    
    
    --,transactionrevenue
    --,SUM(dbt_dalejandrorobledo.ga360_20170707_raw_b.pageviews)

    FROM source_data as v_sessions_w
    --WHERE transactions_int > 0
    --JOIN dbt_dalejandrorobledo.ga360_20170707_raw_b ON dbt_dalejandrorobledo.ga360_20170707_raw_b.rownum = v_sessions_w.rownum_int
    --JOIN dbt_dalejandrorobledo.ga360_20170707_raw_b ON dbt_dalejandrorobledo.ga360_20170707_raw_b.visitId = v_sessions_w.session_id
    --check with pageviews or eccomerce action type
    --GROUP BY 1,2,3,4
    --eCommerceAction.action_type !!!

    

) SELECT * FROM sessions_sequence

        ORDER BY session_timestamp DESC