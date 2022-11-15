WITH cte as (SELECT
    v_sessions_w.session_id
    ,v_sessions_w.user_key
    ,v_sessions_w.created_at_utc_cast
    --,transactions_int
    ,pageviews_int AS sum_pageviews
    --,SUM(pageviews_int) OVER() AS sum_pageviews
    ,SUM(pageviews_int) OVER (PARTITION BY user_key ORDER BY created_at_utc rows between unbounded preceding and current row) AS cumulated_pageviews

    

    ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_session_sequence
    ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) = 1 AS is_client_first_session
    ,LAG(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_previous_session_id
    ,LEAD(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc ) AS client_next_order_id
    
    
    --,transactionrevenue
    --,SUM(dbt_dalejandrorobledo.ga360_20170707_raw_b.pageviews)

    FROM dbt_dalejandrorobledo.created_at_utc_cast as v_sessions_w
    --WHERE transactions_int > 0
    --JOIN dbt_dalejandrorobledo.ga360_20170707_raw_b ON dbt_dalejandrorobledo.ga360_20170707_raw_b.rownum = v_sessions_w.rownum_int
    --JOIN dbt_dalejandrorobledo.ga360_20170707_raw_b ON dbt_dalejandrorobledo.ga360_20170707_raw_b.visitId = v_sessions_w.session_id
    --check with pageviews or eccomerce action type
    --GROUP BY 1,2,3,4
    --eCommerceAction.action_type !!!

    

) SELECT * FROM cte



    

