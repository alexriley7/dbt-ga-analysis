WITH cte as (SELECT
    v_sessions_w.session_id
    ,v_sessions_w.user_key
    ,v_sessions_w.created_at_utc

    --,transactions_int

    ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_session_sequence
    ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) = 1 AS is_client_first_session
    ,LAG(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_previous_session_id
    ,LEAD(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc ) AS client_next_order_id
    
    --,transactionrevenue

    FROM dbt_dalejandrorobledo.ga_360_raw_b_clean_final as v_sessions_w
    --WHERE transactions_int > 0
    --JOIN dbt_dalejandrorobledo.ga360_20170707_raw_b ON dbt_dalejandrorobledo.ga360_20170707_raw_b.rownum = v_sessions_w.rownum_int

    --check with pageviews or eccomerce action type

    --eCommerceAction.action_type !!!

) SELECT * FROM cte



    

