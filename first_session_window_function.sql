SELECT
    v_sessions_w.session_id
    ,v_sessions_w.user_key
    ,v_sessions_w.created_at_utc

    ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_session_sequence

    ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) = 1 AS is_client_first_session
    
    ,LAG(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_previous_session_id

    ,LEAD(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc ) AS client_next_order_id
    
    FROM dbt_dalejandrorobledo.v_sessions as v_sessions_w



    