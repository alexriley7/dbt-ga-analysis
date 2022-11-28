 {{ 
    config(
            materialized='incremental'
            ,unique_key='visitid'
            ,incremental_strategy='delete+insert'
            
            ) 
            
}}


WITH 
{% if is_incremental() %}

incremental_users_source as (

    select
        distinct fullvisitorId

    from {{ ref('int_ga360__sessions') }}

    where created_at_utc_cast > (select max(created_at_utc_cast) from {{ this }})

),
{% endif %}


sessions_source as (

    select
        *

    from {{ ref('int_ga360__sessions') }} as v_sessions_w


   {% if is_incremental() %}
    where exists (
        select users.fullvisitorId
        from incremental_users_source as users
        where users.fullvisitorId =  v_sessions_w.fullvisitorId	 
    )
    {% endif %}

),

    cte AS (

        select
        v_sessions_w.visitId
        ,v_sessions_w.fullvisitorId
        ,v_sessions_w.created_at_utc_cast

        ,pageviews AS count_pageviews
        ,SUM(pageviews) OVER (PARTITION BY visitId ORDER BY created_at_utc_cast rows between unbounded preceding and current row) AS cumulated_pageviews

        ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.fullvisitorId ORDER BY v_sessions_w.created_at_utc_cast) AS user_session_sequence
        ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.fullvisitorId ORDER BY v_sessions_w.created_at_utc_cast) = 1 AS is_user_first_session
        ,LAG(v_sessions_w.visitId) OVER (PARTITION BY v_sessions_w.fullvisitorId ORDER BY v_sessions_w.created_at_utc_cast) AS user_previous_session_id
        ,LEAD(v_sessions_w.visitId) OVER (PARTITION BY v_sessions_w.fullvisitorId ORDER BY v_sessions_w.created_at_utc_cast ) AS user_next_order_id

        from sessions_source as v_sessions_w

    )

    SELECT * FROM cte

    ORDER BY cte.created_at_utc_cast DESC