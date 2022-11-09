{{ 
    config(
            materialized='incremental'
            ,unique_key='session_id'
            ,incremental_strategy='delete+insert'
            , schema='dev'
            ) 
            
            }}

WITH 

{% if is_incremental() %}

incremental_users_source as (

    select
        distinct user_key

        from {{ ref('stg_ga360_20170701__sessions_b') }}

        where created_at_utc_cast > (select max(created_at_utc_cast) from {{ this }})


),
{% endif %}

sessions_source as (

    select
        *

    from {{ ref('stg_ga360_20170701__sessions_b') }} as v_sessions_w


   {% if is_incremental() %}
    where exists (
        select 1
        from incremental_users_source as users
        where users.user_key =  v_sessions_w.user_key
    )
    {% endif %}

),


    final AS (

        SELECT
    v_sessions_w.session_id
    ,v_sessions_w.user_key
    ,v_sessions_w.created_at_utc_cast
    --,transactions_int
    ,pageviews_int AS sum_pageviews
    --,SUM(pageviews_int) OVER() AS sum_pageviews
    ,SUM(pageviews_int) OVER (PARTITION BY user_key ORDER BY created_at_utc rows between unbounded preceding and current row) AS cumulated_pageviews

    --,0.9 as sum_pageviews
    --,0.8 as cumulated_pageviews

    ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_session_sequence
    ,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) = 1 AS is_client_first_session
    ,LAG(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_previous_session_id
    ,LEAD(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc ) AS client_next_order_id
    
    --,transactionrevenue
    --,SUM(dbt_dalejandrorobledo.ga360_20170707_raw_b.pageviews)

    FROM sessions_source as v_sessions_w

    )

select * from final

order by final.created_at_utc_cast desc

   -- SELECT * FROM {{ ref('stg_ga360_20170701__sessions_b') }}


   -- {% if is_incremental() %}

   -- where created_at_utc_cast > (select max(created_at_utc_cast) from{{this}})

   -- {% endif %}


--),


 --cte as (SELECT
   -- v_sessions_w.session_id
    --,v_sessions_w.user_key
    --,v_sessions_w.created_at_utc_cast
    --,transactions_int
    --,pageviews_int AS sum_pageviews
    --,SUM(pageviews_int) OVER() AS sum_pageviews
    --,SUM(pageviews_int) OVER (PARTITION BY user_key ORDER BY created_at_utc rows between unbounded preceding and current row) AS cumulated_pageviews

    

    --,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_session_sequence
    --,ROW_NUMBER() OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) = 1 AS is_client_first_session
    --,LAG(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc) AS client_previous_session_id
    --,LEAD(v_sessions_w.session_id) OVER (PARTITION BY v_sessions_w.user_key ORDER BY v_sessions_w.created_at_utc ) AS client_next_order_id
    
    
    --,transactionrevenue
    --,SUM(dbt_dalejandrorobledo.ga360_20170707_raw_b.pageviews)

    --FROM sessions_source as v_sessions_w
    --WHERE transactions_int > 0
    --JOIN dbt_dalejandrorobledo.ga360_20170707_raw_b ON dbt_dalejandrorobledo.ga360_20170707_raw_b.rownum = v_sessions_w.rownum_int
    --JOIN dbt_dalejandrorobledo.ga360_20170707_raw_b ON dbt_dalejandrorobledo.ga360_20170707_raw_b.visitId = v_sessions_w.session_id
    --check with pageviews or eccomerce action type
    --GROUP BY 1,2,3
    --eCommerceAction.action_type !!!

    

--) SELECT * FROM cte

--WHERE user_key = 982493725541910361


--ORDER BY created_at_utc_cast DESC




    

