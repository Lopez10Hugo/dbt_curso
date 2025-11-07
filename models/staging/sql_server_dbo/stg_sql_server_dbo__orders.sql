{{
  config(
    materialized='incremental',
    on_schema_change = 'fail',
    unique_key = 'ORDER_ID'
  )
}}

WITH src_orders AS (
    SELECT *
    FROM {{ source('sql_server_dbo','orders')}}
    {% if is_incremental() %}
    WHERE _fivetran_synced > (SELECT MAX(DATA_UPDATED_UTC) FROM {{ this }})
    {% endif %}
)
, orders_transform AS (
    SELECT 
        MD5(ORDER_ID) AS ORDER_ID,
        MD5(SHIPPING_SERVICE) AS SHIPPING_SERVICE_ID,
        COALESCE(SHIPPING_COST,0::FLOAT) AS SHIPPING_COST_DOLLAR,
        MD5(ADDRESS_ID) AS ADDRESS_ID,
        CONVERT_TIMEZONE('UTC',CREATED_AT) AS CREATED_AT_UTC,
        CASE
            WHEN PROMO_ID = ''
                THEN MD5('no promo')
            ELSE MD5(PROMO_ID)
        END AS PROMO_ID,
        CONVERT_TIMEZONE('UTC',ESTIMATED_DELIVERY_AT) AS ESTIMATED_DELIVERY_AT_UTC,
        COALESCE(ORDER_COST,0::FLOAT) AS ORDER_COST_DOLLAR,
        MD5(USER_ID) AS USER_ID,
        COALESCE(ORDER_TOTAL,0::FLOAT) AS ORDER_TOTAL_DOLLAR,
        CONVERT_TIMEZONE('UTC', DELIVERED_AT) AS DELIVERED_AT_UTC,
        TRACKING_ID,
        STATUS,
        _FIVETRAN_DELETED,
        CONVERT_TIMEZONE('UTC',_FIVETRAN_SYNCED) AS DATA_UPDATED_UTC
    FROM src_orders
    WHERE _FIVETRAN_DELETED IS NULL
)

SELECT * FROM orders_transform