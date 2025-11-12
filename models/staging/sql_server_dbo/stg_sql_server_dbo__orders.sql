{{
  config(
    materialized='table'
  )
}}

WITH orders_transform AS (
    SELECT 
        ORDER_ID,
        SHIPPING_SERVICE_ID,
        ADDRESS_ID,
        CREATED_AT_UTC,
        PROMO_ID,
        ESTIMATED_DELIVERY_AT_UTC,
        ORDER_COST_DOLLAR,
        USER_ID,
        ORDER_TOTAL_DOLLAR,
        DELIVERED_AT_UTC,
        TRACKING_ID,
        STATUS,
        _FIVETRAN_DELETED,
        DATA_UPDATED_UTC
    FROM {{ ref('base_sql_server_dbo__orders') }}
    WHERE _FIVETRAN_DELETED IS NULL
)

SELECT * FROM orders_transform