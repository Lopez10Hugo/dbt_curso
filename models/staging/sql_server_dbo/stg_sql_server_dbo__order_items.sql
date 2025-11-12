{{
  config(
    materialized='table'
  )
}}

WITH quantity_product AS(
    SELECT 
        order_id,
        sum(quantity) AS total_quantity
    FROM {{ ref('base_sql_server_dbo__order_items')}}
    GROUP BY order_id
),
shipping_line_calculated AS(
    SELECT 
        o.order_id,
        SHIPPING_COST_DOLLAR/total_quantity AS shipping_line_cost
    FROM {{ ref('base_sql_server_dbo__orders')}} o
    INNER JOIN quantity_product qp
    ON qp.order_id = o.order_id
),
src_order_items AS (
    SELECT 
        ORDER_ITEMS_ID,
        oi.ORDER_ID,
        PRODUCT_ID,
        QUANTITY,
        (shipping_line_cost*quantity)::FLOAT AS SHIPPING_COST_LINE_DOLLAR,
        _FIVETRAN_DELETED,
        DATA_UPDATED_UTC
    FROM {{ ref('base_sql_server_dbo__order_items')}} oi
    INNER JOIN shipping_line_calculated sl
    ON sl.order_id = oi.order_id
)

SELECT * FROM src_order_items