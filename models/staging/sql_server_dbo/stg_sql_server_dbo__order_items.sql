{{
  config(
    materialized='view'
  )
}}

WITH src_order_items AS (
    SELECT 
        MD5(CONCAT(ORDER_ID,PRODUCT_ID)) AS ORDER_ITEMS_ID,
        MD5(ORDER_ID) AS ORDER_ID,
        MD5(PRODUCT_ID) AS PRODUCT_ID,
        QUANTITY,
        _FIVETRAN_DELETED,
        CONVERT_TIMEZONE('UTC',_FIVETRAN_SYNCED) AS DATA_UPDATED_UTC
    FROM {{ source('sql_server_dbo','order_items')}}
)

SELECT * FROM src_order_items