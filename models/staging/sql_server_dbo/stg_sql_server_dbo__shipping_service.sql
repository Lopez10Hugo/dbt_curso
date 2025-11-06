{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT 
        *
    FROM {{ source('sql_server_dbo','orders')}}
),
transform_shipping_service AS (
    SELECT DISTINCT
        md5(SHIPPING_SERVICE) as SHIPPING_SERVICE_ID,
        SHIPPING_SERVICE AS SHIPPING_SERVICE_NAME
    FROM src_orders
    UNION ALL
        SELECT 
            MD5('unkown') AS SHIPPING_SERVICE_ID,
            'unkown' AS SHIPPING_SERVICE_NAME
)

SELECT * FROM transform_shipping_service