{{
  config(
    materialized='view'
  )
}}

WITH src_products AS (
    SELECT 
        MD5(PRODUCT_ID) AS PRODUCT_ID,
        COALESCE(PRICE,0) AS PRICE_DOLLAR,
        NAME AS PRODUCT_NAME,
        INVENTORY AS INVENTORY_QUANTITY,
        _FIVETRAN_DELETED AS _FIVETRAN_DELETED,
        CONVERT_TIMEZONE('UTC',_FIVETRAN_SYNCED) AS DATA_UPDATED_UTC
    FROM {{ source('sql_server_dbo','products')}}
    WHERE _FIVETRAN_DELETED IS NULL
)

SELECT * FROM src_products