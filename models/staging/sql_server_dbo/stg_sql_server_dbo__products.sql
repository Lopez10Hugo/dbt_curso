{{
  config(
    materialized='view'
  )
}}

WITH src_products AS (
    SELECT DISTINCT
        MD5(PRODUCT_ID) AS PRODUCT_ID,
        COALESCE(PRICE,0) AS PRICE_DOLLAR,
        NAME AS PRODUCT_NAME
    FROM {{ source('sql_server_dbo','products')}}
    WHERE _FIVETRAN_DELETED IS NULL
)

SELECT * FROM src_products