{{
  config(
    materialized='view'
  )
}}

WITH inventory_transform AS (
    SELECT 
        CONCAT(CONVERT_TIMEZONE('UTC',_FIVETRAN_SYNCED),Product_id) AS INVENTORY_ID
        MD5(PRODUCT_ID) AS PRODUCT_ID,
        INVENTORY AS INVENTORY_QUANTITY,
        _FIVETRAN_DELETED,
        DAY(CONVERT_TIMEZONE('UTC',_FIVETRAN_SYNCED)) AS DAY_UPDATED,
        CONVERT_TIMEZONE('UTC',_FIVETRAN_SYNCED) AS DATA_UPDATED_UTC
    FROM {{ source('sql_server_dbo','products')}}
    WHERE _FIVETRAN_DELETED IS NULL
)

SELECT * FROM inventory_transform