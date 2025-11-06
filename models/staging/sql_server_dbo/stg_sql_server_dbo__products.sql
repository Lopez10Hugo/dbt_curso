{{
  config(
    materialized='view'
  )
}}

WITH src_products AS (
    SELECT 
        *
    FROM {{ source('sql_server_dbo','products')}}
)

SELECT * FROM src_products