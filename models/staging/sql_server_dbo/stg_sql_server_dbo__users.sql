{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT 
        *
    FROM {{ source('sql_server_dbo','users')}}
)

SELECT * FROM src_users