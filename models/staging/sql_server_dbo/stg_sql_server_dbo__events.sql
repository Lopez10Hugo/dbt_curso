{{
  config(
    materialized='view'
  )
}}

WITH src_events AS (
    SELECT 
        *
    FROM {{ source('sql_server_dbo','events')}}
)

SELECT * FROM src_events