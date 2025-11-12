{{
  config(
    materialized='view'
  )
}}

WITH src_addresses AS (
    SELECT 
        *
    FROM {{ source('sql_server_dbo','addresses')}}
),
transform_state AS (
    SELECT DISTINCT
        md5(STATE) as STATE_ID,
        STATE AS STATE_NAME,
        MD5(COUNTRY) AS COUNTRY_ID,
    FROM src_addresses
)

SELECT * FROM transform_state