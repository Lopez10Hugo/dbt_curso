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
transform_zipcode AS (
    SELECT DISTINCT
        MD5(ZIPCODE) as ZIPCODE_ID,
        ZIPCODE::NUMBER(38,0) AS ZIPCODE,
        MD5(STATE) AS STATE_ID,
    FROM src_addresses
)

SELECT * FROM transform_zipcode