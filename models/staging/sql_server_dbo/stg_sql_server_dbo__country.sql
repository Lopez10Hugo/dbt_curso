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
transform_country AS (
    SELECT DISTINCT
        md5(country) as COUNTRY_ID,
        country AS COUNTRY_NAME
    FROM src_addresses
)

SELECT * FROM transform_country