{{ config(materialized='table') }}
SELECT 
   *
FROM {{ ref('bankdistrict') }}