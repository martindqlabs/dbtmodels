with bankdistrict as (
    select * from {{ source('dqlabs', 'bankdistrict') }}
)

select * from bankdistrict
