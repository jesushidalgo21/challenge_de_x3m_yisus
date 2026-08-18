with raw as (
    select
        load_date,
        jsonb_array_elements(data -> 'products') as product
    from {{ source('raw', 'raw_products') }}
)

select
    load_date as date,
    (product ->> 'id')::int as product_id,
    product ->> 'title' as product_title,
    (product ->> 'price')::numeric as price
from raw
