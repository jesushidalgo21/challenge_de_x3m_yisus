with raw as (
    select
        load_date,
        jsonb_array_elements(data -> 'carts') as cart
    from {{ source('raw', 'raw_carts') }}
),

cart_items as (
    select
        load_date,
        (cart ->> 'id')::int as cart_id,
        jsonb_array_elements(cart -> 'products') as item
    from raw
)

select
    load_date as date,
    cart_id,
    (item ->> 'id')::int as product_id,
    (item ->> 'quantity')::int as quantity,
    (item ->> 'price')::numeric as unit_price,
    (item ->> 'total')::numeric as total,
    (item ->> 'discountPercentage')::numeric as discount_percentage,
    (item ->> 'discountedTotal')::numeric as discounted_total
from cart_items
