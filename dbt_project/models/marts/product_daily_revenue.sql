{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'date'],
        incremental_strategy='merge'
    )
}}

with cart_items as (
    select * from {{ ref('stg_cart_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

daily_revenue as (
    select
        product_id,
        date,
        sum(quantity) as units_sold,
        sum(discounted_total) as revenue
    from cart_items
    group by product_id, date
    having sum(quantity) > 0
)

select
    daily_revenue.product_id,
    products.product_title,
    daily_revenue.date,
    daily_revenue.units_sold,
    daily_revenue.revenue
from daily_revenue
left join products
    on products.product_id = daily_revenue.product_id
    and products.date = daily_revenue.date
