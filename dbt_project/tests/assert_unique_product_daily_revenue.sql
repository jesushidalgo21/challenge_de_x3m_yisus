-- Safety net for the incremental merge: (product_id, date) must be unique.
select
    product_id,
    date,
    count(*) as row_count
from {{ ref('product_daily_revenue') }}
group by product_id, date
having count(*) > 1
