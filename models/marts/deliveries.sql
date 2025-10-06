with orders as (
    select
        order_id,
        customer_id
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select
        order_id,
        delivery_status,
        delivered_at
    from {{ ref('stg_ecomm__deliveries') }}
),

order_counts as (
    select
        customer_id,
        count(order_id) as total_order_count
    from orders
    group by 1
),

joined as (
    select
        orders.customer_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from deliveries
    inner join orders using (order_id)
),

aggregated as (
    select
        joined.customer_id,
        count(*) as total_delivery_count,
        count(case when delivery_status = 'delivered' then 1 end) as successful_delivery_count,
        count(case when delivery_status = 'cancelled' then 1 end) as failed_delivery_count,
        max(delivered_at) as last_delivery_date
    from joined
    group by 1
)

select
    aggregated.customer_id,
    total_order_count,
    total_delivery_count,
    successful_delivery_count,
    failed_delivery_count,
    last_delivery_date
from aggregated
inner join order_counts
    on aggregated.customer_id = order_counts.customer_id
order by aggregated.customer_id