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
        customer_id,
        count(*) as total_delivery_count,
        count(case when delivery_status = 'delivered' then 1 end) as successful_delivery_count,
        count(case when delivery_status = 'cancelled' then 1 end) as failed_delivery_count,
        max(delivered_at) as last_delivery_date
    from joined
    group by 1
)

select
    customer_id,
    total_delivery_count,
    successful_delivery_count,
    failed_delivery_count,
    last_delivery_date
from aggregated
order by customer_id