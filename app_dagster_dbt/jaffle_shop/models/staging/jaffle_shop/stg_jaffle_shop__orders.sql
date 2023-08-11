with
    -- Import CTEs
    orders as (select * from {{ source("data_warehouse_test", "jaffle_shop_orders") }}),

    transformed as (
        select

            cast(id as int64) as order_id,
            cast(user_id as int64) as customer_id,
            order_date as order_placed_at,
            status as order_status,

            case
                when status not in ('returned', 'return_pending') then order_date
            end as valid_order_date

        from orders
    )

select *
from transformed
