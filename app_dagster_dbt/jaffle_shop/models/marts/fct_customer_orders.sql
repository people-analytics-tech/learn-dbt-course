with
    -- Import CTEs
    customers as (select * from {{ ref("stg_jaffle_shop__customers") }}),
    paid_orders as (select * from {{ ref("int_orders") }}),

    -- Final CTE
    final as (
        select
            paid_orders.order_id,
            paid_orders.customer_id,
            paid_orders.order_placed_at,
            paid_orders.order_status,
            paid_orders.total_amount_paid,
            paid_orders.payment_finalized_date,
            customers.customer_first_name,
            customers.customer_last_name,
            -- sales transaction sequence
            row_number() over (
                order by paid_orders.order_id, paid_orders.order_placed_at
            ) as transaction_sequence,
            -- customer sales sequence
            row_number() over (
                partition by paid_orders.customer_id
                order by paid_orders.order_id, paid_orders.order_placed_at
            ) as customer_sales_sequence,
            -- new customer flag
            case
                when
                    (
                        rank() over (
                            partition by paid_orders.customer_id
                            order by paid_orders.order_id, paid_orders.order_placed_at
                        )
                        = 1
                    )
                then 'new'
                else 'return'
            end as new_or_return_customer,
            -- customer lifetime value
            sum(paid_orders.total_amount_paid) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_id, paid_orders.order_placed_at
            ) as customer_lifetime_value,
            -- first order date
            first_value(paid_orders.order_placed_at) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_id, paid_orders.order_placed_at
            ) as first_order_date
        from paid_orders
        left join customers on paid_orders.customer_id = customers.customer_id

    )

-- Simple Select Statment
select *
from final
order by order_id desc
