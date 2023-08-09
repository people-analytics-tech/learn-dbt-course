with
    -- Import CTEs
    orders as (
        select
            *
        from {{ source("data_warehouse_test", "jaffle_shop_orders") }}
    ),
    customers as (
        select
            *
        from {{ source("data_warehouse_test", "jaffle_shop_customers") }}
    ),
    payments as (
        select
            *
        from {{ source("data_warehouse_test", "stripe_payments") }}
    ),
    -- Logical CTEs
    succesfull_payments as (
        select
            orderid as order_id,
            max(created) as payment_finalized_date,
            sum(amount) / 100.0 as total_amount_paid
        from payments
        where status <> 'fail'
        group by 1
    ),

    paid_orders as (
        select
            orders.id as order_id,
            orders.user_id as customer_id,
            orders.order_date as order_placed_at,
            orders.status as order_status,
            succesfull_payments.total_amount_paid,
            succesfull_payments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from orders as orders
        left join
            succesfull_payments
            on orders.id = succesfull_payments.order_id
        left join
            customers
            on orders.user_id = customers.id
    ),

    total_paid_by_order as (
        select paid_orders.order_id, sum(t2.total_amount_paid) as clv_bad
        from paid_orders
        left join
            paid_orders t2
            on paid_orders.customer_id = t2.customer_id
            and paid_orders.order_id >= t2.order_id
        group by 1
        order by paid_orders.order_id
    ),

    customer_orders as (
        select
            customers.id as customer_id,
            min(order_date) as first_order_date,
            max(order_date) as most_recent_order_date,
            count(orders.id) as number_of_orders
        from customers
        left join
            orders as orders
            on orders.user_id = customers.id
        group by 1
    )
	-- Final CTE
    -- Simple Select Statment

    ------------------------

select
    paid_orders.*,
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    row_number() over (
        partition by customer_id order by paid_orders.order_id
    ) as customer_sales_seq,
    case
        when customer_orders.first_order_date = paid_orders.order_placed_at then 'new' else 'return'
    end as nvsr,
    total_paid_by_order.clv_bad as customer_lifetime_value,
    customer_orders.first_order_date as fdos
from paid_orders
left join customer_orders using (customer_id)
left outer join
    total_paid_by_order
    on total_paid_by_order.order_id = paid_orders.order_id
order by order_id
