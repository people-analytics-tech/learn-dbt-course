with

    payments as (select * from {{ source("data_warehouse_test", "stripe_payments") }}),

    transformed as (

        select

            cast(id as int64) as payment_id,
            cast(orderid as int64) as order_id,
            created as payment_created_at,
            paymentmethod as payment_method,
            status as payment_status,
            cast(round(amount / 100.0, 2) as int64) as payment_amount

        from payments

    )

select *
from transformed
