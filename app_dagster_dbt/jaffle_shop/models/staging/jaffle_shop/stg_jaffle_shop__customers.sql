with

    customers as (
        select * from {{ source("data_warehouse_test", "jaffle_shop_customers") }}
    ),

    transformed as (

        select

            cast(id as int64) as customer_id,
            upper(trim(last_name)) as customer_last_name,
            upper(trim(first_name)) as customer_first_name,
            concat(
                upper(trim(first_name)), " ", upper(trim(last_name))
            ) as customer_full_name

        from customers

    )

select *
from transformed
