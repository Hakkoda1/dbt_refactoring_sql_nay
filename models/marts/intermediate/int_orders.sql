--import CTEs
with 

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}
    where payment_status != 'fail'
),

--logical CTEs
order_totals as (

    select 

        order_id,
        payment_status,
        sum(payment_amount) as order_value_dollars

    from payments

    group by 1, 2

),

order_totals_payments_joined as (

    select

        orders.*,
        order_totals.payment_status,
        order_totals.order_value_dollars

    from orders

    left join order_totals
    on orders.order_id = order_totals.order_id

)

select * from order_totals_payments_joined