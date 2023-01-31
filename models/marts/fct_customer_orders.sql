--import CTEs
with 

orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),


select

    order_id,
    customer_id,
    surname,
    givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    payment_amount as order_value_dollars,
    order_status,
    payment_status

from orders

join customers
on orders.customer_id = customers.customer_id

join (

    select 

        b.customer_id,
        b.name as full_name,
        b.surname,
        b.givenname,
        min(order_date) as first_order_date,
        min(case 
                when a.order_status not in ('returned','return_pending') 
                then order_date 
            end) as first_non_returned_order_date,
        max(case 
                when a.order_status not in ('returned','return_pending') 
                then order_date 
            end) as most_recent_non_returned_order_date,
        coalesce(max(user_order_seq), 0) as order_count,
        coalesce(count(case 
                            when a.order_status != 'returned' 
                            then 1 
                        end), 0) as non_returned_order_count,
        sum(case 
                when a.order_status not in ('returned','return_pending') 
                then c.payment_amount
                else 0 
            end) as total_lifetime_value,
        sum(case 
                when a.order_status not in ('returned','return_pending') 
                then c.payment_amount
                else 0 
            end) / nullif(count(case 
                                when a.order_status not in ('returned','return_pending') 
                                then 1 
                              end), 0) as avg_non_returned_order_value,
        array_agg(distinct a.id) as order_ids

    from orders a

    join customers b
    on a.customer_id = b.customer_id

    left outer join payments c
    on a.order_id = c.order_id

    where a.order_status not in ('pending') and c.payment_status != 'fail'

    group by 1, 2, 3, 4

    ) customer_order_history
on orders.customer_id = customer_order_history.customer_id

left outer join payments
on orders.order_id = payments.order_id

where payments.payment_status != 'fail'