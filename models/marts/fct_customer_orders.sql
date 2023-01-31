with 

orders as (

    select * from {{ ref('int_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

-----
base_customer_orders as (

    select 

        orders.*,
        --customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,

        ---customer-level aggregations

        min(orders.order_date) over(partition by orders.customer_id) as customer_first_order_date,
        min(orders.valid_order_date) over(partition by orders.customer_id) as customer_first_non_returned_order_date,
        max(orders.valid_order_date) over(partition by orders.customer_id) as customer_first_non_returned_order_date,
        count(*) over(partition by orders.customer_id) as customer_order_count,
        sum(nvl2(orders.valid_order_date, 1, 0)) over(partition by orders.customer_id) as customer_non_returned_order_count,
        sum(nvl2(orders.valid_order_date, order_value_dollars, 0)) over(partition by orders.customer_id) as customer_total_lifetime_value,
        array_agg(distinct orders.order_id) over(partition by orders.customer_id) as customer_order_ids


    from orders

    inner join customers
    on orders.customer_id = customers.customer_id

),

customer_orders as (

    select

        base_customer_orders.*,
        customer_total_lifetime_value / customer_non_returned_order_count as avg_non_returned_order_value

    from base_customer_orders

),

--final CTE
final as (

    select

        order_id,
        customer_id,
        surname,
        givenname,
        customer_first_order_date as first_order_date,
        customer_order_count as customer_order_count,
        customer_total_lifetime_value as total_lifetime_value,
        order_value_dollars,
        order_status,
        payment_status

    from customer_orders
    
)

select * from final