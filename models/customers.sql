{{ config(materialized = 'table')}}


with customers as (

    select * from {{ ref('stg_customers')}}
),

orders as (

    select * from {{ ref('stg_orders')}}

),

payments as (

    select * from {{ ref('stg_payments')}}
),

customer_orders as (

        select
        user_id as customer_id,

        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(id) as number_of_orders
    from orders

    group by user_id

),

customer_payments as (

    select
        user_id customer_id,
        sum(amount) as total_amount

    from payments

    left join orders on
         payments.orderid = orders.id

    group by user_id

),

final as (

    select
        customers.id customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value

    from customers

    left join customer_orders
        on customers.id = customer_orders.customer_id

    left join customer_payments
        on  customers.id = customer_payments.customer_id

)

select * from final