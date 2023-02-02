WITH orders AS(
    SELECT * FROM {{ref('stg_orders_data')}}
),

payment AS(
    SELECT * FROM {{ref('stg_payment')}}
),

customers_data AS(
    SELECT
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    payment.amount

    FROM orders
    LEFT JOIN payment USING (order_id)
),

customers_transformed AS(
    SELECT
    customer_id,
    MAX(order_date) AS recent_order,
    COALESCE(COUNT(order_id),0) AS orders_number,
    SUM(amount) AS total_spent

    FROM customers_data
    GROUP BY 1
)

SELECT * FROM customers_transformed