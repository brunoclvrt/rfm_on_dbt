WITH customers_list AS(
    SELECT * FROM {{ref('customers_list_raw')}}
),

customers AS(
    SELECT * FROM {{ref('stg_customers')}}
),

combined AS(
    SELECT
    customers.customer_id,
    customers.first_name,
    DATE_DIFF ('2018-04-09',customers_list.recent_order,DAY) AS days_last_order,
    customers_list.orders_number,
    customers_list.total_spent

    FROM customers
    LEFT JOIN customers_list USING(customer_id)
    WHERE customers_list.orders_number IS NOT NULL
    ORDER BY 1    
)

SELECT * FROM combined