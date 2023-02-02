WITH data AS(
    SELECT * FROM {{ref('customers_list_trusted')}}
),

quintile AS(
    SELECT
    data.customer_id,
    data.first_name,
    NTILE(5) OVER (ORDER BY data.days_last_order) AS recency_quintile,
    NTILE(5) OVER (ORDER BY data.orders_number) AS frequency_quintile,
    NTILE(5) OVER (ORDER BY data.total_spent) AS monetary_quintile

    FROM data ORDER BY 1
),

classific AS(
    SELECT
    quintile.customer_id,
    quintile.first_name,
    CASE
        WHEN
        quintile.monetary_quintile >= 4 AND quintile.frequency_quintile >= 4 THEN "Champion"
        WHEN
        quintile.frequency_quintile >= 4 THEN "Loyal"
        WHEN
        quintile.recency_quintile <= 1 THEN "At Risk"
        WHEN
        quintile.recency_quintile >= 4 THEN "Promissing"
    END AS classification

    FROM quintile
)

SELECT * FROM classific WHERE classification IS NOT NULL