SELECT
    orderid AS order_id,
    amount,
    status

    FROM dbt_test_jaffle_shop.payment

    WHERE status = 'success'