SELECT TOP 20
    order_id,
    customer_id,
    status,
    order_amount,
    created_at,
    updated_at
FROM dbo.orders
ORDER BY order_id DESC;