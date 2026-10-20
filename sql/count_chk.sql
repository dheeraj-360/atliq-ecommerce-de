SELECT 'customers' AS tbl, COUNT(*) AS [rows] FROM dbo.customers
UNION ALL SELECT 'products',    COUNT(*) FROM dbo.products
UNION ALL SELECT 'orders',      COUNT(*) FROM dbo.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM dbo.order_items
;
