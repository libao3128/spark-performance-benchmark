SELECT c.customer_id, c.customer_first_name, c.customer_last_name, d.year,
       COUNT(DISTINCT ss.item_id) AS unique_items,
       SUM(ss.sales_price) AS total_sales,
       COUNT(*) AS total_transactions
FROM store_sales ss
JOIN customer c ON ss.customer_id = c.customer_id
JOIN date_dim d ON ss.sold_date_sk = d.date_sk
GROUP BY CUBE (c.customer_id, c.customer_first_name, c.customer_last_name, d.year)
ORDER BY total_sales DESC;
