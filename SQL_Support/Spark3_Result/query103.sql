SELECT item_id, item_desc, total_revenue
FROM (
    SELECT i.item_id, i.item_desc, SUM(ss.sales_price) AS total_revenue,
           RANK() OVER (ORDER BY SUM(ss.sales_price) DESC) AS rnk
    FROM store_sales ss
    JOIN item i ON ss.item_id = i.item_id
    GROUP BY i.item_id, i.item_desc
) ranked_items
WHERE rnk <= 10;