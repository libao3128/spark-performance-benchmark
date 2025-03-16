-- SELECT COUNT(*) FROM tpcds.inventory WHERE inv_date_sk IS NOT NULL;
-- SELECT COUNT(*) FROM tpcds.inventory WHERE inv_item_sk IS NULL;
EXPLAIN EXTENDED
SELECT i_item_sk, w_warehouse_sk 
FROM inventory 
JOIN item ON inv_item_sk = i_item_sk
JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
JOIN date_dim ON inv_date_sk = d_date_sk
WHERE d_year = 2001;



