SELECT  
   substr(w_warehouse_name,1,20) AS warehouse_name,
   sm_type,
   cc_name,
   SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS days_30, 
   SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 30) AND (cs_ship_date_sk - cs_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS days_31_60,
   SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 60) AND (cs_ship_date_sk - cs_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS days_61_90,
   SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 90) AND (cs_ship_date_sk - cs_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS days_91_120,
   SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 120) THEN 1 ELSE 0 END) AS above120_days
FROM
   catalog_sales cs
JOIN warehouse w ON cs.cs_warehouse_sk = w.w_warehouse_sk
JOIN ship_mode sm ON cs.cs_ship_mode_sk = sm.sm_ship_mode_sk
JOIN call_center cc ON cs.cs_call_center_sk = cc.cc_call_center_sk
JOIN date_dim d 
  ON cs.cs_ship_date_sk = d.d_date_sk
  AND d_month_seq BETWEEN 1200 AND 1211 -- Partition Pruning on date_dim
GROUP BY 
   substr(w_warehouse_name,1,20),
   sm_type,
   cc_name
ORDER BY 
   substr(w_warehouse_name,1,20),
   sm_type,
   cc_name
LIMIT 100;
