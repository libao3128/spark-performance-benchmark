== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['substr('w_warehouse_name, 1, 20) ASC NULLS FIRST, 'sm_type ASC NULLS FIRST, 'web_name ASC NULLS FIRST], true
      +- 'Aggregate ['substr('w_warehouse_name, 1, 20), 'sm_type, 'web_name], [unresolvedalias('substr('w_warehouse_name, 1, 20), None), 'sm_type, 'web_name, 'sum(CASE WHEN (('ws_ship_date_sk - 'ws_sold_date_sk) <= 30) THEN 1 ELSE 0 END) AS 30_days#0, 'sum(CASE WHEN ((('ws_ship_date_sk - 'ws_sold_date_sk) > 30) && (('ws_ship_date_sk - 'ws_sold_date_sk) <= 60)) THEN 1 ELSE 0 END) AS 31_60_days#1, 'sum(CASE WHEN ((('ws_ship_date_sk - 'ws_sold_date_sk) > 60) && (('ws_ship_date_sk - 'ws_sold_date_sk) <= 90)) THEN 1 ELSE 0 END) AS 61_90_days#2, 'sum(CASE WHEN ((('ws_ship_date_sk - 'ws_sold_date_sk) > 90) && (('ws_ship_date_sk - 'ws_sold_date_sk) <= 120)) THEN 1 ELSE 0 END) AS 91_120_days#3, 'sum(CASE WHEN (('ws_ship_date_sk - 'ws_sold_date_sk) > 120) THEN 1 ELSE 0 END) AS above120_days#4]
         +- 'Filter ((((('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11))) && ('ws_ship_date_sk = 'd_date_sk)) && ('ws_warehouse_sk = 'w_warehouse_sk)) && (('ws_ship_mode_sk = 'sm_ship_mode_sk) && ('ws_web_site_sk = 'web_site_sk)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation `web_sales`
               :  :  :  +- 'UnresolvedRelation `warehouse`
               :  :  +- 'UnresolvedRelation `ship_mode`
               :  +- 'UnresolvedRelation `web_site`
               +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
substring(w_warehouse_name, 1, 20): string, sm_type: string, web_name: string, 30_days: bigint, 31_60_days: bigint, 61_90_days: bigint, 91_120_days: bigint, above120_days: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Project [substring(w_warehouse_name, 1, 20)#120, sm_type#57, web_name#65, 30_days#0L, 31_60_days#1L, 61_90_days#2L, 91_120_days#3L, above120_days#4L]
      +- Sort [substring(w_warehouse_name, 1, 20)#120 ASC NULLS FIRST, sm_type#57 ASC NULLS FIRST, web_name#65 ASC NULLS FIRST], true
         +- Aggregate [substring(w_warehouse_name#43, 1, 20), sm_type#57, web_name#65], [substring(w_warehouse_name#43, 1, 20) AS substring(w_warehouse_name, 1, 20)#120, sm_type#57, web_name#65, sum(cast(CASE WHEN ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)) AS 30_days#0L, sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 30) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)) AS 31_60_days#1L, sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 60) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)) AS 61_90_days#2L, sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 90) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)) AS 91_120_days#3L, sum(cast(CASE WHEN ((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint)) AS above120_days#4L]
            +- Filter (((((d_month_seq#90 >= 1200) && (d_month_seq#90 <= (1200 + 11))) && (ws_ship_date_sk#9 = d_date_sk#87)) && (ws_warehouse_sk#22 = w_warehouse_sk#41)) && ((ws_ship_mode_sk#21 = sm_ship_mode_sk#55) && (ws_web_site_sk#20 = web_site_sk#61)))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- SubqueryAlias `tpcds`.`web_sales`
                  :  :  :  :  +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
                  :  :  :  +- SubqueryAlias `tpcds`.`warehouse`
                  :  :  :     +- Relation[w_warehouse_sk#41,w_warehouse_id#42,w_warehouse_name#43,w_warehouse_sq_ft#44,w_street_number#45,w_street_name#46,w_street_type#47,w_suite_number#48,w_city#49,w_county#50,w_state#51,w_zip#52,w_country#53,w_gmt_offset#54] parquet
                  :  :  +- SubqueryAlias `tpcds`.`ship_mode`
                  :  :     +- Relation[sm_ship_mode_sk#55,sm_ship_mode_id#56,sm_type#57,sm_code#58,sm_carrier#59,sm_contract#60] parquet
                  :  +- SubqueryAlias `tpcds`.`web_site`
                  :     +- Relation[web_site_sk#61,web_site_id#62,web_rec_start_date#63,web_rec_end_date#64,web_name#65,web_open_date_sk#66,web_close_date_sk#67,web_class#68,web_manager#69,web_mkt_id#70,web_mkt_class#71,web_mkt_desc#72,web_market_manager#73,web_company_id#74,web_company_name#75,web_street_number#76,web_street_name#77,web_street_type#78,web_suite_number#79,web_city#80,web_county#81,web_state#82,web_zip#83,web_country#84,... 2 more fields] parquet
                  +- SubqueryAlias `tpcds`.`date_dim`
                     +- Relation[d_date_sk#87,d_date_id#88,d_date#89,d_month_seq#90,d_week_seq#91,d_quarter_seq#92,d_year#93,d_dow#94,d_moy#95,d_dom#96,d_qoy#97,d_fy_year#98,d_fy_quarter_seq#99,d_fy_week_seq#100,d_day_name#101,d_quarter_name#102,d_holiday#103,d_weekend#104,d_following_holiday#105,d_first_dom#106,d_last_dom#107,d_same_day_ly#108,d_same_day_lq#109,d_current_day#110,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [substring(w_warehouse_name, 1, 20)#120 ASC NULLS FIRST, sm_type#57 ASC NULLS FIRST, web_name#65 ASC NULLS FIRST], true
      +- Aggregate [substring(w_warehouse_name#43, 1, 20), sm_type#57, web_name#65], [substring(w_warehouse_name#43, 1, 20) AS substring(w_warehouse_name, 1, 20)#120, sm_type#57, web_name#65, sum(cast(CASE WHEN ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)) AS 30_days#0L, sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 30) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)) AS 31_60_days#1L, sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 60) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)) AS 61_90_days#2L, sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 90) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)) AS 91_120_days#3L, sum(cast(CASE WHEN ((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint)) AS above120_days#4L]
         +- Project [ws_sold_date_sk#7, ws_ship_date_sk#9, w_warehouse_name#43, sm_type#57, web_name#65]
            +- Join Inner, (ws_ship_date_sk#9 = d_date_sk#87)
               :- Project [ws_sold_date_sk#7, ws_ship_date_sk#9, w_warehouse_name#43, sm_type#57, web_name#65]
               :  +- Join Inner, (ws_web_site_sk#20 = web_site_sk#61)
               :     :- Project [ws_sold_date_sk#7, ws_ship_date_sk#9, ws_web_site_sk#20, w_warehouse_name#43, sm_type#57]
               :     :  +- Join Inner, (ws_ship_mode_sk#21 = sm_ship_mode_sk#55)
               :     :     :- Project [ws_sold_date_sk#7, ws_ship_date_sk#9, ws_web_site_sk#20, ws_ship_mode_sk#21, w_warehouse_name#43]
               :     :     :  +- Join Inner, (ws_warehouse_sk#22 = w_warehouse_sk#41)
               :     :     :     :- Project [ws_sold_date_sk#7, ws_ship_date_sk#9, ws_web_site_sk#20, ws_ship_mode_sk#21, ws_warehouse_sk#22]
               :     :     :     :  +- Filter (((isnotnull(ws_warehouse_sk#22) && isnotnull(ws_ship_mode_sk#21)) && isnotnull(ws_web_site_sk#20)) && isnotnull(ws_ship_date_sk#9))
               :     :     :     :     +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
               :     :     :     +- Project [w_warehouse_sk#41, w_warehouse_name#43]
               :     :     :        +- Filter isnotnull(w_warehouse_sk#41)
               :     :     :           +- Relation[w_warehouse_sk#41,w_warehouse_id#42,w_warehouse_name#43,w_warehouse_sq_ft#44,w_street_number#45,w_street_name#46,w_street_type#47,w_suite_number#48,w_city#49,w_county#50,w_state#51,w_zip#52,w_country#53,w_gmt_offset#54] parquet
               :     :     +- Project [sm_ship_mode_sk#55, sm_type#57]
               :     :        +- Filter isnotnull(sm_ship_mode_sk#55)
               :     :           +- Relation[sm_ship_mode_sk#55,sm_ship_mode_id#56,sm_type#57,sm_code#58,sm_carrier#59,sm_contract#60] parquet
               :     +- Project [web_site_sk#61, web_name#65]
               :        +- Filter isnotnull(web_site_sk#61)
               :           +- Relation[web_site_sk#61,web_site_id#62,web_rec_start_date#63,web_rec_end_date#64,web_name#65,web_open_date_sk#66,web_close_date_sk#67,web_class#68,web_manager#69,web_mkt_id#70,web_mkt_class#71,web_mkt_desc#72,web_market_manager#73,web_company_id#74,web_company_name#75,web_street_number#76,web_street_name#77,web_street_type#78,web_suite_number#79,web_city#80,web_county#81,web_state#82,web_zip#83,web_country#84,... 2 more fields] parquet
               +- Project [d_date_sk#87]
                  +- Filter (((isnotnull(d_month_seq#90) && (d_month_seq#90 >= 1200)) && (d_month_seq#90 <= 1211)) && isnotnull(d_date_sk#87))
                     +- Relation[d_date_sk#87,d_date_id#88,d_date#89,d_month_seq#90,d_week_seq#91,d_quarter_seq#92,d_year#93,d_dow#94,d_moy#95,d_dom#96,d_qoy#97,d_fy_year#98,d_fy_quarter_seq#99,d_fy_week_seq#100,d_day_name#101,d_quarter_name#102,d_holiday#103,d_weekend#104,d_following_holiday#105,d_first_dom#106,d_last_dom#107,d_same_day_ly#108,d_same_day_lq#109,d_current_day#110,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[substring(w_warehouse_name, 1, 20)#120 ASC NULLS FIRST,sm_type#57 ASC NULLS FIRST,web_name#65 ASC NULLS FIRST], output=[substring(w_warehouse_name, 1, 20)#120,sm_type#57,web_name#65,30_days#0L,31_60_days#1L,61_90_days#2L,91_120_days#3L,above120_days#4L])
+- *(6) HashAggregate(keys=[substring(w_warehouse_name#43, 1, 20)#124, sm_type#57, web_name#65], functions=[sum(cast(CASE WHEN ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 30) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 60) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 90) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN ((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint))], output=[substring(w_warehouse_name, 1, 20)#120, sm_type#57, web_name#65, 30_days#0L, 31_60_days#1L, 61_90_days#2L, 91_120_days#3L, above120_days#4L])
   +- Exchange hashpartitioning(substring(w_warehouse_name#43, 1, 20)#124, sm_type#57, web_name#65, 200)
      +- *(5) HashAggregate(keys=[substring(w_warehouse_name#43, 1, 20) AS substring(w_warehouse_name#43, 1, 20)#124, sm_type#57, web_name#65], functions=[partial_sum(cast(CASE WHEN ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 30) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 60) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 90) && ((ws_ship_date_sk#9 - ws_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN ((ws_ship_date_sk#9 - ws_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint))], output=[substring(w_warehouse_name#43, 1, 20)#124, sm_type#57, web_name#65, sum#130L, sum#131L, sum#132L, sum#133L, sum#134L])
         +- *(5) Project [ws_sold_date_sk#7, ws_ship_date_sk#9, w_warehouse_name#43, sm_type#57, web_name#65]
            +- *(5) BroadcastHashJoin [ws_ship_date_sk#9], [d_date_sk#87], Inner, BuildRight
               :- *(5) Project [ws_sold_date_sk#7, ws_ship_date_sk#9, w_warehouse_name#43, sm_type#57, web_name#65]
               :  +- *(5) BroadcastHashJoin [ws_web_site_sk#20], [web_site_sk#61], Inner, BuildRight
               :     :- *(5) Project [ws_sold_date_sk#7, ws_ship_date_sk#9, ws_web_site_sk#20, w_warehouse_name#43, sm_type#57]
               :     :  +- *(5) BroadcastHashJoin [ws_ship_mode_sk#21], [sm_ship_mode_sk#55], Inner, BuildRight
               :     :     :- *(5) Project [ws_sold_date_sk#7, ws_ship_date_sk#9, ws_web_site_sk#20, ws_ship_mode_sk#21, w_warehouse_name#43]
               :     :     :  +- *(5) BroadcastHashJoin [ws_warehouse_sk#22], [w_warehouse_sk#41], Inner, BuildRight
               :     :     :     :- *(5) Project [ws_sold_date_sk#7, ws_ship_date_sk#9, ws_web_site_sk#20, ws_ship_mode_sk#21, ws_warehouse_sk#22]
               :     :     :     :  +- *(5) Filter (((isnotnull(ws_warehouse_sk#22) && isnotnull(ws_ship_mode_sk#21)) && isnotnull(ws_web_site_sk#20)) && isnotnull(ws_ship_date_sk#9))
               :     :     :     :     +- *(5) FileScan parquet tpcds.web_sales[ws_sold_date_sk#7,ws_ship_date_sk#9,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_warehouse_sk), IsNotNull(ws_ship_mode_sk), IsNotNull(ws_web_site_sk), IsNotNull(ws_..., ReadSchema: struct<ws_sold_date_sk:int,ws_ship_date_sk:int,ws_web_site_sk:int,ws_ship_mode_sk:int,ws_warehous...
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :        +- *(1) Project [w_warehouse_sk#41, w_warehouse_name#43]
               :     :     :           +- *(1) Filter isnotnull(w_warehouse_sk#41)
               :     :     :              +- *(1) FileScan parquet tpcds.warehouse[w_warehouse_sk#41,w_warehouse_name#43] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(2) Project [sm_ship_mode_sk#55, sm_type#57]
               :     :           +- *(2) Filter isnotnull(sm_ship_mode_sk#55)
               :     :              +- *(2) FileScan parquet tpcds.ship_mode[sm_ship_mode_sk#55,sm_type#57] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sm_ship_mode_sk)], ReadSchema: struct<sm_ship_mode_sk:int,sm_type:string>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(3) Project [web_site_sk#61, web_name#65]
               :           +- *(3) Filter isnotnull(web_site_sk#61)
               :              +- *(3) FileScan parquet tpcds.web_site[web_site_sk#61,web_name#65] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_name:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(4) Project [d_date_sk#87]
                     +- *(4) Filter (((isnotnull(d_month_seq#90) && (d_month_seq#90 >= 1200)) && (d_month_seq#90 <= 1211)) && isnotnull(d_date_sk#87))
                        +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#87,d_month_seq#90] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
Time taken: 3.819 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 62 in stream 0 using template query62.tpl
------------------------------------------------------^^^

