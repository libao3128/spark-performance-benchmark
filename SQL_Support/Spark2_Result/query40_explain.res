== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['w_state ASC NULLS FIRST, 'i_item_id ASC NULLS FIRST], true
      +- 'Aggregate ['w_state, 'i_item_id], ['w_state, 'i_item_id, 'sum(CASE WHEN (cast('d_date as date) < cast(2000-03-11 as date)) THEN ('cs_sales_price - 'coalesce('cr_refunded_cash, 0)) ELSE 0 END) AS sales_before#0, 'sum(CASE WHEN (cast('d_date as date) >= cast(2000-03-11 as date)) THEN ('cs_sales_price - 'coalesce('cr_refunded_cash, 0)) ELSE 0 END) AS sales_after#1]
         +- 'Filter ((((('i_current_price >= 0.99) && ('i_current_price <= 1.49)) && ('i_item_sk = 'cs_item_sk)) && ('cs_warehouse_sk = 'w_warehouse_sk)) && (('cs_sold_date_sk = 'd_date_sk) && (('d_date >= 'date_sub(cast(2000-03-11 as date), 30)) && ('d_date <= 'date_add(cast(2000-03-11 as date), 30)))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join LeftOuter, (('cs_order_number = 'cr_order_number) && ('cs_item_sk = 'cr_item_sk))
               :  :  :  :- 'UnresolvedRelation `catalog_sales`
               :  :  :  +- 'UnresolvedRelation `catalog_returns`
               :  :  +- 'UnresolvedRelation `warehouse`
               :  +- 'UnresolvedRelation `item`
               +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
w_state: string, i_item_id: string, sales_before: double, sales_after: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [w_state#75, i_item_id#80, sales_before#0, sales_after#1]
      +- Sort [w_state#75 ASC NULLS FIRST, i_item_id#80 ASC NULLS FIRST], true
         +- Aggregate [w_state#75, i_item_id#80], [w_state#75, i_item_id#80, sum(CASE WHEN (cast(d_date#103 as date) < cast(2000-03-11 as date)) THEN (cs_sales_price#25 - coalesce(cr_refunded_cash#61, cast(0 as double))) ELSE cast(0 as double) END) AS sales_before#0, sum(CASE WHEN (cast(d_date#103 as date) >= cast(2000-03-11 as date)) THEN (cs_sales_price#25 - coalesce(cr_refunded_cash#61, cast(0 as double))) ELSE cast(0 as double) END) AS sales_after#1]
            +- Filter (((((i_current_price#84 >= cast(0.99 as double)) && (i_current_price#84 <= cast(1.49 as double))) && (i_item_sk#79 = cs_item_sk#19)) && (cs_warehouse_sk#18 = w_warehouse_sk#65)) && ((cs_sold_date_sk#4 = d_date_sk#101) && ((d_date#103 >= cast(date_sub(cast(2000-03-11 as date), 30) as string)) && (d_date#103 <= cast(date_add(cast(2000-03-11 as date), 30) as string)))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join LeftOuter, ((cs_order_number#21 = cr_order_number#54) && (cs_item_sk#19 = cr_item_sk#40))
                  :  :  :  :- SubqueryAlias `tpcds`.`catalog_sales`
                  :  :  :  :  +- Relation[cs_sold_date_sk#4,cs_sold_time_sk#5,cs_ship_date_sk#6,cs_bill_customer_sk#7,cs_bill_cdemo_sk#8,cs_bill_hdemo_sk#9,cs_bill_addr_sk#10,cs_ship_customer_sk#11,cs_ship_cdemo_sk#12,cs_ship_hdemo_sk#13,cs_ship_addr_sk#14,cs_call_center_sk#15,cs_catalog_page_sk#16,cs_ship_mode_sk#17,cs_warehouse_sk#18,cs_item_sk#19,cs_promo_sk#20,cs_order_number#21,cs_quantity#22,cs_wholesale_cost#23,cs_list_price#24,cs_sales_price#25,cs_ext_discount_amt#26,cs_ext_sales_price#27,... 10 more fields] parquet
                  :  :  :  +- SubqueryAlias `tpcds`.`catalog_returns`
                  :  :  :     +- Relation[cr_returned_date_sk#38,cr_returned_time_sk#39,cr_item_sk#40,cr_refunded_customer_sk#41,cr_refunded_cdemo_sk#42,cr_refunded_hdemo_sk#43,cr_refunded_addr_sk#44,cr_returning_customer_sk#45,cr_returning_cdemo_sk#46,cr_returning_hdemo_sk#47,cr_returning_addr_sk#48,cr_call_center_sk#49,cr_catalog_page_sk#50,cr_ship_mode_sk#51,cr_warehouse_sk#52,cr_reason_sk#53,cr_order_number#54,cr_return_quantity#55,cr_return_amount#56,cr_return_tax#57,cr_return_amt_inc_tax#58,cr_fee#59,cr_return_ship_cost#60,cr_refunded_cash#61,... 3 more fields] parquet
                  :  :  +- SubqueryAlias `tpcds`.`warehouse`
                  :  :     +- Relation[w_warehouse_sk#65,w_warehouse_id#66,w_warehouse_name#67,w_warehouse_sq_ft#68,w_street_number#69,w_street_name#70,w_street_type#71,w_suite_number#72,w_city#73,w_county#74,w_state#75,w_zip#76,w_country#77,w_gmt_offset#78] parquet
                  :  +- SubqueryAlias `tpcds`.`item`
                  :     +- Relation[i_item_sk#79,i_item_id#80,i_rec_start_date#81,i_rec_end_date#82,i_item_desc#83,i_current_price#84,i_wholesale_cost#85,i_brand_id#86,i_brand#87,i_class_id#88,i_class#89,i_category_id#90,i_category#91,i_manufact_id#92,i_manufact#93,i_size#94,i_formulation#95,i_color#96,i_units#97,i_container#98,i_manager_id#99,i_product_name#100] parquet
                  +- SubqueryAlias `tpcds`.`date_dim`
                     +- Relation[d_date_sk#101,d_date_id#102,d_date#103,d_month_seq#104,d_week_seq#105,d_quarter_seq#106,d_year#107,d_dow#108,d_moy#109,d_dom#110,d_qoy#111,d_fy_year#112,d_fy_quarter_seq#113,d_fy_week_seq#114,d_day_name#115,d_quarter_name#116,d_holiday#117,d_weekend#118,d_following_holiday#119,d_first_dom#120,d_last_dom#121,d_same_day_ly#122,d_same_day_lq#123,d_current_day#124,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [w_state#75 ASC NULLS FIRST, i_item_id#80 ASC NULLS FIRST], true
      +- Aggregate [w_state#75, i_item_id#80], [w_state#75, i_item_id#80, sum(CASE WHEN (cast(d_date#103 as date) < 11027) THEN (cs_sales_price#25 - coalesce(cr_refunded_cash#61, 0.0)) ELSE 0.0 END) AS sales_before#0, sum(CASE WHEN (cast(d_date#103 as date) >= 11027) THEN (cs_sales_price#25 - coalesce(cr_refunded_cash#61, 0.0)) ELSE 0.0 END) AS sales_after#1]
         +- Project [cs_sales_price#25, cr_refunded_cash#61, w_state#75, i_item_id#80, d_date#103]
            +- Join Inner, (cs_sold_date_sk#4 = d_date_sk#101)
               :- Project [cs_sold_date_sk#4, cs_sales_price#25, cr_refunded_cash#61, w_state#75, i_item_id#80]
               :  +- Join Inner, (i_item_sk#79 = cs_item_sk#19)
               :     :- Project [cs_sold_date_sk#4, cs_item_sk#19, cs_sales_price#25, cr_refunded_cash#61, w_state#75]
               :     :  +- Join Inner, (cs_warehouse_sk#18 = w_warehouse_sk#65)
               :     :     :- Project [cs_sold_date_sk#4, cs_warehouse_sk#18, cs_item_sk#19, cs_sales_price#25, cr_refunded_cash#61]
               :     :     :  +- Join LeftOuter, ((cs_order_number#21 = cr_order_number#54) && (cs_item_sk#19 = cr_item_sk#40))
               :     :     :     :- Project [cs_sold_date_sk#4, cs_warehouse_sk#18, cs_item_sk#19, cs_order_number#21, cs_sales_price#25]
               :     :     :     :  +- Filter ((isnotnull(cs_warehouse_sk#18) && isnotnull(cs_item_sk#19)) && isnotnull(cs_sold_date_sk#4))
               :     :     :     :     +- Relation[cs_sold_date_sk#4,cs_sold_time_sk#5,cs_ship_date_sk#6,cs_bill_customer_sk#7,cs_bill_cdemo_sk#8,cs_bill_hdemo_sk#9,cs_bill_addr_sk#10,cs_ship_customer_sk#11,cs_ship_cdemo_sk#12,cs_ship_hdemo_sk#13,cs_ship_addr_sk#14,cs_call_center_sk#15,cs_catalog_page_sk#16,cs_ship_mode_sk#17,cs_warehouse_sk#18,cs_item_sk#19,cs_promo_sk#20,cs_order_number#21,cs_quantity#22,cs_wholesale_cost#23,cs_list_price#24,cs_sales_price#25,cs_ext_discount_amt#26,cs_ext_sales_price#27,... 10 more fields] parquet
               :     :     :     +- Project [cr_item_sk#40, cr_order_number#54, cr_refunded_cash#61]
               :     :     :        +- Filter (isnotnull(cr_order_number#54) && isnotnull(cr_item_sk#40))
               :     :     :           +- Relation[cr_returned_date_sk#38,cr_returned_time_sk#39,cr_item_sk#40,cr_refunded_customer_sk#41,cr_refunded_cdemo_sk#42,cr_refunded_hdemo_sk#43,cr_refunded_addr_sk#44,cr_returning_customer_sk#45,cr_returning_cdemo_sk#46,cr_returning_hdemo_sk#47,cr_returning_addr_sk#48,cr_call_center_sk#49,cr_catalog_page_sk#50,cr_ship_mode_sk#51,cr_warehouse_sk#52,cr_reason_sk#53,cr_order_number#54,cr_return_quantity#55,cr_return_amount#56,cr_return_tax#57,cr_return_amt_inc_tax#58,cr_fee#59,cr_return_ship_cost#60,cr_refunded_cash#61,... 3 more fields] parquet
               :     :     +- Project [w_warehouse_sk#65, w_state#75]
               :     :        +- Filter isnotnull(w_warehouse_sk#65)
               :     :           +- Relation[w_warehouse_sk#65,w_warehouse_id#66,w_warehouse_name#67,w_warehouse_sq_ft#68,w_street_number#69,w_street_name#70,w_street_type#71,w_suite_number#72,w_city#73,w_county#74,w_state#75,w_zip#76,w_country#77,w_gmt_offset#78] parquet
               :     +- Project [i_item_sk#79, i_item_id#80]
               :        +- Filter (((isnotnull(i_current_price#84) && (i_current_price#84 >= 0.99)) && (i_current_price#84 <= 1.49)) && isnotnull(i_item_sk#79))
               :           +- Relation[i_item_sk#79,i_item_id#80,i_rec_start_date#81,i_rec_end_date#82,i_item_desc#83,i_current_price#84,i_wholesale_cost#85,i_brand_id#86,i_brand#87,i_class_id#88,i_class#89,i_category_id#90,i_category#91,i_manufact_id#92,i_manufact#93,i_size#94,i_formulation#95,i_color#96,i_units#97,i_container#98,i_manager_id#99,i_product_name#100] parquet
               +- Project [d_date_sk#101, d_date#103]
                  +- Filter (((isnotnull(d_date#103) && (d_date#103 >= 2000-02-10)) && (d_date#103 <= 2000-04-10)) && isnotnull(d_date_sk#101))
                     +- Relation[d_date_sk#101,d_date_id#102,d_date#103,d_month_seq#104,d_week_seq#105,d_quarter_seq#106,d_year#107,d_dow#108,d_moy#109,d_dom#110,d_qoy#111,d_fy_year#112,d_fy_quarter_seq#113,d_fy_week_seq#114,d_day_name#115,d_quarter_name#116,d_holiday#117,d_weekend#118,d_following_holiday#119,d_first_dom#120,d_last_dom#121,d_same_day_ly#122,d_same_day_lq#123,d_current_day#124,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[w_state#75 ASC NULLS FIRST,i_item_id#80 ASC NULLS FIRST], output=[w_state#75,i_item_id#80,sales_before#0,sales_after#1])
+- *(6) HashAggregate(keys=[w_state#75, i_item_id#80], functions=[sum(CASE WHEN (cast(d_date#103 as date) < 11027) THEN (cs_sales_price#25 - coalesce(cr_refunded_cash#61, 0.0)) ELSE 0.0 END), sum(CASE WHEN (cast(d_date#103 as date) >= 11027) THEN (cs_sales_price#25 - coalesce(cr_refunded_cash#61, 0.0)) ELSE 0.0 END)], output=[w_state#75, i_item_id#80, sales_before#0, sales_after#1])
   +- Exchange hashpartitioning(w_state#75, i_item_id#80, 200)
      +- *(5) HashAggregate(keys=[w_state#75, i_item_id#80], functions=[partial_sum(CASE WHEN (cast(d_date#103 as date) < 11027) THEN (cs_sales_price#25 - coalesce(cr_refunded_cash#61, 0.0)) ELSE 0.0 END), partial_sum(CASE WHEN (cast(d_date#103 as date) >= 11027) THEN (cs_sales_price#25 - coalesce(cr_refunded_cash#61, 0.0)) ELSE 0.0 END)], output=[w_state#75, i_item_id#80, sum#135, sum#136])
         +- *(5) Project [cs_sales_price#25, cr_refunded_cash#61, w_state#75, i_item_id#80, d_date#103]
            +- *(5) BroadcastHashJoin [cs_sold_date_sk#4], [d_date_sk#101], Inner, BuildRight
               :- *(5) Project [cs_sold_date_sk#4, cs_sales_price#25, cr_refunded_cash#61, w_state#75, i_item_id#80]
               :  +- *(5) BroadcastHashJoin [cs_item_sk#19], [i_item_sk#79], Inner, BuildRight
               :     :- *(5) Project [cs_sold_date_sk#4, cs_item_sk#19, cs_sales_price#25, cr_refunded_cash#61, w_state#75]
               :     :  +- *(5) BroadcastHashJoin [cs_warehouse_sk#18], [w_warehouse_sk#65], Inner, BuildRight
               :     :     :- *(5) Project [cs_sold_date_sk#4, cs_warehouse_sk#18, cs_item_sk#19, cs_sales_price#25, cr_refunded_cash#61]
               :     :     :  +- *(5) BroadcastHashJoin [cs_order_number#21, cs_item_sk#19], [cr_order_number#54, cr_item_sk#40], LeftOuter, BuildRight
               :     :     :     :- *(5) Project [cs_sold_date_sk#4, cs_warehouse_sk#18, cs_item_sk#19, cs_order_number#21, cs_sales_price#25]
               :     :     :     :  +- *(5) Filter ((isnotnull(cs_warehouse_sk#18) && isnotnull(cs_item_sk#19)) && isnotnull(cs_sold_date_sk#4))
               :     :     :     :     +- *(5) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#4,cs_warehouse_sk#18,cs_item_sk#19,cs_order_number#21,cs_sales_price#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_warehouse_sk), IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_warehouse_sk:int,cs_item_sk:int,cs_order_number:int,cs_sales_price:...
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
               :     :     :        +- *(1) Project [cr_item_sk#40, cr_order_number#54, cr_refunded_cash#61]
               :     :     :           +- *(1) Filter (isnotnull(cr_order_number#54) && isnotnull(cr_item_sk#40))
               :     :     :              +- *(1) FileScan parquet tpcds.catalog_returns[cr_item_sk#40,cr_order_number#54,cr_refunded_cash#61] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_order_number), IsNotNull(cr_item_sk)], ReadSchema: struct<cr_item_sk:int,cr_order_number:int,cr_refunded_cash:double>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(2) Project [w_warehouse_sk#65, w_state#75]
               :     :           +- *(2) Filter isnotnull(w_warehouse_sk#65)
               :     :              +- *(2) FileScan parquet tpcds.warehouse[w_warehouse_sk#65,w_state#75] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_state:string>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(3) Project [i_item_sk#79, i_item_id#80]
               :           +- *(3) Filter (((isnotnull(i_current_price#84) && (i_current_price#84 >= 0.99)) && (i_current_price#84 <= 1.49)) && isnotnull(i_item_sk#79))
               :              +- *(3) FileScan parquet tpcds.item[i_item_sk#79,i_item_id#80,i_current_price#84] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThanOrEqual(i_current_price,0.99), LessThanOrEqual(i_current_..., ReadSchema: struct<i_item_sk:int,i_item_id:string,i_current_price:double>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(4) Project [d_date_sk#101, d_date#103]
                     +- *(4) Filter (((isnotnull(d_date#103) && (d_date#103 >= 2000-02-10)) && (d_date#103 <= 2000-04-10)) && isnotnull(d_date_sk#101))
                        +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#101,d_date#103] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-02-10), LessThanOrEqual(d_date,2000-04-10), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
Time taken: 4.11 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 40 in stream 0 using template query40.tpl
------------------------------------------------------^^^

