== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['lochierarchy DESC NULLS LAST, CASE WHEN ('lochierarchy = 0) THEN 'i_category END ASC NULLS FIRST, 'rank_within_parent ASC NULLS FIRST], true
      +- 'Aggregate ['rollup('i_category, 'i_class)], ['sum('ws_net_paid) AS total_sum#0, 'i_category, 'i_class, ('grouping('i_category) + 'grouping('i_class)) AS lochierarchy#1, 'rank() windowspecdefinition(('grouping('i_category) + 'grouping('i_class)), CASE WHEN ('grouping('i_class) = 0) THEN 'i_category END, 'sum('ws_net_paid) DESC NULLS LAST, unspecifiedframe$()) AS rank_within_parent#2]
         +- 'Filter (((('d1.d_month_seq >= 1200) && ('d1.d_month_seq <= (1200 + 11))) && ('d1.d_date_sk = 'ws_sold_date_sk)) && ('i_item_sk = 'ws_item_sk))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation `web_sales`
               :  +- 'SubqueryAlias `d1`
               :     +- 'UnresolvedRelation `date_dim`
               +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
total_sum: double, i_category: string, i_class: string, lochierarchy: tinyint, rank_within_parent: int
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (cast(lochierarchy#1 as int) = 0) THEN i_category#96 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [total_sum#0, i_category#96, i_class#97, lochierarchy#1, rank_within_parent#2]
         +- Project [total_sum#0, i_category#96, i_class#97, lochierarchy#1, _w0#106, _w1#110, _w2#111, _w3#112, rank_within_parent#2, rank_within_parent#2]
            +- Window [rank(_w3#112) windowspecdefinition(_w1#110, _w2#111, _w3#112 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#110, _w2#111], [_w3#112 DESC NULLS LAST]
               +- Aggregate [i_category#96, i_class#97, spark_grouping_id#93], [sum(ws_net_paid#36) AS total_sum#0, i_category#96, i_class#97, (cast((shiftright(spark_grouping_id#93, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#93, 0) & 1) as tinyint)) AS lochierarchy#1, sum(ws_net_paid#36) AS _w0#106, (cast((shiftright(spark_grouping_id#93, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#93, 0) & 1) as tinyint)) AS _w1#110, CASE WHEN (cast(cast((shiftright(spark_grouping_id#93, 0) & 1) as tinyint) as int) = 0) THEN i_category#96 END AS _w2#111, sum(ws_net_paid#36) AS _w3#112]
                  +- Expand [List(ws_sold_date_sk#7, ws_sold_time_sk#8, ws_ship_date_sk#9, ws_item_sk#10, ws_bill_customer_sk#11, ws_bill_cdemo_sk#12, ws_bill_hdemo_sk#13, ws_bill_addr_sk#14, ws_ship_customer_sk#15, ws_ship_cdemo_sk#16, ws_ship_hdemo_sk#17, ws_ship_addr_sk#18, ws_web_page_sk#19, ws_web_site_sk#20, ws_ship_mode_sk#21, ws_warehouse_sk#22, ws_promo_sk#23, ws_order_number#24, ws_quantity#25, ws_wholesale_cost#26, ws_list_price#27, ws_sales_price#28, ws_ext_discount_amt#29, ws_ext_sales_price#30, ws_ext_wholesale_cost#31, ws_ext_list_price#32, ws_ext_tax#33, ws_coupon_amt#34, ws_ext_ship_cost#35, ws_net_paid#36, ws_net_paid_inc_tax#37, ws_net_paid_inc_ship#38, ws_net_paid_inc_ship_tax#39, ws_net_profit#40, d_date_sk#41, d_date_id#42, d_date#43, d_month_seq#44, d_week_seq#45, d_quarter_seq#46, d_year#47, d_dow#48, d_moy#49, d_dom#50, d_qoy#51, d_fy_year#52, d_fy_quarter_seq#53, d_fy_week_seq#54, d_day_name#55, d_quarter_name#56, d_holiday#57, d_weekend#58, d_following_holiday#59, d_first_dom#60, d_last_dom#61, d_same_day_ly#62, d_same_day_lq#63, d_current_day#64, d_current_week#65, d_current_month#66, d_current_quarter#67, d_current_year#68, i_item_sk#69, i_item_id#70, i_rec_start_date#71, i_rec_end_date#72, i_item_desc#73, i_current_price#74, i_wholesale_cost#75, i_brand_id#76, i_brand#77, i_class_id#78, i_class#79, i_category_id#80, i_category#81, i_manufact_id#82, i_manufact#83, i_size#84, i_formulation#85, i_color#86, i_units#87, i_container#88, i_manager_id#89, i_product_name#90, i_category#94, i_class#95, 0), List(ws_sold_date_sk#7, ws_sold_time_sk#8, ws_ship_date_sk#9, ws_item_sk#10, ws_bill_customer_sk#11, ws_bill_cdemo_sk#12, ws_bill_hdemo_sk#13, ws_bill_addr_sk#14, ws_ship_customer_sk#15, ws_ship_cdemo_sk#16, ws_ship_hdemo_sk#17, ws_ship_addr_sk#18, ws_web_page_sk#19, ws_web_site_sk#20, ws_ship_mode_sk#21, ws_warehouse_sk#22, ws_promo_sk#23, ws_order_number#24, ws_quantity#25, ws_wholesale_cost#26, ws_list_price#27, ws_sales_price#28, ws_ext_discount_amt#29, ws_ext_sales_price#30, ws_ext_wholesale_cost#31, ws_ext_list_price#32, ws_ext_tax#33, ws_coupon_amt#34, ws_ext_ship_cost#35, ws_net_paid#36, ws_net_paid_inc_tax#37, ws_net_paid_inc_ship#38, ws_net_paid_inc_ship_tax#39, ws_net_profit#40, d_date_sk#41, d_date_id#42, d_date#43, d_month_seq#44, d_week_seq#45, d_quarter_seq#46, d_year#47, d_dow#48, d_moy#49, d_dom#50, d_qoy#51, d_fy_year#52, d_fy_quarter_seq#53, d_fy_week_seq#54, d_day_name#55, d_quarter_name#56, d_holiday#57, d_weekend#58, d_following_holiday#59, d_first_dom#60, d_last_dom#61, d_same_day_ly#62, d_same_day_lq#63, d_current_day#64, d_current_week#65, d_current_month#66, d_current_quarter#67, d_current_year#68, i_item_sk#69, i_item_id#70, i_rec_start_date#71, i_rec_end_date#72, i_item_desc#73, i_current_price#74, i_wholesale_cost#75, i_brand_id#76, i_brand#77, i_class_id#78, i_class#79, i_category_id#80, i_category#81, i_manufact_id#82, i_manufact#83, i_size#84, i_formulation#85, i_color#86, i_units#87, i_container#88, i_manager_id#89, i_product_name#90, i_category#94, null, 1), List(ws_sold_date_sk#7, ws_sold_time_sk#8, ws_ship_date_sk#9, ws_item_sk#10, ws_bill_customer_sk#11, ws_bill_cdemo_sk#12, ws_bill_hdemo_sk#13, ws_bill_addr_sk#14, ws_ship_customer_sk#15, ws_ship_cdemo_sk#16, ws_ship_hdemo_sk#17, ws_ship_addr_sk#18, ws_web_page_sk#19, ws_web_site_sk#20, ws_ship_mode_sk#21, ws_warehouse_sk#22, ws_promo_sk#23, ws_order_number#24, ws_quantity#25, ws_wholesale_cost#26, ws_list_price#27, ws_sales_price#28, ws_ext_discount_amt#29, ws_ext_sales_price#30, ws_ext_wholesale_cost#31, ws_ext_list_price#32, ws_ext_tax#33, ws_coupon_amt#34, ws_ext_ship_cost#35, ws_net_paid#36, ws_net_paid_inc_tax#37, ws_net_paid_inc_ship#38, ws_net_paid_inc_ship_tax#39, ws_net_profit#40, d_date_sk#41, d_date_id#42, d_date#43, d_month_seq#44, d_week_seq#45, d_quarter_seq#46, d_year#47, d_dow#48, d_moy#49, d_dom#50, d_qoy#51, d_fy_year#52, d_fy_quarter_seq#53, d_fy_week_seq#54, d_day_name#55, d_quarter_name#56, d_holiday#57, d_weekend#58, d_following_holiday#59, d_first_dom#60, d_last_dom#61, d_same_day_ly#62, d_same_day_lq#63, d_current_day#64, d_current_week#65, d_current_month#66, d_current_quarter#67, d_current_year#68, i_item_sk#69, i_item_id#70, i_rec_start_date#71, i_rec_end_date#72, i_item_desc#73, i_current_price#74, i_wholesale_cost#75, i_brand_id#76, i_brand#77, i_class_id#78, i_class#79, i_category_id#80, i_category#81, i_manufact_id#82, i_manufact#83, i_size#84, i_formulation#85, i_color#86, i_units#87, i_container#88, i_manager_id#89, i_product_name#90, null, null, 3)], [ws_sold_date_sk#7, ws_sold_time_sk#8, ws_ship_date_sk#9, ws_item_sk#10, ws_bill_customer_sk#11, ws_bill_cdemo_sk#12, ws_bill_hdemo_sk#13, ws_bill_addr_sk#14, ws_ship_customer_sk#15, ws_ship_cdemo_sk#16, ws_ship_hdemo_sk#17, ws_ship_addr_sk#18, ws_web_page_sk#19, ws_web_site_sk#20, ws_ship_mode_sk#21, ws_warehouse_sk#22, ws_promo_sk#23, ws_order_number#24, ws_quantity#25, ws_wholesale_cost#26, ws_list_price#27, ws_sales_price#28, ws_ext_discount_amt#29, ws_ext_sales_price#30, ... 63 more fields]
                     +- Project [ws_sold_date_sk#7, ws_sold_time_sk#8, ws_ship_date_sk#9, ws_item_sk#10, ws_bill_customer_sk#11, ws_bill_cdemo_sk#12, ws_bill_hdemo_sk#13, ws_bill_addr_sk#14, ws_ship_customer_sk#15, ws_ship_cdemo_sk#16, ws_ship_hdemo_sk#17, ws_ship_addr_sk#18, ws_web_page_sk#19, ws_web_site_sk#20, ws_ship_mode_sk#21, ws_warehouse_sk#22, ws_promo_sk#23, ws_order_number#24, ws_quantity#25, ws_wholesale_cost#26, ws_list_price#27, ws_sales_price#28, ws_ext_discount_amt#29, ws_ext_sales_price#30, ... 62 more fields]
                        +- Filter ((((d_month_seq#44 >= 1200) && (d_month_seq#44 <= (1200 + 11))) && (d_date_sk#41 = ws_sold_date_sk#7)) && (i_item_sk#69 = ws_item_sk#10))
                           +- Join Inner
                              :- Join Inner
                              :  :- SubqueryAlias `tpcds`.`web_sales`
                              :  :  +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
                              :  +- SubqueryAlias `d1`
                              :     +- SubqueryAlias `tpcds`.`date_dim`
                              :        +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
                              +- SubqueryAlias `tpcds`.`item`
                                 +- Relation[i_item_sk#69,i_item_id#70,i_rec_start_date#71,i_rec_end_date#72,i_item_desc#73,i_current_price#74,i_wholesale_cost#75,i_brand_id#76,i_brand#77,i_class_id#78,i_class#79,i_category_id#80,i_category#81,i_manufact_id#82,i_manufact#83,i_size#84,i_formulation#85,i_color#86,i_units#87,i_container#88,i_manager_id#89,i_product_name#90] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (cast(lochierarchy#1 as int) = 0) THEN i_category#96 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [total_sum#0, i_category#96, i_class#97, lochierarchy#1, rank_within_parent#2]
         +- Window [rank(_w3#112) windowspecdefinition(_w1#110, _w2#111, _w3#112 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#110, _w2#111], [_w3#112 DESC NULLS LAST]
            +- Aggregate [i_category#96, i_class#97, spark_grouping_id#93], [sum(ws_net_paid#36) AS total_sum#0, i_category#96, i_class#97, (cast((shiftright(spark_grouping_id#93, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#93, 0) & 1) as tinyint)) AS lochierarchy#1, (cast((shiftright(spark_grouping_id#93, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#93, 0) & 1) as tinyint)) AS _w1#110, CASE WHEN (cast(cast((shiftright(spark_grouping_id#93, 0) & 1) as tinyint) as int) = 0) THEN i_category#96 END AS _w2#111, sum(ws_net_paid#36) AS _w3#112]
               +- Expand [List(ws_net_paid#36, i_category#81, i_class#79, 0), List(ws_net_paid#36, i_category#81, null, 1), List(ws_net_paid#36, null, null, 3)], [ws_net_paid#36, i_category#96, i_class#97, spark_grouping_id#93]
                  +- Project [ws_net_paid#36, i_category#81, i_class#79]
                     +- Join Inner, (i_item_sk#69 = ws_item_sk#10)
                        :- Project [ws_item_sk#10, ws_net_paid#36]
                        :  +- Join Inner, (d_date_sk#41 = ws_sold_date_sk#7)
                        :     :- Project [ws_sold_date_sk#7, ws_item_sk#10, ws_net_paid#36]
                        :     :  +- Filter (isnotnull(ws_sold_date_sk#7) && isnotnull(ws_item_sk#10))
                        :     :     +- Relation[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
                        :     +- Project [d_date_sk#41]
                        :        +- Filter (((isnotnull(d_month_seq#44) && (d_month_seq#44 >= 1200)) && (d_month_seq#44 <= 1211)) && isnotnull(d_date_sk#41))
                        :           +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
                        +- Project [i_item_sk#69, i_class#79, i_category#81]
                           +- Filter isnotnull(i_item_sk#69)
                              +- Relation[i_item_sk#69,i_item_id#70,i_rec_start_date#71,i_rec_end_date#72,i_item_desc#73,i_current_price#74,i_wholesale_cost#75,i_brand_id#76,i_brand#77,i_class_id#78,i_class#79,i_category_id#80,i_category#81,i_manufact_id#82,i_manufact#83,i_size#84,i_formulation#85,i_color#86,i_units#87,i_container#88,i_manager_id#89,i_product_name#90] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[lochierarchy#1 DESC NULLS LAST,CASE WHEN (cast(lochierarchy#1 as int) = 0) THEN i_category#96 END ASC NULLS FIRST,rank_within_parent#2 ASC NULLS FIRST], output=[total_sum#0,i_category#96,i_class#97,lochierarchy#1,rank_within_parent#2])
+- *(6) Project [total_sum#0, i_category#96, i_class#97, lochierarchy#1, rank_within_parent#2]
   +- Window [rank(_w3#112) windowspecdefinition(_w1#110, _w2#111, _w3#112 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#110, _w2#111], [_w3#112 DESC NULLS LAST]
      +- *(5) Sort [_w1#110 ASC NULLS FIRST, _w2#111 ASC NULLS FIRST, _w3#112 DESC NULLS LAST], false, 0
         +- Exchange hashpartitioning(_w1#110, _w2#111, 200)
            +- *(4) HashAggregate(keys=[i_category#96, i_class#97, spark_grouping_id#93], functions=[sum(ws_net_paid#36)], output=[total_sum#0, i_category#96, i_class#97, lochierarchy#1, _w1#110, _w2#111, _w3#112])
               +- Exchange hashpartitioning(i_category#96, i_class#97, spark_grouping_id#93, 200)
                  +- *(3) HashAggregate(keys=[i_category#96, i_class#97, spark_grouping_id#93], functions=[partial_sum(ws_net_paid#36)], output=[i_category#96, i_class#97, spark_grouping_id#93, sum#123])
                     +- *(3) Expand [List(ws_net_paid#36, i_category#81, i_class#79, 0), List(ws_net_paid#36, i_category#81, null, 1), List(ws_net_paid#36, null, null, 3)], [ws_net_paid#36, i_category#96, i_class#97, spark_grouping_id#93]
                        +- *(3) Project [ws_net_paid#36, i_category#81, i_class#79]
                           +- *(3) BroadcastHashJoin [ws_item_sk#10], [i_item_sk#69], Inner, BuildRight
                              :- *(3) Project [ws_item_sk#10, ws_net_paid#36]
                              :  +- *(3) BroadcastHashJoin [ws_sold_date_sk#7], [d_date_sk#41], Inner, BuildRight
                              :     :- *(3) Project [ws_sold_date_sk#7, ws_item_sk#10, ws_net_paid#36]
                              :     :  +- *(3) Filter (isnotnull(ws_sold_date_sk#7) && isnotnull(ws_item_sk#10))
                              :     :     +- *(3) FileScan parquet tpcds.web_sales[ws_sold_date_sk#7,ws_item_sk#10,ws_net_paid#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_item_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_net_paid:double>
                              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              :        +- *(1) Project [d_date_sk#41]
                              :           +- *(1) Filter (((isnotnull(d_month_seq#44) && (d_month_seq#44 >= 1200)) && (d_month_seq#44 <= 1211)) && isnotnull(d_date_sk#41))
                              :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#41,d_month_seq#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 +- *(2) Project [i_item_sk#69, i_class#79, i_category#81]
                                    +- *(2) Filter isnotnull(i_item_sk#69)
                                       +- *(2) FileScan parquet tpcds.item[i_item_sk#69,i_class#79,i_category#81] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_class:string,i_category:string>
Time taken: 3.758 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 86 in stream 0 using template query86.tpl
------------------------------------------------------^^^

