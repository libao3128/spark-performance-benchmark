Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582909676
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['lochierarchy DESC NULLS LAST, CASE WHEN ('lochierarchy = 0) THEN 'i_category END ASC NULLS FIRST, 'rank_within_parent ASC NULLS FIRST], true
      +- 'Aggregate [rollup(Vector(0), Vector(1), 'i_category, 'i_class)], ['sum('ws_net_paid) AS total_sum#0, 'i_category, 'i_class, ('grouping('i_category) + 'grouping('i_class)) AS lochierarchy#1, 'rank() windowspecdefinition(('grouping('i_category) + 'grouping('i_class)), CASE WHEN ('grouping('i_class) = 0) THEN 'i_category END, 'sum('ws_net_paid) DESC NULLS LAST, unspecifiedframe$()) AS rank_within_parent#2]
         +- 'Filter (((('d1.d_month_seq >= 1200) AND ('d1.d_month_seq <= (1200 + 11))) AND ('d1.d_date_sk = 'ws_sold_date_sk)) AND ('i_item_sk = 'ws_item_sk))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation [web_sales], [], false
               :  +- 'SubqueryAlias d1
               :     +- 'UnresolvedRelation [date_dim], [], false
               +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
total_sum: double, i_category: string, i_class: string, lochierarchy: tinyint, rank_within_parent: int
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (cast(lochierarchy#1 as int) = 0) THEN i_category#102 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [total_sum#0, i_category#102, i_class#103, lochierarchy#1, rank_within_parent#2]
         +- Project [total_sum#0, i_category#102, i_class#103, lochierarchy#1, _w0#112, _w1#116, _w2#117, rank_within_parent#2, rank_within_parent#2]
            +- Window [rank(_w0#112) windowspecdefinition(_w1#116, _w2#117, _w0#112 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#116, _w2#117], [_w0#112 DESC NULLS LAST]
               +- Aggregate [i_category#102, i_class#103, spark_grouping_id#101L], [sum(ws_net_paid#39) AS total_sum#0, i_category#102, i_class#103, (cast((shiftright(spark_grouping_id#101L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#101L, 0) & 1) as tinyint)) AS lochierarchy#1, sum(ws_net_paid#39) AS _w0#112, (cast((shiftright(spark_grouping_id#101L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#101L, 0) & 1) as tinyint)) AS _w1#116, CASE WHEN (cast(cast((shiftright(spark_grouping_id#101L, 0) & 1) as tinyint) as int) = 0) THEN i_category#102 END AS _w2#117]
                  +- Expand [[ws_sold_date_sk#10, ws_sold_time_sk#11, ws_ship_date_sk#12, ws_item_sk#13, ws_bill_customer_sk#14, ws_bill_cdemo_sk#15, ws_bill_hdemo_sk#16, ws_bill_addr_sk#17, ws_ship_customer_sk#18, ws_ship_cdemo_sk#19, ws_ship_hdemo_sk#20, ws_ship_addr_sk#21, ws_web_page_sk#22, ws_web_site_sk#23, ws_ship_mode_sk#24, ws_warehouse_sk#25, ws_promo_sk#26, ws_order_number#27, ws_quantity#28, ws_wholesale_cost#29, ws_list_price#30, ws_sales_price#31, ws_ext_discount_amt#32, ws_ext_sales_price#33, ... 63 more fields], [ws_sold_date_sk#10, ws_sold_time_sk#11, ws_ship_date_sk#12, ws_item_sk#13, ws_bill_customer_sk#14, ws_bill_cdemo_sk#15, ws_bill_hdemo_sk#16, ws_bill_addr_sk#17, ws_ship_customer_sk#18, ws_ship_cdemo_sk#19, ws_ship_hdemo_sk#20, ws_ship_addr_sk#21, ws_web_page_sk#22, ws_web_site_sk#23, ws_ship_mode_sk#24, ws_warehouse_sk#25, ws_promo_sk#26, ws_order_number#27, ws_quantity#28, ws_wholesale_cost#29, ws_list_price#30, ws_sales_price#31, ws_ext_discount_amt#32, ws_ext_sales_price#33, ... 63 more fields], [ws_sold_date_sk#10, ws_sold_time_sk#11, ws_ship_date_sk#12, ws_item_sk#13, ws_bill_customer_sk#14, ws_bill_cdemo_sk#15, ws_bill_hdemo_sk#16, ws_bill_addr_sk#17, ws_ship_customer_sk#18, ws_ship_cdemo_sk#19, ws_ship_hdemo_sk#20, ws_ship_addr_sk#21, ws_web_page_sk#22, ws_web_site_sk#23, ws_ship_mode_sk#24, ws_warehouse_sk#25, ws_promo_sk#26, ws_order_number#27, ws_quantity#28, ws_wholesale_cost#29, ws_list_price#30, ws_sales_price#31, ws_ext_discount_amt#32, ws_ext_sales_price#33, ... 63 more fields]], [ws_sold_date_sk#10, ws_sold_time_sk#11, ws_ship_date_sk#12, ws_item_sk#13, ws_bill_customer_sk#14, ws_bill_cdemo_sk#15, ws_bill_hdemo_sk#16, ws_bill_addr_sk#17, ws_ship_customer_sk#18, ws_ship_cdemo_sk#19, ws_ship_hdemo_sk#20, ws_ship_addr_sk#21, ws_web_page_sk#22, ws_web_site_sk#23, ws_ship_mode_sk#24, ws_warehouse_sk#25, ws_promo_sk#26, ws_order_number#27, ws_quantity#28, ws_wholesale_cost#29, ws_list_price#30, ws_sales_price#31, ws_ext_discount_amt#32, ws_ext_sales_price#33, ... 63 more fields]
                     +- Project [ws_sold_date_sk#10, ws_sold_time_sk#11, ws_ship_date_sk#12, ws_item_sk#13, ws_bill_customer_sk#14, ws_bill_cdemo_sk#15, ws_bill_hdemo_sk#16, ws_bill_addr_sk#17, ws_ship_customer_sk#18, ws_ship_cdemo_sk#19, ws_ship_hdemo_sk#20, ws_ship_addr_sk#21, ws_web_page_sk#22, ws_web_site_sk#23, ws_ship_mode_sk#24, ws_warehouse_sk#25, ws_promo_sk#26, ws_order_number#27, ws_quantity#28, ws_wholesale_cost#29, ws_list_price#30, ws_sales_price#31, ws_ext_discount_amt#32, ws_ext_sales_price#33, ... 62 more fields]
                        +- Filter ((((d_month_seq#47 >= 1200) AND (d_month_seq#47 <= (1200 + 11))) AND (d_date_sk#44 = ws_sold_date_sk#10)) AND (i_item_sk#72 = ws_item_sk#13))
                           +- Join Inner
                              :- Join Inner
                              :  :- SubqueryAlias spark_catalog.tpcds.web_sales
                              :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_sold_time_sk#11,ws_ship_date_sk#12,ws_item_sk#13,ws_bill_customer_sk#14,ws_bill_cdemo_sk#15,ws_bill_hdemo_sk#16,ws_bill_addr_sk#17,ws_ship_customer_sk#18,ws_ship_cdemo_sk#19,ws_ship_hdemo_sk#20,ws_ship_addr_sk#21,ws_web_page_sk#22,ws_web_site_sk#23,ws_ship_mode_sk#24,ws_warehouse_sk#25,ws_promo_sk#26,ws_order_number#27,ws_quantity#28,ws_wholesale_cost#29,ws_list_price#30,ws_sales_price#31,ws_ext_discount_amt#32,ws_ext_sales_price#33,... 10 more fields] parquet
                              :  +- SubqueryAlias d1
                              :     +- SubqueryAlias spark_catalog.tpcds.date_dim
                              :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet
                              +- SubqueryAlias spark_catalog.tpcds.item
                                 +- Relation spark_catalog.tpcds.item[i_item_sk#72,i_item_id#73,i_rec_start_date#74,i_rec_end_date#75,i_item_desc#76,i_current_price#77,i_wholesale_cost#78,i_brand_id#79,i_brand#80,i_class_id#81,i_class#82,i_category_id#83,i_category#84,i_manufact_id#85,i_manufact#86,i_size#87,i_formulation#88,i_color#89,i_units#90,i_container#91,i_manager_id#92,i_product_name#93] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (lochierarchy#1 = 0) THEN i_category#102 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [total_sum#0, i_category#102, i_class#103, lochierarchy#1, rank_within_parent#2]
         +- Window [rank(_w0#112) windowspecdefinition(_w1#116, _w2#117, _w0#112 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#116, _w2#117], [_w0#112 DESC NULLS LAST]
            +- Aggregate [i_category#102, i_class#103, spark_grouping_id#101L], [sum(ws_net_paid#39) AS total_sum#0, i_category#102, i_class#103, (cast((shiftright(spark_grouping_id#101L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#101L, 0) & 1) as tinyint)) AS lochierarchy#1, sum(ws_net_paid#39) AS _w0#112, (cast((shiftright(spark_grouping_id#101L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#101L, 0) & 1) as tinyint)) AS _w1#116, CASE WHEN (cast((shiftright(spark_grouping_id#101L, 0) & 1) as tinyint) = 0) THEN i_category#102 END AS _w2#117]
               +- Expand [[ws_net_paid#39, i_category#84, i_class#82, 0], [ws_net_paid#39, i_category#84, null, 1], [ws_net_paid#39, null, null, 3]], [ws_net_paid#39, i_category#102, i_class#103, spark_grouping_id#101L]
                  +- Project [ws_net_paid#39, i_category#84, i_class#82]
                     +- Join Inner, (i_item_sk#72 = ws_item_sk#13)
                        :- Project [ws_item_sk#13, ws_net_paid#39]
                        :  +- Join Inner, (d_date_sk#44 = ws_sold_date_sk#10)
                        :     :- Project [ws_sold_date_sk#10, ws_item_sk#13, ws_net_paid#39]
                        :     :  +- Filter (isnotnull(ws_sold_date_sk#10) AND isnotnull(ws_item_sk#13))
                        :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_sold_time_sk#11,ws_ship_date_sk#12,ws_item_sk#13,ws_bill_customer_sk#14,ws_bill_cdemo_sk#15,ws_bill_hdemo_sk#16,ws_bill_addr_sk#17,ws_ship_customer_sk#18,ws_ship_cdemo_sk#19,ws_ship_hdemo_sk#20,ws_ship_addr_sk#21,ws_web_page_sk#22,ws_web_site_sk#23,ws_ship_mode_sk#24,ws_warehouse_sk#25,ws_promo_sk#26,ws_order_number#27,ws_quantity#28,ws_wholesale_cost#29,ws_list_price#30,ws_sales_price#31,ws_ext_discount_amt#32,ws_ext_sales_price#33,... 10 more fields] parquet
                        :     +- Project [d_date_sk#44]
                        :        +- Filter ((isnotnull(d_month_seq#47) AND ((d_month_seq#47 >= 1200) AND (d_month_seq#47 <= 1211))) AND isnotnull(d_date_sk#44))
                        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet
                        +- Project [i_item_sk#72, i_class#82, i_category#84]
                           +- Filter isnotnull(i_item_sk#72)
                              +- Relation spark_catalog.tpcds.item[i_item_sk#72,i_item_id#73,i_rec_start_date#74,i_rec_end_date#75,i_item_desc#76,i_current_price#77,i_wholesale_cost#78,i_brand_id#79,i_brand#80,i_class_id#81,i_class#82,i_category_id#83,i_category#84,i_manufact_id#85,i_manufact#86,i_size#87,i_formulation#88,i_color#89,i_units#90,i_container#91,i_manager_id#92,i_product_name#93] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[lochierarchy#1 DESC NULLS LAST,CASE WHEN (lochierarchy#1 = 0) THEN i_category#102 END ASC NULLS FIRST,rank_within_parent#2 ASC NULLS FIRST], output=[total_sum#0,i_category#102,i_class#103,lochierarchy#1,rank_within_parent#2])
   +- Project [total_sum#0, i_category#102, i_class#103, lochierarchy#1, rank_within_parent#2]
      +- Window [rank(_w0#112) windowspecdefinition(_w1#116, _w2#117, _w0#112 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#116, _w2#117], [_w0#112 DESC NULLS LAST]
         +- Sort [_w1#116 ASC NULLS FIRST, _w2#117 ASC NULLS FIRST, _w0#112 DESC NULLS LAST], false, 0
            +- Exchange hashpartitioning(_w1#116, _w2#117, 200), ENSURE_REQUIREMENTS, [plan_id=91]
               +- HashAggregate(keys=[i_category#102, i_class#103, spark_grouping_id#101L], functions=[sum(ws_net_paid#39)], output=[total_sum#0, i_category#102, i_class#103, lochierarchy#1, _w0#112, _w1#116, _w2#117])
                  +- Exchange hashpartitioning(i_category#102, i_class#103, spark_grouping_id#101L, 200), ENSURE_REQUIREMENTS, [plan_id=88]
                     +- HashAggregate(keys=[i_category#102, i_class#103, spark_grouping_id#101L], functions=[partial_sum(ws_net_paid#39)], output=[i_category#102, i_class#103, spark_grouping_id#101L, sum#122])
                        +- Expand [[ws_net_paid#39, i_category#84, i_class#82, 0], [ws_net_paid#39, i_category#84, null, 1], [ws_net_paid#39, null, null, 3]], [ws_net_paid#39, i_category#102, i_class#103, spark_grouping_id#101L]
                           +- Project [ws_net_paid#39, i_category#84, i_class#82]
                              +- BroadcastHashJoin [ws_item_sk#13], [i_item_sk#72], Inner, BuildRight, false
                                 :- Project [ws_item_sk#13, ws_net_paid#39]
                                 :  +- BroadcastHashJoin [ws_sold_date_sk#10], [d_date_sk#44], Inner, BuildRight, false
                                 :     :- Filter (isnotnull(ws_sold_date_sk#10) AND isnotnull(ws_item_sk#13))
                                 :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_item_sk#13,ws_net_paid#39] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#10), isnotnull(ws_item_sk#13)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_item_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_net_paid:double>
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=78]
                                 :        +- Project [d_date_sk#44]
                                 :           +- Filter (((isnotnull(d_month_seq#47) AND (d_month_seq#47 >= 1200)) AND (d_month_seq#47 <= 1211)) AND isnotnull(d_date_sk#44))
                                 :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#44,d_month_seq#47] Batched: true, DataFilters: [isnotnull(d_month_seq#47), (d_month_seq#47 >= 1200), (d_month_seq#47 <= 1211), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=82]
                                    +- Filter isnotnull(i_item_sk#72)
                                       +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#72,i_class#82,i_category#84] Batched: true, DataFilters: [isnotnull(i_item_sk#72)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_class:string,i_category:string>

Time taken: 3.165 seconds, Fetched 1 row(s)
