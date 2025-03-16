Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580934952
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['lochierarchy DESC NULLS LAST, CASE WHEN ('lochierarchy = 0) THEN 'i_category END ASC NULLS FIRST, 'rank_within_parent ASC NULLS FIRST], true
      +- 'Aggregate [rollup(Vector(0), Vector(1), 'i_category, 'i_class)], [('sum('ss_net_profit) / 'sum('ss_ext_sales_price)) AS gross_margin#0, 'i_category, 'i_class, ('grouping('i_category) + 'grouping('i_class)) AS lochierarchy#1, 'rank() windowspecdefinition(('grouping('i_category) + 'grouping('i_class)), CASE WHEN ('grouping('i_class) = 0) THEN 'i_category END, ('sum('ss_net_profit) / 'sum('ss_ext_sales_price)) ASC NULLS FIRST, unspecifiedframe$()) AS rank_within_parent#2]
         +- 'Filter (((('d1.d_year = 2001) AND ('d1.d_date_sk = 'ss_sold_date_sk)) AND ('i_item_sk = 'ss_item_sk)) AND (('s_store_sk = 'ss_store_sk) AND 's_state IN (TN,TN,TN,TN,TN,TN,TN,TN)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation [store_sales], [], false
               :  :  +- 'SubqueryAlias d1
               :  :     +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [item], [], false
               +- 'UnresolvedRelation [store], [], false

== Analyzed Logical Plan ==
gross_margin: double, i_category: string, i_class: string, lochierarchy: tinyint, rank_within_parent: int
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (cast(lochierarchy#1 as int) = 0) THEN i_category#123 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [gross_margin#0, i_category#123, i_class#124, lochierarchy#1, rank_within_parent#2]
         +- Project [gross_margin#0, i_category#123, i_class#124, lochierarchy#1, _w0#133, _w1#137, _w2#138, rank_within_parent#2, rank_within_parent#2]
            +- Window [rank(_w0#133) windowspecdefinition(_w1#137, _w2#138, _w0#133 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#137, _w2#138], [_w0#133 ASC NULLS FIRST]
               +- Aggregate [i_category#123, i_class#124, spark_grouping_id#122L], [(sum(ss_net_profit#32) / sum(ss_ext_sales_price#25)) AS gross_margin#0, i_category#123, i_class#124, (cast((shiftright(spark_grouping_id#122L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#122L, 0) & 1) as tinyint)) AS lochierarchy#1, (sum(ss_net_profit#32) / sum(ss_ext_sales_price#25)) AS _w0#133, (cast((shiftright(spark_grouping_id#122L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#122L, 0) & 1) as tinyint)) AS _w1#137, CASE WHEN (cast(cast((shiftright(spark_grouping_id#122L, 0) & 1) as tinyint) as int) = 0) THEN i_category#123 END AS _w2#138]
                  +- Expand [[ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, ... 81 more fields], [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, ... 81 more fields], [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, ... 81 more fields]], [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, ... 81 more fields]
                     +- Project [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, ... 80 more fields]
                        +- Filter ((((d_year#39 = 2001) AND (d_date_sk#33 = ss_sold_date_sk#10)) AND (i_item_sk#61 = ss_item_sk#12)) AND ((s_store_sk#83 = ss_store_sk#17) AND s_state#107 IN (TN,TN,TN,TN,TN,TN,TN,TN)))
                           +- Join Inner
                              :- Join Inner
                              :  :- Join Inner
                              :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
                              :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
                              :  :  +- SubqueryAlias d1
                              :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
                              :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
                              :  +- SubqueryAlias spark_catalog.tpcds.item
                              :     +- Relation spark_catalog.tpcds.item[i_item_sk#61,i_item_id#62,i_rec_start_date#63,i_rec_end_date#64,i_item_desc#65,i_current_price#66,i_wholesale_cost#67,i_brand_id#68,i_brand#69,i_class_id#70,i_class#71,i_category_id#72,i_category#73,i_manufact_id#74,i_manufact#75,i_size#76,i_formulation#77,i_color#78,i_units#79,i_container#80,i_manager_id#81,i_product_name#82] parquet
                              +- SubqueryAlias spark_catalog.tpcds.store
                                 +- Relation spark_catalog.tpcds.store[s_store_sk#83,s_store_id#84,s_rec_start_date#85,s_rec_end_date#86,s_closed_date_sk#87,s_store_name#88,s_number_employees#89,s_floor_space#90,s_hours#91,s_manager#92,s_market_id#93,s_geography_class#94,s_market_desc#95,s_market_manager#96,s_division_id#97,s_division_name#98,s_company_id#99,s_company_name#100,s_street_number#101,s_street_name#102,s_street_type#103,s_suite_number#104,s_city#105,s_county#106,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (lochierarchy#1 = 0) THEN i_category#123 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [gross_margin#0, i_category#123, i_class#124, lochierarchy#1, rank_within_parent#2]
         +- Window [rank(_w0#133) windowspecdefinition(_w1#137, _w2#138, _w0#133 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#137, _w2#138], [_w0#133 ASC NULLS FIRST]
            +- Aggregate [i_category#123, i_class#124, spark_grouping_id#122L], [(sum(ss_net_profit#32) / sum(ss_ext_sales_price#25)) AS gross_margin#0, i_category#123, i_class#124, (cast((shiftright(spark_grouping_id#122L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#122L, 0) & 1) as tinyint)) AS lochierarchy#1, (sum(ss_net_profit#32) / sum(ss_ext_sales_price#25)) AS _w0#133, (cast((shiftright(spark_grouping_id#122L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#122L, 0) & 1) as tinyint)) AS _w1#137, CASE WHEN (cast((shiftright(spark_grouping_id#122L, 0) & 1) as tinyint) = 0) THEN i_category#123 END AS _w2#138]
               +- Expand [[ss_ext_sales_price#25, ss_net_profit#32, i_category#73, i_class#71, 0], [ss_ext_sales_price#25, ss_net_profit#32, i_category#73, null, 1], [ss_ext_sales_price#25, ss_net_profit#32, null, null, 3]], [ss_ext_sales_price#25, ss_net_profit#32, i_category#123, i_class#124, spark_grouping_id#122L]
                  +- Project [ss_ext_sales_price#25, ss_net_profit#32, i_category#73, i_class#71]
                     +- Join Inner, (s_store_sk#83 = ss_store_sk#17)
                        :- Project [ss_store_sk#17, ss_ext_sales_price#25, ss_net_profit#32, i_class#71, i_category#73]
                        :  +- Join Inner, (i_item_sk#61 = ss_item_sk#12)
                        :     :- Project [ss_item_sk#12, ss_store_sk#17, ss_ext_sales_price#25, ss_net_profit#32]
                        :     :  +- Join Inner, (d_date_sk#33 = ss_sold_date_sk#10)
                        :     :     :- Project [ss_sold_date_sk#10, ss_item_sk#12, ss_store_sk#17, ss_ext_sales_price#25, ss_net_profit#32]
                        :     :     :  +- Filter (isnotnull(ss_sold_date_sk#10) AND (isnotnull(ss_item_sk#12) AND isnotnull(ss_store_sk#17)))
                        :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
                        :     :     +- Project [d_date_sk#33]
                        :     :        +- Filter ((isnotnull(d_year#39) AND (d_year#39 = 2001)) AND isnotnull(d_date_sk#33))
                        :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
                        :     +- Project [i_item_sk#61, i_class#71, i_category#73]
                        :        +- Filter isnotnull(i_item_sk#61)
                        :           +- Relation spark_catalog.tpcds.item[i_item_sk#61,i_item_id#62,i_rec_start_date#63,i_rec_end_date#64,i_item_desc#65,i_current_price#66,i_wholesale_cost#67,i_brand_id#68,i_brand#69,i_class_id#70,i_class#71,i_category_id#72,i_category#73,i_manufact_id#74,i_manufact#75,i_size#76,i_formulation#77,i_color#78,i_units#79,i_container#80,i_manager_id#81,i_product_name#82] parquet
                        +- Project [s_store_sk#83]
                           +- Filter ((isnotnull(s_state#107) AND (s_state#107 = TN)) AND isnotnull(s_store_sk#83))
                              +- Relation spark_catalog.tpcds.store[s_store_sk#83,s_store_id#84,s_rec_start_date#85,s_rec_end_date#86,s_closed_date_sk#87,s_store_name#88,s_number_employees#89,s_floor_space#90,s_hours#91,s_manager#92,s_market_id#93,s_geography_class#94,s_market_desc#95,s_market_manager#96,s_division_id#97,s_division_name#98,s_company_id#99,s_company_name#100,s_street_number#101,s_street_name#102,s_street_type#103,s_suite_number#104,s_city#105,s_county#106,... 5 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[lochierarchy#1 DESC NULLS LAST,CASE WHEN (lochierarchy#1 = 0) THEN i_category#123 END ASC NULLS FIRST,rank_within_parent#2 ASC NULLS FIRST], output=[gross_margin#0,i_category#123,i_class#124,lochierarchy#1,rank_within_parent#2])
   +- Project [gross_margin#0, i_category#123, i_class#124, lochierarchy#1, rank_within_parent#2]
      +- Window [rank(_w0#133) windowspecdefinition(_w1#137, _w2#138, _w0#133 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#137, _w2#138], [_w0#133 ASC NULLS FIRST]
         +- Sort [_w1#137 ASC NULLS FIRST, _w2#138 ASC NULLS FIRST, _w0#133 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(_w1#137, _w2#138, 200), ENSURE_REQUIREMENTS, [plan_id=113]
               +- HashAggregate(keys=[i_category#123, i_class#124, spark_grouping_id#122L], functions=[sum(ss_net_profit#32), sum(ss_ext_sales_price#25)], output=[gross_margin#0, i_category#123, i_class#124, lochierarchy#1, _w0#133, _w1#137, _w2#138])
                  +- Exchange hashpartitioning(i_category#123, i_class#124, spark_grouping_id#122L, 200), ENSURE_REQUIREMENTS, [plan_id=110]
                     +- HashAggregate(keys=[i_category#123, i_class#124, spark_grouping_id#122L], functions=[partial_sum(ss_net_profit#32), partial_sum(ss_ext_sales_price#25)], output=[i_category#123, i_class#124, spark_grouping_id#122L, sum#144, sum#145])
                        +- Expand [[ss_ext_sales_price#25, ss_net_profit#32, i_category#73, i_class#71, 0], [ss_ext_sales_price#25, ss_net_profit#32, i_category#73, null, 1], [ss_ext_sales_price#25, ss_net_profit#32, null, null, 3]], [ss_ext_sales_price#25, ss_net_profit#32, i_category#123, i_class#124, spark_grouping_id#122L]
                           +- Project [ss_ext_sales_price#25, ss_net_profit#32, i_category#73, i_class#71]
                              +- BroadcastHashJoin [ss_store_sk#17], [s_store_sk#83], Inner, BuildRight, false
                                 :- Project [ss_store_sk#17, ss_ext_sales_price#25, ss_net_profit#32, i_class#71, i_category#73]
                                 :  +- BroadcastHashJoin [ss_item_sk#12], [i_item_sk#61], Inner, BuildRight, false
                                 :     :- Project [ss_item_sk#12, ss_store_sk#17, ss_ext_sales_price#25, ss_net_profit#32]
                                 :     :  +- BroadcastHashJoin [ss_sold_date_sk#10], [d_date_sk#33], Inner, BuildRight, false
                                 :     :     :- Filter ((isnotnull(ss_sold_date_sk#10) AND isnotnull(ss_item_sk#12)) AND isnotnull(ss_store_sk#17))
                                 :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#10,ss_item_sk#12,ss_store_sk#17,ss_ext_sales_price#25,ss_net_profit#32] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#10), isnotnull(ss_item_sk#12), isnotnull(ss_store_sk#17)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_ext_sales_price:double,ss_net_profit...
                                 :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=96]
                                 :     :        +- Project [d_date_sk#33]
                                 :     :           +- Filter ((isnotnull(d_year#39) AND (d_year#39 = 2001)) AND isnotnull(d_date_sk#33))
                                 :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#33,d_year#39] Batched: true, DataFilters: [isnotnull(d_year#39), (d_year#39 = 2001), isnotnull(d_date_sk#33)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=100]
                                 :        +- Filter isnotnull(i_item_sk#61)
                                 :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#61,i_class#71,i_category#73] Batched: true, DataFilters: [isnotnull(i_item_sk#61)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_class:string,i_category:string>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=104]
                                    +- Project [s_store_sk#83]
                                       +- Filter ((isnotnull(s_state#107) AND (s_state#107 = TN)) AND isnotnull(s_store_sk#83))
                                          +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#83,s_state#107] Batched: true, DataFilters: [isnotnull(s_state#107), (s_state#107 = TN), isnotnull(s_store_sk#83)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_state), EqualTo(s_state,TN), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_state:string>

Time taken: 2.867 seconds, Fetched 1 row(s)
