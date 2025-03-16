Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582246161
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['lochierarchy DESC NULLS LAST, CASE WHEN ('lochierarchy = 0) THEN 's_state END ASC NULLS FIRST, 'rank_within_parent ASC NULLS FIRST], true
      +- 'Aggregate [rollup(Vector(0), Vector(1), 's_state, 's_county)], ['sum('ss_net_profit) AS total_sum#0, 's_state, 's_county, ('grouping('s_state) + 'grouping('s_county)) AS lochierarchy#1, 'rank() windowspecdefinition(('grouping('s_state) + 'grouping('s_county)), CASE WHEN ('grouping('s_county) = 0) THEN 's_state END, 'sum('ss_net_profit) DESC NULLS LAST, unspecifiedframe$()) AS rank_within_parent#2]
         +- 'Filter (((('d1.d_month_seq >= 1200) AND ('d1.d_month_seq <= (1200 + 11))) AND ('d1.d_date_sk = 'ss_sold_date_sk)) AND (('s_store_sk = 'ss_store_sk) AND 's_state IN (list#5 [])))
            :  +- 'Project ['s_state]
            :     +- 'Filter ('ranking <= 5)
            :        +- 'SubqueryAlias tmp1
            :           +- 'Aggregate ['s_state], ['s_state AS s_state#3, 'rank() windowspecdefinition('s_state, 'sum('ss_net_profit) DESC NULLS LAST, unspecifiedframe$()) AS ranking#4]
            :              +- 'Filter (((('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11))) AND ('d_date_sk = 'ss_sold_date_sk)) AND ('s_store_sk = 'ss_store_sk))
            :                 +- 'Join Inner
            :                    :- 'Join Inner
            :                    :  :- 'UnresolvedRelation [store_sales], [], false
            :                    :  +- 'UnresolvedRelation [store], [], false
            :                    +- 'UnresolvedRelation [date_dim], [], false
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation [store_sales], [], false
               :  +- 'SubqueryAlias d1
               :     +- 'UnresolvedRelation [date_dim], [], false
               +- 'UnresolvedRelation [store], [], false

== Analyzed Logical Plan ==
total_sum: double, s_state: string, s_county: string, lochierarchy: tinyint, rank_within_parent: int
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (cast(lochierarchy#1 as int) = 0) THEN s_state#194 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [total_sum#0, s_state#194, s_county#195, lochierarchy#1, rank_within_parent#2]
         +- Project [total_sum#0, s_state#194, s_county#195, lochierarchy#1, _w0#204, _w1#208, _w2#209, rank_within_parent#2, rank_within_parent#2]
            +- Window [rank(_w0#204) windowspecdefinition(_w1#208, _w2#209, _w0#204 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#208, _w2#209], [_w0#204 DESC NULLS LAST]
               +- Aggregate [s_state#194, s_county#195, spark_grouping_id#193L], [sum(ss_net_profit#35) AS total_sum#0, s_state#194, s_county#195, (cast((shiftright(spark_grouping_id#193L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#193L, 0) & 1) as tinyint)) AS lochierarchy#1, sum(ss_net_profit#35) AS _w0#204, (cast((shiftright(spark_grouping_id#193L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#193L, 0) & 1) as tinyint)) AS _w1#208, CASE WHEN (cast(cast((shiftright(spark_grouping_id#193L, 0) & 1) as tinyint) as int) = 0) THEN s_state#194 END AS _w2#209]
                  +- Expand [[ss_sold_date_sk#13, ss_sold_time_sk#14, ss_item_sk#15, ss_customer_sk#16, ss_cdemo_sk#17, ss_hdemo_sk#18, ss_addr_sk#19, ss_store_sk#20, ss_promo_sk#21, ss_ticket_number#22, ss_quantity#23, ss_wholesale_cost#24, ss_list_price#25, ss_sales_price#26, ss_ext_discount_amt#27, ss_ext_sales_price#28, ss_ext_wholesale_cost#29, ss_ext_list_price#30, ss_ext_tax#31, ss_coupon_amt#32, ss_net_paid#33, ss_net_paid_inc_tax#34, ss_net_profit#35, d_date_sk#36, ... 59 more fields], [ss_sold_date_sk#13, ss_sold_time_sk#14, ss_item_sk#15, ss_customer_sk#16, ss_cdemo_sk#17, ss_hdemo_sk#18, ss_addr_sk#19, ss_store_sk#20, ss_promo_sk#21, ss_ticket_number#22, ss_quantity#23, ss_wholesale_cost#24, ss_list_price#25, ss_sales_price#26, ss_ext_discount_amt#27, ss_ext_sales_price#28, ss_ext_wholesale_cost#29, ss_ext_list_price#30, ss_ext_tax#31, ss_coupon_amt#32, ss_net_paid#33, ss_net_paid_inc_tax#34, ss_net_profit#35, d_date_sk#36, ... 59 more fields], [ss_sold_date_sk#13, ss_sold_time_sk#14, ss_item_sk#15, ss_customer_sk#16, ss_cdemo_sk#17, ss_hdemo_sk#18, ss_addr_sk#19, ss_store_sk#20, ss_promo_sk#21, ss_ticket_number#22, ss_quantity#23, ss_wholesale_cost#24, ss_list_price#25, ss_sales_price#26, ss_ext_discount_amt#27, ss_ext_sales_price#28, ss_ext_wholesale_cost#29, ss_ext_list_price#30, ss_ext_tax#31, ss_coupon_amt#32, ss_net_paid#33, ss_net_paid_inc_tax#34, ss_net_profit#35, d_date_sk#36, ... 59 more fields]], [ss_sold_date_sk#13, ss_sold_time_sk#14, ss_item_sk#15, ss_customer_sk#16, ss_cdemo_sk#17, ss_hdemo_sk#18, ss_addr_sk#19, ss_store_sk#20, ss_promo_sk#21, ss_ticket_number#22, ss_quantity#23, ss_wholesale_cost#24, ss_list_price#25, ss_sales_price#26, ss_ext_discount_amt#27, ss_ext_sales_price#28, ss_ext_wholesale_cost#29, ss_ext_list_price#30, ss_ext_tax#31, ss_coupon_amt#32, ss_net_paid#33, ss_net_paid_inc_tax#34, ss_net_profit#35, d_date_sk#36, ... 59 more fields]
                     +- Project [ss_sold_date_sk#13, ss_sold_time_sk#14, ss_item_sk#15, ss_customer_sk#16, ss_cdemo_sk#17, ss_hdemo_sk#18, ss_addr_sk#19, ss_store_sk#20, ss_promo_sk#21, ss_ticket_number#22, ss_quantity#23, ss_wholesale_cost#24, ss_list_price#25, ss_sales_price#26, ss_ext_discount_amt#27, ss_ext_sales_price#28, ss_ext_wholesale_cost#29, ss_ext_list_price#30, ss_ext_tax#31, ss_coupon_amt#32, ss_net_paid#33, ss_net_paid_inc_tax#34, ss_net_profit#35, d_date_sk#36, ... 58 more fields]
                        +- Filter ((((d_month_seq#39 >= 1200) AND (d_month_seq#39 <= (1200 + 11))) AND (d_date_sk#36 = ss_sold_date_sk#13)) AND ((s_store_sk#64 = ss_store_sk#20) AND s_state#88 IN (list#5 [])))
                           :  +- Project [s_state#3]
                           :     +- Filter (ranking#4 <= 5)
                           :        +- SubqueryAlias tmp1
                           :           +- Project [s_state#3, ranking#4]
                           :              +- Project [s_state#3, _w0#99, s_state#156, ranking#4, ranking#4]
                           :                 +- Window [rank(_w0#99) windowspecdefinition(s_state#156, _w0#99 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS ranking#4], [s_state#156], [_w0#99 DESC NULLS LAST]
                           :                    +- Aggregate [s_state#156], [s_state#156 AS s_state#3, sum(ss_net_profit#131) AS _w0#99, s_state#156]
                           :                       +- Filter ((((d_month_seq#164 >= 1200) AND (d_month_seq#164 <= (1200 + 11))) AND (d_date_sk#161 = ss_sold_date_sk#109)) AND (s_store_sk#132 = ss_store_sk#116))
                           :                          +- Join Inner
                           :                             :- Join Inner
                           :                             :  :- SubqueryAlias spark_catalog.tpcds.store_sales
                           :                             :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#109,ss_sold_time_sk#110,ss_item_sk#111,ss_customer_sk#112,ss_cdemo_sk#113,ss_hdemo_sk#114,ss_addr_sk#115,ss_store_sk#116,ss_promo_sk#117,ss_ticket_number#118,ss_quantity#119,ss_wholesale_cost#120,ss_list_price#121,ss_sales_price#122,ss_ext_discount_amt#123,ss_ext_sales_price#124,ss_ext_wholesale_cost#125,ss_ext_list_price#126,ss_ext_tax#127,ss_coupon_amt#128,ss_net_paid#129,ss_net_paid_inc_tax#130,ss_net_profit#131] parquet
                           :                             :  +- SubqueryAlias spark_catalog.tpcds.store
                           :                             :     +- Relation spark_catalog.tpcds.store[s_store_sk#132,s_store_id#133,s_rec_start_date#134,s_rec_end_date#135,s_closed_date_sk#136,s_store_name#137,s_number_employees#138,s_floor_space#139,s_hours#140,s_manager#141,s_market_id#142,s_geography_class#143,s_market_desc#144,s_market_manager#145,s_division_id#146,s_division_name#147,s_company_id#148,s_company_name#149,s_street_number#150,s_street_name#151,s_street_type#152,s_suite_number#153,s_city#154,s_county#155,... 5 more fields] parquet
                           :                             +- SubqueryAlias spark_catalog.tpcds.date_dim
                           :                                +- Relation spark_catalog.tpcds.date_dim[d_date_sk#161,d_date_id#162,d_date#163,d_month_seq#164,d_week_seq#165,d_quarter_seq#166,d_year#167,d_dow#168,d_moy#169,d_dom#170,d_qoy#171,d_fy_year#172,d_fy_quarter_seq#173,d_fy_week_seq#174,d_day_name#175,d_quarter_name#176,d_holiday#177,d_weekend#178,d_following_holiday#179,d_first_dom#180,d_last_dom#181,d_same_day_ly#182,d_same_day_lq#183,d_current_day#184,... 4 more fields] parquet
                           +- Join Inner
                              :- Join Inner
                              :  :- SubqueryAlias spark_catalog.tpcds.store_sales
                              :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#13,ss_sold_time_sk#14,ss_item_sk#15,ss_customer_sk#16,ss_cdemo_sk#17,ss_hdemo_sk#18,ss_addr_sk#19,ss_store_sk#20,ss_promo_sk#21,ss_ticket_number#22,ss_quantity#23,ss_wholesale_cost#24,ss_list_price#25,ss_sales_price#26,ss_ext_discount_amt#27,ss_ext_sales_price#28,ss_ext_wholesale_cost#29,ss_ext_list_price#30,ss_ext_tax#31,ss_coupon_amt#32,ss_net_paid#33,ss_net_paid_inc_tax#34,ss_net_profit#35] parquet
                              :  +- SubqueryAlias d1
                              :     +- SubqueryAlias spark_catalog.tpcds.date_dim
                              :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#36,d_date_id#37,d_date#38,d_month_seq#39,d_week_seq#40,d_quarter_seq#41,d_year#42,d_dow#43,d_moy#44,d_dom#45,d_qoy#46,d_fy_year#47,d_fy_quarter_seq#48,d_fy_week_seq#49,d_day_name#50,d_quarter_name#51,d_holiday#52,d_weekend#53,d_following_holiday#54,d_first_dom#55,d_last_dom#56,d_same_day_ly#57,d_same_day_lq#58,d_current_day#59,... 4 more fields] parquet
                              +- SubqueryAlias spark_catalog.tpcds.store
                                 +- Relation spark_catalog.tpcds.store[s_store_sk#64,s_store_id#65,s_rec_start_date#66,s_rec_end_date#67,s_closed_date_sk#68,s_store_name#69,s_number_employees#70,s_floor_space#71,s_hours#72,s_manager#73,s_market_id#74,s_geography_class#75,s_market_desc#76,s_market_manager#77,s_division_id#78,s_division_name#79,s_company_id#80,s_company_name#81,s_street_number#82,s_street_name#83,s_street_type#84,s_suite_number#85,s_city#86,s_county#87,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (lochierarchy#1 = 0) THEN s_state#194 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [total_sum#0, s_state#194, s_county#195, lochierarchy#1, rank_within_parent#2]
         +- Window [rank(_w0#204) windowspecdefinition(_w1#208, _w2#209, _w0#204 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#208, _w2#209], [_w0#204 DESC NULLS LAST]
            +- Aggregate [s_state#194, s_county#195, spark_grouping_id#193L], [sum(ss_net_profit#35) AS total_sum#0, s_state#194, s_county#195, (cast((shiftright(spark_grouping_id#193L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#193L, 0) & 1) as tinyint)) AS lochierarchy#1, sum(ss_net_profit#35) AS _w0#204, (cast((shiftright(spark_grouping_id#193L, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#193L, 0) & 1) as tinyint)) AS _w1#208, CASE WHEN (cast((shiftright(spark_grouping_id#193L, 0) & 1) as tinyint) = 0) THEN s_state#194 END AS _w2#209]
               +- Expand [[ss_net_profit#35, s_state#88, s_county#87, 0], [ss_net_profit#35, s_state#88, null, 1], [ss_net_profit#35, null, null, 3]], [ss_net_profit#35, s_state#194, s_county#195, spark_grouping_id#193L]
                  +- Project [ss_net_profit#35, s_state#88, s_county#87]
                     +- Join Inner, (s_store_sk#64 = ss_store_sk#20)
                        :- Project [ss_store_sk#20, ss_net_profit#35]
                        :  +- Join Inner, (d_date_sk#36 = ss_sold_date_sk#13)
                        :     :- Project [ss_sold_date_sk#13, ss_store_sk#20, ss_net_profit#35]
                        :     :  +- Filter (isnotnull(ss_sold_date_sk#13) AND isnotnull(ss_store_sk#20))
                        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#13,ss_sold_time_sk#14,ss_item_sk#15,ss_customer_sk#16,ss_cdemo_sk#17,ss_hdemo_sk#18,ss_addr_sk#19,ss_store_sk#20,ss_promo_sk#21,ss_ticket_number#22,ss_quantity#23,ss_wholesale_cost#24,ss_list_price#25,ss_sales_price#26,ss_ext_discount_amt#27,ss_ext_sales_price#28,ss_ext_wholesale_cost#29,ss_ext_list_price#30,ss_ext_tax#31,ss_coupon_amt#32,ss_net_paid#33,ss_net_paid_inc_tax#34,ss_net_profit#35] parquet
                        :     +- Project [d_date_sk#36]
                        :        +- Filter ((isnotnull(d_month_seq#39) AND ((d_month_seq#39 >= 1200) AND (d_month_seq#39 <= 1211))) AND isnotnull(d_date_sk#36))
                        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#36,d_date_id#37,d_date#38,d_month_seq#39,d_week_seq#40,d_quarter_seq#41,d_year#42,d_dow#43,d_moy#44,d_dom#45,d_qoy#46,d_fy_year#47,d_fy_quarter_seq#48,d_fy_week_seq#49,d_day_name#50,d_quarter_name#51,d_holiday#52,d_weekend#53,d_following_holiday#54,d_first_dom#55,d_last_dom#56,d_same_day_ly#57,d_same_day_lq#58,d_current_day#59,... 4 more fields] parquet
                        +- Join LeftSemi, (s_state#88 = s_state#156)
                           :- Project [s_store_sk#64, s_county#87, s_state#88]
                           :  +- Filter isnotnull(s_store_sk#64)
                           :     +- Relation spark_catalog.tpcds.store[s_store_sk#64,s_store_id#65,s_rec_start_date#66,s_rec_end_date#67,s_closed_date_sk#68,s_store_name#69,s_number_employees#70,s_floor_space#71,s_hours#72,s_manager#73,s_market_id#74,s_geography_class#75,s_market_desc#76,s_market_manager#77,s_division_id#78,s_division_name#79,s_company_id#80,s_company_name#81,s_street_number#82,s_street_name#83,s_street_type#84,s_suite_number#85,s_city#86,s_county#87,... 5 more fields] parquet
                           +- Project [s_state#156]
                              +- Filter (ranking#4 <= 5)
                                 +- Window [rank(_w0#99) windowspecdefinition(s_state#156, _w0#99 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS ranking#4], [s_state#156], [_w0#99 DESC NULLS LAST]
                                    +- WindowGroupLimit [s_state#156], [_w0#99 DESC NULLS LAST], rank(_w0#99), 5
                                       +- Aggregate [s_state#156], [s_state#156, sum(ss_net_profit#131) AS _w0#99, s_state#156]
                                          +- Project [ss_net_profit#131, s_state#156]
                                             +- Join Inner, (d_date_sk#161 = ss_sold_date_sk#109)
                                                :- Project [ss_sold_date_sk#109, ss_net_profit#131, s_state#156]
                                                :  +- Join Inner, (s_store_sk#132 = ss_store_sk#116)
                                                :     :- Project [ss_sold_date_sk#109, ss_store_sk#116, ss_net_profit#131]
                                                :     :  +- Filter (isnotnull(ss_store_sk#116) AND isnotnull(ss_sold_date_sk#109))
                                                :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#109,ss_sold_time_sk#110,ss_item_sk#111,ss_customer_sk#112,ss_cdemo_sk#113,ss_hdemo_sk#114,ss_addr_sk#115,ss_store_sk#116,ss_promo_sk#117,ss_ticket_number#118,ss_quantity#119,ss_wholesale_cost#120,ss_list_price#121,ss_sales_price#122,ss_ext_discount_amt#123,ss_ext_sales_price#124,ss_ext_wholesale_cost#125,ss_ext_list_price#126,ss_ext_tax#127,ss_coupon_amt#128,ss_net_paid#129,ss_net_paid_inc_tax#130,ss_net_profit#131] parquet
                                                :     +- Project [s_store_sk#132, s_state#156]
                                                :        +- Filter isnotnull(s_store_sk#132)
                                                :           +- Relation spark_catalog.tpcds.store[s_store_sk#132,s_store_id#133,s_rec_start_date#134,s_rec_end_date#135,s_closed_date_sk#136,s_store_name#137,s_number_employees#138,s_floor_space#139,s_hours#140,s_manager#141,s_market_id#142,s_geography_class#143,s_market_desc#144,s_market_manager#145,s_division_id#146,s_division_name#147,s_company_id#148,s_company_name#149,s_street_number#150,s_street_name#151,s_street_type#152,s_suite_number#153,s_city#154,s_county#155,... 5 more fields] parquet
                                                +- Project [d_date_sk#161]
                                                   +- Filter ((isnotnull(d_month_seq#164) AND ((d_month_seq#164 >= 1200) AND (d_month_seq#164 <= 1211))) AND isnotnull(d_date_sk#161))
                                                      +- Relation spark_catalog.tpcds.date_dim[d_date_sk#161,d_date_id#162,d_date#163,d_month_seq#164,d_week_seq#165,d_quarter_seq#166,d_year#167,d_dow#168,d_moy#169,d_dom#170,d_qoy#171,d_fy_year#172,d_fy_quarter_seq#173,d_fy_week_seq#174,d_day_name#175,d_quarter_name#176,d_holiday#177,d_weekend#178,d_following_holiday#179,d_first_dom#180,d_last_dom#181,d_same_day_ly#182,d_same_day_lq#183,d_current_day#184,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[lochierarchy#1 DESC NULLS LAST,CASE WHEN (lochierarchy#1 = 0) THEN s_state#194 END ASC NULLS FIRST,rank_within_parent#2 ASC NULLS FIRST], output=[total_sum#0,s_state#194,s_county#195,lochierarchy#1,rank_within_parent#2])
   +- Project [total_sum#0, s_state#194, s_county#195, lochierarchy#1, rank_within_parent#2]
      +- Window [rank(_w0#204) windowspecdefinition(_w1#208, _w2#209, _w0#204 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#208, _w2#209], [_w0#204 DESC NULLS LAST]
         +- Sort [_w1#208 ASC NULLS FIRST, _w2#209 ASC NULLS FIRST, _w0#204 DESC NULLS LAST], false, 0
            +- Exchange hashpartitioning(_w1#208, _w2#209, 200), ENSURE_REQUIREMENTS, [plan_id=213]
               +- HashAggregate(keys=[s_state#194, s_county#195, spark_grouping_id#193L], functions=[sum(ss_net_profit#35)], output=[total_sum#0, s_state#194, s_county#195, lochierarchy#1, _w0#204, _w1#208, _w2#209])
                  +- Exchange hashpartitioning(s_state#194, s_county#195, spark_grouping_id#193L, 200), ENSURE_REQUIREMENTS, [plan_id=211]
                     +- HashAggregate(keys=[s_state#194, s_county#195, spark_grouping_id#193L], functions=[partial_sum(ss_net_profit#35)], output=[s_state#194, s_county#195, spark_grouping_id#193L, sum#226])
                        +- Expand [[ss_net_profit#35, s_state#88, s_county#87, 0], [ss_net_profit#35, s_state#88, null, 1], [ss_net_profit#35, null, null, 3]], [ss_net_profit#35, s_state#194, s_county#195, spark_grouping_id#193L]
                           +- Project [ss_net_profit#35, s_state#88, s_county#87]
                              +- BroadcastHashJoin [ss_store_sk#20], [s_store_sk#64], Inner, BuildRight, false
                                 :- Project [ss_store_sk#20, ss_net_profit#35]
                                 :  +- BroadcastHashJoin [ss_sold_date_sk#13], [d_date_sk#36], Inner, BuildRight, false
                                 :     :- Filter (isnotnull(ss_sold_date_sk#13) AND isnotnull(ss_store_sk#20))
                                 :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#13,ss_store_sk#20,ss_net_profit#35] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#13), isnotnull(ss_store_sk#20)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_net_profit:double>
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=160]
                                 :        +- Project [d_date_sk#36]
                                 :           +- Filter (((isnotnull(d_month_seq#39) AND (d_month_seq#39 >= 1200)) AND (d_month_seq#39 <= 1211)) AND isnotnull(d_date_sk#36))
                                 :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#36,d_month_seq#39] Batched: true, DataFilters: [isnotnull(d_month_seq#39), (d_month_seq#39 >= 1200), (d_month_seq#39 <= 1211), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=206]
                                    +- SortMergeJoin [s_state#88], [s_state#156], LeftSemi
                                       :- Sort [s_state#88 ASC NULLS FIRST], false, 0
                                       :  +- Exchange hashpartitioning(s_state#88, 200), ENSURE_REQUIREMENTS, [plan_id=183]
                                       :     +- Filter isnotnull(s_store_sk#64)
                                       :        +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#64,s_county#87,s_state#88] Batched: true, DataFilters: [isnotnull(s_store_sk#64)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_county:string,s_state:string>
                                       +- Project [s_state#156]
                                          +- Filter (ranking#4 <= 5)
                                             +- Window [rank(_w0#99) windowspecdefinition(s_state#156, _w0#99 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS ranking#4], [s_state#156], [_w0#99 DESC NULLS LAST]
                                                +- WindowGroupLimit [s_state#156], [_w0#99 DESC NULLS LAST], rank(_w0#99), 5, Final
                                                   +- Sort [s_state#156 ASC NULLS FIRST, _w0#99 DESC NULLS LAST], false, 0
                                                      +- HashAggregate(keys=[s_state#156], functions=[sum(ss_net_profit#131)], output=[s_state#156, _w0#99, s_state#156])
                                                         +- Exchange hashpartitioning(s_state#156, 200), ENSURE_REQUIREMENTS, [plan_id=172]
                                                            +- HashAggregate(keys=[s_state#156], functions=[partial_sum(ss_net_profit#131)], output=[s_state#156, sum#228])
                                                               +- Project [ss_net_profit#131, s_state#156]
                                                                  +- BroadcastHashJoin [ss_sold_date_sk#109], [d_date_sk#161], Inner, BuildRight, false
                                                                     :- Project [ss_sold_date_sk#109, ss_net_profit#131, s_state#156]
                                                                     :  +- BroadcastHashJoin [ss_store_sk#116], [s_store_sk#132], Inner, BuildRight, false
                                                                     :     :- Filter (isnotnull(ss_store_sk#116) AND isnotnull(ss_sold_date_sk#109))
                                                                     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#109,ss_store_sk#116,ss_net_profit#131] Batched: true, DataFilters: [isnotnull(ss_store_sk#116), isnotnull(ss_sold_date_sk#109)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_net_profit:double>
                                                                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=163]
                                                                     :        +- Filter isnotnull(s_store_sk#132)
                                                                     :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#132,s_state#156] Batched: true, DataFilters: [isnotnull(s_store_sk#132)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_state:string>
                                                                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=167]
                                                                        +- Project [d_date_sk#161]
                                                                           +- Filter (((isnotnull(d_month_seq#164) AND (d_month_seq#164 >= 1200)) AND (d_month_seq#164 <= 1211)) AND isnotnull(d_date_sk#161))
                                                                              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#161,d_month_seq#164] Batched: true, DataFilters: [isnotnull(d_month_seq#164), (d_month_seq#164 >= 1200), (d_month_seq#164 <= 1211), isnotnull(d_da..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>

Time taken: 2.949 seconds, Fetched 1 row(s)
