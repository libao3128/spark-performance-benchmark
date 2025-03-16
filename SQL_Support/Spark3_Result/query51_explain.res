Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581525320
== Parsed Logical Plan ==
CTE [web_v1, store_v1]
:  :- 'SubqueryAlias web_v1
:  :  +- 'Aggregate ['ws_item_sk, 'd_date], ['ws_item_sk AS item_sk#6, 'd_date, 'sum('sum('ws_sales_price)) windowspecdefinition('ws_item_sk, 'd_date ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#7]
:  :     +- 'Filter ((('ws_sold_date_sk = 'd_date_sk) AND (('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11)))) AND isnotnull('ws_item_sk))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation [web_sales], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  +- 'SubqueryAlias store_v1
:     +- 'Aggregate ['ss_item_sk, 'd_date], ['ss_item_sk AS item_sk#8, 'd_date, 'sum('sum('ss_sales_price)) windowspecdefinition('ss_item_sk, 'd_date ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#9]
:        +- 'Filter ((('ss_sold_date_sk = 'd_date_sk) AND (('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11)))) AND isnotnull('ss_item_sk))
:           +- 'Join Inner
:              :- 'UnresolvedRelation [store_sales], [], false
:              +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['item_sk ASC NULLS FIRST, 'd_date ASC NULLS FIRST], true
         +- 'Project [*]
            +- 'Filter ('web_cumulative > 'store_cumulative)
               +- 'SubqueryAlias y
                  +- 'Project ['item_sk, 'd_date, 'web_sales, 'store_sales, 'max('web_sales) windowspecdefinition('item_sk, 'd_date ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS web_cumulative#4, 'max('store_sales) windowspecdefinition('item_sk, 'd_date ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS store_cumulative#5]
                     +- 'SubqueryAlias x
                        +- 'Project [CASE WHEN isnotnull('web.item_sk) THEN 'web.item_sk ELSE 'store.item_sk END AS item_sk#0, CASE WHEN isnotnull('web.d_date) THEN 'web.d_date ELSE 'store.d_date END AS d_date#1, 'web.cume_sales AS web_sales#2, 'store.cume_sales AS store_sales#3]
                           +- 'Join FullOuter, (('web.item_sk = 'store.item_sk) AND ('web.d_date = 'store.d_date))
                              :- 'SubqueryAlias web
                              :  +- 'UnresolvedRelation [web_v1], [], false
                              +- 'SubqueryAlias store
                                 +- 'UnresolvedRelation [store_v1], [], false

== Analyzed Logical Plan ==
item_sk: int, d_date: string, web_sales: double, store_sales: double, web_cumulative: double, store_cumulative: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias web_v1
:     +- Project [item_sk#6, d_date#51, cume_sales#7]
:        +- Project [item_sk#6, d_date#51, _w0#136, ws_item_sk#18, cume_sales#7, cume_sales#7]
:           +- Window [sum(_w0#136) windowspecdefinition(ws_item_sk#18, d_date#51 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#7], [ws_item_sk#18], [d_date#51 ASC NULLS FIRST]
:              +- Aggregate [ws_item_sk#18, d_date#51], [ws_item_sk#18 AS item_sk#6, d_date#51, sum(ws_sales_price#36) AS _w0#136, ws_item_sk#18]
:                 +- Filter (((ws_sold_date_sk#15 = d_date_sk#49) AND ((d_month_seq#52 >= 1200) AND (d_month_seq#52 <= (1200 + 11)))) AND isnotnull(ws_item_sk#18))
:                    +- Join Inner
:                       :- SubqueryAlias spark_catalog.tpcds.web_sales
:                       :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#15,ws_sold_time_sk#16,ws_ship_date_sk#17,ws_item_sk#18,ws_bill_customer_sk#19,ws_bill_cdemo_sk#20,ws_bill_hdemo_sk#21,ws_bill_addr_sk#22,ws_ship_customer_sk#23,ws_ship_cdemo_sk#24,ws_ship_hdemo_sk#25,ws_ship_addr_sk#26,ws_web_page_sk#27,ws_web_site_sk#28,ws_ship_mode_sk#29,ws_warehouse_sk#30,ws_promo_sk#31,ws_order_number#32,ws_quantity#33,ws_wholesale_cost#34,ws_list_price#35,ws_sales_price#36,ws_ext_discount_amt#37,ws_ext_sales_price#38,... 10 more fields] parquet
:                       +- SubqueryAlias spark_catalog.tpcds.date_dim
:                          +- Relation spark_catalog.tpcds.date_dim[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias store_v1
:     +- Project [item_sk#8, d_date#102, cume_sales#9]
:        +- Project [item_sk#8, d_date#102, _w0#137, ss_item_sk#79, cume_sales#9, cume_sales#9]
:           +- Window [sum(_w0#137) windowspecdefinition(ss_item_sk#79, d_date#102 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#9], [ss_item_sk#79], [d_date#102 ASC NULLS FIRST]
:              +- Aggregate [ss_item_sk#79, d_date#102], [ss_item_sk#79 AS item_sk#8, d_date#102, sum(ss_sales_price#90) AS _w0#137, ss_item_sk#79]
:                 +- Filter (((ss_sold_date_sk#77 = d_date_sk#100) AND ((d_month_seq#103 >= 1200) AND (d_month_seq#103 <= (1200 + 11)))) AND isnotnull(ss_item_sk#79))
:                    +- Join Inner
:                       :- SubqueryAlias spark_catalog.tpcds.store_sales
:                       :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#77,ss_sold_time_sk#78,ss_item_sk#79,ss_customer_sk#80,ss_cdemo_sk#81,ss_hdemo_sk#82,ss_addr_sk#83,ss_store_sk#84,ss_promo_sk#85,ss_ticket_number#86,ss_quantity#87,ss_wholesale_cost#88,ss_list_price#89,ss_sales_price#90,ss_ext_discount_amt#91,ss_ext_sales_price#92,ss_ext_wholesale_cost#93,ss_ext_list_price#94,ss_ext_tax#95,ss_coupon_amt#96,ss_net_paid#97,ss_net_paid_inc_tax#98,ss_net_profit#99] parquet
:                       +- SubqueryAlias spark_catalog.tpcds.date_dim
:                          +- Relation spark_catalog.tpcds.date_dim[d_date_sk#100,d_date_id#101,d_date#102,d_month_seq#103,d_week_seq#104,d_quarter_seq#105,d_year#106,d_dow#107,d_moy#108,d_dom#109,d_qoy#110,d_fy_year#111,d_fy_quarter_seq#112,d_fy_week_seq#113,d_day_name#114,d_quarter_name#115,d_holiday#116,d_weekend#117,d_following_holiday#118,d_first_dom#119,d_last_dom#120,d_same_day_ly#121,d_same_day_lq#122,d_current_day#123,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [item_sk#0 ASC NULLS FIRST, d_date#1 ASC NULLS FIRST], true
         +- Project [item_sk#0, d_date#1, web_sales#2, store_sales#3, web_cumulative#4, store_cumulative#5]
            +- Filter (web_cumulative#4 > store_cumulative#5)
               +- SubqueryAlias y
                  +- Project [item_sk#0, d_date#1, web_sales#2, store_sales#3, web_cumulative#4, store_cumulative#5]
                     +- Project [item_sk#0, d_date#1, web_sales#2, store_sales#3, web_cumulative#4, store_cumulative#5, web_cumulative#4, store_cumulative#5]
                        +- Window [max(web_sales#2) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS web_cumulative#4, max(store_sales#3) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS store_cumulative#5], [item_sk#0], [d_date#1 ASC NULLS FIRST]
                           +- Project [item_sk#0, d_date#1, web_sales#2, store_sales#3]
                              +- SubqueryAlias x
                                 +- Project [CASE WHEN isnotnull(item_sk#6) THEN item_sk#6 ELSE item_sk#8 END AS item_sk#0, CASE WHEN isnotnull(d_date#51) THEN d_date#51 ELSE d_date#102 END AS d_date#1, cume_sales#7 AS web_sales#2, cume_sales#9 AS store_sales#3]
                                    +- Join FullOuter, ((item_sk#6 = item_sk#8) AND (d_date#51 = d_date#102))
                                       :- SubqueryAlias web
                                       :  +- SubqueryAlias web_v1
                                       :     +- CTERelationRef 0, true, [item_sk#6, d_date#51, cume_sales#7], false
                                       +- SubqueryAlias store
                                          +- SubqueryAlias store_v1
                                             +- CTERelationRef 1, true, [item_sk#8, d_date#102, cume_sales#9], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [item_sk#0 ASC NULLS FIRST, d_date#1 ASC NULLS FIRST], true
      +- Filter ((isnotnull(web_cumulative#4) AND isnotnull(store_cumulative#5)) AND (web_cumulative#4 > store_cumulative#5))
         +- Window [max(web_sales#2) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS web_cumulative#4, max(store_sales#3) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS store_cumulative#5], [item_sk#0], [d_date#1 ASC NULLS FIRST]
            +- Project [CASE WHEN isnotnull(item_sk#6) THEN item_sk#6 ELSE item_sk#8 END AS item_sk#0, CASE WHEN isnotnull(d_date#51) THEN d_date#51 ELSE d_date#102 END AS d_date#1, cume_sales#7 AS web_sales#2, cume_sales#9 AS store_sales#3]
               +- Join FullOuter, ((item_sk#6 = item_sk#8) AND (d_date#51 = d_date#102))
                  :- Project [item_sk#6, d_date#51, cume_sales#7]
                  :  +- Window [sum(_w0#136) windowspecdefinition(ws_item_sk#18, d_date#51 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#7], [ws_item_sk#18], [d_date#51 ASC NULLS FIRST]
                  :     +- Aggregate [ws_item_sk#18, d_date#51], [ws_item_sk#18 AS item_sk#6, d_date#51, sum(ws_sales_price#36) AS _w0#136, ws_item_sk#18]
                  :        +- Project [ws_item_sk#18, ws_sales_price#36, d_date#51]
                  :           +- Join Inner, (ws_sold_date_sk#15 = d_date_sk#49)
                  :              :- Project [ws_sold_date_sk#15, ws_item_sk#18, ws_sales_price#36]
                  :              :  +- Filter (isnotnull(ws_item_sk#18) AND isnotnull(ws_sold_date_sk#15))
                  :              :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#15,ws_sold_time_sk#16,ws_ship_date_sk#17,ws_item_sk#18,ws_bill_customer_sk#19,ws_bill_cdemo_sk#20,ws_bill_hdemo_sk#21,ws_bill_addr_sk#22,ws_ship_customer_sk#23,ws_ship_cdemo_sk#24,ws_ship_hdemo_sk#25,ws_ship_addr_sk#26,ws_web_page_sk#27,ws_web_site_sk#28,ws_ship_mode_sk#29,ws_warehouse_sk#30,ws_promo_sk#31,ws_order_number#32,ws_quantity#33,ws_wholesale_cost#34,ws_list_price#35,ws_sales_price#36,ws_ext_discount_amt#37,ws_ext_sales_price#38,... 10 more fields] parquet
                  :              +- Project [d_date_sk#49, d_date#51]
                  :                 +- Filter ((isnotnull(d_month_seq#52) AND ((d_month_seq#52 >= 1200) AND (d_month_seq#52 <= 1211))) AND isnotnull(d_date_sk#49))
                  :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet
                  +- Project [item_sk#8, d_date#102, cume_sales#9]
                     +- Window [sum(_w0#137) windowspecdefinition(ss_item_sk#79, d_date#102 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#9], [ss_item_sk#79], [d_date#102 ASC NULLS FIRST]
                        +- Aggregate [ss_item_sk#79, d_date#102], [ss_item_sk#79 AS item_sk#8, d_date#102, sum(ss_sales_price#90) AS _w0#137, ss_item_sk#79]
                           +- Project [ss_item_sk#79, ss_sales_price#90, d_date#102]
                              +- Join Inner, (ss_sold_date_sk#77 = d_date_sk#100)
                                 :- Project [ss_sold_date_sk#77, ss_item_sk#79, ss_sales_price#90]
                                 :  +- Filter (isnotnull(ss_item_sk#79) AND isnotnull(ss_sold_date_sk#77))
                                 :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#77,ss_sold_time_sk#78,ss_item_sk#79,ss_customer_sk#80,ss_cdemo_sk#81,ss_hdemo_sk#82,ss_addr_sk#83,ss_store_sk#84,ss_promo_sk#85,ss_ticket_number#86,ss_quantity#87,ss_wholesale_cost#88,ss_list_price#89,ss_sales_price#90,ss_ext_discount_amt#91,ss_ext_sales_price#92,ss_ext_wholesale_cost#93,ss_ext_list_price#94,ss_ext_tax#95,ss_coupon_amt#96,ss_net_paid#97,ss_net_paid_inc_tax#98,ss_net_profit#99] parquet
                                 +- Project [d_date_sk#100, d_date#102]
                                    +- Filter ((isnotnull(d_month_seq#103) AND ((d_month_seq#103 >= 1200) AND (d_month_seq#103 <= 1211))) AND isnotnull(d_date_sk#100))
                                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#100,d_date_id#101,d_date#102,d_month_seq#103,d_week_seq#104,d_quarter_seq#105,d_year#106,d_dow#107,d_moy#108,d_dom#109,d_qoy#110,d_fy_year#111,d_fy_quarter_seq#112,d_fy_week_seq#113,d_day_name#114,d_quarter_name#115,d_holiday#116,d_weekend#117,d_following_holiday#118,d_first_dom#119,d_last_dom#120,d_same_day_ly#121,d_same_day_lq#122,d_current_day#123,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[item_sk#0 ASC NULLS FIRST,d_date#1 ASC NULLS FIRST], output=[item_sk#0,d_date#1,web_sales#2,store_sales#3,web_cumulative#4,store_cumulative#5])
   +- Filter ((isnotnull(web_cumulative#4) AND isnotnull(store_cumulative#5)) AND (web_cumulative#4 > store_cumulative#5))
      +- Window [max(web_sales#2) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS web_cumulative#4, max(store_sales#3) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS store_cumulative#5], [item_sk#0], [d_date#1 ASC NULLS FIRST]
         +- Sort [item_sk#0 ASC NULLS FIRST, d_date#1 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(item_sk#0, 200), ENSURE_REQUIREMENTS, [plan_id=153]
               +- Project [CASE WHEN isnotnull(item_sk#6) THEN item_sk#6 ELSE item_sk#8 END AS item_sk#0, CASE WHEN isnotnull(d_date#51) THEN d_date#51 ELSE d_date#102 END AS d_date#1, cume_sales#7 AS web_sales#2, cume_sales#9 AS store_sales#3]
                  +- SortMergeJoin [item_sk#6, d_date#51], [item_sk#8, d_date#102], FullOuter
                     :- Sort [item_sk#6 ASC NULLS FIRST, d_date#51 ASC NULLS FIRST], false, 0
                     :  +- Exchange hashpartitioning(item_sk#6, d_date#51, 200), ENSURE_REQUIREMENTS, [plan_id=146]
                     :     +- Project [item_sk#6, d_date#51, cume_sales#7]
                     :        +- Window [sum(_w0#136) windowspecdefinition(ws_item_sk#18, d_date#51 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#7], [ws_item_sk#18], [d_date#51 ASC NULLS FIRST]
                     :           +- Sort [ws_item_sk#18 ASC NULLS FIRST, d_date#51 ASC NULLS FIRST], false, 0
                     :              +- Exchange hashpartitioning(ws_item_sk#18, 200), ENSURE_REQUIREMENTS, [plan_id=128]
                     :                 +- HashAggregate(keys=[ws_item_sk#18, d_date#51], functions=[sum(ws_sales_price#36)], output=[item_sk#6, d_date#51, _w0#136, ws_item_sk#18])
                     :                    +- Exchange hashpartitioning(ws_item_sk#18, d_date#51, 200), ENSURE_REQUIREMENTS, [plan_id=125]
                     :                       +- HashAggregate(keys=[ws_item_sk#18, d_date#51], functions=[partial_sum(ws_sales_price#36)], output=[ws_item_sk#18, d_date#51, sum#141])
                     :                          +- Project [ws_item_sk#18, ws_sales_price#36, d_date#51]
                     :                             +- BroadcastHashJoin [ws_sold_date_sk#15], [d_date_sk#49], Inner, BuildRight, false
                     :                                :- Filter (isnotnull(ws_item_sk#18) AND isnotnull(ws_sold_date_sk#15))
                     :                                :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#15,ws_item_sk#18,ws_sales_price#36] Batched: true, DataFilters: [isnotnull(ws_item_sk#18), isnotnull(ws_sold_date_sk#15)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_sales_price:double>
                     :                                +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=120]
                     :                                   +- Project [d_date_sk#49, d_date#51]
                     :                                      +- Filter (((isnotnull(d_month_seq#52) AND (d_month_seq#52 >= 1200)) AND (d_month_seq#52 <= 1211)) AND isnotnull(d_date_sk#49))
                     :                                         +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#49,d_date#51,d_month_seq#52] Batched: true, DataFilters: [isnotnull(d_month_seq#52), (d_month_seq#52 >= 1200), (d_month_seq#52 <= 1211), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_date:string,d_month_seq:int>
                     +- Sort [item_sk#8 ASC NULLS FIRST, d_date#102 ASC NULLS FIRST], false, 0
                        +- Exchange hashpartitioning(item_sk#8, d_date#102, 200), ENSURE_REQUIREMENTS, [plan_id=147]
                           +- Project [item_sk#8, d_date#102, cume_sales#9]
                              +- Window [sum(_w0#137) windowspecdefinition(ss_item_sk#79, d_date#102 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#9], [ss_item_sk#79], [d_date#102 ASC NULLS FIRST]
                                 +- Sort [ss_item_sk#79 ASC NULLS FIRST, d_date#102 ASC NULLS FIRST], false, 0
                                    +- Exchange hashpartitioning(ss_item_sk#79, 200), ENSURE_REQUIREMENTS, [plan_id=140]
                                       +- HashAggregate(keys=[ss_item_sk#79, d_date#102], functions=[sum(ss_sales_price#90)], output=[item_sk#8, d_date#102, _w0#137, ss_item_sk#79])
                                          +- Exchange hashpartitioning(ss_item_sk#79, d_date#102, 200), ENSURE_REQUIREMENTS, [plan_id=137]
                                             +- HashAggregate(keys=[ss_item_sk#79, d_date#102], functions=[partial_sum(ss_sales_price#90)], output=[ss_item_sk#79, d_date#102, sum#143])
                                                +- Project [ss_item_sk#79, ss_sales_price#90, d_date#102]
                                                   +- BroadcastHashJoin [ss_sold_date_sk#77], [d_date_sk#100], Inner, BuildRight, false
                                                      :- Filter (isnotnull(ss_item_sk#79) AND isnotnull(ss_sold_date_sk#77))
                                                      :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#77,ss_item_sk#79,ss_sales_price#90] Batched: true, DataFilters: [isnotnull(ss_item_sk#79), isnotnull(ss_sold_date_sk#77)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_sales_price:double>
                                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=132]
                                                         +- Project [d_date_sk#100, d_date#102]
                                                            +- Filter (((isnotnull(d_month_seq#103) AND (d_month_seq#103 >= 1200)) AND (d_month_seq#103 <= 1211)) AND isnotnull(d_date_sk#100))
                                                               +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#100,d_date#102,d_month_seq#103] Batched: true, DataFilters: [isnotnull(d_month_seq#103), (d_month_seq#103 >= 1200), (d_month_seq#103 <= 1211), isnotnull(d_da..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_date:string,d_month_seq:int>

Time taken: 2.642 seconds, Fetched 1 row(s)
