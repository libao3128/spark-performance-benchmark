== Parsed Logical Plan ==
CTE [web_v1, store_v1]
:  :- 'SubqueryAlias `web_v1`
:  :  +- 'Aggregate ['ws_item_sk, 'd_date], ['ws_item_sk AS item_sk#6, 'd_date, 'sum('sum('ws_sales_price)) windowspecdefinition('ws_item_sk, 'd_date ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#7]
:  :     +- 'Filter ((('ws_sold_date_sk = 'd_date_sk) && (('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11)))) && isnotnull('ws_item_sk))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation `web_sales`
:  :           +- 'UnresolvedRelation `date_dim`
:  +- 'SubqueryAlias `store_v1`
:     +- 'Aggregate ['ss_item_sk, 'd_date], ['ss_item_sk AS item_sk#8, 'd_date, 'sum('sum('ss_sales_price)) windowspecdefinition('ss_item_sk, 'd_date ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#9]
:        +- 'Filter ((('ss_sold_date_sk = 'd_date_sk) && (('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11)))) && isnotnull('ss_item_sk))
:           +- 'Join Inner
:              :- 'UnresolvedRelation `store_sales`
:              +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['item_sk ASC NULLS FIRST, 'd_date ASC NULLS FIRST], true
         +- 'Project [*]
            +- 'Filter ('web_cumulative > 'store_cumulative)
               +- 'SubqueryAlias `y`
                  +- 'Project ['item_sk, 'd_date, 'web_sales, 'store_sales, 'max('web_sales) windowspecdefinition('item_sk, 'd_date ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS web_cumulative#4, 'max('store_sales) windowspecdefinition('item_sk, 'd_date ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS store_cumulative#5]
                     +- 'SubqueryAlias `x`
                        +- 'Project [CASE WHEN isnotnull('web.item_sk) THEN 'web.item_sk ELSE 'store.item_sk END AS item_sk#0, CASE WHEN isnotnull('web.d_date) THEN 'web.d_date ELSE 'store.d_date END AS d_date#1, 'web.cume_sales AS web_sales#2, 'store.cume_sales AS store_sales#3]
                           +- 'Join FullOuter, (('web.item_sk = 'store.item_sk) && ('web.d_date = 'store.d_date))
                              :- 'SubqueryAlias `web`
                              :  +- 'UnresolvedRelation `web_v1`
                              +- 'SubqueryAlias `store`
                                 +- 'UnresolvedRelation `store_v1`

== Analyzed Logical Plan ==
item_sk: int, d_date: string, web_sales: double, store_sales: double, web_cumulative: double, store_cumulative: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [item_sk#0 ASC NULLS FIRST, d_date#1 ASC NULLS FIRST], true
      +- Project [item_sk#0, d_date#1, web_sales#2, store_sales#3, web_cumulative#4, store_cumulative#5]
         +- Filter (web_cumulative#4 > store_cumulative#5)
            +- SubqueryAlias `y`
               +- Project [item_sk#0, d_date#1, web_sales#2, store_sales#3, web_cumulative#4, store_cumulative#5]
                  +- Project [item_sk#0, d_date#1, web_sales#2, store_sales#3, web_cumulative#4, store_cumulative#5, web_cumulative#4, store_cumulative#5]
                     +- Window [max(web_sales#2) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS web_cumulative#4, max(store_sales#3) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS store_cumulative#5], [item_sk#0], [d_date#1 ASC NULLS FIRST]
                        +- Project [item_sk#0, d_date#1, web_sales#2, store_sales#3]
                           +- SubqueryAlias `x`
                              +- Project [CASE WHEN isnotnull(item_sk#6) THEN item_sk#6 ELSE item_sk#8 END AS item_sk#0, CASE WHEN isnotnull(d_date#48) THEN d_date#48 ELSE d_date#105 END AS d_date#1, cume_sales#7 AS web_sales#2, cume_sales#9 AS store_sales#3]
                                 +- Join FullOuter, ((item_sk#6 = item_sk#8) && (d_date#48 = d_date#105))
                                    :- SubqueryAlias `web`
                                    :  +- SubqueryAlias `web_v1`
                                    :     +- Project [item_sk#6, d_date#48, cume_sales#7]
                                    :        +- Project [item_sk#6, d_date#48, _w0#76, ws_item_sk#15, cume_sales#7, cume_sales#7]
                                    :           +- Window [sum(_w0#76) windowspecdefinition(ws_item_sk#15, d_date#48 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#7], [ws_item_sk#15], [d_date#48 ASC NULLS FIRST]
                                    :              +- Aggregate [ws_item_sk#15, d_date#48], [ws_item_sk#15 AS item_sk#6, d_date#48, sum(ws_sales_price#33) AS _w0#76, ws_item_sk#15]
                                    :                 +- Filter (((ws_sold_date_sk#12 = d_date_sk#46) && ((d_month_seq#49 >= 1200) && (d_month_seq#49 <= (1200 + 11)))) && isnotnull(ws_item_sk#15))
                                    :                    +- Join Inner
                                    :                       :- SubqueryAlias `tpcds`.`web_sales`
                                    :                       :  +- Relation[ws_sold_date_sk#12,ws_sold_time_sk#13,ws_ship_date_sk#14,ws_item_sk#15,ws_bill_customer_sk#16,ws_bill_cdemo_sk#17,ws_bill_hdemo_sk#18,ws_bill_addr_sk#19,ws_ship_customer_sk#20,ws_ship_cdemo_sk#21,ws_ship_hdemo_sk#22,ws_ship_addr_sk#23,ws_web_page_sk#24,ws_web_site_sk#25,ws_ship_mode_sk#26,ws_warehouse_sk#27,ws_promo_sk#28,ws_order_number#29,ws_quantity#30,ws_wholesale_cost#31,ws_list_price#32,ws_sales_price#33,ws_ext_discount_amt#34,ws_ext_sales_price#35,... 10 more fields] parquet
                                    :                       +- SubqueryAlias `tpcds`.`date_dim`
                                    :                          +- Relation[d_date_sk#46,d_date_id#47,d_date#48,d_month_seq#49,d_week_seq#50,d_quarter_seq#51,d_year#52,d_dow#53,d_moy#54,d_dom#55,d_qoy#56,d_fy_year#57,d_fy_quarter_seq#58,d_fy_week_seq#59,d_day_name#60,d_quarter_name#61,d_holiday#62,d_weekend#63,d_following_holiday#64,d_first_dom#65,d_last_dom#66,d_same_day_ly#67,d_same_day_lq#68,d_current_day#69,... 4 more fields] parquet
                                    +- SubqueryAlias `store`
                                       +- SubqueryAlias `store_v1`
                                          +- Project [item_sk#8, d_date#105, cume_sales#9]
                                             +- Project [item_sk#8, d_date#105, _w0#102, ss_item_sk#79, cume_sales#9, cume_sales#9]
                                                +- Window [sum(_w0#102) windowspecdefinition(ss_item_sk#79, d_date#105 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#9], [ss_item_sk#79], [d_date#105 ASC NULLS FIRST]
                                                   +- Aggregate [ss_item_sk#79, d_date#105], [ss_item_sk#79 AS item_sk#8, d_date#105, sum(ss_sales_price#90) AS _w0#102, ss_item_sk#79]
                                                      +- Filter (((ss_sold_date_sk#77 = d_date_sk#103) && ((d_month_seq#106 >= 1200) && (d_month_seq#106 <= (1200 + 11)))) && isnotnull(ss_item_sk#79))
                                                         +- Join Inner
                                                            :- SubqueryAlias `tpcds`.`store_sales`
                                                            :  +- Relation[ss_sold_date_sk#77,ss_sold_time_sk#78,ss_item_sk#79,ss_customer_sk#80,ss_cdemo_sk#81,ss_hdemo_sk#82,ss_addr_sk#83,ss_store_sk#84,ss_promo_sk#85,ss_ticket_number#86,ss_quantity#87,ss_wholesale_cost#88,ss_list_price#89,ss_sales_price#90,ss_ext_discount_amt#91,ss_ext_sales_price#92,ss_ext_wholesale_cost#93,ss_ext_list_price#94,ss_ext_tax#95,ss_coupon_amt#96,ss_net_paid#97,ss_net_paid_inc_tax#98,ss_net_profit#99] parquet
                                                            +- SubqueryAlias `tpcds`.`date_dim`
                                                               +- Relation[d_date_sk#103,d_date_id#104,d_date#105,d_month_seq#106,d_week_seq#107,d_quarter_seq#108,d_year#109,d_dow#110,d_moy#111,d_dom#112,d_qoy#113,d_fy_year#114,d_fy_quarter_seq#115,d_fy_week_seq#116,d_day_name#117,d_quarter_name#118,d_holiday#119,d_weekend#120,d_following_holiday#121,d_first_dom#122,d_last_dom#123,d_same_day_ly#124,d_same_day_lq#125,d_current_day#126,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [item_sk#0 ASC NULLS FIRST, d_date#1 ASC NULLS FIRST], true
      +- Filter ((isnotnull(web_cumulative#4) && isnotnull(store_cumulative#5)) && (web_cumulative#4 > store_cumulative#5))
         +- Window [max(web_sales#2) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS web_cumulative#4, max(store_sales#3) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS store_cumulative#5], [item_sk#0], [d_date#1 ASC NULLS FIRST]
            +- Project [CASE WHEN isnotnull(item_sk#6) THEN item_sk#6 ELSE item_sk#8 END AS item_sk#0, CASE WHEN isnotnull(d_date#48) THEN d_date#48 ELSE d_date#105 END AS d_date#1, cume_sales#7 AS web_sales#2, cume_sales#9 AS store_sales#3]
               +- Join FullOuter, ((item_sk#6 = item_sk#8) && (d_date#48 = d_date#105))
                  :- Project [item_sk#6, d_date#48, cume_sales#7]
                  :  +- Window [sum(_w0#76) windowspecdefinition(ws_item_sk#15, d_date#48 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#7], [ws_item_sk#15], [d_date#48 ASC NULLS FIRST]
                  :     +- Aggregate [ws_item_sk#15, d_date#48], [ws_item_sk#15 AS item_sk#6, d_date#48, sum(ws_sales_price#33) AS _w0#76, ws_item_sk#15]
                  :        +- Project [ws_item_sk#15, ws_sales_price#33, d_date#48]
                  :           +- Join Inner, (ws_sold_date_sk#12 = d_date_sk#46)
                  :              :- Project [ws_sold_date_sk#12, ws_item_sk#15, ws_sales_price#33]
                  :              :  +- Filter (isnotnull(ws_item_sk#15) && isnotnull(ws_sold_date_sk#12))
                  :              :     +- Relation[ws_sold_date_sk#12,ws_sold_time_sk#13,ws_ship_date_sk#14,ws_item_sk#15,ws_bill_customer_sk#16,ws_bill_cdemo_sk#17,ws_bill_hdemo_sk#18,ws_bill_addr_sk#19,ws_ship_customer_sk#20,ws_ship_cdemo_sk#21,ws_ship_hdemo_sk#22,ws_ship_addr_sk#23,ws_web_page_sk#24,ws_web_site_sk#25,ws_ship_mode_sk#26,ws_warehouse_sk#27,ws_promo_sk#28,ws_order_number#29,ws_quantity#30,ws_wholesale_cost#31,ws_list_price#32,ws_sales_price#33,ws_ext_discount_amt#34,ws_ext_sales_price#35,... 10 more fields] parquet
                  :              +- Project [d_date_sk#46, d_date#48]
                  :                 +- Filter (((isnotnull(d_month_seq#49) && (d_month_seq#49 >= 1200)) && (d_month_seq#49 <= 1211)) && isnotnull(d_date_sk#46))
                  :                    +- Relation[d_date_sk#46,d_date_id#47,d_date#48,d_month_seq#49,d_week_seq#50,d_quarter_seq#51,d_year#52,d_dow#53,d_moy#54,d_dom#55,d_qoy#56,d_fy_year#57,d_fy_quarter_seq#58,d_fy_week_seq#59,d_day_name#60,d_quarter_name#61,d_holiday#62,d_weekend#63,d_following_holiday#64,d_first_dom#65,d_last_dom#66,d_same_day_ly#67,d_same_day_lq#68,d_current_day#69,... 4 more fields] parquet
                  +- Project [item_sk#8, d_date#105, cume_sales#9]
                     +- Window [sum(_w0#102) windowspecdefinition(ss_item_sk#79, d_date#105 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#9], [ss_item_sk#79], [d_date#105 ASC NULLS FIRST]
                        +- Aggregate [ss_item_sk#79, d_date#105], [ss_item_sk#79 AS item_sk#8, d_date#105, sum(ss_sales_price#90) AS _w0#102, ss_item_sk#79]
                           +- Project [ss_item_sk#79, ss_sales_price#90, d_date#105]
                              +- Join Inner, (ss_sold_date_sk#77 = d_date_sk#103)
                                 :- Project [ss_sold_date_sk#77, ss_item_sk#79, ss_sales_price#90]
                                 :  +- Filter (isnotnull(ss_item_sk#79) && isnotnull(ss_sold_date_sk#77))
                                 :     +- Relation[ss_sold_date_sk#77,ss_sold_time_sk#78,ss_item_sk#79,ss_customer_sk#80,ss_cdemo_sk#81,ss_hdemo_sk#82,ss_addr_sk#83,ss_store_sk#84,ss_promo_sk#85,ss_ticket_number#86,ss_quantity#87,ss_wholesale_cost#88,ss_list_price#89,ss_sales_price#90,ss_ext_discount_amt#91,ss_ext_sales_price#92,ss_ext_wholesale_cost#93,ss_ext_list_price#94,ss_ext_tax#95,ss_coupon_amt#96,ss_net_paid#97,ss_net_paid_inc_tax#98,ss_net_profit#99] parquet
                                 +- Project [d_date_sk#103, d_date#105]
                                    +- Filter (((isnotnull(d_month_seq#106) && (d_month_seq#106 >= 1200)) && (d_month_seq#106 <= 1211)) && isnotnull(d_date_sk#103))
                                       +- Relation[d_date_sk#103,d_date_id#104,d_date#105,d_month_seq#106,d_week_seq#107,d_quarter_seq#108,d_year#109,d_dow#110,d_moy#111,d_dom#112,d_qoy#113,d_fy_year#114,d_fy_quarter_seq#115,d_fy_week_seq#116,d_day_name#117,d_quarter_name#118,d_holiday#119,d_weekend#120,d_following_holiday#121,d_first_dom#122,d_last_dom#123,d_same_day_ly#124,d_same_day_lq#125,d_current_day#126,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[item_sk#0 ASC NULLS FIRST,d_date#1 ASC NULLS FIRST], output=[item_sk#0,d_date#1,web_sales#2,store_sales#3,web_cumulative#4,store_cumulative#5])
+- *(15) Filter ((isnotnull(web_cumulative#4) && isnotnull(store_cumulative#5)) && (web_cumulative#4 > store_cumulative#5))
   +- Window [max(web_sales#2) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS web_cumulative#4, max(store_sales#3) windowspecdefinition(item_sk#0, d_date#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS store_cumulative#5], [item_sk#0], [d_date#1 ASC NULLS FIRST]
      +- *(14) Sort [item_sk#0 ASC NULLS FIRST, d_date#1 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(item_sk#0, 200)
            +- *(13) Project [CASE WHEN isnotnull(item_sk#6) THEN item_sk#6 ELSE item_sk#8 END AS item_sk#0, CASE WHEN isnotnull(d_date#48) THEN d_date#48 ELSE d_date#105 END AS d_date#1, cume_sales#7 AS web_sales#2, cume_sales#9 AS store_sales#3]
               +- SortMergeJoin [item_sk#6, d_date#48], [item_sk#8, d_date#105], FullOuter
                  :- *(6) Sort [item_sk#6 ASC NULLS FIRST, d_date#48 ASC NULLS FIRST], false, 0
                  :  +- Exchange hashpartitioning(item_sk#6, d_date#48, 200)
                  :     +- *(5) Project [item_sk#6, d_date#48, cume_sales#7]
                  :        +- Window [sum(_w0#76) windowspecdefinition(ws_item_sk#15, d_date#48 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#7], [ws_item_sk#15], [d_date#48 ASC NULLS FIRST]
                  :           +- *(4) Sort [ws_item_sk#15 ASC NULLS FIRST, d_date#48 ASC NULLS FIRST], false, 0
                  :              +- Exchange hashpartitioning(ws_item_sk#15, 200)
                  :                 +- *(3) HashAggregate(keys=[ws_item_sk#15, d_date#48], functions=[sum(ws_sales_price#33)], output=[item_sk#6, d_date#48, _w0#76, ws_item_sk#15])
                  :                    +- Exchange hashpartitioning(ws_item_sk#15, d_date#48, 200)
                  :                       +- *(2) HashAggregate(keys=[ws_item_sk#15, d_date#48], functions=[partial_sum(ws_sales_price#33)], output=[ws_item_sk#15, d_date#48, sum#134])
                  :                          +- *(2) Project [ws_item_sk#15, ws_sales_price#33, d_date#48]
                  :                             +- *(2) BroadcastHashJoin [ws_sold_date_sk#12], [d_date_sk#46], Inner, BuildRight
                  :                                :- *(2) Project [ws_sold_date_sk#12, ws_item_sk#15, ws_sales_price#33]
                  :                                :  +- *(2) Filter (isnotnull(ws_item_sk#15) && isnotnull(ws_sold_date_sk#12))
                  :                                :     +- *(2) FileScan parquet tpcds.web_sales[ws_sold_date_sk#12,ws_item_sk#15,ws_sales_price#33] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_sales_price:double>
                  :                                +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :                                   +- *(1) Project [d_date_sk#46, d_date#48]
                  :                                      +- *(1) Filter (((isnotnull(d_month_seq#49) && (d_month_seq#49 >= 1200)) && (d_month_seq#49 <= 1211)) && isnotnull(d_date_sk#46))
                  :                                         +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#46,d_date#48,d_month_seq#49] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_date:string,d_month_seq:int>
                  +- *(12) Sort [item_sk#8 ASC NULLS FIRST, d_date#105 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(item_sk#8, d_date#105, 200)
                        +- *(11) Project [item_sk#8, d_date#105, cume_sales#9]
                           +- Window [sum(_w0#102) windowspecdefinition(ss_item_sk#79, d_date#105 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS cume_sales#9], [ss_item_sk#79], [d_date#105 ASC NULLS FIRST]
                              +- *(10) Sort [ss_item_sk#79 ASC NULLS FIRST, d_date#105 ASC NULLS FIRST], false, 0
                                 +- Exchange hashpartitioning(ss_item_sk#79, 200)
                                    +- *(9) HashAggregate(keys=[ss_item_sk#79, d_date#105], functions=[sum(ss_sales_price#90)], output=[item_sk#8, d_date#105, _w0#102, ss_item_sk#79])
                                       +- Exchange hashpartitioning(ss_item_sk#79, d_date#105, 200)
                                          +- *(8) HashAggregate(keys=[ss_item_sk#79, d_date#105], functions=[partial_sum(ss_sales_price#90)], output=[ss_item_sk#79, d_date#105, sum#136])
                                             +- *(8) Project [ss_item_sk#79, ss_sales_price#90, d_date#105]
                                                +- *(8) BroadcastHashJoin [ss_sold_date_sk#77], [d_date_sk#103], Inner, BuildRight
                                                   :- *(8) Project [ss_sold_date_sk#77, ss_item_sk#79, ss_sales_price#90]
                                                   :  +- *(8) Filter (isnotnull(ss_item_sk#79) && isnotnull(ss_sold_date_sk#77))
                                                   :     +- *(8) FileScan parquet tpcds.store_sales[ss_sold_date_sk#77,ss_item_sk#79,ss_sales_price#90] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_sales_price:double>
                                                   +- ReusedExchange [d_date_sk#103, d_date#105], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 4.233 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 51 in stream 0 using template query51.tpl
------------------------------------------------------^^^

