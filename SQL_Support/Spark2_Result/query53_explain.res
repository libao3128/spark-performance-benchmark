== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['avg_quarterly_sales ASC NULLS FIRST, 'sum_sales ASC NULLS FIRST, 'i_manufact_id ASC NULLS FIRST], true
      +- 'Project [*]
         +- 'Filter (CASE WHEN ('avg_quarterly_sales > 0) THEN ('abs(('sum_sales - 'avg_quarterly_sales)) / 'avg_quarterly_sales) ELSE null END > 0.1)
            +- 'SubqueryAlias `tmp1`
               +- 'Aggregate ['i_manufact_id, 'd_qoy], ['i_manufact_id, 'sum('ss_sales_price) AS sum_sales#0, 'avg('sum('ss_sales_price)) windowspecdefinition('i_manufact_id, unspecifiedframe$()) AS avg_quarterly_sales#1]
                  +- 'Filter (((('ss_item_sk = 'i_item_sk) && ('ss_sold_date_sk = 'd_date_sk)) && ('ss_store_sk = 's_store_sk)) && ('d_month_seq IN (1200,(1200 + 1),(1200 + 2),(1200 + 3),(1200 + 4),(1200 + 5),(1200 + 6),(1200 + 7),(1200 + 8),(1200 + 9),(1200 + 10),(1200 + 11)) && ((('i_category IN (Books,Children,Electronics) && 'i_class IN (personal,portable,reference,self-help)) && 'i_brand IN (scholaramalgamalg #14,scholaramalgamalg #7,exportiunivamalg #9,scholaramalgamalg #9)) || (('i_category IN (Women,Music,Men) && 'i_class IN (accessories,classical,fragrances,pants)) && 'i_brand IN (amalgimporto #1,edu packscholar #1,exportiimporto #1,importoamalg #1)))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'Join Inner
                        :  :  :- 'UnresolvedRelation `item`
                        :  :  +- 'UnresolvedRelation `store_sales`
                        :  +- 'UnresolvedRelation `date_dim`
                        +- 'UnresolvedRelation `store`

== Analyzed Logical Plan ==
i_manufact_id: int, sum_sales: double, avg_quarterly_sales: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [avg_quarterly_sales#1 ASC NULLS FIRST, sum_sales#0 ASC NULLS FIRST, i_manufact_id#17 ASC NULLS FIRST], true
      +- Project [i_manufact_id#17, sum_sales#0, avg_quarterly_sales#1]
         +- Filter (CASE WHEN (avg_quarterly_sales#1 > cast(0 as double)) THEN (abs((sum_sales#0 - avg_quarterly_sales#1)) / avg_quarterly_sales#1) ELSE cast(null as double) END > cast(0.1 as double))
            +- SubqueryAlias `tmp1`
               +- Project [i_manufact_id#17, sum_sales#0, avg_quarterly_sales#1]
                  +- Project [i_manufact_id#17, sum_sales#0, _w0#109, avg_quarterly_sales#1, avg_quarterly_sales#1]
                     +- Window [avg(_w0#109) windowspecdefinition(i_manufact_id#17, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_quarterly_sales#1], [i_manufact_id#17]
                        +- Aggregate [i_manufact_id#17, d_qoy#59], [i_manufact_id#17, sum(ss_sales_price#39) AS sum_sales#0, sum(ss_sales_price#39) AS _w0#109]
                           +- Filter ((((ss_item_sk#28 = i_item_sk#4) && (ss_sold_date_sk#26 = d_date_sk#49)) && (ss_store_sk#33 = s_store_sk#77)) && (d_month_seq#52 IN (1200,(1200 + 1),(1200 + 2),(1200 + 3),(1200 + 4),(1200 + 5),(1200 + 6),(1200 + 7),(1200 + 8),(1200 + 9),(1200 + 10),(1200 + 11)) && (((i_category#16 IN (Books,Children,Electronics) && i_class#14 IN (personal,portable,reference,self-help)) && i_brand#12 IN (scholaramalgamalg #14,scholaramalgamalg #7,exportiunivamalg #9,scholaramalgamalg #9)) || ((i_category#16 IN (Women,Music,Men) && i_class#14 IN (accessories,classical,fragrances,pants)) && i_brand#12 IN (amalgimporto #1,edu packscholar #1,exportiimporto #1,importoamalg #1)))))
                              +- Join Inner
                                 :- Join Inner
                                 :  :- Join Inner
                                 :  :  :- SubqueryAlias `tpcds`.`item`
                                 :  :  :  +- Relation[i_item_sk#4,i_item_id#5,i_rec_start_date#6,i_rec_end_date#7,i_item_desc#8,i_current_price#9,i_wholesale_cost#10,i_brand_id#11,i_brand#12,i_class_id#13,i_class#14,i_category_id#15,i_category#16,i_manufact_id#17,i_manufact#18,i_size#19,i_formulation#20,i_color#21,i_units#22,i_container#23,i_manager_id#24,i_product_name#25] parquet
                                 :  :  +- SubqueryAlias `tpcds`.`store_sales`
                                 :  :     +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
                                 :  +- SubqueryAlias `tpcds`.`date_dim`
                                 :     +- Relation[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet
                                 +- SubqueryAlias `tpcds`.`store`
                                    +- Relation[s_store_sk#77,s_store_id#78,s_rec_start_date#79,s_rec_end_date#80,s_closed_date_sk#81,s_store_name#82,s_number_employees#83,s_floor_space#84,s_hours#85,s_manager#86,s_market_id#87,s_geography_class#88,s_market_desc#89,s_market_manager#90,s_division_id#91,s_division_name#92,s_company_id#93,s_company_name#94,s_street_number#95,s_street_name#96,s_street_type#97,s_suite_number#98,s_city#99,s_county#100,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [avg_quarterly_sales#1 ASC NULLS FIRST, sum_sales#0 ASC NULLS FIRST, i_manufact_id#17 ASC NULLS FIRST], true
      +- Project [i_manufact_id#17, sum_sales#0, avg_quarterly_sales#1]
         +- Filter (CASE WHEN (avg_quarterly_sales#1 > 0.0) THEN (abs((sum_sales#0 - avg_quarterly_sales#1)) / avg_quarterly_sales#1) ELSE null END > 0.1)
            +- Window [avg(_w0#109) windowspecdefinition(i_manufact_id#17, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_quarterly_sales#1], [i_manufact_id#17]
               +- Aggregate [i_manufact_id#17, d_qoy#59], [i_manufact_id#17, sum(ss_sales_price#39) AS sum_sales#0, sum(ss_sales_price#39) AS _w0#109]
                  +- Project [i_manufact_id#17, ss_sales_price#39, d_qoy#59]
                     +- Join Inner, (ss_store_sk#33 = s_store_sk#77)
                        :- Project [i_manufact_id#17, ss_store_sk#33, ss_sales_price#39, d_qoy#59]
                        :  +- Join Inner, (ss_sold_date_sk#26 = d_date_sk#49)
                        :     :- Project [i_manufact_id#17, ss_sold_date_sk#26, ss_store_sk#33, ss_sales_price#39]
                        :     :  +- Join Inner, (ss_item_sk#28 = i_item_sk#4)
                        :     :     :- Project [i_item_sk#4, i_manufact_id#17]
                        :     :     :  +- Filter ((((i_category#16 IN (Books,Children,Electronics) && i_class#14 IN (personal,portable,reference,self-help)) && i_brand#12 IN (scholaramalgamalg #14,scholaramalgamalg #7,exportiunivamalg #9,scholaramalgamalg #9)) || ((i_category#16 IN (Women,Music,Men) && i_class#14 IN (accessories,classical,fragrances,pants)) && i_brand#12 IN (amalgimporto #1,edu packscholar #1,exportiimporto #1,importoamalg #1))) && isnotnull(i_item_sk#4))
                        :     :     :     +- Relation[i_item_sk#4,i_item_id#5,i_rec_start_date#6,i_rec_end_date#7,i_item_desc#8,i_current_price#9,i_wholesale_cost#10,i_brand_id#11,i_brand#12,i_class_id#13,i_class#14,i_category_id#15,i_category#16,i_manufact_id#17,i_manufact#18,i_size#19,i_formulation#20,i_color#21,i_units#22,i_container#23,i_manager_id#24,i_product_name#25] parquet
                        :     :     +- Project [ss_sold_date_sk#26, ss_item_sk#28, ss_store_sk#33, ss_sales_price#39]
                        :     :        +- Filter ((isnotnull(ss_item_sk#28) && isnotnull(ss_sold_date_sk#26)) && isnotnull(ss_store_sk#33))
                        :     :           +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
                        :     +- Project [d_date_sk#49, d_qoy#59]
                        :        +- Filter (d_month_seq#52 INSET (1200,1211,1205,1201,1206,1210,1207,1202,1209,1203,1208,1204) && isnotnull(d_date_sk#49))
                        :           +- Relation[d_date_sk#49,d_date_id#50,d_date#51,d_month_seq#52,d_week_seq#53,d_quarter_seq#54,d_year#55,d_dow#56,d_moy#57,d_dom#58,d_qoy#59,d_fy_year#60,d_fy_quarter_seq#61,d_fy_week_seq#62,d_day_name#63,d_quarter_name#64,d_holiday#65,d_weekend#66,d_following_holiday#67,d_first_dom#68,d_last_dom#69,d_same_day_ly#70,d_same_day_lq#71,d_current_day#72,... 4 more fields] parquet
                        +- Project [s_store_sk#77]
                           +- Filter isnotnull(s_store_sk#77)
                              +- Relation[s_store_sk#77,s_store_id#78,s_rec_start_date#79,s_rec_end_date#80,s_closed_date_sk#81,s_store_name#82,s_number_employees#83,s_floor_space#84,s_hours#85,s_manager#86,s_market_id#87,s_geography_class#88,s_market_desc#89,s_market_manager#90,s_division_id#91,s_division_name#92,s_company_id#93,s_company_name#94,s_street_number#95,s_street_name#96,s_street_type#97,s_suite_number#98,s_city#99,s_county#100,... 5 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[avg_quarterly_sales#1 ASC NULLS FIRST,sum_sales#0 ASC NULLS FIRST,i_manufact_id#17 ASC NULLS FIRST], output=[i_manufact_id#17,sum_sales#0,avg_quarterly_sales#1])
+- *(7) Project [i_manufact_id#17, sum_sales#0, avg_quarterly_sales#1]
   +- *(7) Filter (CASE WHEN (avg_quarterly_sales#1 > 0.0) THEN (abs((sum_sales#0 - avg_quarterly_sales#1)) / avg_quarterly_sales#1) ELSE null END > 0.1)
      +- Window [avg(_w0#109) windowspecdefinition(i_manufact_id#17, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_quarterly_sales#1], [i_manufact_id#17]
         +- *(6) Sort [i_manufact_id#17 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(i_manufact_id#17, 200)
               +- *(5) HashAggregate(keys=[i_manufact_id#17, d_qoy#59], functions=[sum(ss_sales_price#39)], output=[i_manufact_id#17, sum_sales#0, _w0#109])
                  +- Exchange hashpartitioning(i_manufact_id#17, d_qoy#59, 200)
                     +- *(4) HashAggregate(keys=[i_manufact_id#17, d_qoy#59], functions=[partial_sum(ss_sales_price#39)], output=[i_manufact_id#17, d_qoy#59, sum#111])
                        +- *(4) Project [i_manufact_id#17, ss_sales_price#39, d_qoy#59]
                           +- *(4) BroadcastHashJoin [ss_store_sk#33], [s_store_sk#77], Inner, BuildRight
                              :- *(4) Project [i_manufact_id#17, ss_store_sk#33, ss_sales_price#39, d_qoy#59]
                              :  +- *(4) BroadcastHashJoin [ss_sold_date_sk#26], [d_date_sk#49], Inner, BuildRight
                              :     :- *(4) Project [i_manufact_id#17, ss_sold_date_sk#26, ss_store_sk#33, ss_sales_price#39]
                              :     :  +- *(4) BroadcastHashJoin [i_item_sk#4], [ss_item_sk#28], Inner, BuildLeft
                              :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              :     :     :  +- *(1) Project [i_item_sk#4, i_manufact_id#17]
                              :     :     :     +- *(1) Filter ((((i_category#16 IN (Books,Children,Electronics) && i_class#14 IN (personal,portable,reference,self-help)) && i_brand#12 IN (scholaramalgamalg #14,scholaramalgamalg #7,exportiunivamalg #9,scholaramalgamalg #9)) || ((i_category#16 IN (Women,Music,Men) && i_class#14 IN (accessories,classical,fragrances,pants)) && i_brand#12 IN (amalgimporto #1,edu packscholar #1,exportiimporto #1,importoamalg #1))) && isnotnull(i_item_sk#4))
                              :     :     :        +- *(1) FileScan parquet tpcds.item[i_item_sk#4,i_brand#12,i_class#14,i_category#16,i_manufact_id#17] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [Or(And(And(In(i_category, [Books,Children,Electronics]),In(i_class, [personal,portable,reference..., ReadSchema: struct<i_item_sk:int,i_brand:string,i_class:string,i_category:string,i_manufact_id:int>
                              :     :     +- *(4) Project [ss_sold_date_sk#26, ss_item_sk#28, ss_store_sk#33, ss_sales_price#39]
                              :     :        +- *(4) Filter ((isnotnull(ss_item_sk#28) && isnotnull(ss_sold_date_sk#26)) && isnotnull(ss_store_sk#33))
                              :     :           +- *(4) FileScan parquet tpcds.store_sales[ss_sold_date_sk#26,ss_item_sk#28,ss_store_sk#33,ss_sales_price#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
                              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              :        +- *(2) Project [d_date_sk#49, d_qoy#59]
                              :           +- *(2) Filter (d_month_seq#52 INSET (1200,1211,1205,1201,1206,1210,1207,1202,1209,1203,1208,1204) && isnotnull(d_date_sk#49))
                              :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#49,d_month_seq#52,d_qoy#59] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [In(d_month_seq, [1200,1211,1205,1201,1206,1210,1207,1202,1209,1203,1208,1204]), IsNotNull(d_date..., ReadSchema: struct<d_date_sk:int,d_month_seq:int,d_qoy:int>
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 +- *(3) Project [s_store_sk#77]
                                    +- *(3) Filter isnotnull(s_store_sk#77)
                                       +- *(3) FileScan parquet tpcds.store[s_store_sk#77] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int>
Time taken: 4.282 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 53 in stream 0 using template query53.tpl
------------------------------------------------------^^^

