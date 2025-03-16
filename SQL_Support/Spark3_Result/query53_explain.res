Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581599840
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['avg_quarterly_sales ASC NULLS FIRST, 'sum_sales ASC NULLS FIRST, 'i_manufact_id ASC NULLS FIRST], true
      +- 'Project [*]
         +- 'Filter (CASE WHEN ('avg_quarterly_sales > 0) THEN ('abs(('sum_sales - 'avg_quarterly_sales)) / 'avg_quarterly_sales) ELSE null END > 0.1)
            +- 'SubqueryAlias tmp1
               +- 'Aggregate ['i_manufact_id, 'd_qoy], ['i_manufact_id, 'sum('ss_sales_price) AS sum_sales#0, 'avg('sum('ss_sales_price)) windowspecdefinition('i_manufact_id, unspecifiedframe$()) AS avg_quarterly_sales#1]
                  +- 'Filter (((('ss_item_sk = 'i_item_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND ('ss_store_sk = 's_store_sk)) AND ('d_month_seq IN (1200,(1200 + 1),(1200 + 2),(1200 + 3),(1200 + 4),(1200 + 5),(1200 + 6),(1200 + 7),(1200 + 8),(1200 + 9),(1200 + 10),(1200 + 11)) AND ((('i_category IN (Books,Children,Electronics) AND 'i_class IN (personal,portable,reference,self-help)) AND 'i_brand IN (scholaramalgamalg #14,scholaramalgamalg #7,exportiunivamalg #9,scholaramalgamalg #9)) OR (('i_category IN (Women,Music,Men) AND 'i_class IN (accessories,classical,fragrances,pants)) AND 'i_brand IN (amalgimporto #1,edu packscholar #1,exportiimporto #1,importoamalg #1)))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'Join Inner
                        :  :  :- 'UnresolvedRelation [item], [], false
                        :  :  +- 'UnresolvedRelation [store_sales], [], false
                        :  +- 'UnresolvedRelation [date_dim], [], false
                        +- 'UnresolvedRelation [store], [], false

== Analyzed Logical Plan ==
i_manufact_id: int, sum_sales: double, avg_quarterly_sales: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [avg_quarterly_sales#1 ASC NULLS FIRST, sum_sales#0 ASC NULLS FIRST, i_manufact_id#20 ASC NULLS FIRST], true
      +- Project [i_manufact_id#20, sum_sales#0, avg_quarterly_sales#1]
         +- Filter (CASE WHEN (avg_quarterly_sales#1 > cast(0 as double)) THEN (abs((sum_sales#0 - avg_quarterly_sales#1)) / avg_quarterly_sales#1) ELSE cast(null as double) END > cast(0.1 as double))
            +- SubqueryAlias tmp1
               +- Project [i_manufact_id#20, sum_sales#0, avg_quarterly_sales#1]
                  +- Project [i_manufact_id#20, sum_sales#0, _w0#116, avg_quarterly_sales#1, avg_quarterly_sales#1]
                     +- Window [avg(_w0#116) windowspecdefinition(i_manufact_id#20, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_quarterly_sales#1], [i_manufact_id#20]
                        +- Aggregate [i_manufact_id#20, d_qoy#62], [i_manufact_id#20, sum(ss_sales_price#42) AS sum_sales#0, sum(ss_sales_price#42) AS _w0#116]
                           +- Filter ((((ss_item_sk#31 = i_item_sk#7) AND (ss_sold_date_sk#29 = d_date_sk#52)) AND (ss_store_sk#36 = s_store_sk#80)) AND (d_month_seq#55 IN (1200,(1200 + 1),(1200 + 2),(1200 + 3),(1200 + 4),(1200 + 5),(1200 + 6),(1200 + 7),(1200 + 8),(1200 + 9),(1200 + 10),(1200 + 11)) AND (((i_category#19 IN (Books,Children,Electronics) AND i_class#17 IN (personal,portable,reference,self-help)) AND i_brand#15 IN (scholaramalgamalg #14,scholaramalgamalg #7,exportiunivamalg #9,scholaramalgamalg #9)) OR ((i_category#19 IN (Women,Music,Men) AND i_class#17 IN (accessories,classical,fragrances,pants)) AND i_brand#15 IN (amalgimporto #1,edu packscholar #1,exportiimporto #1,importoamalg #1)))))
                              +- Join Inner
                                 :- Join Inner
                                 :  :- Join Inner
                                 :  :  :- SubqueryAlias spark_catalog.tpcds.item
                                 :  :  :  +- Relation spark_catalog.tpcds.item[i_item_sk#7,i_item_id#8,i_rec_start_date#9,i_rec_end_date#10,i_item_desc#11,i_current_price#12,i_wholesale_cost#13,i_brand_id#14,i_brand#15,i_class_id#16,i_class#17,i_category_id#18,i_category#19,i_manufact_id#20,i_manufact#21,i_size#22,i_formulation#23,i_color#24,i_units#25,i_container#26,i_manager_id#27,i_product_name#28] parquet
                                 :  :  +- SubqueryAlias spark_catalog.tpcds.store_sales
                                 :  :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
                                 :  +- SubqueryAlias spark_catalog.tpcds.date_dim
                                 :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet
                                 +- SubqueryAlias spark_catalog.tpcds.store
                                    +- Relation spark_catalog.tpcds.store[s_store_sk#80,s_store_id#81,s_rec_start_date#82,s_rec_end_date#83,s_closed_date_sk#84,s_store_name#85,s_number_employees#86,s_floor_space#87,s_hours#88,s_manager#89,s_market_id#90,s_geography_class#91,s_market_desc#92,s_market_manager#93,s_division_id#94,s_division_name#95,s_company_id#96,s_company_name#97,s_street_number#98,s_street_name#99,s_street_type#100,s_suite_number#101,s_city#102,s_county#103,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [avg_quarterly_sales#1 ASC NULLS FIRST, sum_sales#0 ASC NULLS FIRST, i_manufact_id#20 ASC NULLS FIRST], true
      +- Project [i_manufact_id#20, sum_sales#0, avg_quarterly_sales#1]
         +- Filter CASE WHEN (avg_quarterly_sales#1 > 0.0) THEN ((abs((sum_sales#0 - avg_quarterly_sales#1)) / avg_quarterly_sales#1) > 0.1) ELSE false END
            +- Window [avg(_w0#116) windowspecdefinition(i_manufact_id#20, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_quarterly_sales#1], [i_manufact_id#20]
               +- Aggregate [i_manufact_id#20, d_qoy#62], [i_manufact_id#20, sum(ss_sales_price#42) AS sum_sales#0, sum(ss_sales_price#42) AS _w0#116]
                  +- Project [i_manufact_id#20, ss_sales_price#42, d_qoy#62]
                     +- Join Inner, (ss_store_sk#36 = s_store_sk#80)
                        :- Project [i_manufact_id#20, ss_store_sk#36, ss_sales_price#42, d_qoy#62]
                        :  +- Join Inner, (ss_sold_date_sk#29 = d_date_sk#52)
                        :     :- Project [i_manufact_id#20, ss_sold_date_sk#29, ss_store_sk#36, ss_sales_price#42]
                        :     :  +- Join Inner, (ss_item_sk#31 = i_item_sk#7)
                        :     :     :- Project [i_item_sk#7, i_manufact_id#20]
                        :     :     :  +- Filter ((((i_category#19 IN (Books,Children,Electronics) AND i_class#17 IN (personal,portable,reference,self-help)) AND i_brand#15 IN (scholaramalgamalg #14,scholaramalgamalg #7,exportiunivamalg #9,scholaramalgamalg #9)) OR ((i_category#19 IN (Women,Music,Men) AND i_class#17 IN (accessories,classical,fragrances,pants)) AND i_brand#15 IN (amalgimporto #1,edu packscholar #1,exportiimporto #1,importoamalg #1))) AND isnotnull(i_item_sk#7))
                        :     :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#7,i_item_id#8,i_rec_start_date#9,i_rec_end_date#10,i_item_desc#11,i_current_price#12,i_wholesale_cost#13,i_brand_id#14,i_brand#15,i_class_id#16,i_class#17,i_category_id#18,i_category#19,i_manufact_id#20,i_manufact#21,i_size#22,i_formulation#23,i_color#24,i_units#25,i_container#26,i_manager_id#27,i_product_name#28] parquet
                        :     :     +- Project [ss_sold_date_sk#29, ss_item_sk#31, ss_store_sk#36, ss_sales_price#42]
                        :     :        +- Filter (isnotnull(ss_item_sk#31) AND (isnotnull(ss_sold_date_sk#29) AND isnotnull(ss_store_sk#36)))
                        :     :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
                        :     +- Project [d_date_sk#52, d_qoy#62]
                        :        +- Filter (d_month_seq#55 INSET 1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211 AND isnotnull(d_date_sk#52))
                        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet
                        +- Project [s_store_sk#80]
                           +- Filter isnotnull(s_store_sk#80)
                              +- Relation spark_catalog.tpcds.store[s_store_sk#80,s_store_id#81,s_rec_start_date#82,s_rec_end_date#83,s_closed_date_sk#84,s_store_name#85,s_number_employees#86,s_floor_space#87,s_hours#88,s_manager#89,s_market_id#90,s_geography_class#91,s_market_desc#92,s_market_manager#93,s_division_id#94,s_division_name#95,s_company_id#96,s_company_name#97,s_street_number#98,s_street_name#99,s_street_type#100,s_suite_number#101,s_city#102,s_county#103,... 5 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[avg_quarterly_sales#1 ASC NULLS FIRST,sum_sales#0 ASC NULLS FIRST,i_manufact_id#20 ASC NULLS FIRST], output=[i_manufact_id#20,sum_sales#0,avg_quarterly_sales#1])
   +- Project [i_manufact_id#20, sum_sales#0, avg_quarterly_sales#1]
      +- Filter CASE WHEN (avg_quarterly_sales#1 > 0.0) THEN ((abs((sum_sales#0 - avg_quarterly_sales#1)) / avg_quarterly_sales#1) > 0.1) ELSE false END
         +- Window [avg(_w0#116) windowspecdefinition(i_manufact_id#20, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_quarterly_sales#1], [i_manufact_id#20]
            +- Sort [i_manufact_id#20 ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning(i_manufact_id#20, 200), ENSURE_REQUIREMENTS, [plan_id=112]
                  +- HashAggregate(keys=[i_manufact_id#20, d_qoy#62], functions=[sum(ss_sales_price#42)], output=[i_manufact_id#20, sum_sales#0, _w0#116])
                     +- Exchange hashpartitioning(i_manufact_id#20, d_qoy#62, 200), ENSURE_REQUIREMENTS, [plan_id=109]
                        +- HashAggregate(keys=[i_manufact_id#20, d_qoy#62], functions=[partial_sum(ss_sales_price#42)], output=[i_manufact_id#20, d_qoy#62, sum#118])
                           +- Project [i_manufact_id#20, ss_sales_price#42, d_qoy#62]
                              +- BroadcastHashJoin [ss_store_sk#36], [s_store_sk#80], Inner, BuildRight, false
                                 :- Project [i_manufact_id#20, ss_store_sk#36, ss_sales_price#42, d_qoy#62]
                                 :  +- BroadcastHashJoin [ss_sold_date_sk#29], [d_date_sk#52], Inner, BuildRight, false
                                 :     :- Project [i_manufact_id#20, ss_sold_date_sk#29, ss_store_sk#36, ss_sales_price#42]
                                 :     :  +- BroadcastHashJoin [i_item_sk#7], [ss_item_sk#31], Inner, BuildLeft, false
                                 :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=96]
                                 :     :     :  +- Project [i_item_sk#7, i_manufact_id#20]
                                 :     :     :     +- Filter ((((i_category#19 IN (Books,Children,Electronics) AND i_class#17 IN (personal,portable,reference,self-help)) AND i_brand#15 IN (scholaramalgamalg #14,scholaramalgamalg #7,exportiunivamalg #9,scholaramalgamalg #9)) OR ((i_category#19 IN (Women,Music,Men) AND i_class#17 IN (accessories,classical,fragrances,pants)) AND i_brand#15 IN (amalgimporto #1,edu packscholar #1,exportiimporto #1,importoamalg #1))) AND isnotnull(i_item_sk#7))
                                 :     :     :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#7,i_brand#15,i_class#17,i_category#19,i_manufact_id#20] Batched: true, DataFilters: [(((i_category#19 IN (Books,Children,Electronics) AND i_class#17 IN (personal,portable,reference,..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(And(And(In(i_category, [Books,Children,Electronics]),In(i_class, [personal,portable,reference..., ReadSchema: struct<i_item_sk:int,i_brand:string,i_class:string,i_category:string,i_manufact_id:int>
                                 :     :     +- Filter ((isnotnull(ss_item_sk#31) AND isnotnull(ss_sold_date_sk#29)) AND isnotnull(ss_store_sk#36))
                                 :     :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#29,ss_item_sk#31,ss_store_sk#36,ss_sales_price#42] Batched: true, DataFilters: [isnotnull(ss_item_sk#31), isnotnull(ss_sold_date_sk#29), isnotnull(ss_store_sk#36)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=100]
                                 :        +- Project [d_date_sk#52, d_qoy#62]
                                 :           +- Filter (d_month_seq#55 INSET 1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211 AND isnotnull(d_date_sk#52))
                                 :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#52,d_month_seq#55,d_qoy#62] Batched: true, DataFilters: [d_month_seq#55 INSET 1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211, isn..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_month_seq, [1200,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1211]), IsNotNull(d_date..., ReadSchema: struct<d_date_sk:int,d_month_seq:int,d_qoy:int>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=104]
                                    +- Filter isnotnull(s_store_sk#80)
                                       +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#80] Batched: true, DataFilters: [isnotnull(s_store_sk#80)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int>

Time taken: 2.611 seconds, Fetched 1 row(s)
