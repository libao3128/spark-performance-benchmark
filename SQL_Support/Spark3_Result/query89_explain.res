Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583040087
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort [('sum_sales - 'avg_monthly_sales) ASC NULLS FIRST, 's_store_name ASC NULLS FIRST], true
      +- 'Project [*]
         +- 'Filter (CASE WHEN NOT ('avg_monthly_sales = 0) THEN ('abs(('sum_sales - 'avg_monthly_sales)) / 'avg_monthly_sales) ELSE null END > 0.1)
            +- 'SubqueryAlias tmp1
               +- 'Aggregate ['i_category, 'i_class, 'i_brand, 's_store_name, 's_company_name, 'd_moy], ['i_category, 'i_class, 'i_brand, 's_store_name, 's_company_name, 'd_moy, 'sum('ss_sales_price) AS sum_sales#0, 'avg('sum('ss_sales_price)) windowspecdefinition('i_category, 'i_brand, 's_store_name, 's_company_name, unspecifiedframe$()) AS avg_monthly_sales#1]
                  +- 'Filter (((('ss_item_sk = 'i_item_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND ('ss_store_sk = 's_store_sk)) AND ('d_year IN (1999) AND (('i_category IN (Books,Electronics,Sports) AND 'i_class IN (computers,stereo,football)) OR ('i_category IN (Men,Jewelry,Women) AND 'i_class IN (shirts,birdal,dresses)))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'Join Inner
                        :  :  :- 'UnresolvedRelation [item], [], false
                        :  :  +- 'UnresolvedRelation [store_sales], [], false
                        :  +- 'UnresolvedRelation [date_dim], [], false
                        +- 'UnresolvedRelation [store], [], false

== Analyzed Logical Plan ==
i_category: string, i_class: string, i_brand: string, s_store_name: string, s_company_name: string, d_moy: int, sum_sales: double, avg_monthly_sales: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, s_store_name#85 ASC NULLS FIRST], true
      +- Project [i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, sum_sales#0, avg_monthly_sales#1]
         +- Filter (CASE WHEN NOT (avg_monthly_sales#1 = cast(0 as double)) THEN (abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) ELSE cast(null as double) END > cast(0.1 as double))
            +- SubqueryAlias tmp1
               +- Project [i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, sum_sales#0, avg_monthly_sales#1]
                  +- Project [i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, sum_sales#0, _w0#116, avg_monthly_sales#1, avg_monthly_sales#1]
                     +- Window [avg(_w0#116) windowspecdefinition(i_category#19, i_brand#15, s_store_name#85, s_company_name#97, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#19, i_brand#15, s_store_name#85, s_company_name#97]
                        +- Aggregate [i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60], [i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, sum(ss_sales_price#42) AS sum_sales#0, sum(ss_sales_price#42) AS _w0#116]
                           +- Filter ((((ss_item_sk#31 = i_item_sk#7) AND (ss_sold_date_sk#29 = d_date_sk#52)) AND (ss_store_sk#36 = s_store_sk#80)) AND (d_year#58 IN (1999) AND ((i_category#19 IN (Books,Electronics,Sports) AND i_class#17 IN (computers,stereo,football)) OR (i_category#19 IN (Men,Jewelry,Women) AND i_class#17 IN (shirts,birdal,dresses)))))
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
   +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, s_store_name#85 ASC NULLS FIRST], true
      +- Project [i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, sum_sales#0, avg_monthly_sales#1]
         +- Filter CASE WHEN NOT (avg_monthly_sales#1 = 0.0) THEN ((abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) > 0.1) END
            +- Window [avg(_w0#116) windowspecdefinition(i_category#19, i_brand#15, s_store_name#85, s_company_name#97, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#19, i_brand#15, s_store_name#85, s_company_name#97]
               +- Aggregate [i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60], [i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, sum(ss_sales_price#42) AS sum_sales#0, sum(ss_sales_price#42) AS _w0#116]
                  +- Project [i_brand#15, i_class#17, i_category#19, ss_sales_price#42, d_moy#60, s_store_name#85, s_company_name#97]
                     +- Join Inner, (ss_store_sk#36 = s_store_sk#80)
                        :- Project [i_brand#15, i_class#17, i_category#19, ss_store_sk#36, ss_sales_price#42, d_moy#60]
                        :  +- Join Inner, (ss_sold_date_sk#29 = d_date_sk#52)
                        :     :- Project [i_brand#15, i_class#17, i_category#19, ss_sold_date_sk#29, ss_store_sk#36, ss_sales_price#42]
                        :     :  +- Join Inner, (ss_item_sk#31 = i_item_sk#7)
                        :     :     :- Project [i_item_sk#7, i_brand#15, i_class#17, i_category#19]
                        :     :     :  +- Filter (((i_category#19 IN (Books,Electronics,Sports) AND i_class#17 IN (computers,stereo,football)) OR (i_category#19 IN (Men,Jewelry,Women) AND i_class#17 IN (shirts,birdal,dresses))) AND isnotnull(i_item_sk#7))
                        :     :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#7,i_item_id#8,i_rec_start_date#9,i_rec_end_date#10,i_item_desc#11,i_current_price#12,i_wholesale_cost#13,i_brand_id#14,i_brand#15,i_class_id#16,i_class#17,i_category_id#18,i_category#19,i_manufact_id#20,i_manufact#21,i_size#22,i_formulation#23,i_color#24,i_units#25,i_container#26,i_manager_id#27,i_product_name#28] parquet
                        :     :     +- Project [ss_sold_date_sk#29, ss_item_sk#31, ss_store_sk#36, ss_sales_price#42]
                        :     :        +- Filter (isnotnull(ss_item_sk#31) AND (isnotnull(ss_sold_date_sk#29) AND isnotnull(ss_store_sk#36)))
                        :     :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
                        :     +- Project [d_date_sk#52, d_moy#60]
                        :        +- Filter ((isnotnull(d_year#58) AND (d_year#58 = 1999)) AND isnotnull(d_date_sk#52))
                        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet
                        +- Project [s_store_sk#80, s_store_name#85, s_company_name#97]
                           +- Filter isnotnull(s_store_sk#80)
                              +- Relation spark_catalog.tpcds.store[s_store_sk#80,s_store_id#81,s_rec_start_date#82,s_rec_end_date#83,s_closed_date_sk#84,s_store_name#85,s_number_employees#86,s_floor_space#87,s_hours#88,s_manager#89,s_market_id#90,s_geography_class#91,s_market_desc#92,s_market_manager#93,s_division_id#94,s_division_name#95,s_company_id#96,s_company_name#97,s_street_number#98,s_street_name#99,s_street_type#100,s_suite_number#101,s_city#102,s_county#103,... 5 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST,s_store_name#85 ASC NULLS FIRST], output=[i_category#19,i_class#17,i_brand#15,s_store_name#85,s_company_name#97,d_moy#60,sum_sales#0,avg_monthly_sales#1])
   +- Project [i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, sum_sales#0, avg_monthly_sales#1]
      +- Filter CASE WHEN NOT (avg_monthly_sales#1 = 0.0) THEN ((abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) > 0.1) END
         +- Window [avg(_w0#116) windowspecdefinition(i_category#19, i_brand#15, s_store_name#85, s_company_name#97, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#19, i_brand#15, s_store_name#85, s_company_name#97]
            +- Sort [i_category#19 ASC NULLS FIRST, i_brand#15 ASC NULLS FIRST, s_store_name#85 ASC NULLS FIRST, s_company_name#97 ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning(i_category#19, i_brand#15, s_store_name#85, s_company_name#97, 200), ENSURE_REQUIREMENTS, [plan_id=112]
                  +- HashAggregate(keys=[i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60], functions=[sum(ss_sales_price#42)], output=[i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, sum_sales#0, _w0#116])
                     +- Exchange hashpartitioning(i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, 200), ENSURE_REQUIREMENTS, [plan_id=109]
                        +- HashAggregate(keys=[i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60], functions=[partial_sum(ss_sales_price#42)], output=[i_category#19, i_class#17, i_brand#15, s_store_name#85, s_company_name#97, d_moy#60, sum#118])
                           +- Project [i_brand#15, i_class#17, i_category#19, ss_sales_price#42, d_moy#60, s_store_name#85, s_company_name#97]
                              +- BroadcastHashJoin [ss_store_sk#36], [s_store_sk#80], Inner, BuildRight, false
                                 :- Project [i_brand#15, i_class#17, i_category#19, ss_store_sk#36, ss_sales_price#42, d_moy#60]
                                 :  +- BroadcastHashJoin [ss_sold_date_sk#29], [d_date_sk#52], Inner, BuildRight, false
                                 :     :- Project [i_brand#15, i_class#17, i_category#19, ss_sold_date_sk#29, ss_store_sk#36, ss_sales_price#42]
                                 :     :  +- BroadcastHashJoin [i_item_sk#7], [ss_item_sk#31], Inner, BuildLeft, false
                                 :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=96]
                                 :     :     :  +- Filter (((i_category#19 IN (Books,Electronics,Sports) AND i_class#17 IN (computers,stereo,football)) OR (i_category#19 IN (Men,Jewelry,Women) AND i_class#17 IN (shirts,birdal,dresses))) AND isnotnull(i_item_sk#7))
                                 :     :     :     +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#7,i_brand#15,i_class#17,i_category#19] Batched: true, DataFilters: [((i_category#19 IN (Books,Electronics,Sports) AND i_class#17 IN (computers,stereo,football)) OR ..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(And(In(i_category, [Books,Electronics,Sports]),In(i_class, [computers,football,stereo])),And(..., ReadSchema: struct<i_item_sk:int,i_brand:string,i_class:string,i_category:string>
                                 :     :     +- Filter ((isnotnull(ss_item_sk#31) AND isnotnull(ss_sold_date_sk#29)) AND isnotnull(ss_store_sk#36))
                                 :     :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#29,ss_item_sk#31,ss_store_sk#36,ss_sales_price#42] Batched: true, DataFilters: [isnotnull(ss_item_sk#31), isnotnull(ss_sold_date_sk#29), isnotnull(ss_store_sk#36)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=100]
                                 :        +- Project [d_date_sk#52, d_moy#60]
                                 :           +- Filter ((isnotnull(d_year#58) AND (d_year#58 = 1999)) AND isnotnull(d_date_sk#52))
                                 :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#52,d_year#58,d_moy#60] Batched: true, DataFilters: [isnotnull(d_year#58), (d_year#58 = 1999), isnotnull(d_date_sk#52)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,1999), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=104]
                                    +- Filter isnotnull(s_store_sk#80)
                                       +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#80,s_store_name#85,s_company_name#97] Batched: true, DataFilters: [isnotnull(s_store_sk#80)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_company_name:string>

Time taken: 2.996 seconds, Fetched 1 row(s)
