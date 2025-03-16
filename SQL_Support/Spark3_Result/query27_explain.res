Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580579809
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST, 's_state ASC NULLS FIRST], true
      +- 'Aggregate [rollup(Vector(0), Vector(1), 'i_item_id, 's_state)], ['i_item_id, 's_state, 'grouping('s_state) AS g_state#0, 'avg('ss_quantity) AS agg1#1, 'avg('ss_list_price) AS agg2#2, 'avg('ss_coupon_amt) AS agg3#3, 'avg('ss_sales_price) AS agg4#4]
         +- 'Filter ((((('ss_sold_date_sk = 'd_date_sk) AND ('ss_item_sk = 'i_item_sk)) AND ('ss_store_sk = 's_store_sk)) AND (('ss_cdemo_sk = 'cd_demo_sk) AND ('cd_gender = M))) AND ((('cd_marital_status = S) AND ('cd_education_status = College)) AND (('d_year = 2002) AND 's_state IN (TN,TN,TN,TN,TN,TN))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation [store_sales], [], false
               :  :  :  +- 'UnresolvedRelation [customer_demographics], [], false
               :  :  +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [store], [], false
               +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
i_item_id: string, s_state: string, g_state: tinyint, agg1: double, agg2: double, agg3: double, agg4: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#133 ASC NULLS FIRST, s_state#134 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#133, s_state#134, spark_grouping_id#132L], [i_item_id#133, s_state#134, cast((shiftright(spark_grouping_id#132L, 0) & 1) as tinyint) AS g_state#0, avg(ss_quantity#20) AS agg1#1, avg(ss_list_price#22) AS agg2#2, avg(ss_coupon_amt#29) AS agg3#3, avg(ss_sales_price#23) AS agg4#4]
         +- Expand [[ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, cd_demo_sk#33, ... 90 more fields], [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, cd_demo_sk#33, ... 90 more fields], [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, cd_demo_sk#33, ... 90 more fields]], [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, cd_demo_sk#33, ... 90 more fields]
            +- Project [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, cd_demo_sk#33, ... 89 more fields]
               +- Filter (((((ss_sold_date_sk#10 = d_date_sk#42) AND (ss_item_sk#12 = i_item_sk#99)) AND (ss_store_sk#17 = s_store_sk#70)) AND ((ss_cdemo_sk#14 = cd_demo_sk#33) AND (cd_gender#34 = M))) AND (((cd_marital_status#35 = S) AND (cd_education_status#36 = College)) AND ((d_year#48 = 2002) AND s_state#94 IN (TN,TN,TN,TN,TN,TN))))
                  +- Join Inner
                     :- Join Inner
                     :  :- Join Inner
                     :  :  :- Join Inner
                     :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
                     :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
                     :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer_demographics
                     :  :  :     +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#33,cd_gender#34,cd_marital_status#35,cd_education_status#36,cd_purchase_estimate#37,cd_credit_rating#38,cd_dep_count#39,cd_dep_employed_count#40,cd_dep_college_count#41] parquet
                     :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
                     :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#42,d_date_id#43,d_date#44,d_month_seq#45,d_week_seq#46,d_quarter_seq#47,d_year#48,d_dow#49,d_moy#50,d_dom#51,d_qoy#52,d_fy_year#53,d_fy_quarter_seq#54,d_fy_week_seq#55,d_day_name#56,d_quarter_name#57,d_holiday#58,d_weekend#59,d_following_holiday#60,d_first_dom#61,d_last_dom#62,d_same_day_ly#63,d_same_day_lq#64,d_current_day#65,... 4 more fields] parquet
                     :  +- SubqueryAlias spark_catalog.tpcds.store
                     :     +- Relation spark_catalog.tpcds.store[s_store_sk#70,s_store_id#71,s_rec_start_date#72,s_rec_end_date#73,s_closed_date_sk#74,s_store_name#75,s_number_employees#76,s_floor_space#77,s_hours#78,s_manager#79,s_market_id#80,s_geography_class#81,s_market_desc#82,s_market_manager#83,s_division_id#84,s_division_name#85,s_company_id#86,s_company_name#87,s_street_number#88,s_street_name#89,s_street_type#90,s_suite_number#91,s_city#92,s_county#93,... 5 more fields] parquet
                     +- SubqueryAlias spark_catalog.tpcds.item
                        +- Relation spark_catalog.tpcds.item[i_item_sk#99,i_item_id#100,i_rec_start_date#101,i_rec_end_date#102,i_item_desc#103,i_current_price#104,i_wholesale_cost#105,i_brand_id#106,i_brand#107,i_class_id#108,i_class#109,i_category_id#110,i_category#111,i_manufact_id#112,i_manufact#113,i_size#114,i_formulation#115,i_color#116,i_units#117,i_container#118,i_manager_id#119,i_product_name#120] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#133 ASC NULLS FIRST, s_state#134 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#133, s_state#134, spark_grouping_id#132L], [i_item_id#133, s_state#134, cast((shiftright(spark_grouping_id#132L, 0) & 1) as tinyint) AS g_state#0, avg(ss_quantity#20) AS agg1#1, avg(ss_list_price#22) AS agg2#2, avg(ss_coupon_amt#29) AS agg3#3, avg(ss_sales_price#23) AS agg4#4]
         +- Expand [[ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, i_item_id#100, s_state#94, 0], [ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, i_item_id#100, null, 1], [ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, null, null, 3]], [ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, i_item_id#133, s_state#134, spark_grouping_id#132L]
            +- Project [ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, i_item_id#100, s_state#94]
               +- Join Inner, (ss_item_sk#12 = i_item_sk#99)
                  :- Project [ss_item_sk#12, ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, s_state#94]
                  :  +- Join Inner, (ss_store_sk#17 = s_store_sk#70)
                  :     :- Project [ss_item_sk#12, ss_store_sk#17, ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29]
                  :     :  +- Join Inner, (ss_sold_date_sk#10 = d_date_sk#42)
                  :     :     :- Project [ss_sold_date_sk#10, ss_item_sk#12, ss_store_sk#17, ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29]
                  :     :     :  +- Join Inner, (ss_cdemo_sk#14 = cd_demo_sk#33)
                  :     :     :     :- Project [ss_sold_date_sk#10, ss_item_sk#12, ss_cdemo_sk#14, ss_store_sk#17, ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29]
                  :     :     :     :  +- Filter ((isnotnull(ss_cdemo_sk#14) AND isnotnull(ss_sold_date_sk#10)) AND (isnotnull(ss_store_sk#17) AND isnotnull(ss_item_sk#12)))
                  :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
                  :     :     :     +- Project [cd_demo_sk#33]
                  :     :     :        +- Filter ((((isnotnull(cd_gender#34) AND isnotnull(cd_marital_status#35)) AND isnotnull(cd_education_status#36)) AND (((cd_gender#34 = M) AND (cd_marital_status#35 = S)) AND (cd_education_status#36 = College))) AND isnotnull(cd_demo_sk#33))
                  :     :     :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#33,cd_gender#34,cd_marital_status#35,cd_education_status#36,cd_purchase_estimate#37,cd_credit_rating#38,cd_dep_count#39,cd_dep_employed_count#40,cd_dep_college_count#41] parquet
                  :     :     +- Project [d_date_sk#42]
                  :     :        +- Filter ((isnotnull(d_year#48) AND (d_year#48 = 2002)) AND isnotnull(d_date_sk#42))
                  :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#42,d_date_id#43,d_date#44,d_month_seq#45,d_week_seq#46,d_quarter_seq#47,d_year#48,d_dow#49,d_moy#50,d_dom#51,d_qoy#52,d_fy_year#53,d_fy_quarter_seq#54,d_fy_week_seq#55,d_day_name#56,d_quarter_name#57,d_holiday#58,d_weekend#59,d_following_holiday#60,d_first_dom#61,d_last_dom#62,d_same_day_ly#63,d_same_day_lq#64,d_current_day#65,... 4 more fields] parquet
                  :     +- Project [s_store_sk#70, s_state#94]
                  :        +- Filter ((isnotnull(s_state#94) AND (s_state#94 = TN)) AND isnotnull(s_store_sk#70))
                  :           +- Relation spark_catalog.tpcds.store[s_store_sk#70,s_store_id#71,s_rec_start_date#72,s_rec_end_date#73,s_closed_date_sk#74,s_store_name#75,s_number_employees#76,s_floor_space#77,s_hours#78,s_manager#79,s_market_id#80,s_geography_class#81,s_market_desc#82,s_market_manager#83,s_division_id#84,s_division_name#85,s_company_id#86,s_company_name#87,s_street_number#88,s_street_name#89,s_street_type#90,s_suite_number#91,s_city#92,s_county#93,... 5 more fields] parquet
                  +- Project [i_item_sk#99, i_item_id#100]
                     +- Filter isnotnull(i_item_sk#99)
                        +- Relation spark_catalog.tpcds.item[i_item_sk#99,i_item_id#100,i_rec_start_date#101,i_rec_end_date#102,i_item_desc#103,i_current_price#104,i_wholesale_cost#105,i_brand_id#106,i_brand#107,i_class_id#108,i_class#109,i_category_id#110,i_category#111,i_manufact_id#112,i_manufact#113,i_size#114,i_formulation#115,i_color#116,i_units#117,i_container#118,i_manager_id#119,i_product_name#120] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[i_item_id#133 ASC NULLS FIRST,s_state#134 ASC NULLS FIRST], output=[i_item_id#133,s_state#134,g_state#0,agg1#1,agg2#2,agg3#3,agg4#4])
   +- HashAggregate(keys=[i_item_id#133, s_state#134, spark_grouping_id#132L], functions=[avg(ss_quantity#20), avg(ss_list_price#22), avg(ss_coupon_amt#29), avg(ss_sales_price#23)], output=[i_item_id#133, s_state#134, g_state#0, agg1#1, agg2#2, agg3#3, agg4#4])
      +- Exchange hashpartitioning(i_item_id#133, s_state#134, spark_grouping_id#132L, 200), ENSURE_REQUIREMENTS, [plan_id=122]
         +- HashAggregate(keys=[i_item_id#133, s_state#134, spark_grouping_id#132L], functions=[partial_avg(ss_quantity#20), partial_avg(ss_list_price#22), partial_avg(ss_coupon_amt#29), partial_avg(ss_sales_price#23)], output=[i_item_id#133, s_state#134, spark_grouping_id#132L, sum#144, count#145L, sum#146, count#147L, sum#148, count#149L, sum#150, count#151L])
            +- Expand [[ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, i_item_id#100, s_state#94, 0], [ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, i_item_id#100, null, 1], [ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, null, null, 3]], [ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, i_item_id#133, s_state#134, spark_grouping_id#132L]
               +- Project [ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, i_item_id#100, s_state#94]
                  +- BroadcastHashJoin [ss_item_sk#12], [i_item_sk#99], Inner, BuildRight, false
                     :- Project [ss_item_sk#12, ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29, s_state#94]
                     :  +- BroadcastHashJoin [ss_store_sk#17], [s_store_sk#70], Inner, BuildRight, false
                     :     :- Project [ss_item_sk#12, ss_store_sk#17, ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29]
                     :     :  +- BroadcastHashJoin [ss_sold_date_sk#10], [d_date_sk#42], Inner, BuildRight, false
                     :     :     :- Project [ss_sold_date_sk#10, ss_item_sk#12, ss_store_sk#17, ss_quantity#20, ss_list_price#22, ss_sales_price#23, ss_coupon_amt#29]
                     :     :     :  +- BroadcastHashJoin [ss_cdemo_sk#14], [cd_demo_sk#33], Inner, BuildRight, false
                     :     :     :     :- Filter (((isnotnull(ss_cdemo_sk#14) AND isnotnull(ss_sold_date_sk#10)) AND isnotnull(ss_store_sk#17)) AND isnotnull(ss_item_sk#12))
                     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#10,ss_item_sk#12,ss_cdemo_sk#14,ss_store_sk#17,ss_quantity#20,ss_list_price#22,ss_sales_price#23,ss_coupon_amt#29] Batched: true, DataFilters: [isnotnull(ss_cdemo_sk#14), isnotnull(ss_sold_date_sk#10), isnotnull(ss_store_sk#17), isnotnull(s..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_cdemo_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_cdemo_sk:int,ss_store_sk:int,ss_quantity:int,ss_list...
                     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=104]
                     :     :     :        +- Project [cd_demo_sk#33]
                     :     :     :           +- Filter ((((((isnotnull(cd_gender#34) AND isnotnull(cd_marital_status#35)) AND isnotnull(cd_education_status#36)) AND (cd_gender#34 = M)) AND (cd_marital_status#35 = S)) AND (cd_education_status#36 = College)) AND isnotnull(cd_demo_sk#33))
                     :     :     :              +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#33,cd_gender#34,cd_marital_status#35,cd_education_status#36] Batched: true, DataFilters: [isnotnull(cd_gender#34), isnotnull(cd_marital_status#35), isnotnull(cd_education_status#36), (cd..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_gender), IsNotNull(cd_marital_status), IsNotNull(cd_education_status), EqualTo(cd_g..., ReadSchema: struct<cd_demo_sk:int,cd_gender:string,cd_marital_status:string,cd_education_status:string>
                     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=108]
                     :     :        +- Project [d_date_sk#42]
                     :     :           +- Filter ((isnotnull(d_year#48) AND (d_year#48 = 2002)) AND isnotnull(d_date_sk#42))
                     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#42,d_year#48] Batched: true, DataFilters: [isnotnull(d_year#48), (d_year#48 = 2002), isnotnull(d_date_sk#42)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=112]
                     :        +- Filter ((isnotnull(s_state#94) AND (s_state#94 = TN)) AND isnotnull(s_store_sk#70))
                     :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#70,s_state#94] Batched: true, DataFilters: [isnotnull(s_state#94), (s_state#94 = TN), isnotnull(s_store_sk#70)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_state), EqualTo(s_state,TN), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_state:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=116]
                        +- Filter isnotnull(i_item_sk#99)
                           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#99,i_item_id#100] Batched: true, DataFilters: [isnotnull(i_item_sk#99)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string>

Time taken: 2.778 seconds, Fetched 1 row(s)
