Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4041
Spark master: local[*], Application Id: local-1741583336954
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['count(1) ASC NULLS FIRST], true
      +- 'Project [unresolvedalias('count(1), None)]
         +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) AND ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND (('ss_store_sk = 's_store_sk) AND ('time_dim.t_hour = 20))) AND ((('time_dim.t_minute >= 30) AND ('household_demographics.hd_dep_count = 7)) AND ('store.s_store_name = ese)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation [store_sales], [], false
               :  :  +- 'UnresolvedRelation [household_demographics], [], false
               :  +- 'UnresolvedRelation [time_dim], [], false
               +- 'UnresolvedRelation [store], [], false

== Analyzed Logical Plan ==
count(1): bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [count(1)#74L ASC NULLS FIRST], true
      +- Aggregate [count(1) AS count(1)#74L]
         +- Filter ((((ss_sold_time_sk#8 = t_time_sk#35) AND (ss_hdemo_sk#12 = hd_demo_sk#30)) AND ((ss_store_sk#14 = s_store_sk#45) AND (t_hour#38 = 20))) AND (((t_minute#39 >= 30) AND (hd_dep_count#33 = 7)) AND (s_store_name#50 = ese)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
               :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#30,hd_income_band_sk#31,hd_buy_potential#32,hd_dep_count#33,hd_vehicle_count#34] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.time_dim
               :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#35,t_time_id#36,t_time#37,t_hour#38,t_minute#39,t_second#40,t_am_pm#41,t_shift#42,t_sub_shift#43,t_meal_time#44] parquet
               +- SubqueryAlias spark_catalog.tpcds.store
                  +- Relation spark_catalog.tpcds.store[s_store_sk#45,s_store_id#46,s_rec_start_date#47,s_rec_end_date#48,s_closed_date_sk#49,s_store_name#50,s_number_employees#51,s_floor_space#52,s_hours#53,s_manager#54,s_market_id#55,s_geography_class#56,s_market_desc#57,s_market_manager#58,s_division_id#59,s_division_name#60,s_company_id#61,s_company_name#62,s_street_number#63,s_street_name#64,s_street_type#65,s_suite_number#66,s_city#67,s_county#68,... 5 more fields] parquet

== Optimized Logical Plan ==
Aggregate [count(1) AS count(1)#74L]
+- Project
   +- Join Inner, (ss_store_sk#14 = s_store_sk#45)
      :- Project [ss_store_sk#14]
      :  +- Join Inner, (ss_sold_time_sk#8 = t_time_sk#35)
      :     :- Project [ss_sold_time_sk#8, ss_store_sk#14]
      :     :  +- Join Inner, (ss_hdemo_sk#12 = hd_demo_sk#30)
      :     :     :- Project [ss_sold_time_sk#8, ss_hdemo_sk#12, ss_store_sk#14]
      :     :     :  +- Filter (isnotnull(ss_hdemo_sk#12) AND (isnotnull(ss_sold_time_sk#8) AND isnotnull(ss_store_sk#14)))
      :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
      :     :     +- Project [hd_demo_sk#30]
      :     :        +- Filter ((isnotnull(hd_dep_count#33) AND (hd_dep_count#33 = 7)) AND isnotnull(hd_demo_sk#30))
      :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#30,hd_income_band_sk#31,hd_buy_potential#32,hd_dep_count#33,hd_vehicle_count#34] parquet
      :     +- Project [t_time_sk#35]
      :        +- Filter (((isnotnull(t_hour#38) AND isnotnull(t_minute#39)) AND ((t_hour#38 = 20) AND (t_minute#39 >= 30))) AND isnotnull(t_time_sk#35))
      :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#35,t_time_id#36,t_time#37,t_hour#38,t_minute#39,t_second#40,t_am_pm#41,t_shift#42,t_sub_shift#43,t_meal_time#44] parquet
      +- Project [s_store_sk#45]
         +- Filter ((isnotnull(s_store_name#50) AND (s_store_name#50 = ese)) AND isnotnull(s_store_sk#45))
            +- Relation spark_catalog.tpcds.store[s_store_sk#45,s_store_id#46,s_rec_start_date#47,s_rec_end_date#48,s_closed_date_sk#49,s_store_name#50,s_number_employees#51,s_floor_space#52,s_hours#53,s_manager#54,s_market_id#55,s_geography_class#56,s_market_desc#57,s_market_manager#58,s_division_id#59,s_division_name#60,s_company_id#61,s_company_name#62,s_street_number#63,s_street_name#64,s_street_type#65,s_suite_number#66,s_city#67,s_county#68,... 5 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count(1)#74L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=90]
      +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#80L])
         +- Project
            +- BroadcastHashJoin [ss_store_sk#14], [s_store_sk#45], Inner, BuildRight, false
               :- Project [ss_store_sk#14]
               :  +- BroadcastHashJoin [ss_sold_time_sk#8], [t_time_sk#35], Inner, BuildRight, false
               :     :- Project [ss_sold_time_sk#8, ss_store_sk#14]
               :     :  +- BroadcastHashJoin [ss_hdemo_sk#12], [hd_demo_sk#30], Inner, BuildRight, false
               :     :     :- Filter ((isnotnull(ss_hdemo_sk#12) AND isnotnull(ss_sold_time_sk#8)) AND isnotnull(ss_store_sk#14))
               :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_time_sk#8,ss_hdemo_sk#12,ss_store_sk#14] Batched: true, DataFilters: [isnotnull(ss_hdemo_sk#12), isnotnull(ss_sold_time_sk#8), isnotnull(ss_store_sk#14)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=77]
               :     :        +- Project [hd_demo_sk#30]
               :     :           +- Filter ((isnotnull(hd_dep_count#33) AND (hd_dep_count#33 = 7)) AND isnotnull(hd_demo_sk#30))
               :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#30,hd_dep_count#33] Batched: true, DataFilters: [isnotnull(hd_dep_count#33), (hd_dep_count#33 = 7), isnotnull(hd_demo_sk#30)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_dep_count), EqualTo(hd_dep_count,7), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=81]
               :        +- Project [t_time_sk#35]
               :           +- Filter ((((isnotnull(t_hour#38) AND isnotnull(t_minute#39)) AND (t_hour#38 = 20)) AND (t_minute#39 >= 30)) AND isnotnull(t_time_sk#35))
               :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#35,t_hour#38,t_minute#39] Batched: true, DataFilters: [isnotnull(t_hour#38), isnotnull(t_minute#39), (t_hour#38 = 20), (t_minute#39 >= 30), isnotnull(t..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,20), GreaterThanOrEqual(t_minute,30), IsN..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=85]
                  +- Project [s_store_sk#45]
                     +- Filter ((isnotnull(s_store_name#50) AND (s_store_name#50 = ese)) AND isnotnull(s_store_sk#45))
                        +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#45,s_store_name#50] Batched: true, DataFilters: [isnotnull(s_store_name#50), (s_store_name#50 = ese), isnotnull(s_store_sk#45)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>

Time taken: 2.588 seconds, Fetched 1 row(s)
