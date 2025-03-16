Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583080087
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['am_pm_ratio ASC NULLS FIRST], true
      +- 'Project [(cast('amc as decimal(15,4)) / cast('pmc as decimal(15,4))) AS am_pm_ratio#2]
         +- 'Join Inner
            :- 'SubqueryAlias at1
            :  +- 'Project ['count(1) AS amc#0]
            :     +- 'Filter (((('ws_sold_time_sk = 'time_dim.t_time_sk) AND ('ws_ship_hdemo_sk = 'household_demographics.hd_demo_sk)) AND ('ws_web_page_sk = 'web_page.wp_web_page_sk)) AND (((('time_dim.t_hour >= 8) AND ('time_dim.t_hour <= (8 + 1))) AND ('household_demographics.hd_dep_count = 6)) AND (('web_page.wp_char_count >= 5000) AND ('web_page.wp_char_count <= 5200))))
            :        +- 'Join Inner
            :           :- 'Join Inner
            :           :  :- 'Join Inner
            :           :  :  :- 'UnresolvedRelation [web_sales], [], false
            :           :  :  +- 'UnresolvedRelation [household_demographics], [], false
            :           :  +- 'UnresolvedRelation [time_dim], [], false
            :           +- 'UnresolvedRelation [web_page], [], false
            +- 'SubqueryAlias pt
               +- 'Project ['count(1) AS pmc#1]
                  +- 'Filter (((('ws_sold_time_sk = 'time_dim.t_time_sk) AND ('ws_ship_hdemo_sk = 'household_demographics.hd_demo_sk)) AND ('ws_web_page_sk = 'web_page.wp_web_page_sk)) AND (((('time_dim.t_hour >= 19) AND ('time_dim.t_hour <= (19 + 1))) AND ('household_demographics.hd_dep_count = 6)) AND (('web_page.wp_char_count >= 5000) AND ('web_page.wp_char_count <= 5200))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'Join Inner
                        :  :  :- 'UnresolvedRelation [web_sales], [], false
                        :  :  +- 'UnresolvedRelation [household_demographics], [], false
                        :  +- 'UnresolvedRelation [time_dim], [], false
                        +- 'UnresolvedRelation [web_page], [], false

== Analyzed Logical Plan ==
am_pm_ratio: decimal(35,20)
GlobalLimit 100
+- LocalLimit 100
   +- Sort [am_pm_ratio#2 ASC NULLS FIRST], true
      +- Project [(cast(amc#0L as decimal(15,4)) / cast(pmc#1L as decimal(15,4))) AS am_pm_ratio#2]
         +- Join Inner
            :- SubqueryAlias at1
            :  +- Aggregate [count(1) AS amc#0L]
            :     +- Filter ((((ws_sold_time_sk#11 = t_time_sk#49) AND (ws_ship_hdemo_sk#20 = hd_demo_sk#44)) AND (ws_web_page_sk#22 = wp_web_page_sk#59)) AND ((((t_hour#52 >= 8) AND (t_hour#52 <= (8 + 1))) AND (hd_dep_count#47 = 6)) AND ((wp_char_count#69 >= 5000) AND (wp_char_count#69 <= 5200))))
            :        +- Join Inner
            :           :- Join Inner
            :           :  :- Join Inner
            :           :  :  :- SubqueryAlias spark_catalog.tpcds.web_sales
            :           :  :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_sold_time_sk#11,ws_ship_date_sk#12,ws_item_sk#13,ws_bill_customer_sk#14,ws_bill_cdemo_sk#15,ws_bill_hdemo_sk#16,ws_bill_addr_sk#17,ws_ship_customer_sk#18,ws_ship_cdemo_sk#19,ws_ship_hdemo_sk#20,ws_ship_addr_sk#21,ws_web_page_sk#22,ws_web_site_sk#23,ws_ship_mode_sk#24,ws_warehouse_sk#25,ws_promo_sk#26,ws_order_number#27,ws_quantity#28,ws_wholesale_cost#29,ws_list_price#30,ws_sales_price#31,ws_ext_discount_amt#32,ws_ext_sales_price#33,... 10 more fields] parquet
            :           :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
            :           :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#44,hd_income_band_sk#45,hd_buy_potential#46,hd_dep_count#47,hd_vehicle_count#48] parquet
            :           :  +- SubqueryAlias spark_catalog.tpcds.time_dim
            :           :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#49,t_time_id#50,t_time#51,t_hour#52,t_minute#53,t_second#54,t_am_pm#55,t_shift#56,t_sub_shift#57,t_meal_time#58] parquet
            :           +- SubqueryAlias spark_catalog.tpcds.web_page
            :              +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#59,wp_web_page_id#60,wp_rec_start_date#61,wp_rec_end_date#62,wp_creation_date_sk#63,wp_access_date_sk#64,wp_autogen_flag#65,wp_customer_sk#66,wp_url#67,wp_type#68,wp_char_count#69,wp_link_count#70,wp_image_count#71,wp_max_ad_count#72] parquet
            +- SubqueryAlias pt
               +- Aggregate [count(1) AS pmc#1L]
                  +- Filter ((((ws_sold_time_sk#74 = t_time_sk#112) AND (ws_ship_hdemo_sk#83 = hd_demo_sk#107)) AND (ws_web_page_sk#85 = wp_web_page_sk#122)) AND ((((t_hour#115 >= 19) AND (t_hour#115 <= (19 + 1))) AND (hd_dep_count#110 = 6)) AND ((wp_char_count#132 >= 5000) AND (wp_char_count#132 <= 5200))))
                     +- Join Inner
                        :- Join Inner
                        :  :- Join Inner
                        :  :  :- SubqueryAlias spark_catalog.tpcds.web_sales
                        :  :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#73,ws_sold_time_sk#74,ws_ship_date_sk#75,ws_item_sk#76,ws_bill_customer_sk#77,ws_bill_cdemo_sk#78,ws_bill_hdemo_sk#79,ws_bill_addr_sk#80,ws_ship_customer_sk#81,ws_ship_cdemo_sk#82,ws_ship_hdemo_sk#83,ws_ship_addr_sk#84,ws_web_page_sk#85,ws_web_site_sk#86,ws_ship_mode_sk#87,ws_warehouse_sk#88,ws_promo_sk#89,ws_order_number#90,ws_quantity#91,ws_wholesale_cost#92,ws_list_price#93,ws_sales_price#94,ws_ext_discount_amt#95,ws_ext_sales_price#96,... 10 more fields] parquet
                        :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
                        :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#107,hd_income_band_sk#108,hd_buy_potential#109,hd_dep_count#110,hd_vehicle_count#111] parquet
                        :  +- SubqueryAlias spark_catalog.tpcds.time_dim
                        :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#112,t_time_id#113,t_time#114,t_hour#115,t_minute#116,t_second#117,t_am_pm#118,t_shift#119,t_sub_shift#120,t_meal_time#121] parquet
                        +- SubqueryAlias spark_catalog.tpcds.web_page
                           +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#122,wp_web_page_id#123,wp_rec_start_date#124,wp_rec_end_date#125,wp_creation_date_sk#126,wp_access_date_sk#127,wp_autogen_flag#128,wp_customer_sk#129,wp_url#130,wp_type#131,wp_char_count#132,wp_link_count#133,wp_image_count#134,wp_max_ad_count#135] parquet

== Optimized Logical Plan ==
Project [(cast(amc#0L as decimal(15,4)) / cast(pmc#1L as decimal(15,4))) AS am_pm_ratio#2]
+- Join Inner
   :- Aggregate [count(1) AS amc#0L]
   :  +- Project
   :     +- Join Inner, (ws_web_page_sk#22 = wp_web_page_sk#59)
   :        :- Project [ws_web_page_sk#22]
   :        :  +- Join Inner, (ws_sold_time_sk#11 = t_time_sk#49)
   :        :     :- Project [ws_sold_time_sk#11, ws_web_page_sk#22]
   :        :     :  +- Join Inner, (ws_ship_hdemo_sk#20 = hd_demo_sk#44)
   :        :     :     :- Project [ws_sold_time_sk#11, ws_ship_hdemo_sk#20, ws_web_page_sk#22]
   :        :     :     :  +- Filter (isnotnull(ws_ship_hdemo_sk#20) AND (isnotnull(ws_sold_time_sk#11) AND isnotnull(ws_web_page_sk#22)))
   :        :     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_sold_time_sk#11,ws_ship_date_sk#12,ws_item_sk#13,ws_bill_customer_sk#14,ws_bill_cdemo_sk#15,ws_bill_hdemo_sk#16,ws_bill_addr_sk#17,ws_ship_customer_sk#18,ws_ship_cdemo_sk#19,ws_ship_hdemo_sk#20,ws_ship_addr_sk#21,ws_web_page_sk#22,ws_web_site_sk#23,ws_ship_mode_sk#24,ws_warehouse_sk#25,ws_promo_sk#26,ws_order_number#27,ws_quantity#28,ws_wholesale_cost#29,ws_list_price#30,ws_sales_price#31,ws_ext_discount_amt#32,ws_ext_sales_price#33,... 10 more fields] parquet
   :        :     :     +- Project [hd_demo_sk#44]
   :        :     :        +- Filter ((isnotnull(hd_dep_count#47) AND (hd_dep_count#47 = 6)) AND isnotnull(hd_demo_sk#44))
   :        :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#44,hd_income_band_sk#45,hd_buy_potential#46,hd_dep_count#47,hd_vehicle_count#48] parquet
   :        :     +- Project [t_time_sk#49]
   :        :        +- Filter ((isnotnull(t_hour#52) AND ((t_hour#52 >= 8) AND (t_hour#52 <= 9))) AND isnotnull(t_time_sk#49))
   :        :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#49,t_time_id#50,t_time#51,t_hour#52,t_minute#53,t_second#54,t_am_pm#55,t_shift#56,t_sub_shift#57,t_meal_time#58] parquet
   :        +- Project [wp_web_page_sk#59]
   :           +- Filter ((isnotnull(wp_char_count#69) AND ((wp_char_count#69 >= 5000) AND (wp_char_count#69 <= 5200))) AND isnotnull(wp_web_page_sk#59))
   :              +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#59,wp_web_page_id#60,wp_rec_start_date#61,wp_rec_end_date#62,wp_creation_date_sk#63,wp_access_date_sk#64,wp_autogen_flag#65,wp_customer_sk#66,wp_url#67,wp_type#68,wp_char_count#69,wp_link_count#70,wp_image_count#71,wp_max_ad_count#72] parquet
   +- Aggregate [count(1) AS pmc#1L]
      +- Project
         +- Join Inner, (ws_web_page_sk#85 = wp_web_page_sk#122)
            :- Project [ws_web_page_sk#85]
            :  +- Join Inner, (ws_sold_time_sk#74 = t_time_sk#112)
            :     :- Project [ws_sold_time_sk#74, ws_web_page_sk#85]
            :     :  +- Join Inner, (ws_ship_hdemo_sk#83 = hd_demo_sk#107)
            :     :     :- Project [ws_sold_time_sk#74, ws_ship_hdemo_sk#83, ws_web_page_sk#85]
            :     :     :  +- Filter (isnotnull(ws_ship_hdemo_sk#83) AND (isnotnull(ws_sold_time_sk#74) AND isnotnull(ws_web_page_sk#85)))
            :     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#73,ws_sold_time_sk#74,ws_ship_date_sk#75,ws_item_sk#76,ws_bill_customer_sk#77,ws_bill_cdemo_sk#78,ws_bill_hdemo_sk#79,ws_bill_addr_sk#80,ws_ship_customer_sk#81,ws_ship_cdemo_sk#82,ws_ship_hdemo_sk#83,ws_ship_addr_sk#84,ws_web_page_sk#85,ws_web_site_sk#86,ws_ship_mode_sk#87,ws_warehouse_sk#88,ws_promo_sk#89,ws_order_number#90,ws_quantity#91,ws_wholesale_cost#92,ws_list_price#93,ws_sales_price#94,ws_ext_discount_amt#95,ws_ext_sales_price#96,... 10 more fields] parquet
            :     :     +- Project [hd_demo_sk#107]
            :     :        +- Filter ((isnotnull(hd_dep_count#110) AND (hd_dep_count#110 = 6)) AND isnotnull(hd_demo_sk#107))
            :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#107,hd_income_band_sk#108,hd_buy_potential#109,hd_dep_count#110,hd_vehicle_count#111] parquet
            :     +- Project [t_time_sk#112]
            :        +- Filter ((isnotnull(t_hour#115) AND ((t_hour#115 >= 19) AND (t_hour#115 <= 20))) AND isnotnull(t_time_sk#112))
            :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#112,t_time_id#113,t_time#114,t_hour#115,t_minute#116,t_second#117,t_am_pm#118,t_shift#119,t_sub_shift#120,t_meal_time#121] parquet
            +- Project [wp_web_page_sk#122]
               +- Filter ((isnotnull(wp_char_count#132) AND ((wp_char_count#132 >= 5000) AND (wp_char_count#132 <= 5200))) AND isnotnull(wp_web_page_sk#122))
                  +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#122,wp_web_page_id#123,wp_rec_start_date#124,wp_rec_end_date#125,wp_creation_date_sk#126,wp_access_date_sk#127,wp_autogen_flag#128,wp_customer_sk#129,wp_url#130,wp_type#131,wp_char_count#132,wp_link_count#133,wp_image_count#134,wp_max_ad_count#135] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [(cast(amc#0L as decimal(15,4)) / cast(pmc#1L as decimal(15,4))) AS am_pm_ratio#2]
   +- BroadcastNestedLoopJoin BuildRight, Inner
      :- HashAggregate(keys=[], functions=[count(1)], output=[amc#0L])
      :  +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=171]
      :     +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#145L])
      :        +- Project
      :           +- BroadcastHashJoin [ws_web_page_sk#22], [wp_web_page_sk#59], Inner, BuildRight, false
      :              :- Project [ws_web_page_sk#22]
      :              :  +- BroadcastHashJoin [ws_sold_time_sk#11], [t_time_sk#49], Inner, BuildRight, false
      :              :     :- Project [ws_sold_time_sk#11, ws_web_page_sk#22]
      :              :     :  +- BroadcastHashJoin [ws_ship_hdemo_sk#20], [hd_demo_sk#44], Inner, BuildRight, false
      :              :     :     :- Filter ((isnotnull(ws_ship_hdemo_sk#20) AND isnotnull(ws_sold_time_sk#11)) AND isnotnull(ws_web_page_sk#22))
      :              :     :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_time_sk#11,ws_ship_hdemo_sk#20,ws_web_page_sk#22] Batched: true, DataFilters: [isnotnull(ws_ship_hdemo_sk#20), isnotnull(ws_sold_time_sk#11), isnotnull(ws_web_page_sk#22)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_ship_hdemo_sk), IsNotNull(ws_sold_time_sk), IsNotNull(ws_web_page_sk)], ReadSchema: struct<ws_sold_time_sk:int,ws_ship_hdemo_sk:int,ws_web_page_sk:int>
      :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=158]
      :              :     :        +- Project [hd_demo_sk#44]
      :              :     :           +- Filter ((isnotnull(hd_dep_count#47) AND (hd_dep_count#47 = 6)) AND isnotnull(hd_demo_sk#44))
      :              :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#44,hd_dep_count#47] Batched: true, DataFilters: [isnotnull(hd_dep_count#47), (hd_dep_count#47 = 6), isnotnull(hd_demo_sk#44)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_dep_count), EqualTo(hd_dep_count,6), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int>
      :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=162]
      :              :        +- Project [t_time_sk#49]
      :              :           +- Filter (((isnotnull(t_hour#52) AND (t_hour#52 >= 8)) AND (t_hour#52 <= 9)) AND isnotnull(t_time_sk#49))
      :              :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#49,t_hour#52] Batched: true, DataFilters: [isnotnull(t_hour#52), (t_hour#52 >= 8), (t_hour#52 <= 9), isnotnull(t_time_sk#49)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), GreaterThanOrEqual(t_hour,8), LessThanOrEqual(t_hour,9), IsNotNull(t_time_sk)], ReadSchema: struct<t_time_sk:int,t_hour:int>
      :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=166]
      :                 +- Project [wp_web_page_sk#59]
      :                    +- Filter (((isnotnull(wp_char_count#69) AND (wp_char_count#69 >= 5000)) AND (wp_char_count#69 <= 5200)) AND isnotnull(wp_web_page_sk#59))
      :                       +- FileScan parquet spark_catalog.tpcds.web_page[wp_web_page_sk#59,wp_char_count#69] Batched: true, DataFilters: [isnotnull(wp_char_count#69), (wp_char_count#69 >= 5000), (wp_char_count#69 <= 5200), isnotnull(w..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wp_char_count), GreaterThanOrEqual(wp_char_count,5000), LessThanOrEqual(wp_char_count,..., ReadSchema: struct<wp_web_page_sk:int,wp_char_count:int>
      +- BroadcastExchange IdentityBroadcastMode, [plan_id=189]
         +- HashAggregate(keys=[], functions=[count(1)], output=[pmc#1L])
            +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=186]
               +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#147L])
                  +- Project
                     +- BroadcastHashJoin [ws_web_page_sk#85], [wp_web_page_sk#122], Inner, BuildRight, false
                        :- Project [ws_web_page_sk#85]
                        :  +- BroadcastHashJoin [ws_sold_time_sk#74], [t_time_sk#112], Inner, BuildRight, false
                        :     :- Project [ws_sold_time_sk#74, ws_web_page_sk#85]
                        :     :  +- BroadcastHashJoin [ws_ship_hdemo_sk#83], [hd_demo_sk#107], Inner, BuildRight, false
                        :     :     :- Filter ((isnotnull(ws_ship_hdemo_sk#83) AND isnotnull(ws_sold_time_sk#74)) AND isnotnull(ws_web_page_sk#85))
                        :     :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_time_sk#74,ws_ship_hdemo_sk#83,ws_web_page_sk#85] Batched: true, DataFilters: [isnotnull(ws_ship_hdemo_sk#83), isnotnull(ws_sold_time_sk#74), isnotnull(ws_web_page_sk#85)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_ship_hdemo_sk), IsNotNull(ws_sold_time_sk), IsNotNull(ws_web_page_sk)], ReadSchema: struct<ws_sold_time_sk:int,ws_ship_hdemo_sk:int,ws_web_page_sk:int>
                        :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=173]
                        :     :        +- Project [hd_demo_sk#107]
                        :     :           +- Filter ((isnotnull(hd_dep_count#110) AND (hd_dep_count#110 = 6)) AND isnotnull(hd_demo_sk#107))
                        :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#107,hd_dep_count#110] Batched: true, DataFilters: [isnotnull(hd_dep_count#110), (hd_dep_count#110 = 6), isnotnull(hd_demo_sk#107)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_dep_count), EqualTo(hd_dep_count,6), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int>
                        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=177]
                        :        +- Project [t_time_sk#112]
                        :           +- Filter (((isnotnull(t_hour#115) AND (t_hour#115 >= 19)) AND (t_hour#115 <= 20)) AND isnotnull(t_time_sk#112))
                        :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#112,t_hour#115] Batched: true, DataFilters: [isnotnull(t_hour#115), (t_hour#115 >= 19), (t_hour#115 <= 20), isnotnull(t_time_sk#112)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), GreaterThanOrEqual(t_hour,19), LessThanOrEqual(t_hour,20), IsNotNull(t_time_sk)], ReadSchema: struct<t_time_sk:int,t_hour:int>
                        +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=181]
                           +- Project [wp_web_page_sk#122]
                              +- Filter (((isnotnull(wp_char_count#132) AND (wp_char_count#132 >= 5000)) AND (wp_char_count#132 <= 5200)) AND isnotnull(wp_web_page_sk#122))
                                 +- FileScan parquet spark_catalog.tpcds.web_page[wp_web_page_sk#122,wp_char_count#132] Batched: true, DataFilters: [isnotnull(wp_char_count#132), (wp_char_count#132 >= 5000), (wp_char_count#132 <= 5200), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wp_char_count), GreaterThanOrEqual(wp_char_count,5000), LessThanOrEqual(wp_char_count,..., ReadSchema: struct<wp_web_page_sk:int,wp_char_count:int>

Time taken: 2.761 seconds, Fetched 1 row(s)
