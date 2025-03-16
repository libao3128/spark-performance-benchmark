Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582282858
== Parsed Logical Plan ==
'Sort ['ext_price DESC NULLS LAST, 'i_brand_id ASC NULLS FIRST], true
+- 'Aggregate ['i_brand, 'i_brand_id, 't_hour, 't_minute], ['i_brand_id AS brand_id#12, 'i_brand AS brand#13, 't_hour, 't_minute, 'sum('ext_price) AS ext_price#14]
   +- 'Filter ((('sold_item_sk = 'i_item_sk) AND ('i_manager_id = 1)) AND (('time_sk = 't_time_sk) AND (('t_meal_time = breakfast) OR ('t_meal_time = dinner))))
      +- 'Join Inner
         :- 'Join Inner
         :  :- 'UnresolvedRelation [item], [], false
         :  +- 'SubqueryAlias tmp
         :     +- 'Union false, false
         :        :- 'Union false, false
         :        :  :- 'Project ['ws_ext_sales_price AS ext_price#0, 'ws_sold_date_sk AS sold_date_sk#1, 'ws_item_sk AS sold_item_sk#2, 'ws_sold_time_sk AS time_sk#3]
         :        :  :  +- 'Filter ((('d_date_sk = 'ws_sold_date_sk) AND ('d_moy = 11)) AND ('d_year = 1999))
         :        :  :     +- 'Join Inner
         :        :  :        :- 'UnresolvedRelation [web_sales], [], false
         :        :  :        +- 'UnresolvedRelation [date_dim], [], false
         :        :  +- 'Project ['cs_ext_sales_price AS ext_price#4, 'cs_sold_date_sk AS sold_date_sk#5, 'cs_item_sk AS sold_item_sk#6, 'cs_sold_time_sk AS time_sk#7]
         :        :     +- 'Filter ((('d_date_sk = 'cs_sold_date_sk) AND ('d_moy = 11)) AND ('d_year = 1999))
         :        :        +- 'Join Inner
         :        :           :- 'UnresolvedRelation [catalog_sales], [], false
         :        :           +- 'UnresolvedRelation [date_dim], [], false
         :        +- 'Project ['ss_ext_sales_price AS ext_price#8, 'ss_sold_date_sk AS sold_date_sk#9, 'ss_item_sk AS sold_item_sk#10, 'ss_sold_time_sk AS time_sk#11]
         :           +- 'Filter ((('d_date_sk = 'ss_sold_date_sk) AND ('d_moy = 11)) AND ('d_year = 1999))
         :              +- 'Join Inner
         :                 :- 'UnresolvedRelation [store_sales], [], false
         :                 +- 'UnresolvedRelation [date_dim], [], false
         +- 'UnresolvedRelation [time_dim], [], false

== Analyzed Logical Plan ==
brand_id: int, brand: string, t_hour: int, t_minute: int, ext_price: double
Sort [ext_price#14 DESC NULLS LAST, brand_id#12 ASC NULLS FIRST], true
+- Aggregate [i_brand#28, i_brand_id#27, t_hour#164, t_minute#165], [i_brand_id#27 AS brand_id#12, i_brand#28 AS brand#13, t_hour#164, t_minute#165, sum(ext_price#0) AS ext_price#14]
   +- Filter (((sold_item_sk#2 = i_item_sk#20) AND (i_manager_id#40 = 1)) AND ((time_sk#3 = t_time_sk#161) AND ((t_meal_time#170 = breakfast) OR (t_meal_time#170 = dinner))))
      +- Join Inner
         :- Join Inner
         :  :- SubqueryAlias spark_catalog.tpcds.item
         :  :  +- Relation spark_catalog.tpcds.item[i_item_sk#20,i_item_id#21,i_rec_start_date#22,i_rec_end_date#23,i_item_desc#24,i_current_price#25,i_wholesale_cost#26,i_brand_id#27,i_brand#28,i_class_id#29,i_class#30,i_category_id#31,i_category#32,i_manufact_id#33,i_manufact#34,i_size#35,i_formulation#36,i_color#37,i_units#38,i_container#39,i_manager_id#40,i_product_name#41] parquet
         :  +- SubqueryAlias tmp
         :     +- Union false, false
         :        :- Union false, false
         :        :  :- Project [ws_ext_sales_price#65 AS ext_price#0, ws_sold_date_sk#42 AS sold_date_sk#1, ws_item_sk#45 AS sold_item_sk#2, ws_sold_time_sk#43 AS time_sk#3]
         :        :  :  +- Filter (((d_date_sk#76 = ws_sold_date_sk#42) AND (d_moy#84 = 11)) AND (d_year#82 = 1999))
         :        :  :     +- Join Inner
         :        :  :        :- SubqueryAlias spark_catalog.tpcds.web_sales
         :        :  :        :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#42,ws_sold_time_sk#43,ws_ship_date_sk#44,ws_item_sk#45,ws_bill_customer_sk#46,ws_bill_cdemo_sk#47,ws_bill_hdemo_sk#48,ws_bill_addr_sk#49,ws_ship_customer_sk#50,ws_ship_cdemo_sk#51,ws_ship_hdemo_sk#52,ws_ship_addr_sk#53,ws_web_page_sk#54,ws_web_site_sk#55,ws_ship_mode_sk#56,ws_warehouse_sk#57,ws_promo_sk#58,ws_order_number#59,ws_quantity#60,ws_wholesale_cost#61,ws_list_price#62,ws_sales_price#63,ws_ext_discount_amt#64,ws_ext_sales_price#65,... 10 more fields] parquet
         :        :  :        +- SubqueryAlias spark_catalog.tpcds.date_dim
         :        :  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#76,d_date_id#77,d_date#78,d_month_seq#79,d_week_seq#80,d_quarter_seq#81,d_year#82,d_dow#83,d_moy#84,d_dom#85,d_qoy#86,d_fy_year#87,d_fy_quarter_seq#88,d_fy_week_seq#89,d_day_name#90,d_quarter_name#91,d_holiday#92,d_weekend#93,d_following_holiday#94,d_first_dom#95,d_last_dom#96,d_same_day_ly#97,d_same_day_lq#98,d_current_day#99,... 4 more fields] parquet
         :        :  +- Project [cs_ext_sales_price#127 AS ext_price#4, cs_sold_date_sk#104 AS sold_date_sk#5, cs_item_sk#119 AS sold_item_sk#6, cs_sold_time_sk#105 AS time_sk#7]
         :        :     +- Filter (((d_date_sk#171 = cs_sold_date_sk#104) AND (d_moy#179 = 11)) AND (d_year#177 = 1999))
         :        :        +- Join Inner
         :        :           :- SubqueryAlias spark_catalog.tpcds.catalog_sales
         :        :           :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#104,cs_sold_time_sk#105,cs_ship_date_sk#106,cs_bill_customer_sk#107,cs_bill_cdemo_sk#108,cs_bill_hdemo_sk#109,cs_bill_addr_sk#110,cs_ship_customer_sk#111,cs_ship_cdemo_sk#112,cs_ship_hdemo_sk#113,cs_ship_addr_sk#114,cs_call_center_sk#115,cs_catalog_page_sk#116,cs_ship_mode_sk#117,cs_warehouse_sk#118,cs_item_sk#119,cs_promo_sk#120,cs_order_number#121,cs_quantity#122,cs_wholesale_cost#123,cs_list_price#124,cs_sales_price#125,cs_ext_discount_amt#126,cs_ext_sales_price#127,... 10 more fields] parquet
         :        :           +- SubqueryAlias spark_catalog.tpcds.date_dim
         :        :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#171,d_date_id#172,d_date#173,d_month_seq#174,d_week_seq#175,d_quarter_seq#176,d_year#177,d_dow#178,d_moy#179,d_dom#180,d_qoy#181,d_fy_year#182,d_fy_quarter_seq#183,d_fy_week_seq#184,d_day_name#185,d_quarter_name#186,d_holiday#187,d_weekend#188,d_following_holiday#189,d_first_dom#190,d_last_dom#191,d_same_day_ly#192,d_same_day_lq#193,d_current_day#194,... 4 more fields] parquet
         :        +- Project [ss_ext_sales_price#153 AS ext_price#8, ss_sold_date_sk#138 AS sold_date_sk#9, ss_item_sk#140 AS sold_item_sk#10, ss_sold_time_sk#139 AS time_sk#11]
         :           +- Filter (((d_date_sk#199 = ss_sold_date_sk#138) AND (d_moy#207 = 11)) AND (d_year#205 = 1999))
         :              +- Join Inner
         :                 :- SubqueryAlias spark_catalog.tpcds.store_sales
         :                 :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#138,ss_sold_time_sk#139,ss_item_sk#140,ss_customer_sk#141,ss_cdemo_sk#142,ss_hdemo_sk#143,ss_addr_sk#144,ss_store_sk#145,ss_promo_sk#146,ss_ticket_number#147,ss_quantity#148,ss_wholesale_cost#149,ss_list_price#150,ss_sales_price#151,ss_ext_discount_amt#152,ss_ext_sales_price#153,ss_ext_wholesale_cost#154,ss_ext_list_price#155,ss_ext_tax#156,ss_coupon_amt#157,ss_net_paid#158,ss_net_paid_inc_tax#159,ss_net_profit#160] parquet
         :                 +- SubqueryAlias spark_catalog.tpcds.date_dim
         :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#199,d_date_id#200,d_date#201,d_month_seq#202,d_week_seq#203,d_quarter_seq#204,d_year#205,d_dow#206,d_moy#207,d_dom#208,d_qoy#209,d_fy_year#210,d_fy_quarter_seq#211,d_fy_week_seq#212,d_day_name#213,d_quarter_name#214,d_holiday#215,d_weekend#216,d_following_holiday#217,d_first_dom#218,d_last_dom#219,d_same_day_ly#220,d_same_day_lq#221,d_current_day#222,... 4 more fields] parquet
         +- SubqueryAlias spark_catalog.tpcds.time_dim
            +- Relation spark_catalog.tpcds.time_dim[t_time_sk#161,t_time_id#162,t_time#163,t_hour#164,t_minute#165,t_second#166,t_am_pm#167,t_shift#168,t_sub_shift#169,t_meal_time#170] parquet

== Optimized Logical Plan ==
Sort [ext_price#14 DESC NULLS LAST, brand_id#12 ASC NULLS FIRST], true
+- Aggregate [i_brand#28, i_brand_id#27, t_hour#164, t_minute#165], [i_brand_id#27 AS brand_id#12, i_brand#28 AS brand#13, t_hour#164, t_minute#165, sum(ext_price#0) AS ext_price#14]
   +- Project [i_brand_id#27, i_brand#28, ext_price#0, t_hour#164, t_minute#165]
      +- Join Inner, (time_sk#3 = t_time_sk#161)
         :- Project [i_brand_id#27, i_brand#28, ext_price#0, time_sk#3]
         :  +- Join Inner, (sold_item_sk#2 = i_item_sk#20)
         :     :- Project [i_item_sk#20, i_brand_id#27, i_brand#28]
         :     :  +- Filter ((isnotnull(i_manager_id#40) AND (i_manager_id#40 = 1)) AND isnotnull(i_item_sk#20))
         :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#20,i_item_id#21,i_rec_start_date#22,i_rec_end_date#23,i_item_desc#24,i_current_price#25,i_wholesale_cost#26,i_brand_id#27,i_brand#28,i_class_id#29,i_class#30,i_category_id#31,i_category#32,i_manufact_id#33,i_manufact#34,i_size#35,i_formulation#36,i_color#37,i_units#38,i_container#39,i_manager_id#40,i_product_name#41] parquet
         :     +- Union false, false
         :        :- Project [ws_ext_sales_price#65 AS ext_price#0, ws_item_sk#45 AS sold_item_sk#2, ws_sold_time_sk#43 AS time_sk#3]
         :        :  +- Join Inner, (d_date_sk#76 = ws_sold_date_sk#42)
         :        :     :- Project [ws_sold_date_sk#42, ws_sold_time_sk#43, ws_item_sk#45, ws_ext_sales_price#65]
         :        :     :  +- Filter (isnotnull(ws_sold_date_sk#42) AND (isnotnull(ws_item_sk#45) AND isnotnull(ws_sold_time_sk#43)))
         :        :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#42,ws_sold_time_sk#43,ws_ship_date_sk#44,ws_item_sk#45,ws_bill_customer_sk#46,ws_bill_cdemo_sk#47,ws_bill_hdemo_sk#48,ws_bill_addr_sk#49,ws_ship_customer_sk#50,ws_ship_cdemo_sk#51,ws_ship_hdemo_sk#52,ws_ship_addr_sk#53,ws_web_page_sk#54,ws_web_site_sk#55,ws_ship_mode_sk#56,ws_warehouse_sk#57,ws_promo_sk#58,ws_order_number#59,ws_quantity#60,ws_wholesale_cost#61,ws_list_price#62,ws_sales_price#63,ws_ext_discount_amt#64,ws_ext_sales_price#65,... 10 more fields] parquet
         :        :     +- Project [d_date_sk#76]
         :        :        +- Filter (((isnotnull(d_moy#84) AND isnotnull(d_year#82)) AND ((d_moy#84 = 11) AND (d_year#82 = 1999))) AND isnotnull(d_date_sk#76))
         :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#76,d_date_id#77,d_date#78,d_month_seq#79,d_week_seq#80,d_quarter_seq#81,d_year#82,d_dow#83,d_moy#84,d_dom#85,d_qoy#86,d_fy_year#87,d_fy_quarter_seq#88,d_fy_week_seq#89,d_day_name#90,d_quarter_name#91,d_holiday#92,d_weekend#93,d_following_holiday#94,d_first_dom#95,d_last_dom#96,d_same_day_ly#97,d_same_day_lq#98,d_current_day#99,... 4 more fields] parquet
         :        :- Project [cs_ext_sales_price#127 AS ext_price#4, cs_item_sk#119 AS sold_item_sk#6, cs_sold_time_sk#105 AS time_sk#7]
         :        :  +- Join Inner, (d_date_sk#171 = cs_sold_date_sk#104)
         :        :     :- Project [cs_sold_date_sk#104, cs_sold_time_sk#105, cs_item_sk#119, cs_ext_sales_price#127]
         :        :     :  +- Filter (isnotnull(cs_sold_date_sk#104) AND (isnotnull(cs_item_sk#119) AND isnotnull(cs_sold_time_sk#105)))
         :        :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#104,cs_sold_time_sk#105,cs_ship_date_sk#106,cs_bill_customer_sk#107,cs_bill_cdemo_sk#108,cs_bill_hdemo_sk#109,cs_bill_addr_sk#110,cs_ship_customer_sk#111,cs_ship_cdemo_sk#112,cs_ship_hdemo_sk#113,cs_ship_addr_sk#114,cs_call_center_sk#115,cs_catalog_page_sk#116,cs_ship_mode_sk#117,cs_warehouse_sk#118,cs_item_sk#119,cs_promo_sk#120,cs_order_number#121,cs_quantity#122,cs_wholesale_cost#123,cs_list_price#124,cs_sales_price#125,cs_ext_discount_amt#126,cs_ext_sales_price#127,... 10 more fields] parquet
         :        :     +- Project [d_date_sk#171]
         :        :        +- Filter (((isnotnull(d_moy#179) AND isnotnull(d_year#177)) AND ((d_moy#179 = 11) AND (d_year#177 = 1999))) AND isnotnull(d_date_sk#171))
         :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#171,d_date_id#172,d_date#173,d_month_seq#174,d_week_seq#175,d_quarter_seq#176,d_year#177,d_dow#178,d_moy#179,d_dom#180,d_qoy#181,d_fy_year#182,d_fy_quarter_seq#183,d_fy_week_seq#184,d_day_name#185,d_quarter_name#186,d_holiday#187,d_weekend#188,d_following_holiday#189,d_first_dom#190,d_last_dom#191,d_same_day_ly#192,d_same_day_lq#193,d_current_day#194,... 4 more fields] parquet
         :        +- Project [ss_ext_sales_price#153 AS ext_price#8, ss_item_sk#140 AS sold_item_sk#10, ss_sold_time_sk#139 AS time_sk#11]
         :           +- Join Inner, (d_date_sk#199 = ss_sold_date_sk#138)
         :              :- Project [ss_sold_date_sk#138, ss_sold_time_sk#139, ss_item_sk#140, ss_ext_sales_price#153]
         :              :  +- Filter (isnotnull(ss_sold_date_sk#138) AND (isnotnull(ss_item_sk#140) AND isnotnull(ss_sold_time_sk#139)))
         :              :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#138,ss_sold_time_sk#139,ss_item_sk#140,ss_customer_sk#141,ss_cdemo_sk#142,ss_hdemo_sk#143,ss_addr_sk#144,ss_store_sk#145,ss_promo_sk#146,ss_ticket_number#147,ss_quantity#148,ss_wholesale_cost#149,ss_list_price#150,ss_sales_price#151,ss_ext_discount_amt#152,ss_ext_sales_price#153,ss_ext_wholesale_cost#154,ss_ext_list_price#155,ss_ext_tax#156,ss_coupon_amt#157,ss_net_paid#158,ss_net_paid_inc_tax#159,ss_net_profit#160] parquet
         :              +- Project [d_date_sk#199]
         :                 +- Filter (((isnotnull(d_moy#207) AND isnotnull(d_year#205)) AND ((d_moy#207 = 11) AND (d_year#205 = 1999))) AND isnotnull(d_date_sk#199))
         :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#199,d_date_id#200,d_date#201,d_month_seq#202,d_week_seq#203,d_quarter_seq#204,d_year#205,d_dow#206,d_moy#207,d_dom#208,d_qoy#209,d_fy_year#210,d_fy_quarter_seq#211,d_fy_week_seq#212,d_day_name#213,d_quarter_name#214,d_holiday#215,d_weekend#216,d_following_holiday#217,d_first_dom#218,d_last_dom#219,d_same_day_ly#220,d_same_day_lq#221,d_current_day#222,... 4 more fields] parquet
         +- Project [t_time_sk#161, t_hour#164, t_minute#165]
            +- Filter (((t_meal_time#170 = breakfast) OR (t_meal_time#170 = dinner)) AND isnotnull(t_time_sk#161))
               +- Relation spark_catalog.tpcds.time_dim[t_time_sk#161,t_time_id#162,t_time#163,t_hour#164,t_minute#165,t_second#166,t_am_pm#167,t_shift#168,t_sub_shift#169,t_meal_time#170] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [ext_price#14 DESC NULLS LAST, brand_id#12 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(ext_price#14 DESC NULLS LAST, brand_id#12 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=162]
      +- HashAggregate(keys=[i_brand#28, i_brand_id#27, t_hour#164, t_minute#165], functions=[sum(ext_price#0)], output=[brand_id#12, brand#13, t_hour#164, t_minute#165, ext_price#14])
         +- Exchange hashpartitioning(i_brand#28, i_brand_id#27, t_hour#164, t_minute#165, 200), ENSURE_REQUIREMENTS, [plan_id=159]
            +- HashAggregate(keys=[i_brand#28, i_brand_id#27, t_hour#164, t_minute#165], functions=[partial_sum(ext_price#0)], output=[i_brand#28, i_brand_id#27, t_hour#164, t_minute#165, sum#237])
               +- Project [i_brand_id#27, i_brand#28, ext_price#0, t_hour#164, t_minute#165]
                  +- BroadcastHashJoin [time_sk#3], [t_time_sk#161], Inner, BuildRight, false
                     :- Project [i_brand_id#27, i_brand#28, ext_price#0, time_sk#3]
                     :  +- BroadcastHashJoin [i_item_sk#20], [sold_item_sk#2], Inner, BuildLeft, false
                     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=150]
                     :     :  +- Project [i_item_sk#20, i_brand_id#27, i_brand#28]
                     :     :     +- Filter ((isnotnull(i_manager_id#40) AND (i_manager_id#40 = 1)) AND isnotnull(i_item_sk#20))
                     :     :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#20,i_brand_id#27,i_brand#28,i_manager_id#40] Batched: true, DataFilters: [isnotnull(i_manager_id#40), (i_manager_id#40 = 1), isnotnull(i_item_sk#20)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manager_id), EqualTo(i_manager_id,1), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_brand:string,i_manager_id:int>
                     :     +- Union
                     :        :- Project [ws_ext_sales_price#65 AS ext_price#0, ws_item_sk#45 AS sold_item_sk#2, ws_sold_time_sk#43 AS time_sk#3]
                     :        :  +- BroadcastHashJoin [ws_sold_date_sk#42], [d_date_sk#76], Inner, BuildRight, false
                     :        :     :- Filter ((isnotnull(ws_sold_date_sk#42) AND isnotnull(ws_item_sk#45)) AND isnotnull(ws_sold_time_sk#43))
                     :        :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#42,ws_sold_time_sk#43,ws_item_sk#45,ws_ext_sales_price#65] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#42), isnotnull(ws_item_sk#45), isnotnull(ws_sold_time_sk#43)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_item_sk), IsNotNull(ws_sold_time_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_sold_time_sk:int,ws_item_sk:int,ws_ext_sales_price:double>
                     :        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=139]
                     :        :        +- Project [d_date_sk#76]
                     :        :           +- Filter ((((isnotnull(d_moy#84) AND isnotnull(d_year#82)) AND (d_moy#84 = 11)) AND (d_year#82 = 1999)) AND isnotnull(d_date_sk#76))
                     :        :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#76,d_year#82,d_moy#84] Batched: true, DataFilters: [isnotnull(d_moy#84), isnotnull(d_year#82), (d_moy#84 = 11), (d_year#82 = 1999), isnotnull(d_date..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,11), EqualTo(d_year,1999), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                     :        :- Project [cs_ext_sales_price#127 AS ext_price#4, cs_item_sk#119 AS sold_item_sk#6, cs_sold_time_sk#105 AS time_sk#7]
                     :        :  +- BroadcastHashJoin [cs_sold_date_sk#104], [d_date_sk#171], Inner, BuildRight, false
                     :        :     :- Filter ((isnotnull(cs_sold_date_sk#104) AND isnotnull(cs_item_sk#119)) AND isnotnull(cs_sold_time_sk#105))
                     :        :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#104,cs_sold_time_sk#105,cs_item_sk#119,cs_ext_sales_price#127] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#104), isnotnull(cs_item_sk#119), isnotnull(cs_sold_time_sk#105)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_item_sk), IsNotNull(cs_sold_time_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_sold_time_sk:int,cs_item_sk:int,cs_ext_sales_price:double>
                     :        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=142]
                     :        :        +- Project [d_date_sk#171]
                     :        :           +- Filter ((((isnotnull(d_moy#179) AND isnotnull(d_year#177)) AND (d_moy#179 = 11)) AND (d_year#177 = 1999)) AND isnotnull(d_date_sk#171))
                     :        :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#171,d_year#177,d_moy#179] Batched: true, DataFilters: [isnotnull(d_moy#179), isnotnull(d_year#177), (d_moy#179 = 11), (d_year#177 = 1999), isnotnull(d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,11), EqualTo(d_year,1999), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                     :        +- Project [ss_ext_sales_price#153 AS ext_price#8, ss_item_sk#140 AS sold_item_sk#10, ss_sold_time_sk#139 AS time_sk#11]
                     :           +- BroadcastHashJoin [ss_sold_date_sk#138], [d_date_sk#199], Inner, BuildRight, false
                     :              :- Filter ((isnotnull(ss_sold_date_sk#138) AND isnotnull(ss_item_sk#140)) AND isnotnull(ss_sold_time_sk#139))
                     :              :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#138,ss_sold_time_sk#139,ss_item_sk#140,ss_ext_sales_price#153] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#138), isnotnull(ss_item_sk#140), isnotnull(ss_sold_time_sk#139)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk), IsNotNull(ss_sold_time_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_sold_time_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
                     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=145]
                     :                 +- Project [d_date_sk#199]
                     :                    +- Filter ((((isnotnull(d_moy#207) AND isnotnull(d_year#205)) AND (d_moy#207 = 11)) AND (d_year#205 = 1999)) AND isnotnull(d_date_sk#199))
                     :                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#199,d_year#205,d_moy#207] Batched: true, DataFilters: [isnotnull(d_moy#207), isnotnull(d_year#205), (d_moy#207 = 11), (d_year#205 = 1999), isnotnull(d_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,11), EqualTo(d_year,1999), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=154]
                        +- Project [t_time_sk#161, t_hour#164, t_minute#165]
                           +- Filter (((t_meal_time#170 = breakfast) OR (t_meal_time#170 = dinner)) AND isnotnull(t_time_sk#161))
                              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#161,t_hour#164,t_minute#165,t_meal_time#170] Batched: true, DataFilters: [((t_meal_time#170 = breakfast) OR (t_meal_time#170 = dinner)), isnotnull(t_time_sk#161)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(EqualTo(t_meal_time,breakfast),EqualTo(t_meal_time,dinner)), IsNotNull(t_time_sk)], ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int,t_meal_time:string>

Time taken: 2.86 seconds, Fetched 1 row(s)
