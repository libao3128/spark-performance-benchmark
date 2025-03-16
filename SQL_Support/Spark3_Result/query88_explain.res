Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582993619
== Parsed Logical Plan ==
'Project [*]
+- 'Join Inner
   :- 'Join Inner
   :  :- 'Join Inner
   :  :  :- 'Join Inner
   :  :  :  :- 'Join Inner
   :  :  :  :  :- 'Join Inner
   :  :  :  :  :  :- 'Join Inner
   :  :  :  :  :  :  :- 'SubqueryAlias s1
   :  :  :  :  :  :  :  +- 'Project ['count(1) AS h8_30_to_9#0]
   :  :  :  :  :  :  :     +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) AND ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND (('ss_store_sk = 's_store_sk) AND ('time_dim.t_hour = 8))) AND ((('time_dim.t_minute >= 30) AND (((('household_demographics.hd_dep_count = 4) AND ('household_demographics.hd_vehicle_count <= (4 + 2))) OR (('household_demographics.hd_dep_count = 2) AND ('household_demographics.hd_vehicle_count <= (2 + 2)))) OR (('household_demographics.hd_dep_count = 0) AND ('household_demographics.hd_vehicle_count <= (0 + 2))))) AND ('store.s_store_name = ese)))
   :  :  :  :  :  :  :        +- 'Join Inner
   :  :  :  :  :  :  :           :- 'Join Inner
   :  :  :  :  :  :  :           :  :- 'Join Inner
   :  :  :  :  :  :  :           :  :  :- 'UnresolvedRelation [store_sales], [], false
   :  :  :  :  :  :  :           :  :  +- 'UnresolvedRelation [household_demographics], [], false
   :  :  :  :  :  :  :           :  +- 'UnresolvedRelation [time_dim], [], false
   :  :  :  :  :  :  :           +- 'UnresolvedRelation [store], [], false
   :  :  :  :  :  :  +- 'SubqueryAlias s2
   :  :  :  :  :  :     +- 'Project ['count(1) AS h9_to_9_30#1]
   :  :  :  :  :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) AND ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND (('ss_store_sk = 's_store_sk) AND ('time_dim.t_hour = 9))) AND ((('time_dim.t_minute < 30) AND (((('household_demographics.hd_dep_count = 4) AND ('household_demographics.hd_vehicle_count <= (4 + 2))) OR (('household_demographics.hd_dep_count = 2) AND ('household_demographics.hd_vehicle_count <= (2 + 2)))) OR (('household_demographics.hd_dep_count = 0) AND ('household_demographics.hd_vehicle_count <= (0 + 2))))) AND ('store.s_store_name = ese)))
   :  :  :  :  :  :           +- 'Join Inner
   :  :  :  :  :  :              :- 'Join Inner
   :  :  :  :  :  :              :  :- 'Join Inner
   :  :  :  :  :  :              :  :  :- 'UnresolvedRelation [store_sales], [], false
   :  :  :  :  :  :              :  :  +- 'UnresolvedRelation [household_demographics], [], false
   :  :  :  :  :  :              :  +- 'UnresolvedRelation [time_dim], [], false
   :  :  :  :  :  :              +- 'UnresolvedRelation [store], [], false
   :  :  :  :  :  +- 'SubqueryAlias s3
   :  :  :  :  :     +- 'Project ['count(1) AS h9_30_to_10#2]
   :  :  :  :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) AND ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND (('ss_store_sk = 's_store_sk) AND ('time_dim.t_hour = 9))) AND ((('time_dim.t_minute >= 30) AND (((('household_demographics.hd_dep_count = 4) AND ('household_demographics.hd_vehicle_count <= (4 + 2))) OR (('household_demographics.hd_dep_count = 2) AND ('household_demographics.hd_vehicle_count <= (2 + 2)))) OR (('household_demographics.hd_dep_count = 0) AND ('household_demographics.hd_vehicle_count <= (0 + 2))))) AND ('store.s_store_name = ese)))
   :  :  :  :  :           +- 'Join Inner
   :  :  :  :  :              :- 'Join Inner
   :  :  :  :  :              :  :- 'Join Inner
   :  :  :  :  :              :  :  :- 'UnresolvedRelation [store_sales], [], false
   :  :  :  :  :              :  :  +- 'UnresolvedRelation [household_demographics], [], false
   :  :  :  :  :              :  +- 'UnresolvedRelation [time_dim], [], false
   :  :  :  :  :              +- 'UnresolvedRelation [store], [], false
   :  :  :  :  +- 'SubqueryAlias s4
   :  :  :  :     +- 'Project ['count(1) AS h10_to_10_30#3]
   :  :  :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) AND ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND (('ss_store_sk = 's_store_sk) AND ('time_dim.t_hour = 10))) AND ((('time_dim.t_minute < 30) AND (((('household_demographics.hd_dep_count = 4) AND ('household_demographics.hd_vehicle_count <= (4 + 2))) OR (('household_demographics.hd_dep_count = 2) AND ('household_demographics.hd_vehicle_count <= (2 + 2)))) OR (('household_demographics.hd_dep_count = 0) AND ('household_demographics.hd_vehicle_count <= (0 + 2))))) AND ('store.s_store_name = ese)))
   :  :  :  :           +- 'Join Inner
   :  :  :  :              :- 'Join Inner
   :  :  :  :              :  :- 'Join Inner
   :  :  :  :              :  :  :- 'UnresolvedRelation [store_sales], [], false
   :  :  :  :              :  :  +- 'UnresolvedRelation [household_demographics], [], false
   :  :  :  :              :  +- 'UnresolvedRelation [time_dim], [], false
   :  :  :  :              +- 'UnresolvedRelation [store], [], false
   :  :  :  +- 'SubqueryAlias s5
   :  :  :     +- 'Project ['count(1) AS h10_30_to_11#4]
   :  :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) AND ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND (('ss_store_sk = 's_store_sk) AND ('time_dim.t_hour = 10))) AND ((('time_dim.t_minute >= 30) AND (((('household_demographics.hd_dep_count = 4) AND ('household_demographics.hd_vehicle_count <= (4 + 2))) OR (('household_demographics.hd_dep_count = 2) AND ('household_demographics.hd_vehicle_count <= (2 + 2)))) OR (('household_demographics.hd_dep_count = 0) AND ('household_demographics.hd_vehicle_count <= (0 + 2))))) AND ('store.s_store_name = ese)))
   :  :  :           +- 'Join Inner
   :  :  :              :- 'Join Inner
   :  :  :              :  :- 'Join Inner
   :  :  :              :  :  :- 'UnresolvedRelation [store_sales], [], false
   :  :  :              :  :  +- 'UnresolvedRelation [household_demographics], [], false
   :  :  :              :  +- 'UnresolvedRelation [time_dim], [], false
   :  :  :              +- 'UnresolvedRelation [store], [], false
   :  :  +- 'SubqueryAlias s6
   :  :     +- 'Project ['count(1) AS h11_to_11_30#5]
   :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) AND ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND (('ss_store_sk = 's_store_sk) AND ('time_dim.t_hour = 11))) AND ((('time_dim.t_minute < 30) AND (((('household_demographics.hd_dep_count = 4) AND ('household_demographics.hd_vehicle_count <= (4 + 2))) OR (('household_demographics.hd_dep_count = 2) AND ('household_demographics.hd_vehicle_count <= (2 + 2)))) OR (('household_demographics.hd_dep_count = 0) AND ('household_demographics.hd_vehicle_count <= (0 + 2))))) AND ('store.s_store_name = ese)))
   :  :           +- 'Join Inner
   :  :              :- 'Join Inner
   :  :              :  :- 'Join Inner
   :  :              :  :  :- 'UnresolvedRelation [store_sales], [], false
   :  :              :  :  +- 'UnresolvedRelation [household_demographics], [], false
   :  :              :  +- 'UnresolvedRelation [time_dim], [], false
   :  :              +- 'UnresolvedRelation [store], [], false
   :  +- 'SubqueryAlias s7
   :     +- 'Project ['count(1) AS h11_30_to_12#6]
   :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) AND ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND (('ss_store_sk = 's_store_sk) AND ('time_dim.t_hour = 11))) AND ((('time_dim.t_minute >= 30) AND (((('household_demographics.hd_dep_count = 4) AND ('household_demographics.hd_vehicle_count <= (4 + 2))) OR (('household_demographics.hd_dep_count = 2) AND ('household_demographics.hd_vehicle_count <= (2 + 2)))) OR (('household_demographics.hd_dep_count = 0) AND ('household_demographics.hd_vehicle_count <= (0 + 2))))) AND ('store.s_store_name = ese)))
   :           +- 'Join Inner
   :              :- 'Join Inner
   :              :  :- 'Join Inner
   :              :  :  :- 'UnresolvedRelation [store_sales], [], false
   :              :  :  +- 'UnresolvedRelation [household_demographics], [], false
   :              :  +- 'UnresolvedRelation [time_dim], [], false
   :              +- 'UnresolvedRelation [store], [], false
   +- 'SubqueryAlias s8
      +- 'Project ['count(1) AS h12_to_12_30#7]
         +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) AND ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) AND (('ss_store_sk = 's_store_sk) AND ('time_dim.t_hour = 12))) AND ((('time_dim.t_minute < 30) AND (((('household_demographics.hd_dep_count = 4) AND ('household_demographics.hd_vehicle_count <= (4 + 2))) OR (('household_demographics.hd_dep_count = 2) AND ('household_demographics.hd_vehicle_count <= (2 + 2)))) OR (('household_demographics.hd_dep_count = 0) AND ('household_demographics.hd_vehicle_count <= (0 + 2))))) AND ('store.s_store_name = ese)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation [store_sales], [], false
               :  :  +- 'UnresolvedRelation [household_demographics], [], false
               :  +- 'UnresolvedRelation [time_dim], [], false
               +- 'UnresolvedRelation [store], [], false

== Analyzed Logical Plan ==
h8_30_to_9: bigint, h9_to_9_30: bigint, h9_30_to_10: bigint, h10_to_10_30: bigint, h10_30_to_11: bigint, h11_to_11_30: bigint, h11_30_to_12: bigint, h12_to_12_30: bigint
Project [h8_30_to_9#0L, h9_to_9_30#1L, h9_30_to_10#2L, h10_to_10_30#3L, h10_30_to_11#4L, h11_to_11_30#5L, h11_30_to_12#6L, h12_to_12_30#7L]
+- Join Inner
   :- Join Inner
   :  :- Join Inner
   :  :  :- Join Inner
   :  :  :  :- Join Inner
   :  :  :  :  :- Join Inner
   :  :  :  :  :  :- Join Inner
   :  :  :  :  :  :  :- SubqueryAlias s1
   :  :  :  :  :  :  :  +- Aggregate [count(1) AS h8_30_to_9#0L]
   :  :  :  :  :  :  :     +- Filter ((((ss_sold_time_sk#22 = t_time_sk#49) AND (ss_hdemo_sk#26 = hd_demo_sk#44)) AND ((ss_store_sk#28 = s_store_sk#59) AND (t_hour#52 = 8))) AND (((t_minute#53 >= 30) AND ((((hd_dep_count#47 = 4) AND (hd_vehicle_count#48 <= (4 + 2))) OR ((hd_dep_count#47 = 2) AND (hd_vehicle_count#48 <= (2 + 2)))) OR ((hd_dep_count#47 = 0) AND (hd_vehicle_count#48 <= (0 + 2))))) AND (s_store_name#64 = ese)))
   :  :  :  :  :  :  :        +- Join Inner
   :  :  :  :  :  :  :           :- Join Inner
   :  :  :  :  :  :  :           :  :- Join Inner
   :  :  :  :  :  :  :           :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
   :  :  :  :  :  :  :           :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_sold_time_sk#22,ss_item_sk#23,ss_customer_sk#24,ss_cdemo_sk#25,ss_hdemo_sk#26,ss_addr_sk#27,ss_store_sk#28,ss_promo_sk#29,ss_ticket_number#30,ss_quantity#31,ss_wholesale_cost#32,ss_list_price#33,ss_sales_price#34,ss_ext_discount_amt#35,ss_ext_sales_price#36,ss_ext_wholesale_cost#37,ss_ext_list_price#38,ss_ext_tax#39,ss_coupon_amt#40,ss_net_paid#41,ss_net_paid_inc_tax#42,ss_net_profit#43] parquet
   :  :  :  :  :  :  :           :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
   :  :  :  :  :  :  :           :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#44,hd_income_band_sk#45,hd_buy_potential#46,hd_dep_count#47,hd_vehicle_count#48] parquet
   :  :  :  :  :  :  :           :  +- SubqueryAlias spark_catalog.tpcds.time_dim
   :  :  :  :  :  :  :           :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#49,t_time_id#50,t_time#51,t_hour#52,t_minute#53,t_second#54,t_am_pm#55,t_shift#56,t_sub_shift#57,t_meal_time#58] parquet
   :  :  :  :  :  :  :           +- SubqueryAlias spark_catalog.tpcds.store
   :  :  :  :  :  :  :              +- Relation spark_catalog.tpcds.store[s_store_sk#59,s_store_id#60,s_rec_start_date#61,s_rec_end_date#62,s_closed_date_sk#63,s_store_name#64,s_number_employees#65,s_floor_space#66,s_hours#67,s_manager#68,s_market_id#69,s_geography_class#70,s_market_desc#71,s_market_manager#72,s_division_id#73,s_division_name#74,s_company_id#75,s_company_name#76,s_street_number#77,s_street_name#78,s_street_type#79,s_suite_number#80,s_city#81,s_county#82,... 5 more fields] parquet
   :  :  :  :  :  :  +- SubqueryAlias s2
   :  :  :  :  :  :     +- Aggregate [count(1) AS h9_to_9_30#1L]
   :  :  :  :  :  :        +- Filter ((((ss_sold_time_sk#89 = t_time_sk#116) AND (ss_hdemo_sk#93 = hd_demo_sk#111)) AND ((ss_store_sk#95 = s_store_sk#126) AND (t_hour#119 = 9))) AND (((t_minute#120 < 30) AND ((((hd_dep_count#114 = 4) AND (hd_vehicle_count#115 <= (4 + 2))) OR ((hd_dep_count#114 = 2) AND (hd_vehicle_count#115 <= (2 + 2)))) OR ((hd_dep_count#114 = 0) AND (hd_vehicle_count#115 <= (0 + 2))))) AND (s_store_name#131 = ese)))
   :  :  :  :  :  :           +- Join Inner
   :  :  :  :  :  :              :- Join Inner
   :  :  :  :  :  :              :  :- Join Inner
   :  :  :  :  :  :              :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
   :  :  :  :  :  :              :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#88,ss_sold_time_sk#89,ss_item_sk#90,ss_customer_sk#91,ss_cdemo_sk#92,ss_hdemo_sk#93,ss_addr_sk#94,ss_store_sk#95,ss_promo_sk#96,ss_ticket_number#97,ss_quantity#98,ss_wholesale_cost#99,ss_list_price#100,ss_sales_price#101,ss_ext_discount_amt#102,ss_ext_sales_price#103,ss_ext_wholesale_cost#104,ss_ext_list_price#105,ss_ext_tax#106,ss_coupon_amt#107,ss_net_paid#108,ss_net_paid_inc_tax#109,ss_net_profit#110] parquet
   :  :  :  :  :  :              :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
   :  :  :  :  :  :              :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#111,hd_income_band_sk#112,hd_buy_potential#113,hd_dep_count#114,hd_vehicle_count#115] parquet
   :  :  :  :  :  :              :  +- SubqueryAlias spark_catalog.tpcds.time_dim
   :  :  :  :  :  :              :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#116,t_time_id#117,t_time#118,t_hour#119,t_minute#120,t_second#121,t_am_pm#122,t_shift#123,t_sub_shift#124,t_meal_time#125] parquet
   :  :  :  :  :  :              +- SubqueryAlias spark_catalog.tpcds.store
   :  :  :  :  :  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#126,s_store_id#127,s_rec_start_date#128,s_rec_end_date#129,s_closed_date_sk#130,s_store_name#131,s_number_employees#132,s_floor_space#133,s_hours#134,s_manager#135,s_market_id#136,s_geography_class#137,s_market_desc#138,s_market_manager#139,s_division_id#140,s_division_name#141,s_company_id#142,s_company_name#143,s_street_number#144,s_street_name#145,s_street_type#146,s_suite_number#147,s_city#148,s_county#149,... 5 more fields] parquet
   :  :  :  :  :  +- SubqueryAlias s3
   :  :  :  :  :     +- Aggregate [count(1) AS h9_30_to_10#2L]
   :  :  :  :  :        +- Filter ((((ss_sold_time_sk#156 = t_time_sk#183) AND (ss_hdemo_sk#160 = hd_demo_sk#178)) AND ((ss_store_sk#162 = s_store_sk#193) AND (t_hour#186 = 9))) AND (((t_minute#187 >= 30) AND ((((hd_dep_count#181 = 4) AND (hd_vehicle_count#182 <= (4 + 2))) OR ((hd_dep_count#181 = 2) AND (hd_vehicle_count#182 <= (2 + 2)))) OR ((hd_dep_count#181 = 0) AND (hd_vehicle_count#182 <= (0 + 2))))) AND (s_store_name#198 = ese)))
   :  :  :  :  :           +- Join Inner
   :  :  :  :  :              :- Join Inner
   :  :  :  :  :              :  :- Join Inner
   :  :  :  :  :              :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
   :  :  :  :  :              :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#155,ss_sold_time_sk#156,ss_item_sk#157,ss_customer_sk#158,ss_cdemo_sk#159,ss_hdemo_sk#160,ss_addr_sk#161,ss_store_sk#162,ss_promo_sk#163,ss_ticket_number#164,ss_quantity#165,ss_wholesale_cost#166,ss_list_price#167,ss_sales_price#168,ss_ext_discount_amt#169,ss_ext_sales_price#170,ss_ext_wholesale_cost#171,ss_ext_list_price#172,ss_ext_tax#173,ss_coupon_amt#174,ss_net_paid#175,ss_net_paid_inc_tax#176,ss_net_profit#177] parquet
   :  :  :  :  :              :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
   :  :  :  :  :              :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#178,hd_income_band_sk#179,hd_buy_potential#180,hd_dep_count#181,hd_vehicle_count#182] parquet
   :  :  :  :  :              :  +- SubqueryAlias spark_catalog.tpcds.time_dim
   :  :  :  :  :              :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#183,t_time_id#184,t_time#185,t_hour#186,t_minute#187,t_second#188,t_am_pm#189,t_shift#190,t_sub_shift#191,t_meal_time#192] parquet
   :  :  :  :  :              +- SubqueryAlias spark_catalog.tpcds.store
   :  :  :  :  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#193,s_store_id#194,s_rec_start_date#195,s_rec_end_date#196,s_closed_date_sk#197,s_store_name#198,s_number_employees#199,s_floor_space#200,s_hours#201,s_manager#202,s_market_id#203,s_geography_class#204,s_market_desc#205,s_market_manager#206,s_division_id#207,s_division_name#208,s_company_id#209,s_company_name#210,s_street_number#211,s_street_name#212,s_street_type#213,s_suite_number#214,s_city#215,s_county#216,... 5 more fields] parquet
   :  :  :  :  +- SubqueryAlias s4
   :  :  :  :     +- Aggregate [count(1) AS h10_to_10_30#3L]
   :  :  :  :        +- Filter ((((ss_sold_time_sk#223 = t_time_sk#250) AND (ss_hdemo_sk#227 = hd_demo_sk#245)) AND ((ss_store_sk#229 = s_store_sk#260) AND (t_hour#253 = 10))) AND (((t_minute#254 < 30) AND ((((hd_dep_count#248 = 4) AND (hd_vehicle_count#249 <= (4 + 2))) OR ((hd_dep_count#248 = 2) AND (hd_vehicle_count#249 <= (2 + 2)))) OR ((hd_dep_count#248 = 0) AND (hd_vehicle_count#249 <= (0 + 2))))) AND (s_store_name#265 = ese)))
   :  :  :  :           +- Join Inner
   :  :  :  :              :- Join Inner
   :  :  :  :              :  :- Join Inner
   :  :  :  :              :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
   :  :  :  :              :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#222,ss_sold_time_sk#223,ss_item_sk#224,ss_customer_sk#225,ss_cdemo_sk#226,ss_hdemo_sk#227,ss_addr_sk#228,ss_store_sk#229,ss_promo_sk#230,ss_ticket_number#231,ss_quantity#232,ss_wholesale_cost#233,ss_list_price#234,ss_sales_price#235,ss_ext_discount_amt#236,ss_ext_sales_price#237,ss_ext_wholesale_cost#238,ss_ext_list_price#239,ss_ext_tax#240,ss_coupon_amt#241,ss_net_paid#242,ss_net_paid_inc_tax#243,ss_net_profit#244] parquet
   :  :  :  :              :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
   :  :  :  :              :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#245,hd_income_band_sk#246,hd_buy_potential#247,hd_dep_count#248,hd_vehicle_count#249] parquet
   :  :  :  :              :  +- SubqueryAlias spark_catalog.tpcds.time_dim
   :  :  :  :              :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#250,t_time_id#251,t_time#252,t_hour#253,t_minute#254,t_second#255,t_am_pm#256,t_shift#257,t_sub_shift#258,t_meal_time#259] parquet
   :  :  :  :              +- SubqueryAlias spark_catalog.tpcds.store
   :  :  :  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#260,s_store_id#261,s_rec_start_date#262,s_rec_end_date#263,s_closed_date_sk#264,s_store_name#265,s_number_employees#266,s_floor_space#267,s_hours#268,s_manager#269,s_market_id#270,s_geography_class#271,s_market_desc#272,s_market_manager#273,s_division_id#274,s_division_name#275,s_company_id#276,s_company_name#277,s_street_number#278,s_street_name#279,s_street_type#280,s_suite_number#281,s_city#282,s_county#283,... 5 more fields] parquet
   :  :  :  +- SubqueryAlias s5
   :  :  :     +- Aggregate [count(1) AS h10_30_to_11#4L]
   :  :  :        +- Filter ((((ss_sold_time_sk#290 = t_time_sk#317) AND (ss_hdemo_sk#294 = hd_demo_sk#312)) AND ((ss_store_sk#296 = s_store_sk#327) AND (t_hour#320 = 10))) AND (((t_minute#321 >= 30) AND ((((hd_dep_count#315 = 4) AND (hd_vehicle_count#316 <= (4 + 2))) OR ((hd_dep_count#315 = 2) AND (hd_vehicle_count#316 <= (2 + 2)))) OR ((hd_dep_count#315 = 0) AND (hd_vehicle_count#316 <= (0 + 2))))) AND (s_store_name#332 = ese)))
   :  :  :           +- Join Inner
   :  :  :              :- Join Inner
   :  :  :              :  :- Join Inner
   :  :  :              :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
   :  :  :              :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#289,ss_sold_time_sk#290,ss_item_sk#291,ss_customer_sk#292,ss_cdemo_sk#293,ss_hdemo_sk#294,ss_addr_sk#295,ss_store_sk#296,ss_promo_sk#297,ss_ticket_number#298,ss_quantity#299,ss_wholesale_cost#300,ss_list_price#301,ss_sales_price#302,ss_ext_discount_amt#303,ss_ext_sales_price#304,ss_ext_wholesale_cost#305,ss_ext_list_price#306,ss_ext_tax#307,ss_coupon_amt#308,ss_net_paid#309,ss_net_paid_inc_tax#310,ss_net_profit#311] parquet
   :  :  :              :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
   :  :  :              :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#312,hd_income_band_sk#313,hd_buy_potential#314,hd_dep_count#315,hd_vehicle_count#316] parquet
   :  :  :              :  +- SubqueryAlias spark_catalog.tpcds.time_dim
   :  :  :              :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#317,t_time_id#318,t_time#319,t_hour#320,t_minute#321,t_second#322,t_am_pm#323,t_shift#324,t_sub_shift#325,t_meal_time#326] parquet
   :  :  :              +- SubqueryAlias spark_catalog.tpcds.store
   :  :  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#327,s_store_id#328,s_rec_start_date#329,s_rec_end_date#330,s_closed_date_sk#331,s_store_name#332,s_number_employees#333,s_floor_space#334,s_hours#335,s_manager#336,s_market_id#337,s_geography_class#338,s_market_desc#339,s_market_manager#340,s_division_id#341,s_division_name#342,s_company_id#343,s_company_name#344,s_street_number#345,s_street_name#346,s_street_type#347,s_suite_number#348,s_city#349,s_county#350,... 5 more fields] parquet
   :  :  +- SubqueryAlias s6
   :  :     +- Aggregate [count(1) AS h11_to_11_30#5L]
   :  :        +- Filter ((((ss_sold_time_sk#357 = t_time_sk#384) AND (ss_hdemo_sk#361 = hd_demo_sk#379)) AND ((ss_store_sk#363 = s_store_sk#394) AND (t_hour#387 = 11))) AND (((t_minute#388 < 30) AND ((((hd_dep_count#382 = 4) AND (hd_vehicle_count#383 <= (4 + 2))) OR ((hd_dep_count#382 = 2) AND (hd_vehicle_count#383 <= (2 + 2)))) OR ((hd_dep_count#382 = 0) AND (hd_vehicle_count#383 <= (0 + 2))))) AND (s_store_name#399 = ese)))
   :  :           +- Join Inner
   :  :              :- Join Inner
   :  :              :  :- Join Inner
   :  :              :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
   :  :              :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#356,ss_sold_time_sk#357,ss_item_sk#358,ss_customer_sk#359,ss_cdemo_sk#360,ss_hdemo_sk#361,ss_addr_sk#362,ss_store_sk#363,ss_promo_sk#364,ss_ticket_number#365,ss_quantity#366,ss_wholesale_cost#367,ss_list_price#368,ss_sales_price#369,ss_ext_discount_amt#370,ss_ext_sales_price#371,ss_ext_wholesale_cost#372,ss_ext_list_price#373,ss_ext_tax#374,ss_coupon_amt#375,ss_net_paid#376,ss_net_paid_inc_tax#377,ss_net_profit#378] parquet
   :  :              :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
   :  :              :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#379,hd_income_band_sk#380,hd_buy_potential#381,hd_dep_count#382,hd_vehicle_count#383] parquet
   :  :              :  +- SubqueryAlias spark_catalog.tpcds.time_dim
   :  :              :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#384,t_time_id#385,t_time#386,t_hour#387,t_minute#388,t_second#389,t_am_pm#390,t_shift#391,t_sub_shift#392,t_meal_time#393] parquet
   :  :              +- SubqueryAlias spark_catalog.tpcds.store
   :  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#394,s_store_id#395,s_rec_start_date#396,s_rec_end_date#397,s_closed_date_sk#398,s_store_name#399,s_number_employees#400,s_floor_space#401,s_hours#402,s_manager#403,s_market_id#404,s_geography_class#405,s_market_desc#406,s_market_manager#407,s_division_id#408,s_division_name#409,s_company_id#410,s_company_name#411,s_street_number#412,s_street_name#413,s_street_type#414,s_suite_number#415,s_city#416,s_county#417,... 5 more fields] parquet
   :  +- SubqueryAlias s7
   :     +- Aggregate [count(1) AS h11_30_to_12#6L]
   :        +- Filter ((((ss_sold_time_sk#424 = t_time_sk#451) AND (ss_hdemo_sk#428 = hd_demo_sk#446)) AND ((ss_store_sk#430 = s_store_sk#461) AND (t_hour#454 = 11))) AND (((t_minute#455 >= 30) AND ((((hd_dep_count#449 = 4) AND (hd_vehicle_count#450 <= (4 + 2))) OR ((hd_dep_count#449 = 2) AND (hd_vehicle_count#450 <= (2 + 2)))) OR ((hd_dep_count#449 = 0) AND (hd_vehicle_count#450 <= (0 + 2))))) AND (s_store_name#466 = ese)))
   :           +- Join Inner
   :              :- Join Inner
   :              :  :- Join Inner
   :              :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
   :              :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#423,ss_sold_time_sk#424,ss_item_sk#425,ss_customer_sk#426,ss_cdemo_sk#427,ss_hdemo_sk#428,ss_addr_sk#429,ss_store_sk#430,ss_promo_sk#431,ss_ticket_number#432,ss_quantity#433,ss_wholesale_cost#434,ss_list_price#435,ss_sales_price#436,ss_ext_discount_amt#437,ss_ext_sales_price#438,ss_ext_wholesale_cost#439,ss_ext_list_price#440,ss_ext_tax#441,ss_coupon_amt#442,ss_net_paid#443,ss_net_paid_inc_tax#444,ss_net_profit#445] parquet
   :              :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
   :              :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#446,hd_income_band_sk#447,hd_buy_potential#448,hd_dep_count#449,hd_vehicle_count#450] parquet
   :              :  +- SubqueryAlias spark_catalog.tpcds.time_dim
   :              :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#451,t_time_id#452,t_time#453,t_hour#454,t_minute#455,t_second#456,t_am_pm#457,t_shift#458,t_sub_shift#459,t_meal_time#460] parquet
   :              +- SubqueryAlias spark_catalog.tpcds.store
   :                 +- Relation spark_catalog.tpcds.store[s_store_sk#461,s_store_id#462,s_rec_start_date#463,s_rec_end_date#464,s_closed_date_sk#465,s_store_name#466,s_number_employees#467,s_floor_space#468,s_hours#469,s_manager#470,s_market_id#471,s_geography_class#472,s_market_desc#473,s_market_manager#474,s_division_id#475,s_division_name#476,s_company_id#477,s_company_name#478,s_street_number#479,s_street_name#480,s_street_type#481,s_suite_number#482,s_city#483,s_county#484,... 5 more fields] parquet
   +- SubqueryAlias s8
      +- Aggregate [count(1) AS h12_to_12_30#7L]
         +- Filter ((((ss_sold_time_sk#491 = t_time_sk#518) AND (ss_hdemo_sk#495 = hd_demo_sk#513)) AND ((ss_store_sk#497 = s_store_sk#528) AND (t_hour#521 = 12))) AND (((t_minute#522 < 30) AND ((((hd_dep_count#516 = 4) AND (hd_vehicle_count#517 <= (4 + 2))) OR ((hd_dep_count#516 = 2) AND (hd_vehicle_count#517 <= (2 + 2)))) OR ((hd_dep_count#516 = 0) AND (hd_vehicle_count#517 <= (0 + 2))))) AND (s_store_name#533 = ese)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#490,ss_sold_time_sk#491,ss_item_sk#492,ss_customer_sk#493,ss_cdemo_sk#494,ss_hdemo_sk#495,ss_addr_sk#496,ss_store_sk#497,ss_promo_sk#498,ss_ticket_number#499,ss_quantity#500,ss_wholesale_cost#501,ss_list_price#502,ss_sales_price#503,ss_ext_discount_amt#504,ss_ext_sales_price#505,ss_ext_wholesale_cost#506,ss_ext_list_price#507,ss_ext_tax#508,ss_coupon_amt#509,ss_net_paid#510,ss_net_paid_inc_tax#511,ss_net_profit#512] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
               :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#513,hd_income_band_sk#514,hd_buy_potential#515,hd_dep_count#516,hd_vehicle_count#517] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.time_dim
               :     +- Relation spark_catalog.tpcds.time_dim[t_time_sk#518,t_time_id#519,t_time#520,t_hour#521,t_minute#522,t_second#523,t_am_pm#524,t_shift#525,t_sub_shift#526,t_meal_time#527] parquet
               +- SubqueryAlias spark_catalog.tpcds.store
                  +- Relation spark_catalog.tpcds.store[s_store_sk#528,s_store_id#529,s_rec_start_date#530,s_rec_end_date#531,s_closed_date_sk#532,s_store_name#533,s_number_employees#534,s_floor_space#535,s_hours#536,s_manager#537,s_market_id#538,s_geography_class#539,s_market_desc#540,s_market_manager#541,s_division_id#542,s_division_name#543,s_company_id#544,s_company_name#545,s_street_number#546,s_street_name#547,s_street_type#548,s_suite_number#549,s_city#550,s_county#551,... 5 more fields] parquet

== Optimized Logical Plan ==
Join Inner
:- Join Inner
:  :- Join Inner
:  :  :- Join Inner
:  :  :  :- Join Inner
:  :  :  :  :- Join Inner
:  :  :  :  :  :- Join Inner
:  :  :  :  :  :  :- Aggregate [count(1) AS h8_30_to_9#0L]
:  :  :  :  :  :  :  +- Project
:  :  :  :  :  :  :     +- Join Inner, (ss_store_sk#28 = s_store_sk#59)
:  :  :  :  :  :  :        :- Project [ss_store_sk#28]
:  :  :  :  :  :  :        :  +- Join Inner, (ss_sold_time_sk#22 = t_time_sk#49)
:  :  :  :  :  :  :        :     :- Project [ss_sold_time_sk#22, ss_store_sk#28]
:  :  :  :  :  :  :        :     :  +- Join Inner, (ss_hdemo_sk#26 = hd_demo_sk#44)
:  :  :  :  :  :  :        :     :     :- Project [ss_sold_time_sk#22, ss_hdemo_sk#26, ss_store_sk#28]
:  :  :  :  :  :  :        :     :     :  +- Filter (isnotnull(ss_hdemo_sk#26) AND (isnotnull(ss_sold_time_sk#22) AND isnotnull(ss_store_sk#28)))
:  :  :  :  :  :  :        :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_sold_time_sk#22,ss_item_sk#23,ss_customer_sk#24,ss_cdemo_sk#25,ss_hdemo_sk#26,ss_addr_sk#27,ss_store_sk#28,ss_promo_sk#29,ss_ticket_number#30,ss_quantity#31,ss_wholesale_cost#32,ss_list_price#33,ss_sales_price#34,ss_ext_discount_amt#35,ss_ext_sales_price#36,ss_ext_wholesale_cost#37,ss_ext_list_price#38,ss_ext_tax#39,ss_coupon_amt#40,ss_net_paid#41,ss_net_paid_inc_tax#42,ss_net_profit#43] parquet
:  :  :  :  :  :  :        :     :     +- Project [hd_demo_sk#44]
:  :  :  :  :  :  :        :     :        +- Filter (((((hd_dep_count#47 = 4) AND (hd_vehicle_count#48 <= 6)) OR ((hd_dep_count#47 = 2) AND (hd_vehicle_count#48 <= 4))) OR ((hd_dep_count#47 = 0) AND (hd_vehicle_count#48 <= 2))) AND isnotnull(hd_demo_sk#44))
:  :  :  :  :  :  :        :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#44,hd_income_band_sk#45,hd_buy_potential#46,hd_dep_count#47,hd_vehicle_count#48] parquet
:  :  :  :  :  :  :        :     +- Project [t_time_sk#49]
:  :  :  :  :  :  :        :        +- Filter (((isnotnull(t_hour#52) AND isnotnull(t_minute#53)) AND ((t_hour#52 = 8) AND (t_minute#53 >= 30))) AND isnotnull(t_time_sk#49))
:  :  :  :  :  :  :        :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#49,t_time_id#50,t_time#51,t_hour#52,t_minute#53,t_second#54,t_am_pm#55,t_shift#56,t_sub_shift#57,t_meal_time#58] parquet
:  :  :  :  :  :  :        +- Project [s_store_sk#59]
:  :  :  :  :  :  :           +- Filter ((isnotnull(s_store_name#64) AND (s_store_name#64 = ese)) AND isnotnull(s_store_sk#59))
:  :  :  :  :  :  :              +- Relation spark_catalog.tpcds.store[s_store_sk#59,s_store_id#60,s_rec_start_date#61,s_rec_end_date#62,s_closed_date_sk#63,s_store_name#64,s_number_employees#65,s_floor_space#66,s_hours#67,s_manager#68,s_market_id#69,s_geography_class#70,s_market_desc#71,s_market_manager#72,s_division_id#73,s_division_name#74,s_company_id#75,s_company_name#76,s_street_number#77,s_street_name#78,s_street_type#79,s_suite_number#80,s_city#81,s_county#82,... 5 more fields] parquet
:  :  :  :  :  :  +- Aggregate [count(1) AS h9_to_9_30#1L]
:  :  :  :  :  :     +- Project
:  :  :  :  :  :        +- Join Inner, (ss_store_sk#95 = s_store_sk#126)
:  :  :  :  :  :           :- Project [ss_store_sk#95]
:  :  :  :  :  :           :  +- Join Inner, (ss_sold_time_sk#89 = t_time_sk#116)
:  :  :  :  :  :           :     :- Project [ss_sold_time_sk#89, ss_store_sk#95]
:  :  :  :  :  :           :     :  +- Join Inner, (ss_hdemo_sk#93 = hd_demo_sk#111)
:  :  :  :  :  :           :     :     :- Project [ss_sold_time_sk#89, ss_hdemo_sk#93, ss_store_sk#95]
:  :  :  :  :  :           :     :     :  +- Filter (isnotnull(ss_hdemo_sk#93) AND (isnotnull(ss_sold_time_sk#89) AND isnotnull(ss_store_sk#95)))
:  :  :  :  :  :           :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#88,ss_sold_time_sk#89,ss_item_sk#90,ss_customer_sk#91,ss_cdemo_sk#92,ss_hdemo_sk#93,ss_addr_sk#94,ss_store_sk#95,ss_promo_sk#96,ss_ticket_number#97,ss_quantity#98,ss_wholesale_cost#99,ss_list_price#100,ss_sales_price#101,ss_ext_discount_amt#102,ss_ext_sales_price#103,ss_ext_wholesale_cost#104,ss_ext_list_price#105,ss_ext_tax#106,ss_coupon_amt#107,ss_net_paid#108,ss_net_paid_inc_tax#109,ss_net_profit#110] parquet
:  :  :  :  :  :           :     :     +- Project [hd_demo_sk#111]
:  :  :  :  :  :           :     :        +- Filter (((((hd_dep_count#114 = 4) AND (hd_vehicle_count#115 <= 6)) OR ((hd_dep_count#114 = 2) AND (hd_vehicle_count#115 <= 4))) OR ((hd_dep_count#114 = 0) AND (hd_vehicle_count#115 <= 2))) AND isnotnull(hd_demo_sk#111))
:  :  :  :  :  :           :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#111,hd_income_band_sk#112,hd_buy_potential#113,hd_dep_count#114,hd_vehicle_count#115] parquet
:  :  :  :  :  :           :     +- Project [t_time_sk#116]
:  :  :  :  :  :           :        +- Filter (((isnotnull(t_hour#119) AND isnotnull(t_minute#120)) AND ((t_hour#119 = 9) AND (t_minute#120 < 30))) AND isnotnull(t_time_sk#116))
:  :  :  :  :  :           :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#116,t_time_id#117,t_time#118,t_hour#119,t_minute#120,t_second#121,t_am_pm#122,t_shift#123,t_sub_shift#124,t_meal_time#125] parquet
:  :  :  :  :  :           +- Project [s_store_sk#126]
:  :  :  :  :  :              +- Filter ((isnotnull(s_store_name#131) AND (s_store_name#131 = ese)) AND isnotnull(s_store_sk#126))
:  :  :  :  :  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#126,s_store_id#127,s_rec_start_date#128,s_rec_end_date#129,s_closed_date_sk#130,s_store_name#131,s_number_employees#132,s_floor_space#133,s_hours#134,s_manager#135,s_market_id#136,s_geography_class#137,s_market_desc#138,s_market_manager#139,s_division_id#140,s_division_name#141,s_company_id#142,s_company_name#143,s_street_number#144,s_street_name#145,s_street_type#146,s_suite_number#147,s_city#148,s_county#149,... 5 more fields] parquet
:  :  :  :  :  +- Aggregate [count(1) AS h9_30_to_10#2L]
:  :  :  :  :     +- Project
:  :  :  :  :        +- Join Inner, (ss_store_sk#162 = s_store_sk#193)
:  :  :  :  :           :- Project [ss_store_sk#162]
:  :  :  :  :           :  +- Join Inner, (ss_sold_time_sk#156 = t_time_sk#183)
:  :  :  :  :           :     :- Project [ss_sold_time_sk#156, ss_store_sk#162]
:  :  :  :  :           :     :  +- Join Inner, (ss_hdemo_sk#160 = hd_demo_sk#178)
:  :  :  :  :           :     :     :- Project [ss_sold_time_sk#156, ss_hdemo_sk#160, ss_store_sk#162]
:  :  :  :  :           :     :     :  +- Filter (isnotnull(ss_hdemo_sk#160) AND (isnotnull(ss_sold_time_sk#156) AND isnotnull(ss_store_sk#162)))
:  :  :  :  :           :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#155,ss_sold_time_sk#156,ss_item_sk#157,ss_customer_sk#158,ss_cdemo_sk#159,ss_hdemo_sk#160,ss_addr_sk#161,ss_store_sk#162,ss_promo_sk#163,ss_ticket_number#164,ss_quantity#165,ss_wholesale_cost#166,ss_list_price#167,ss_sales_price#168,ss_ext_discount_amt#169,ss_ext_sales_price#170,ss_ext_wholesale_cost#171,ss_ext_list_price#172,ss_ext_tax#173,ss_coupon_amt#174,ss_net_paid#175,ss_net_paid_inc_tax#176,ss_net_profit#177] parquet
:  :  :  :  :           :     :     +- Project [hd_demo_sk#178]
:  :  :  :  :           :     :        +- Filter (((((hd_dep_count#181 = 4) AND (hd_vehicle_count#182 <= 6)) OR ((hd_dep_count#181 = 2) AND (hd_vehicle_count#182 <= 4))) OR ((hd_dep_count#181 = 0) AND (hd_vehicle_count#182 <= 2))) AND isnotnull(hd_demo_sk#178))
:  :  :  :  :           :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#178,hd_income_band_sk#179,hd_buy_potential#180,hd_dep_count#181,hd_vehicle_count#182] parquet
:  :  :  :  :           :     +- Project [t_time_sk#183]
:  :  :  :  :           :        +- Filter (((isnotnull(t_hour#186) AND isnotnull(t_minute#187)) AND ((t_hour#186 = 9) AND (t_minute#187 >= 30))) AND isnotnull(t_time_sk#183))
:  :  :  :  :           :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#183,t_time_id#184,t_time#185,t_hour#186,t_minute#187,t_second#188,t_am_pm#189,t_shift#190,t_sub_shift#191,t_meal_time#192] parquet
:  :  :  :  :           +- Project [s_store_sk#193]
:  :  :  :  :              +- Filter ((isnotnull(s_store_name#198) AND (s_store_name#198 = ese)) AND isnotnull(s_store_sk#193))
:  :  :  :  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#193,s_store_id#194,s_rec_start_date#195,s_rec_end_date#196,s_closed_date_sk#197,s_store_name#198,s_number_employees#199,s_floor_space#200,s_hours#201,s_manager#202,s_market_id#203,s_geography_class#204,s_market_desc#205,s_market_manager#206,s_division_id#207,s_division_name#208,s_company_id#209,s_company_name#210,s_street_number#211,s_street_name#212,s_street_type#213,s_suite_number#214,s_city#215,s_county#216,... 5 more fields] parquet
:  :  :  :  +- Aggregate [count(1) AS h10_to_10_30#3L]
:  :  :  :     +- Project
:  :  :  :        +- Join Inner, (ss_store_sk#229 = s_store_sk#260)
:  :  :  :           :- Project [ss_store_sk#229]
:  :  :  :           :  +- Join Inner, (ss_sold_time_sk#223 = t_time_sk#250)
:  :  :  :           :     :- Project [ss_sold_time_sk#223, ss_store_sk#229]
:  :  :  :           :     :  +- Join Inner, (ss_hdemo_sk#227 = hd_demo_sk#245)
:  :  :  :           :     :     :- Project [ss_sold_time_sk#223, ss_hdemo_sk#227, ss_store_sk#229]
:  :  :  :           :     :     :  +- Filter (isnotnull(ss_hdemo_sk#227) AND (isnotnull(ss_sold_time_sk#223) AND isnotnull(ss_store_sk#229)))
:  :  :  :           :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#222,ss_sold_time_sk#223,ss_item_sk#224,ss_customer_sk#225,ss_cdemo_sk#226,ss_hdemo_sk#227,ss_addr_sk#228,ss_store_sk#229,ss_promo_sk#230,ss_ticket_number#231,ss_quantity#232,ss_wholesale_cost#233,ss_list_price#234,ss_sales_price#235,ss_ext_discount_amt#236,ss_ext_sales_price#237,ss_ext_wholesale_cost#238,ss_ext_list_price#239,ss_ext_tax#240,ss_coupon_amt#241,ss_net_paid#242,ss_net_paid_inc_tax#243,ss_net_profit#244] parquet
:  :  :  :           :     :     +- Project [hd_demo_sk#245]
:  :  :  :           :     :        +- Filter (((((hd_dep_count#248 = 4) AND (hd_vehicle_count#249 <= 6)) OR ((hd_dep_count#248 = 2) AND (hd_vehicle_count#249 <= 4))) OR ((hd_dep_count#248 = 0) AND (hd_vehicle_count#249 <= 2))) AND isnotnull(hd_demo_sk#245))
:  :  :  :           :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#245,hd_income_band_sk#246,hd_buy_potential#247,hd_dep_count#248,hd_vehicle_count#249] parquet
:  :  :  :           :     +- Project [t_time_sk#250]
:  :  :  :           :        +- Filter (((isnotnull(t_hour#253) AND isnotnull(t_minute#254)) AND ((t_hour#253 = 10) AND (t_minute#254 < 30))) AND isnotnull(t_time_sk#250))
:  :  :  :           :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#250,t_time_id#251,t_time#252,t_hour#253,t_minute#254,t_second#255,t_am_pm#256,t_shift#257,t_sub_shift#258,t_meal_time#259] parquet
:  :  :  :           +- Project [s_store_sk#260]
:  :  :  :              +- Filter ((isnotnull(s_store_name#265) AND (s_store_name#265 = ese)) AND isnotnull(s_store_sk#260))
:  :  :  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#260,s_store_id#261,s_rec_start_date#262,s_rec_end_date#263,s_closed_date_sk#264,s_store_name#265,s_number_employees#266,s_floor_space#267,s_hours#268,s_manager#269,s_market_id#270,s_geography_class#271,s_market_desc#272,s_market_manager#273,s_division_id#274,s_division_name#275,s_company_id#276,s_company_name#277,s_street_number#278,s_street_name#279,s_street_type#280,s_suite_number#281,s_city#282,s_county#283,... 5 more fields] parquet
:  :  :  +- Aggregate [count(1) AS h10_30_to_11#4L]
:  :  :     +- Project
:  :  :        +- Join Inner, (ss_store_sk#296 = s_store_sk#327)
:  :  :           :- Project [ss_store_sk#296]
:  :  :           :  +- Join Inner, (ss_sold_time_sk#290 = t_time_sk#317)
:  :  :           :     :- Project [ss_sold_time_sk#290, ss_store_sk#296]
:  :  :           :     :  +- Join Inner, (ss_hdemo_sk#294 = hd_demo_sk#312)
:  :  :           :     :     :- Project [ss_sold_time_sk#290, ss_hdemo_sk#294, ss_store_sk#296]
:  :  :           :     :     :  +- Filter (isnotnull(ss_hdemo_sk#294) AND (isnotnull(ss_sold_time_sk#290) AND isnotnull(ss_store_sk#296)))
:  :  :           :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#289,ss_sold_time_sk#290,ss_item_sk#291,ss_customer_sk#292,ss_cdemo_sk#293,ss_hdemo_sk#294,ss_addr_sk#295,ss_store_sk#296,ss_promo_sk#297,ss_ticket_number#298,ss_quantity#299,ss_wholesale_cost#300,ss_list_price#301,ss_sales_price#302,ss_ext_discount_amt#303,ss_ext_sales_price#304,ss_ext_wholesale_cost#305,ss_ext_list_price#306,ss_ext_tax#307,ss_coupon_amt#308,ss_net_paid#309,ss_net_paid_inc_tax#310,ss_net_profit#311] parquet
:  :  :           :     :     +- Project [hd_demo_sk#312]
:  :  :           :     :        +- Filter (((((hd_dep_count#315 = 4) AND (hd_vehicle_count#316 <= 6)) OR ((hd_dep_count#315 = 2) AND (hd_vehicle_count#316 <= 4))) OR ((hd_dep_count#315 = 0) AND (hd_vehicle_count#316 <= 2))) AND isnotnull(hd_demo_sk#312))
:  :  :           :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#312,hd_income_band_sk#313,hd_buy_potential#314,hd_dep_count#315,hd_vehicle_count#316] parquet
:  :  :           :     +- Project [t_time_sk#317]
:  :  :           :        +- Filter (((isnotnull(t_hour#320) AND isnotnull(t_minute#321)) AND ((t_hour#320 = 10) AND (t_minute#321 >= 30))) AND isnotnull(t_time_sk#317))
:  :  :           :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#317,t_time_id#318,t_time#319,t_hour#320,t_minute#321,t_second#322,t_am_pm#323,t_shift#324,t_sub_shift#325,t_meal_time#326] parquet
:  :  :           +- Project [s_store_sk#327]
:  :  :              +- Filter ((isnotnull(s_store_name#332) AND (s_store_name#332 = ese)) AND isnotnull(s_store_sk#327))
:  :  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#327,s_store_id#328,s_rec_start_date#329,s_rec_end_date#330,s_closed_date_sk#331,s_store_name#332,s_number_employees#333,s_floor_space#334,s_hours#335,s_manager#336,s_market_id#337,s_geography_class#338,s_market_desc#339,s_market_manager#340,s_division_id#341,s_division_name#342,s_company_id#343,s_company_name#344,s_street_number#345,s_street_name#346,s_street_type#347,s_suite_number#348,s_city#349,s_county#350,... 5 more fields] parquet
:  :  +- Aggregate [count(1) AS h11_to_11_30#5L]
:  :     +- Project
:  :        +- Join Inner, (ss_store_sk#363 = s_store_sk#394)
:  :           :- Project [ss_store_sk#363]
:  :           :  +- Join Inner, (ss_sold_time_sk#357 = t_time_sk#384)
:  :           :     :- Project [ss_sold_time_sk#357, ss_store_sk#363]
:  :           :     :  +- Join Inner, (ss_hdemo_sk#361 = hd_demo_sk#379)
:  :           :     :     :- Project [ss_sold_time_sk#357, ss_hdemo_sk#361, ss_store_sk#363]
:  :           :     :     :  +- Filter (isnotnull(ss_hdemo_sk#361) AND (isnotnull(ss_sold_time_sk#357) AND isnotnull(ss_store_sk#363)))
:  :           :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#356,ss_sold_time_sk#357,ss_item_sk#358,ss_customer_sk#359,ss_cdemo_sk#360,ss_hdemo_sk#361,ss_addr_sk#362,ss_store_sk#363,ss_promo_sk#364,ss_ticket_number#365,ss_quantity#366,ss_wholesale_cost#367,ss_list_price#368,ss_sales_price#369,ss_ext_discount_amt#370,ss_ext_sales_price#371,ss_ext_wholesale_cost#372,ss_ext_list_price#373,ss_ext_tax#374,ss_coupon_amt#375,ss_net_paid#376,ss_net_paid_inc_tax#377,ss_net_profit#378] parquet
:  :           :     :     +- Project [hd_demo_sk#379]
:  :           :     :        +- Filter (((((hd_dep_count#382 = 4) AND (hd_vehicle_count#383 <= 6)) OR ((hd_dep_count#382 = 2) AND (hd_vehicle_count#383 <= 4))) OR ((hd_dep_count#382 = 0) AND (hd_vehicle_count#383 <= 2))) AND isnotnull(hd_demo_sk#379))
:  :           :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#379,hd_income_band_sk#380,hd_buy_potential#381,hd_dep_count#382,hd_vehicle_count#383] parquet
:  :           :     +- Project [t_time_sk#384]
:  :           :        +- Filter (((isnotnull(t_hour#387) AND isnotnull(t_minute#388)) AND ((t_hour#387 = 11) AND (t_minute#388 < 30))) AND isnotnull(t_time_sk#384))
:  :           :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#384,t_time_id#385,t_time#386,t_hour#387,t_minute#388,t_second#389,t_am_pm#390,t_shift#391,t_sub_shift#392,t_meal_time#393] parquet
:  :           +- Project [s_store_sk#394]
:  :              +- Filter ((isnotnull(s_store_name#399) AND (s_store_name#399 = ese)) AND isnotnull(s_store_sk#394))
:  :                 +- Relation spark_catalog.tpcds.store[s_store_sk#394,s_store_id#395,s_rec_start_date#396,s_rec_end_date#397,s_closed_date_sk#398,s_store_name#399,s_number_employees#400,s_floor_space#401,s_hours#402,s_manager#403,s_market_id#404,s_geography_class#405,s_market_desc#406,s_market_manager#407,s_division_id#408,s_division_name#409,s_company_id#410,s_company_name#411,s_street_number#412,s_street_name#413,s_street_type#414,s_suite_number#415,s_city#416,s_county#417,... 5 more fields] parquet
:  +- Aggregate [count(1) AS h11_30_to_12#6L]
:     +- Project
:        +- Join Inner, (ss_store_sk#430 = s_store_sk#461)
:           :- Project [ss_store_sk#430]
:           :  +- Join Inner, (ss_sold_time_sk#424 = t_time_sk#451)
:           :     :- Project [ss_sold_time_sk#424, ss_store_sk#430]
:           :     :  +- Join Inner, (ss_hdemo_sk#428 = hd_demo_sk#446)
:           :     :     :- Project [ss_sold_time_sk#424, ss_hdemo_sk#428, ss_store_sk#430]
:           :     :     :  +- Filter (isnotnull(ss_hdemo_sk#428) AND (isnotnull(ss_sold_time_sk#424) AND isnotnull(ss_store_sk#430)))
:           :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#423,ss_sold_time_sk#424,ss_item_sk#425,ss_customer_sk#426,ss_cdemo_sk#427,ss_hdemo_sk#428,ss_addr_sk#429,ss_store_sk#430,ss_promo_sk#431,ss_ticket_number#432,ss_quantity#433,ss_wholesale_cost#434,ss_list_price#435,ss_sales_price#436,ss_ext_discount_amt#437,ss_ext_sales_price#438,ss_ext_wholesale_cost#439,ss_ext_list_price#440,ss_ext_tax#441,ss_coupon_amt#442,ss_net_paid#443,ss_net_paid_inc_tax#444,ss_net_profit#445] parquet
:           :     :     +- Project [hd_demo_sk#446]
:           :     :        +- Filter (((((hd_dep_count#449 = 4) AND (hd_vehicle_count#450 <= 6)) OR ((hd_dep_count#449 = 2) AND (hd_vehicle_count#450 <= 4))) OR ((hd_dep_count#449 = 0) AND (hd_vehicle_count#450 <= 2))) AND isnotnull(hd_demo_sk#446))
:           :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#446,hd_income_band_sk#447,hd_buy_potential#448,hd_dep_count#449,hd_vehicle_count#450] parquet
:           :     +- Project [t_time_sk#451]
:           :        +- Filter (((isnotnull(t_hour#454) AND isnotnull(t_minute#455)) AND ((t_hour#454 = 11) AND (t_minute#455 >= 30))) AND isnotnull(t_time_sk#451))
:           :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#451,t_time_id#452,t_time#453,t_hour#454,t_minute#455,t_second#456,t_am_pm#457,t_shift#458,t_sub_shift#459,t_meal_time#460] parquet
:           +- Project [s_store_sk#461]
:              +- Filter ((isnotnull(s_store_name#466) AND (s_store_name#466 = ese)) AND isnotnull(s_store_sk#461))
:                 +- Relation spark_catalog.tpcds.store[s_store_sk#461,s_store_id#462,s_rec_start_date#463,s_rec_end_date#464,s_closed_date_sk#465,s_store_name#466,s_number_employees#467,s_floor_space#468,s_hours#469,s_manager#470,s_market_id#471,s_geography_class#472,s_market_desc#473,s_market_manager#474,s_division_id#475,s_division_name#476,s_company_id#477,s_company_name#478,s_street_number#479,s_street_name#480,s_street_type#481,s_suite_number#482,s_city#483,s_county#484,... 5 more fields] parquet
+- Aggregate [count(1) AS h12_to_12_30#7L]
   +- Project
      +- Join Inner, (ss_store_sk#497 = s_store_sk#528)
         :- Project [ss_store_sk#497]
         :  +- Join Inner, (ss_sold_time_sk#491 = t_time_sk#518)
         :     :- Project [ss_sold_time_sk#491, ss_store_sk#497]
         :     :  +- Join Inner, (ss_hdemo_sk#495 = hd_demo_sk#513)
         :     :     :- Project [ss_sold_time_sk#491, ss_hdemo_sk#495, ss_store_sk#497]
         :     :     :  +- Filter (isnotnull(ss_hdemo_sk#495) AND (isnotnull(ss_sold_time_sk#491) AND isnotnull(ss_store_sk#497)))
         :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#490,ss_sold_time_sk#491,ss_item_sk#492,ss_customer_sk#493,ss_cdemo_sk#494,ss_hdemo_sk#495,ss_addr_sk#496,ss_store_sk#497,ss_promo_sk#498,ss_ticket_number#499,ss_quantity#500,ss_wholesale_cost#501,ss_list_price#502,ss_sales_price#503,ss_ext_discount_amt#504,ss_ext_sales_price#505,ss_ext_wholesale_cost#506,ss_ext_list_price#507,ss_ext_tax#508,ss_coupon_amt#509,ss_net_paid#510,ss_net_paid_inc_tax#511,ss_net_profit#512] parquet
         :     :     +- Project [hd_demo_sk#513]
         :     :        +- Filter (((((hd_dep_count#516 = 4) AND (hd_vehicle_count#517 <= 6)) OR ((hd_dep_count#516 = 2) AND (hd_vehicle_count#517 <= 4))) OR ((hd_dep_count#516 = 0) AND (hd_vehicle_count#517 <= 2))) AND isnotnull(hd_demo_sk#513))
         :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#513,hd_income_band_sk#514,hd_buy_potential#515,hd_dep_count#516,hd_vehicle_count#517] parquet
         :     +- Project [t_time_sk#518]
         :        +- Filter (((isnotnull(t_hour#521) AND isnotnull(t_minute#522)) AND ((t_hour#521 = 12) AND (t_minute#522 < 30))) AND isnotnull(t_time_sk#518))
         :           +- Relation spark_catalog.tpcds.time_dim[t_time_sk#518,t_time_id#519,t_time#520,t_hour#521,t_minute#522,t_second#523,t_am_pm#524,t_shift#525,t_sub_shift#526,t_meal_time#527] parquet
         +- Project [s_store_sk#528]
            +- Filter ((isnotnull(s_store_name#533) AND (s_store_name#533 = ese)) AND isnotnull(s_store_sk#528))
               +- Relation spark_catalog.tpcds.store[s_store_sk#528,s_store_id#529,s_rec_start_date#530,s_rec_end_date#531,s_closed_date_sk#532,s_store_name#533,s_number_employees#534,s_floor_space#535,s_hours#536,s_manager#537,s_market_id#538,s_geography_class#539,s_market_desc#540,s_market_manager#541,s_division_id#542,s_division_name#543,s_company_id#544,s_company_name#545,s_street_number#546,s_street_name#547,s_street_type#548,s_suite_number#549,s_city#550,s_county#551,... 5 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- BroadcastNestedLoopJoin BuildRight, Inner
   :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :  :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :  :  :  :  :- HashAggregate(keys=[], functions=[count(1)], output=[h8_30_to_9#0L])
   :  :  :  :  :  :  :  +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=622]
   :  :  :  :  :  :  :     +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#590L])
   :  :  :  :  :  :  :        +- Project
   :  :  :  :  :  :  :           +- BroadcastHashJoin [ss_store_sk#28], [s_store_sk#59], Inner, BuildRight, false
   :  :  :  :  :  :  :              :- Project [ss_store_sk#28]
   :  :  :  :  :  :  :              :  +- BroadcastHashJoin [ss_sold_time_sk#22], [t_time_sk#49], Inner, BuildRight, false
   :  :  :  :  :  :  :              :     :- Project [ss_sold_time_sk#22, ss_store_sk#28]
   :  :  :  :  :  :  :              :     :  +- BroadcastHashJoin [ss_hdemo_sk#26], [hd_demo_sk#44], Inner, BuildRight, false
   :  :  :  :  :  :  :              :     :     :- Filter ((isnotnull(ss_hdemo_sk#26) AND isnotnull(ss_sold_time_sk#22)) AND isnotnull(ss_store_sk#28))
   :  :  :  :  :  :  :              :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_time_sk#22,ss_hdemo_sk#26,ss_store_sk#28] Batched: true, DataFilters: [isnotnull(ss_hdemo_sk#26), isnotnull(ss_sold_time_sk#22), isnotnull(ss_store_sk#28)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
   :  :  :  :  :  :  :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=609]
   :  :  :  :  :  :  :              :     :        +- Project [hd_demo_sk#44]
   :  :  :  :  :  :  :              :     :           +- Filter (((((hd_dep_count#47 = 4) AND (hd_vehicle_count#48 <= 6)) OR ((hd_dep_count#47 = 2) AND (hd_vehicle_count#48 <= 4))) OR ((hd_dep_count#47 = 0) AND (hd_vehicle_count#48 <= 2))) AND isnotnull(hd_demo_sk#44))
   :  :  :  :  :  :  :              :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#44,hd_dep_count#47,hd_vehicle_count#48] Batched: true, DataFilters: [((((hd_dep_count#47 = 4) AND (hd_vehicle_count#48 <= 6)) OR ((hd_dep_count#47 = 2) AND (hd_vehic..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(hd_dep_count,4),LessThanOrEqual(hd_vehicle_count,6)),And(EqualTo(hd_dep_count,..., ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
   :  :  :  :  :  :  :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=613]
   :  :  :  :  :  :  :              :        +- Project [t_time_sk#49]
   :  :  :  :  :  :  :              :           +- Filter ((((isnotnull(t_hour#52) AND isnotnull(t_minute#53)) AND (t_hour#52 = 8)) AND (t_minute#53 >= 30)) AND isnotnull(t_time_sk#49))
   :  :  :  :  :  :  :              :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#49,t_hour#52,t_minute#53] Batched: true, DataFilters: [isnotnull(t_hour#52), isnotnull(t_minute#53), (t_hour#52 = 8), (t_minute#53 >= 30), isnotnull(t_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,8), GreaterThanOrEqual(t_minute,30), IsNo..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
   :  :  :  :  :  :  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=617]
   :  :  :  :  :  :  :                 +- Project [s_store_sk#59]
   :  :  :  :  :  :  :                    +- Filter ((isnotnull(s_store_name#64) AND (s_store_name#64 = ese)) AND isnotnull(s_store_sk#59))
   :  :  :  :  :  :  :                       +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#59,s_store_name#64] Batched: true, DataFilters: [isnotnull(s_store_name#64), (s_store_name#64 = ese), isnotnull(s_store_sk#59)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
   :  :  :  :  :  :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=640]
   :  :  :  :  :  :     +- HashAggregate(keys=[], functions=[count(1)], output=[h9_to_9_30#1L])
   :  :  :  :  :  :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=637]
   :  :  :  :  :  :           +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#592L])
   :  :  :  :  :  :              +- Project
   :  :  :  :  :  :                 +- BroadcastHashJoin [ss_store_sk#95], [s_store_sk#126], Inner, BuildRight, false
   :  :  :  :  :  :                    :- Project [ss_store_sk#95]
   :  :  :  :  :  :                    :  +- BroadcastHashJoin [ss_sold_time_sk#89], [t_time_sk#116], Inner, BuildRight, false
   :  :  :  :  :  :                    :     :- Project [ss_sold_time_sk#89, ss_store_sk#95]
   :  :  :  :  :  :                    :     :  +- BroadcastHashJoin [ss_hdemo_sk#93], [hd_demo_sk#111], Inner, BuildRight, false
   :  :  :  :  :  :                    :     :     :- Filter ((isnotnull(ss_hdemo_sk#93) AND isnotnull(ss_sold_time_sk#89)) AND isnotnull(ss_store_sk#95))
   :  :  :  :  :  :                    :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_time_sk#89,ss_hdemo_sk#93,ss_store_sk#95] Batched: true, DataFilters: [isnotnull(ss_hdemo_sk#93), isnotnull(ss_sold_time_sk#89), isnotnull(ss_store_sk#95)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
   :  :  :  :  :  :                    :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=624]
   :  :  :  :  :  :                    :     :        +- Project [hd_demo_sk#111]
   :  :  :  :  :  :                    :     :           +- Filter (((((hd_dep_count#114 = 4) AND (hd_vehicle_count#115 <= 6)) OR ((hd_dep_count#114 = 2) AND (hd_vehicle_count#115 <= 4))) OR ((hd_dep_count#114 = 0) AND (hd_vehicle_count#115 <= 2))) AND isnotnull(hd_demo_sk#111))
   :  :  :  :  :  :                    :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#111,hd_dep_count#114,hd_vehicle_count#115] Batched: true, DataFilters: [((((hd_dep_count#114 = 4) AND (hd_vehicle_count#115 <= 6)) OR ((hd_dep_count#114 = 2) AND (hd_ve..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(hd_dep_count,4),LessThanOrEqual(hd_vehicle_count,6)),And(EqualTo(hd_dep_count,..., ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
   :  :  :  :  :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=628]
   :  :  :  :  :  :                    :        +- Project [t_time_sk#116]
   :  :  :  :  :  :                    :           +- Filter ((((isnotnull(t_hour#119) AND isnotnull(t_minute#120)) AND (t_hour#119 = 9)) AND (t_minute#120 < 30)) AND isnotnull(t_time_sk#116))
   :  :  :  :  :  :                    :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#116,t_hour#119,t_minute#120] Batched: true, DataFilters: [isnotnull(t_hour#119), isnotnull(t_minute#120), (t_hour#119 = 9), (t_minute#120 < 30), isnotnull..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,9), LessThan(t_minute,30), IsNotNull(t_ti..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
   :  :  :  :  :  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=632]
   :  :  :  :  :  :                       +- Project [s_store_sk#126]
   :  :  :  :  :  :                          +- Filter ((isnotnull(s_store_name#131) AND (s_store_name#131 = ese)) AND isnotnull(s_store_sk#126))
   :  :  :  :  :  :                             +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#126,s_store_name#131] Batched: true, DataFilters: [isnotnull(s_store_name#131), (s_store_name#131 = ese), isnotnull(s_store_sk#126)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
   :  :  :  :  :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=658]
   :  :  :  :  :     +- HashAggregate(keys=[], functions=[count(1)], output=[h9_30_to_10#2L])
   :  :  :  :  :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=655]
   :  :  :  :  :           +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#594L])
   :  :  :  :  :              +- Project
   :  :  :  :  :                 +- BroadcastHashJoin [ss_store_sk#162], [s_store_sk#193], Inner, BuildRight, false
   :  :  :  :  :                    :- Project [ss_store_sk#162]
   :  :  :  :  :                    :  +- BroadcastHashJoin [ss_sold_time_sk#156], [t_time_sk#183], Inner, BuildRight, false
   :  :  :  :  :                    :     :- Project [ss_sold_time_sk#156, ss_store_sk#162]
   :  :  :  :  :                    :     :  +- BroadcastHashJoin [ss_hdemo_sk#160], [hd_demo_sk#178], Inner, BuildRight, false
   :  :  :  :  :                    :     :     :- Filter ((isnotnull(ss_hdemo_sk#160) AND isnotnull(ss_sold_time_sk#156)) AND isnotnull(ss_store_sk#162))
   :  :  :  :  :                    :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_time_sk#156,ss_hdemo_sk#160,ss_store_sk#162] Batched: true, DataFilters: [isnotnull(ss_hdemo_sk#160), isnotnull(ss_sold_time_sk#156), isnotnull(ss_store_sk#162)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
   :  :  :  :  :                    :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=642]
   :  :  :  :  :                    :     :        +- Project [hd_demo_sk#178]
   :  :  :  :  :                    :     :           +- Filter (((((hd_dep_count#181 = 4) AND (hd_vehicle_count#182 <= 6)) OR ((hd_dep_count#181 = 2) AND (hd_vehicle_count#182 <= 4))) OR ((hd_dep_count#181 = 0) AND (hd_vehicle_count#182 <= 2))) AND isnotnull(hd_demo_sk#178))
   :  :  :  :  :                    :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#178,hd_dep_count#181,hd_vehicle_count#182] Batched: true, DataFilters: [((((hd_dep_count#181 = 4) AND (hd_vehicle_count#182 <= 6)) OR ((hd_dep_count#181 = 2) AND (hd_ve..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(hd_dep_count,4),LessThanOrEqual(hd_vehicle_count,6)),And(EqualTo(hd_dep_count,..., ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
   :  :  :  :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=646]
   :  :  :  :  :                    :        +- Project [t_time_sk#183]
   :  :  :  :  :                    :           +- Filter ((((isnotnull(t_hour#186) AND isnotnull(t_minute#187)) AND (t_hour#186 = 9)) AND (t_minute#187 >= 30)) AND isnotnull(t_time_sk#183))
   :  :  :  :  :                    :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#183,t_hour#186,t_minute#187] Batched: true, DataFilters: [isnotnull(t_hour#186), isnotnull(t_minute#187), (t_hour#186 = 9), (t_minute#187 >= 30), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,9), GreaterThanOrEqual(t_minute,30), IsNo..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
   :  :  :  :  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=650]
   :  :  :  :  :                       +- Project [s_store_sk#193]
   :  :  :  :  :                          +- Filter ((isnotnull(s_store_name#198) AND (s_store_name#198 = ese)) AND isnotnull(s_store_sk#193))
   :  :  :  :  :                             +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#193,s_store_name#198] Batched: true, DataFilters: [isnotnull(s_store_name#198), (s_store_name#198 = ese), isnotnull(s_store_sk#193)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
   :  :  :  :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=676]
   :  :  :  :     +- HashAggregate(keys=[], functions=[count(1)], output=[h10_to_10_30#3L])
   :  :  :  :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=673]
   :  :  :  :           +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#596L])
   :  :  :  :              +- Project
   :  :  :  :                 +- BroadcastHashJoin [ss_store_sk#229], [s_store_sk#260], Inner, BuildRight, false
   :  :  :  :                    :- Project [ss_store_sk#229]
   :  :  :  :                    :  +- BroadcastHashJoin [ss_sold_time_sk#223], [t_time_sk#250], Inner, BuildRight, false
   :  :  :  :                    :     :- Project [ss_sold_time_sk#223, ss_store_sk#229]
   :  :  :  :                    :     :  +- BroadcastHashJoin [ss_hdemo_sk#227], [hd_demo_sk#245], Inner, BuildRight, false
   :  :  :  :                    :     :     :- Filter ((isnotnull(ss_hdemo_sk#227) AND isnotnull(ss_sold_time_sk#223)) AND isnotnull(ss_store_sk#229))
   :  :  :  :                    :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_time_sk#223,ss_hdemo_sk#227,ss_store_sk#229] Batched: true, DataFilters: [isnotnull(ss_hdemo_sk#227), isnotnull(ss_sold_time_sk#223), isnotnull(ss_store_sk#229)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
   :  :  :  :                    :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=660]
   :  :  :  :                    :     :        +- Project [hd_demo_sk#245]
   :  :  :  :                    :     :           +- Filter (((((hd_dep_count#248 = 4) AND (hd_vehicle_count#249 <= 6)) OR ((hd_dep_count#248 = 2) AND (hd_vehicle_count#249 <= 4))) OR ((hd_dep_count#248 = 0) AND (hd_vehicle_count#249 <= 2))) AND isnotnull(hd_demo_sk#245))
   :  :  :  :                    :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#245,hd_dep_count#248,hd_vehicle_count#249] Batched: true, DataFilters: [((((hd_dep_count#248 = 4) AND (hd_vehicle_count#249 <= 6)) OR ((hd_dep_count#248 = 2) AND (hd_ve..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(hd_dep_count,4),LessThanOrEqual(hd_vehicle_count,6)),And(EqualTo(hd_dep_count,..., ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
   :  :  :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=664]
   :  :  :  :                    :        +- Project [t_time_sk#250]
   :  :  :  :                    :           +- Filter ((((isnotnull(t_hour#253) AND isnotnull(t_minute#254)) AND (t_hour#253 = 10)) AND (t_minute#254 < 30)) AND isnotnull(t_time_sk#250))
   :  :  :  :                    :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#250,t_hour#253,t_minute#254] Batched: true, DataFilters: [isnotnull(t_hour#253), isnotnull(t_minute#254), (t_hour#253 = 10), (t_minute#254 < 30), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,10), LessThan(t_minute,30), IsNotNull(t_t..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
   :  :  :  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=668]
   :  :  :  :                       +- Project [s_store_sk#260]
   :  :  :  :                          +- Filter ((isnotnull(s_store_name#265) AND (s_store_name#265 = ese)) AND isnotnull(s_store_sk#260))
   :  :  :  :                             +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#260,s_store_name#265] Batched: true, DataFilters: [isnotnull(s_store_name#265), (s_store_name#265 = ese), isnotnull(s_store_sk#260)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
   :  :  :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=694]
   :  :  :     +- HashAggregate(keys=[], functions=[count(1)], output=[h10_30_to_11#4L])
   :  :  :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=691]
   :  :  :           +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#598L])
   :  :  :              +- Project
   :  :  :                 +- BroadcastHashJoin [ss_store_sk#296], [s_store_sk#327], Inner, BuildRight, false
   :  :  :                    :- Project [ss_store_sk#296]
   :  :  :                    :  +- BroadcastHashJoin [ss_sold_time_sk#290], [t_time_sk#317], Inner, BuildRight, false
   :  :  :                    :     :- Project [ss_sold_time_sk#290, ss_store_sk#296]
   :  :  :                    :     :  +- BroadcastHashJoin [ss_hdemo_sk#294], [hd_demo_sk#312], Inner, BuildRight, false
   :  :  :                    :     :     :- Filter ((isnotnull(ss_hdemo_sk#294) AND isnotnull(ss_sold_time_sk#290)) AND isnotnull(ss_store_sk#296))
   :  :  :                    :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_time_sk#290,ss_hdemo_sk#294,ss_store_sk#296] Batched: true, DataFilters: [isnotnull(ss_hdemo_sk#294), isnotnull(ss_sold_time_sk#290), isnotnull(ss_store_sk#296)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
   :  :  :                    :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=678]
   :  :  :                    :     :        +- Project [hd_demo_sk#312]
   :  :  :                    :     :           +- Filter (((((hd_dep_count#315 = 4) AND (hd_vehicle_count#316 <= 6)) OR ((hd_dep_count#315 = 2) AND (hd_vehicle_count#316 <= 4))) OR ((hd_dep_count#315 = 0) AND (hd_vehicle_count#316 <= 2))) AND isnotnull(hd_demo_sk#312))
   :  :  :                    :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#312,hd_dep_count#315,hd_vehicle_count#316] Batched: true, DataFilters: [((((hd_dep_count#315 = 4) AND (hd_vehicle_count#316 <= 6)) OR ((hd_dep_count#315 = 2) AND (hd_ve..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(hd_dep_count,4),LessThanOrEqual(hd_vehicle_count,6)),And(EqualTo(hd_dep_count,..., ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
   :  :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=682]
   :  :  :                    :        +- Project [t_time_sk#317]
   :  :  :                    :           +- Filter ((((isnotnull(t_hour#320) AND isnotnull(t_minute#321)) AND (t_hour#320 = 10)) AND (t_minute#321 >= 30)) AND isnotnull(t_time_sk#317))
   :  :  :                    :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#317,t_hour#320,t_minute#321] Batched: true, DataFilters: [isnotnull(t_hour#320), isnotnull(t_minute#321), (t_hour#320 = 10), (t_minute#321 >= 30), isnotnu..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,10), GreaterThanOrEqual(t_minute,30), IsN..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
   :  :  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=686]
   :  :  :                       +- Project [s_store_sk#327]
   :  :  :                          +- Filter ((isnotnull(s_store_name#332) AND (s_store_name#332 = ese)) AND isnotnull(s_store_sk#327))
   :  :  :                             +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#327,s_store_name#332] Batched: true, DataFilters: [isnotnull(s_store_name#332), (s_store_name#332 = ese), isnotnull(s_store_sk#327)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
   :  :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=712]
   :  :     +- HashAggregate(keys=[], functions=[count(1)], output=[h11_to_11_30#5L])
   :  :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=709]
   :  :           +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#600L])
   :  :              +- Project
   :  :                 +- BroadcastHashJoin [ss_store_sk#363], [s_store_sk#394], Inner, BuildRight, false
   :  :                    :- Project [ss_store_sk#363]
   :  :                    :  +- BroadcastHashJoin [ss_sold_time_sk#357], [t_time_sk#384], Inner, BuildRight, false
   :  :                    :     :- Project [ss_sold_time_sk#357, ss_store_sk#363]
   :  :                    :     :  +- BroadcastHashJoin [ss_hdemo_sk#361], [hd_demo_sk#379], Inner, BuildRight, false
   :  :                    :     :     :- Filter ((isnotnull(ss_hdemo_sk#361) AND isnotnull(ss_sold_time_sk#357)) AND isnotnull(ss_store_sk#363))
   :  :                    :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_time_sk#357,ss_hdemo_sk#361,ss_store_sk#363] Batched: true, DataFilters: [isnotnull(ss_hdemo_sk#361), isnotnull(ss_sold_time_sk#357), isnotnull(ss_store_sk#363)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
   :  :                    :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=696]
   :  :                    :     :        +- Project [hd_demo_sk#379]
   :  :                    :     :           +- Filter (((((hd_dep_count#382 = 4) AND (hd_vehicle_count#383 <= 6)) OR ((hd_dep_count#382 = 2) AND (hd_vehicle_count#383 <= 4))) OR ((hd_dep_count#382 = 0) AND (hd_vehicle_count#383 <= 2))) AND isnotnull(hd_demo_sk#379))
   :  :                    :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#379,hd_dep_count#382,hd_vehicle_count#383] Batched: true, DataFilters: [((((hd_dep_count#382 = 4) AND (hd_vehicle_count#383 <= 6)) OR ((hd_dep_count#382 = 2) AND (hd_ve..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(hd_dep_count,4),LessThanOrEqual(hd_vehicle_count,6)),And(EqualTo(hd_dep_count,..., ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
   :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=700]
   :  :                    :        +- Project [t_time_sk#384]
   :  :                    :           +- Filter ((((isnotnull(t_hour#387) AND isnotnull(t_minute#388)) AND (t_hour#387 = 11)) AND (t_minute#388 < 30)) AND isnotnull(t_time_sk#384))
   :  :                    :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#384,t_hour#387,t_minute#388] Batched: true, DataFilters: [isnotnull(t_hour#387), isnotnull(t_minute#388), (t_hour#387 = 11), (t_minute#388 < 30), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,11), LessThan(t_minute,30), IsNotNull(t_t..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
   :  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=704]
   :  :                       +- Project [s_store_sk#394]
   :  :                          +- Filter ((isnotnull(s_store_name#399) AND (s_store_name#399 = ese)) AND isnotnull(s_store_sk#394))
   :  :                             +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#394,s_store_name#399] Batched: true, DataFilters: [isnotnull(s_store_name#399), (s_store_name#399 = ese), isnotnull(s_store_sk#394)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
   :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=730]
   :     +- HashAggregate(keys=[], functions=[count(1)], output=[h11_30_to_12#6L])
   :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=727]
   :           +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#602L])
   :              +- Project
   :                 +- BroadcastHashJoin [ss_store_sk#430], [s_store_sk#461], Inner, BuildRight, false
   :                    :- Project [ss_store_sk#430]
   :                    :  +- BroadcastHashJoin [ss_sold_time_sk#424], [t_time_sk#451], Inner, BuildRight, false
   :                    :     :- Project [ss_sold_time_sk#424, ss_store_sk#430]
   :                    :     :  +- BroadcastHashJoin [ss_hdemo_sk#428], [hd_demo_sk#446], Inner, BuildRight, false
   :                    :     :     :- Filter ((isnotnull(ss_hdemo_sk#428) AND isnotnull(ss_sold_time_sk#424)) AND isnotnull(ss_store_sk#430))
   :                    :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_time_sk#424,ss_hdemo_sk#428,ss_store_sk#430] Batched: true, DataFilters: [isnotnull(ss_hdemo_sk#428), isnotnull(ss_sold_time_sk#424), isnotnull(ss_store_sk#430)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
   :                    :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=714]
   :                    :     :        +- Project [hd_demo_sk#446]
   :                    :     :           +- Filter (((((hd_dep_count#449 = 4) AND (hd_vehicle_count#450 <= 6)) OR ((hd_dep_count#449 = 2) AND (hd_vehicle_count#450 <= 4))) OR ((hd_dep_count#449 = 0) AND (hd_vehicle_count#450 <= 2))) AND isnotnull(hd_demo_sk#446))
   :                    :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#446,hd_dep_count#449,hd_vehicle_count#450] Batched: true, DataFilters: [((((hd_dep_count#449 = 4) AND (hd_vehicle_count#450 <= 6)) OR ((hd_dep_count#449 = 2) AND (hd_ve..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(hd_dep_count,4),LessThanOrEqual(hd_vehicle_count,6)),And(EqualTo(hd_dep_count,..., ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
   :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=718]
   :                    :        +- Project [t_time_sk#451]
   :                    :           +- Filter ((((isnotnull(t_hour#454) AND isnotnull(t_minute#455)) AND (t_hour#454 = 11)) AND (t_minute#455 >= 30)) AND isnotnull(t_time_sk#451))
   :                    :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#451,t_hour#454,t_minute#455] Batched: true, DataFilters: [isnotnull(t_hour#454), isnotnull(t_minute#455), (t_hour#454 = 11), (t_minute#455 >= 30), isnotnu..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,11), GreaterThanOrEqual(t_minute,30), IsN..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
   :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=722]
   :                       +- Project [s_store_sk#461]
   :                          +- Filter ((isnotnull(s_store_name#466) AND (s_store_name#466 = ese)) AND isnotnull(s_store_sk#461))
   :                             +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#461,s_store_name#466] Batched: true, DataFilters: [isnotnull(s_store_name#466), (s_store_name#466 = ese), isnotnull(s_store_sk#461)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
   +- BroadcastExchange IdentityBroadcastMode, [plan_id=748]
      +- HashAggregate(keys=[], functions=[count(1)], output=[h12_to_12_30#7L])
         +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=745]
            +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#604L])
               +- Project
                  +- BroadcastHashJoin [ss_store_sk#497], [s_store_sk#528], Inner, BuildRight, false
                     :- Project [ss_store_sk#497]
                     :  +- BroadcastHashJoin [ss_sold_time_sk#491], [t_time_sk#518], Inner, BuildRight, false
                     :     :- Project [ss_sold_time_sk#491, ss_store_sk#497]
                     :     :  +- BroadcastHashJoin [ss_hdemo_sk#495], [hd_demo_sk#513], Inner, BuildRight, false
                     :     :     :- Filter ((isnotnull(ss_hdemo_sk#495) AND isnotnull(ss_sold_time_sk#491)) AND isnotnull(ss_store_sk#497))
                     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_time_sk#491,ss_hdemo_sk#495,ss_store_sk#497] Batched: true, DataFilters: [isnotnull(ss_hdemo_sk#495), isnotnull(ss_sold_time_sk#491), isnotnull(ss_store_sk#497)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
                     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=732]
                     :     :        +- Project [hd_demo_sk#513]
                     :     :           +- Filter (((((hd_dep_count#516 = 4) AND (hd_vehicle_count#517 <= 6)) OR ((hd_dep_count#516 = 2) AND (hd_vehicle_count#517 <= 4))) OR ((hd_dep_count#516 = 0) AND (hd_vehicle_count#517 <= 2))) AND isnotnull(hd_demo_sk#513))
                     :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#513,hd_dep_count#516,hd_vehicle_count#517] Batched: true, DataFilters: [((((hd_dep_count#516 = 4) AND (hd_vehicle_count#517 <= 6)) OR ((hd_dep_count#516 = 2) AND (hd_ve..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(hd_dep_count,4),LessThanOrEqual(hd_vehicle_count,6)),And(EqualTo(hd_dep_count,..., ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=736]
                     :        +- Project [t_time_sk#518]
                     :           +- Filter ((((isnotnull(t_hour#521) AND isnotnull(t_minute#522)) AND (t_hour#521 = 12)) AND (t_minute#522 < 30)) AND isnotnull(t_time_sk#518))
                     :              +- FileScan parquet spark_catalog.tpcds.time_dim[t_time_sk#518,t_hour#521,t_minute#522] Batched: true, DataFilters: [isnotnull(t_hour#521), isnotnull(t_minute#522), (t_hour#521 = 12), (t_minute#522 < 30), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,12), LessThan(t_minute,30), IsNotNull(t_t..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=740]
                        +- Project [s_store_sk#528]
                           +- Filter ((isnotnull(s_store_name#533) AND (s_store_name#533 = ese)) AND isnotnull(s_store_sk#528))
                              +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#528,s_store_name#533] Batched: true, DataFilters: [isnotnull(s_store_name#533), (s_store_name#533 = ese), isnotnull(s_store_sk#528)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>

Time taken: 4.8 seconds, Fetched 1 row(s)
