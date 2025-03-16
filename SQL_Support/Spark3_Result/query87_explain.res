Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582949927
== Parsed Logical Plan ==
'Project [unresolvedalias('count(1), None)]
+- 'SubqueryAlias cool_cust
   +- 'Except false
      :- 'Except false
      :  :- 'Distinct
      :  :  +- 'Project ['c_last_name, 'c_first_name, 'd_date]
      :  :     +- 'Filter ((('store_sales.ss_sold_date_sk = 'date_dim.d_date_sk) AND ('store_sales.ss_customer_sk = 'customer.c_customer_sk)) AND (('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11))))
      :  :        +- 'Join Inner
      :  :           :- 'Join Inner
      :  :           :  :- 'UnresolvedRelation [store_sales], [], false
      :  :           :  +- 'UnresolvedRelation [date_dim], [], false
      :  :           +- 'UnresolvedRelation [customer], [], false
      :  +- 'Distinct
      :     +- 'Project ['c_last_name, 'c_first_name, 'd_date]
      :        +- 'Filter ((('catalog_sales.cs_sold_date_sk = 'date_dim.d_date_sk) AND ('catalog_sales.cs_bill_customer_sk = 'customer.c_customer_sk)) AND (('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11))))
      :           +- 'Join Inner
      :              :- 'Join Inner
      :              :  :- 'UnresolvedRelation [catalog_sales], [], false
      :              :  +- 'UnresolvedRelation [date_dim], [], false
      :              +- 'UnresolvedRelation [customer], [], false
      +- 'Distinct
         +- 'Project ['c_last_name, 'c_first_name, 'd_date]
            +- 'Filter ((('web_sales.ws_sold_date_sk = 'date_dim.d_date_sk) AND ('web_sales.ws_bill_customer_sk = 'customer.c_customer_sk)) AND (('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11))))
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'UnresolvedRelation [web_sales], [], false
                  :  +- 'UnresolvedRelation [date_dim], [], false
                  +- 'UnresolvedRelation [customer], [], false

== Analyzed Logical Plan ==
count(1): bigint
Aggregate [count(1) AS count(1)#235L]
+- SubqueryAlias cool_cust
   +- Except false
      :- Except false
      :  :- Distinct
      :  :  +- Project [c_last_name#66, c_first_name#65, d_date#31]
      :  :     +- Filter (((ss_sold_date_sk#6 = d_date_sk#29) AND (ss_customer_sk#9 = c_customer_sk#57)) AND ((d_month_seq#32 >= 1200) AND (d_month_seq#32 <= (1200 + 11))))
      :  :        +- Join Inner
      :  :           :- Join Inner
      :  :           :  :- SubqueryAlias spark_catalog.tpcds.store_sales
      :  :           :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#6,ss_sold_time_sk#7,ss_item_sk#8,ss_customer_sk#9,ss_cdemo_sk#10,ss_hdemo_sk#11,ss_addr_sk#12,ss_store_sk#13,ss_promo_sk#14,ss_ticket_number#15,ss_quantity#16,ss_wholesale_cost#17,ss_list_price#18,ss_sales_price#19,ss_ext_discount_amt#20,ss_ext_sales_price#21,ss_ext_wholesale_cost#22,ss_ext_list_price#23,ss_ext_tax#24,ss_coupon_amt#25,ss_net_paid#26,ss_net_paid_inc_tax#27,ss_net_profit#28] parquet
      :  :           :  +- SubqueryAlias spark_catalog.tpcds.date_dim
      :  :           :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#29,d_date_id#30,d_date#31,d_month_seq#32,d_week_seq#33,d_quarter_seq#34,d_year#35,d_dow#36,d_moy#37,d_dom#38,d_qoy#39,d_fy_year#40,d_fy_quarter_seq#41,d_fy_week_seq#42,d_day_name#43,d_quarter_name#44,d_holiday#45,d_weekend#46,d_following_holiday#47,d_first_dom#48,d_last_dom#49,d_same_day_ly#50,d_same_day_lq#51,d_current_day#52,... 4 more fields] parquet
      :  :           +- SubqueryAlias spark_catalog.tpcds.customer
      :  :              +- Relation spark_catalog.tpcds.customer[c_customer_sk#57,c_customer_id#58,c_current_cdemo_sk#59,c_current_hdemo_sk#60,c_current_addr_sk#61,c_first_shipto_date_sk#62,c_first_sales_date_sk#63,c_salutation#64,c_first_name#65,c_last_name#66,c_preferred_cust_flag#67,c_birth_day#68,c_birth_month#69,c_birth_year#70,c_birth_country#71,c_login#72,c_email_address#73,c_last_review_date#74] parquet
      :  +- Distinct
      :     +- Project [c_last_name#180, c_first_name#179, d_date#145]
      :        +- Filter (((cs_sold_date_sk#75 = d_date_sk#143) AND (cs_bill_customer_sk#78 = c_customer_sk#171)) AND ((d_month_seq#146 >= 1200) AND (d_month_seq#146 <= (1200 + 11))))
      :           +- Join Inner
      :              :- Join Inner
      :              :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
      :              :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#75,cs_sold_time_sk#76,cs_ship_date_sk#77,cs_bill_customer_sk#78,cs_bill_cdemo_sk#79,cs_bill_hdemo_sk#80,cs_bill_addr_sk#81,cs_ship_customer_sk#82,cs_ship_cdemo_sk#83,cs_ship_hdemo_sk#84,cs_ship_addr_sk#85,cs_call_center_sk#86,cs_catalog_page_sk#87,cs_ship_mode_sk#88,cs_warehouse_sk#89,cs_item_sk#90,cs_promo_sk#91,cs_order_number#92,cs_quantity#93,cs_wholesale_cost#94,cs_list_price#95,cs_sales_price#96,cs_ext_discount_amt#97,cs_ext_sales_price#98,... 10 more fields] parquet
      :              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
      :              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#143,d_date_id#144,d_date#145,d_month_seq#146,d_week_seq#147,d_quarter_seq#148,d_year#149,d_dow#150,d_moy#151,d_dom#152,d_qoy#153,d_fy_year#154,d_fy_quarter_seq#155,d_fy_week_seq#156,d_day_name#157,d_quarter_name#158,d_holiday#159,d_weekend#160,d_following_holiday#161,d_first_dom#162,d_last_dom#163,d_same_day_ly#164,d_same_day_lq#165,d_current_day#166,... 4 more fields] parquet
      :              +- SubqueryAlias spark_catalog.tpcds.customer
      :                 +- Relation spark_catalog.tpcds.customer[c_customer_sk#171,c_customer_id#172,c_current_cdemo_sk#173,c_current_hdemo_sk#174,c_current_addr_sk#175,c_first_shipto_date_sk#176,c_first_sales_date_sk#177,c_salutation#178,c_first_name#179,c_last_name#180,c_preferred_cust_flag#181,c_birth_day#182,c_birth_month#183,c_birth_year#184,c_birth_country#185,c_login#186,c_email_address#187,c_last_review_date#188] parquet
      +- Distinct
         +- Project [c_last_name#226, c_first_name#225, d_date#191]
            +- Filter (((ws_sold_date_sk#109 = d_date_sk#189) AND (ws_bill_customer_sk#113 = c_customer_sk#217)) AND ((d_month_seq#192 >= 1200) AND (d_month_seq#192 <= (1200 + 11))))
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias spark_catalog.tpcds.web_sales
                  :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#109,ws_sold_time_sk#110,ws_ship_date_sk#111,ws_item_sk#112,ws_bill_customer_sk#113,ws_bill_cdemo_sk#114,ws_bill_hdemo_sk#115,ws_bill_addr_sk#116,ws_ship_customer_sk#117,ws_ship_cdemo_sk#118,ws_ship_hdemo_sk#119,ws_ship_addr_sk#120,ws_web_page_sk#121,ws_web_site_sk#122,ws_ship_mode_sk#123,ws_warehouse_sk#124,ws_promo_sk#125,ws_order_number#126,ws_quantity#127,ws_wholesale_cost#128,ws_list_price#129,ws_sales_price#130,ws_ext_discount_amt#131,ws_ext_sales_price#132,... 10 more fields] parquet
                  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
                  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#189,d_date_id#190,d_date#191,d_month_seq#192,d_week_seq#193,d_quarter_seq#194,d_year#195,d_dow#196,d_moy#197,d_dom#198,d_qoy#199,d_fy_year#200,d_fy_quarter_seq#201,d_fy_week_seq#202,d_day_name#203,d_quarter_name#204,d_holiday#205,d_weekend#206,d_following_holiday#207,d_first_dom#208,d_last_dom#209,d_same_day_ly#210,d_same_day_lq#211,d_current_day#212,... 4 more fields] parquet
                  +- SubqueryAlias spark_catalog.tpcds.customer
                     +- Relation spark_catalog.tpcds.customer[c_customer_sk#217,c_customer_id#218,c_current_cdemo_sk#219,c_current_hdemo_sk#220,c_current_addr_sk#221,c_first_shipto_date_sk#222,c_first_sales_date_sk#223,c_salutation#224,c_first_name#225,c_last_name#226,c_preferred_cust_flag#227,c_birth_day#228,c_birth_month#229,c_birth_year#230,c_birth_country#231,c_login#232,c_email_address#233,c_last_review_date#234] parquet

== Optimized Logical Plan ==
Aggregate [count(1) AS count(1)#235L]
+- Project
   +- Join LeftAnti, (((c_last_name#66 <=> c_last_name#226) AND (c_first_name#65 <=> c_first_name#225)) AND (d_date#31 <=> d_date#191))
      :- Join LeftAnti, (((c_last_name#66 <=> c_last_name#180) AND (c_first_name#65 <=> c_first_name#179)) AND (d_date#31 <=> d_date#145))
      :  :- Aggregate [c_last_name#66, c_first_name#65, d_date#31], [c_last_name#66, c_first_name#65, d_date#31]
      :  :  +- Project [c_last_name#66, c_first_name#65, d_date#31]
      :  :     +- Join Inner, (ss_customer_sk#9 = c_customer_sk#57)
      :  :        :- Project [ss_customer_sk#9, d_date#31]
      :  :        :  +- Join Inner, (ss_sold_date_sk#6 = d_date_sk#29)
      :  :        :     :- Project [ss_sold_date_sk#6, ss_customer_sk#9]
      :  :        :     :  +- Filter (isnotnull(ss_sold_date_sk#6) AND isnotnull(ss_customer_sk#9))
      :  :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#6,ss_sold_time_sk#7,ss_item_sk#8,ss_customer_sk#9,ss_cdemo_sk#10,ss_hdemo_sk#11,ss_addr_sk#12,ss_store_sk#13,ss_promo_sk#14,ss_ticket_number#15,ss_quantity#16,ss_wholesale_cost#17,ss_list_price#18,ss_sales_price#19,ss_ext_discount_amt#20,ss_ext_sales_price#21,ss_ext_wholesale_cost#22,ss_ext_list_price#23,ss_ext_tax#24,ss_coupon_amt#25,ss_net_paid#26,ss_net_paid_inc_tax#27,ss_net_profit#28] parquet
      :  :        :     +- Project [d_date_sk#29, d_date#31]
      :  :        :        +- Filter ((isnotnull(d_month_seq#32) AND ((d_month_seq#32 >= 1200) AND (d_month_seq#32 <= 1211))) AND isnotnull(d_date_sk#29))
      :  :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#29,d_date_id#30,d_date#31,d_month_seq#32,d_week_seq#33,d_quarter_seq#34,d_year#35,d_dow#36,d_moy#37,d_dom#38,d_qoy#39,d_fy_year#40,d_fy_quarter_seq#41,d_fy_week_seq#42,d_day_name#43,d_quarter_name#44,d_holiday#45,d_weekend#46,d_following_holiday#47,d_first_dom#48,d_last_dom#49,d_same_day_ly#50,d_same_day_lq#51,d_current_day#52,... 4 more fields] parquet
      :  :        +- Project [c_customer_sk#57, c_first_name#65, c_last_name#66]
      :  :           +- Filter isnotnull(c_customer_sk#57)
      :  :              +- Relation spark_catalog.tpcds.customer[c_customer_sk#57,c_customer_id#58,c_current_cdemo_sk#59,c_current_hdemo_sk#60,c_current_addr_sk#61,c_first_shipto_date_sk#62,c_first_sales_date_sk#63,c_salutation#64,c_first_name#65,c_last_name#66,c_preferred_cust_flag#67,c_birth_day#68,c_birth_month#69,c_birth_year#70,c_birth_country#71,c_login#72,c_email_address#73,c_last_review_date#74] parquet
      :  +- Aggregate [c_last_name#180, c_first_name#179, d_date#145], [c_last_name#180, c_first_name#179, d_date#145]
      :     +- Project [c_last_name#180, c_first_name#179, d_date#145]
      :        +- Join Inner, (cs_bill_customer_sk#78 = c_customer_sk#171)
      :           :- Project [cs_bill_customer_sk#78, d_date#145]
      :           :  +- Join Inner, (cs_sold_date_sk#75 = d_date_sk#143)
      :           :     :- Project [cs_sold_date_sk#75, cs_bill_customer_sk#78]
      :           :     :  +- Filter (isnotnull(cs_sold_date_sk#75) AND isnotnull(cs_bill_customer_sk#78))
      :           :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#75,cs_sold_time_sk#76,cs_ship_date_sk#77,cs_bill_customer_sk#78,cs_bill_cdemo_sk#79,cs_bill_hdemo_sk#80,cs_bill_addr_sk#81,cs_ship_customer_sk#82,cs_ship_cdemo_sk#83,cs_ship_hdemo_sk#84,cs_ship_addr_sk#85,cs_call_center_sk#86,cs_catalog_page_sk#87,cs_ship_mode_sk#88,cs_warehouse_sk#89,cs_item_sk#90,cs_promo_sk#91,cs_order_number#92,cs_quantity#93,cs_wholesale_cost#94,cs_list_price#95,cs_sales_price#96,cs_ext_discount_amt#97,cs_ext_sales_price#98,... 10 more fields] parquet
      :           :     +- Project [d_date_sk#143, d_date#145]
      :           :        +- Filter ((isnotnull(d_month_seq#146) AND ((d_month_seq#146 >= 1200) AND (d_month_seq#146 <= 1211))) AND isnotnull(d_date_sk#143))
      :           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#143,d_date_id#144,d_date#145,d_month_seq#146,d_week_seq#147,d_quarter_seq#148,d_year#149,d_dow#150,d_moy#151,d_dom#152,d_qoy#153,d_fy_year#154,d_fy_quarter_seq#155,d_fy_week_seq#156,d_day_name#157,d_quarter_name#158,d_holiday#159,d_weekend#160,d_following_holiday#161,d_first_dom#162,d_last_dom#163,d_same_day_ly#164,d_same_day_lq#165,d_current_day#166,... 4 more fields] parquet
      :           +- Project [c_customer_sk#171, c_first_name#179, c_last_name#180]
      :              +- Filter isnotnull(c_customer_sk#171)
      :                 +- Relation spark_catalog.tpcds.customer[c_customer_sk#171,c_customer_id#172,c_current_cdemo_sk#173,c_current_hdemo_sk#174,c_current_addr_sk#175,c_first_shipto_date_sk#176,c_first_sales_date_sk#177,c_salutation#178,c_first_name#179,c_last_name#180,c_preferred_cust_flag#181,c_birth_day#182,c_birth_month#183,c_birth_year#184,c_birth_country#185,c_login#186,c_email_address#187,c_last_review_date#188] parquet
      +- Aggregate [c_last_name#226, c_first_name#225, d_date#191], [c_last_name#226, c_first_name#225, d_date#191]
         +- Project [c_last_name#226, c_first_name#225, d_date#191]
            +- Join Inner, (ws_bill_customer_sk#113 = c_customer_sk#217)
               :- Project [ws_bill_customer_sk#113, d_date#191]
               :  +- Join Inner, (ws_sold_date_sk#109 = d_date_sk#189)
               :     :- Project [ws_sold_date_sk#109, ws_bill_customer_sk#113]
               :     :  +- Filter (isnotnull(ws_sold_date_sk#109) AND isnotnull(ws_bill_customer_sk#113))
               :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#109,ws_sold_time_sk#110,ws_ship_date_sk#111,ws_item_sk#112,ws_bill_customer_sk#113,ws_bill_cdemo_sk#114,ws_bill_hdemo_sk#115,ws_bill_addr_sk#116,ws_ship_customer_sk#117,ws_ship_cdemo_sk#118,ws_ship_hdemo_sk#119,ws_ship_addr_sk#120,ws_web_page_sk#121,ws_web_site_sk#122,ws_ship_mode_sk#123,ws_warehouse_sk#124,ws_promo_sk#125,ws_order_number#126,ws_quantity#127,ws_wholesale_cost#128,ws_list_price#129,ws_sales_price#130,ws_ext_discount_amt#131,ws_ext_sales_price#132,... 10 more fields] parquet
               :     +- Project [d_date_sk#189, d_date#191]
               :        +- Filter ((isnotnull(d_month_seq#192) AND ((d_month_seq#192 >= 1200) AND (d_month_seq#192 <= 1211))) AND isnotnull(d_date_sk#189))
               :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#189,d_date_id#190,d_date#191,d_month_seq#192,d_week_seq#193,d_quarter_seq#194,d_year#195,d_dow#196,d_moy#197,d_dom#198,d_qoy#199,d_fy_year#200,d_fy_quarter_seq#201,d_fy_week_seq#202,d_day_name#203,d_quarter_name#204,d_holiday#205,d_weekend#206,d_following_holiday#207,d_first_dom#208,d_last_dom#209,d_same_day_ly#210,d_same_day_lq#211,d_current_day#212,... 4 more fields] parquet
               +- Project [c_customer_sk#217, c_first_name#225, c_last_name#226]
                  +- Filter isnotnull(c_customer_sk#217)
                     +- Relation spark_catalog.tpcds.customer[c_customer_sk#217,c_customer_id#218,c_current_cdemo_sk#219,c_current_hdemo_sk#220,c_current_addr_sk#221,c_first_shipto_date_sk#222,c_first_sales_date_sk#223,c_salutation#224,c_first_name#225,c_last_name#226,c_preferred_cust_flag#227,c_birth_day#228,c_birth_month#229,c_birth_year#230,c_birth_country#231,c_login#232,c_email_address#233,c_last_review_date#234] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count(1)#235L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=237]
      +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#246L])
         +- Project
            +- SortMergeJoin [coalesce(c_last_name#66, ), isnull(c_last_name#66), coalesce(c_first_name#65, ), isnull(c_first_name#65), coalesce(d_date#31, ), isnull(d_date#31)], [coalesce(c_last_name#226, ), isnull(c_last_name#226), coalesce(c_first_name#225, ), isnull(c_first_name#225), coalesce(d_date#191, ), isnull(d_date#191)], LeftAnti
               :- SortMergeJoin [coalesce(c_last_name#66, ), isnull(c_last_name#66), coalesce(c_first_name#65, ), isnull(c_first_name#65), coalesce(d_date#31, ), isnull(d_date#31)], [coalesce(c_last_name#180, ), isnull(c_last_name#180), coalesce(c_first_name#179, ), isnull(c_first_name#179), coalesce(d_date#145, ), isnull(d_date#145)], LeftAnti
               :  :- Sort [coalesce(c_last_name#66, ) ASC NULLS FIRST, isnull(c_last_name#66) ASC NULLS FIRST, coalesce(c_first_name#65, ) ASC NULLS FIRST, isnull(c_first_name#65) ASC NULLS FIRST, coalesce(d_date#31, ) ASC NULLS FIRST, isnull(d_date#31) ASC NULLS FIRST], false, 0
               :  :  +- Exchange hashpartitioning(coalesce(c_last_name#66, ), isnull(c_last_name#66), coalesce(c_first_name#65, ), isnull(c_first_name#65), coalesce(d_date#31, ), isnull(d_date#31), 200), ENSURE_REQUIREMENTS, [plan_id=213]
               :  :     +- HashAggregate(keys=[c_last_name#66, c_first_name#65, d_date#31], functions=[], output=[c_last_name#66, c_first_name#65, d_date#31])
               :  :        +- Exchange hashpartitioning(c_last_name#66, c_first_name#65, d_date#31, 200), ENSURE_REQUIREMENTS, [plan_id=198]
               :  :           +- HashAggregate(keys=[c_last_name#66, c_first_name#65, d_date#31], functions=[], output=[c_last_name#66, c_first_name#65, d_date#31])
               :  :              +- Project [c_last_name#66, c_first_name#65, d_date#31]
               :  :                 +- BroadcastHashJoin [ss_customer_sk#9], [c_customer_sk#57], Inner, BuildRight, false
               :  :                    :- Project [ss_customer_sk#9, d_date#31]
               :  :                    :  +- BroadcastHashJoin [ss_sold_date_sk#6], [d_date_sk#29], Inner, BuildRight, false
               :  :                    :     :- Filter (isnotnull(ss_sold_date_sk#6) AND isnotnull(ss_customer_sk#9))
               :  :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#6,ss_customer_sk#9] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#6), isnotnull(ss_customer_sk#9)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_customer_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int>
               :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=189]
               :  :                    :        +- Project [d_date_sk#29, d_date#31]
               :  :                    :           +- Filter (((isnotnull(d_month_seq#32) AND (d_month_seq#32 >= 1200)) AND (d_month_seq#32 <= 1211)) AND isnotnull(d_date_sk#29))
               :  :                    :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#29,d_date#31,d_month_seq#32] Batched: true, DataFilters: [isnotnull(d_month_seq#32), (d_month_seq#32 >= 1200), (d_month_seq#32 <= 1211), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_date:string,d_month_seq:int>
               :  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=193]
               :  :                       +- Filter isnotnull(c_customer_sk#57)
               :  :                          +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#57,c_first_name#65,c_last_name#66] Batched: true, DataFilters: [isnotnull(c_customer_sk#57)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_first_name:string,c_last_name:string>
               :  +- Sort [coalesce(c_last_name#180, ) ASC NULLS FIRST, isnull(c_last_name#180) ASC NULLS FIRST, coalesce(c_first_name#179, ) ASC NULLS FIRST, isnull(c_first_name#179) ASC NULLS FIRST, coalesce(d_date#145, ) ASC NULLS FIRST, isnull(d_date#145) ASC NULLS FIRST], false, 0
               :     +- Exchange hashpartitioning(coalesce(c_last_name#180, ), isnull(c_last_name#180), coalesce(c_first_name#179, ), isnull(c_first_name#179), coalesce(d_date#145, ), isnull(d_date#145), 200), ENSURE_REQUIREMENTS, [plan_id=214]
               :        +- HashAggregate(keys=[c_last_name#180, c_first_name#179, d_date#145], functions=[], output=[c_last_name#180, c_first_name#179, d_date#145])
               :           +- Exchange hashpartitioning(c_last_name#180, c_first_name#179, d_date#145, 200), ENSURE_REQUIREMENTS, [plan_id=209]
               :              +- HashAggregate(keys=[c_last_name#180, c_first_name#179, d_date#145], functions=[], output=[c_last_name#180, c_first_name#179, d_date#145])
               :                 +- Project [c_last_name#180, c_first_name#179, d_date#145]
               :                    +- BroadcastHashJoin [cs_bill_customer_sk#78], [c_customer_sk#171], Inner, BuildRight, false
               :                       :- Project [cs_bill_customer_sk#78, d_date#145]
               :                       :  +- BroadcastHashJoin [cs_sold_date_sk#75], [d_date_sk#143], Inner, BuildRight, false
               :                       :     :- Filter (isnotnull(cs_sold_date_sk#75) AND isnotnull(cs_bill_customer_sk#78))
               :                       :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#75,cs_bill_customer_sk#78] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#75), isnotnull(cs_bill_customer_sk#78)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_bill_customer_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int>
               :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=200]
               :                       :        +- Project [d_date_sk#143, d_date#145]
               :                       :           +- Filter (((isnotnull(d_month_seq#146) AND (d_month_seq#146 >= 1200)) AND (d_month_seq#146 <= 1211)) AND isnotnull(d_date_sk#143))
               :                       :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#143,d_date#145,d_month_seq#146] Batched: true, DataFilters: [isnotnull(d_month_seq#146), (d_month_seq#146 >= 1200), (d_month_seq#146 <= 1211), isnotnull(d_da..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_date:string,d_month_seq:int>
               :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=204]
               :                          +- Filter isnotnull(c_customer_sk#171)
               :                             +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#171,c_first_name#179,c_last_name#180] Batched: true, DataFilters: [isnotnull(c_customer_sk#171)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_first_name:string,c_last_name:string>
               +- Sort [coalesce(c_last_name#226, ) ASC NULLS FIRST, isnull(c_last_name#226) ASC NULLS FIRST, coalesce(c_first_name#225, ) ASC NULLS FIRST, isnull(c_first_name#225) ASC NULLS FIRST, coalesce(d_date#191, ) ASC NULLS FIRST, isnull(d_date#191) ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(coalesce(c_last_name#226, ), isnull(c_last_name#226), coalesce(c_first_name#225, ), isnull(c_first_name#225), coalesce(d_date#191, ), isnull(d_date#191), 200), ENSURE_REQUIREMENTS, [plan_id=231]
                     +- HashAggregate(keys=[c_last_name#226, c_first_name#225, d_date#191], functions=[], output=[c_last_name#226, c_first_name#225, d_date#191])
                        +- Exchange hashpartitioning(c_last_name#226, c_first_name#225, d_date#191, 200), ENSURE_REQUIREMENTS, [plan_id=227]
                           +- HashAggregate(keys=[c_last_name#226, c_first_name#225, d_date#191], functions=[], output=[c_last_name#226, c_first_name#225, d_date#191])
                              +- Project [c_last_name#226, c_first_name#225, d_date#191]
                                 +- BroadcastHashJoin [ws_bill_customer_sk#113], [c_customer_sk#217], Inner, BuildRight, false
                                    :- Project [ws_bill_customer_sk#113, d_date#191]
                                    :  +- BroadcastHashJoin [ws_sold_date_sk#109], [d_date_sk#189], Inner, BuildRight, false
                                    :     :- Filter (isnotnull(ws_sold_date_sk#109) AND isnotnull(ws_bill_customer_sk#113))
                                    :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#109,ws_bill_customer_sk#113] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#109), isnotnull(ws_bill_customer_sk#113)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_customer_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int>
                                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=218]
                                    :        +- Project [d_date_sk#189, d_date#191]
                                    :           +- Filter (((isnotnull(d_month_seq#192) AND (d_month_seq#192 >= 1200)) AND (d_month_seq#192 <= 1211)) AND isnotnull(d_date_sk#189))
                                    :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#189,d_date#191,d_month_seq#192] Batched: true, DataFilters: [isnotnull(d_month_seq#192), (d_month_seq#192 >= 1200), (d_month_seq#192 <= 1211), isnotnull(d_da..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_date:string,d_month_seq:int>
                                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=222]
                                       +- Filter isnotnull(c_customer_sk#217)
                                          +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#217,c_first_name#225,c_last_name#226] Batched: true, DataFilters: [isnotnull(c_customer_sk#217)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_first_name:string,c_last_name:string>

Time taken: 3.024 seconds, Fetched 1 row(s)
