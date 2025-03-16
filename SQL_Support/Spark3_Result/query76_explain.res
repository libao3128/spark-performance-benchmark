Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582487382
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['channel ASC NULLS FIRST, 'col_name ASC NULLS FIRST, 'd_year ASC NULLS FIRST, 'd_qoy ASC NULLS FIRST, 'i_category ASC NULLS FIRST], true
      +- 'Aggregate ['channel, 'col_name, 'd_year, 'd_qoy, 'i_category], ['channel, 'col_name, 'd_year, 'd_qoy, 'i_category, 'COUNT(1) AS sales_cnt#9, 'SUM('ext_sales_price) AS sales_amt#10]
         +- 'SubqueryAlias foo
            +- 'Union false, false
               :- 'Union false, false
               :  :- 'Project [store AS channel#0, ss_store_sk AS col_name#1, 'd_year, 'd_qoy, 'i_category, 'ss_ext_sales_price AS ext_sales_price#2]
               :  :  +- 'Filter ((isnull('ss_store_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND ('ss_item_sk = 'i_item_sk))
               :  :     +- 'Join Inner
               :  :        :- 'Join Inner
               :  :        :  :- 'UnresolvedRelation [store_sales], [], false
               :  :        :  +- 'UnresolvedRelation [item], [], false
               :  :        +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'Project [web AS channel#3, ws_ship_customer_sk AS col_name#4, 'd_year, 'd_qoy, 'i_category, 'ws_ext_sales_price AS ext_sales_price#5]
               :     +- 'Filter ((isnull('ws_ship_customer_sk) AND ('ws_sold_date_sk = 'd_date_sk)) AND ('ws_item_sk = 'i_item_sk))
               :        +- 'Join Inner
               :           :- 'Join Inner
               :           :  :- 'UnresolvedRelation [web_sales], [], false
               :           :  +- 'UnresolvedRelation [item], [], false
               :           +- 'UnresolvedRelation [date_dim], [], false
               +- 'Project [catalog AS channel#6, cs_ship_addr_sk AS col_name#7, 'd_year, 'd_qoy, 'i_category, 'cs_ext_sales_price AS ext_sales_price#8]
                  +- 'Filter ((isnull('cs_ship_addr_sk) AND ('cs_sold_date_sk = 'd_date_sk)) AND ('cs_item_sk = 'i_item_sk))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'UnresolvedRelation [catalog_sales], [], false
                        :  +- 'UnresolvedRelation [item], [], false
                        +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
channel: string, col_name: string, d_year: int, d_qoy: int, i_category: string, sales_cnt: bigint, sales_amt: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#0 ASC NULLS FIRST, col_name#1 ASC NULLS FIRST, d_year#68 ASC NULLS FIRST, d_qoy#72 ASC NULLS FIRST, i_category#52 ASC NULLS FIRST], true
      +- Aggregate [channel#0, col_name#1, d_year#68, d_qoy#72, i_category#52], [channel#0, col_name#1, d_year#68, d_qoy#72, i_category#52, count(1) AS sales_cnt#9L, sum(ext_sales_price#2) AS sales_amt#10]
         +- SubqueryAlias foo
            +- Union false, false
               :- Union false, false
               :  :- Project [store AS channel#0, ss_store_sk AS col_name#1, d_year#68, d_qoy#72, i_category#52, ss_ext_sales_price#32 AS ext_sales_price#2]
               :  :  +- Filter ((isnull(ss_store_sk#24) AND (ss_sold_date_sk#17 = d_date_sk#62)) AND (ss_item_sk#19 = i_item_sk#40))
               :  :     +- Join Inner
               :  :        :- Join Inner
               :  :        :  :- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :        :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#17,ss_sold_time_sk#18,ss_item_sk#19,ss_customer_sk#20,ss_cdemo_sk#21,ss_hdemo_sk#22,ss_addr_sk#23,ss_store_sk#24,ss_promo_sk#25,ss_ticket_number#26,ss_quantity#27,ss_wholesale_cost#28,ss_list_price#29,ss_sales_price#30,ss_ext_discount_amt#31,ss_ext_sales_price#32,ss_ext_wholesale_cost#33,ss_ext_list_price#34,ss_ext_tax#35,ss_coupon_amt#36,ss_net_paid#37,ss_net_paid_inc_tax#38,ss_net_profit#39] parquet
               :  :        :  +- SubqueryAlias spark_catalog.tpcds.item
               :  :        :     +- Relation spark_catalog.tpcds.item[i_item_sk#40,i_item_id#41,i_rec_start_date#42,i_rec_end_date#43,i_item_desc#44,i_current_price#45,i_wholesale_cost#46,i_brand_id#47,i_brand#48,i_class_id#49,i_class#50,i_category_id#51,i_category#52,i_manufact_id#53,i_manufact#54,i_size#55,i_formulation#56,i_color#57,i_units#58,i_container#59,i_manager_id#60,i_product_name#61] parquet
               :  :        +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#62,d_date_id#63,d_date#64,d_month_seq#65,d_week_seq#66,d_quarter_seq#67,d_year#68,d_dow#69,d_moy#70,d_dom#71,d_qoy#72,d_fy_year#73,d_fy_quarter_seq#74,d_fy_week_seq#75,d_day_name#76,d_quarter_name#77,d_holiday#78,d_weekend#79,d_following_holiday#80,d_first_dom#81,d_last_dom#82,d_same_day_ly#83,d_same_day_lq#84,d_current_day#85,... 4 more fields] parquet
               :  +- Project [web AS channel#3, ws_ship_customer_sk AS col_name#4, d_year#186, d_qoy#190, i_category#170, ws_ext_sales_price#113 AS ext_sales_price#5]
               :     +- Filter ((isnull(ws_ship_customer_sk#98) AND (ws_sold_date_sk#90 = d_date_sk#180)) AND (ws_item_sk#93 = i_item_sk#158))
               :        +- Join Inner
               :           :- Join Inner
               :           :  :- SubqueryAlias spark_catalog.tpcds.web_sales
               :           :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#90,ws_sold_time_sk#91,ws_ship_date_sk#92,ws_item_sk#93,ws_bill_customer_sk#94,ws_bill_cdemo_sk#95,ws_bill_hdemo_sk#96,ws_bill_addr_sk#97,ws_ship_customer_sk#98,ws_ship_cdemo_sk#99,ws_ship_hdemo_sk#100,ws_ship_addr_sk#101,ws_web_page_sk#102,ws_web_site_sk#103,ws_ship_mode_sk#104,ws_warehouse_sk#105,ws_promo_sk#106,ws_order_number#107,ws_quantity#108,ws_wholesale_cost#109,ws_list_price#110,ws_sales_price#111,ws_ext_discount_amt#112,ws_ext_sales_price#113,... 10 more fields] parquet
               :           :  +- SubqueryAlias spark_catalog.tpcds.item
               :           :     +- Relation spark_catalog.tpcds.item[i_item_sk#158,i_item_id#159,i_rec_start_date#160,i_rec_end_date#161,i_item_desc#162,i_current_price#163,i_wholesale_cost#164,i_brand_id#165,i_brand#166,i_class_id#167,i_class#168,i_category_id#169,i_category#170,i_manufact_id#171,i_manufact#172,i_size#173,i_formulation#174,i_color#175,i_units#176,i_container#177,i_manager_id#178,i_product_name#179] parquet
               :           +- SubqueryAlias spark_catalog.tpcds.date_dim
               :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#180,d_date_id#181,d_date#182,d_month_seq#183,d_week_seq#184,d_quarter_seq#185,d_year#186,d_dow#187,d_moy#188,d_dom#189,d_qoy#190,d_fy_year#191,d_fy_quarter_seq#192,d_fy_week_seq#193,d_day_name#194,d_quarter_name#195,d_holiday#196,d_weekend#197,d_following_holiday#198,d_first_dom#199,d_last_dom#200,d_same_day_ly#201,d_same_day_lq#202,d_current_day#203,... 4 more fields] parquet
               +- Project [catalog AS channel#6, cs_ship_addr_sk AS col_name#7, d_year#236, d_qoy#240, i_category#220, cs_ext_sales_price#147 AS ext_sales_price#8]
                  +- Filter ((isnull(cs_ship_addr_sk#134) AND (cs_sold_date_sk#124 = d_date_sk#230)) AND (cs_item_sk#139 = i_item_sk#208))
                     +- Join Inner
                        :- Join Inner
                        :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
                        :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#124,cs_sold_time_sk#125,cs_ship_date_sk#126,cs_bill_customer_sk#127,cs_bill_cdemo_sk#128,cs_bill_hdemo_sk#129,cs_bill_addr_sk#130,cs_ship_customer_sk#131,cs_ship_cdemo_sk#132,cs_ship_hdemo_sk#133,cs_ship_addr_sk#134,cs_call_center_sk#135,cs_catalog_page_sk#136,cs_ship_mode_sk#137,cs_warehouse_sk#138,cs_item_sk#139,cs_promo_sk#140,cs_order_number#141,cs_quantity#142,cs_wholesale_cost#143,cs_list_price#144,cs_sales_price#145,cs_ext_discount_amt#146,cs_ext_sales_price#147,... 10 more fields] parquet
                        :  +- SubqueryAlias spark_catalog.tpcds.item
                        :     +- Relation spark_catalog.tpcds.item[i_item_sk#208,i_item_id#209,i_rec_start_date#210,i_rec_end_date#211,i_item_desc#212,i_current_price#213,i_wholesale_cost#214,i_brand_id#215,i_brand#216,i_class_id#217,i_class#218,i_category_id#219,i_category#220,i_manufact_id#221,i_manufact#222,i_size#223,i_formulation#224,i_color#225,i_units#226,i_container#227,i_manager_id#228,i_product_name#229] parquet
                        +- SubqueryAlias spark_catalog.tpcds.date_dim
                           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#230,d_date_id#231,d_date#232,d_month_seq#233,d_week_seq#234,d_quarter_seq#235,d_year#236,d_dow#237,d_moy#238,d_dom#239,d_qoy#240,d_fy_year#241,d_fy_quarter_seq#242,d_fy_week_seq#243,d_day_name#244,d_quarter_name#245,d_holiday#246,d_weekend#247,d_following_holiday#248,d_first_dom#249,d_last_dom#250,d_same_day_ly#251,d_same_day_lq#252,d_current_day#253,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#0 ASC NULLS FIRST, col_name#1 ASC NULLS FIRST, d_year#68 ASC NULLS FIRST, d_qoy#72 ASC NULLS FIRST, i_category#52 ASC NULLS FIRST], true
      +- Aggregate [channel#0, col_name#1, d_year#68, d_qoy#72, i_category#52], [channel#0, col_name#1, d_year#68, d_qoy#72, i_category#52, count(1) AS sales_cnt#9L, sum(ext_sales_price#2) AS sales_amt#10]
         +- Union false, false
            :- Project [store AS channel#0, ss_store_sk AS col_name#1, d_year#68, d_qoy#72, i_category#52, ss_ext_sales_price#32 AS ext_sales_price#2]
            :  +- Join Inner, (ss_sold_date_sk#17 = d_date_sk#62)
            :     :- Project [ss_sold_date_sk#17, ss_ext_sales_price#32, i_category#52]
            :     :  +- Join Inner, (ss_item_sk#19 = i_item_sk#40)
            :     :     :- Project [ss_sold_date_sk#17, ss_item_sk#19, ss_ext_sales_price#32]
            :     :     :  +- Filter (isnull(ss_store_sk#24) AND (isnotnull(ss_item_sk#19) AND isnotnull(ss_sold_date_sk#17)))
            :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#17,ss_sold_time_sk#18,ss_item_sk#19,ss_customer_sk#20,ss_cdemo_sk#21,ss_hdemo_sk#22,ss_addr_sk#23,ss_store_sk#24,ss_promo_sk#25,ss_ticket_number#26,ss_quantity#27,ss_wholesale_cost#28,ss_list_price#29,ss_sales_price#30,ss_ext_discount_amt#31,ss_ext_sales_price#32,ss_ext_wholesale_cost#33,ss_ext_list_price#34,ss_ext_tax#35,ss_coupon_amt#36,ss_net_paid#37,ss_net_paid_inc_tax#38,ss_net_profit#39] parquet
            :     :     +- Project [i_item_sk#40, i_category#52]
            :     :        +- Filter isnotnull(i_item_sk#40)
            :     :           +- Relation spark_catalog.tpcds.item[i_item_sk#40,i_item_id#41,i_rec_start_date#42,i_rec_end_date#43,i_item_desc#44,i_current_price#45,i_wholesale_cost#46,i_brand_id#47,i_brand#48,i_class_id#49,i_class#50,i_category_id#51,i_category#52,i_manufact_id#53,i_manufact#54,i_size#55,i_formulation#56,i_color#57,i_units#58,i_container#59,i_manager_id#60,i_product_name#61] parquet
            :     +- Project [d_date_sk#62, d_year#68, d_qoy#72]
            :        +- Filter isnotnull(d_date_sk#62)
            :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#62,d_date_id#63,d_date#64,d_month_seq#65,d_week_seq#66,d_quarter_seq#67,d_year#68,d_dow#69,d_moy#70,d_dom#71,d_qoy#72,d_fy_year#73,d_fy_quarter_seq#74,d_fy_week_seq#75,d_day_name#76,d_quarter_name#77,d_holiday#78,d_weekend#79,d_following_holiday#80,d_first_dom#81,d_last_dom#82,d_same_day_ly#83,d_same_day_lq#84,d_current_day#85,... 4 more fields] parquet
            :- Project [web AS channel#3, ws_ship_customer_sk AS col_name#4, d_year#186, d_qoy#190, i_category#170, ws_ext_sales_price#113 AS ext_sales_price#5]
            :  +- Join Inner, (ws_sold_date_sk#90 = d_date_sk#180)
            :     :- Project [ws_sold_date_sk#90, ws_ext_sales_price#113, i_category#170]
            :     :  +- Join Inner, (ws_item_sk#93 = i_item_sk#158)
            :     :     :- Project [ws_sold_date_sk#90, ws_item_sk#93, ws_ext_sales_price#113]
            :     :     :  +- Filter (isnull(ws_ship_customer_sk#98) AND (isnotnull(ws_item_sk#93) AND isnotnull(ws_sold_date_sk#90)))
            :     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#90,ws_sold_time_sk#91,ws_ship_date_sk#92,ws_item_sk#93,ws_bill_customer_sk#94,ws_bill_cdemo_sk#95,ws_bill_hdemo_sk#96,ws_bill_addr_sk#97,ws_ship_customer_sk#98,ws_ship_cdemo_sk#99,ws_ship_hdemo_sk#100,ws_ship_addr_sk#101,ws_web_page_sk#102,ws_web_site_sk#103,ws_ship_mode_sk#104,ws_warehouse_sk#105,ws_promo_sk#106,ws_order_number#107,ws_quantity#108,ws_wholesale_cost#109,ws_list_price#110,ws_sales_price#111,ws_ext_discount_amt#112,ws_ext_sales_price#113,... 10 more fields] parquet
            :     :     +- Project [i_item_sk#158, i_category#170]
            :     :        +- Filter isnotnull(i_item_sk#158)
            :     :           +- Relation spark_catalog.tpcds.item[i_item_sk#158,i_item_id#159,i_rec_start_date#160,i_rec_end_date#161,i_item_desc#162,i_current_price#163,i_wholesale_cost#164,i_brand_id#165,i_brand#166,i_class_id#167,i_class#168,i_category_id#169,i_category#170,i_manufact_id#171,i_manufact#172,i_size#173,i_formulation#174,i_color#175,i_units#176,i_container#177,i_manager_id#178,i_product_name#179] parquet
            :     +- Project [d_date_sk#180, d_year#186, d_qoy#190]
            :        +- Filter isnotnull(d_date_sk#180)
            :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#180,d_date_id#181,d_date#182,d_month_seq#183,d_week_seq#184,d_quarter_seq#185,d_year#186,d_dow#187,d_moy#188,d_dom#189,d_qoy#190,d_fy_year#191,d_fy_quarter_seq#192,d_fy_week_seq#193,d_day_name#194,d_quarter_name#195,d_holiday#196,d_weekend#197,d_following_holiday#198,d_first_dom#199,d_last_dom#200,d_same_day_ly#201,d_same_day_lq#202,d_current_day#203,... 4 more fields] parquet
            +- Project [catalog AS channel#6, cs_ship_addr_sk AS col_name#7, d_year#236, d_qoy#240, i_category#220, cs_ext_sales_price#147 AS ext_sales_price#8]
               +- Join Inner, (cs_sold_date_sk#124 = d_date_sk#230)
                  :- Project [cs_sold_date_sk#124, cs_ext_sales_price#147, i_category#220]
                  :  +- Join Inner, (cs_item_sk#139 = i_item_sk#208)
                  :     :- Project [cs_sold_date_sk#124, cs_item_sk#139, cs_ext_sales_price#147]
                  :     :  +- Filter (isnull(cs_ship_addr_sk#134) AND (isnotnull(cs_item_sk#139) AND isnotnull(cs_sold_date_sk#124)))
                  :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#124,cs_sold_time_sk#125,cs_ship_date_sk#126,cs_bill_customer_sk#127,cs_bill_cdemo_sk#128,cs_bill_hdemo_sk#129,cs_bill_addr_sk#130,cs_ship_customer_sk#131,cs_ship_cdemo_sk#132,cs_ship_hdemo_sk#133,cs_ship_addr_sk#134,cs_call_center_sk#135,cs_catalog_page_sk#136,cs_ship_mode_sk#137,cs_warehouse_sk#138,cs_item_sk#139,cs_promo_sk#140,cs_order_number#141,cs_quantity#142,cs_wholesale_cost#143,cs_list_price#144,cs_sales_price#145,cs_ext_discount_amt#146,cs_ext_sales_price#147,... 10 more fields] parquet
                  :     +- Project [i_item_sk#208, i_category#220]
                  :        +- Filter isnotnull(i_item_sk#208)
                  :           +- Relation spark_catalog.tpcds.item[i_item_sk#208,i_item_id#209,i_rec_start_date#210,i_rec_end_date#211,i_item_desc#212,i_current_price#213,i_wholesale_cost#214,i_brand_id#215,i_brand#216,i_class_id#217,i_class#218,i_category_id#219,i_category#220,i_manufact_id#221,i_manufact#222,i_size#223,i_formulation#224,i_color#225,i_units#226,i_container#227,i_manager_id#228,i_product_name#229] parquet
                  +- Project [d_date_sk#230, d_year#236, d_qoy#240]
                     +- Filter isnotnull(d_date_sk#230)
                        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#230,d_date_id#231,d_date#232,d_month_seq#233,d_week_seq#234,d_quarter_seq#235,d_year#236,d_dow#237,d_moy#238,d_dom#239,d_qoy#240,d_fy_year#241,d_fy_quarter_seq#242,d_fy_week_seq#243,d_day_name#244,d_quarter_name#245,d_holiday#246,d_weekend#247,d_following_holiday#248,d_first_dom#249,d_last_dom#250,d_same_day_ly#251,d_same_day_lq#252,d_current_day#253,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[channel#0 ASC NULLS FIRST,col_name#1 ASC NULLS FIRST,d_year#68 ASC NULLS FIRST,d_qoy#72 ASC NULLS FIRST,i_category#52 ASC NULLS FIRST], output=[channel#0,col_name#1,d_year#68,d_qoy#72,i_category#52,sales_cnt#9L,sales_amt#10])
   +- HashAggregate(keys=[channel#0, col_name#1, d_year#68, d_qoy#72, i_category#52], functions=[count(1), sum(ext_sales_price#2)], output=[channel#0, col_name#1, d_year#68, d_qoy#72, i_category#52, sales_cnt#9L, sales_amt#10])
      +- Exchange hashpartitioning(channel#0, col_name#1, d_year#68, d_qoy#72, i_category#52, 200), ENSURE_REQUIREMENTS, [plan_id=180]
         +- HashAggregate(keys=[channel#0, col_name#1, d_year#68, d_qoy#72, i_category#52], functions=[partial_count(1), partial_sum(ext_sales_price#2)], output=[channel#0, col_name#1, d_year#68, d_qoy#72, i_category#52, count#270L, sum#271])
            +- Union
               :- Project [store AS channel#0, ss_store_sk AS col_name#1, d_year#68, d_qoy#72, i_category#52, ss_ext_sales_price#32 AS ext_sales_price#2]
               :  +- BroadcastHashJoin [ss_sold_date_sk#17], [d_date_sk#62], Inner, BuildRight, false
               :     :- Project [ss_sold_date_sk#17, ss_ext_sales_price#32, i_category#52]
               :     :  +- BroadcastHashJoin [ss_item_sk#19], [i_item_sk#40], Inner, BuildRight, false
               :     :     :- Project [ss_sold_date_sk#17, ss_item_sk#19, ss_ext_sales_price#32]
               :     :     :  +- Filter ((isnull(ss_store_sk#24) AND isnotnull(ss_item_sk#19)) AND isnotnull(ss_sold_date_sk#17))
               :     :     :     +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#17,ss_item_sk#19,ss_store_sk#24,ss_ext_sales_price#32] Batched: true, DataFilters: [isnull(ss_store_sk#24), isnotnull(ss_item_sk#19), isnotnull(ss_sold_date_sk#17)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNull(ss_store_sk), IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_ext_sales_price:double>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=156]
               :     :        +- Filter isnotnull(i_item_sk#40)
               :     :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#40,i_category#52] Batched: true, DataFilters: [isnotnull(i_item_sk#40)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_category:string>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=160]
               :        +- Filter isnotnull(d_date_sk#62)
               :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#62,d_year#68,d_qoy#72] Batched: true, DataFilters: [isnotnull(d_date_sk#62)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
               :- Project [web AS channel#3, ws_ship_customer_sk AS col_name#4, d_year#186, d_qoy#190, i_category#170, ws_ext_sales_price#113 AS ext_sales_price#5]
               :  +- BroadcastHashJoin [ws_sold_date_sk#90], [d_date_sk#180], Inner, BuildRight, false
               :     :- Project [ws_sold_date_sk#90, ws_ext_sales_price#113, i_category#170]
               :     :  +- BroadcastHashJoin [ws_item_sk#93], [i_item_sk#158], Inner, BuildRight, false
               :     :     :- Project [ws_sold_date_sk#90, ws_item_sk#93, ws_ext_sales_price#113]
               :     :     :  +- Filter ((isnull(ws_ship_customer_sk#98) AND isnotnull(ws_item_sk#93)) AND isnotnull(ws_sold_date_sk#90))
               :     :     :     +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#90,ws_item_sk#93,ws_ship_customer_sk#98,ws_ext_sales_price#113] Batched: true, DataFilters: [isnull(ws_ship_customer_sk#98), isnotnull(ws_item_sk#93), isnotnull(ws_sold_date_sk#90)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNull(ws_ship_customer_sk), IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_ship_customer_sk:int,ws_ext_sales_price:double>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=163]
               :     :        +- Filter isnotnull(i_item_sk#158)
               :     :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#158,i_category#170] Batched: true, DataFilters: [isnotnull(i_item_sk#158)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_category:string>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=167]
               :        +- Filter isnotnull(d_date_sk#180)
               :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#180,d_year#186,d_qoy#190] Batched: true, DataFilters: [isnotnull(d_date_sk#180)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
               +- Project [catalog AS channel#6, cs_ship_addr_sk AS col_name#7, d_year#236, d_qoy#240, i_category#220, cs_ext_sales_price#147 AS ext_sales_price#8]
                  +- BroadcastHashJoin [cs_sold_date_sk#124], [d_date_sk#230], Inner, BuildRight, false
                     :- Project [cs_sold_date_sk#124, cs_ext_sales_price#147, i_category#220]
                     :  +- BroadcastHashJoin [cs_item_sk#139], [i_item_sk#208], Inner, BuildRight, false
                     :     :- Project [cs_sold_date_sk#124, cs_item_sk#139, cs_ext_sales_price#147]
                     :     :  +- Filter ((isnull(cs_ship_addr_sk#134) AND isnotnull(cs_item_sk#139)) AND isnotnull(cs_sold_date_sk#124))
                     :     :     +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#124,cs_ship_addr_sk#134,cs_item_sk#139,cs_ext_sales_price#147] Batched: true, DataFilters: [isnull(cs_ship_addr_sk#134), isnotnull(cs_item_sk#139), isnotnull(cs_sold_date_sk#124)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNull(cs_ship_addr_sk), IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_ship_addr_sk:int,cs_item_sk:int,cs_ext_sales_price:double>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=170]
                     :        +- Filter isnotnull(i_item_sk#208)
                     :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#208,i_category#220] Batched: true, DataFilters: [isnotnull(i_item_sk#208)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_category:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=174]
                        +- Filter isnotnull(d_date_sk#230)
                           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#230,d_year#236,d_qoy#240] Batched: true, DataFilters: [isnotnull(d_date_sk#230)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>

Time taken: 2.656 seconds, Fetched 1 row(s)
