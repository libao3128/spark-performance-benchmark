Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582054717
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['s_store_name ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST], true
      +- 'Project ['s_store_name, 'i_item_desc, 'sc.revenue, 'i_current_price, 'i_wholesale_cost, 'i_brand]
         +- 'Filter ((('sb.ss_store_sk = 'sc.ss_store_sk) AND ('sc.revenue <= (0.1 * 'sb.ave))) AND (('s_store_sk = 'sc.ss_store_sk) AND ('i_item_sk = 'sc.ss_item_sk)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation [store], [], false
               :  :  +- 'UnresolvedRelation [item], [], false
               :  +- 'SubqueryAlias sb
               :     +- 'Aggregate ['ss_store_sk], ['ss_store_sk, 'avg('revenue) AS ave#1]
               :        +- 'SubqueryAlias sa
               :           +- 'Aggregate ['ss_store_sk, 'ss_item_sk], ['ss_store_sk, 'ss_item_sk, 'sum('ss_sales_price) AS revenue#0]
               :              +- 'Filter (('ss_sold_date_sk = 'd_date_sk) AND (('d_month_seq >= 1176) AND ('d_month_seq <= (1176 + 11))))
               :                 +- 'Join Inner
               :                    :- 'UnresolvedRelation [store_sales], [], false
               :                    +- 'UnresolvedRelation [date_dim], [], false
               +- 'SubqueryAlias sc
                  +- 'Aggregate ['ss_store_sk, 'ss_item_sk], ['ss_store_sk, 'ss_item_sk, 'sum('ss_sales_price) AS revenue#2]
                     +- 'Filter (('ss_sold_date_sk = 'd_date_sk) AND (('d_month_seq >= 1176) AND ('d_month_seq <= (1176 + 11))))
                        +- 'Join Inner
                           :- 'UnresolvedRelation [store_sales], [], false
                           +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
s_store_name: string, i_item_desc: string, revenue: double, i_current_price: double, i_wholesale_cost: double, i_brand: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#13 ASC NULLS FIRST, i_item_desc#41 ASC NULLS FIRST], true
      +- Project [s_store_name#13, i_item_desc#41, revenue#2, i_current_price#42, i_wholesale_cost#43, i_brand#45]
         +- Filter (((ss_store_sk#66 = ss_store_sk#117) AND (revenue#2 <= (cast(0.1 as double) * ave#1))) AND ((s_store_sk#8 = ss_store_sk#117) AND (i_item_sk#37 = ss_item_sk#112)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias spark_catalog.tpcds.store
               :  :  :  +- Relation spark_catalog.tpcds.store[s_store_sk#8,s_store_id#9,s_rec_start_date#10,s_rec_end_date#11,s_closed_date_sk#12,s_store_name#13,s_number_employees#14,s_floor_space#15,s_hours#16,s_manager#17,s_market_id#18,s_geography_class#19,s_market_desc#20,s_market_manager#21,s_division_id#22,s_division_name#23,s_company_id#24,s_company_name#25,s_street_number#26,s_street_name#27,s_street_type#28,s_suite_number#29,s_city#30,s_county#31,... 5 more fields] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.item
               :  :     +- Relation spark_catalog.tpcds.item[i_item_sk#37,i_item_id#38,i_rec_start_date#39,i_rec_end_date#40,i_item_desc#41,i_current_price#42,i_wholesale_cost#43,i_brand_id#44,i_brand#45,i_class_id#46,i_class#47,i_category_id#48,i_category#49,i_manufact_id#50,i_manufact#51,i_size#52,i_formulation#53,i_color#54,i_units#55,i_container#56,i_manager_id#57,i_product_name#58] parquet
               :  +- SubqueryAlias sb
               :     +- Aggregate [ss_store_sk#66], [ss_store_sk#66, avg(revenue#0) AS ave#1]
               :        +- SubqueryAlias sa
               :           +- Aggregate [ss_store_sk#66, ss_item_sk#61], [ss_store_sk#66, ss_item_sk#61, sum(ss_sales_price#72) AS revenue#0]
               :              +- Filter ((ss_sold_date_sk#59 = d_date_sk#82) AND ((d_month_seq#85 >= 1176) AND (d_month_seq#85 <= (1176 + 11))))
               :                 +- Join Inner
               :                    :- SubqueryAlias spark_catalog.tpcds.store_sales
               :                    :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#59,ss_sold_time_sk#60,ss_item_sk#61,ss_customer_sk#62,ss_cdemo_sk#63,ss_hdemo_sk#64,ss_addr_sk#65,ss_store_sk#66,ss_promo_sk#67,ss_ticket_number#68,ss_quantity#69,ss_wholesale_cost#70,ss_list_price#71,ss_sales_price#72,ss_ext_discount_amt#73,ss_ext_sales_price#74,ss_ext_wholesale_cost#75,ss_ext_list_price#76,ss_ext_tax#77,ss_coupon_amt#78,ss_net_paid#79,ss_net_paid_inc_tax#80,ss_net_profit#81] parquet
               :                    +- SubqueryAlias spark_catalog.tpcds.date_dim
               :                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#82,d_date_id#83,d_date#84,d_month_seq#85,d_week_seq#86,d_quarter_seq#87,d_year#88,d_dow#89,d_moy#90,d_dom#91,d_qoy#92,d_fy_year#93,d_fy_quarter_seq#94,d_fy_week_seq#95,d_day_name#96,d_quarter_name#97,d_holiday#98,d_weekend#99,d_following_holiday#100,d_first_dom#101,d_last_dom#102,d_same_day_ly#103,d_same_day_lq#104,d_current_day#105,... 4 more fields] parquet
               +- SubqueryAlias sc
                  +- Aggregate [ss_store_sk#117, ss_item_sk#112], [ss_store_sk#117, ss_item_sk#112, sum(ss_sales_price#123) AS revenue#2]
                     +- Filter ((ss_sold_date_sk#110 = d_date_sk#133) AND ((d_month_seq#136 >= 1176) AND (d_month_seq#136 <= (1176 + 11))))
                        +- Join Inner
                           :- SubqueryAlias spark_catalog.tpcds.store_sales
                           :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#110,ss_sold_time_sk#111,ss_item_sk#112,ss_customer_sk#113,ss_cdemo_sk#114,ss_hdemo_sk#115,ss_addr_sk#116,ss_store_sk#117,ss_promo_sk#118,ss_ticket_number#119,ss_quantity#120,ss_wholesale_cost#121,ss_list_price#122,ss_sales_price#123,ss_ext_discount_amt#124,ss_ext_sales_price#125,ss_ext_wholesale_cost#126,ss_ext_list_price#127,ss_ext_tax#128,ss_coupon_amt#129,ss_net_paid#130,ss_net_paid_inc_tax#131,ss_net_profit#132] parquet
                           +- SubqueryAlias spark_catalog.tpcds.date_dim
                              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#133,d_date_id#134,d_date#135,d_month_seq#136,d_week_seq#137,d_quarter_seq#138,d_year#139,d_dow#140,d_moy#141,d_dom#142,d_qoy#143,d_fy_year#144,d_fy_quarter_seq#145,d_fy_week_seq#146,d_day_name#147,d_quarter_name#148,d_holiday#149,d_weekend#150,d_following_holiday#151,d_first_dom#152,d_last_dom#153,d_same_day_ly#154,d_same_day_lq#155,d_current_day#156,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#13 ASC NULLS FIRST, i_item_desc#41 ASC NULLS FIRST], true
      +- Project [s_store_name#13, i_item_desc#41, revenue#2, i_current_price#42, i_wholesale_cost#43, i_brand#45]
         +- Join Inner, ((ss_store_sk#66 = ss_store_sk#117) AND (revenue#2 <= (0.1 * ave#1)))
            :- Project [s_store_name#13, ss_store_sk#117, revenue#2, i_item_desc#41, i_current_price#42, i_wholesale_cost#43, i_brand#45]
            :  +- Join Inner, (i_item_sk#37 = ss_item_sk#112)
            :     :- Project [s_store_name#13, ss_store_sk#117, ss_item_sk#112, revenue#2]
            :     :  +- Join Inner, (s_store_sk#8 = ss_store_sk#117)
            :     :     :- Project [s_store_sk#8, s_store_name#13]
            :     :     :  +- Filter isnotnull(s_store_sk#8)
            :     :     :     +- Relation spark_catalog.tpcds.store[s_store_sk#8,s_store_id#9,s_rec_start_date#10,s_rec_end_date#11,s_closed_date_sk#12,s_store_name#13,s_number_employees#14,s_floor_space#15,s_hours#16,s_manager#17,s_market_id#18,s_geography_class#19,s_market_desc#20,s_market_manager#21,s_division_id#22,s_division_name#23,s_company_id#24,s_company_name#25,s_street_number#26,s_street_name#27,s_street_type#28,s_suite_number#29,s_city#30,s_county#31,... 5 more fields] parquet
            :     :     +- Filter isnotnull(revenue#2)
            :     :        +- Aggregate [ss_store_sk#117, ss_item_sk#112], [ss_store_sk#117, ss_item_sk#112, sum(ss_sales_price#123) AS revenue#2]
            :     :           +- Project [ss_item_sk#112, ss_store_sk#117, ss_sales_price#123]
            :     :              +- Join Inner, (ss_sold_date_sk#110 = d_date_sk#133)
            :     :                 :- Project [ss_sold_date_sk#110, ss_item_sk#112, ss_store_sk#117, ss_sales_price#123]
            :     :                 :  +- Filter ((isnotnull(ss_sold_date_sk#110) AND isnotnull(ss_store_sk#117)) AND isnotnull(ss_item_sk#112))
            :     :                 :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#110,ss_sold_time_sk#111,ss_item_sk#112,ss_customer_sk#113,ss_cdemo_sk#114,ss_hdemo_sk#115,ss_addr_sk#116,ss_store_sk#117,ss_promo_sk#118,ss_ticket_number#119,ss_quantity#120,ss_wholesale_cost#121,ss_list_price#122,ss_sales_price#123,ss_ext_discount_amt#124,ss_ext_sales_price#125,ss_ext_wholesale_cost#126,ss_ext_list_price#127,ss_ext_tax#128,ss_coupon_amt#129,ss_net_paid#130,ss_net_paid_inc_tax#131,ss_net_profit#132] parquet
            :     :                 +- Project [d_date_sk#133]
            :     :                    +- Filter ((isnotnull(d_month_seq#136) AND ((d_month_seq#136 >= 1176) AND (d_month_seq#136 <= 1187))) AND isnotnull(d_date_sk#133))
            :     :                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#133,d_date_id#134,d_date#135,d_month_seq#136,d_week_seq#137,d_quarter_seq#138,d_year#139,d_dow#140,d_moy#141,d_dom#142,d_qoy#143,d_fy_year#144,d_fy_quarter_seq#145,d_fy_week_seq#146,d_day_name#147,d_quarter_name#148,d_holiday#149,d_weekend#150,d_following_holiday#151,d_first_dom#152,d_last_dom#153,d_same_day_ly#154,d_same_day_lq#155,d_current_day#156,... 4 more fields] parquet
            :     +- Project [i_item_sk#37, i_item_desc#41, i_current_price#42, i_wholesale_cost#43, i_brand#45]
            :        +- Filter isnotnull(i_item_sk#37)
            :           +- Relation spark_catalog.tpcds.item[i_item_sk#37,i_item_id#38,i_rec_start_date#39,i_rec_end_date#40,i_item_desc#41,i_current_price#42,i_wholesale_cost#43,i_brand_id#44,i_brand#45,i_class_id#46,i_class#47,i_category_id#48,i_category#49,i_manufact_id#50,i_manufact#51,i_size#52,i_formulation#53,i_color#54,i_units#55,i_container#56,i_manager_id#57,i_product_name#58] parquet
            +- Filter isnotnull(ave#1)
               +- Aggregate [ss_store_sk#66], [ss_store_sk#66, avg(revenue#0) AS ave#1]
                  +- Aggregate [ss_store_sk#66, ss_item_sk#61], [ss_store_sk#66, sum(ss_sales_price#72) AS revenue#0]
                     +- Project [ss_item_sk#61, ss_store_sk#66, ss_sales_price#72]
                        +- Join Inner, (ss_sold_date_sk#59 = d_date_sk#82)
                           :- Project [ss_sold_date_sk#59, ss_item_sk#61, ss_store_sk#66, ss_sales_price#72]
                           :  +- Filter (isnotnull(ss_sold_date_sk#59) AND isnotnull(ss_store_sk#66))
                           :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#59,ss_sold_time_sk#60,ss_item_sk#61,ss_customer_sk#62,ss_cdemo_sk#63,ss_hdemo_sk#64,ss_addr_sk#65,ss_store_sk#66,ss_promo_sk#67,ss_ticket_number#68,ss_quantity#69,ss_wholesale_cost#70,ss_list_price#71,ss_sales_price#72,ss_ext_discount_amt#73,ss_ext_sales_price#74,ss_ext_wholesale_cost#75,ss_ext_list_price#76,ss_ext_tax#77,ss_coupon_amt#78,ss_net_paid#79,ss_net_paid_inc_tax#80,ss_net_profit#81] parquet
                           +- Project [d_date_sk#82]
                              +- Filter ((isnotnull(d_month_seq#85) AND ((d_month_seq#85 >= 1176) AND (d_month_seq#85 <= 1187))) AND isnotnull(d_date_sk#82))
                                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#82,d_date_id#83,d_date#84,d_month_seq#85,d_week_seq#86,d_quarter_seq#87,d_year#88,d_dow#89,d_moy#90,d_dom#91,d_qoy#92,d_fy_year#93,d_fy_quarter_seq#94,d_fy_week_seq#95,d_day_name#96,d_quarter_name#97,d_holiday#98,d_weekend#99,d_following_holiday#100,d_first_dom#101,d_last_dom#102,d_same_day_ly#103,d_same_day_lq#104,d_current_day#105,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[s_store_name#13 ASC NULLS FIRST,i_item_desc#41 ASC NULLS FIRST], output=[s_store_name#13,i_item_desc#41,revenue#2,i_current_price#42,i_wholesale_cost#43,i_brand#45])
   +- Project [s_store_name#13, i_item_desc#41, revenue#2, i_current_price#42, i_wholesale_cost#43, i_brand#45]
      +- SortMergeJoin [ss_store_sk#117], [ss_store_sk#66], Inner, (revenue#2 <= (0.1 * ave#1))
         :- Sort [ss_store_sk#117 ASC NULLS FIRST], false, 0
         :  +- Exchange hashpartitioning(ss_store_sk#117, 200), ENSURE_REQUIREMENTS, [plan_id=175]
         :     +- Project [s_store_name#13, ss_store_sk#117, revenue#2, i_item_desc#41, i_current_price#42, i_wholesale_cost#43, i_brand#45]
         :        +- BroadcastHashJoin [ss_item_sk#112], [i_item_sk#37], Inner, BuildRight, false
         :           :- Project [s_store_name#13, ss_store_sk#117, ss_item_sk#112, revenue#2]
         :           :  +- BroadcastHashJoin [s_store_sk#8], [ss_store_sk#117], Inner, BuildLeft, false
         :           :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=154]
         :           :     :  +- Filter isnotnull(s_store_sk#8)
         :           :     :     +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#8,s_store_name#13] Batched: true, DataFilters: [isnotnull(s_store_sk#8)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
         :           :     +- Filter isnotnull(revenue#2)
         :           :        +- HashAggregate(keys=[ss_store_sk#117, ss_item_sk#112], functions=[sum(ss_sales_price#123)], output=[ss_store_sk#117, ss_item_sk#112, revenue#2])
         :           :           +- Exchange hashpartitioning(ss_store_sk#117, ss_item_sk#112, 200), ENSURE_REQUIREMENTS, [plan_id=150]
         :           :              +- HashAggregate(keys=[ss_store_sk#117, ss_item_sk#112], functions=[partial_sum(ss_sales_price#123)], output=[ss_store_sk#117, ss_item_sk#112, sum#171])
         :           :                 +- Project [ss_item_sk#112, ss_store_sk#117, ss_sales_price#123]
         :           :                    +- BroadcastHashJoin [ss_sold_date_sk#110], [d_date_sk#133], Inner, BuildRight, false
         :           :                       :- Filter ((isnotnull(ss_sold_date_sk#110) AND isnotnull(ss_store_sk#117)) AND isnotnull(ss_item_sk#112))
         :           :                       :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#110,ss_item_sk#112,ss_store_sk#117,ss_sales_price#123] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#110), isnotnull(ss_store_sk#117), isnotnull(ss_item_sk#112)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
         :           :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=145]
         :           :                          +- Project [d_date_sk#133]
         :           :                             +- Filter (((isnotnull(d_month_seq#136) AND (d_month_seq#136 >= 1176)) AND (d_month_seq#136 <= 1187)) AND isnotnull(d_date_sk#133))
         :           :                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#133,d_month_seq#136] Batched: true, DataFilters: [isnotnull(d_month_seq#136), (d_month_seq#136 >= 1176), (d_month_seq#136 <= 1187), isnotnull(d_da..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1176), LessThanOrEqual(d_month_seq,1187),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
         :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=158]
         :              +- Filter isnotnull(i_item_sk#37)
         :                 +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#37,i_item_desc#41,i_current_price#42,i_wholesale_cost#43,i_brand#45] Batched: true, DataFilters: [isnotnull(i_item_sk#37)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_desc:string,i_current_price:double,i_wholesale_cost:double,i_brand:st...
         +- Sort [ss_store_sk#66 ASC NULLS FIRST], false, 0
            +- Filter isnotnull(ave#1)
               +- HashAggregate(keys=[ss_store_sk#66], functions=[avg(revenue#0)], output=[ss_store_sk#66, ave#1])
                  +- Exchange hashpartitioning(ss_store_sk#66, 200), ENSURE_REQUIREMENTS, [plan_id=170]
                     +- HashAggregate(keys=[ss_store_sk#66], functions=[partial_avg(revenue#0)], output=[ss_store_sk#66, sum#174, count#175L])
                        +- HashAggregate(keys=[ss_store_sk#66, ss_item_sk#61], functions=[sum(ss_sales_price#72)], output=[ss_store_sk#66, revenue#0])
                           +- Exchange hashpartitioning(ss_store_sk#66, ss_item_sk#61, 200), ENSURE_REQUIREMENTS, [plan_id=166]
                              +- HashAggregate(keys=[ss_store_sk#66, ss_item_sk#61], functions=[partial_sum(ss_sales_price#72)], output=[ss_store_sk#66, ss_item_sk#61, sum#177])
                                 +- Project [ss_item_sk#61, ss_store_sk#66, ss_sales_price#72]
                                    +- BroadcastHashJoin [ss_sold_date_sk#59], [d_date_sk#82], Inner, BuildRight, false
                                       :- Filter (isnotnull(ss_sold_date_sk#59) AND isnotnull(ss_store_sk#66))
                                       :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#59,ss_item_sk#61,ss_store_sk#66,ss_sales_price#72] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#59), isnotnull(ss_store_sk#66)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
                                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=161]
                                          +- Project [d_date_sk#82]
                                             +- Filter (((isnotnull(d_month_seq#85) AND (d_month_seq#85 >= 1176)) AND (d_month_seq#85 <= 1187)) AND isnotnull(d_date_sk#82))
                                                +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#82,d_month_seq#85] Batched: true, DataFilters: [isnotnull(d_month_seq#85), (d_month_seq#85 >= 1176), (d_month_seq#85 <= 1187), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1176), LessThanOrEqual(d_month_seq,1187),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>

Time taken: 2.753 seconds, Fetched 1 row(s)
