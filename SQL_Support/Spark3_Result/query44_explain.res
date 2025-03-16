Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581257866
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['asceding.rnk ASC NULLS FIRST], true
      +- 'Project ['asceding.rnk, 'i1.i_product_name AS best_performing#10, 'i2.i_product_name AS worst_performing#11]
         +- 'Filter ((('asceding.rnk = 'descending.rnk) AND ('i1.i_item_sk = 'asceding.item_sk)) AND ('i2.i_item_sk = 'descending.item_sk))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'SubqueryAlias asceding
               :  :  :  +- 'Project [*]
               :  :  :     +- 'Filter ('rnk < 11)
               :  :  :        +- 'SubqueryAlias V11
               :  :  :           +- 'Project ['item_sk, 'rank() windowspecdefinition('rank_col ASC NULLS FIRST, unspecifiedframe$()) AS rnk#4]
               :  :  :              +- 'SubqueryAlias V1
               :  :  :                 +- 'UnresolvedHaving ('avg('ss_net_profit) > (0.9 * scalar-subquery#3 []))
               :  :  :                    :  +- 'Aggregate ['ss_store_sk], ['avg('ss_net_profit) AS rank_col#2]
               :  :  :                    :     +- 'Filter (('ss_store_sk = 4) AND isnull('ss_addr_sk))
               :  :  :                    :        +- 'UnresolvedRelation [store_sales], [], false
               :  :  :                    +- 'Aggregate ['ss_item_sk], ['ss_item_sk AS item_sk#0, 'avg('ss_net_profit) AS rank_col#1]
               :  :  :                       +- 'Filter ('ss_store_sk = 4)
               :  :  :                          +- 'SubqueryAlias ss1
               :  :  :                             +- 'UnresolvedRelation [store_sales], [], false
               :  :  +- 'SubqueryAlias descending
               :  :     +- 'Project [*]
               :  :        +- 'Filter ('rnk < 11)
               :  :           +- 'SubqueryAlias V21
               :  :              +- 'Project ['item_sk, 'rank() windowspecdefinition('rank_col DESC NULLS LAST, unspecifiedframe$()) AS rnk#9]
               :  :                 +- 'SubqueryAlias V2
               :  :                    +- 'UnresolvedHaving ('avg('ss_net_profit) > (0.9 * scalar-subquery#8 []))
               :  :                       :  +- 'Aggregate ['ss_store_sk], ['avg('ss_net_profit) AS rank_col#7]
               :  :                       :     +- 'Filter (('ss_store_sk = 4) AND isnull('ss_addr_sk))
               :  :                       :        +- 'UnresolvedRelation [store_sales], [], false
               :  :                       +- 'Aggregate ['ss_item_sk], ['ss_item_sk AS item_sk#5, 'avg('ss_net_profit) AS rank_col#6]
               :  :                          +- 'Filter ('ss_store_sk = 4)
               :  :                             +- 'SubqueryAlias ss1
               :  :                                +- 'UnresolvedRelation [store_sales], [], false
               :  +- 'SubqueryAlias i1
               :     +- 'UnresolvedRelation [item], [], false
               +- 'SubqueryAlias i2
                  +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
rnk: int, best_performing: string, worst_performing: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [rnk#4 ASC NULLS FIRST], true
      +- Project [rnk#4, i_product_name#65 AS best_performing#10, i_product_name#110 AS worst_performing#11]
         +- Filter (((rnk#4 = rnk#9) AND (i_item_sk#44 = item_sk#0)) AND (i_item_sk#89 = item_sk#5))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias asceding
               :  :  :  +- Project [item_sk#0, rnk#4]
               :  :  :     +- Filter (rnk#4 < 11)
               :  :  :        +- SubqueryAlias V11
               :  :  :           +- Project [item_sk#0, rnk#4]
               :  :  :              +- Project [item_sk#0, rank_col#1, rnk#4, rnk#4]
               :  :  :                 +- Window [rank(rank_col#1) windowspecdefinition(rank_col#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#4], [rank_col#1 ASC NULLS FIRST]
               :  :  :                    +- Project [item_sk#0, rank_col#1]
               :  :  :                       +- SubqueryAlias V1
               :  :  :                          +- Filter (rank_col#1 > (cast(0.9 as double) * scalar-subquery#3 []))
               :  :  :                             :  +- Aggregate [ss_store_sk#124], [avg(ss_net_profit#139) AS rank_col#2]
               :  :  :                             :     +- Filter ((ss_store_sk#124 = 4) AND isnull(ss_addr_sk#123))
               :  :  :                             :        +- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :  :                             :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#117,ss_sold_time_sk#118,ss_item_sk#119,ss_customer_sk#120,ss_cdemo_sk#121,ss_hdemo_sk#122,ss_addr_sk#123,ss_store_sk#124,ss_promo_sk#125,ss_ticket_number#126,ss_quantity#127,ss_wholesale_cost#128,ss_list_price#129,ss_sales_price#130,ss_ext_discount_amt#131,ss_ext_sales_price#132,ss_ext_wholesale_cost#133,ss_ext_list_price#134,ss_ext_tax#135,ss_coupon_amt#136,ss_net_paid#137,ss_net_paid_inc_tax#138,ss_net_profit#139] parquet
               :  :  :                             +- Aggregate [ss_item_sk#23], [ss_item_sk#23 AS item_sk#0, avg(ss_net_profit#43) AS rank_col#1]
               :  :  :                                +- Filter (ss_store_sk#28 = 4)
               :  :  :                                   +- SubqueryAlias ss1
               :  :  :                                      +- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :  :                                         +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_sold_time_sk#22,ss_item_sk#23,ss_customer_sk#24,ss_cdemo_sk#25,ss_hdemo_sk#26,ss_addr_sk#27,ss_store_sk#28,ss_promo_sk#29,ss_ticket_number#30,ss_quantity#31,ss_wholesale_cost#32,ss_list_price#33,ss_sales_price#34,ss_ext_discount_amt#35,ss_ext_sales_price#36,ss_ext_wholesale_cost#37,ss_ext_list_price#38,ss_ext_tax#39,ss_coupon_amt#40,ss_net_paid#41,ss_net_paid_inc_tax#42,ss_net_profit#43] parquet
               :  :  +- SubqueryAlias descending
               :  :     +- Project [item_sk#5, rnk#9]
               :  :        +- Filter (rnk#9 < 11)
               :  :           +- SubqueryAlias V21
               :  :              +- Project [item_sk#5, rnk#9]
               :  :                 +- Project [item_sk#5, rank_col#6, rnk#9, rnk#9]
               :  :                    +- Window [rank(rank_col#6) windowspecdefinition(rank_col#6 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#9], [rank_col#6 DESC NULLS LAST]
               :  :                       +- Project [item_sk#5, rank_col#6]
               :  :                          +- SubqueryAlias V2
               :  :                             +- Filter (rank_col#6 > (cast(0.9 as double) * scalar-subquery#8 []))
               :  :                                :  +- Aggregate [ss_store_sk#147], [avg(ss_net_profit#162) AS rank_col#7]
               :  :                                :     +- Filter ((ss_store_sk#147 = 4) AND isnull(ss_addr_sk#146))
               :  :                                :        +- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :                                :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#140,ss_sold_time_sk#141,ss_item_sk#142,ss_customer_sk#143,ss_cdemo_sk#144,ss_hdemo_sk#145,ss_addr_sk#146,ss_store_sk#147,ss_promo_sk#148,ss_ticket_number#149,ss_quantity#150,ss_wholesale_cost#151,ss_list_price#152,ss_sales_price#153,ss_ext_discount_amt#154,ss_ext_sales_price#155,ss_ext_wholesale_cost#156,ss_ext_list_price#157,ss_ext_tax#158,ss_coupon_amt#159,ss_net_paid#160,ss_net_paid_inc_tax#161,ss_net_profit#162] parquet
               :  :                                +- Aggregate [ss_item_sk#68], [ss_item_sk#68 AS item_sk#5, avg(ss_net_profit#88) AS rank_col#6]
               :  :                                   +- Filter (ss_store_sk#73 = 4)
               :  :                                      +- SubqueryAlias ss1
               :  :                                         +- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :                                            +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#66,ss_sold_time_sk#67,ss_item_sk#68,ss_customer_sk#69,ss_cdemo_sk#70,ss_hdemo_sk#71,ss_addr_sk#72,ss_store_sk#73,ss_promo_sk#74,ss_ticket_number#75,ss_quantity#76,ss_wholesale_cost#77,ss_list_price#78,ss_sales_price#79,ss_ext_discount_amt#80,ss_ext_sales_price#81,ss_ext_wholesale_cost#82,ss_ext_list_price#83,ss_ext_tax#84,ss_coupon_amt#85,ss_net_paid#86,ss_net_paid_inc_tax#87,ss_net_profit#88] parquet
               :  +- SubqueryAlias i1
               :     +- SubqueryAlias spark_catalog.tpcds.item
               :        +- Relation spark_catalog.tpcds.item[i_item_sk#44,i_item_id#45,i_rec_start_date#46,i_rec_end_date#47,i_item_desc#48,i_current_price#49,i_wholesale_cost#50,i_brand_id#51,i_brand#52,i_class_id#53,i_class#54,i_category_id#55,i_category#56,i_manufact_id#57,i_manufact#58,i_size#59,i_formulation#60,i_color#61,i_units#62,i_container#63,i_manager_id#64,i_product_name#65] parquet
               +- SubqueryAlias i2
                  +- SubqueryAlias spark_catalog.tpcds.item
                     +- Relation spark_catalog.tpcds.item[i_item_sk#89,i_item_id#90,i_rec_start_date#91,i_rec_end_date#92,i_item_desc#93,i_current_price#94,i_wholesale_cost#95,i_brand_id#96,i_brand#97,i_class_id#98,i_class#99,i_category_id#100,i_category#101,i_manufact_id#102,i_manufact#103,i_size#104,i_formulation#105,i_color#106,i_units#107,i_container#108,i_manager_id#109,i_product_name#110] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [rnk#4 ASC NULLS FIRST], true
      +- Project [rnk#4, i_product_name#65 AS best_performing#10, i_product_name#110 AS worst_performing#11]
         +- Join Inner, (i_item_sk#89 = item_sk#5)
            :- Project [rnk#4, item_sk#5, i_product_name#65]
            :  +- Join Inner, (i_item_sk#44 = item_sk#0)
            :     :- Project [item_sk#0, rnk#4, item_sk#5]
            :     :  +- Join Inner, (rnk#4 = rnk#9)
            :     :     :- Project [item_sk#0, rnk#4]
            :     :     :  +- Filter ((rnk#4 < 11) AND isnotnull(item_sk#0))
            :     :     :     +- Window [rank(rank_col#1) windowspecdefinition(rank_col#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#4], [rank_col#1 ASC NULLS FIRST]
            :     :     :        +- WindowGroupLimit [rank_col#1 ASC NULLS FIRST], rank(rank_col#1), 10
            :     :     :           +- Filter (isnotnull(rank_col#1) AND (rank_col#1 > (0.9 * scalar-subquery#3 [])))
            :     :     :              :  +- Aggregate [ss_store_sk#124], [avg(ss_net_profit#139) AS rank_col#2]
            :     :     :              :     +- Project [ss_store_sk#124, ss_net_profit#139]
            :     :     :              :        +- Filter (isnotnull(ss_store_sk#124) AND ((ss_store_sk#124 = 4) AND isnull(ss_addr_sk#123)))
            :     :     :              :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#117,ss_sold_time_sk#118,ss_item_sk#119,ss_customer_sk#120,ss_cdemo_sk#121,ss_hdemo_sk#122,ss_addr_sk#123,ss_store_sk#124,ss_promo_sk#125,ss_ticket_number#126,ss_quantity#127,ss_wholesale_cost#128,ss_list_price#129,ss_sales_price#130,ss_ext_discount_amt#131,ss_ext_sales_price#132,ss_ext_wholesale_cost#133,ss_ext_list_price#134,ss_ext_tax#135,ss_coupon_amt#136,ss_net_paid#137,ss_net_paid_inc_tax#138,ss_net_profit#139] parquet
            :     :     :              +- Aggregate [ss_item_sk#23], [ss_item_sk#23 AS item_sk#0, avg(ss_net_profit#43) AS rank_col#1]
            :     :     :                 +- Project [ss_item_sk#23, ss_net_profit#43]
            :     :     :                    +- Filter (isnotnull(ss_store_sk#28) AND (ss_store_sk#28 = 4))
            :     :     :                       +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#21,ss_sold_time_sk#22,ss_item_sk#23,ss_customer_sk#24,ss_cdemo_sk#25,ss_hdemo_sk#26,ss_addr_sk#27,ss_store_sk#28,ss_promo_sk#29,ss_ticket_number#30,ss_quantity#31,ss_wholesale_cost#32,ss_list_price#33,ss_sales_price#34,ss_ext_discount_amt#35,ss_ext_sales_price#36,ss_ext_wholesale_cost#37,ss_ext_list_price#38,ss_ext_tax#39,ss_coupon_amt#40,ss_net_paid#41,ss_net_paid_inc_tax#42,ss_net_profit#43] parquet
            :     :     +- Project [item_sk#5, rnk#9]
            :     :        +- Filter ((rnk#9 < 11) AND isnotnull(item_sk#5))
            :     :           +- Window [rank(rank_col#6) windowspecdefinition(rank_col#6 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#9], [rank_col#6 DESC NULLS LAST]
            :     :              +- WindowGroupLimit [rank_col#6 DESC NULLS LAST], rank(rank_col#6), 10
            :     :                 +- Filter (isnotnull(rank_col#6) AND (rank_col#6 > (0.9 * scalar-subquery#8 [])))
            :     :                    :  +- Aggregate [ss_store_sk#124], [avg(ss_net_profit#139) AS rank_col#2]
            :     :                    :     +- Project [ss_store_sk#124, ss_net_profit#139]
            :     :                    :        +- Filter (isnotnull(ss_store_sk#124) AND ((ss_store_sk#124 = 4) AND isnull(ss_addr_sk#123)))
            :     :                    :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#117,ss_sold_time_sk#118,ss_item_sk#119,ss_customer_sk#120,ss_cdemo_sk#121,ss_hdemo_sk#122,ss_addr_sk#123,ss_store_sk#124,ss_promo_sk#125,ss_ticket_number#126,ss_quantity#127,ss_wholesale_cost#128,ss_list_price#129,ss_sales_price#130,ss_ext_discount_amt#131,ss_ext_sales_price#132,ss_ext_wholesale_cost#133,ss_ext_list_price#134,ss_ext_tax#135,ss_coupon_amt#136,ss_net_paid#137,ss_net_paid_inc_tax#138,ss_net_profit#139] parquet
            :     :                    +- Aggregate [ss_item_sk#68], [ss_item_sk#68 AS item_sk#5, avg(ss_net_profit#88) AS rank_col#6]
            :     :                       +- Project [ss_item_sk#68, ss_net_profit#88]
            :     :                          +- Filter (isnotnull(ss_store_sk#73) AND (ss_store_sk#73 = 4))
            :     :                             +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#66,ss_sold_time_sk#67,ss_item_sk#68,ss_customer_sk#69,ss_cdemo_sk#70,ss_hdemo_sk#71,ss_addr_sk#72,ss_store_sk#73,ss_promo_sk#74,ss_ticket_number#75,ss_quantity#76,ss_wholesale_cost#77,ss_list_price#78,ss_sales_price#79,ss_ext_discount_amt#80,ss_ext_sales_price#81,ss_ext_wholesale_cost#82,ss_ext_list_price#83,ss_ext_tax#84,ss_coupon_amt#85,ss_net_paid#86,ss_net_paid_inc_tax#87,ss_net_profit#88] parquet
            :     +- Project [i_item_sk#44, i_product_name#65]
            :        +- Filter isnotnull(i_item_sk#44)
            :           +- Relation spark_catalog.tpcds.item[i_item_sk#44,i_item_id#45,i_rec_start_date#46,i_rec_end_date#47,i_item_desc#48,i_current_price#49,i_wholesale_cost#50,i_brand_id#51,i_brand#52,i_class_id#53,i_class#54,i_category_id#55,i_category#56,i_manufact_id#57,i_manufact#58,i_size#59,i_formulation#60,i_color#61,i_units#62,i_container#63,i_manager_id#64,i_product_name#65] parquet
            +- Project [i_item_sk#89, i_product_name#110]
               +- Filter isnotnull(i_item_sk#89)
                  +- Relation spark_catalog.tpcds.item[i_item_sk#89,i_item_id#90,i_rec_start_date#91,i_rec_end_date#92,i_item_desc#93,i_current_price#94,i_wholesale_cost#95,i_brand_id#96,i_brand#97,i_class_id#98,i_class#99,i_category_id#100,i_category#101,i_manufact_id#102,i_manufact#103,i_size#104,i_formulation#105,i_color#106,i_units#107,i_container#108,i_manager_id#109,i_product_name#110] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[rnk#4 ASC NULLS FIRST], output=[rnk#4,best_performing#10,worst_performing#11])
   +- Project [rnk#4, i_product_name#65 AS best_performing#10, i_product_name#110 AS worst_performing#11]
      +- BroadcastHashJoin [item_sk#5], [i_item_sk#89], Inner, BuildRight, false
         :- Project [rnk#4, item_sk#5, i_product_name#65]
         :  +- BroadcastHashJoin [item_sk#0], [i_item_sk#44], Inner, BuildRight, false
         :     :- Project [item_sk#0, rnk#4, item_sk#5]
         :     :  +- SortMergeJoin [rnk#4], [rnk#9], Inner
         :     :     :- Sort [rnk#4 ASC NULLS FIRST], false, 0
         :     :     :  +- Project [item_sk#0, rnk#4]
         :     :     :     +- Filter ((rnk#4 < 11) AND isnotnull(item_sk#0))
         :     :     :        +- Window [rank(rank_col#1) windowspecdefinition(rank_col#1 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#4], [rank_col#1 ASC NULLS FIRST]
         :     :     :           +- WindowGroupLimit [rank_col#1 ASC NULLS FIRST], rank(rank_col#1), 10, Final
         :     :     :              +- Sort [rank_col#1 ASC NULLS FIRST], false, 0
         :     :     :                 +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=182]
         :     :     :                    +- WindowGroupLimit [rank_col#1 ASC NULLS FIRST], rank(rank_col#1), 10, Partial
         :     :     :                       +- Sort [rank_col#1 ASC NULLS FIRST], false, 0
         :     :     :                          +- Filter (isnotnull(rank_col#1) AND (rank_col#1 > (0.9 * Subquery subquery#3, [id=#148])))
         :     :     :                             :  +- Subquery subquery#3, [id=#148]
         :     :     :                             :     +- AdaptiveSparkPlan isFinalPlan=false
         :     :     :                             :        +- HashAggregate(keys=[ss_store_sk#124], functions=[avg(ss_net_profit#139)], output=[rank_col#2])
         :     :     :                             :           +- Exchange hashpartitioning(ss_store_sk#124, 200), ENSURE_REQUIREMENTS, [plan_id=134]
         :     :     :                             :              +- HashAggregate(keys=[ss_store_sk#124], functions=[partial_avg(ss_net_profit#139)], output=[ss_store_sk#124, sum#189, count#190L])
         :     :     :                             :                 +- Project [ss_store_sk#124, ss_net_profit#139]
         :     :     :                             :                    +- Filter ((isnotnull(ss_store_sk#124) AND (ss_store_sk#124 = 4)) AND isnull(ss_addr_sk#123))
         :     :     :                             :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_addr_sk#123,ss_store_sk#124,ss_net_profit#139] Batched: true, DataFilters: [isnotnull(ss_store_sk#124), (ss_store_sk#124 = 4), isnull(ss_addr_sk#123)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), EqualTo(ss_store_sk,4), IsNull(ss_addr_sk)], ReadSchema: struct<ss_addr_sk:int,ss_store_sk:int,ss_net_profit:double>
         :     :     :                             +- HashAggregate(keys=[ss_item_sk#23], functions=[avg(ss_net_profit#43)], output=[item_sk#0, rank_col#1])
         :     :     :                                +- Exchange hashpartitioning(ss_item_sk#23, 200), ENSURE_REQUIREMENTS, [plan_id=175]
         :     :     :                                   +- HashAggregate(keys=[ss_item_sk#23], functions=[partial_avg(ss_net_profit#43)], output=[ss_item_sk#23, sum#181, count#182L])
         :     :     :                                      +- Project [ss_item_sk#23, ss_net_profit#43]
         :     :     :                                         +- Filter (isnotnull(ss_store_sk#28) AND (ss_store_sk#28 = 4))
         :     :     :                                            +- FileScan parquet spark_catalog.tpcds.store_sales[ss_item_sk#23,ss_store_sk#28,ss_net_profit#43] Batched: true, DataFilters: [isnotnull(ss_store_sk#28), (ss_store_sk#28 = 4)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), EqualTo(ss_store_sk,4)], ReadSchema: struct<ss_item_sk:int,ss_store_sk:int,ss_net_profit:double>
         :     :     +- Sort [rnk#9 ASC NULLS FIRST], false, 0
         :     :        +- Project [item_sk#5, rnk#9]
         :     :           +- Filter ((rnk#9 < 11) AND isnotnull(item_sk#5))
         :     :              +- Window [rank(rank_col#6) windowspecdefinition(rank_col#6 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#9], [rank_col#6 DESC NULLS LAST]
         :     :                 +- WindowGroupLimit [rank_col#6 DESC NULLS LAST], rank(rank_col#6), 10, Final
         :     :                    +- Sort [rank_col#6 DESC NULLS LAST], false, 0
         :     :                       +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=195]
         :     :                          +- WindowGroupLimit [rank_col#6 DESC NULLS LAST], rank(rank_col#6), 10, Partial
         :     :                             +- Sort [rank_col#6 DESC NULLS LAST], false, 0
         :     :                                +- Filter (isnotnull(rank_col#6) AND (rank_col#6 > (0.9 * Subquery subquery#8, [id=#155])))
         :     :                                   :  +- Subquery subquery#8, [id=#155]
         :     :                                   :     +- AdaptiveSparkPlan isFinalPlan=false
         :     :                                   :        +- HashAggregate(keys=[ss_store_sk#124], functions=[avg(ss_net_profit#139)], output=[rank_col#2])
         :     :                                   :           +- Exchange hashpartitioning(ss_store_sk#124, 200), ENSURE_REQUIREMENTS, [plan_id=146]
         :     :                                   :              +- HashAggregate(keys=[ss_store_sk#124], functions=[partial_avg(ss_net_profit#139)], output=[ss_store_sk#124, sum#189, count#190L])
         :     :                                   :                 +- Project [ss_store_sk#124, ss_net_profit#139]
         :     :                                   :                    +- Filter ((isnotnull(ss_store_sk#124) AND (ss_store_sk#124 = 4)) AND isnull(ss_addr_sk#123))
         :     :                                   :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_addr_sk#123,ss_store_sk#124,ss_net_profit#139] Batched: true, DataFilters: [isnotnull(ss_store_sk#124), (ss_store_sk#124 = 4), isnull(ss_addr_sk#123)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), EqualTo(ss_store_sk,4), IsNull(ss_addr_sk)], ReadSchema: struct<ss_addr_sk:int,ss_store_sk:int,ss_net_profit:double>
         :     :                                   +- HashAggregate(keys=[ss_item_sk#68], functions=[avg(ss_net_profit#88)], output=[item_sk#5, rank_col#6])
         :     :                                      +- Exchange hashpartitioning(ss_item_sk#68, 200), ENSURE_REQUIREMENTS, [plan_id=188]
         :     :                                         +- HashAggregate(keys=[ss_item_sk#68], functions=[partial_avg(ss_net_profit#88)], output=[ss_item_sk#68, sum#185, count#186L])
         :     :                                            +- Project [ss_item_sk#68, ss_net_profit#88]
         :     :                                               +- Filter (isnotnull(ss_store_sk#73) AND (ss_store_sk#73 = 4))
         :     :                                                  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_item_sk#68,ss_store_sk#73,ss_net_profit#88] Batched: true, DataFilters: [isnotnull(ss_store_sk#73), (ss_store_sk#73 = 4)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), EqualTo(ss_store_sk,4)], ReadSchema: struct<ss_item_sk:int,ss_store_sk:int,ss_net_profit:double>
         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=208]
         :        +- Filter isnotnull(i_item_sk#44)
         :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#44,i_product_name#65] Batched: true, DataFilters: [isnotnull(i_item_sk#44)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_product_name:string>
         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=212]
            +- Filter isnotnull(i_item_sk#89)
               +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#89,i_product_name#110] Batched: true, DataFilters: [isnotnull(i_item_sk#89)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_product_name:string>

Time taken: 2.822 seconds, Fetched 1 row(s)
