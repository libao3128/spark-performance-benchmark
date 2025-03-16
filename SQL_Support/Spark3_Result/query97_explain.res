Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583371586
== Parsed Logical Plan ==
CTE [ssci, csci]
:  :- 'SubqueryAlias ssci
:  :  +- 'Aggregate ['ss_customer_sk, 'ss_item_sk], ['ss_customer_sk AS customer_sk#3, 'ss_item_sk AS item_sk#4]
:  :     +- 'Filter (('ss_sold_date_sk = 'd_date_sk) AND (('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11))))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation [store_sales], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  +- 'SubqueryAlias csci
:     +- 'Aggregate ['cs_bill_customer_sk, 'cs_item_sk], ['cs_bill_customer_sk AS customer_sk#5, 'cs_item_sk AS item_sk#6]
:        +- 'Filter (('cs_sold_date_sk = 'd_date_sk) AND (('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11))))
:           +- 'Join Inner
:              :- 'UnresolvedRelation [catalog_sales], [], false
:              +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Project ['sum(CASE WHEN (isnotnull('ssci.customer_sk) AND isnull('csci.customer_sk)) THEN 1 ELSE 0 END) AS store_only#0, 'sum(CASE WHEN (isnull('ssci.customer_sk) AND isnotnull('csci.customer_sk)) THEN 1 ELSE 0 END) AS catalog_only#1, 'sum(CASE WHEN (isnotnull('ssci.customer_sk) AND isnotnull('csci.customer_sk)) THEN 1 ELSE 0 END) AS store_and_catalog#2]
         +- 'Join FullOuter, (('ssci.customer_sk = 'csci.customer_sk) AND ('ssci.item_sk = 'csci.item_sk))
            :- 'UnresolvedRelation [ssci], [], false
            +- 'UnresolvedRelation [csci], [], false

== Analyzed Logical Plan ==
store_only: bigint, catalog_only: bigint, store_and_catalog: bigint
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ssci
:     +- Aggregate [ss_customer_sk#15, ss_item_sk#14], [ss_customer_sk#15 AS customer_sk#3, ss_item_sk#14 AS item_sk#4]
:        +- Filter ((ss_sold_date_sk#12 = d_date_sk#35) AND ((d_month_seq#38 >= 1200) AND (d_month_seq#38 <= (1200 + 11))))
:           +- Join Inner
:              :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#12,ss_sold_time_sk#13,ss_item_sk#14,ss_customer_sk#15,ss_cdemo_sk#16,ss_hdemo_sk#17,ss_addr_sk#18,ss_store_sk#19,ss_promo_sk#20,ss_ticket_number#21,ss_quantity#22,ss_wholesale_cost#23,ss_list_price#24,ss_sales_price#25,ss_ext_discount_amt#26,ss_ext_sales_price#27,ss_ext_wholesale_cost#28,ss_ext_list_price#29,ss_ext_tax#30,ss_coupon_amt#31,ss_net_paid#32,ss_net_paid_inc_tax#33,ss_net_profit#34] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#35,d_date_id#36,d_date#37,d_month_seq#38,d_week_seq#39,d_quarter_seq#40,d_year#41,d_dow#42,d_moy#43,d_dom#44,d_qoy#45,d_fy_year#46,d_fy_quarter_seq#47,d_fy_week_seq#48,d_day_name#49,d_quarter_name#50,d_holiday#51,d_weekend#52,d_following_holiday#53,d_first_dom#54,d_last_dom#55,d_same_day_ly#56,d_same_day_lq#57,d_current_day#58,... 4 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias csci
:     +- Aggregate [cs_bill_customer_sk#66, cs_item_sk#78], [cs_bill_customer_sk#66 AS customer_sk#5, cs_item_sk#78 AS item_sk#6]
:        +- Filter ((cs_sold_date_sk#63 = d_date_sk#97) AND ((d_month_seq#100 >= 1200) AND (d_month_seq#100 <= (1200 + 11))))
:           +- Join Inner
:              :- SubqueryAlias spark_catalog.tpcds.catalog_sales
:              :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#63,cs_sold_time_sk#64,cs_ship_date_sk#65,cs_bill_customer_sk#66,cs_bill_cdemo_sk#67,cs_bill_hdemo_sk#68,cs_bill_addr_sk#69,cs_ship_customer_sk#70,cs_ship_cdemo_sk#71,cs_ship_hdemo_sk#72,cs_ship_addr_sk#73,cs_call_center_sk#74,cs_catalog_page_sk#75,cs_ship_mode_sk#76,cs_warehouse_sk#77,cs_item_sk#78,cs_promo_sk#79,cs_order_number#80,cs_quantity#81,cs_wholesale_cost#82,cs_list_price#83,cs_sales_price#84,cs_ext_discount_amt#85,cs_ext_sales_price#86,... 10 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#97,d_date_id#98,d_date#99,d_month_seq#100,d_week_seq#101,d_quarter_seq#102,d_year#103,d_dow#104,d_moy#105,d_dom#106,d_qoy#107,d_fy_year#108,d_fy_quarter_seq#109,d_fy_week_seq#110,d_day_name#111,d_quarter_name#112,d_holiday#113,d_weekend#114,d_following_holiday#115,d_first_dom#116,d_last_dom#117,d_same_day_ly#118,d_same_day_lq#119,d_current_day#120,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Aggregate [sum(CASE WHEN (isnotnull(customer_sk#3) AND isnull(customer_sk#5)) THEN 1 ELSE 0 END) AS store_only#0L, sum(CASE WHEN (isnull(customer_sk#3) AND isnotnull(customer_sk#5)) THEN 1 ELSE 0 END) AS catalog_only#1L, sum(CASE WHEN (isnotnull(customer_sk#3) AND isnotnull(customer_sk#5)) THEN 1 ELSE 0 END) AS store_and_catalog#2L]
         +- Join FullOuter, ((customer_sk#3 = customer_sk#5) AND (item_sk#4 = item_sk#6))
            :- SubqueryAlias ssci
            :  +- CTERelationRef 0, true, [customer_sk#3, item_sk#4], false
            +- SubqueryAlias csci
               +- CTERelationRef 1, true, [customer_sk#5, item_sk#6], false

== Optimized Logical Plan ==
Aggregate [sum(CASE WHEN (isnotnull(customer_sk#3) AND isnull(customer_sk#5)) THEN 1 ELSE 0 END) AS store_only#0L, sum(CASE WHEN (isnull(customer_sk#3) AND isnotnull(customer_sk#5)) THEN 1 ELSE 0 END) AS catalog_only#1L, sum(CASE WHEN (isnotnull(customer_sk#3) AND isnotnull(customer_sk#5)) THEN 1 ELSE 0 END) AS store_and_catalog#2L]
+- Project [customer_sk#3, customer_sk#5]
   +- Join FullOuter, ((customer_sk#3 = customer_sk#5) AND (item_sk#4 = item_sk#6))
      :- Aggregate [ss_customer_sk#15, ss_item_sk#14], [ss_customer_sk#15 AS customer_sk#3, ss_item_sk#14 AS item_sk#4]
      :  +- Project [ss_item_sk#14, ss_customer_sk#15]
      :     +- Join Inner, (ss_sold_date_sk#12 = d_date_sk#35)
      :        :- Project [ss_sold_date_sk#12, ss_item_sk#14, ss_customer_sk#15]
      :        :  +- Filter isnotnull(ss_sold_date_sk#12)
      :        :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#12,ss_sold_time_sk#13,ss_item_sk#14,ss_customer_sk#15,ss_cdemo_sk#16,ss_hdemo_sk#17,ss_addr_sk#18,ss_store_sk#19,ss_promo_sk#20,ss_ticket_number#21,ss_quantity#22,ss_wholesale_cost#23,ss_list_price#24,ss_sales_price#25,ss_ext_discount_amt#26,ss_ext_sales_price#27,ss_ext_wholesale_cost#28,ss_ext_list_price#29,ss_ext_tax#30,ss_coupon_amt#31,ss_net_paid#32,ss_net_paid_inc_tax#33,ss_net_profit#34] parquet
      :        +- Project [d_date_sk#35]
      :           +- Filter ((isnotnull(d_month_seq#38) AND ((d_month_seq#38 >= 1200) AND (d_month_seq#38 <= 1211))) AND isnotnull(d_date_sk#35))
      :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#35,d_date_id#36,d_date#37,d_month_seq#38,d_week_seq#39,d_quarter_seq#40,d_year#41,d_dow#42,d_moy#43,d_dom#44,d_qoy#45,d_fy_year#46,d_fy_quarter_seq#47,d_fy_week_seq#48,d_day_name#49,d_quarter_name#50,d_holiday#51,d_weekend#52,d_following_holiday#53,d_first_dom#54,d_last_dom#55,d_same_day_ly#56,d_same_day_lq#57,d_current_day#58,... 4 more fields] parquet
      +- Aggregate [cs_bill_customer_sk#66, cs_item_sk#78], [cs_bill_customer_sk#66 AS customer_sk#5, cs_item_sk#78 AS item_sk#6]
         +- Project [cs_bill_customer_sk#66, cs_item_sk#78]
            +- Join Inner, (cs_sold_date_sk#63 = d_date_sk#97)
               :- Project [cs_sold_date_sk#63, cs_bill_customer_sk#66, cs_item_sk#78]
               :  +- Filter isnotnull(cs_sold_date_sk#63)
               :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#63,cs_sold_time_sk#64,cs_ship_date_sk#65,cs_bill_customer_sk#66,cs_bill_cdemo_sk#67,cs_bill_hdemo_sk#68,cs_bill_addr_sk#69,cs_ship_customer_sk#70,cs_ship_cdemo_sk#71,cs_ship_hdemo_sk#72,cs_ship_addr_sk#73,cs_call_center_sk#74,cs_catalog_page_sk#75,cs_ship_mode_sk#76,cs_warehouse_sk#77,cs_item_sk#78,cs_promo_sk#79,cs_order_number#80,cs_quantity#81,cs_wholesale_cost#82,cs_list_price#83,cs_sales_price#84,cs_ext_discount_amt#85,cs_ext_sales_price#86,... 10 more fields] parquet
               +- Project [d_date_sk#97]
                  +- Filter ((isnotnull(d_month_seq#100) AND ((d_month_seq#100 >= 1200) AND (d_month_seq#100 <= 1211))) AND isnotnull(d_date_sk#97))
                     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#97,d_date_id#98,d_date#99,d_month_seq#100,d_week_seq#101,d_quarter_seq#102,d_year#103,d_dow#104,d_moy#105,d_dom#106,d_qoy#107,d_fy_year#108,d_fy_quarter_seq#109,d_fy_week_seq#110,d_day_name#111,d_quarter_name#112,d_holiday#113,d_weekend#114,d_following_holiday#115,d_first_dom#116,d_last_dom#117,d_same_day_ly#118,d_same_day_lq#119,d_current_day#120,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[sum(CASE WHEN (isnotnull(customer_sk#3) AND isnull(customer_sk#5)) THEN 1 ELSE 0 END), sum(CASE WHEN (isnull(customer_sk#3) AND isnotnull(customer_sk#5)) THEN 1 ELSE 0 END), sum(CASE WHEN (isnotnull(customer_sk#3) AND isnotnull(customer_sk#5)) THEN 1 ELSE 0 END)], output=[store_only#0L, catalog_only#1L, store_and_catalog#2L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=117]
      +- HashAggregate(keys=[], functions=[partial_sum(CASE WHEN (isnotnull(customer_sk#3) AND isnull(customer_sk#5)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (isnull(customer_sk#3) AND isnotnull(customer_sk#5)) THEN 1 ELSE 0 END), partial_sum(CASE WHEN (isnotnull(customer_sk#3) AND isnotnull(customer_sk#5)) THEN 1 ELSE 0 END)], output=[sum#135L, sum#136L, sum#137L])
         +- Project [customer_sk#3, customer_sk#5]
            +- SortMergeJoin [customer_sk#3, item_sk#4], [customer_sk#5, item_sk#6], FullOuter
               :- Sort [customer_sk#3 ASC NULLS FIRST, item_sk#4 ASC NULLS FIRST], false, 0
               :  +- HashAggregate(keys=[ss_customer_sk#15, ss_item_sk#14], functions=[], output=[customer_sk#3, item_sk#4])
               :     +- Exchange hashpartitioning(ss_customer_sk#15, ss_item_sk#14, 200), ENSURE_REQUIREMENTS, [plan_id=100]
               :        +- HashAggregate(keys=[ss_customer_sk#15, ss_item_sk#14], functions=[], output=[ss_customer_sk#15, ss_item_sk#14])
               :           +- Project [ss_item_sk#14, ss_customer_sk#15]
               :              +- BroadcastHashJoin [ss_sold_date_sk#12], [d_date_sk#35], Inner, BuildRight, false
               :                 :- Filter isnotnull(ss_sold_date_sk#12)
               :                 :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#12,ss_item_sk#14,ss_customer_sk#15] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#12)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int>
               :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=95]
               :                    +- Project [d_date_sk#35]
               :                       +- Filter (((isnotnull(d_month_seq#38) AND (d_month_seq#38 >= 1200)) AND (d_month_seq#38 <= 1211)) AND isnotnull(d_date_sk#35))
               :                          +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#35,d_month_seq#38] Batched: true, DataFilters: [isnotnull(d_month_seq#38), (d_month_seq#38 >= 1200), (d_month_seq#38 <= 1211), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
               +- Sort [customer_sk#5 ASC NULLS FIRST, item_sk#6 ASC NULLS FIRST], false, 0
                  +- HashAggregate(keys=[cs_bill_customer_sk#66, cs_item_sk#78], functions=[], output=[customer_sk#5, item_sk#6])
                     +- Exchange hashpartitioning(cs_bill_customer_sk#66, cs_item_sk#78, 200), ENSURE_REQUIREMENTS, [plan_id=107]
                        +- HashAggregate(keys=[cs_bill_customer_sk#66, cs_item_sk#78], functions=[], output=[cs_bill_customer_sk#66, cs_item_sk#78])
                           +- Project [cs_bill_customer_sk#66, cs_item_sk#78]
                              +- BroadcastHashJoin [cs_sold_date_sk#63], [d_date_sk#97], Inner, BuildRight, false
                                 :- Filter isnotnull(cs_sold_date_sk#63)
                                 :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#63,cs_bill_customer_sk#66,cs_item_sk#78] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#63)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=102]
                                    +- Project [d_date_sk#97]
                                       +- Filter (((isnotnull(d_month_seq#100) AND (d_month_seq#100 >= 1200)) AND (d_month_seq#100 <= 1211)) AND isnotnull(d_date_sk#97))
                                          +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#97,d_month_seq#100] Batched: true, DataFilters: [isnotnull(d_month_seq#100), (d_month_seq#100 >= 1200), (d_month_seq#100 <= 1211), isnotnull(d_da..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>

Time taken: 2.906 seconds, Fetched 1 row(s)
