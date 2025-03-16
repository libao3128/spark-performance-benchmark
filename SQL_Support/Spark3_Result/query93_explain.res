Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4041
Spark master: local[*], Application Id: local-1741583202875
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['sumsales ASC NULLS FIRST, 'ss_customer_sk ASC NULLS FIRST], true
      +- 'Aggregate ['ss_customer_sk], ['ss_customer_sk, 'sum('act_sales) AS sumsales#1]
         +- 'SubqueryAlias t
            +- 'Project ['ss_item_sk, 'ss_ticket_number, 'ss_customer_sk, CASE WHEN isnotnull('sr_return_quantity) THEN (('ss_quantity - 'sr_return_quantity) * 'ss_sales_price) ELSE ('ss_quantity * 'ss_sales_price) END AS act_sales#0]
               +- 'Filter (('sr_reason_sk = 'r_reason_sk) AND ('r_reason_desc = reason 28))
                  +- 'Join Inner
                     :- 'Join LeftOuter, (('sr_item_sk = 'ss_item_sk) AND ('sr_ticket_number = 'ss_ticket_number))
                     :  :- 'UnresolvedRelation [store_sales], [], false
                     :  +- 'UnresolvedRelation [store_returns], [], false
                     +- 'UnresolvedRelation [reason], [], false

== Analyzed Logical Plan ==
ss_customer_sk: int, sumsales: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [sumsales#1 ASC NULLS FIRST, ss_customer_sk#10 ASC NULLS FIRST], true
      +- Aggregate [ss_customer_sk#10], [ss_customer_sk#10, sum(act_sales#0) AS sumsales#1]
         +- SubqueryAlias t
            +- Project [ss_item_sk#9, ss_ticket_number#16, ss_customer_sk#10, CASE WHEN isnotnull(sr_return_quantity#40) THEN (cast((ss_quantity#17 - sr_return_quantity#40) as double) * ss_sales_price#20) ELSE (cast(ss_quantity#17 as double) * ss_sales_price#20) END AS act_sales#0]
               +- Filter ((sr_reason_sk#38 = r_reason_sk#50) AND (r_reason_desc#52 = reason 28))
                  +- Join Inner
                     :- Join LeftOuter, ((sr_item_sk#32 = ss_item_sk#9) AND (sr_ticket_number#39 = ss_ticket_number#16))
                     :  :- SubqueryAlias spark_catalog.tpcds.store_sales
                     :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
                     :  +- SubqueryAlias spark_catalog.tpcds.store_returns
                     :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#30,sr_return_time_sk#31,sr_item_sk#32,sr_customer_sk#33,sr_cdemo_sk#34,sr_hdemo_sk#35,sr_addr_sk#36,sr_store_sk#37,sr_reason_sk#38,sr_ticket_number#39,sr_return_quantity#40,sr_return_amt#41,sr_return_tax#42,sr_return_amt_inc_tax#43,sr_fee#44,sr_return_ship_cost#45,sr_refunded_cash#46,sr_reversed_charge#47,sr_store_credit#48,sr_net_loss#49] parquet
                     +- SubqueryAlias spark_catalog.tpcds.reason
                        +- Relation spark_catalog.tpcds.reason[r_reason_sk#50,r_reason_id#51,r_reason_desc#52] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [sumsales#1 ASC NULLS FIRST, ss_customer_sk#10 ASC NULLS FIRST], true
      +- Aggregate [ss_customer_sk#10], [ss_customer_sk#10, sum(act_sales#0) AS sumsales#1]
         +- Project [ss_customer_sk#10, CASE WHEN isnotnull(sr_return_quantity#40) THEN (cast((ss_quantity#17 - sr_return_quantity#40) as double) * ss_sales_price#20) ELSE (cast(ss_quantity#17 as double) * ss_sales_price#20) END AS act_sales#0]
            +- Join Inner, (sr_reason_sk#38 = r_reason_sk#50)
               :- Project [ss_customer_sk#10, ss_quantity#17, ss_sales_price#20, sr_reason_sk#38, sr_return_quantity#40]
               :  +- Join Inner, ((sr_item_sk#32 = ss_item_sk#9) AND (sr_ticket_number#39 = ss_ticket_number#16))
               :     :- Project [ss_item_sk#9, ss_customer_sk#10, ss_ticket_number#16, ss_quantity#17, ss_sales_price#20]
               :     :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
               :     +- Project [sr_item_sk#32, sr_reason_sk#38, sr_ticket_number#39, sr_return_quantity#40]
               :        +- Filter ((isnotnull(sr_item_sk#32) AND isnotnull(sr_ticket_number#39)) AND isnotnull(sr_reason_sk#38))
               :           +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#30,sr_return_time_sk#31,sr_item_sk#32,sr_customer_sk#33,sr_cdemo_sk#34,sr_hdemo_sk#35,sr_addr_sk#36,sr_store_sk#37,sr_reason_sk#38,sr_ticket_number#39,sr_return_quantity#40,sr_return_amt#41,sr_return_tax#42,sr_return_amt_inc_tax#43,sr_fee#44,sr_return_ship_cost#45,sr_refunded_cash#46,sr_reversed_charge#47,sr_store_credit#48,sr_net_loss#49] parquet
               +- Project [r_reason_sk#50]
                  +- Filter ((isnotnull(r_reason_desc#52) AND (r_reason_desc#52 = reason 28)) AND isnotnull(r_reason_sk#50))
                     +- Relation spark_catalog.tpcds.reason[r_reason_sk#50,r_reason_id#51,r_reason_desc#52] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[sumsales#1 ASC NULLS FIRST,ss_customer_sk#10 ASC NULLS FIRST], output=[ss_customer_sk#10,sumsales#1])
   +- HashAggregate(keys=[ss_customer_sk#10], functions=[sum(act_sales#0)], output=[ss_customer_sk#10, sumsales#1])
      +- Exchange hashpartitioning(ss_customer_sk#10, 200), ENSURE_REQUIREMENTS, [plan_id=68]
         +- HashAggregate(keys=[ss_customer_sk#10], functions=[partial_sum(act_sales#0)], output=[ss_customer_sk#10, sum#58])
            +- Project [ss_customer_sk#10, CASE WHEN isnotnull(sr_return_quantity#40) THEN (cast((ss_quantity#17 - sr_return_quantity#40) as double) * ss_sales_price#20) ELSE (cast(ss_quantity#17 as double) * ss_sales_price#20) END AS act_sales#0]
               +- BroadcastHashJoin [sr_reason_sk#38], [r_reason_sk#50], Inner, BuildRight, false
                  :- Project [ss_customer_sk#10, ss_quantity#17, ss_sales_price#20, sr_reason_sk#38, sr_return_quantity#40]
                  :  +- BroadcastHashJoin [ss_item_sk#9, ss_ticket_number#16], [sr_item_sk#32, sr_ticket_number#39], Inner, BuildRight, false
                  :     :- FileScan parquet spark_catalog.tpcds.store_sales[ss_item_sk#9,ss_customer_sk#10,ss_ticket_number#16,ss_quantity#17,ss_sales_price#20] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ss_item_sk:int,ss_customer_sk:int,ss_ticket_number:int,ss_quantity:int,ss_sales_price:double>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, false] as bigint), 32) | (cast(input[2, int, false] as bigint) & 4294967295))),false), [plan_id=59]
                  :        +- Filter ((isnotnull(sr_item_sk#32) AND isnotnull(sr_ticket_number#39)) AND isnotnull(sr_reason_sk#38))
                  :           +- FileScan parquet spark_catalog.tpcds.store_returns[sr_item_sk#32,sr_reason_sk#38,sr_ticket_number#39,sr_return_quantity#40] Batched: true, DataFilters: [isnotnull(sr_item_sk#32), isnotnull(sr_ticket_number#39), isnotnull(sr_reason_sk#38)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_ticket_number), IsNotNull(sr_reason_sk)], ReadSchema: struct<sr_item_sk:int,sr_reason_sk:int,sr_ticket_number:int,sr_return_quantity:int>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=63]
                     +- Project [r_reason_sk#50]
                        +- Filter ((isnotnull(r_reason_desc#52) AND (r_reason_desc#52 = reason 28)) AND isnotnull(r_reason_sk#50))
                           +- FileScan parquet spark_catalog.tpcds.reason[r_reason_sk#50,r_reason_desc#52] Batched: true, DataFilters: [isnotnull(r_reason_desc#52), (r_reason_desc#52 = reason 28), isnotnull(r_reason_sk#50)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(r_reason_desc), EqualTo(r_reason_desc,reason 28), IsNotNull(r_reason_sk)], ReadSchema: struct<r_reason_sk:int,r_reason_desc:string>

Time taken: 2.433 seconds, Fetched 1 row(s)
