== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['sumsales ASC NULLS FIRST, 'ss_customer_sk ASC NULLS FIRST], true
      +- 'Aggregate ['ss_customer_sk], ['ss_customer_sk, 'sum('act_sales) AS sumsales#1]
         +- 'SubqueryAlias `t`
            +- 'Project ['ss_item_sk, 'ss_ticket_number, 'ss_customer_sk, CASE WHEN isnotnull('sr_return_quantity) THEN (('ss_quantity - 'sr_return_quantity) * 'ss_sales_price) ELSE ('ss_quantity * 'ss_sales_price) END AS act_sales#0]
               +- 'Filter (('sr_reason_sk = 'r_reason_sk) && ('r_reason_desc = reason 28))
                  +- 'Join Inner
                     :- 'Join LeftOuter, (('sr_item_sk = 'ss_item_sk) && ('sr_ticket_number = 'ss_ticket_number))
                     :  :- 'UnresolvedRelation `store_sales`
                     :  +- 'UnresolvedRelation `store_returns`
                     +- 'UnresolvedRelation `reason`

== Analyzed Logical Plan ==
ss_customer_sk: int, sumsales: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [sumsales#1 ASC NULLS FIRST, ss_customer_sk#7 ASC NULLS FIRST], true
      +- Aggregate [ss_customer_sk#7], [ss_customer_sk#7, sum(act_sales#0) AS sumsales#1]
         +- SubqueryAlias `t`
            +- Project [ss_item_sk#6, ss_ticket_number#13, ss_customer_sk#7, CASE WHEN isnotnull(sr_return_quantity#37) THEN (cast((ss_quantity#14 - sr_return_quantity#37) as double) * ss_sales_price#17) ELSE (cast(ss_quantity#14 as double) * ss_sales_price#17) END AS act_sales#0]
               +- Filter ((sr_reason_sk#35 = r_reason_sk#47) && (r_reason_desc#49 = reason 28))
                  +- Join Inner
                     :- Join LeftOuter, ((sr_item_sk#29 = ss_item_sk#6) && (sr_ticket_number#36 = ss_ticket_number#13))
                     :  :- SubqueryAlias `tpcds`.`store_sales`
                     :  :  +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
                     :  +- SubqueryAlias `tpcds`.`store_returns`
                     :     +- Relation[sr_returned_date_sk#27,sr_return_time_sk#28,sr_item_sk#29,sr_customer_sk#30,sr_cdemo_sk#31,sr_hdemo_sk#32,sr_addr_sk#33,sr_store_sk#34,sr_reason_sk#35,sr_ticket_number#36,sr_return_quantity#37,sr_return_amt#38,sr_return_tax#39,sr_return_amt_inc_tax#40,sr_fee#41,sr_return_ship_cost#42,sr_refunded_cash#43,sr_reversed_charge#44,sr_store_credit#45,sr_net_loss#46] parquet
                     +- SubqueryAlias `tpcds`.`reason`
                        +- Relation[r_reason_sk#47,r_reason_id#48,r_reason_desc#49] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [sumsales#1 ASC NULLS FIRST, ss_customer_sk#7 ASC NULLS FIRST], true
      +- Aggregate [ss_customer_sk#7], [ss_customer_sk#7, sum(act_sales#0) AS sumsales#1]
         +- Project [ss_customer_sk#7, CASE WHEN isnotnull(sr_return_quantity#37) THEN (cast((ss_quantity#14 - sr_return_quantity#37) as double) * ss_sales_price#17) ELSE (cast(ss_quantity#14 as double) * ss_sales_price#17) END AS act_sales#0]
            +- Join Inner, (sr_reason_sk#35 = r_reason_sk#47)
               :- Project [ss_customer_sk#7, ss_quantity#14, ss_sales_price#17, sr_reason_sk#35, sr_return_quantity#37]
               :  +- Join Inner, ((sr_item_sk#29 = ss_item_sk#6) && (sr_ticket_number#36 = ss_ticket_number#13))
               :     :- Project [ss_item_sk#6, ss_customer_sk#7, ss_ticket_number#13, ss_quantity#14, ss_sales_price#17]
               :     :  +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
               :     +- Project [sr_item_sk#29, sr_reason_sk#35, sr_ticket_number#36, sr_return_quantity#37]
               :        +- Filter ((isnotnull(sr_item_sk#29) && isnotnull(sr_ticket_number#36)) && isnotnull(sr_reason_sk#35))
               :           +- Relation[sr_returned_date_sk#27,sr_return_time_sk#28,sr_item_sk#29,sr_customer_sk#30,sr_cdemo_sk#31,sr_hdemo_sk#32,sr_addr_sk#33,sr_store_sk#34,sr_reason_sk#35,sr_ticket_number#36,sr_return_quantity#37,sr_return_amt#38,sr_return_tax#39,sr_return_amt_inc_tax#40,sr_fee#41,sr_return_ship_cost#42,sr_refunded_cash#43,sr_reversed_charge#44,sr_store_credit#45,sr_net_loss#46] parquet
               +- Project [r_reason_sk#47]
                  +- Filter ((isnotnull(r_reason_desc#49) && (r_reason_desc#49 = reason 28)) && isnotnull(r_reason_sk#47))
                     +- Relation[r_reason_sk#47,r_reason_id#48,r_reason_desc#49] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[sumsales#1 ASC NULLS FIRST,ss_customer_sk#7 ASC NULLS FIRST], output=[ss_customer_sk#7,sumsales#1])
+- *(4) HashAggregate(keys=[ss_customer_sk#7], functions=[sum(act_sales#0)], output=[ss_customer_sk#7, sumsales#1])
   +- Exchange hashpartitioning(ss_customer_sk#7, 200)
      +- *(3) HashAggregate(keys=[ss_customer_sk#7], functions=[partial_sum(act_sales#0)], output=[ss_customer_sk#7, sum#54])
         +- *(3) Project [ss_customer_sk#7, CASE WHEN isnotnull(sr_return_quantity#37) THEN (cast((ss_quantity#14 - sr_return_quantity#37) as double) * ss_sales_price#17) ELSE (cast(ss_quantity#14 as double) * ss_sales_price#17) END AS act_sales#0]
            +- *(3) BroadcastHashJoin [sr_reason_sk#35], [r_reason_sk#47], Inner, BuildRight
               :- *(3) Project [ss_customer_sk#7, ss_quantity#14, ss_sales_price#17, sr_reason_sk#35, sr_return_quantity#37]
               :  +- *(3) BroadcastHashJoin [ss_item_sk#6, ss_ticket_number#13], [sr_item_sk#29, sr_ticket_number#36], Inner, BuildRight
               :     :- *(3) FileScan parquet tpcds.store_sales[ss_item_sk#6,ss_customer_sk#7,ss_ticket_number#13,ss_quantity#14,ss_sales_price#17] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ss_item_sk:int,ss_customer_sk:int,ss_ticket_number:int,ss_quantity:int,ss_sales_price:double>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, true] as bigint), 32) | (cast(input[2, int, true] as bigint) & 4294967295))))
               :        +- *(1) Project [sr_item_sk#29, sr_reason_sk#35, sr_ticket_number#36, sr_return_quantity#37]
               :           +- *(1) Filter ((isnotnull(sr_item_sk#29) && isnotnull(sr_ticket_number#36)) && isnotnull(sr_reason_sk#35))
               :              +- *(1) FileScan parquet tpcds.store_returns[sr_item_sk#29,sr_reason_sk#35,sr_ticket_number#36,sr_return_quantity#37] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_ticket_number), IsNotNull(sr_reason_sk)], ReadSchema: struct<sr_item_sk:int,sr_reason_sk:int,sr_ticket_number:int,sr_return_quantity:int>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(2) Project [r_reason_sk#47]
                     +- *(2) Filter ((isnotnull(r_reason_desc#49) && (r_reason_desc#49 = reason 28)) && isnotnull(r_reason_sk#47))
                        +- *(2) FileScan parquet tpcds.reason[r_reason_sk#47,r_reason_desc#49] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/r..., PartitionFilters: [], PushedFilters: [IsNotNull(r_reason_desc), EqualTo(r_reason_desc,reason 28), IsNotNull(r_reason_sk)], ReadSchema: struct<r_reason_sk:int,r_reason_desc:string>
Time taken: 3.441 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 93 in stream 0 using template query93.tpl
------------------------------------------------------^^^

