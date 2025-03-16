== Parsed Logical Plan ==
CTE [ssci, csci]
:  :- 'SubqueryAlias `ssci`
:  :  +- 'Aggregate ['ss_customer_sk, 'ss_item_sk], ['ss_customer_sk AS customer_sk#3, 'ss_item_sk AS item_sk#4]
:  :     +- 'Filter (('ss_sold_date_sk = 'd_date_sk) && (('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11))))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation `store_sales`
:  :           +- 'UnresolvedRelation `date_dim`
:  +- 'SubqueryAlias `csci`
:     +- 'Aggregate ['cs_bill_customer_sk, 'cs_item_sk], ['cs_bill_customer_sk AS customer_sk#5, 'cs_item_sk AS item_sk#6]
:        +- 'Filter (('cs_sold_date_sk = 'd_date_sk) && (('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11))))
:           +- 'Join Inner
:              :- 'UnresolvedRelation `catalog_sales`
:              +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Project ['sum(CASE WHEN (isnotnull('ssci.customer_sk) && isnull('csci.customer_sk)) THEN 1 ELSE 0 END) AS store_only#0, 'sum(CASE WHEN (isnull('ssci.customer_sk) && isnotnull('csci.customer_sk)) THEN 1 ELSE 0 END) AS catalog_only#1, 'sum(CASE WHEN (isnotnull('ssci.customer_sk) && isnotnull('csci.customer_sk)) THEN 1 ELSE 0 END) AS store_and_catalog#2]
         +- 'Join FullOuter, (('ssci.customer_sk = 'csci.customer_sk) && ('ssci.item_sk = 'csci.item_sk))
            :- 'UnresolvedRelation `ssci`
            +- 'UnresolvedRelation `csci`

== Analyzed Logical Plan ==
store_only: bigint, catalog_only: bigint, store_and_catalog: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Aggregate [sum(cast(CASE WHEN (isnotnull(customer_sk#3) && isnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)) AS store_only#0L, sum(cast(CASE WHEN (isnull(customer_sk#3) && isnotnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)) AS catalog_only#1L, sum(cast(CASE WHEN (isnotnull(customer_sk#3) && isnotnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)) AS store_and_catalog#2L]
      +- Join FullOuter, ((customer_sk#3 = customer_sk#5) && (item_sk#4 = item_sk#6))
         :- SubqueryAlias `ssci`
         :  +- Aggregate [ss_customer_sk#12, ss_item_sk#11], [ss_customer_sk#12 AS customer_sk#3, ss_item_sk#11 AS item_sk#4]
         :     +- Filter ((ss_sold_date_sk#9 = d_date_sk#32) && ((d_month_seq#35 >= 1200) && (d_month_seq#35 <= (1200 + 11))))
         :        +- Join Inner
         :           :- SubqueryAlias `tpcds`.`store_sales`
         :           :  +- Relation[ss_sold_date_sk#9,ss_sold_time_sk#10,ss_item_sk#11,ss_customer_sk#12,ss_cdemo_sk#13,ss_hdemo_sk#14,ss_addr_sk#15,ss_store_sk#16,ss_promo_sk#17,ss_ticket_number#18,ss_quantity#19,ss_wholesale_cost#20,ss_list_price#21,ss_sales_price#22,ss_ext_discount_amt#23,ss_ext_sales_price#24,ss_ext_wholesale_cost#25,ss_ext_list_price#26,ss_ext_tax#27,ss_coupon_amt#28,ss_net_paid#29,ss_net_paid_inc_tax#30,ss_net_profit#31] parquet
         :           +- SubqueryAlias `tpcds`.`date_dim`
         :              +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
         +- SubqueryAlias `csci`
            +- Aggregate [cs_bill_customer_sk#63, cs_item_sk#75], [cs_bill_customer_sk#63 AS customer_sk#5, cs_item_sk#75 AS item_sk#6]
               +- Filter ((cs_sold_date_sk#60 = d_date_sk#32) && ((d_month_seq#35 >= 1200) && (d_month_seq#35 <= (1200 + 11))))
                  +- Join Inner
                     :- SubqueryAlias `tpcds`.`catalog_sales`
                     :  +- Relation[cs_sold_date_sk#60,cs_sold_time_sk#61,cs_ship_date_sk#62,cs_bill_customer_sk#63,cs_bill_cdemo_sk#64,cs_bill_hdemo_sk#65,cs_bill_addr_sk#66,cs_ship_customer_sk#67,cs_ship_cdemo_sk#68,cs_ship_hdemo_sk#69,cs_ship_addr_sk#70,cs_call_center_sk#71,cs_catalog_page_sk#72,cs_ship_mode_sk#73,cs_warehouse_sk#74,cs_item_sk#75,cs_promo_sk#76,cs_order_number#77,cs_quantity#78,cs_wholesale_cost#79,cs_list_price#80,cs_sales_price#81,cs_ext_discount_amt#82,cs_ext_sales_price#83,... 10 more fields] parquet
                     +- SubqueryAlias `tpcds`.`date_dim`
                        +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Aggregate [sum(cast(CASE WHEN (isnotnull(customer_sk#3) && isnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)) AS store_only#0L, sum(cast(CASE WHEN (isnull(customer_sk#3) && isnotnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)) AS catalog_only#1L, sum(cast(CASE WHEN (isnotnull(customer_sk#3) && isnotnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)) AS store_and_catalog#2L]
      +- Project [customer_sk#3, customer_sk#5]
         +- Join FullOuter, ((customer_sk#3 = customer_sk#5) && (item_sk#4 = item_sk#6))
            :- Aggregate [ss_customer_sk#12, ss_item_sk#11], [ss_customer_sk#12 AS customer_sk#3, ss_item_sk#11 AS item_sk#4]
            :  +- Project [ss_item_sk#11, ss_customer_sk#12]
            :     +- Join Inner, (ss_sold_date_sk#9 = d_date_sk#32)
            :        :- Project [ss_sold_date_sk#9, ss_item_sk#11, ss_customer_sk#12]
            :        :  +- Filter isnotnull(ss_sold_date_sk#9)
            :        :     +- Relation[ss_sold_date_sk#9,ss_sold_time_sk#10,ss_item_sk#11,ss_customer_sk#12,ss_cdemo_sk#13,ss_hdemo_sk#14,ss_addr_sk#15,ss_store_sk#16,ss_promo_sk#17,ss_ticket_number#18,ss_quantity#19,ss_wholesale_cost#20,ss_list_price#21,ss_sales_price#22,ss_ext_discount_amt#23,ss_ext_sales_price#24,ss_ext_wholesale_cost#25,ss_ext_list_price#26,ss_ext_tax#27,ss_coupon_amt#28,ss_net_paid#29,ss_net_paid_inc_tax#30,ss_net_profit#31] parquet
            :        +- Project [d_date_sk#32]
            :           +- Filter (((isnotnull(d_month_seq#35) && (d_month_seq#35 >= 1200)) && (d_month_seq#35 <= 1211)) && isnotnull(d_date_sk#32))
            :              +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
            +- Aggregate [cs_bill_customer_sk#63, cs_item_sk#75], [cs_bill_customer_sk#63 AS customer_sk#5, cs_item_sk#75 AS item_sk#6]
               +- Project [cs_bill_customer_sk#63, cs_item_sk#75]
                  +- Join Inner, (cs_sold_date_sk#60 = d_date_sk#32)
                     :- Project [cs_sold_date_sk#60, cs_bill_customer_sk#63, cs_item_sk#75]
                     :  +- Filter isnotnull(cs_sold_date_sk#60)
                     :     +- Relation[cs_sold_date_sk#60,cs_sold_time_sk#61,cs_ship_date_sk#62,cs_bill_customer_sk#63,cs_bill_cdemo_sk#64,cs_bill_hdemo_sk#65,cs_bill_addr_sk#66,cs_ship_customer_sk#67,cs_ship_cdemo_sk#68,cs_ship_hdemo_sk#69,cs_ship_addr_sk#70,cs_call_center_sk#71,cs_catalog_page_sk#72,cs_ship_mode_sk#73,cs_warehouse_sk#74,cs_item_sk#75,cs_promo_sk#76,cs_order_number#77,cs_quantity#78,cs_wholesale_cost#79,cs_list_price#80,cs_sales_price#81,cs_ext_discount_amt#82,cs_ext_sales_price#83,... 10 more fields] parquet
                     +- Project [d_date_sk#32]
                        +- Filter (((isnotnull(d_month_seq#35) && (d_month_seq#35 >= 1200)) && (d_month_seq#35 <= 1211)) && isnotnull(d_date_sk#32))
                           +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet

== Physical Plan ==
CollectLimit 100
+- *(10) HashAggregate(keys=[], functions=[sum(cast(CASE WHEN (isnotnull(customer_sk#3) && isnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (isnull(customer_sk#3) && isnotnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (isnotnull(customer_sk#3) && isnotnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint))], output=[store_only#0L, catalog_only#1L, store_and_catalog#2L])
   +- Exchange SinglePartition
      +- *(9) HashAggregate(keys=[], functions=[partial_sum(cast(CASE WHEN (isnotnull(customer_sk#3) && isnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (isnull(customer_sk#3) && isnotnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (isnotnull(customer_sk#3) && isnotnull(customer_sk#5)) THEN 1 ELSE 0 END as bigint))], output=[sum#100L, sum#101L, sum#102L])
         +- *(9) Project [customer_sk#3, customer_sk#5]
            +- SortMergeJoin [customer_sk#3, item_sk#4], [customer_sk#5, item_sk#6], FullOuter
               :- *(4) Sort [customer_sk#3 ASC NULLS FIRST, item_sk#4 ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(customer_sk#3, item_sk#4, 200)
               :     +- *(3) HashAggregate(keys=[ss_customer_sk#12, ss_item_sk#11], functions=[], output=[customer_sk#3, item_sk#4])
               :        +- Exchange hashpartitioning(ss_customer_sk#12, ss_item_sk#11, 200)
               :           +- *(2) HashAggregate(keys=[ss_customer_sk#12, ss_item_sk#11], functions=[], output=[ss_customer_sk#12, ss_item_sk#11])
               :              +- *(2) Project [ss_item_sk#11, ss_customer_sk#12]
               :                 +- *(2) BroadcastHashJoin [ss_sold_date_sk#9], [d_date_sk#32], Inner, BuildRight
               :                    :- *(2) Project [ss_sold_date_sk#9, ss_item_sk#11, ss_customer_sk#12]
               :                    :  +- *(2) Filter isnotnull(ss_sold_date_sk#9)
               :                    :     +- *(2) FileScan parquet tpcds.store_sales[ss_sold_date_sk#9,ss_item_sk#11,ss_customer_sk#12] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int>
               :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                       +- *(1) Project [d_date_sk#32]
               :                          +- *(1) Filter (((isnotnull(d_month_seq#35) && (d_month_seq#35 >= 1200)) && (d_month_seq#35 <= 1211)) && isnotnull(d_date_sk#32))
               :                             +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#32,d_month_seq#35] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
               +- *(8) Sort [customer_sk#5 ASC NULLS FIRST, item_sk#6 ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(customer_sk#5, item_sk#6, 200)
                     +- *(7) HashAggregate(keys=[cs_bill_customer_sk#63, cs_item_sk#75], functions=[], output=[customer_sk#5, item_sk#6])
                        +- Exchange hashpartitioning(cs_bill_customer_sk#63, cs_item_sk#75, 200)
                           +- *(6) HashAggregate(keys=[cs_bill_customer_sk#63, cs_item_sk#75], functions=[], output=[cs_bill_customer_sk#63, cs_item_sk#75])
                              +- *(6) Project [cs_bill_customer_sk#63, cs_item_sk#75]
                                 +- *(6) BroadcastHashJoin [cs_sold_date_sk#60], [d_date_sk#32], Inner, BuildRight
                                    :- *(6) Project [cs_sold_date_sk#60, cs_bill_customer_sk#63, cs_item_sk#75]
                                    :  +- *(6) Filter isnotnull(cs_sold_date_sk#60)
                                    :     +- *(6) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#60,cs_bill_customer_sk#63,cs_item_sk#75] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int>
                                    +- ReusedExchange [d_date_sk#32], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 3.718 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 97 in stream 0 using template query97.tpl
------------------------------------------------------^^^

