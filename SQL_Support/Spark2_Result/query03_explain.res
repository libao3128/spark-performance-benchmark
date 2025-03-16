== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['dt.d_year ASC NULLS FIRST, 'sum_agg DESC NULLS LAST, 'brand_id ASC NULLS FIRST], true
      +- 'Aggregate ['dt.d_year, 'item.i_brand, 'item.i_brand_id], ['dt.d_year, 'item.i_brand_id AS brand_id#0, 'item.i_brand AS brand#1, 'sum('ss_ext_sales_price) AS sum_agg#2]
         +- 'Filter ((('dt.d_date_sk = 'store_sales.ss_sold_date_sk) && ('store_sales.ss_item_sk = 'item.i_item_sk)) && (('item.i_manufact_id = 128) && ('dt.d_moy = 11)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias `dt`
               :  :  +- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `store_sales`
               +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
d_year: int, brand_id: int, brand: string, sum_agg: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [d_year#11 ASC NULLS FIRST, sum_agg#2 DESC NULLS LAST, brand_id#0 ASC NULLS FIRST], true
      +- Aggregate [d_year#11, i_brand#64, i_brand_id#63], [d_year#11, i_brand_id#63 AS brand_id#0, i_brand#64 AS brand#1, sum(ss_ext_sales_price#48) AS sum_agg#2]
         +- Filter (((d_date_sk#5 = ss_sold_date_sk#33) && (ss_item_sk#35 = i_item_sk#56)) && ((i_manufact_id#69 = 128) && (d_moy#13 = 11)))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `dt`
               :  :  +- SubqueryAlias `tpcds`.`date_dim`
               :  :     +- Relation[d_date_sk#5,d_date_id#6,d_date#7,d_month_seq#8,d_week_seq#9,d_quarter_seq#10,d_year#11,d_dow#12,d_moy#13,d_dom#14,d_qoy#15,d_fy_year#16,d_fy_quarter_seq#17,d_fy_week_seq#18,d_day_name#19,d_quarter_name#20,d_holiday#21,d_weekend#22,d_following_holiday#23,d_first_dom#24,d_last_dom#25,d_same_day_ly#26,d_same_day_lq#27,d_current_day#28,... 4 more fields] parquet
               :  +- SubqueryAlias `tpcds`.`store_sales`
               :     +- Relation[ss_sold_date_sk#33,ss_sold_time_sk#34,ss_item_sk#35,ss_customer_sk#36,ss_cdemo_sk#37,ss_hdemo_sk#38,ss_addr_sk#39,ss_store_sk#40,ss_promo_sk#41,ss_ticket_number#42,ss_quantity#43,ss_wholesale_cost#44,ss_list_price#45,ss_sales_price#46,ss_ext_discount_amt#47,ss_ext_sales_price#48,ss_ext_wholesale_cost#49,ss_ext_list_price#50,ss_ext_tax#51,ss_coupon_amt#52,ss_net_paid#53,ss_net_paid_inc_tax#54,ss_net_profit#55] parquet
               +- SubqueryAlias `tpcds`.`item`
                  +- Relation[i_item_sk#56,i_item_id#57,i_rec_start_date#58,i_rec_end_date#59,i_item_desc#60,i_current_price#61,i_wholesale_cost#62,i_brand_id#63,i_brand#64,i_class_id#65,i_class#66,i_category_id#67,i_category#68,i_manufact_id#69,i_manufact#70,i_size#71,i_formulation#72,i_color#73,i_units#74,i_container#75,i_manager_id#76,i_product_name#77] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [d_year#11 ASC NULLS FIRST, sum_agg#2 DESC NULLS LAST, brand_id#0 ASC NULLS FIRST], true
      +- Aggregate [d_year#11, i_brand#64, i_brand_id#63], [d_year#11, i_brand_id#63 AS brand_id#0, i_brand#64 AS brand#1, sum(ss_ext_sales_price#48) AS sum_agg#2]
         +- Project [d_year#11, ss_ext_sales_price#48, i_brand_id#63, i_brand#64]
            +- Join Inner, (ss_item_sk#35 = i_item_sk#56)
               :- Project [d_year#11, ss_item_sk#35, ss_ext_sales_price#48]
               :  +- Join Inner, (d_date_sk#5 = ss_sold_date_sk#33)
               :     :- Project [d_date_sk#5, d_year#11]
               :     :  +- Filter ((isnotnull(d_moy#13) && (d_moy#13 = 11)) && isnotnull(d_date_sk#5))
               :     :     +- Relation[d_date_sk#5,d_date_id#6,d_date#7,d_month_seq#8,d_week_seq#9,d_quarter_seq#10,d_year#11,d_dow#12,d_moy#13,d_dom#14,d_qoy#15,d_fy_year#16,d_fy_quarter_seq#17,d_fy_week_seq#18,d_day_name#19,d_quarter_name#20,d_holiday#21,d_weekend#22,d_following_holiday#23,d_first_dom#24,d_last_dom#25,d_same_day_ly#26,d_same_day_lq#27,d_current_day#28,... 4 more fields] parquet
               :     +- Project [ss_sold_date_sk#33, ss_item_sk#35, ss_ext_sales_price#48]
               :        +- Filter (isnotnull(ss_sold_date_sk#33) && isnotnull(ss_item_sk#35))
               :           +- Relation[ss_sold_date_sk#33,ss_sold_time_sk#34,ss_item_sk#35,ss_customer_sk#36,ss_cdemo_sk#37,ss_hdemo_sk#38,ss_addr_sk#39,ss_store_sk#40,ss_promo_sk#41,ss_ticket_number#42,ss_quantity#43,ss_wholesale_cost#44,ss_list_price#45,ss_sales_price#46,ss_ext_discount_amt#47,ss_ext_sales_price#48,ss_ext_wholesale_cost#49,ss_ext_list_price#50,ss_ext_tax#51,ss_coupon_amt#52,ss_net_paid#53,ss_net_paid_inc_tax#54,ss_net_profit#55] parquet
               +- Project [i_item_sk#56, i_brand_id#63, i_brand#64]
                  +- Filter ((isnotnull(i_manufact_id#69) && (i_manufact_id#69 = 128)) && isnotnull(i_item_sk#56))
                     +- Relation[i_item_sk#56,i_item_id#57,i_rec_start_date#58,i_rec_end_date#59,i_item_desc#60,i_current_price#61,i_wholesale_cost#62,i_brand_id#63,i_brand#64,i_class_id#65,i_class#66,i_category_id#67,i_category#68,i_manufact_id#69,i_manufact#70,i_size#71,i_formulation#72,i_color#73,i_units#74,i_container#75,i_manager_id#76,i_product_name#77] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[d_year#11 ASC NULLS FIRST,sum_agg#2 DESC NULLS LAST,brand_id#0 ASC NULLS FIRST], output=[d_year#11,brand_id#0,brand#1,sum_agg#2])
+- *(4) HashAggregate(keys=[d_year#11, i_brand#64, i_brand_id#63], functions=[sum(ss_ext_sales_price#48)], output=[d_year#11, brand_id#0, brand#1, sum_agg#2])
   +- Exchange hashpartitioning(d_year#11, i_brand#64, i_brand_id#63, 200)
      +- *(3) HashAggregate(keys=[d_year#11, i_brand#64, i_brand_id#63], functions=[partial_sum(ss_ext_sales_price#48)], output=[d_year#11, i_brand#64, i_brand_id#63, sum#83])
         +- *(3) Project [d_year#11, ss_ext_sales_price#48, i_brand_id#63, i_brand#64]
            +- *(3) BroadcastHashJoin [ss_item_sk#35], [i_item_sk#56], Inner, BuildRight
               :- *(3) Project [d_year#11, ss_item_sk#35, ss_ext_sales_price#48]
               :  +- *(3) BroadcastHashJoin [d_date_sk#5], [ss_sold_date_sk#33], Inner, BuildLeft
               :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :  +- *(1) Project [d_date_sk#5, d_year#11]
               :     :     +- *(1) Filter ((isnotnull(d_moy#13) && (d_moy#13 = 11)) && isnotnull(d_date_sk#5))
               :     :        +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#5,d_year#11,d_moy#13] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :     +- *(3) Project [ss_sold_date_sk#33, ss_item_sk#35, ss_ext_sales_price#48]
               :        +- *(3) Filter (isnotnull(ss_sold_date_sk#33) && isnotnull(ss_item_sk#35))
               :           +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#33,ss_item_sk#35,ss_ext_sales_price#48] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(2) Project [i_item_sk#56, i_brand_id#63, i_brand#64]
                     +- *(2) Filter ((isnotnull(i_manufact_id#69) && (i_manufact_id#69 = 128)) && isnotnull(i_item_sk#56))
                        +- *(2) FileScan parquet tpcds.item[i_item_sk#56,i_brand_id#63,i_brand#64,i_manufact_id#69] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manufact_id), EqualTo(i_manufact_id,128), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_brand:string,i_manufact_id:int>
Time taken: 3.592 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 52)

== SQL ==

-- end query 3 in stream 0 using template query3.tpl
----------------------------------------------------^^^

