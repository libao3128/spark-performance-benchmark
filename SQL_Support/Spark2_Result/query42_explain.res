== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['sum('ss_ext_sales_price) DESC NULLS LAST, 'dt.d_year ASC NULLS FIRST, 'item.i_category_id ASC NULLS FIRST, 'item.i_category ASC NULLS FIRST], true
      +- 'Aggregate ['dt.d_year, 'item.i_category_id, 'item.i_category], ['dt.d_year, 'item.i_category_id, 'item.i_category, unresolvedalias('sum('ss_ext_sales_price), None)]
         +- 'Filter (((('dt.d_date_sk = 'store_sales.ss_sold_date_sk) && ('store_sales.ss_item_sk = 'item.i_item_sk)) && ('item.i_manager_id = 1)) && (('dt.d_moy = 11) && ('dt.d_year = 2000)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias `dt`
               :  :  +- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `store_sales`
               +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
d_year: int, i_category_id: int, i_category: string, sum(ss_ext_sales_price): double
GlobalLimit 100
+- LocalLimit 100
   +- Project [d_year#8, i_category_id#64, i_category#65, sum(ss_ext_sales_price)#76]
      +- Sort [sum(ss_ext_sales_price)#76 DESC NULLS LAST, d_year#8 ASC NULLS FIRST, i_category_id#64 ASC NULLS FIRST, i_category#65 ASC NULLS FIRST], true
         +- Aggregate [d_year#8, i_category_id#64, i_category#65], [d_year#8, i_category_id#64, i_category#65, sum(ss_ext_sales_price#45) AS sum(ss_ext_sales_price)#76]
            +- Filter ((((d_date_sk#2 = ss_sold_date_sk#30) && (ss_item_sk#32 = i_item_sk#53)) && (i_manager_id#73 = 1)) && ((d_moy#10 = 11) && (d_year#8 = 2000)))
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias `dt`
                  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                  :  :     +- Relation[d_date_sk#2,d_date_id#3,d_date#4,d_month_seq#5,d_week_seq#6,d_quarter_seq#7,d_year#8,d_dow#9,d_moy#10,d_dom#11,d_qoy#12,d_fy_year#13,d_fy_quarter_seq#14,d_fy_week_seq#15,d_day_name#16,d_quarter_name#17,d_holiday#18,d_weekend#19,d_following_holiday#20,d_first_dom#21,d_last_dom#22,d_same_day_ly#23,d_same_day_lq#24,d_current_day#25,... 4 more fields] parquet
                  :  +- SubqueryAlias `tpcds`.`store_sales`
                  :     +- Relation[ss_sold_date_sk#30,ss_sold_time_sk#31,ss_item_sk#32,ss_customer_sk#33,ss_cdemo_sk#34,ss_hdemo_sk#35,ss_addr_sk#36,ss_store_sk#37,ss_promo_sk#38,ss_ticket_number#39,ss_quantity#40,ss_wholesale_cost#41,ss_list_price#42,ss_sales_price#43,ss_ext_discount_amt#44,ss_ext_sales_price#45,ss_ext_wholesale_cost#46,ss_ext_list_price#47,ss_ext_tax#48,ss_coupon_amt#49,ss_net_paid#50,ss_net_paid_inc_tax#51,ss_net_profit#52] parquet
                  +- SubqueryAlias `tpcds`.`item`
                     +- Relation[i_item_sk#53,i_item_id#54,i_rec_start_date#55,i_rec_end_date#56,i_item_desc#57,i_current_price#58,i_wholesale_cost#59,i_brand_id#60,i_brand#61,i_class_id#62,i_class#63,i_category_id#64,i_category#65,i_manufact_id#66,i_manufact#67,i_size#68,i_formulation#69,i_color#70,i_units#71,i_container#72,i_manager_id#73,i_product_name#74] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [sum(ss_ext_sales_price)#76 DESC NULLS LAST, d_year#8 ASC NULLS FIRST, i_category_id#64 ASC NULLS FIRST, i_category#65 ASC NULLS FIRST], true
      +- Aggregate [d_year#8, i_category_id#64, i_category#65], [d_year#8, i_category_id#64, i_category#65, sum(ss_ext_sales_price#45) AS sum(ss_ext_sales_price)#76]
         +- Project [d_year#8, ss_ext_sales_price#45, i_category_id#64, i_category#65]
            +- Join Inner, (ss_item_sk#32 = i_item_sk#53)
               :- Project [d_year#8, ss_item_sk#32, ss_ext_sales_price#45]
               :  +- Join Inner, (d_date_sk#2 = ss_sold_date_sk#30)
               :     :- Project [d_date_sk#2, d_year#8]
               :     :  +- Filter ((((isnotnull(d_moy#10) && isnotnull(d_year#8)) && (d_moy#10 = 11)) && (d_year#8 = 2000)) && isnotnull(d_date_sk#2))
               :     :     +- Relation[d_date_sk#2,d_date_id#3,d_date#4,d_month_seq#5,d_week_seq#6,d_quarter_seq#7,d_year#8,d_dow#9,d_moy#10,d_dom#11,d_qoy#12,d_fy_year#13,d_fy_quarter_seq#14,d_fy_week_seq#15,d_day_name#16,d_quarter_name#17,d_holiday#18,d_weekend#19,d_following_holiday#20,d_first_dom#21,d_last_dom#22,d_same_day_ly#23,d_same_day_lq#24,d_current_day#25,... 4 more fields] parquet
               :     +- Project [ss_sold_date_sk#30, ss_item_sk#32, ss_ext_sales_price#45]
               :        +- Filter (isnotnull(ss_sold_date_sk#30) && isnotnull(ss_item_sk#32))
               :           +- Relation[ss_sold_date_sk#30,ss_sold_time_sk#31,ss_item_sk#32,ss_customer_sk#33,ss_cdemo_sk#34,ss_hdemo_sk#35,ss_addr_sk#36,ss_store_sk#37,ss_promo_sk#38,ss_ticket_number#39,ss_quantity#40,ss_wholesale_cost#41,ss_list_price#42,ss_sales_price#43,ss_ext_discount_amt#44,ss_ext_sales_price#45,ss_ext_wholesale_cost#46,ss_ext_list_price#47,ss_ext_tax#48,ss_coupon_amt#49,ss_net_paid#50,ss_net_paid_inc_tax#51,ss_net_profit#52] parquet
               +- Project [i_item_sk#53, i_category_id#64, i_category#65]
                  +- Filter ((isnotnull(i_manager_id#73) && (i_manager_id#73 = 1)) && isnotnull(i_item_sk#53))
                     +- Relation[i_item_sk#53,i_item_id#54,i_rec_start_date#55,i_rec_end_date#56,i_item_desc#57,i_current_price#58,i_wholesale_cost#59,i_brand_id#60,i_brand#61,i_class_id#62,i_class#63,i_category_id#64,i_category#65,i_manufact_id#66,i_manufact#67,i_size#68,i_formulation#69,i_color#70,i_units#71,i_container#72,i_manager_id#73,i_product_name#74] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[sum(ss_ext_sales_price)#76 DESC NULLS LAST,d_year#8 ASC NULLS FIRST,i_category_id#64 ASC NULLS FIRST,i_category#65 ASC NULLS FIRST], output=[d_year#8,i_category_id#64,i_category#65,sum(ss_ext_sales_price)#76])
+- *(4) HashAggregate(keys=[d_year#8, i_category_id#64, i_category#65], functions=[sum(ss_ext_sales_price#45)], output=[d_year#8, i_category_id#64, i_category#65, sum(ss_ext_sales_price)#76])
   +- Exchange hashpartitioning(d_year#8, i_category_id#64, i_category#65, 200)
      +- *(3) HashAggregate(keys=[d_year#8, i_category_id#64, i_category#65], functions=[partial_sum(ss_ext_sales_price#45)], output=[d_year#8, i_category_id#64, i_category#65, sum#83])
         +- *(3) Project [d_year#8, ss_ext_sales_price#45, i_category_id#64, i_category#65]
            +- *(3) BroadcastHashJoin [ss_item_sk#32], [i_item_sk#53], Inner, BuildRight
               :- *(3) Project [d_year#8, ss_item_sk#32, ss_ext_sales_price#45]
               :  +- *(3) BroadcastHashJoin [d_date_sk#2], [ss_sold_date_sk#30], Inner, BuildLeft
               :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :  +- *(1) Project [d_date_sk#2, d_year#8]
               :     :     +- *(1) Filter ((((isnotnull(d_moy#10) && isnotnull(d_year#8)) && (d_moy#10 = 11)) && (d_year#8 = 2000)) && isnotnull(d_date_sk#2))
               :     :        +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#2,d_year#8,d_moy#10] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,11), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :     +- *(3) Project [ss_sold_date_sk#30, ss_item_sk#32, ss_ext_sales_price#45]
               :        +- *(3) Filter (isnotnull(ss_sold_date_sk#30) && isnotnull(ss_item_sk#32))
               :           +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#30,ss_item_sk#32,ss_ext_sales_price#45] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(2) Project [i_item_sk#53, i_category_id#64, i_category#65]
                     +- *(2) Filter ((isnotnull(i_manager_id#73) && (i_manager_id#73 = 1)) && isnotnull(i_item_sk#53))
                        +- *(2) FileScan parquet tpcds.item[i_item_sk#53,i_category_id#64,i_category#65,i_manager_id#73] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manager_id), EqualTo(i_manager_id,1), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_category_id:int,i_category:string,i_manager_id:int>
Time taken: 3.506 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 42 in stream 0 using template query42.tpl
------------------------------------------------------^^^

