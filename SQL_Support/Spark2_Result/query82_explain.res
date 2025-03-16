== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id, 'i_item_desc, 'i_current_price], ['i_item_id, 'i_item_desc, 'i_current_price]
         +- 'Filter ((((('i_current_price >= 62) && ('i_current_price <= (62 + 30))) && ('inv_item_sk = 'i_item_sk)) && (('d_date_sk = 'inv_date_sk) && (('d_date >= cast(2000-05-25 as date)) && ('d_date <= 'date_add(cast(2000-05-25 as date), 60))))) && (('i_manufact_id IN (129,270,821,423) && (('inv_quantity_on_hand >= 100) && ('inv_quantity_on_hand <= 500))) && ('ss_item_sk = 'i_item_sk)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation `item`
               :  :  +- 'UnresolvedRelation `inventory`
               :  +- 'UnresolvedRelation `date_dim`
               +- 'UnresolvedRelation `store_sales`

== Analyzed Logical Plan ==
i_item_id: string, i_item_desc: string, i_current_price: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#3 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#3, i_item_desc#6, i_current_price#7], [i_item_id#3, i_item_desc#6, i_current_price#7]
         +- Filter (((((i_current_price#7 >= cast(62 as double)) && (i_current_price#7 <= cast((62 + 30) as double))) && (inv_item_sk#25 = i_item_sk#2)) && ((d_date_sk#28 = inv_date_sk#24) && ((d_date#30 >= cast(cast(2000-05-25 as date) as string)) && (d_date#30 <= cast(date_add(cast(2000-05-25 as date), 60) as string))))) && ((i_manufact_id#15 IN (129,270,821,423) && ((inv_quantity_on_hand#27L >= cast(100 as bigint)) && (inv_quantity_on_hand#27L <= cast(500 as bigint)))) && (ss_item_sk#58 = i_item_sk#2)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias `tpcds`.`item`
               :  :  :  +- Relation[i_item_sk#2,i_item_id#3,i_rec_start_date#4,i_rec_end_date#5,i_item_desc#6,i_current_price#7,i_wholesale_cost#8,i_brand_id#9,i_brand#10,i_class_id#11,i_class#12,i_category_id#13,i_category#14,i_manufact_id#15,i_manufact#16,i_size#17,i_formulation#18,i_color#19,i_units#20,i_container#21,i_manager_id#22,i_product_name#23] parquet
               :  :  +- SubqueryAlias `tpcds`.`inventory`
               :  :     +- Relation[inv_date_sk#24,inv_item_sk#25,inv_warehouse_sk#26,inv_quantity_on_hand#27L] parquet
               :  +- SubqueryAlias `tpcds`.`date_dim`
               :     +- Relation[d_date_sk#28,d_date_id#29,d_date#30,d_month_seq#31,d_week_seq#32,d_quarter_seq#33,d_year#34,d_dow#35,d_moy#36,d_dom#37,d_qoy#38,d_fy_year#39,d_fy_quarter_seq#40,d_fy_week_seq#41,d_day_name#42,d_quarter_name#43,d_holiday#44,d_weekend#45,d_following_holiday#46,d_first_dom#47,d_last_dom#48,d_same_day_ly#49,d_same_day_lq#50,d_current_day#51,... 4 more fields] parquet
               +- SubqueryAlias `tpcds`.`store_sales`
                  +- Relation[ss_sold_date_sk#56,ss_sold_time_sk#57,ss_item_sk#58,ss_customer_sk#59,ss_cdemo_sk#60,ss_hdemo_sk#61,ss_addr_sk#62,ss_store_sk#63,ss_promo_sk#64,ss_ticket_number#65,ss_quantity#66,ss_wholesale_cost#67,ss_list_price#68,ss_sales_price#69,ss_ext_discount_amt#70,ss_ext_sales_price#71,ss_ext_wholesale_cost#72,ss_ext_list_price#73,ss_ext_tax#74,ss_coupon_amt#75,ss_net_paid#76,ss_net_paid_inc_tax#77,ss_net_profit#78] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#3 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#3, i_item_desc#6, i_current_price#7], [i_item_id#3, i_item_desc#6, i_current_price#7]
         +- Project [i_item_id#3, i_item_desc#6, i_current_price#7]
            +- Join Inner, (ss_item_sk#58 = i_item_sk#2)
               :- Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7]
               :  +- Join Inner, (d_date_sk#28 = inv_date_sk#24)
               :     :- Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7, inv_date_sk#24]
               :     :  +- Join Inner, (inv_item_sk#25 = i_item_sk#2)
               :     :     :- Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7]
               :     :     :  +- Filter ((((isnotnull(i_current_price#7) && (i_current_price#7 >= 62.0)) && (i_current_price#7 <= 92.0)) && i_manufact_id#15 IN (129,270,821,423)) && isnotnull(i_item_sk#2))
               :     :     :     +- Relation[i_item_sk#2,i_item_id#3,i_rec_start_date#4,i_rec_end_date#5,i_item_desc#6,i_current_price#7,i_wholesale_cost#8,i_brand_id#9,i_brand#10,i_class_id#11,i_class#12,i_category_id#13,i_category#14,i_manufact_id#15,i_manufact#16,i_size#17,i_formulation#18,i_color#19,i_units#20,i_container#21,i_manager_id#22,i_product_name#23] parquet
               :     :     +- Project [inv_date_sk#24, inv_item_sk#25]
               :     :        +- Filter ((((isnotnull(inv_quantity_on_hand#27L) && (inv_quantity_on_hand#27L >= 100)) && (inv_quantity_on_hand#27L <= 500)) && isnotnull(inv_item_sk#25)) && isnotnull(inv_date_sk#24))
               :     :           +- Relation[inv_date_sk#24,inv_item_sk#25,inv_warehouse_sk#26,inv_quantity_on_hand#27L] parquet
               :     +- Project [d_date_sk#28]
               :        +- Filter (((isnotnull(d_date#30) && (d_date#30 >= 2000-05-25)) && (d_date#30 <= 2000-07-24)) && isnotnull(d_date_sk#28))
               :           +- Relation[d_date_sk#28,d_date_id#29,d_date#30,d_month_seq#31,d_week_seq#32,d_quarter_seq#33,d_year#34,d_dow#35,d_moy#36,d_dom#37,d_qoy#38,d_fy_year#39,d_fy_quarter_seq#40,d_fy_week_seq#41,d_day_name#42,d_quarter_name#43,d_holiday#44,d_weekend#45,d_following_holiday#46,d_first_dom#47,d_last_dom#48,d_same_day_ly#49,d_same_day_lq#50,d_current_day#51,... 4 more fields] parquet
               +- Project [ss_item_sk#58]
                  +- Filter isnotnull(ss_item_sk#58)
                     +- Relation[ss_sold_date_sk#56,ss_sold_time_sk#57,ss_item_sk#58,ss_customer_sk#59,ss_cdemo_sk#60,ss_hdemo_sk#61,ss_addr_sk#62,ss_store_sk#63,ss_promo_sk#64,ss_ticket_number#65,ss_quantity#66,ss_wholesale_cost#67,ss_list_price#68,ss_sales_price#69,ss_ext_discount_amt#70,ss_ext_sales_price#71,ss_ext_wholesale_cost#72,ss_ext_list_price#73,ss_ext_tax#74,ss_coupon_amt#75,ss_net_paid#76,ss_net_paid_inc_tax#77,ss_net_profit#78] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[i_item_id#3 ASC NULLS FIRST], output=[i_item_id#3,i_item_desc#6,i_current_price#7])
+- *(8) HashAggregate(keys=[i_item_id#3, i_item_desc#6, i_current_price#7], functions=[], output=[i_item_id#3, i_item_desc#6, i_current_price#7])
   +- Exchange hashpartitioning(i_item_id#3, i_item_desc#6, i_current_price#7, 200)
      +- *(7) HashAggregate(keys=[i_item_id#3, i_item_desc#6, i_current_price#7], functions=[], output=[i_item_id#3, i_item_desc#6, i_current_price#7])
         +- *(7) Project [i_item_id#3, i_item_desc#6, i_current_price#7]
            +- *(7) SortMergeJoin [i_item_sk#2], [ss_item_sk#58], Inner
               :- *(4) Sort [i_item_sk#2 ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(i_item_sk#2, 200)
               :     +- *(3) Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7]
               :        +- *(3) BroadcastHashJoin [inv_date_sk#24], [d_date_sk#28], Inner, BuildRight
               :           :- *(3) Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7, inv_date_sk#24]
               :           :  +- *(3) BroadcastHashJoin [i_item_sk#2], [inv_item_sk#25], Inner, BuildLeft
               :           :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :           :     :  +- *(1) Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7]
               :           :     :     +- *(1) Filter ((((isnotnull(i_current_price#7) && (i_current_price#7 >= 62.0)) && (i_current_price#7 <= 92.0)) && i_manufact_id#15 IN (129,270,821,423)) && isnotnull(i_item_sk#2))
               :           :     :        +- *(1) FileScan parquet tpcds.item[i_item_sk#2,i_item_id#3,i_item_desc#6,i_current_price#7,i_manufact_id#15] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThanOrEqual(i_current_price,62.0), LessThanOrEqual(i_current_..., ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string,i_current_price:double,i_manufact_id:int>
               :           :     +- *(3) Project [inv_date_sk#24, inv_item_sk#25]
               :           :        +- *(3) Filter ((((isnotnull(inv_quantity_on_hand#27L) && (inv_quantity_on_hand#27L >= 100)) && (inv_quantity_on_hand#27L <= 500)) && isnotnull(inv_item_sk#25)) && isnotnull(inv_date_sk#24))
               :           :           +- *(3) FileScan parquet tpcds.inventory[inv_date_sk#24,inv_item_sk#25,inv_quantity_on_hand#27L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_quantity_on_hand), GreaterThanOrEqual(inv_quantity_on_hand,100), LessThanOrEqual(i..., ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_quantity_on_hand:bigint>
               :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :              +- *(2) Project [d_date_sk#28]
               :                 +- *(2) Filter (((isnotnull(d_date#30) && (d_date#30 >= 2000-05-25)) && (d_date#30 <= 2000-07-24)) && isnotnull(d_date_sk#28))
               :                    +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#28,d_date#30] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-05-25), LessThanOrEqual(d_date,2000-07-24), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
               +- *(6) Sort [ss_item_sk#58 ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(ss_item_sk#58, 200)
                     +- *(5) Project [ss_item_sk#58]
                        +- *(5) Filter isnotnull(ss_item_sk#58)
                           +- *(5) FileScan parquet tpcds.store_sales[ss_item_sk#58] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk)], ReadSchema: struct<ss_item_sk:int>
Time taken: 3.602 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 82 in stream 0 using template query82.tpl
------------------------------------------------------^^^

