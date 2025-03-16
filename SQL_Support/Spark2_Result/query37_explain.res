== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id, 'i_item_desc, 'i_current_price], ['i_item_id, 'i_item_desc, 'i_current_price]
         +- 'Filter ((((('i_current_price >= 68) && ('i_current_price <= (68 + 30))) && ('inv_item_sk = 'i_item_sk)) && (('d_date_sk = 'inv_date_sk) && (('d_date >= cast(2000-02-01 as date)) && ('d_date <= 'date_add(cast(2000-02-01 as date), 60))))) && (('i_manufact_id IN (677,940,694,808) && (('inv_quantity_on_hand >= 100) && ('inv_quantity_on_hand <= 500))) && ('cs_item_sk = 'i_item_sk)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation `item`
               :  :  +- 'UnresolvedRelation `inventory`
               :  +- 'UnresolvedRelation `date_dim`
               +- 'UnresolvedRelation `catalog_sales`

== Analyzed Logical Plan ==
i_item_id: string, i_item_desc: string, i_current_price: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#3 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#3, i_item_desc#6, i_current_price#7], [i_item_id#3, i_item_desc#6, i_current_price#7]
         +- Filter (((((i_current_price#7 >= cast(68 as double)) && (i_current_price#7 <= cast((68 + 30) as double))) && (inv_item_sk#25 = i_item_sk#2)) && ((d_date_sk#28 = inv_date_sk#24) && ((d_date#30 >= cast(cast(2000-02-01 as date) as string)) && (d_date#30 <= cast(date_add(cast(2000-02-01 as date), 60) as string))))) && ((i_manufact_id#15 IN (677,940,694,808) && ((inv_quantity_on_hand#27L >= cast(100 as bigint)) && (inv_quantity_on_hand#27L <= cast(500 as bigint)))) && (cs_item_sk#71 = i_item_sk#2)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias `tpcds`.`item`
               :  :  :  +- Relation[i_item_sk#2,i_item_id#3,i_rec_start_date#4,i_rec_end_date#5,i_item_desc#6,i_current_price#7,i_wholesale_cost#8,i_brand_id#9,i_brand#10,i_class_id#11,i_class#12,i_category_id#13,i_category#14,i_manufact_id#15,i_manufact#16,i_size#17,i_formulation#18,i_color#19,i_units#20,i_container#21,i_manager_id#22,i_product_name#23] parquet
               :  :  +- SubqueryAlias `tpcds`.`inventory`
               :  :     +- Relation[inv_date_sk#24,inv_item_sk#25,inv_warehouse_sk#26,inv_quantity_on_hand#27L] parquet
               :  +- SubqueryAlias `tpcds`.`date_dim`
               :     +- Relation[d_date_sk#28,d_date_id#29,d_date#30,d_month_seq#31,d_week_seq#32,d_quarter_seq#33,d_year#34,d_dow#35,d_moy#36,d_dom#37,d_qoy#38,d_fy_year#39,d_fy_quarter_seq#40,d_fy_week_seq#41,d_day_name#42,d_quarter_name#43,d_holiday#44,d_weekend#45,d_following_holiday#46,d_first_dom#47,d_last_dom#48,d_same_day_ly#49,d_same_day_lq#50,d_current_day#51,... 4 more fields] parquet
               +- SubqueryAlias `tpcds`.`catalog_sales`
                  +- Relation[cs_sold_date_sk#56,cs_sold_time_sk#57,cs_ship_date_sk#58,cs_bill_customer_sk#59,cs_bill_cdemo_sk#60,cs_bill_hdemo_sk#61,cs_bill_addr_sk#62,cs_ship_customer_sk#63,cs_ship_cdemo_sk#64,cs_ship_hdemo_sk#65,cs_ship_addr_sk#66,cs_call_center_sk#67,cs_catalog_page_sk#68,cs_ship_mode_sk#69,cs_warehouse_sk#70,cs_item_sk#71,cs_promo_sk#72,cs_order_number#73,cs_quantity#74,cs_wholesale_cost#75,cs_list_price#76,cs_sales_price#77,cs_ext_discount_amt#78,cs_ext_sales_price#79,... 10 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#3 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#3, i_item_desc#6, i_current_price#7], [i_item_id#3, i_item_desc#6, i_current_price#7]
         +- Project [i_item_id#3, i_item_desc#6, i_current_price#7]
            +- Join Inner, (cs_item_sk#71 = i_item_sk#2)
               :- Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7]
               :  +- Join Inner, (d_date_sk#28 = inv_date_sk#24)
               :     :- Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7, inv_date_sk#24]
               :     :  +- Join Inner, (inv_item_sk#25 = i_item_sk#2)
               :     :     :- Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7]
               :     :     :  +- Filter ((((isnotnull(i_current_price#7) && (i_current_price#7 >= 68.0)) && (i_current_price#7 <= 98.0)) && i_manufact_id#15 IN (677,940,694,808)) && isnotnull(i_item_sk#2))
               :     :     :     +- Relation[i_item_sk#2,i_item_id#3,i_rec_start_date#4,i_rec_end_date#5,i_item_desc#6,i_current_price#7,i_wholesale_cost#8,i_brand_id#9,i_brand#10,i_class_id#11,i_class#12,i_category_id#13,i_category#14,i_manufact_id#15,i_manufact#16,i_size#17,i_formulation#18,i_color#19,i_units#20,i_container#21,i_manager_id#22,i_product_name#23] parquet
               :     :     +- Project [inv_date_sk#24, inv_item_sk#25]
               :     :        +- Filter ((((isnotnull(inv_quantity_on_hand#27L) && (inv_quantity_on_hand#27L >= 100)) && (inv_quantity_on_hand#27L <= 500)) && isnotnull(inv_item_sk#25)) && isnotnull(inv_date_sk#24))
               :     :           +- Relation[inv_date_sk#24,inv_item_sk#25,inv_warehouse_sk#26,inv_quantity_on_hand#27L] parquet
               :     +- Project [d_date_sk#28]
               :        +- Filter (((isnotnull(d_date#30) && (d_date#30 >= 2000-02-01)) && (d_date#30 <= 2000-04-01)) && isnotnull(d_date_sk#28))
               :           +- Relation[d_date_sk#28,d_date_id#29,d_date#30,d_month_seq#31,d_week_seq#32,d_quarter_seq#33,d_year#34,d_dow#35,d_moy#36,d_dom#37,d_qoy#38,d_fy_year#39,d_fy_quarter_seq#40,d_fy_week_seq#41,d_day_name#42,d_quarter_name#43,d_holiday#44,d_weekend#45,d_following_holiday#46,d_first_dom#47,d_last_dom#48,d_same_day_ly#49,d_same_day_lq#50,d_current_day#51,... 4 more fields] parquet
               +- Project [cs_item_sk#71]
                  +- Filter isnotnull(cs_item_sk#71)
                     +- Relation[cs_sold_date_sk#56,cs_sold_time_sk#57,cs_ship_date_sk#58,cs_bill_customer_sk#59,cs_bill_cdemo_sk#60,cs_bill_hdemo_sk#61,cs_bill_addr_sk#62,cs_ship_customer_sk#63,cs_ship_cdemo_sk#64,cs_ship_hdemo_sk#65,cs_ship_addr_sk#66,cs_call_center_sk#67,cs_catalog_page_sk#68,cs_ship_mode_sk#69,cs_warehouse_sk#70,cs_item_sk#71,cs_promo_sk#72,cs_order_number#73,cs_quantity#74,cs_wholesale_cost#75,cs_list_price#76,cs_sales_price#77,cs_ext_discount_amt#78,cs_ext_sales_price#79,... 10 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[i_item_id#3 ASC NULLS FIRST], output=[i_item_id#3,i_item_desc#6,i_current_price#7])
+- *(5) HashAggregate(keys=[i_item_id#3, i_item_desc#6, i_current_price#7], functions=[], output=[i_item_id#3, i_item_desc#6, i_current_price#7])
   +- Exchange hashpartitioning(i_item_id#3, i_item_desc#6, i_current_price#7, 200)
      +- *(4) HashAggregate(keys=[i_item_id#3, i_item_desc#6, i_current_price#7], functions=[], output=[i_item_id#3, i_item_desc#6, i_current_price#7])
         +- *(4) Project [i_item_id#3, i_item_desc#6, i_current_price#7]
            +- *(4) BroadcastHashJoin [i_item_sk#2], [cs_item_sk#71], Inner, BuildRight
               :- *(4) Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7]
               :  +- *(4) BroadcastHashJoin [inv_date_sk#24], [d_date_sk#28], Inner, BuildRight
               :     :- *(4) Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7, inv_date_sk#24]
               :     :  +- *(4) BroadcastHashJoin [i_item_sk#2], [inv_item_sk#25], Inner, BuildLeft
               :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :  +- *(1) Project [i_item_sk#2, i_item_id#3, i_item_desc#6, i_current_price#7]
               :     :     :     +- *(1) Filter ((((isnotnull(i_current_price#7) && (i_current_price#7 >= 68.0)) && (i_current_price#7 <= 98.0)) && i_manufact_id#15 IN (677,940,694,808)) && isnotnull(i_item_sk#2))
               :     :     :        +- *(1) FileScan parquet tpcds.item[i_item_sk#2,i_item_id#3,i_item_desc#6,i_current_price#7,i_manufact_id#15] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThanOrEqual(i_current_price,68.0), LessThanOrEqual(i_current_..., ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string,i_current_price:double,i_manufact_id:int>
               :     :     +- *(4) Project [inv_date_sk#24, inv_item_sk#25]
               :     :        +- *(4) Filter ((((isnotnull(inv_quantity_on_hand#27L) && (inv_quantity_on_hand#27L >= 100)) && (inv_quantity_on_hand#27L <= 500)) && isnotnull(inv_item_sk#25)) && isnotnull(inv_date_sk#24))
               :     :           +- *(4) FileScan parquet tpcds.inventory[inv_date_sk#24,inv_item_sk#25,inv_quantity_on_hand#27L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_quantity_on_hand), GreaterThanOrEqual(inv_quantity_on_hand,100), LessThanOrEqual(i..., ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_quantity_on_hand:bigint>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(2) Project [d_date_sk#28]
               :           +- *(2) Filter (((isnotnull(d_date#30) && (d_date#30 >= 2000-02-01)) && (d_date#30 <= 2000-04-01)) && isnotnull(d_date_sk#28))
               :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#28,d_date#30] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-02-01), LessThanOrEqual(d_date,2000-04-01), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(3) Project [cs_item_sk#71]
                     +- *(3) Filter isnotnull(cs_item_sk#71)
                        +- *(3) FileScan parquet tpcds.catalog_sales[cs_item_sk#71] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk)], ReadSchema: struct<cs_item_sk:int>
Time taken: 3.694 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 37 in stream 0 using template query37.tpl
------------------------------------------------------^^^

