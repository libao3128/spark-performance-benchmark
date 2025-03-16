== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Project ['sum('cs_ext_discount_amt) AS excess_discount_amount#0]
      +- 'Filter (((('i_manufact_id = 977) && ('i_item_sk = 'cs_item_sk)) && (('d_date >= 2000-01-27) && ('d_date <= 'date_add(cast(2000-01-27 as date), 90)))) && (('d_date_sk = 'cs_sold_date_sk) && ('cs_ext_discount_amt > scalar-subquery#1 [])))
         :  +- 'Project [unresolvedalias((1.3 * 'avg('cs_ext_discount_amt)), None)]
         :     +- 'Filter ((('cs_item_sk = 'i_item_sk) && (('d_date >= 2000-01-27) && ('d_date <= 'date_add(cast(2000-01-27 as date), 90)))) && ('d_date_sk = 'cs_sold_date_sk))
         :        +- 'Join Inner
         :           :- 'UnresolvedRelation `catalog_sales`
         :           +- 'UnresolvedRelation `date_dim`
         +- 'Join Inner
            :- 'Join Inner
            :  :- 'UnresolvedRelation `catalog_sales`
            :  +- 'UnresolvedRelation `item`
            +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
excess_discount_amount: double
GlobalLimit 100
+- LocalLimit 100
   +- Aggregate [sum(cs_ext_discount_amt#26) AS excess_discount_amount#0]
      +- Filter ((((i_manufact_id#51 = 977) && (i_item_sk#38 = cs_item_sk#19)) && ((d_date#62 >= 2000-01-27) && (d_date#62 <= cast(date_add(cast(2000-01-27 as date), 90) as string)))) && ((d_date_sk#60 = cs_sold_date_sk#4) && (cs_ext_discount_amt#26 > scalar-subquery#1 [i_item_sk#38])))
         :  +- Aggregate [(cast(1.3 as double) * avg(cs_ext_discount_amt#26)) AS (CAST(1.3 AS DOUBLE) * avg(cs_ext_discount_amt))#89]
         :     +- Filter (((cs_item_sk#19 = outer(i_item_sk#38)) && ((d_date#62 >= 2000-01-27) && (d_date#62 <= cast(date_add(cast(2000-01-27 as date), 90) as string)))) && (d_date_sk#60 = cs_sold_date_sk#4))
         :        +- Join Inner
         :           :- SubqueryAlias `tpcds`.`catalog_sales`
         :           :  +- Relation[cs_sold_date_sk#4,cs_sold_time_sk#5,cs_ship_date_sk#6,cs_bill_customer_sk#7,cs_bill_cdemo_sk#8,cs_bill_hdemo_sk#9,cs_bill_addr_sk#10,cs_ship_customer_sk#11,cs_ship_cdemo_sk#12,cs_ship_hdemo_sk#13,cs_ship_addr_sk#14,cs_call_center_sk#15,cs_catalog_page_sk#16,cs_ship_mode_sk#17,cs_warehouse_sk#18,cs_item_sk#19,cs_promo_sk#20,cs_order_number#21,cs_quantity#22,cs_wholesale_cost#23,cs_list_price#24,cs_sales_price#25,cs_ext_discount_amt#26,cs_ext_sales_price#27,... 10 more fields] parquet
         :           +- SubqueryAlias `tpcds`.`date_dim`
         :              +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
         +- Join Inner
            :- Join Inner
            :  :- SubqueryAlias `tpcds`.`catalog_sales`
            :  :  +- Relation[cs_sold_date_sk#4,cs_sold_time_sk#5,cs_ship_date_sk#6,cs_bill_customer_sk#7,cs_bill_cdemo_sk#8,cs_bill_hdemo_sk#9,cs_bill_addr_sk#10,cs_ship_customer_sk#11,cs_ship_cdemo_sk#12,cs_ship_hdemo_sk#13,cs_ship_addr_sk#14,cs_call_center_sk#15,cs_catalog_page_sk#16,cs_ship_mode_sk#17,cs_warehouse_sk#18,cs_item_sk#19,cs_promo_sk#20,cs_order_number#21,cs_quantity#22,cs_wholesale_cost#23,cs_list_price#24,cs_sales_price#25,cs_ext_discount_amt#26,cs_ext_sales_price#27,... 10 more fields] parquet
            :  +- SubqueryAlias `tpcds`.`item`
            :     +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
            +- SubqueryAlias `tpcds`.`date_dim`
               +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Aggregate [sum(cs_ext_discount_amt#26) AS excess_discount_amount#0]
      +- Project [cs_ext_discount_amt#26]
         +- Join Inner, (d_date_sk#60 = cs_sold_date_sk#4)
            :- Project [cs_sold_date_sk#4, cs_ext_discount_amt#26]
            :  +- Join Inner, ((cs_ext_discount_amt#26 > (CAST(1.3 AS DOUBLE) * avg(cs_ext_discount_amt))#89) && (cs_item_sk#19#91 = i_item_sk#38))
            :     :- Project [cs_sold_date_sk#4, cs_ext_discount_amt#26, i_item_sk#38]
            :     :  +- Join Inner, (i_item_sk#38 = cs_item_sk#19)
            :     :     :- Project [cs_sold_date_sk#4, cs_item_sk#19, cs_ext_discount_amt#26]
            :     :     :  +- Filter ((isnotnull(cs_item_sk#19) && isnotnull(cs_ext_discount_amt#26)) && isnotnull(cs_sold_date_sk#4))
            :     :     :     +- Relation[cs_sold_date_sk#4,cs_sold_time_sk#5,cs_ship_date_sk#6,cs_bill_customer_sk#7,cs_bill_cdemo_sk#8,cs_bill_hdemo_sk#9,cs_bill_addr_sk#10,cs_ship_customer_sk#11,cs_ship_cdemo_sk#12,cs_ship_hdemo_sk#13,cs_ship_addr_sk#14,cs_call_center_sk#15,cs_catalog_page_sk#16,cs_ship_mode_sk#17,cs_warehouse_sk#18,cs_item_sk#19,cs_promo_sk#20,cs_order_number#21,cs_quantity#22,cs_wholesale_cost#23,cs_list_price#24,cs_sales_price#25,cs_ext_discount_amt#26,cs_ext_sales_price#27,... 10 more fields] parquet
            :     :     +- Project [i_item_sk#38]
            :     :        +- Filter ((isnotnull(i_manufact_id#51) && (i_manufact_id#51 = 977)) && isnotnull(i_item_sk#38))
            :     :           +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
            :     +- Filter isnotnull((CAST(1.3 AS DOUBLE) * avg(cs_ext_discount_amt))#89)
            :        +- Aggregate [cs_item_sk#19], [(1.3 * avg(cs_ext_discount_amt#26)) AS (CAST(1.3 AS DOUBLE) * avg(cs_ext_discount_amt))#89, cs_item_sk#19 AS cs_item_sk#19#91]
            :           +- Project [cs_item_sk#19, cs_ext_discount_amt#26]
            :              +- Join Inner, (d_date_sk#60 = cs_sold_date_sk#4)
            :                 :- Project [cs_sold_date_sk#4, cs_item_sk#19, cs_ext_discount_amt#26]
            :                 :  +- Filter (isnotnull(cs_sold_date_sk#4) && isnotnull(cs_item_sk#19))
            :                 :     +- Relation[cs_sold_date_sk#4,cs_sold_time_sk#5,cs_ship_date_sk#6,cs_bill_customer_sk#7,cs_bill_cdemo_sk#8,cs_bill_hdemo_sk#9,cs_bill_addr_sk#10,cs_ship_customer_sk#11,cs_ship_cdemo_sk#12,cs_ship_hdemo_sk#13,cs_ship_addr_sk#14,cs_call_center_sk#15,cs_catalog_page_sk#16,cs_ship_mode_sk#17,cs_warehouse_sk#18,cs_item_sk#19,cs_promo_sk#20,cs_order_number#21,cs_quantity#22,cs_wholesale_cost#23,cs_list_price#24,cs_sales_price#25,cs_ext_discount_amt#26,cs_ext_sales_price#27,... 10 more fields] parquet
            :                 +- Project [d_date_sk#60]
            :                    +- Filter (((isnotnull(d_date#62) && (d_date#62 >= 2000-01-27)) && (d_date#62 <= 2000-04-26)) && isnotnull(d_date_sk#60))
            :                       +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
            +- Project [d_date_sk#60]
               +- Filter (((isnotnull(d_date#62) && (d_date#62 >= 2000-01-27)) && (d_date#62 <= 2000-04-26)) && isnotnull(d_date_sk#60))
                  +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet

== Physical Plan ==
CollectLimit 100
+- *(10) HashAggregate(keys=[], functions=[sum(cs_ext_discount_amt#26)], output=[excess_discount_amount#0])
   +- Exchange SinglePartition
      +- *(9) HashAggregate(keys=[], functions=[partial_sum(cs_ext_discount_amt#26)], output=[sum#93])
         +- *(9) Project [cs_ext_discount_amt#26]
            +- *(9) BroadcastHashJoin [cs_sold_date_sk#4], [d_date_sk#60], Inner, BuildRight
               :- *(9) Project [cs_sold_date_sk#4, cs_ext_discount_amt#26]
               :  +- *(9) SortMergeJoin [i_item_sk#38], [cs_item_sk#19#91], Inner, (cs_ext_discount_amt#26 > (CAST(1.3 AS DOUBLE) * avg(cs_ext_discount_amt))#89)
               :     :- *(3) Sort [i_item_sk#38 ASC NULLS FIRST], false, 0
               :     :  +- Exchange hashpartitioning(i_item_sk#38, 200)
               :     :     +- *(2) Project [cs_sold_date_sk#4, cs_ext_discount_amt#26, i_item_sk#38]
               :     :        +- *(2) BroadcastHashJoin [cs_item_sk#19], [i_item_sk#38], Inner, BuildRight
               :     :           :- *(2) Project [cs_sold_date_sk#4, cs_item_sk#19, cs_ext_discount_amt#26]
               :     :           :  +- *(2) Filter ((isnotnull(cs_item_sk#19) && isnotnull(cs_ext_discount_amt#26)) && isnotnull(cs_sold_date_sk#4))
               :     :           :     +- *(2) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#4,cs_item_sk#19,cs_ext_discount_amt#26] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_ext_discount_amt), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_ext_discount_amt:double>
               :     :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :              +- *(1) Project [i_item_sk#38]
               :     :                 +- *(1) Filter ((isnotnull(i_manufact_id#51) && (i_manufact_id#51 = 977)) && isnotnull(i_item_sk#38))
               :     :                    +- *(1) FileScan parquet tpcds.item[i_item_sk#38,i_manufact_id#51] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manufact_id), EqualTo(i_manufact_id,977), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_manufact_id:int>
               :     +- *(7) Sort [cs_item_sk#19#91 ASC NULLS FIRST], false, 0
               :        +- Exchange hashpartitioning(cs_item_sk#19#91, 200)
               :           +- *(6) Filter isnotnull((CAST(1.3 AS DOUBLE) * avg(cs_ext_discount_amt))#89)
               :              +- *(6) HashAggregate(keys=[cs_item_sk#19], functions=[avg(cs_ext_discount_amt#26)], output=[(CAST(1.3 AS DOUBLE) * avg(cs_ext_discount_amt))#89, cs_item_sk#19#91])
               :                 +- Exchange hashpartitioning(cs_item_sk#19, 200)
               :                    +- *(5) HashAggregate(keys=[cs_item_sk#19], functions=[partial_avg(cs_ext_discount_amt#26)], output=[cs_item_sk#19, sum#96, count#97L])
               :                       +- *(5) Project [cs_item_sk#19, cs_ext_discount_amt#26]
               :                          +- *(5) BroadcastHashJoin [cs_sold_date_sk#4], [d_date_sk#60], Inner, BuildRight
               :                             :- *(5) Project [cs_sold_date_sk#4, cs_item_sk#19, cs_ext_discount_amt#26]
               :                             :  +- *(5) Filter (isnotnull(cs_sold_date_sk#4) && isnotnull(cs_item_sk#19))
               :                             :     +- *(5) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#4,cs_item_sk#19,cs_ext_discount_amt#26] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_item_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_ext_discount_amt:double>
               :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                                +- *(4) Project [d_date_sk#60]
               :                                   +- *(4) Filter (((isnotnull(d_date#62) && (d_date#62 >= 2000-01-27)) && (d_date#62 <= 2000-04-26)) && isnotnull(d_date_sk#60))
               :                                      +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#60,d_date#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-01-27), LessThanOrEqual(d_date,2000-04-26), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
               +- ReusedExchange [d_date_sk#60], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 4.279 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 32 in stream 0 using template query32.tpl
------------------------------------------------------^^^

