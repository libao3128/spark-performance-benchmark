== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['sum('ws_ext_discount_amt) ASC NULLS FIRST], true
      +- 'Project ['sum('ws_ext_discount_amt) AS Excess_Discount_Amount#0]
         +- 'Filter (((('i_manufact_id = 350) && ('i_item_sk = 'ws_item_sk)) && (('d_date >= 2000-01-27) && ('d_date <= 'date_add(cast(2000-01-27 as date), 90)))) && (('d_date_sk = 'ws_sold_date_sk) && ('ws_ext_discount_amt > scalar-subquery#1 [])))
            :  +- 'Project [unresolvedalias((1.3 * 'avg('ws_ext_discount_amt)), None)]
            :     +- 'Filter ((('ws_item_sk = 'i_item_sk) && (('d_date >= 2000-01-27) && ('d_date <= 'date_add(cast(2000-01-27 as date), 90)))) && ('d_date_sk = 'ws_sold_date_sk))
            :        +- 'Join Inner
            :           :- 'UnresolvedRelation `web_sales`
            :           +- 'UnresolvedRelation `date_dim`
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation `web_sales`
               :  +- 'UnresolvedRelation `item`
               +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
Excess_Discount_Amount: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [Excess_Discount_Amount#0]
      +- Sort [Excess_Discount_Amount#0 ASC NULLS FIRST], true
         +- Aggregate [sum(ws_ext_discount_amt#26) AS Excess_Discount_Amount#0]
            +- Filter ((((i_manufact_id#51 = 350) && (i_item_sk#38 = ws_item_sk#7)) && ((d_date#62 >= 2000-01-27) && (d_date#62 <= cast(date_add(cast(2000-01-27 as date), 90) as string)))) && ((d_date_sk#60 = ws_sold_date_sk#4) && (ws_ext_discount_amt#26 > scalar-subquery#1 [i_item_sk#38])))
               :  +- Aggregate [(cast(1.3 as double) * avg(ws_ext_discount_amt#26)) AS (CAST(1.3 AS DOUBLE) * avg(ws_ext_discount_amt))#89]
               :     +- Filter (((ws_item_sk#7 = outer(i_item_sk#38)) && ((d_date#62 >= 2000-01-27) && (d_date#62 <= cast(date_add(cast(2000-01-27 as date), 90) as string)))) && (d_date_sk#60 = ws_sold_date_sk#4))
               :        +- Join Inner
               :           :- SubqueryAlias `tpcds`.`web_sales`
               :           :  +- Relation[ws_sold_date_sk#4,ws_sold_time_sk#5,ws_ship_date_sk#6,ws_item_sk#7,ws_bill_customer_sk#8,ws_bill_cdemo_sk#9,ws_bill_hdemo_sk#10,ws_bill_addr_sk#11,ws_ship_customer_sk#12,ws_ship_cdemo_sk#13,ws_ship_hdemo_sk#14,ws_ship_addr_sk#15,ws_web_page_sk#16,ws_web_site_sk#17,ws_ship_mode_sk#18,ws_warehouse_sk#19,ws_promo_sk#20,ws_order_number#21,ws_quantity#22,ws_wholesale_cost#23,ws_list_price#24,ws_sales_price#25,ws_ext_discount_amt#26,ws_ext_sales_price#27,... 10 more fields] parquet
               :           +- SubqueryAlias `tpcds`.`date_dim`
               :              +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias `tpcds`.`web_sales`
                  :  :  +- Relation[ws_sold_date_sk#4,ws_sold_time_sk#5,ws_ship_date_sk#6,ws_item_sk#7,ws_bill_customer_sk#8,ws_bill_cdemo_sk#9,ws_bill_hdemo_sk#10,ws_bill_addr_sk#11,ws_ship_customer_sk#12,ws_ship_cdemo_sk#13,ws_ship_hdemo_sk#14,ws_ship_addr_sk#15,ws_web_page_sk#16,ws_web_site_sk#17,ws_ship_mode_sk#18,ws_warehouse_sk#19,ws_promo_sk#20,ws_order_number#21,ws_quantity#22,ws_wholesale_cost#23,ws_list_price#24,ws_sales_price#25,ws_ext_discount_amt#26,ws_ext_sales_price#27,... 10 more fields] parquet
                  :  +- SubqueryAlias `tpcds`.`item`
                  :     +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
                  +- SubqueryAlias `tpcds`.`date_dim`
                     +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [Excess_Discount_Amount#0 ASC NULLS FIRST], true
      +- Aggregate [sum(ws_ext_discount_amt#26) AS Excess_Discount_Amount#0]
         +- Project [ws_ext_discount_amt#26]
            +- Join Inner, (d_date_sk#60 = ws_sold_date_sk#4)
               :- Project [ws_sold_date_sk#4, ws_ext_discount_amt#26]
               :  +- Join Inner, ((ws_ext_discount_amt#26 > (CAST(1.3 AS DOUBLE) * avg(ws_ext_discount_amt))#89) && (ws_item_sk#7#93 = i_item_sk#38))
               :     :- Project [ws_sold_date_sk#4, ws_ext_discount_amt#26, i_item_sk#38]
               :     :  +- Join Inner, (i_item_sk#38 = ws_item_sk#7)
               :     :     :- Project [ws_sold_date_sk#4, ws_item_sk#7, ws_ext_discount_amt#26]
               :     :     :  +- Filter ((isnotnull(ws_item_sk#7) && isnotnull(ws_ext_discount_amt#26)) && isnotnull(ws_sold_date_sk#4))
               :     :     :     +- Relation[ws_sold_date_sk#4,ws_sold_time_sk#5,ws_ship_date_sk#6,ws_item_sk#7,ws_bill_customer_sk#8,ws_bill_cdemo_sk#9,ws_bill_hdemo_sk#10,ws_bill_addr_sk#11,ws_ship_customer_sk#12,ws_ship_cdemo_sk#13,ws_ship_hdemo_sk#14,ws_ship_addr_sk#15,ws_web_page_sk#16,ws_web_site_sk#17,ws_ship_mode_sk#18,ws_warehouse_sk#19,ws_promo_sk#20,ws_order_number#21,ws_quantity#22,ws_wholesale_cost#23,ws_list_price#24,ws_sales_price#25,ws_ext_discount_amt#26,ws_ext_sales_price#27,... 10 more fields] parquet
               :     :     +- Project [i_item_sk#38]
               :     :        +- Filter ((isnotnull(i_manufact_id#51) && (i_manufact_id#51 = 350)) && isnotnull(i_item_sk#38))
               :     :           +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
               :     +- Filter isnotnull((CAST(1.3 AS DOUBLE) * avg(ws_ext_discount_amt))#89)
               :        +- Aggregate [ws_item_sk#7], [(1.3 * avg(ws_ext_discount_amt#26)) AS (CAST(1.3 AS DOUBLE) * avg(ws_ext_discount_amt))#89, ws_item_sk#7 AS ws_item_sk#7#93]
               :           +- Project [ws_item_sk#7, ws_ext_discount_amt#26]
               :              +- Join Inner, (d_date_sk#60 = ws_sold_date_sk#4)
               :                 :- Project [ws_sold_date_sk#4, ws_item_sk#7, ws_ext_discount_amt#26]
               :                 :  +- Filter (isnotnull(ws_sold_date_sk#4) && isnotnull(ws_item_sk#7))
               :                 :     +- Relation[ws_sold_date_sk#4,ws_sold_time_sk#5,ws_ship_date_sk#6,ws_item_sk#7,ws_bill_customer_sk#8,ws_bill_cdemo_sk#9,ws_bill_hdemo_sk#10,ws_bill_addr_sk#11,ws_ship_customer_sk#12,ws_ship_cdemo_sk#13,ws_ship_hdemo_sk#14,ws_ship_addr_sk#15,ws_web_page_sk#16,ws_web_site_sk#17,ws_ship_mode_sk#18,ws_warehouse_sk#19,ws_promo_sk#20,ws_order_number#21,ws_quantity#22,ws_wholesale_cost#23,ws_list_price#24,ws_sales_price#25,ws_ext_discount_amt#26,ws_ext_sales_price#27,... 10 more fields] parquet
               :                 +- Project [d_date_sk#60]
               :                    +- Filter (((isnotnull(d_date#62) && (d_date#62 >= 2000-01-27)) && (d_date#62 <= 2000-04-26)) && isnotnull(d_date_sk#60))
               :                       +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
               +- Project [d_date_sk#60]
                  +- Filter (((isnotnull(d_date#62) && (d_date#62 >= 2000-01-27)) && (d_date#62 <= 2000-04-26)) && isnotnull(d_date_sk#60))
                     +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[Excess_Discount_Amount#0 ASC NULLS FIRST], output=[Excess_Discount_Amount#0])
+- *(10) HashAggregate(keys=[], functions=[sum(ws_ext_discount_amt#26)], output=[Excess_Discount_Amount#0])
   +- Exchange SinglePartition
      +- *(9) HashAggregate(keys=[], functions=[partial_sum(ws_ext_discount_amt#26)], output=[sum#95])
         +- *(9) Project [ws_ext_discount_amt#26]
            +- *(9) BroadcastHashJoin [ws_sold_date_sk#4], [d_date_sk#60], Inner, BuildRight
               :- *(9) Project [ws_sold_date_sk#4, ws_ext_discount_amt#26]
               :  +- *(9) SortMergeJoin [i_item_sk#38], [ws_item_sk#7#93], Inner, (ws_ext_discount_amt#26 > (CAST(1.3 AS DOUBLE) * avg(ws_ext_discount_amt))#89)
               :     :- *(3) Sort [i_item_sk#38 ASC NULLS FIRST], false, 0
               :     :  +- Exchange hashpartitioning(i_item_sk#38, 200)
               :     :     +- *(2) Project [ws_sold_date_sk#4, ws_ext_discount_amt#26, i_item_sk#38]
               :     :        +- *(2) BroadcastHashJoin [ws_item_sk#7], [i_item_sk#38], Inner, BuildRight
               :     :           :- *(2) Project [ws_sold_date_sk#4, ws_item_sk#7, ws_ext_discount_amt#26]
               :     :           :  +- *(2) Filter ((isnotnull(ws_item_sk#7) && isnotnull(ws_ext_discount_amt#26)) && isnotnull(ws_sold_date_sk#4))
               :     :           :     +- *(2) FileScan parquet tpcds.web_sales[ws_sold_date_sk#4,ws_item_sk#7,ws_ext_discount_amt#26] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_ext_discount_amt), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_ext_discount_amt:double>
               :     :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :              +- *(1) Project [i_item_sk#38]
               :     :                 +- *(1) Filter ((isnotnull(i_manufact_id#51) && (i_manufact_id#51 = 350)) && isnotnull(i_item_sk#38))
               :     :                    +- *(1) FileScan parquet tpcds.item[i_item_sk#38,i_manufact_id#51] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manufact_id), EqualTo(i_manufact_id,350), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_manufact_id:int>
               :     +- *(7) Sort [ws_item_sk#7#93 ASC NULLS FIRST], false, 0
               :        +- Exchange hashpartitioning(ws_item_sk#7#93, 200)
               :           +- *(6) Filter isnotnull((CAST(1.3 AS DOUBLE) * avg(ws_ext_discount_amt))#89)
               :              +- *(6) HashAggregate(keys=[ws_item_sk#7], functions=[avg(ws_ext_discount_amt#26)], output=[(CAST(1.3 AS DOUBLE) * avg(ws_ext_discount_amt))#89, ws_item_sk#7#93])
               :                 +- Exchange hashpartitioning(ws_item_sk#7, 200)
               :                    +- *(5) HashAggregate(keys=[ws_item_sk#7], functions=[partial_avg(ws_ext_discount_amt#26)], output=[ws_item_sk#7, sum#98, count#99L])
               :                       +- *(5) Project [ws_item_sk#7, ws_ext_discount_amt#26]
               :                          +- *(5) BroadcastHashJoin [ws_sold_date_sk#4], [d_date_sk#60], Inner, BuildRight
               :                             :- *(5) Project [ws_sold_date_sk#4, ws_item_sk#7, ws_ext_discount_amt#26]
               :                             :  +- *(5) Filter (isnotnull(ws_sold_date_sk#4) && isnotnull(ws_item_sk#7))
               :                             :     +- *(5) FileScan parquet tpcds.web_sales[ws_sold_date_sk#4,ws_item_sk#7,ws_ext_discount_amt#26] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_item_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_ext_discount_amt:double>
               :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                                +- *(4) Project [d_date_sk#60]
               :                                   +- *(4) Filter (((isnotnull(d_date#62) && (d_date#62 >= 2000-01-27)) && (d_date#62 <= 2000-04-26)) && isnotnull(d_date_sk#60))
               :                                      +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#60,d_date#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-01-27), LessThanOrEqual(d_date,2000-04-26), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
               +- ReusedExchange [d_date_sk#60], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 3.964 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 92 in stream 0 using template query92.tpl
------------------------------------------------------^^^

