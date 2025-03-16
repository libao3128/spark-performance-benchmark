== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['qoh ASC NULLS FIRST, 'i_product_name ASC NULLS FIRST, 'i_brand ASC NULLS FIRST, 'i_class ASC NULLS FIRST, 'i_category ASC NULLS FIRST], true
      +- 'Aggregate ['rollup('i_product_name, 'i_brand, 'i_class, 'i_category)], ['i_product_name, 'i_brand, 'i_class, 'i_category, 'avg('inv_quantity_on_hand) AS qoh#0]
         +- 'Filter ((('inv_date_sk = 'd_date_sk) && ('inv_item_sk = 'i_item_sk)) && (('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation `inventory`
               :  +- 'UnresolvedRelation `date_dim`
               +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
i_product_name: string, i_brand: string, i_class: string, i_category: string, qoh: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [qoh#0 ASC NULLS FIRST, i_product_name#63 ASC NULLS FIRST, i_brand#64 ASC NULLS FIRST, i_class#65 ASC NULLS FIRST, i_category#66 ASC NULLS FIRST], true
      +- Aggregate [i_product_name#63, i_brand#64, i_class#65, i_category#66, spark_grouping_id#58], [i_product_name#63, i_brand#64, i_class#65, i_category#66, avg(inv_quantity_on_hand#6L) AS qoh#0]
         +- Expand [List(inv_date_sk#3, inv_item_sk#4, inv_warehouse_sk#5, inv_quantity_on_hand#6L, d_date_sk#7, d_date_id#8, d_date#9, d_month_seq#10, d_week_seq#11, d_quarter_seq#12, d_year#13, d_dow#14, d_moy#15, d_dom#16, d_qoy#17, d_fy_year#18, d_fy_quarter_seq#19, d_fy_week_seq#20, d_day_name#21, d_quarter_name#22, d_holiday#23, d_weekend#24, d_following_holiday#25, d_first_dom#26, d_last_dom#27, d_same_day_ly#28, d_same_day_lq#29, d_current_day#30, d_current_week#31, d_current_month#32, d_current_quarter#33, d_current_year#34, i_item_sk#35, i_item_id#36, i_rec_start_date#37, i_rec_end_date#38, i_item_desc#39, i_current_price#40, i_wholesale_cost#41, i_brand_id#42, i_brand#43, i_class_id#44, i_class#45, i_category_id#46, i_category#47, i_manufact_id#48, i_manufact#49, i_size#50, i_formulation#51, i_color#52, i_units#53, i_container#54, i_manager_id#55, i_product_name#56, i_product_name#59, i_brand#60, i_class#61, i_category#62, 0), List(inv_date_sk#3, inv_item_sk#4, inv_warehouse_sk#5, inv_quantity_on_hand#6L, d_date_sk#7, d_date_id#8, d_date#9, d_month_seq#10, d_week_seq#11, d_quarter_seq#12, d_year#13, d_dow#14, d_moy#15, d_dom#16, d_qoy#17, d_fy_year#18, d_fy_quarter_seq#19, d_fy_week_seq#20, d_day_name#21, d_quarter_name#22, d_holiday#23, d_weekend#24, d_following_holiday#25, d_first_dom#26, d_last_dom#27, d_same_day_ly#28, d_same_day_lq#29, d_current_day#30, d_current_week#31, d_current_month#32, d_current_quarter#33, d_current_year#34, i_item_sk#35, i_item_id#36, i_rec_start_date#37, i_rec_end_date#38, i_item_desc#39, i_current_price#40, i_wholesale_cost#41, i_brand_id#42, i_brand#43, i_class_id#44, i_class#45, i_category_id#46, i_category#47, i_manufact_id#48, i_manufact#49, i_size#50, i_formulation#51, i_color#52, i_units#53, i_container#54, i_manager_id#55, i_product_name#56, i_product_name#59, i_brand#60, i_class#61, null, 1), List(inv_date_sk#3, inv_item_sk#4, inv_warehouse_sk#5, inv_quantity_on_hand#6L, d_date_sk#7, d_date_id#8, d_date#9, d_month_seq#10, d_week_seq#11, d_quarter_seq#12, d_year#13, d_dow#14, d_moy#15, d_dom#16, d_qoy#17, d_fy_year#18, d_fy_quarter_seq#19, d_fy_week_seq#20, d_day_name#21, d_quarter_name#22, d_holiday#23, d_weekend#24, d_following_holiday#25, d_first_dom#26, d_last_dom#27, d_same_day_ly#28, d_same_day_lq#29, d_current_day#30, d_current_week#31, d_current_month#32, d_current_quarter#33, d_current_year#34, i_item_sk#35, i_item_id#36, i_rec_start_date#37, i_rec_end_date#38, i_item_desc#39, i_current_price#40, i_wholesale_cost#41, i_brand_id#42, i_brand#43, i_class_id#44, i_class#45, i_category_id#46, i_category#47, i_manufact_id#48, i_manufact#49, i_size#50, i_formulation#51, i_color#52, i_units#53, i_container#54, i_manager_id#55, i_product_name#56, i_product_name#59, i_brand#60, null, null, 3), List(inv_date_sk#3, inv_item_sk#4, inv_warehouse_sk#5, inv_quantity_on_hand#6L, d_date_sk#7, d_date_id#8, d_date#9, d_month_seq#10, d_week_seq#11, d_quarter_seq#12, d_year#13, d_dow#14, d_moy#15, d_dom#16, d_qoy#17, d_fy_year#18, d_fy_quarter_seq#19, d_fy_week_seq#20, d_day_name#21, d_quarter_name#22, d_holiday#23, d_weekend#24, d_following_holiday#25, d_first_dom#26, d_last_dom#27, d_same_day_ly#28, d_same_day_lq#29, d_current_day#30, d_current_week#31, d_current_month#32, d_current_quarter#33, d_current_year#34, i_item_sk#35, i_item_id#36, i_rec_start_date#37, i_rec_end_date#38, i_item_desc#39, i_current_price#40, i_wholesale_cost#41, i_brand_id#42, i_brand#43, i_class_id#44, i_class#45, i_category_id#46, i_category#47, i_manufact_id#48, i_manufact#49, i_size#50, i_formulation#51, i_color#52, i_units#53, i_container#54, i_manager_id#55, i_product_name#56, i_product_name#59, null, null, null, 7), List(inv_date_sk#3, inv_item_sk#4, inv_warehouse_sk#5, inv_quantity_on_hand#6L, d_date_sk#7, d_date_id#8, d_date#9, d_month_seq#10, d_week_seq#11, d_quarter_seq#12, d_year#13, d_dow#14, d_moy#15, d_dom#16, d_qoy#17, d_fy_year#18, d_fy_quarter_seq#19, d_fy_week_seq#20, d_day_name#21, d_quarter_name#22, d_holiday#23, d_weekend#24, d_following_holiday#25, d_first_dom#26, d_last_dom#27, d_same_day_ly#28, d_same_day_lq#29, d_current_day#30, d_current_week#31, d_current_month#32, d_current_quarter#33, d_current_year#34, i_item_sk#35, i_item_id#36, i_rec_start_date#37, i_rec_end_date#38, i_item_desc#39, i_current_price#40, i_wholesale_cost#41, i_brand_id#42, i_brand#43, i_class_id#44, i_class#45, i_category_id#46, i_category#47, i_manufact_id#48, i_manufact#49, i_size#50, i_formulation#51, i_color#52, i_units#53, i_container#54, i_manager_id#55, i_product_name#56, null, null, null, null, 15)], [inv_date_sk#3, inv_item_sk#4, inv_warehouse_sk#5, inv_quantity_on_hand#6L, d_date_sk#7, d_date_id#8, d_date#9, d_month_seq#10, d_week_seq#11, d_quarter_seq#12, d_year#13, d_dow#14, d_moy#15, d_dom#16, d_qoy#17, d_fy_year#18, d_fy_quarter_seq#19, d_fy_week_seq#20, d_day_name#21, d_quarter_name#22, d_holiday#23, d_weekend#24, d_following_holiday#25, d_first_dom#26, ... 35 more fields]
            +- Project [inv_date_sk#3, inv_item_sk#4, inv_warehouse_sk#5, inv_quantity_on_hand#6L, d_date_sk#7, d_date_id#8, d_date#9, d_month_seq#10, d_week_seq#11, d_quarter_seq#12, d_year#13, d_dow#14, d_moy#15, d_dom#16, d_qoy#17, d_fy_year#18, d_fy_quarter_seq#19, d_fy_week_seq#20, d_day_name#21, d_quarter_name#22, d_holiday#23, d_weekend#24, d_following_holiday#25, d_first_dom#26, ... 34 more fields]
               +- Filter (((inv_date_sk#3 = d_date_sk#7) && (inv_item_sk#4 = i_item_sk#35)) && ((d_month_seq#10 >= 1200) && (d_month_seq#10 <= (1200 + 11))))
                  +- Join Inner
                     :- Join Inner
                     :  :- SubqueryAlias `tpcds`.`inventory`
                     :  :  +- Relation[inv_date_sk#3,inv_item_sk#4,inv_warehouse_sk#5,inv_quantity_on_hand#6L] parquet
                     :  +- SubqueryAlias `tpcds`.`date_dim`
                     :     +- Relation[d_date_sk#7,d_date_id#8,d_date#9,d_month_seq#10,d_week_seq#11,d_quarter_seq#12,d_year#13,d_dow#14,d_moy#15,d_dom#16,d_qoy#17,d_fy_year#18,d_fy_quarter_seq#19,d_fy_week_seq#20,d_day_name#21,d_quarter_name#22,d_holiday#23,d_weekend#24,d_following_holiday#25,d_first_dom#26,d_last_dom#27,d_same_day_ly#28,d_same_day_lq#29,d_current_day#30,... 4 more fields] parquet
                     +- SubqueryAlias `tpcds`.`item`
                        +- Relation[i_item_sk#35,i_item_id#36,i_rec_start_date#37,i_rec_end_date#38,i_item_desc#39,i_current_price#40,i_wholesale_cost#41,i_brand_id#42,i_brand#43,i_class_id#44,i_class#45,i_category_id#46,i_category#47,i_manufact_id#48,i_manufact#49,i_size#50,i_formulation#51,i_color#52,i_units#53,i_container#54,i_manager_id#55,i_product_name#56] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [qoh#0 ASC NULLS FIRST, i_product_name#63 ASC NULLS FIRST, i_brand#64 ASC NULLS FIRST, i_class#65 ASC NULLS FIRST, i_category#66 ASC NULLS FIRST], true
      +- Aggregate [i_product_name#63, i_brand#64, i_class#65, i_category#66, spark_grouping_id#58], [i_product_name#63, i_brand#64, i_class#65, i_category#66, avg(inv_quantity_on_hand#6L) AS qoh#0]
         +- Expand [List(inv_quantity_on_hand#6L, i_product_name#56, i_brand#43, i_class#45, i_category#47, 0), List(inv_quantity_on_hand#6L, i_product_name#56, i_brand#43, i_class#45, null, 1), List(inv_quantity_on_hand#6L, i_product_name#56, i_brand#43, null, null, 3), List(inv_quantity_on_hand#6L, i_product_name#56, null, null, null, 7), List(inv_quantity_on_hand#6L, null, null, null, null, 15)], [inv_quantity_on_hand#6L, i_product_name#63, i_brand#64, i_class#65, i_category#66, spark_grouping_id#58]
            +- Project [inv_quantity_on_hand#6L, i_product_name#56, i_brand#43, i_class#45, i_category#47]
               +- Join Inner, (inv_item_sk#4 = i_item_sk#35)
                  :- Project [inv_item_sk#4, inv_quantity_on_hand#6L]
                  :  +- Join Inner, (inv_date_sk#3 = d_date_sk#7)
                  :     :- Project [inv_date_sk#3, inv_item_sk#4, inv_quantity_on_hand#6L]
                  :     :  +- Filter (isnotnull(inv_date_sk#3) && isnotnull(inv_item_sk#4))
                  :     :     +- Relation[inv_date_sk#3,inv_item_sk#4,inv_warehouse_sk#5,inv_quantity_on_hand#6L] parquet
                  :     +- Project [d_date_sk#7]
                  :        +- Filter (((isnotnull(d_month_seq#10) && (d_month_seq#10 >= 1200)) && (d_month_seq#10 <= 1211)) && isnotnull(d_date_sk#7))
                  :           +- Relation[d_date_sk#7,d_date_id#8,d_date#9,d_month_seq#10,d_week_seq#11,d_quarter_seq#12,d_year#13,d_dow#14,d_moy#15,d_dom#16,d_qoy#17,d_fy_year#18,d_fy_quarter_seq#19,d_fy_week_seq#20,d_day_name#21,d_quarter_name#22,d_holiday#23,d_weekend#24,d_following_holiday#25,d_first_dom#26,d_last_dom#27,d_same_day_ly#28,d_same_day_lq#29,d_current_day#30,... 4 more fields] parquet
                  +- Project [i_item_sk#35, i_brand#43, i_class#45, i_category#47, i_product_name#56]
                     +- Filter isnotnull(i_item_sk#35)
                        +- Relation[i_item_sk#35,i_item_id#36,i_rec_start_date#37,i_rec_end_date#38,i_item_desc#39,i_current_price#40,i_wholesale_cost#41,i_brand_id#42,i_brand#43,i_class_id#44,i_class#45,i_category_id#46,i_category#47,i_manufact_id#48,i_manufact#49,i_size#50,i_formulation#51,i_color#52,i_units#53,i_container#54,i_manager_id#55,i_product_name#56] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[qoh#0 ASC NULLS FIRST,i_product_name#63 ASC NULLS FIRST,i_brand#64 ASC NULLS FIRST,i_class#65 ASC NULLS FIRST,i_category#66 ASC NULLS FIRST], output=[i_product_name#63,i_brand#64,i_class#65,i_category#66,qoh#0])
+- *(4) HashAggregate(keys=[i_product_name#63, i_brand#64, i_class#65, i_category#66, spark_grouping_id#58], functions=[avg(inv_quantity_on_hand#6L)], output=[i_product_name#63, i_brand#64, i_class#65, i_category#66, qoh#0])
   +- Exchange hashpartitioning(i_product_name#63, i_brand#64, i_class#65, i_category#66, spark_grouping_id#58, 200)
      +- *(3) HashAggregate(keys=[i_product_name#63, i_brand#64, i_class#65, i_category#66, spark_grouping_id#58], functions=[partial_avg(inv_quantity_on_hand#6L)], output=[i_product_name#63, i_brand#64, i_class#65, i_category#66, spark_grouping_id#58, sum#74, count#75L])
         +- *(3) Expand [List(inv_quantity_on_hand#6L, i_product_name#56, i_brand#43, i_class#45, i_category#47, 0), List(inv_quantity_on_hand#6L, i_product_name#56, i_brand#43, i_class#45, null, 1), List(inv_quantity_on_hand#6L, i_product_name#56, i_brand#43, null, null, 3), List(inv_quantity_on_hand#6L, i_product_name#56, null, null, null, 7), List(inv_quantity_on_hand#6L, null, null, null, null, 15)], [inv_quantity_on_hand#6L, i_product_name#63, i_brand#64, i_class#65, i_category#66, spark_grouping_id#58]
            +- *(3) Project [inv_quantity_on_hand#6L, i_product_name#56, i_brand#43, i_class#45, i_category#47]
               +- *(3) BroadcastHashJoin [inv_item_sk#4], [i_item_sk#35], Inner, BuildRight
                  :- *(3) Project [inv_item_sk#4, inv_quantity_on_hand#6L]
                  :  +- *(3) BroadcastHashJoin [inv_date_sk#3], [d_date_sk#7], Inner, BuildRight
                  :     :- *(3) Project [inv_date_sk#3, inv_item_sk#4, inv_quantity_on_hand#6L]
                  :     :  +- *(3) Filter (isnotnull(inv_date_sk#3) && isnotnull(inv_item_sk#4))
                  :     :     +- *(3) FileScan parquet tpcds.inventory[inv_date_sk#3,inv_item_sk#4,inv_quantity_on_hand#6L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_date_sk), IsNotNull(inv_item_sk)], ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_quantity_on_hand:bigint>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :        +- *(1) Project [d_date_sk#7]
                  :           +- *(1) Filter (((isnotnull(d_month_seq#10) && (d_month_seq#10 >= 1200)) && (d_month_seq#10 <= 1211)) && isnotnull(d_date_sk#7))
                  :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#7,d_month_seq#10] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     +- *(2) Project [i_item_sk#35, i_brand#43, i_class#45, i_category#47, i_product_name#56]
                        +- *(2) Filter isnotnull(i_item_sk#35)
                           +- *(2) FileScan parquet tpcds.item[i_item_sk#35,i_brand#43,i_class#45,i_category#47,i_product_name#56] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_class:string,i_category:string,i_product_name:string>
Time taken: 3.703 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 22 in stream 0 using template query22.tpl
------------------------------------------------------^^^

