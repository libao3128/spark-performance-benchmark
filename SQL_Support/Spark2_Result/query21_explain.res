== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['w_warehouse_name ASC NULLS FIRST, 'i_item_id ASC NULLS FIRST], true
      +- 'Project [*]
         +- 'Filter ((CASE WHEN ('inv_before > 0) THEN ('inv_after / 'inv_before) ELSE null END >= (2.0 / 3.0)) && (CASE WHEN ('inv_before > 0) THEN ('inv_after / 'inv_before) ELSE null END <= (3.0 / 2.0)))
            +- 'SubqueryAlias `x`
               +- 'Aggregate ['w_warehouse_name, 'i_item_id], ['w_warehouse_name, 'i_item_id, 'sum(CASE WHEN (cast('d_date as date) < cast(2000-03-11 as date)) THEN 'inv_quantity_on_hand ELSE 0 END) AS inv_before#0, 'sum(CASE WHEN (cast('d_date as date) >= cast(2000-03-11 as date)) THEN 'inv_quantity_on_hand ELSE 0 END) AS inv_after#1]
                  +- 'Filter ((((('i_current_price >= 0.99) && ('i_current_price <= 1.49)) && ('i_item_sk = 'inv_item_sk)) && ('inv_warehouse_sk = 'w_warehouse_sk)) && (('inv_date_sk = 'd_date_sk) && (('d_date >= 'date_sub(cast(2000-03-11 as date), 30)) && ('d_date <= 'date_add(cast(2000-03-11 as date), 30)))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'Join Inner
                        :  :  :- 'UnresolvedRelation `inventory`
                        :  :  +- 'UnresolvedRelation `warehouse`
                        :  +- 'UnresolvedRelation `item`
                        +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
w_warehouse_name: string, i_item_id: string, inv_before: bigint, inv_after: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [w_warehouse_name#10 ASC NULLS FIRST, i_item_id#23 ASC NULLS FIRST], true
      +- Project [w_warehouse_name#10, i_item_id#23, inv_before#0L, inv_after#1L]
         +- Filter ((CASE WHEN (inv_before#0L > cast(0 as bigint)) THEN (cast(inv_after#1L as double) / cast(inv_before#0L as double)) ELSE cast(null as double) END >= cast(CheckOverflow((promote_precision(cast(2.0 as decimal(2,1))) / promote_precision(cast(3.0 as decimal(2,1)))), DecimalType(8,6)) as double)) && (CASE WHEN (inv_before#0L > cast(0 as bigint)) THEN (cast(inv_after#1L as double) / cast(inv_before#0L as double)) ELSE cast(null as double) END <= cast(CheckOverflow((promote_precision(cast(3.0 as decimal(2,1))) / promote_precision(cast(2.0 as decimal(2,1)))), DecimalType(8,6)) as double)))
            +- SubqueryAlias `x`
               +- Aggregate [w_warehouse_name#10, i_item_id#23], [w_warehouse_name#10, i_item_id#23, sum(CASE WHEN (cast(d_date#46 as date) < cast(2000-03-11 as date)) THEN inv_quantity_on_hand#7L ELSE cast(0 as bigint) END) AS inv_before#0L, sum(CASE WHEN (cast(d_date#46 as date) >= cast(2000-03-11 as date)) THEN inv_quantity_on_hand#7L ELSE cast(0 as bigint) END) AS inv_after#1L]
                  +- Filter (((((i_current_price#27 >= cast(0.99 as double)) && (i_current_price#27 <= cast(1.49 as double))) && (i_item_sk#22 = inv_item_sk#5)) && (inv_warehouse_sk#6 = w_warehouse_sk#8)) && ((inv_date_sk#4 = d_date_sk#44) && ((d_date#46 >= cast(date_sub(cast(2000-03-11 as date), 30) as string)) && (d_date#46 <= cast(date_add(cast(2000-03-11 as date), 30) as string)))))
                     +- Join Inner
                        :- Join Inner
                        :  :- Join Inner
                        :  :  :- SubqueryAlias `tpcds`.`inventory`
                        :  :  :  +- Relation[inv_date_sk#4,inv_item_sk#5,inv_warehouse_sk#6,inv_quantity_on_hand#7L] parquet
                        :  :  +- SubqueryAlias `tpcds`.`warehouse`
                        :  :     +- Relation[w_warehouse_sk#8,w_warehouse_id#9,w_warehouse_name#10,w_warehouse_sq_ft#11,w_street_number#12,w_street_name#13,w_street_type#14,w_suite_number#15,w_city#16,w_county#17,w_state#18,w_zip#19,w_country#20,w_gmt_offset#21] parquet
                        :  +- SubqueryAlias `tpcds`.`item`
                        :     +- Relation[i_item_sk#22,i_item_id#23,i_rec_start_date#24,i_rec_end_date#25,i_item_desc#26,i_current_price#27,i_wholesale_cost#28,i_brand_id#29,i_brand#30,i_class_id#31,i_class#32,i_category_id#33,i_category#34,i_manufact_id#35,i_manufact#36,i_size#37,i_formulation#38,i_color#39,i_units#40,i_container#41,i_manager_id#42,i_product_name#43] parquet
                        +- SubqueryAlias `tpcds`.`date_dim`
                           +- Relation[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [w_warehouse_name#10 ASC NULLS FIRST, i_item_id#23 ASC NULLS FIRST], true
      +- Filter ((CASE WHEN (inv_before#0L > 0) THEN (cast(inv_after#1L as double) / cast(inv_before#0L as double)) ELSE null END >= 0.666667) && (CASE WHEN (inv_before#0L > 0) THEN (cast(inv_after#1L as double) / cast(inv_before#0L as double)) ELSE null END <= 1.5))
         +- Aggregate [w_warehouse_name#10, i_item_id#23], [w_warehouse_name#10, i_item_id#23, sum(CASE WHEN (cast(d_date#46 as date) < 11027) THEN inv_quantity_on_hand#7L ELSE 0 END) AS inv_before#0L, sum(CASE WHEN (cast(d_date#46 as date) >= 11027) THEN inv_quantity_on_hand#7L ELSE 0 END) AS inv_after#1L]
            +- Project [inv_quantity_on_hand#7L, w_warehouse_name#10, i_item_id#23, d_date#46]
               +- Join Inner, (inv_date_sk#4 = d_date_sk#44)
                  :- Project [inv_date_sk#4, inv_quantity_on_hand#7L, w_warehouse_name#10, i_item_id#23]
                  :  +- Join Inner, (i_item_sk#22 = inv_item_sk#5)
                  :     :- Project [inv_date_sk#4, inv_item_sk#5, inv_quantity_on_hand#7L, w_warehouse_name#10]
                  :     :  +- Join Inner, (inv_warehouse_sk#6 = w_warehouse_sk#8)
                  :     :     :- Filter ((isnotnull(inv_warehouse_sk#6) && isnotnull(inv_item_sk#5)) && isnotnull(inv_date_sk#4))
                  :     :     :  +- Relation[inv_date_sk#4,inv_item_sk#5,inv_warehouse_sk#6,inv_quantity_on_hand#7L] parquet
                  :     :     +- Project [w_warehouse_sk#8, w_warehouse_name#10]
                  :     :        +- Filter isnotnull(w_warehouse_sk#8)
                  :     :           +- Relation[w_warehouse_sk#8,w_warehouse_id#9,w_warehouse_name#10,w_warehouse_sq_ft#11,w_street_number#12,w_street_name#13,w_street_type#14,w_suite_number#15,w_city#16,w_county#17,w_state#18,w_zip#19,w_country#20,w_gmt_offset#21] parquet
                  :     +- Project [i_item_sk#22, i_item_id#23]
                  :        +- Filter (((isnotnull(i_current_price#27) && (i_current_price#27 >= 0.99)) && (i_current_price#27 <= 1.49)) && isnotnull(i_item_sk#22))
                  :           +- Relation[i_item_sk#22,i_item_id#23,i_rec_start_date#24,i_rec_end_date#25,i_item_desc#26,i_current_price#27,i_wholesale_cost#28,i_brand_id#29,i_brand#30,i_class_id#31,i_class#32,i_category_id#33,i_category#34,i_manufact_id#35,i_manufact#36,i_size#37,i_formulation#38,i_color#39,i_units#40,i_container#41,i_manager_id#42,i_product_name#43] parquet
                  +- Project [d_date_sk#44, d_date#46]
                     +- Filter (((isnotnull(d_date#46) && (d_date#46 >= 2000-02-10)) && (d_date#46 <= 2000-04-10)) && isnotnull(d_date_sk#44))
                        +- Relation[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[w_warehouse_name#10 ASC NULLS FIRST,i_item_id#23 ASC NULLS FIRST], output=[w_warehouse_name#10,i_item_id#23,inv_before#0L,inv_after#1L])
+- *(5) Filter ((CASE WHEN (inv_before#0L > 0) THEN (cast(inv_after#1L as double) / cast(inv_before#0L as double)) ELSE null END >= 0.666667) && (CASE WHEN (inv_before#0L > 0) THEN (cast(inv_after#1L as double) / cast(inv_before#0L as double)) ELSE null END <= 1.5))
   +- *(5) HashAggregate(keys=[w_warehouse_name#10, i_item_id#23], functions=[sum(CASE WHEN (cast(d_date#46 as date) < 11027) THEN inv_quantity_on_hand#7L ELSE 0 END), sum(CASE WHEN (cast(d_date#46 as date) >= 11027) THEN inv_quantity_on_hand#7L ELSE 0 END)], output=[w_warehouse_name#10, i_item_id#23, inv_before#0L, inv_after#1L])
      +- Exchange hashpartitioning(w_warehouse_name#10, i_item_id#23, 200)
         +- *(4) HashAggregate(keys=[w_warehouse_name#10, i_item_id#23], functions=[partial_sum(CASE WHEN (cast(d_date#46 as date) < 11027) THEN inv_quantity_on_hand#7L ELSE 0 END), partial_sum(CASE WHEN (cast(d_date#46 as date) >= 11027) THEN inv_quantity_on_hand#7L ELSE 0 END)], output=[w_warehouse_name#10, i_item_id#23, sum#76L, sum#77L])
            +- *(4) Project [inv_quantity_on_hand#7L, w_warehouse_name#10, i_item_id#23, d_date#46]
               +- *(4) BroadcastHashJoin [inv_date_sk#4], [d_date_sk#44], Inner, BuildRight
                  :- *(4) Project [inv_date_sk#4, inv_quantity_on_hand#7L, w_warehouse_name#10, i_item_id#23]
                  :  +- *(4) BroadcastHashJoin [inv_item_sk#5], [i_item_sk#22], Inner, BuildRight
                  :     :- *(4) Project [inv_date_sk#4, inv_item_sk#5, inv_quantity_on_hand#7L, w_warehouse_name#10]
                  :     :  +- *(4) BroadcastHashJoin [inv_warehouse_sk#6], [w_warehouse_sk#8], Inner, BuildRight
                  :     :     :- *(4) Project [inv_date_sk#4, inv_item_sk#5, inv_warehouse_sk#6, inv_quantity_on_hand#7L]
                  :     :     :  +- *(4) Filter ((isnotnull(inv_warehouse_sk#6) && isnotnull(inv_item_sk#5)) && isnotnull(inv_date_sk#4))
                  :     :     :     +- *(4) FileScan parquet tpcds.inventory[inv_date_sk#4,inv_item_sk#5,inv_warehouse_sk#6,inv_quantity_on_hand#7L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_warehouse_sk), IsNotNull(inv_item_sk), IsNotNull(inv_date_sk)], ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_warehouse_sk:int,inv_quantity_on_hand:bigint>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     :        +- *(1) Project [w_warehouse_sk#8, w_warehouse_name#10]
                  :     :           +- *(1) Filter isnotnull(w_warehouse_sk#8)
                  :     :              +- *(1) FileScan parquet tpcds.warehouse[w_warehouse_sk#8,w_warehouse_name#10] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :        +- *(2) Project [i_item_sk#22, i_item_id#23]
                  :           +- *(2) Filter (((isnotnull(i_current_price#27) && (i_current_price#27 >= 0.99)) && (i_current_price#27 <= 1.49)) && isnotnull(i_item_sk#22))
                  :              +- *(2) FileScan parquet tpcds.item[i_item_sk#22,i_item_id#23,i_current_price#27] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThanOrEqual(i_current_price,0.99), LessThanOrEqual(i_current_..., ReadSchema: struct<i_item_sk:int,i_item_id:string,i_current_price:double>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     +- *(3) Project [d_date_sk#44, d_date#46]
                        +- *(3) Filter (((isnotnull(d_date#46) && (d_date#46 >= 2000-02-10)) && (d_date#46 <= 2000-04-10)) && isnotnull(d_date_sk#44))
                           +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#44,d_date#46] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-02-10), LessThanOrEqual(d_date,2000-04-10), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
Time taken: 4.179 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 21 in stream 0 using template query21.tpl
------------------------------------------------------^^^

