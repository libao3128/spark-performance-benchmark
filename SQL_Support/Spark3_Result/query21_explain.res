Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580296271
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['w_warehouse_name ASC NULLS FIRST, 'i_item_id ASC NULLS FIRST], true
      +- 'Project [*]
         +- 'Filter ((CASE WHEN ('inv_before > 0) THEN ('inv_after / 'inv_before) ELSE null END >= (2.0 / 3.0)) AND (CASE WHEN ('inv_before > 0) THEN ('inv_after / 'inv_before) ELSE null END <= (3.0 / 2.0)))
            +- 'SubqueryAlias x
               +- 'Aggregate ['w_warehouse_name, 'i_item_id], ['w_warehouse_name, 'i_item_id, 'sum(CASE WHEN (cast('d_date as date) < cast(2000-03-11 as date)) THEN 'inv_quantity_on_hand ELSE 0 END) AS inv_before#0, 'sum(CASE WHEN (cast('d_date as date) >= cast(2000-03-11 as date)) THEN 'inv_quantity_on_hand ELSE 0 END) AS inv_after#1]
                  +- 'Filter ((((('i_current_price >= 0.99) AND ('i_current_price <= 1.49)) AND ('i_item_sk = 'inv_item_sk)) AND ('inv_warehouse_sk = 'w_warehouse_sk)) AND (('inv_date_sk = 'd_date_sk) AND (('d_date >= 'date_sub(cast(2000-03-11 as date), 30)) AND ('d_date <= 'date_add(cast(2000-03-11 as date), 30)))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'Join Inner
                        :  :  :- 'UnresolvedRelation [inventory], [], false
                        :  :  +- 'UnresolvedRelation [warehouse], [], false
                        :  +- 'UnresolvedRelation [item], [], false
                        +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
w_warehouse_name: string, i_item_id: string, inv_before: bigint, inv_after: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [w_warehouse_name#13 ASC NULLS FIRST, i_item_id#26 ASC NULLS FIRST], true
      +- Project [w_warehouse_name#13, i_item_id#26, inv_before#0L, inv_after#1L]
         +- Filter ((CASE WHEN (inv_before#0L > cast(0 as bigint)) THEN (cast(inv_after#1L as double) / cast(inv_before#0L as double)) ELSE cast(null as double) END >= cast((2.0 / 3.0) as double)) AND (CASE WHEN (inv_before#0L > cast(0 as bigint)) THEN (cast(inv_after#1L as double) / cast(inv_before#0L as double)) ELSE cast(null as double) END <= cast((3.0 / 2.0) as double)))
            +- SubqueryAlias x
               +- Aggregate [w_warehouse_name#13, i_item_id#26], [w_warehouse_name#13, i_item_id#26, sum(CASE WHEN (cast(d_date#49 as date) < cast(2000-03-11 as date)) THEN inv_quantity_on_hand#10L ELSE cast(0 as bigint) END) AS inv_before#0L, sum(CASE WHEN (cast(d_date#49 as date) >= cast(2000-03-11 as date)) THEN inv_quantity_on_hand#10L ELSE cast(0 as bigint) END) AS inv_after#1L]
                  +- Filter (((((i_current_price#30 >= cast(0.99 as double)) AND (i_current_price#30 <= cast(1.49 as double))) AND (i_item_sk#25 = inv_item_sk#8)) AND (inv_warehouse_sk#9 = w_warehouse_sk#11)) AND ((inv_date_sk#7 = d_date_sk#47) AND ((cast(d_date#49 as date) >= date_sub(cast(2000-03-11 as date), 30)) AND (cast(d_date#49 as date) <= date_add(cast(2000-03-11 as date), 30)))))
                     +- Join Inner
                        :- Join Inner
                        :  :- Join Inner
                        :  :  :- SubqueryAlias spark_catalog.tpcds.inventory
                        :  :  :  +- Relation spark_catalog.tpcds.inventory[inv_date_sk#7,inv_item_sk#8,inv_warehouse_sk#9,inv_quantity_on_hand#10L] parquet
                        :  :  +- SubqueryAlias spark_catalog.tpcds.warehouse
                        :  :     +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#11,w_warehouse_id#12,w_warehouse_name#13,w_warehouse_sq_ft#14,w_street_number#15,w_street_name#16,w_street_type#17,w_suite_number#18,w_city#19,w_county#20,w_state#21,w_zip#22,w_country#23,w_gmt_offset#24] parquet
                        :  +- SubqueryAlias spark_catalog.tpcds.item
                        :     +- Relation spark_catalog.tpcds.item[i_item_sk#25,i_item_id#26,i_rec_start_date#27,i_rec_end_date#28,i_item_desc#29,i_current_price#30,i_wholesale_cost#31,i_brand_id#32,i_brand#33,i_class_id#34,i_class#35,i_category_id#36,i_category#37,i_manufact_id#38,i_manufact#39,i_size#40,i_formulation#41,i_color#42,i_units#43,i_container#44,i_manager_id#45,i_product_name#46] parquet
                        +- SubqueryAlias spark_catalog.tpcds.date_dim
                           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#47,d_date_id#48,d_date#49,d_month_seq#50,d_week_seq#51,d_quarter_seq#52,d_year#53,d_dow#54,d_moy#55,d_dom#56,d_qoy#57,d_fy_year#58,d_fy_quarter_seq#59,d_fy_week_seq#60,d_day_name#61,d_quarter_name#62,d_holiday#63,d_weekend#64,d_following_holiday#65,d_first_dom#66,d_last_dom#67,d_same_day_ly#68,d_same_day_lq#69,d_current_day#70,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [w_warehouse_name#13 ASC NULLS FIRST, i_item_id#26 ASC NULLS FIRST], true
      +- Filter (CASE WHEN (inv_before#0L > 0) THEN ((cast(inv_after#1L as double) / cast(inv_before#0L as double)) >= 0.666667) END AND CASE WHEN (inv_before#0L > 0) THEN ((cast(inv_after#1L as double) / cast(inv_before#0L as double)) <= 1.5) END)
         +- Aggregate [w_warehouse_name#13, i_item_id#26], [w_warehouse_name#13, i_item_id#26, sum(CASE WHEN (cast(d_date#49 as date) < 2000-03-11) THEN inv_quantity_on_hand#10L ELSE 0 END) AS inv_before#0L, sum(CASE WHEN (cast(d_date#49 as date) >= 2000-03-11) THEN inv_quantity_on_hand#10L ELSE 0 END) AS inv_after#1L]
            +- Project [inv_quantity_on_hand#10L, w_warehouse_name#13, i_item_id#26, d_date#49]
               +- Join Inner, (inv_date_sk#7 = d_date_sk#47)
                  :- Project [inv_date_sk#7, inv_quantity_on_hand#10L, w_warehouse_name#13, i_item_id#26]
                  :  +- Join Inner, (i_item_sk#25 = inv_item_sk#8)
                  :     :- Project [inv_date_sk#7, inv_item_sk#8, inv_quantity_on_hand#10L, w_warehouse_name#13]
                  :     :  +- Join Inner, (inv_warehouse_sk#9 = w_warehouse_sk#11)
                  :     :     :- Filter (isnotnull(inv_warehouse_sk#9) AND (isnotnull(inv_item_sk#8) AND isnotnull(inv_date_sk#7)))
                  :     :     :  +- Relation spark_catalog.tpcds.inventory[inv_date_sk#7,inv_item_sk#8,inv_warehouse_sk#9,inv_quantity_on_hand#10L] parquet
                  :     :     +- Project [w_warehouse_sk#11, w_warehouse_name#13]
                  :     :        +- Filter isnotnull(w_warehouse_sk#11)
                  :     :           +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#11,w_warehouse_id#12,w_warehouse_name#13,w_warehouse_sq_ft#14,w_street_number#15,w_street_name#16,w_street_type#17,w_suite_number#18,w_city#19,w_county#20,w_state#21,w_zip#22,w_country#23,w_gmt_offset#24] parquet
                  :     +- Project [i_item_sk#25, i_item_id#26]
                  :        +- Filter ((isnotnull(i_current_price#30) AND ((i_current_price#30 >= 0.99) AND (i_current_price#30 <= 1.49))) AND isnotnull(i_item_sk#25))
                  :           +- Relation spark_catalog.tpcds.item[i_item_sk#25,i_item_id#26,i_rec_start_date#27,i_rec_end_date#28,i_item_desc#29,i_current_price#30,i_wholesale_cost#31,i_brand_id#32,i_brand#33,i_class_id#34,i_class#35,i_category_id#36,i_category#37,i_manufact_id#38,i_manufact#39,i_size#40,i_formulation#41,i_color#42,i_units#43,i_container#44,i_manager_id#45,i_product_name#46] parquet
                  +- Project [d_date_sk#47, d_date#49]
                     +- Filter ((isnotnull(d_date#49) AND ((cast(d_date#49 as date) >= 2000-02-10) AND (cast(d_date#49 as date) <= 2000-04-10))) AND isnotnull(d_date_sk#47))
                        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#47,d_date_id#48,d_date#49,d_month_seq#50,d_week_seq#51,d_quarter_seq#52,d_year#53,d_dow#54,d_moy#55,d_dom#56,d_qoy#57,d_fy_year#58,d_fy_quarter_seq#59,d_fy_week_seq#60,d_day_name#61,d_quarter_name#62,d_holiday#63,d_weekend#64,d_following_holiday#65,d_first_dom#66,d_last_dom#67,d_same_day_ly#68,d_same_day_lq#69,d_current_day#70,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[w_warehouse_name#13 ASC NULLS FIRST,i_item_id#26 ASC NULLS FIRST], output=[w_warehouse_name#13,i_item_id#26,inv_before#0L,inv_after#1L])
   +- Filter (CASE WHEN (inv_before#0L > 0) THEN ((cast(inv_after#1L as double) / cast(inv_before#0L as double)) >= 0.666667) END AND CASE WHEN (inv_before#0L > 0) THEN ((cast(inv_after#1L as double) / cast(inv_before#0L as double)) <= 1.5) END)
      +- HashAggregate(keys=[w_warehouse_name#13, i_item_id#26], functions=[sum(CASE WHEN (cast(d_date#49 as date) < 2000-03-11) THEN inv_quantity_on_hand#10L ELSE 0 END), sum(CASE WHEN (cast(d_date#49 as date) >= 2000-03-11) THEN inv_quantity_on_hand#10L ELSE 0 END)], output=[w_warehouse_name#13, i_item_id#26, inv_before#0L, inv_after#1L])
         +- Exchange hashpartitioning(w_warehouse_name#13, i_item_id#26, 200), ENSURE_REQUIREMENTS, [plan_id=99]
            +- HashAggregate(keys=[w_warehouse_name#13, i_item_id#26], functions=[partial_sum(CASE WHEN (cast(d_date#49 as date) < 2000-03-11) THEN inv_quantity_on_hand#10L ELSE 0 END), partial_sum(CASE WHEN (cast(d_date#49 as date) >= 2000-03-11) THEN inv_quantity_on_hand#10L ELSE 0 END)], output=[w_warehouse_name#13, i_item_id#26, sum#83L, sum#84L])
               +- Project [inv_quantity_on_hand#10L, w_warehouse_name#13, i_item_id#26, d_date#49]
                  +- BroadcastHashJoin [inv_date_sk#7], [d_date_sk#47], Inner, BuildRight, false
                     :- Project [inv_date_sk#7, inv_quantity_on_hand#10L, w_warehouse_name#13, i_item_id#26]
                     :  +- BroadcastHashJoin [inv_item_sk#8], [i_item_sk#25], Inner, BuildRight, false
                     :     :- Project [inv_date_sk#7, inv_item_sk#8, inv_quantity_on_hand#10L, w_warehouse_name#13]
                     :     :  +- BroadcastHashJoin [inv_warehouse_sk#9], [w_warehouse_sk#11], Inner, BuildRight, false
                     :     :     :- Filter ((isnotnull(inv_warehouse_sk#9) AND isnotnull(inv_item_sk#8)) AND isnotnull(inv_date_sk#7))
                     :     :     :  +- FileScan parquet spark_catalog.tpcds.inventory[inv_date_sk#7,inv_item_sk#8,inv_warehouse_sk#9,inv_quantity_on_hand#10L] Batched: true, DataFilters: [isnotnull(inv_warehouse_sk#9), isnotnull(inv_item_sk#8), isnotnull(inv_date_sk#7)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_warehouse_sk), IsNotNull(inv_item_sk), IsNotNull(inv_date_sk)], ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_warehouse_sk:int,inv_quantity_on_hand:bigint>
                     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=86]
                     :     :        +- Filter isnotnull(w_warehouse_sk#11)
                     :     :           +- FileScan parquet spark_catalog.tpcds.warehouse[w_warehouse_sk#11,w_warehouse_name#13] Batched: true, DataFilters: [isnotnull(w_warehouse_sk#11)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=90]
                     :        +- Project [i_item_sk#25, i_item_id#26]
                     :           +- Filter (((isnotnull(i_current_price#30) AND (i_current_price#30 >= 0.99)) AND (i_current_price#30 <= 1.49)) AND isnotnull(i_item_sk#25))
                     :              +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#25,i_item_id#26,i_current_price#30] Batched: true, DataFilters: [isnotnull(i_current_price#30), (i_current_price#30 >= 0.99), (i_current_price#30 <= 1.49), isnot..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThanOrEqual(i_current_price,0.99), LessThanOrEqual(i_current_..., ReadSchema: struct<i_item_sk:int,i_item_id:string,i_current_price:double>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=94]
                        +- Filter (((isnotnull(d_date#49) AND (cast(d_date#49 as date) >= 2000-02-10)) AND (cast(d_date#49 as date) <= 2000-04-10)) AND isnotnull(d_date_sk#47))
                           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#47,d_date#49] Batched: true, DataFilters: [isnotnull(d_date#49), (cast(d_date#49 as date) >= 2000-02-10), (cast(d_date#49 as date) <= 2000-..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>

Time taken: 2.585 seconds, Fetched 1 row(s)
