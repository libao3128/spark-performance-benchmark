Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580331120
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['qoh ASC NULLS FIRST, 'i_product_name ASC NULLS FIRST, 'i_brand ASC NULLS FIRST, 'i_class ASC NULLS FIRST, 'i_category ASC NULLS FIRST], true
      +- 'Aggregate [rollup(Vector(0), Vector(1), Vector(2), Vector(3), 'i_product_name, 'i_brand, 'i_class, 'i_category)], ['i_product_name, 'i_brand, 'i_class, 'i_category, 'avg('inv_quantity_on_hand) AS qoh#0]
         +- 'Filter ((('inv_date_sk = 'd_date_sk) AND ('inv_item_sk = 'i_item_sk)) AND (('d_month_seq >= 1200) AND ('d_month_seq <= (1200 + 11))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation [inventory], [], false
               :  +- 'UnresolvedRelation [date_dim], [], false
               +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
i_product_name: string, i_brand: string, i_class: string, i_category: string, qoh: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [qoh#0 ASC NULLS FIRST, i_product_name#69 ASC NULLS FIRST, i_brand#70 ASC NULLS FIRST, i_class#71 ASC NULLS FIRST, i_category#72 ASC NULLS FIRST], true
      +- Aggregate [i_product_name#69, i_brand#70, i_class#71, i_category#72, spark_grouping_id#68L], [i_product_name#69, i_brand#70, i_class#71, i_category#72, avg(inv_quantity_on_hand#9L) AS qoh#0]
         +- Expand [[inv_date_sk#6, inv_item_sk#7, inv_warehouse_sk#8, inv_quantity_on_hand#9L, d_date_sk#10, d_date_id#11, d_date#12, d_month_seq#13, d_week_seq#14, d_quarter_seq#15, d_year#16, d_dow#17, d_moy#18, d_dom#19, d_qoy#20, d_fy_year#21, d_fy_quarter_seq#22, d_fy_week_seq#23, d_day_name#24, d_quarter_name#25, d_holiday#26, d_weekend#27, d_following_holiday#28, d_first_dom#29, ... 35 more fields], [inv_date_sk#6, inv_item_sk#7, inv_warehouse_sk#8, inv_quantity_on_hand#9L, d_date_sk#10, d_date_id#11, d_date#12, d_month_seq#13, d_week_seq#14, d_quarter_seq#15, d_year#16, d_dow#17, d_moy#18, d_dom#19, d_qoy#20, d_fy_year#21, d_fy_quarter_seq#22, d_fy_week_seq#23, d_day_name#24, d_quarter_name#25, d_holiday#26, d_weekend#27, d_following_holiday#28, d_first_dom#29, ... 35 more fields], [inv_date_sk#6, inv_item_sk#7, inv_warehouse_sk#8, inv_quantity_on_hand#9L, d_date_sk#10, d_date_id#11, d_date#12, d_month_seq#13, d_week_seq#14, d_quarter_seq#15, d_year#16, d_dow#17, d_moy#18, d_dom#19, d_qoy#20, d_fy_year#21, d_fy_quarter_seq#22, d_fy_week_seq#23, d_day_name#24, d_quarter_name#25, d_holiday#26, d_weekend#27, d_following_holiday#28, d_first_dom#29, ... 35 more fields], [inv_date_sk#6, inv_item_sk#7, inv_warehouse_sk#8, inv_quantity_on_hand#9L, d_date_sk#10, d_date_id#11, d_date#12, d_month_seq#13, d_week_seq#14, d_quarter_seq#15, d_year#16, d_dow#17, d_moy#18, d_dom#19, d_qoy#20, d_fy_year#21, d_fy_quarter_seq#22, d_fy_week_seq#23, d_day_name#24, d_quarter_name#25, d_holiday#26, d_weekend#27, d_following_holiday#28, d_first_dom#29, ... 35 more fields], [inv_date_sk#6, inv_item_sk#7, inv_warehouse_sk#8, inv_quantity_on_hand#9L, d_date_sk#10, d_date_id#11, d_date#12, d_month_seq#13, d_week_seq#14, d_quarter_seq#15, d_year#16, d_dow#17, d_moy#18, d_dom#19, d_qoy#20, d_fy_year#21, d_fy_quarter_seq#22, d_fy_week_seq#23, d_day_name#24, d_quarter_name#25, d_holiday#26, d_weekend#27, d_following_holiday#28, d_first_dom#29, ... 35 more fields]], [inv_date_sk#6, inv_item_sk#7, inv_warehouse_sk#8, inv_quantity_on_hand#9L, d_date_sk#10, d_date_id#11, d_date#12, d_month_seq#13, d_week_seq#14, d_quarter_seq#15, d_year#16, d_dow#17, d_moy#18, d_dom#19, d_qoy#20, d_fy_year#21, d_fy_quarter_seq#22, d_fy_week_seq#23, d_day_name#24, d_quarter_name#25, d_holiday#26, d_weekend#27, d_following_holiday#28, d_first_dom#29, ... 35 more fields]
            +- Project [inv_date_sk#6, inv_item_sk#7, inv_warehouse_sk#8, inv_quantity_on_hand#9L, d_date_sk#10, d_date_id#11, d_date#12, d_month_seq#13, d_week_seq#14, d_quarter_seq#15, d_year#16, d_dow#17, d_moy#18, d_dom#19, d_qoy#20, d_fy_year#21, d_fy_quarter_seq#22, d_fy_week_seq#23, d_day_name#24, d_quarter_name#25, d_holiday#26, d_weekend#27, d_following_holiday#28, d_first_dom#29, ... 34 more fields]
               +- Filter (((inv_date_sk#6 = d_date_sk#10) AND (inv_item_sk#7 = i_item_sk#38)) AND ((d_month_seq#13 >= 1200) AND (d_month_seq#13 <= (1200 + 11))))
                  +- Join Inner
                     :- Join Inner
                     :  :- SubqueryAlias spark_catalog.tpcds.inventory
                     :  :  +- Relation spark_catalog.tpcds.inventory[inv_date_sk#6,inv_item_sk#7,inv_warehouse_sk#8,inv_quantity_on_hand#9L] parquet
                     :  +- SubqueryAlias spark_catalog.tpcds.date_dim
                     :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#10,d_date_id#11,d_date#12,d_month_seq#13,d_week_seq#14,d_quarter_seq#15,d_year#16,d_dow#17,d_moy#18,d_dom#19,d_qoy#20,d_fy_year#21,d_fy_quarter_seq#22,d_fy_week_seq#23,d_day_name#24,d_quarter_name#25,d_holiday#26,d_weekend#27,d_following_holiday#28,d_first_dom#29,d_last_dom#30,d_same_day_ly#31,d_same_day_lq#32,d_current_day#33,... 4 more fields] parquet
                     +- SubqueryAlias spark_catalog.tpcds.item
                        +- Relation spark_catalog.tpcds.item[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [qoh#0 ASC NULLS FIRST, i_product_name#69 ASC NULLS FIRST, i_brand#70 ASC NULLS FIRST, i_class#71 ASC NULLS FIRST, i_category#72 ASC NULLS FIRST], true
      +- Aggregate [i_product_name#69, i_brand#70, i_class#71, i_category#72, spark_grouping_id#68L], [i_product_name#69, i_brand#70, i_class#71, i_category#72, avg(inv_quantity_on_hand#9L) AS qoh#0]
         +- Expand [[inv_quantity_on_hand#9L, i_product_name#59, i_brand#46, i_class#48, i_category#50, 0], [inv_quantity_on_hand#9L, i_product_name#59, i_brand#46, i_class#48, null, 1], [inv_quantity_on_hand#9L, i_product_name#59, i_brand#46, null, null, 3], [inv_quantity_on_hand#9L, i_product_name#59, null, null, null, 7], [inv_quantity_on_hand#9L, null, null, null, null, 15]], [inv_quantity_on_hand#9L, i_product_name#69, i_brand#70, i_class#71, i_category#72, spark_grouping_id#68L]
            +- Project [inv_quantity_on_hand#9L, i_product_name#59, i_brand#46, i_class#48, i_category#50]
               +- Join Inner, (inv_item_sk#7 = i_item_sk#38)
                  :- Project [inv_item_sk#7, inv_quantity_on_hand#9L]
                  :  +- Join Inner, (inv_date_sk#6 = d_date_sk#10)
                  :     :- Project [inv_date_sk#6, inv_item_sk#7, inv_quantity_on_hand#9L]
                  :     :  +- Filter (isnotnull(inv_date_sk#6) AND isnotnull(inv_item_sk#7))
                  :     :     +- Relation spark_catalog.tpcds.inventory[inv_date_sk#6,inv_item_sk#7,inv_warehouse_sk#8,inv_quantity_on_hand#9L] parquet
                  :     +- Project [d_date_sk#10]
                  :        +- Filter ((isnotnull(d_month_seq#13) AND ((d_month_seq#13 >= 1200) AND (d_month_seq#13 <= 1211))) AND isnotnull(d_date_sk#10))
                  :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#10,d_date_id#11,d_date#12,d_month_seq#13,d_week_seq#14,d_quarter_seq#15,d_year#16,d_dow#17,d_moy#18,d_dom#19,d_qoy#20,d_fy_year#21,d_fy_quarter_seq#22,d_fy_week_seq#23,d_day_name#24,d_quarter_name#25,d_holiday#26,d_weekend#27,d_following_holiday#28,d_first_dom#29,d_last_dom#30,d_same_day_ly#31,d_same_day_lq#32,d_current_day#33,... 4 more fields] parquet
                  +- Project [i_item_sk#38, i_brand#46, i_class#48, i_category#50, i_product_name#59]
                     +- Filter isnotnull(i_item_sk#38)
                        +- Relation spark_catalog.tpcds.item[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[qoh#0 ASC NULLS FIRST,i_product_name#69 ASC NULLS FIRST,i_brand#70 ASC NULLS FIRST,i_class#71 ASC NULLS FIRST,i_category#72 ASC NULLS FIRST], output=[i_product_name#69,i_brand#70,i_class#71,i_category#72,qoh#0])
   +- HashAggregate(keys=[i_product_name#69, i_brand#70, i_class#71, i_category#72, spark_grouping_id#68L], functions=[avg(inv_quantity_on_hand#9L)], output=[i_product_name#69, i_brand#70, i_class#71, i_category#72, qoh#0])
      +- Exchange hashpartitioning(i_product_name#69, i_brand#70, i_class#71, i_category#72, spark_grouping_id#68L, 200), ENSURE_REQUIREMENTS, [plan_id=78]
         +- HashAggregate(keys=[i_product_name#69, i_brand#70, i_class#71, i_category#72, spark_grouping_id#68L], functions=[partial_avg(inv_quantity_on_hand#9L)], output=[i_product_name#69, i_brand#70, i_class#71, i_category#72, spark_grouping_id#68L, sum#75, count#76L])
            +- Expand [[inv_quantity_on_hand#9L, i_product_name#59, i_brand#46, i_class#48, i_category#50, 0], [inv_quantity_on_hand#9L, i_product_name#59, i_brand#46, i_class#48, null, 1], [inv_quantity_on_hand#9L, i_product_name#59, i_brand#46, null, null, 3], [inv_quantity_on_hand#9L, i_product_name#59, null, null, null, 7], [inv_quantity_on_hand#9L, null, null, null, null, 15]], [inv_quantity_on_hand#9L, i_product_name#69, i_brand#70, i_class#71, i_category#72, spark_grouping_id#68L]
               +- Project [inv_quantity_on_hand#9L, i_product_name#59, i_brand#46, i_class#48, i_category#50]
                  +- BroadcastHashJoin [inv_item_sk#7], [i_item_sk#38], Inner, BuildRight, false
                     :- Project [inv_item_sk#7, inv_quantity_on_hand#9L]
                     :  +- BroadcastHashJoin [inv_date_sk#6], [d_date_sk#10], Inner, BuildRight, false
                     :     :- Filter (isnotnull(inv_date_sk#6) AND isnotnull(inv_item_sk#7))
                     :     :  +- FileScan parquet spark_catalog.tpcds.inventory[inv_date_sk#6,inv_item_sk#7,inv_quantity_on_hand#9L] Batched: true, DataFilters: [isnotnull(inv_date_sk#6), isnotnull(inv_item_sk#7)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_date_sk), IsNotNull(inv_item_sk)], ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_quantity_on_hand:bigint>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=68]
                     :        +- Project [d_date_sk#10]
                     :           +- Filter (((isnotnull(d_month_seq#13) AND (d_month_seq#13 >= 1200)) AND (d_month_seq#13 <= 1211)) AND isnotnull(d_date_sk#10))
                     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#10,d_month_seq#13] Batched: true, DataFilters: [isnotnull(d_month_seq#13), (d_month_seq#13 >= 1200), (d_month_seq#13 <= 1211), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=72]
                        +- Filter isnotnull(i_item_sk#38)
                           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#38,i_brand#46,i_class#48,i_category#50,i_product_name#59] Batched: true, DataFilters: [isnotnull(i_item_sk#38)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_class:string,i_category:string,i_product_name:string>

Time taken: 2.342 seconds, Fetched 1 row(s)
