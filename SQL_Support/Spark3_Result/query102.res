Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583567002
== Parsed Logical Plan ==
'Project ['i_item_sk, 'w_warehouse_sk]
+- 'Filter ('d_year = 2001)
   +- 'Join Inner, ('inv_date_sk = 'd_date_sk)
      :- 'Join Inner, ('inv_warehouse_sk = 'w_warehouse_sk)
      :  :- 'Join Inner, ('inv_item_sk = 'i_item_sk)
      :  :  :- 'UnresolvedRelation [inventory], [], false
      :  :  +- 'UnresolvedRelation [item], [], false
      :  +- 'UnresolvedRelation [warehouse], [], false
      +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
i_item_sk: int, w_warehouse_sk: int
Project [i_item_sk#9, w_warehouse_sk#31]
+- Filter (d_year#51 = 2001)
   +- Join Inner, (inv_date_sk#5 = d_date_sk#45)
      :- Join Inner, (inv_warehouse_sk#7 = w_warehouse_sk#31)
      :  :- Join Inner, (inv_item_sk#6 = i_item_sk#9)
      :  :  :- SubqueryAlias spark_catalog.tpcds.inventory
      :  :  :  +- Relation spark_catalog.tpcds.inventory[inv_date_sk#5,inv_item_sk#6,inv_warehouse_sk#7,inv_quantity_on_hand#8L] parquet
      :  :  +- SubqueryAlias spark_catalog.tpcds.item
      :  :     +- Relation spark_catalog.tpcds.item[i_item_sk#9,i_item_id#10,i_rec_start_date#11,i_rec_end_date#12,i_item_desc#13,i_current_price#14,i_wholesale_cost#15,i_brand_id#16,i_brand#17,i_class_id#18,i_class#19,i_category_id#20,i_category#21,i_manufact_id#22,i_manufact#23,i_size#24,i_formulation#25,i_color#26,i_units#27,i_container#28,i_manager_id#29,i_product_name#30] parquet
      :  +- SubqueryAlias spark_catalog.tpcds.warehouse
      :     +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#31,w_warehouse_id#32,w_warehouse_name#33,w_warehouse_sq_ft#34,w_street_number#35,w_street_name#36,w_street_type#37,w_suite_number#38,w_city#39,w_county#40,w_state#41,w_zip#42,w_country#43,w_gmt_offset#44] parquet
      +- SubqueryAlias spark_catalog.tpcds.date_dim
         +- Relation spark_catalog.tpcds.date_dim[d_date_sk#45,d_date_id#46,d_date#47,d_month_seq#48,d_week_seq#49,d_quarter_seq#50,d_year#51,d_dow#52,d_moy#53,d_dom#54,d_qoy#55,d_fy_year#56,d_fy_quarter_seq#57,d_fy_week_seq#58,d_day_name#59,d_quarter_name#60,d_holiday#61,d_weekend#62,d_following_holiday#63,d_first_dom#64,d_last_dom#65,d_same_day_ly#66,d_same_day_lq#67,d_current_day#68,... 4 more fields] parquet

== Optimized Logical Plan ==
Project [i_item_sk#9, w_warehouse_sk#31]
+- Join Inner, (inv_date_sk#5 = d_date_sk#45)
   :- Project [inv_date_sk#5, i_item_sk#9, w_warehouse_sk#31]
   :  +- Join Inner, (inv_warehouse_sk#7 = w_warehouse_sk#31)
   :     :- Project [inv_date_sk#5, inv_warehouse_sk#7, i_item_sk#9]
   :     :  +- Join Inner, (inv_item_sk#6 = i_item_sk#9)
   :     :     :- Project [inv_date_sk#5, inv_item_sk#6, inv_warehouse_sk#7]
   :     :     :  +- Filter (isnotnull(inv_item_sk#6) AND (isnotnull(inv_warehouse_sk#7) AND isnotnull(inv_date_sk#5)))
   :     :     :     +- Relation spark_catalog.tpcds.inventory[inv_date_sk#5,inv_item_sk#6,inv_warehouse_sk#7,inv_quantity_on_hand#8L] parquet
   :     :     +- Project [i_item_sk#9]
   :     :        +- Filter isnotnull(i_item_sk#9)
   :     :           +- Relation spark_catalog.tpcds.item[i_item_sk#9,i_item_id#10,i_rec_start_date#11,i_rec_end_date#12,i_item_desc#13,i_current_price#14,i_wholesale_cost#15,i_brand_id#16,i_brand#17,i_class_id#18,i_class#19,i_category_id#20,i_category#21,i_manufact_id#22,i_manufact#23,i_size#24,i_formulation#25,i_color#26,i_units#27,i_container#28,i_manager_id#29,i_product_name#30] parquet
   :     +- Project [w_warehouse_sk#31]
   :        +- Filter isnotnull(w_warehouse_sk#31)
   :           +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#31,w_warehouse_id#32,w_warehouse_name#33,w_warehouse_sq_ft#34,w_street_number#35,w_street_name#36,w_street_type#37,w_suite_number#38,w_city#39,w_county#40,w_state#41,w_zip#42,w_country#43,w_gmt_offset#44] parquet
   +- Project [d_date_sk#45]
      +- Filter ((isnotnull(d_year#51) AND (d_year#51 = 2001)) AND isnotnull(d_date_sk#45))
         +- Relation spark_catalog.tpcds.date_dim[d_date_sk#45,d_date_id#46,d_date#47,d_month_seq#48,d_week_seq#49,d_quarter_seq#50,d_year#51,d_dow#52,d_moy#53,d_dom#54,d_qoy#55,d_fy_year#56,d_fy_quarter_seq#57,d_fy_week_seq#58,d_day_name#59,d_quarter_name#60,d_holiday#61,d_weekend#62,d_following_holiday#63,d_first_dom#64,d_last_dom#65,d_same_day_ly#66,d_same_day_lq#67,d_current_day#68,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [i_item_sk#9, w_warehouse_sk#31]
   +- BroadcastHashJoin [inv_date_sk#5], [d_date_sk#45], Inner, BuildRight, false
      :- Project [inv_date_sk#5, i_item_sk#9, w_warehouse_sk#31]
      :  +- BroadcastHashJoin [inv_warehouse_sk#7], [w_warehouse_sk#31], Inner, BuildRight, false
      :     :- Project [inv_date_sk#5, inv_warehouse_sk#7, i_item_sk#9]
      :     :  +- BroadcastHashJoin [inv_item_sk#6], [i_item_sk#9], Inner, BuildRight, false
      :     :     :- Filter ((isnotnull(inv_item_sk#6) AND isnotnull(inv_warehouse_sk#7)) AND isnotnull(inv_date_sk#5))
      :     :     :  +- FileScan parquet spark_catalog.tpcds.inventory[inv_date_sk#5,inv_item_sk#6,inv_warehouse_sk#7] Batched: true, DataFilters: [isnotnull(inv_item_sk#6), isnotnull(inv_warehouse_sk#7), isnotnull(inv_date_sk#5)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_item_sk), IsNotNull(inv_warehouse_sk), IsNotNull(inv_date_sk)], ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_warehouse_sk:int>
      :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=68]
      :     :        +- Filter isnotnull(i_item_sk#9)
      :     :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#9] Batched: true, DataFilters: [isnotnull(i_item_sk#9)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int>
      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=72]
      :        +- Filter isnotnull(w_warehouse_sk#31)
      :           +- FileScan parquet spark_catalog.tpcds.warehouse[w_warehouse_sk#31] Batched: true, DataFilters: [isnotnull(w_warehouse_sk#31)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int>
      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=76]
         +- Project [d_date_sk#45]
            +- Filter ((isnotnull(d_year#51) AND (d_year#51 = 2001)) AND isnotnull(d_date_sk#45))
               +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#45,d_year#51] Batched: true, DataFilters: [isnotnull(d_year#51), (d_year#51 = 2001), isnotnull(d_date_sk#45)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>

Time taken: 2.338 seconds, Fetched 1 row(s)
