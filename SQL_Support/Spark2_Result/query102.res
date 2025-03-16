== Parsed Logical Plan ==
'Project ['i_item_sk, 'w_warehouse_sk]
+- 'Filter ('d_year = 2001)
   +- 'Join Inner, ('inv_date_sk = 'd_date_sk)
      :- 'Join Inner, ('inv_warehouse_sk = 'w_warehouse_sk)
      :  :- 'Join Inner, ('inv_item_sk = 'i_item_sk)
      :  :  :- 'UnresolvedRelation `inventory`
      :  :  +- 'UnresolvedRelation `item`
      :  +- 'UnresolvedRelation `warehouse`
      +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
i_item_sk: int, w_warehouse_sk: int
Project [i_item_sk#6, w_warehouse_sk#28]
+- Filter (d_year#48 = 2001)
   +- Join Inner, (inv_date_sk#2 = d_date_sk#42)
      :- Join Inner, (inv_warehouse_sk#4 = w_warehouse_sk#28)
      :  :- Join Inner, (inv_item_sk#3 = i_item_sk#6)
      :  :  :- SubqueryAlias `tpcds`.`inventory`
      :  :  :  +- Relation[inv_date_sk#2,inv_item_sk#3,inv_warehouse_sk#4,inv_quantity_on_hand#5L] parquet
      :  :  +- SubqueryAlias `tpcds`.`item`
      :  :     +- Relation[i_item_sk#6,i_item_id#7,i_rec_start_date#8,i_rec_end_date#9,i_item_desc#10,i_current_price#11,i_wholesale_cost#12,i_brand_id#13,i_brand#14,i_class_id#15,i_class#16,i_category_id#17,i_category#18,i_manufact_id#19,i_manufact#20,i_size#21,i_formulation#22,i_color#23,i_units#24,i_container#25,i_manager_id#26,i_product_name#27] parquet
      :  +- SubqueryAlias `tpcds`.`warehouse`
      :     +- Relation[w_warehouse_sk#28,w_warehouse_id#29,w_warehouse_name#30,w_warehouse_sq_ft#31,w_street_number#32,w_street_name#33,w_street_type#34,w_suite_number#35,w_city#36,w_county#37,w_state#38,w_zip#39,w_country#40,w_gmt_offset#41] parquet
      +- SubqueryAlias `tpcds`.`date_dim`
         +- Relation[d_date_sk#42,d_date_id#43,d_date#44,d_month_seq#45,d_week_seq#46,d_quarter_seq#47,d_year#48,d_dow#49,d_moy#50,d_dom#51,d_qoy#52,d_fy_year#53,d_fy_quarter_seq#54,d_fy_week_seq#55,d_day_name#56,d_quarter_name#57,d_holiday#58,d_weekend#59,d_following_holiday#60,d_first_dom#61,d_last_dom#62,d_same_day_ly#63,d_same_day_lq#64,d_current_day#65,... 4 more fields] parquet

== Optimized Logical Plan ==
Project [i_item_sk#6, w_warehouse_sk#28]
+- Join Inner, (inv_date_sk#2 = d_date_sk#42)
   :- Project [inv_date_sk#2, i_item_sk#6, w_warehouse_sk#28]
   :  +- Join Inner, (inv_warehouse_sk#4 = w_warehouse_sk#28)
   :     :- Project [inv_date_sk#2, inv_warehouse_sk#4, i_item_sk#6]
   :     :  +- Join Inner, (inv_item_sk#3 = i_item_sk#6)
   :     :     :- Project [inv_date_sk#2, inv_item_sk#3, inv_warehouse_sk#4]
   :     :     :  +- Filter ((isnotnull(inv_item_sk#3) && isnotnull(inv_warehouse_sk#4)) && isnotnull(inv_date_sk#2))
   :     :     :     +- Relation[inv_date_sk#2,inv_item_sk#3,inv_warehouse_sk#4,inv_quantity_on_hand#5L] parquet
   :     :     +- Project [i_item_sk#6]
   :     :        +- Filter isnotnull(i_item_sk#6)
   :     :           +- Relation[i_item_sk#6,i_item_id#7,i_rec_start_date#8,i_rec_end_date#9,i_item_desc#10,i_current_price#11,i_wholesale_cost#12,i_brand_id#13,i_brand#14,i_class_id#15,i_class#16,i_category_id#17,i_category#18,i_manufact_id#19,i_manufact#20,i_size#21,i_formulation#22,i_color#23,i_units#24,i_container#25,i_manager_id#26,i_product_name#27] parquet
   :     +- Project [w_warehouse_sk#28]
   :        +- Filter isnotnull(w_warehouse_sk#28)
   :           +- Relation[w_warehouse_sk#28,w_warehouse_id#29,w_warehouse_name#30,w_warehouse_sq_ft#31,w_street_number#32,w_street_name#33,w_street_type#34,w_suite_number#35,w_city#36,w_county#37,w_state#38,w_zip#39,w_country#40,w_gmt_offset#41] parquet
   +- Project [d_date_sk#42]
      +- Filter ((isnotnull(d_year#48) && (d_year#48 = 2001)) && isnotnull(d_date_sk#42))
         +- Relation[d_date_sk#42,d_date_id#43,d_date#44,d_month_seq#45,d_week_seq#46,d_quarter_seq#47,d_year#48,d_dow#49,d_moy#50,d_dom#51,d_qoy#52,d_fy_year#53,d_fy_quarter_seq#54,d_fy_week_seq#55,d_day_name#56,d_quarter_name#57,d_holiday#58,d_weekend#59,d_following_holiday#60,d_first_dom#61,d_last_dom#62,d_same_day_ly#63,d_same_day_lq#64,d_current_day#65,... 4 more fields] parquet

== Physical Plan ==
*(4) Project [i_item_sk#6, w_warehouse_sk#28]
+- *(4) BroadcastHashJoin [inv_date_sk#2], [d_date_sk#42], Inner, BuildRight
   :- *(4) Project [inv_date_sk#2, i_item_sk#6, w_warehouse_sk#28]
   :  +- *(4) BroadcastHashJoin [inv_warehouse_sk#4], [w_warehouse_sk#28], Inner, BuildRight
   :     :- *(4) Project [inv_date_sk#2, inv_warehouse_sk#4, i_item_sk#6]
   :     :  +- *(4) BroadcastHashJoin [inv_item_sk#3], [i_item_sk#6], Inner, BuildRight
   :     :     :- *(4) Project [inv_date_sk#2, inv_item_sk#3, inv_warehouse_sk#4]
   :     :     :  +- *(4) Filter ((isnotnull(inv_item_sk#3) && isnotnull(inv_warehouse_sk#4)) && isnotnull(inv_date_sk#2))
   :     :     :     +- *(4) FileScan parquet tpcds.inventory[inv_date_sk#2,inv_item_sk#3,inv_warehouse_sk#4] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_item_sk), IsNotNull(inv_warehouse_sk), IsNotNull(inv_date_sk)], ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_warehouse_sk:int>
   :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
   :     :        +- *(1) Project [i_item_sk#6]
   :     :           +- *(1) Filter isnotnull(i_item_sk#6)
   :     :              +- *(1) FileScan parquet tpcds.item[i_item_sk#6] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int>
   :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
   :        +- *(2) Project [w_warehouse_sk#28]
   :           +- *(2) Filter isnotnull(w_warehouse_sk#28)
   :              +- *(2) FileScan parquet tpcds.warehouse[w_warehouse_sk#28] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int>
   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      +- *(3) Project [d_date_sk#42]
         +- *(3) Filter ((isnotnull(d_year#48) && (d_year#48 = 2001)) && isnotnull(d_date_sk#42))
            +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#42,d_year#48] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
Time taken: 3.345 seconds, Fetched 1 row(s)
