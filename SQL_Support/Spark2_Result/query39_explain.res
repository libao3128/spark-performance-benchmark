== Parsed Logical Plan ==
CTE [inv]
:  +- 'SubqueryAlias `inv`
:     +- 'Project ['w_warehouse_name, 'w_warehouse_sk, 'i_item_sk, 'd_moy, 'stdev, 'mean, CASE WHEN ('mean = 0) THEN null ELSE ('stdev / 'mean) END AS cov#7]
:        +- 'Filter (CASE WHEN ('mean = 0) THEN 0 ELSE ('stdev / 'mean) END > 1)
:           +- 'SubqueryAlias `foo`
:              +- 'Aggregate ['w_warehouse_name, 'w_warehouse_sk, 'i_item_sk, 'd_moy], ['w_warehouse_name, 'w_warehouse_sk, 'i_item_sk, 'd_moy, 'stddev_samp('inv_quantity_on_hand) AS stdev#5, 'avg('inv_quantity_on_hand) AS mean#6]
:                 +- 'Filter ((('inv_item_sk = 'i_item_sk) && ('inv_warehouse_sk = 'w_warehouse_sk)) && (('inv_date_sk = 'd_date_sk) && ('d_year = 2001)))
:                    +- 'Join Inner
:                       :- 'Join Inner
:                       :  :- 'Join Inner
:                       :  :  :- 'UnresolvedRelation `inventory`
:                       :  :  +- 'UnresolvedRelation `item`
:                       :  +- 'UnresolvedRelation `warehouse`
:                       +- 'UnresolvedRelation `date_dim`
+- 'Sort ['inv1.w_warehouse_sk ASC NULLS FIRST, 'inv1.i_item_sk ASC NULLS FIRST, 'inv1.d_moy ASC NULLS FIRST, 'inv1.mean ASC NULLS FIRST, 'inv1.cov ASC NULLS FIRST, 'd_moy_2 ASC NULLS FIRST, 'mean_2 ASC NULLS FIRST, 'cov_2 ASC NULLS FIRST], true
   +- 'Project ['inv1.w_warehouse_sk, 'inv1.i_item_sk, 'inv1.d_moy, 'inv1.mean, 'inv1.cov, 'inv2.w_warehouse_sk AS w_warehouse_sk_2#0, 'inv2.i_item_sk AS i_item_sk_2#1, 'inv2.d_moy AS d_moy_2#2, 'inv2.mean AS mean_2#3, 'inv2.cov AS cov_2#4]
      +- 'Filter ((('inv1.i_item_sk = 'inv2.i_item_sk) && ('inv1.w_warehouse_sk = 'inv2.w_warehouse_sk)) && (('inv1.d_moy = 1) && ('inv2.d_moy = (1 + 1))))
         +- 'Join Inner
            :- 'SubqueryAlias `inv1`
            :  +- 'UnresolvedRelation `inv`
            +- 'SubqueryAlias `inv2`
               +- 'UnresolvedRelation `inv`

== Analyzed Logical Plan ==
w_warehouse_sk: int, i_item_sk: int, d_moy: int, mean: double, cov: double, w_warehouse_sk_2: int, i_item_sk_2: int, d_moy_2: int, mean_2: double, cov_2: double
Sort [w_warehouse_sk#36 ASC NULLS FIRST, i_item_sk#14 ASC NULLS FIRST, d_moy#58 ASC NULLS FIRST, mean#6 ASC NULLS FIRST, cov#7 ASC NULLS FIRST, d_moy_2#2 ASC NULLS FIRST, mean_2#3 ASC NULLS FIRST, cov_2#4 ASC NULLS FIRST], true
+- Project [w_warehouse_sk#36, i_item_sk#14, d_moy#58, mean#6, cov#7, w_warehouse_sk#301 AS w_warehouse_sk_2#0, i_item_sk#237 AS i_item_sk_2#1, d_moy#351 AS d_moy_2#2, mean#172 AS mean_2#3, cov#104 AS cov_2#4]
   +- Filter (((i_item_sk#14 = i_item_sk#237) && (w_warehouse_sk#36 = w_warehouse_sk#301)) && ((d_moy#58 = 1) && (d_moy#351 = (1 + 1))))
      +- Join Inner
         :- SubqueryAlias `inv1`
         :  +- SubqueryAlias `inv`
         :     +- Project [w_warehouse_name#38, w_warehouse_sk#36, i_item_sk#14, d_moy#58, stdev#5, mean#6, CASE WHEN (mean#6 = cast(0 as double)) THEN cast(null as double) ELSE (stdev#5 / mean#6) END AS cov#7]
         :        +- Filter (CASE WHEN (mean#6 = cast(0 as double)) THEN cast(0 as double) ELSE (stdev#5 / mean#6) END > cast(1 as double))
         :           +- SubqueryAlias `foo`
         :              +- Aggregate [w_warehouse_name#38, w_warehouse_sk#36, i_item_sk#14, d_moy#58], [w_warehouse_name#38, w_warehouse_sk#36, i_item_sk#14, d_moy#58, stddev_samp(cast(inv_quantity_on_hand#13L as double)) AS stdev#5, avg(inv_quantity_on_hand#13L) AS mean#6]
         :                 +- Filter (((inv_item_sk#11 = i_item_sk#14) && (inv_warehouse_sk#12 = w_warehouse_sk#36)) && ((inv_date_sk#10 = d_date_sk#50) && (d_year#56 = 2001)))
         :                    +- Join Inner
         :                       :- Join Inner
         :                       :  :- Join Inner
         :                       :  :  :- SubqueryAlias `tpcds`.`inventory`
         :                       :  :  :  +- Relation[inv_date_sk#10,inv_item_sk#11,inv_warehouse_sk#12,inv_quantity_on_hand#13L] parquet
         :                       :  :  +- SubqueryAlias `tpcds`.`item`
         :                       :  :     +- Relation[i_item_sk#14,i_item_id#15,i_rec_start_date#16,i_rec_end_date#17,i_item_desc#18,i_current_price#19,i_wholesale_cost#20,i_brand_id#21,i_brand#22,i_class_id#23,i_class#24,i_category_id#25,i_category#26,i_manufact_id#27,i_manufact#28,i_size#29,i_formulation#30,i_color#31,i_units#32,i_container#33,i_manager_id#34,i_product_name#35] parquet
         :                       :  +- SubqueryAlias `tpcds`.`warehouse`
         :                       :     +- Relation[w_warehouse_sk#36,w_warehouse_id#37,w_warehouse_name#38,w_warehouse_sq_ft#39,w_street_number#40,w_street_name#41,w_street_type#42,w_suite_number#43,w_city#44,w_county#45,w_state#46,w_zip#47,w_country#48,w_gmt_offset#49] parquet
         :                       +- SubqueryAlias `tpcds`.`date_dim`
         :                          +- Relation[d_date_sk#50,d_date_id#51,d_date#52,d_month_seq#53,d_week_seq#54,d_quarter_seq#55,d_year#56,d_dow#57,d_moy#58,d_dom#59,d_qoy#60,d_fy_year#61,d_fy_quarter_seq#62,d_fy_week_seq#63,d_day_name#64,d_quarter_name#65,d_holiday#66,d_weekend#67,d_following_holiday#68,d_first_dom#69,d_last_dom#70,d_same_day_ly#71,d_same_day_lq#72,d_current_day#73,... 4 more fields] parquet
         +- SubqueryAlias `inv2`
            +- SubqueryAlias `inv`
               +- Project [w_warehouse_name#303, w_warehouse_sk#301, i_item_sk#237, d_moy#351, stdev#171, mean#172, CASE WHEN (mean#172 = cast(0 as double)) THEN cast(null as double) ELSE (stdev#171 / mean#172) END AS cov#104]
                  +- Filter (CASE WHEN (mean#172 = cast(0 as double)) THEN cast(0 as double) ELSE (stdev#171 / mean#172) END > cast(1 as double))
                     +- SubqueryAlias `foo`
                        +- Aggregate [w_warehouse_name#303, w_warehouse_sk#301, i_item_sk#237, d_moy#351], [w_warehouse_name#303, w_warehouse_sk#301, i_item_sk#237, d_moy#351, stddev_samp(cast(inv_quantity_on_hand#13L as double)) AS stdev#171, avg(inv_quantity_on_hand#13L) AS mean#172]
                           +- Filter (((inv_item_sk#11 = i_item_sk#237) && (inv_warehouse_sk#12 = w_warehouse_sk#301)) && ((inv_date_sk#10 = d_date_sk#343) && (d_year#349 = 2001)))
                              +- Join Inner
                                 :- Join Inner
                                 :  :- Join Inner
                                 :  :  :- SubqueryAlias `tpcds`.`inventory`
                                 :  :  :  +- Relation[inv_date_sk#10,inv_item_sk#11,inv_warehouse_sk#12,inv_quantity_on_hand#13L] parquet
                                 :  :  +- SubqueryAlias `tpcds`.`item`
                                 :  :     +- Relation[i_item_sk#237,i_item_id#238,i_rec_start_date#239,i_rec_end_date#240,i_item_desc#241,i_current_price#242,i_wholesale_cost#243,i_brand_id#244,i_brand#245,i_class_id#246,i_class#247,i_category_id#248,i_category#249,i_manufact_id#250,i_manufact#251,i_size#252,i_formulation#253,i_color#254,i_units#255,i_container#256,i_manager_id#257,i_product_name#258] parquet
                                 :  +- SubqueryAlias `tpcds`.`warehouse`
                                 :     +- Relation[w_warehouse_sk#301,w_warehouse_id#302,w_warehouse_name#303,w_warehouse_sq_ft#304,w_street_number#305,w_street_name#306,w_street_type#307,w_suite_number#308,w_city#309,w_county#310,w_state#311,w_zip#312,w_country#313,w_gmt_offset#314] parquet
                                 +- SubqueryAlias `tpcds`.`date_dim`
                                    +- Relation[d_date_sk#343,d_date_id#344,d_date#345,d_month_seq#346,d_week_seq#347,d_quarter_seq#348,d_year#349,d_dow#350,d_moy#351,d_dom#352,d_qoy#353,d_fy_year#354,d_fy_quarter_seq#355,d_fy_week_seq#356,d_day_name#357,d_quarter_name#358,d_holiday#359,d_weekend#360,d_following_holiday#361,d_first_dom#362,d_last_dom#363,d_same_day_ly#364,d_same_day_lq#365,d_current_day#366,... 4 more fields] parquet

== Optimized Logical Plan ==
Sort [w_warehouse_sk#36 ASC NULLS FIRST, i_item_sk#14 ASC NULLS FIRST, d_moy#58 ASC NULLS FIRST, mean#6 ASC NULLS FIRST, cov#7 ASC NULLS FIRST, d_moy_2#2 ASC NULLS FIRST, mean_2#3 ASC NULLS FIRST, cov_2#4 ASC NULLS FIRST], true
+- Project [w_warehouse_sk#36, i_item_sk#14, d_moy#58, mean#6, cov#7, w_warehouse_sk#301 AS w_warehouse_sk_2#0, i_item_sk#237 AS i_item_sk_2#1, d_moy#351 AS d_moy_2#2, mean#172 AS mean_2#3, cov#104 AS cov_2#4]
   +- Join Inner, ((i_item_sk#14 = i_item_sk#237) && (w_warehouse_sk#36 = w_warehouse_sk#301))
      :- Project [w_warehouse_sk#36, i_item_sk#14, d_moy#58, mean#6, CASE WHEN (mean#6 = 0.0) THEN null ELSE (stdev#5 / mean#6) END AS cov#7]
      :  +- Filter (CASE WHEN (mean#6 = 0.0) THEN 0.0 ELSE (stdev#5 / mean#6) END > 1.0)
      :     +- Aggregate [w_warehouse_name#38, w_warehouse_sk#36, i_item_sk#14, d_moy#58], [w_warehouse_sk#36, i_item_sk#14, d_moy#58, stddev_samp(cast(inv_quantity_on_hand#13L as double)) AS stdev#5, avg(inv_quantity_on_hand#13L) AS mean#6]
      :        +- Project [inv_quantity_on_hand#13L, i_item_sk#14, w_warehouse_sk#36, w_warehouse_name#38, d_moy#58]
      :           +- Join Inner, (inv_date_sk#10 = d_date_sk#50)
      :              :- Project [inv_date_sk#10, inv_quantity_on_hand#13L, i_item_sk#14, w_warehouse_sk#36, w_warehouse_name#38]
      :              :  +- Join Inner, (inv_warehouse_sk#12 = w_warehouse_sk#36)
      :              :     :- Project [inv_date_sk#10, inv_warehouse_sk#12, inv_quantity_on_hand#13L, i_item_sk#14]
      :              :     :  +- Join Inner, (inv_item_sk#11 = i_item_sk#14)
      :              :     :     :- Filter ((isnotnull(inv_item_sk#11) && isnotnull(inv_warehouse_sk#12)) && isnotnull(inv_date_sk#10))
      :              :     :     :  +- Relation[inv_date_sk#10,inv_item_sk#11,inv_warehouse_sk#12,inv_quantity_on_hand#13L] parquet
      :              :     :     +- Project [i_item_sk#14]
      :              :     :        +- Filter isnotnull(i_item_sk#14)
      :              :     :           +- Relation[i_item_sk#14,i_item_id#15,i_rec_start_date#16,i_rec_end_date#17,i_item_desc#18,i_current_price#19,i_wholesale_cost#20,i_brand_id#21,i_brand#22,i_class_id#23,i_class#24,i_category_id#25,i_category#26,i_manufact_id#27,i_manufact#28,i_size#29,i_formulation#30,i_color#31,i_units#32,i_container#33,i_manager_id#34,i_product_name#35] parquet
      :              :     +- Project [w_warehouse_sk#36, w_warehouse_name#38]
      :              :        +- Filter isnotnull(w_warehouse_sk#36)
      :              :           +- Relation[w_warehouse_sk#36,w_warehouse_id#37,w_warehouse_name#38,w_warehouse_sq_ft#39,w_street_number#40,w_street_name#41,w_street_type#42,w_suite_number#43,w_city#44,w_county#45,w_state#46,w_zip#47,w_country#48,w_gmt_offset#49] parquet
      :              +- Project [d_date_sk#50, d_moy#58]
      :                 +- Filter ((((isnotnull(d_year#56) && isnotnull(d_moy#58)) && (d_year#56 = 2001)) && (d_moy#58 = 1)) && isnotnull(d_date_sk#50))
      :                    +- Relation[d_date_sk#50,d_date_id#51,d_date#52,d_month_seq#53,d_week_seq#54,d_quarter_seq#55,d_year#56,d_dow#57,d_moy#58,d_dom#59,d_qoy#60,d_fy_year#61,d_fy_quarter_seq#62,d_fy_week_seq#63,d_day_name#64,d_quarter_name#65,d_holiday#66,d_weekend#67,d_following_holiday#68,d_first_dom#69,d_last_dom#70,d_same_day_ly#71,d_same_day_lq#72,d_current_day#73,... 4 more fields] parquet
      +- Project [w_warehouse_sk#301, i_item_sk#237, d_moy#351, mean#172, CASE WHEN (mean#172 = 0.0) THEN null ELSE (stdev#171 / mean#172) END AS cov#104]
         +- Filter (CASE WHEN (mean#172 = 0.0) THEN 0.0 ELSE (stdev#171 / mean#172) END > 1.0)
            +- Aggregate [w_warehouse_name#303, w_warehouse_sk#301, i_item_sk#237, d_moy#351], [w_warehouse_sk#301, i_item_sk#237, d_moy#351, stddev_samp(cast(inv_quantity_on_hand#13L as double)) AS stdev#171, avg(inv_quantity_on_hand#13L) AS mean#172]
               +- Project [inv_quantity_on_hand#13L, i_item_sk#237, w_warehouse_sk#301, w_warehouse_name#303, d_moy#351]
                  +- Join Inner, (inv_date_sk#10 = d_date_sk#343)
                     :- Project [inv_date_sk#10, inv_quantity_on_hand#13L, i_item_sk#237, w_warehouse_sk#301, w_warehouse_name#303]
                     :  +- Join Inner, (inv_warehouse_sk#12 = w_warehouse_sk#301)
                     :     :- Project [inv_date_sk#10, inv_warehouse_sk#12, inv_quantity_on_hand#13L, i_item_sk#237]
                     :     :  +- Join Inner, (inv_item_sk#11 = i_item_sk#237)
                     :     :     :- Filter ((isnotnull(inv_item_sk#11) && isnotnull(inv_warehouse_sk#12)) && isnotnull(inv_date_sk#10))
                     :     :     :  +- Relation[inv_date_sk#10,inv_item_sk#11,inv_warehouse_sk#12,inv_quantity_on_hand#13L] parquet
                     :     :     +- Project [i_item_sk#237]
                     :     :        +- Filter isnotnull(i_item_sk#237)
                     :     :           +- Relation[i_item_sk#237,i_item_id#238,i_rec_start_date#239,i_rec_end_date#240,i_item_desc#241,i_current_price#242,i_wholesale_cost#243,i_brand_id#244,i_brand#245,i_class_id#246,i_class#247,i_category_id#248,i_category#249,i_manufact_id#250,i_manufact#251,i_size#252,i_formulation#253,i_color#254,i_units#255,i_container#256,i_manager_id#257,i_product_name#258] parquet
                     :     +- Project [w_warehouse_sk#301, w_warehouse_name#303]
                     :        +- Filter isnotnull(w_warehouse_sk#301)
                     :           +- Relation[w_warehouse_sk#301,w_warehouse_id#302,w_warehouse_name#303,w_warehouse_sq_ft#304,w_street_number#305,w_street_name#306,w_street_type#307,w_suite_number#308,w_city#309,w_county#310,w_state#311,w_zip#312,w_country#313,w_gmt_offset#314] parquet
                     +- Project [d_date_sk#343, d_moy#351]
                        +- Filter ((((isnotnull(d_year#349) && isnotnull(d_moy#351)) && (d_year#349 = 2001)) && (d_moy#351 = 2)) && isnotnull(d_date_sk#343))
                           +- Relation[d_date_sk#343,d_date_id#344,d_date#345,d_month_seq#346,d_week_seq#347,d_quarter_seq#348,d_year#349,d_dow#350,d_moy#351,d_dom#352,d_qoy#353,d_fy_year#354,d_fy_quarter_seq#355,d_fy_week_seq#356,d_day_name#357,d_quarter_name#358,d_holiday#359,d_weekend#360,d_following_holiday#361,d_first_dom#362,d_last_dom#363,d_same_day_ly#364,d_same_day_lq#365,d_current_day#366,... 4 more fields] parquet

== Physical Plan ==
*(14) Sort [w_warehouse_sk#36 ASC NULLS FIRST, i_item_sk#14 ASC NULLS FIRST, d_moy#58 ASC NULLS FIRST, mean#6 ASC NULLS FIRST, cov#7 ASC NULLS FIRST, d_moy_2#2 ASC NULLS FIRST, mean_2#3 ASC NULLS FIRST, cov_2#4 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(w_warehouse_sk#36 ASC NULLS FIRST, i_item_sk#14 ASC NULLS FIRST, d_moy#58 ASC NULLS FIRST, mean#6 ASC NULLS FIRST, cov#7 ASC NULLS FIRST, d_moy_2#2 ASC NULLS FIRST, mean_2#3 ASC NULLS FIRST, cov_2#4 ASC NULLS FIRST, 200)
   +- *(13) Project [w_warehouse_sk#36, i_item_sk#14, d_moy#58, mean#6, cov#7, w_warehouse_sk#301 AS w_warehouse_sk_2#0, i_item_sk#237 AS i_item_sk_2#1, d_moy#351 AS d_moy_2#2, mean#172 AS mean_2#3, cov#104 AS cov_2#4]
      +- *(13) SortMergeJoin [i_item_sk#14, w_warehouse_sk#36], [i_item_sk#237, w_warehouse_sk#301], Inner
         :- *(6) Sort [i_item_sk#14 ASC NULLS FIRST, w_warehouse_sk#36 ASC NULLS FIRST], false, 0
         :  +- Exchange hashpartitioning(i_item_sk#14, w_warehouse_sk#36, 200)
         :     +- *(5) Project [w_warehouse_sk#36, i_item_sk#14, d_moy#58, mean#6, CASE WHEN (mean#6 = 0.0) THEN null ELSE (stdev#5 / mean#6) END AS cov#7]
         :        +- *(5) Filter (CASE WHEN (mean#6 = 0.0) THEN 0.0 ELSE (stdev#5 / mean#6) END > 1.0)
         :           +- *(5) HashAggregate(keys=[w_warehouse_name#38, w_warehouse_sk#36, i_item_sk#14, d_moy#58], functions=[stddev_samp(cast(inv_quantity_on_hand#13L as double)), avg(inv_quantity_on_hand#13L)], output=[w_warehouse_sk#36, i_item_sk#14, d_moy#58, stdev#5, mean#6])
         :              +- Exchange hashpartitioning(w_warehouse_name#38, w_warehouse_sk#36, i_item_sk#14, d_moy#58, 200)
         :                 +- *(4) HashAggregate(keys=[w_warehouse_name#38, w_warehouse_sk#36, i_item_sk#14, d_moy#58], functions=[partial_stddev_samp(cast(inv_quantity_on_hand#13L as double)), partial_avg(inv_quantity_on_hand#13L)], output=[w_warehouse_name#38, w_warehouse_sk#36, i_item_sk#14, d_moy#58, n#101, avg#102, m2#103, sum#381, count#382L])
         :                    +- *(4) Project [inv_quantity_on_hand#13L, i_item_sk#14, w_warehouse_sk#36, w_warehouse_name#38, d_moy#58]
         :                       +- *(4) BroadcastHashJoin [inv_date_sk#10], [d_date_sk#50], Inner, BuildRight
         :                          :- *(4) Project [inv_date_sk#10, inv_quantity_on_hand#13L, i_item_sk#14, w_warehouse_sk#36, w_warehouse_name#38]
         :                          :  +- *(4) BroadcastHashJoin [inv_warehouse_sk#12], [w_warehouse_sk#36], Inner, BuildRight
         :                          :     :- *(4) Project [inv_date_sk#10, inv_warehouse_sk#12, inv_quantity_on_hand#13L, i_item_sk#14]
         :                          :     :  +- *(4) BroadcastHashJoin [inv_item_sk#11], [i_item_sk#14], Inner, BuildRight
         :                          :     :     :- *(4) Project [inv_date_sk#10, inv_item_sk#11, inv_warehouse_sk#12, inv_quantity_on_hand#13L]
         :                          :     :     :  +- *(4) Filter ((isnotnull(inv_item_sk#11) && isnotnull(inv_warehouse_sk#12)) && isnotnull(inv_date_sk#10))
         :                          :     :     :     +- *(4) FileScan parquet tpcds.inventory[inv_date_sk#10,inv_item_sk#11,inv_warehouse_sk#12,inv_quantity_on_hand#13L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_item_sk), IsNotNull(inv_warehouse_sk), IsNotNull(inv_date_sk)], ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_warehouse_sk:int,inv_quantity_on_hand:bigint>
         :                          :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                          :     :        +- *(1) Project [i_item_sk#14]
         :                          :     :           +- *(1) Filter isnotnull(i_item_sk#14)
         :                          :     :              +- *(1) FileScan parquet tpcds.item[i_item_sk#14] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int>
         :                          :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                          :        +- *(2) Project [w_warehouse_sk#36, w_warehouse_name#38]
         :                          :           +- *(2) Filter isnotnull(w_warehouse_sk#36)
         :                          :              +- *(2) FileScan parquet tpcds.warehouse[w_warehouse_sk#36,w_warehouse_name#38] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
         :                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                             +- *(3) Project [d_date_sk#50, d_moy#58]
         :                                +- *(3) Filter ((((isnotnull(d_year#56) && isnotnull(d_moy#58)) && (d_year#56 = 2001)) && (d_moy#58 = 1)) && isnotnull(d_date_sk#50))
         :                                   +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#50,d_year#56,d_moy#58] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,1), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
         +- *(12) Sort [i_item_sk#237 ASC NULLS FIRST, w_warehouse_sk#301 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(i_item_sk#237, w_warehouse_sk#301, 200)
               +- *(11) Project [w_warehouse_sk#301, i_item_sk#237, d_moy#351, mean#172, CASE WHEN (mean#172 = 0.0) THEN null ELSE (stdev#171 / mean#172) END AS cov#104]
                  +- *(11) Filter (CASE WHEN (mean#172 = 0.0) THEN 0.0 ELSE (stdev#171 / mean#172) END > 1.0)
                     +- *(11) HashAggregate(keys=[w_warehouse_name#303, w_warehouse_sk#301, i_item_sk#237, d_moy#351], functions=[stddev_samp(cast(inv_quantity_on_hand#13L as double)), avg(inv_quantity_on_hand#13L)], output=[w_warehouse_sk#301, i_item_sk#237, d_moy#351, stdev#171, mean#172])
                        +- Exchange hashpartitioning(w_warehouse_name#303, w_warehouse_sk#301, i_item_sk#237, d_moy#351, 200)
                           +- *(10) HashAggregate(keys=[w_warehouse_name#303, w_warehouse_sk#301, i_item_sk#237, d_moy#351], functions=[partial_stddev_samp(cast(inv_quantity_on_hand#13L as double)), partial_avg(inv_quantity_on_hand#13L)], output=[w_warehouse_name#303, w_warehouse_sk#301, i_item_sk#237, d_moy#351, n#101, avg#102, m2#103, sum#381, count#382L])
                              +- *(10) Project [inv_quantity_on_hand#13L, i_item_sk#237, w_warehouse_sk#301, w_warehouse_name#303, d_moy#351]
                                 +- *(10) BroadcastHashJoin [inv_date_sk#10], [d_date_sk#343], Inner, BuildRight
                                    :- *(10) Project [inv_date_sk#10, inv_quantity_on_hand#13L, i_item_sk#237, w_warehouse_sk#301, w_warehouse_name#303]
                                    :  +- *(10) BroadcastHashJoin [inv_warehouse_sk#12], [w_warehouse_sk#301], Inner, BuildRight
                                    :     :- *(10) Project [inv_date_sk#10, inv_warehouse_sk#12, inv_quantity_on_hand#13L, i_item_sk#237]
                                    :     :  +- *(10) BroadcastHashJoin [inv_item_sk#11], [i_item_sk#237], Inner, BuildRight
                                    :     :     :- *(10) Project [inv_date_sk#10, inv_item_sk#11, inv_warehouse_sk#12, inv_quantity_on_hand#13L]
                                    :     :     :  +- *(10) Filter ((isnotnull(inv_item_sk#11) && isnotnull(inv_warehouse_sk#12)) && isnotnull(inv_date_sk#10))
                                    :     :     :     +- *(10) FileScan parquet tpcds.inventory[inv_date_sk#10,inv_item_sk#11,inv_warehouse_sk#12,inv_quantity_on_hand#13L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_item_sk), IsNotNull(inv_warehouse_sk), IsNotNull(inv_date_sk)], ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_warehouse_sk:int,inv_quantity_on_hand:bigint>
                                    :     :     +- ReusedExchange [i_item_sk#237], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                    :     +- ReusedExchange [w_warehouse_sk#301, w_warehouse_name#303], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                       +- *(9) Project [d_date_sk#343, d_moy#351]
                                          +- *(9) Filter ((((isnotnull(d_year#349) && isnotnull(d_moy#351)) && (d_year#349 = 2001)) && (d_moy#351 = 2)) && isnotnull(d_date_sk#343))
                                             +- *(9) FileScan parquet tpcds.date_dim[d_date_sk#343,d_year#349,d_moy#351] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,2), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
Time taken: 4.731 seconds, Fetched 1 row(s)
1	1155	1	184.0	NaN	1	1155	2	343.3333333333333	1.1700233592269733
1	1569	1	212.0	1.630213519639535	1	1569	2	239.25	1.2641513267800557
1	5627	1	282.75	1.5657032366359889	1	5627	2	297.5	1.2084286841430678
1	7999	1	166.25	1.7924231710846223	1	7999	2	375.3333333333333	1.008092263550718
1	8611	1	300.5	1.5191545184147954	1	8611	2	243.75	1.2342122780960432
1	15345	1	148.5	1.5295784035794024	1	15345	2	246.5	1.5087987747231526
2	71	1	221.5	1.563974108334745	2	71	2	309.0	1.4917057895885681
2	5429	1	152.0	NaN	2	5429	2	355.25	1.1230959744909077
2	6103	1	194.33333333333334	1.5160670179307387	2	6103	2	158.5	1.2743698636165062
2	6489	1	268.0	1.6956372368432269	2	6489	2	389.0	1.4105780519299767
2	15839	1	353.0	1.5063684437542906	2	15839	2	255.5	1.2362393182894105
3	7207	1	329.6666666666667	1.5954482160720393	3	7207	2	414.5	1.017919707908937
3	10547	1	182.33333333333334	1.5325641514869042	3	10547	2	320.25	1.302441844373152
3	12867	1	278.25	1.6403800123947352	3	12867	2	350.75	1.2006933321742796
4	947	1	247.5	1.6933181813486973	4	947	2	203.33333333333334	1.205433145161931
4	10249	1	240.75	1.6058403557773568	4	10249	2	689.0	NaN
5	3137	1	271.25	1.575453220592864	5	3137	2	380.0	1.0834203388600319
Time taken: 13.593 seconds, Fetched 17 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 39 in stream 0 using template query39.tpl
------------------------------------------------------^^^

