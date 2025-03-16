Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580783813
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Project ['sum('cs_ext_discount_amt) AS excess_discount_amount#0]
      +- 'Filter (((('i_manufact_id = 977) AND ('i_item_sk = 'cs_item_sk)) AND (('d_date >= 2000-01-27) AND ('d_date <= 'date_add(cast(2000-01-27 as date), 90)))) AND (('d_date_sk = 'cs_sold_date_sk) AND ('cs_ext_discount_amt > scalar-subquery#1 [])))
         :  +- 'Project [unresolvedalias((1.3 * 'avg('cs_ext_discount_amt)), None)]
         :     +- 'Filter ((('cs_item_sk = 'i_item_sk) AND (('d_date >= 2000-01-27) AND ('d_date <= 'date_add(cast(2000-01-27 as date), 90)))) AND ('d_date_sk = 'cs_sold_date_sk))
         :        +- 'Join Inner
         :           :- 'UnresolvedRelation [catalog_sales], [], false
         :           +- 'UnresolvedRelation [date_dim], [], false
         +- 'Join Inner
            :- 'Join Inner
            :  :- 'UnresolvedRelation [catalog_sales], [], false
            :  +- 'UnresolvedRelation [item], [], false
            +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
excess_discount_amount: double
GlobalLimit 100
+- LocalLimit 100
   +- Aggregate [sum(cs_ext_discount_amt#29) AS excess_discount_amount#0]
      +- Filter ((((i_manufact_id#54 = 977) AND (i_item_sk#41 = cs_item_sk#22)) AND ((d_date#65 >= 2000-01-27) AND (cast(d_date#65 as date) <= date_add(cast(2000-01-27 as date), 90)))) AND ((d_date_sk#63 = cs_sold_date_sk#7) AND (cs_ext_discount_amt#29 > scalar-subquery#1 [i_item_sk#41])))
         :  +- Aggregate [(cast(1.3 as double) * avg(cs_ext_discount_amt#117)) AS (1.3 * avg(cs_ext_discount_amt))#94]
         :     +- Filter (((cs_item_sk#110 = outer(i_item_sk#41)) AND ((d_date#131 >= 2000-01-27) AND (cast(d_date#131 as date) <= date_add(cast(2000-01-27 as date), 90)))) AND (d_date_sk#129 = cs_sold_date_sk#95))
         :        +- Join Inner
         :           :- SubqueryAlias spark_catalog.tpcds.catalog_sales
         :           :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#95,cs_sold_time_sk#96,cs_ship_date_sk#97,cs_bill_customer_sk#98,cs_bill_cdemo_sk#99,cs_bill_hdemo_sk#100,cs_bill_addr_sk#101,cs_ship_customer_sk#102,cs_ship_cdemo_sk#103,cs_ship_hdemo_sk#104,cs_ship_addr_sk#105,cs_call_center_sk#106,cs_catalog_page_sk#107,cs_ship_mode_sk#108,cs_warehouse_sk#109,cs_item_sk#110,cs_promo_sk#111,cs_order_number#112,cs_quantity#113,cs_wholesale_cost#114,cs_list_price#115,cs_sales_price#116,cs_ext_discount_amt#117,cs_ext_sales_price#118,... 10 more fields] parquet
         :           +- SubqueryAlias spark_catalog.tpcds.date_dim
         :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#129,d_date_id#130,d_date#131,d_month_seq#132,d_week_seq#133,d_quarter_seq#134,d_year#135,d_dow#136,d_moy#137,d_dom#138,d_qoy#139,d_fy_year#140,d_fy_quarter_seq#141,d_fy_week_seq#142,d_day_name#143,d_quarter_name#144,d_holiday#145,d_weekend#146,d_following_holiday#147,d_first_dom#148,d_last_dom#149,d_same_day_ly#150,d_same_day_lq#151,d_current_day#152,... 4 more fields] parquet
         +- Join Inner
            :- Join Inner
            :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
            :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
            :  +- SubqueryAlias spark_catalog.tpcds.item
            :     +- Relation spark_catalog.tpcds.item[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
            +- SubqueryAlias spark_catalog.tpcds.date_dim
               +- Relation spark_catalog.tpcds.date_dim[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet

== Optimized Logical Plan ==
Aggregate [sum(cs_ext_discount_amt#29) AS excess_discount_amount#0]
+- Project [cs_ext_discount_amt#29]
   +- Join Inner, (d_date_sk#63 = cs_sold_date_sk#7)
      :- Project [cs_sold_date_sk#7, cs_ext_discount_amt#29]
      :  +- Join Inner, ((cs_ext_discount_amt#29 > (1.3 * avg(cs_ext_discount_amt))#94) AND (cs_item_sk#110 = i_item_sk#41))
      :     :- Project [cs_sold_date_sk#7, cs_ext_discount_amt#29, i_item_sk#41]
      :     :  +- Join Inner, (i_item_sk#41 = cs_item_sk#22)
      :     :     :- Project [cs_sold_date_sk#7, cs_item_sk#22, cs_ext_discount_amt#29]
      :     :     :  +- Filter (isnotnull(cs_item_sk#22) AND (isnotnull(cs_ext_discount_amt#29) AND isnotnull(cs_sold_date_sk#7)))
      :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
      :     :     +- Project [i_item_sk#41]
      :     :        +- Filter ((isnotnull(i_manufact_id#54) AND (i_manufact_id#54 = 977)) AND isnotnull(i_item_sk#41))
      :     :           +- Relation spark_catalog.tpcds.item[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
      :     +- Filter isnotnull((1.3 * avg(cs_ext_discount_amt))#94)
      :        +- Aggregate [cs_item_sk#110], [(1.3 * avg(cs_ext_discount_amt#117)) AS (1.3 * avg(cs_ext_discount_amt))#94, cs_item_sk#110]
      :           +- Project [cs_item_sk#110, cs_ext_discount_amt#117]
      :              +- Join Inner, (d_date_sk#129 = cs_sold_date_sk#95)
      :                 :- Project [cs_sold_date_sk#95, cs_item_sk#110, cs_ext_discount_amt#117]
      :                 :  +- Filter (isnotnull(cs_sold_date_sk#95) AND isnotnull(cs_item_sk#110))
      :                 :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#95,cs_sold_time_sk#96,cs_ship_date_sk#97,cs_bill_customer_sk#98,cs_bill_cdemo_sk#99,cs_bill_hdemo_sk#100,cs_bill_addr_sk#101,cs_ship_customer_sk#102,cs_ship_cdemo_sk#103,cs_ship_hdemo_sk#104,cs_ship_addr_sk#105,cs_call_center_sk#106,cs_catalog_page_sk#107,cs_ship_mode_sk#108,cs_warehouse_sk#109,cs_item_sk#110,cs_promo_sk#111,cs_order_number#112,cs_quantity#113,cs_wholesale_cost#114,cs_list_price#115,cs_sales_price#116,cs_ext_discount_amt#117,cs_ext_sales_price#118,... 10 more fields] parquet
      :                 +- Project [d_date_sk#129]
      :                    +- Filter ((isnotnull(d_date#131) AND ((d_date#131 >= 2000-01-27) AND (cast(d_date#131 as date) <= 2000-04-26))) AND isnotnull(d_date_sk#129))
      :                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#129,d_date_id#130,d_date#131,d_month_seq#132,d_week_seq#133,d_quarter_seq#134,d_year#135,d_dow#136,d_moy#137,d_dom#138,d_qoy#139,d_fy_year#140,d_fy_quarter_seq#141,d_fy_week_seq#142,d_day_name#143,d_quarter_name#144,d_holiday#145,d_weekend#146,d_following_holiday#147,d_first_dom#148,d_last_dom#149,d_same_day_ly#150,d_same_day_lq#151,d_current_day#152,... 4 more fields] parquet
      +- Project [d_date_sk#63]
         +- Filter ((isnotnull(d_date#65) AND ((d_date#65 >= 2000-01-27) AND (cast(d_date#65 as date) <= 2000-04-26))) AND isnotnull(d_date_sk#63))
            +- Relation spark_catalog.tpcds.date_dim[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[sum(cs_ext_discount_amt#29)], output=[excess_discount_amount#0])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=133]
      +- HashAggregate(keys=[], functions=[partial_sum(cs_ext_discount_amt#29)], output=[sum#160])
         +- Project [cs_ext_discount_amt#29]
            +- BroadcastHashJoin [cs_sold_date_sk#7], [d_date_sk#63], Inner, BuildRight, false
               :- Project [cs_sold_date_sk#7, cs_ext_discount_amt#29]
               :  +- SortMergeJoin [i_item_sk#41], [cs_item_sk#110], Inner, (cs_ext_discount_amt#29 > (1.3 * avg(cs_ext_discount_amt))#94)
               :     :- Sort [i_item_sk#41 ASC NULLS FIRST], false, 0
               :     :  +- Exchange hashpartitioning(i_item_sk#41, 200), ENSURE_REQUIREMENTS, [plan_id=122]
               :     :     +- Project [cs_sold_date_sk#7, cs_ext_discount_amt#29, i_item_sk#41]
               :     :        +- BroadcastHashJoin [cs_item_sk#22], [i_item_sk#41], Inner, BuildRight, false
               :     :           :- Filter ((isnotnull(cs_item_sk#22) AND isnotnull(cs_ext_discount_amt#29)) AND isnotnull(cs_sold_date_sk#7))
               :     :           :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#7,cs_item_sk#22,cs_ext_discount_amt#29] Batched: true, DataFilters: [isnotnull(cs_item_sk#22), isnotnull(cs_ext_discount_amt#29), isnotnull(cs_sold_date_sk#7)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_ext_discount_amt), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_ext_discount_amt:double>
               :     :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=109]
               :     :              +- Project [i_item_sk#41]
               :     :                 +- Filter ((isnotnull(i_manufact_id#54) AND (i_manufact_id#54 = 977)) AND isnotnull(i_item_sk#41))
               :     :                    +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#41,i_manufact_id#54] Batched: true, DataFilters: [isnotnull(i_manufact_id#54), (i_manufact_id#54 = 977), isnotnull(i_item_sk#41)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manufact_id), EqualTo(i_manufact_id,977), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_manufact_id:int>
               :     +- Sort [cs_item_sk#110 ASC NULLS FIRST], false, 0
               :        +- Filter isnotnull((1.3 * avg(cs_ext_discount_amt))#94)
               :           +- HashAggregate(keys=[cs_item_sk#110], functions=[avg(cs_ext_discount_amt#117)], output=[(1.3 * avg(cs_ext_discount_amt))#94, cs_item_sk#110])
               :              +- Exchange hashpartitioning(cs_item_sk#110, 200), ENSURE_REQUIREMENTS, [plan_id=117]
               :                 +- HashAggregate(keys=[cs_item_sk#110], functions=[partial_avg(cs_ext_discount_amt#117)], output=[cs_item_sk#110, sum#163, count#164L])
               :                    +- Project [cs_item_sk#110, cs_ext_discount_amt#117]
               :                       +- BroadcastHashJoin [cs_sold_date_sk#95], [d_date_sk#129], Inner, BuildRight, false
               :                          :- Filter (isnotnull(cs_sold_date_sk#95) AND isnotnull(cs_item_sk#110))
               :                          :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#95,cs_item_sk#110,cs_ext_discount_amt#117] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#95), isnotnull(cs_item_sk#110)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_item_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_ext_discount_amt:double>
               :                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=112]
               :                             +- Project [d_date_sk#129]
               :                                +- Filter (((isnotnull(d_date#131) AND (d_date#131 >= 2000-01-27)) AND (cast(d_date#131 as date) <= 2000-04-26)) AND isnotnull(d_date_sk#129))
               :                                   +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#129,d_date#131] Batched: true, DataFilters: [isnotnull(d_date#131), (d_date#131 >= 2000-01-27), (cast(d_date#131 as date) <= 2000-04-26), isn..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-01-27), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=128]
                  +- Project [d_date_sk#63]
                     +- Filter (((isnotnull(d_date#65) AND (d_date#65 >= 2000-01-27)) AND (cast(d_date#65 as date) <= 2000-04-26)) AND isnotnull(d_date_sk#63))
                        +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#63,d_date#65] Batched: true, DataFilters: [isnotnull(d_date#65), (d_date#65 >= 2000-01-27), (cast(d_date#65 as date) <= 2000-04-26), isnotn..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-01-27), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>

Time taken: 2.612 seconds, Fetched 1 row(s)
