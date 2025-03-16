Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582733425
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id, 'i_item_desc, 'i_current_price], ['i_item_id, 'i_item_desc, 'i_current_price]
         +- 'Filter ((((('i_current_price >= 62) AND ('i_current_price <= (62 + 30))) AND ('inv_item_sk = 'i_item_sk)) AND (('d_date_sk = 'inv_date_sk) AND (('d_date >= cast(2000-05-25 as date)) AND ('d_date <= 'date_add(cast(2000-05-25 as date), 60))))) AND (('i_manufact_id IN (129,270,821,423) AND (('inv_quantity_on_hand >= 100) AND ('inv_quantity_on_hand <= 500))) AND ('ss_item_sk = 'i_item_sk)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation [item], [], false
               :  :  +- 'UnresolvedRelation [inventory], [], false
               :  +- 'UnresolvedRelation [date_dim], [], false
               +- 'UnresolvedRelation [store_sales], [], false

== Analyzed Logical Plan ==
i_item_id: string, i_item_desc: string, i_current_price: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#6 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#6, i_item_desc#9, i_current_price#10], [i_item_id#6, i_item_desc#9, i_current_price#10]
         +- Filter (((((i_current_price#10 >= cast(62 as double)) AND (i_current_price#10 <= cast((62 + 30) as double))) AND (inv_item_sk#28 = i_item_sk#5)) AND ((d_date_sk#31 = inv_date_sk#27) AND ((cast(d_date#33 as date) >= cast(2000-05-25 as date)) AND (cast(d_date#33 as date) <= date_add(cast(2000-05-25 as date), 60))))) AND ((i_manufact_id#18 IN (129,270,821,423) AND ((inv_quantity_on_hand#30L >= cast(100 as bigint)) AND (inv_quantity_on_hand#30L <= cast(500 as bigint)))) AND (ss_item_sk#61 = i_item_sk#5)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias spark_catalog.tpcds.item
               :  :  :  +- Relation spark_catalog.tpcds.item[i_item_sk#5,i_item_id#6,i_rec_start_date#7,i_rec_end_date#8,i_item_desc#9,i_current_price#10,i_wholesale_cost#11,i_brand_id#12,i_brand#13,i_class_id#14,i_class#15,i_category_id#16,i_category#17,i_manufact_id#18,i_manufact#19,i_size#20,i_formulation#21,i_color#22,i_units#23,i_container#24,i_manager_id#25,i_product_name#26] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.inventory
               :  :     +- Relation spark_catalog.tpcds.inventory[inv_date_sk#27,inv_item_sk#28,inv_warehouse_sk#29,inv_quantity_on_hand#30L] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#31,d_date_id#32,d_date#33,d_month_seq#34,d_week_seq#35,d_quarter_seq#36,d_year#37,d_dow#38,d_moy#39,d_dom#40,d_qoy#41,d_fy_year#42,d_fy_quarter_seq#43,d_fy_week_seq#44,d_day_name#45,d_quarter_name#46,d_holiday#47,d_weekend#48,d_following_holiday#49,d_first_dom#50,d_last_dom#51,d_same_day_ly#52,d_same_day_lq#53,d_current_day#54,... 4 more fields] parquet
               +- SubqueryAlias spark_catalog.tpcds.store_sales
                  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#59,ss_sold_time_sk#60,ss_item_sk#61,ss_customer_sk#62,ss_cdemo_sk#63,ss_hdemo_sk#64,ss_addr_sk#65,ss_store_sk#66,ss_promo_sk#67,ss_ticket_number#68,ss_quantity#69,ss_wholesale_cost#70,ss_list_price#71,ss_sales_price#72,ss_ext_discount_amt#73,ss_ext_sales_price#74,ss_ext_wholesale_cost#75,ss_ext_list_price#76,ss_ext_tax#77,ss_coupon_amt#78,ss_net_paid#79,ss_net_paid_inc_tax#80,ss_net_profit#81] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#6 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#6, i_item_desc#9, i_current_price#10], [i_item_id#6, i_item_desc#9, i_current_price#10]
         +- Project [i_item_id#6, i_item_desc#9, i_current_price#10]
            +- Join Inner, (ss_item_sk#61 = i_item_sk#5)
               :- Project [i_item_sk#5, i_item_id#6, i_item_desc#9, i_current_price#10]
               :  +- Join Inner, (d_date_sk#31 = inv_date_sk#27)
               :     :- Project [i_item_sk#5, i_item_id#6, i_item_desc#9, i_current_price#10, inv_date_sk#27]
               :     :  +- Join Inner, (inv_item_sk#28 = i_item_sk#5)
               :     :     :- Project [i_item_sk#5, i_item_id#6, i_item_desc#9, i_current_price#10]
               :     :     :  +- Filter ((isnotnull(i_current_price#10) AND (((i_current_price#10 >= 62.0) AND (i_current_price#10 <= 92.0)) AND i_manufact_id#18 IN (129,270,821,423))) AND isnotnull(i_item_sk#5))
               :     :     :     +- Relation spark_catalog.tpcds.item[i_item_sk#5,i_item_id#6,i_rec_start_date#7,i_rec_end_date#8,i_item_desc#9,i_current_price#10,i_wholesale_cost#11,i_brand_id#12,i_brand#13,i_class_id#14,i_class#15,i_category_id#16,i_category#17,i_manufact_id#18,i_manufact#19,i_size#20,i_formulation#21,i_color#22,i_units#23,i_container#24,i_manager_id#25,i_product_name#26] parquet
               :     :     +- Project [inv_date_sk#27, inv_item_sk#28]
               :     :        +- Filter ((isnotnull(inv_quantity_on_hand#30L) AND ((inv_quantity_on_hand#30L >= 100) AND (inv_quantity_on_hand#30L <= 500))) AND (isnotnull(inv_item_sk#28) AND isnotnull(inv_date_sk#27)))
               :     :           +- Relation spark_catalog.tpcds.inventory[inv_date_sk#27,inv_item_sk#28,inv_warehouse_sk#29,inv_quantity_on_hand#30L] parquet
               :     +- Project [d_date_sk#31]
               :        +- Filter ((isnotnull(d_date#33) AND ((cast(d_date#33 as date) >= 2000-05-25) AND (cast(d_date#33 as date) <= 2000-07-24))) AND isnotnull(d_date_sk#31))
               :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#31,d_date_id#32,d_date#33,d_month_seq#34,d_week_seq#35,d_quarter_seq#36,d_year#37,d_dow#38,d_moy#39,d_dom#40,d_qoy#41,d_fy_year#42,d_fy_quarter_seq#43,d_fy_week_seq#44,d_day_name#45,d_quarter_name#46,d_holiday#47,d_weekend#48,d_following_holiday#49,d_first_dom#50,d_last_dom#51,d_same_day_ly#52,d_same_day_lq#53,d_current_day#54,... 4 more fields] parquet
               +- Project [ss_item_sk#61]
                  +- Filter isnotnull(ss_item_sk#61)
                     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#59,ss_sold_time_sk#60,ss_item_sk#61,ss_customer_sk#62,ss_cdemo_sk#63,ss_hdemo_sk#64,ss_addr_sk#65,ss_store_sk#66,ss_promo_sk#67,ss_ticket_number#68,ss_quantity#69,ss_wholesale_cost#70,ss_list_price#71,ss_sales_price#72,ss_ext_discount_amt#73,ss_ext_sales_price#74,ss_ext_wholesale_cost#75,ss_ext_list_price#76,ss_ext_tax#77,ss_coupon_amt#78,ss_net_paid#79,ss_net_paid_inc_tax#80,ss_net_profit#81] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[i_item_id#6 ASC NULLS FIRST], output=[i_item_id#6,i_item_desc#9,i_current_price#10])
   +- HashAggregate(keys=[i_item_id#6, i_item_desc#9, i_current_price#10], functions=[], output=[i_item_id#6, i_item_desc#9, i_current_price#10])
      +- Exchange hashpartitioning(i_item_id#6, i_item_desc#9, i_current_price#10, 200), ENSURE_REQUIREMENTS, [plan_id=94]
         +- HashAggregate(keys=[i_item_id#6, i_item_desc#9, knownfloatingpointnormalized(normalizenanandzero(i_current_price#10)) AS i_current_price#10], functions=[], output=[i_item_id#6, i_item_desc#9, i_current_price#10])
            +- Project [i_item_id#6, i_item_desc#9, i_current_price#10]
               +- SortMergeJoin [i_item_sk#5], [ss_item_sk#61], Inner
                  :- Sort [i_item_sk#5 ASC NULLS FIRST], false, 0
                  :  +- Exchange hashpartitioning(i_item_sk#5, 200), ENSURE_REQUIREMENTS, [plan_id=86]
                  :     +- Project [i_item_sk#5, i_item_id#6, i_item_desc#9, i_current_price#10]
                  :        +- BroadcastHashJoin [inv_date_sk#27], [d_date_sk#31], Inner, BuildRight, false
                  :           :- Project [i_item_sk#5, i_item_id#6, i_item_desc#9, i_current_price#10, inv_date_sk#27]
                  :           :  +- BroadcastHashJoin [i_item_sk#5], [inv_item_sk#28], Inner, BuildLeft, false
                  :           :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=77]
                  :           :     :  +- Project [i_item_sk#5, i_item_id#6, i_item_desc#9, i_current_price#10]
                  :           :     :     +- Filter ((((isnotnull(i_current_price#10) AND (i_current_price#10 >= 62.0)) AND (i_current_price#10 <= 92.0)) AND i_manufact_id#18 IN (129,270,821,423)) AND isnotnull(i_item_sk#5))
                  :           :     :        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#5,i_item_id#6,i_item_desc#9,i_current_price#10,i_manufact_id#18] Batched: true, DataFilters: [isnotnull(i_current_price#10), (i_current_price#10 >= 62.0), (i_current_price#10 <= 92.0), i_man..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThanOrEqual(i_current_price,62.0), LessThanOrEqual(i_current_..., ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string,i_current_price:double,i_manufact_id:int>
                  :           :     +- Project [inv_date_sk#27, inv_item_sk#28]
                  :           :        +- Filter ((((isnotnull(inv_quantity_on_hand#30L) AND (inv_quantity_on_hand#30L >= 100)) AND (inv_quantity_on_hand#30L <= 500)) AND isnotnull(inv_item_sk#28)) AND isnotnull(inv_date_sk#27))
                  :           :           +- FileScan parquet spark_catalog.tpcds.inventory[inv_date_sk#27,inv_item_sk#28,inv_quantity_on_hand#30L] Batched: true, DataFilters: [isnotnull(inv_quantity_on_hand#30L), (inv_quantity_on_hand#30L >= 100), (inv_quantity_on_hand#30..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_quantity_on_hand), GreaterThanOrEqual(inv_quantity_on_hand,100), LessThanOrEqual(i..., ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_quantity_on_hand:bigint>
                  :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=81]
                  :              +- Project [d_date_sk#31]
                  :                 +- Filter (((isnotnull(d_date#33) AND (cast(d_date#33 as date) >= 2000-05-25)) AND (cast(d_date#33 as date) <= 2000-07-24)) AND isnotnull(d_date_sk#31))
                  :                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#31,d_date#33] Batched: true, DataFilters: [isnotnull(d_date#33), (cast(d_date#33 as date) >= 2000-05-25), (cast(d_date#33 as date) <= 2000-..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  +- Sort [ss_item_sk#61 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(ss_item_sk#61, 200), ENSURE_REQUIREMENTS, [plan_id=87]
                        +- Filter isnotnull(ss_item_sk#61)
                           +- FileScan parquet spark_catalog.tpcds.store_sales[ss_item_sk#61] Batched: true, DataFilters: [isnotnull(ss_item_sk#61)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk)], ReadSchema: struct<ss_item_sk:int>

Time taken: 2.753 seconds, Fetched 1 row(s)
