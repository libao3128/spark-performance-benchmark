Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581567970
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['dt.d_year ASC NULLS FIRST, 'ext_price DESC NULLS LAST, 'brand_id ASC NULLS FIRST], true
      +- 'Aggregate ['dt.d_year, 'item.i_brand, 'item.i_brand_id], ['dt.d_year, 'item.i_brand_id AS brand_id#0, 'item.i_brand AS brand#1, 'sum('ss_ext_sales_price) AS ext_price#2]
         +- 'Filter (((('dt.d_date_sk = 'store_sales.ss_sold_date_sk) AND ('store_sales.ss_item_sk = 'item.i_item_sk)) AND ('item.i_manager_id = 1)) AND (('dt.d_moy = 11) AND ('dt.d_year = 2000)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias dt
               :  :  +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [store_sales], [], false
               +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
d_year: int, brand_id: int, brand: string, ext_price: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [d_year#14 ASC NULLS FIRST, ext_price#2 DESC NULLS LAST, brand_id#0 ASC NULLS FIRST], true
      +- Aggregate [d_year#14, i_brand#67, i_brand_id#66], [d_year#14, i_brand_id#66 AS brand_id#0, i_brand#67 AS brand#1, sum(ss_ext_sales_price#51) AS ext_price#2]
         +- Filter ((((d_date_sk#8 = ss_sold_date_sk#36) AND (ss_item_sk#38 = i_item_sk#59)) AND (i_manager_id#79 = 1)) AND ((d_moy#16 = 11) AND (d_year#14 = 2000)))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias dt
               :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#8,d_date_id#9,d_date#10,d_month_seq#11,d_week_seq#12,d_quarter_seq#13,d_year#14,d_dow#15,d_moy#16,d_dom#17,d_qoy#18,d_fy_year#19,d_fy_quarter_seq#20,d_fy_week_seq#21,d_day_name#22,d_quarter_name#23,d_holiday#24,d_weekend#25,d_following_holiday#26,d_first_dom#27,d_last_dom#28,d_same_day_ly#29,d_same_day_lq#30,d_current_day#31,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.store_sales
               :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#36,ss_sold_time_sk#37,ss_item_sk#38,ss_customer_sk#39,ss_cdemo_sk#40,ss_hdemo_sk#41,ss_addr_sk#42,ss_store_sk#43,ss_promo_sk#44,ss_ticket_number#45,ss_quantity#46,ss_wholesale_cost#47,ss_list_price#48,ss_sales_price#49,ss_ext_discount_amt#50,ss_ext_sales_price#51,ss_ext_wholesale_cost#52,ss_ext_list_price#53,ss_ext_tax#54,ss_coupon_amt#55,ss_net_paid#56,ss_net_paid_inc_tax#57,ss_net_profit#58] parquet
               +- SubqueryAlias spark_catalog.tpcds.item
                  +- Relation spark_catalog.tpcds.item[i_item_sk#59,i_item_id#60,i_rec_start_date#61,i_rec_end_date#62,i_item_desc#63,i_current_price#64,i_wholesale_cost#65,i_brand_id#66,i_brand#67,i_class_id#68,i_class#69,i_category_id#70,i_category#71,i_manufact_id#72,i_manufact#73,i_size#74,i_formulation#75,i_color#76,i_units#77,i_container#78,i_manager_id#79,i_product_name#80] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [d_year#14 ASC NULLS FIRST, ext_price#2 DESC NULLS LAST, brand_id#0 ASC NULLS FIRST], true
      +- Aggregate [d_year#14, i_brand#67, i_brand_id#66], [d_year#14, i_brand_id#66 AS brand_id#0, i_brand#67 AS brand#1, sum(ss_ext_sales_price#51) AS ext_price#2]
         +- Project [d_year#14, ss_ext_sales_price#51, i_brand_id#66, i_brand#67]
            +- Join Inner, (ss_item_sk#38 = i_item_sk#59)
               :- Project [d_year#14, ss_item_sk#38, ss_ext_sales_price#51]
               :  +- Join Inner, (d_date_sk#8 = ss_sold_date_sk#36)
               :     :- Project [d_date_sk#8, d_year#14]
               :     :  +- Filter (((isnotnull(d_moy#16) AND isnotnull(d_year#14)) AND ((d_moy#16 = 11) AND (d_year#14 = 2000))) AND isnotnull(d_date_sk#8))
               :     :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#8,d_date_id#9,d_date#10,d_month_seq#11,d_week_seq#12,d_quarter_seq#13,d_year#14,d_dow#15,d_moy#16,d_dom#17,d_qoy#18,d_fy_year#19,d_fy_quarter_seq#20,d_fy_week_seq#21,d_day_name#22,d_quarter_name#23,d_holiday#24,d_weekend#25,d_following_holiday#26,d_first_dom#27,d_last_dom#28,d_same_day_ly#29,d_same_day_lq#30,d_current_day#31,... 4 more fields] parquet
               :     +- Project [ss_sold_date_sk#36, ss_item_sk#38, ss_ext_sales_price#51]
               :        +- Filter (isnotnull(ss_sold_date_sk#36) AND isnotnull(ss_item_sk#38))
               :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#36,ss_sold_time_sk#37,ss_item_sk#38,ss_customer_sk#39,ss_cdemo_sk#40,ss_hdemo_sk#41,ss_addr_sk#42,ss_store_sk#43,ss_promo_sk#44,ss_ticket_number#45,ss_quantity#46,ss_wholesale_cost#47,ss_list_price#48,ss_sales_price#49,ss_ext_discount_amt#50,ss_ext_sales_price#51,ss_ext_wholesale_cost#52,ss_ext_list_price#53,ss_ext_tax#54,ss_coupon_amt#55,ss_net_paid#56,ss_net_paid_inc_tax#57,ss_net_profit#58] parquet
               +- Project [i_item_sk#59, i_brand_id#66, i_brand#67]
                  +- Filter ((isnotnull(i_manager_id#79) AND (i_manager_id#79 = 1)) AND isnotnull(i_item_sk#59))
                     +- Relation spark_catalog.tpcds.item[i_item_sk#59,i_item_id#60,i_rec_start_date#61,i_rec_end_date#62,i_item_desc#63,i_current_price#64,i_wholesale_cost#65,i_brand_id#66,i_brand#67,i_class_id#68,i_class#69,i_category_id#70,i_category#71,i_manufact_id#72,i_manufact#73,i_size#74,i_formulation#75,i_color#76,i_units#77,i_container#78,i_manager_id#79,i_product_name#80] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[d_year#14 ASC NULLS FIRST,ext_price#2 DESC NULLS LAST,brand_id#0 ASC NULLS FIRST], output=[d_year#14,brand_id#0,brand#1,ext_price#2])
   +- HashAggregate(keys=[d_year#14, i_brand#67, i_brand_id#66], functions=[sum(ss_ext_sales_price#51)], output=[d_year#14, brand_id#0, brand#1, ext_price#2])
      +- Exchange hashpartitioning(d_year#14, i_brand#67, i_brand_id#66, 200), ENSURE_REQUIREMENTS, [plan_id=72]
         +- HashAggregate(keys=[d_year#14, i_brand#67, i_brand_id#66], functions=[partial_sum(ss_ext_sales_price#51)], output=[d_year#14, i_brand#67, i_brand_id#66, sum#86])
            +- Project [d_year#14, ss_ext_sales_price#51, i_brand_id#66, i_brand#67]
               +- BroadcastHashJoin [ss_item_sk#38], [i_item_sk#59], Inner, BuildRight, false
                  :- Project [d_year#14, ss_item_sk#38, ss_ext_sales_price#51]
                  :  +- BroadcastHashJoin [d_date_sk#8], [ss_sold_date_sk#36], Inner, BuildLeft, false
                  :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=63]
                  :     :  +- Project [d_date_sk#8, d_year#14]
                  :     :     +- Filter ((((isnotnull(d_moy#16) AND isnotnull(d_year#14)) AND (d_moy#16 = 11)) AND (d_year#14 = 2000)) AND isnotnull(d_date_sk#8))
                  :     :        +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#8,d_year#14,d_moy#16] Batched: true, DataFilters: [isnotnull(d_moy#16), isnotnull(d_year#14), (d_moy#16 = 11), (d_year#14 = 2000), isnotnull(d_date..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,11), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     +- Filter (isnotnull(ss_sold_date_sk#36) AND isnotnull(ss_item_sk#38))
                  :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#36,ss_item_sk#38,ss_ext_sales_price#51] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#36), isnotnull(ss_item_sk#38)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=67]
                     +- Project [i_item_sk#59, i_brand_id#66, i_brand#67]
                        +- Filter ((isnotnull(i_manager_id#79) AND (i_manager_id#79 = 1)) AND isnotnull(i_item_sk#59))
                           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#59,i_brand_id#66,i_brand#67,i_manager_id#79] Batched: true, DataFilters: [isnotnull(i_manager_id#79), (i_manager_id#79 = 1), isnotnull(i_item_sk#59)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manager_id), EqualTo(i_manager_id,1), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_brand:string,i_manager_id:int>

Time taken: 2.099 seconds, Fetched 1 row(s)
