Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581191247
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['sum('ss_ext_sales_price) DESC NULLS LAST, 'dt.d_year ASC NULLS FIRST, 'item.i_category_id ASC NULLS FIRST, 'item.i_category ASC NULLS FIRST], true
      +- 'Aggregate ['dt.d_year, 'item.i_category_id, 'item.i_category], ['dt.d_year, 'item.i_category_id, 'item.i_category, unresolvedalias('sum('ss_ext_sales_price), None)]
         +- 'Filter (((('dt.d_date_sk = 'store_sales.ss_sold_date_sk) AND ('store_sales.ss_item_sk = 'item.i_item_sk)) AND ('item.i_manager_id = 1)) AND (('dt.d_moy = 11) AND ('dt.d_year = 2000)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'SubqueryAlias dt
               :  :  +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [store_sales], [], false
               +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
d_year: int, i_category_id: int, i_category: string, sum(ss_ext_sales_price): double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [sum(ss_ext_sales_price)#79 DESC NULLS LAST, d_year#11 ASC NULLS FIRST, i_category_id#67 ASC NULLS FIRST, i_category#68 ASC NULLS FIRST], true
      +- Aggregate [d_year#11, i_category_id#67, i_category#68], [d_year#11, i_category_id#67, i_category#68, sum(ss_ext_sales_price#48) AS sum(ss_ext_sales_price)#79]
         +- Filter ((((d_date_sk#5 = ss_sold_date_sk#33) AND (ss_item_sk#35 = i_item_sk#56)) AND (i_manager_id#76 = 1)) AND ((d_moy#13 = 11) AND (d_year#11 = 2000)))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias dt
               :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#5,d_date_id#6,d_date#7,d_month_seq#8,d_week_seq#9,d_quarter_seq#10,d_year#11,d_dow#12,d_moy#13,d_dom#14,d_qoy#15,d_fy_year#16,d_fy_quarter_seq#17,d_fy_week_seq#18,d_day_name#19,d_quarter_name#20,d_holiday#21,d_weekend#22,d_following_holiday#23,d_first_dom#24,d_last_dom#25,d_same_day_ly#26,d_same_day_lq#27,d_current_day#28,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.store_sales
               :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#33,ss_sold_time_sk#34,ss_item_sk#35,ss_customer_sk#36,ss_cdemo_sk#37,ss_hdemo_sk#38,ss_addr_sk#39,ss_store_sk#40,ss_promo_sk#41,ss_ticket_number#42,ss_quantity#43,ss_wholesale_cost#44,ss_list_price#45,ss_sales_price#46,ss_ext_discount_amt#47,ss_ext_sales_price#48,ss_ext_wholesale_cost#49,ss_ext_list_price#50,ss_ext_tax#51,ss_coupon_amt#52,ss_net_paid#53,ss_net_paid_inc_tax#54,ss_net_profit#55] parquet
               +- SubqueryAlias spark_catalog.tpcds.item
                  +- Relation spark_catalog.tpcds.item[i_item_sk#56,i_item_id#57,i_rec_start_date#58,i_rec_end_date#59,i_item_desc#60,i_current_price#61,i_wholesale_cost#62,i_brand_id#63,i_brand#64,i_class_id#65,i_class#66,i_category_id#67,i_category#68,i_manufact_id#69,i_manufact#70,i_size#71,i_formulation#72,i_color#73,i_units#74,i_container#75,i_manager_id#76,i_product_name#77] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [sum(ss_ext_sales_price)#79 DESC NULLS LAST, d_year#11 ASC NULLS FIRST, i_category_id#67 ASC NULLS FIRST, i_category#68 ASC NULLS FIRST], true
      +- Aggregate [d_year#11, i_category_id#67, i_category#68], [d_year#11, i_category_id#67, i_category#68, sum(ss_ext_sales_price#48) AS sum(ss_ext_sales_price)#79]
         +- Project [d_year#11, ss_ext_sales_price#48, i_category_id#67, i_category#68]
            +- Join Inner, (ss_item_sk#35 = i_item_sk#56)
               :- Project [d_year#11, ss_item_sk#35, ss_ext_sales_price#48]
               :  +- Join Inner, (d_date_sk#5 = ss_sold_date_sk#33)
               :     :- Project [d_date_sk#5, d_year#11]
               :     :  +- Filter (((isnotnull(d_moy#13) AND isnotnull(d_year#11)) AND ((d_moy#13 = 11) AND (d_year#11 = 2000))) AND isnotnull(d_date_sk#5))
               :     :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#5,d_date_id#6,d_date#7,d_month_seq#8,d_week_seq#9,d_quarter_seq#10,d_year#11,d_dow#12,d_moy#13,d_dom#14,d_qoy#15,d_fy_year#16,d_fy_quarter_seq#17,d_fy_week_seq#18,d_day_name#19,d_quarter_name#20,d_holiday#21,d_weekend#22,d_following_holiday#23,d_first_dom#24,d_last_dom#25,d_same_day_ly#26,d_same_day_lq#27,d_current_day#28,... 4 more fields] parquet
               :     +- Project [ss_sold_date_sk#33, ss_item_sk#35, ss_ext_sales_price#48]
               :        +- Filter (isnotnull(ss_sold_date_sk#33) AND isnotnull(ss_item_sk#35))
               :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#33,ss_sold_time_sk#34,ss_item_sk#35,ss_customer_sk#36,ss_cdemo_sk#37,ss_hdemo_sk#38,ss_addr_sk#39,ss_store_sk#40,ss_promo_sk#41,ss_ticket_number#42,ss_quantity#43,ss_wholesale_cost#44,ss_list_price#45,ss_sales_price#46,ss_ext_discount_amt#47,ss_ext_sales_price#48,ss_ext_wholesale_cost#49,ss_ext_list_price#50,ss_ext_tax#51,ss_coupon_amt#52,ss_net_paid#53,ss_net_paid_inc_tax#54,ss_net_profit#55] parquet
               +- Project [i_item_sk#56, i_category_id#67, i_category#68]
                  +- Filter ((isnotnull(i_manager_id#76) AND (i_manager_id#76 = 1)) AND isnotnull(i_item_sk#56))
                     +- Relation spark_catalog.tpcds.item[i_item_sk#56,i_item_id#57,i_rec_start_date#58,i_rec_end_date#59,i_item_desc#60,i_current_price#61,i_wholesale_cost#62,i_brand_id#63,i_brand#64,i_class_id#65,i_class#66,i_category_id#67,i_category#68,i_manufact_id#69,i_manufact#70,i_size#71,i_formulation#72,i_color#73,i_units#74,i_container#75,i_manager_id#76,i_product_name#77] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[sum(ss_ext_sales_price)#79 DESC NULLS LAST,d_year#11 ASC NULLS FIRST,i_category_id#67 ASC NULLS FIRST,i_category#68 ASC NULLS FIRST], output=[d_year#11,i_category_id#67,i_category#68,sum(ss_ext_sales_price)#79])
   +- HashAggregate(keys=[d_year#11, i_category_id#67, i_category#68], functions=[sum(ss_ext_sales_price#48)], output=[d_year#11, i_category_id#67, i_category#68, sum(ss_ext_sales_price)#79])
      +- Exchange hashpartitioning(d_year#11, i_category_id#67, i_category#68, 200), ENSURE_REQUIREMENTS, [plan_id=72]
         +- HashAggregate(keys=[d_year#11, i_category_id#67, i_category#68], functions=[partial_sum(ss_ext_sales_price#48)], output=[d_year#11, i_category_id#67, i_category#68, sum#85])
            +- Project [d_year#11, ss_ext_sales_price#48, i_category_id#67, i_category#68]
               +- BroadcastHashJoin [ss_item_sk#35], [i_item_sk#56], Inner, BuildRight, false
                  :- Project [d_year#11, ss_item_sk#35, ss_ext_sales_price#48]
                  :  +- BroadcastHashJoin [d_date_sk#5], [ss_sold_date_sk#33], Inner, BuildLeft, false
                  :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=63]
                  :     :  +- Project [d_date_sk#5, d_year#11]
                  :     :     +- Filter ((((isnotnull(d_moy#13) AND isnotnull(d_year#11)) AND (d_moy#13 = 11)) AND (d_year#11 = 2000)) AND isnotnull(d_date_sk#5))
                  :     :        +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#5,d_year#11,d_moy#13] Batched: true, DataFilters: [isnotnull(d_moy#13), isnotnull(d_year#11), (d_moy#13 = 11), (d_year#11 = 2000), isnotnull(d_date..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,11), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     +- Filter (isnotnull(ss_sold_date_sk#33) AND isnotnull(ss_item_sk#35))
                  :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#33,ss_item_sk#35,ss_ext_sales_price#48] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#33), isnotnull(ss_item_sk#35)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=67]
                     +- Project [i_item_sk#56, i_category_id#67, i_category#68]
                        +- Filter ((isnotnull(i_manager_id#76) AND (i_manager_id#76 = 1)) AND isnotnull(i_item_sk#56))
                           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#56,i_category_id#67,i_category#68,i_manager_id#76] Batched: true, DataFilters: [isnotnull(i_manager_id#76), (i_manager_id#76 = 1), isnotnull(i_item_sk#56)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manager_id), EqualTo(i_manager_id,1), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_category_id:int,i_category:string,i_manager_id:int>

Time taken: 2.257 seconds, Fetched 1 row(s)
