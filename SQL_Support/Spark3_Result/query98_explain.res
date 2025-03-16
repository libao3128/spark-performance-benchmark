Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583412125
== Parsed Logical Plan ==
'Sort ['i_category ASC NULLS FIRST, 'i_class ASC NULLS FIRST, 'i_item_id ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST, 'revenueratio ASC NULLS FIRST], true
+- 'Aggregate ['i_item_id, 'i_item_desc, 'i_category, 'i_class, 'i_current_price], ['i_item_id, 'i_item_desc, 'i_category, 'i_class, 'i_current_price, 'sum('ss_ext_sales_price) AS itemrevenue#0, (('sum('ss_ext_sales_price) * 100) / 'sum('sum('ss_ext_sales_price)) windowspecdefinition('i_class, unspecifiedframe$())) AS revenueratio#1]
   +- 'Filter ((('ss_item_sk = 'i_item_sk) AND 'i_category IN (Sports,Books,Home)) AND (('ss_sold_date_sk = 'd_date_sk) AND (('d_date >= cast(1999-02-22 as date)) AND ('d_date <= 'date_add(cast(1999-02-22 as date), 30)))))
      +- 'Join Inner
         :- 'Join Inner
         :  :- 'UnresolvedRelation [store_sales], [], false
         :  +- 'UnresolvedRelation [item], [], false
         +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
i_item_id: string, i_item_desc: string, i_category: string, i_class: string, i_current_price: double, itemrevenue: double, revenueratio: double
Sort [i_category#42 ASC NULLS FIRST, i_class#40 ASC NULLS FIRST, i_item_id#31 ASC NULLS FIRST, i_item_desc#34 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true
+- Project [i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35, itemrevenue#0, revenueratio#1]
   +- Project [i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35, itemrevenue#0, _w0#87, _we0#88, ((_w0#87 * cast(100 as double)) / _we0#88) AS revenueratio#1]
      +- Window [sum(_w0#87) windowspecdefinition(i_class#40, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#88], [i_class#40]
         +- Aggregate [i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35], [i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35, sum(ss_ext_sales_price#22) AS itemrevenue#0, sum(ss_ext_sales_price#22) AS _w0#87]
            +- Filter (((ss_item_sk#9 = i_item_sk#30) AND i_category#42 IN (Sports,Books,Home)) AND ((ss_sold_date_sk#7 = d_date_sk#52) AND ((cast(d_date#54 as date) >= cast(1999-02-22 as date)) AND (cast(d_date#54 as date) <= date_add(cast(1999-02-22 as date), 30)))))
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
                  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
                  :  +- SubqueryAlias spark_catalog.tpcds.item
                  :     +- Relation spark_catalog.tpcds.item[i_item_sk#30,i_item_id#31,i_rec_start_date#32,i_rec_end_date#33,i_item_desc#34,i_current_price#35,i_wholesale_cost#36,i_brand_id#37,i_brand#38,i_class_id#39,i_class#40,i_category_id#41,i_category#42,i_manufact_id#43,i_manufact#44,i_size#45,i_formulation#46,i_color#47,i_units#48,i_container#49,i_manager_id#50,i_product_name#51] parquet
                  +- SubqueryAlias spark_catalog.tpcds.date_dim
                     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet

== Optimized Logical Plan ==
Sort [i_category#42 ASC NULLS FIRST, i_class#40 ASC NULLS FIRST, i_item_id#31 ASC NULLS FIRST, i_item_desc#34 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true
+- Project [i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35, itemrevenue#0, ((_w0#87 * 100.0) / _we0#88) AS revenueratio#1]
   +- Window [sum(_w0#87) windowspecdefinition(i_class#40, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#88], [i_class#40]
      +- Aggregate [i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35], [i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35, sum(ss_ext_sales_price#22) AS itemrevenue#0, sum(ss_ext_sales_price#22) AS _w0#87]
         +- Project [ss_ext_sales_price#22, i_item_id#31, i_item_desc#34, i_current_price#35, i_class#40, i_category#42]
            +- Join Inner, (ss_sold_date_sk#7 = d_date_sk#52)
               :- Project [ss_sold_date_sk#7, ss_ext_sales_price#22, i_item_id#31, i_item_desc#34, i_current_price#35, i_class#40, i_category#42]
               :  +- Join Inner, (ss_item_sk#9 = i_item_sk#30)
               :     :- Project [ss_sold_date_sk#7, ss_item_sk#9, ss_ext_sales_price#22]
               :     :  +- Filter (isnotnull(ss_item_sk#9) AND isnotnull(ss_sold_date_sk#7))
               :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
               :     +- Project [i_item_sk#30, i_item_id#31, i_item_desc#34, i_current_price#35, i_class#40, i_category#42]
               :        +- Filter (i_category#42 IN (Sports,Books,Home) AND isnotnull(i_item_sk#30))
               :           +- Relation spark_catalog.tpcds.item[i_item_sk#30,i_item_id#31,i_rec_start_date#32,i_rec_end_date#33,i_item_desc#34,i_current_price#35,i_wholesale_cost#36,i_brand_id#37,i_brand#38,i_class_id#39,i_class#40,i_category_id#41,i_category#42,i_manufact_id#43,i_manufact#44,i_size#45,i_formulation#46,i_color#47,i_units#48,i_container#49,i_manager_id#50,i_product_name#51] parquet
               +- Project [d_date_sk#52]
                  +- Filter ((isnotnull(d_date#54) AND ((cast(d_date#54 as date) >= 1999-02-22) AND (cast(d_date#54 as date) <= 1999-03-24))) AND isnotnull(d_date_sk#52))
                     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [i_category#42 ASC NULLS FIRST, i_class#40 ASC NULLS FIRST, i_item_id#31 ASC NULLS FIRST, i_item_desc#34 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(i_category#42 ASC NULLS FIRST, i_class#40 ASC NULLS FIRST, i_item_id#31 ASC NULLS FIRST, i_item_desc#34 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=91]
      +- Project [i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35, itemrevenue#0, ((_w0#87 * 100.0) / _we0#88) AS revenueratio#1]
         +- Window [sum(_w0#87) windowspecdefinition(i_class#40, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#88], [i_class#40]
            +- Sort [i_class#40 ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning(i_class#40, 200), ENSURE_REQUIREMENTS, [plan_id=86]
                  +- HashAggregate(keys=[i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35], functions=[sum(ss_ext_sales_price#22)], output=[i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35, itemrevenue#0, _w0#87])
                     +- Exchange hashpartitioning(i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35, 200), ENSURE_REQUIREMENTS, [plan_id=83]
                        +- HashAggregate(keys=[i_item_id#31, i_item_desc#34, i_category#42, i_class#40, knownfloatingpointnormalized(normalizenanandzero(i_current_price#35)) AS i_current_price#35], functions=[partial_sum(ss_ext_sales_price#22)], output=[i_item_id#31, i_item_desc#34, i_category#42, i_class#40, i_current_price#35, sum#90])
                           +- Project [ss_ext_sales_price#22, i_item_id#31, i_item_desc#34, i_current_price#35, i_class#40, i_category#42]
                              +- BroadcastHashJoin [ss_sold_date_sk#7], [d_date_sk#52], Inner, BuildRight, false
                                 :- Project [ss_sold_date_sk#7, ss_ext_sales_price#22, i_item_id#31, i_item_desc#34, i_current_price#35, i_class#40, i_category#42]
                                 :  +- BroadcastHashJoin [ss_item_sk#9], [i_item_sk#30], Inner, BuildRight, false
                                 :     :- Filter (isnotnull(ss_item_sk#9) AND isnotnull(ss_sold_date_sk#7))
                                 :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#7,ss_item_sk#9,ss_ext_sales_price#22] Batched: true, DataFilters: [isnotnull(ss_item_sk#9), isnotnull(ss_sold_date_sk#7)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=74]
                                 :        +- Filter (i_category#42 IN (Sports,Books,Home) AND isnotnull(i_item_sk#30))
                                 :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#30,i_item_id#31,i_item_desc#34,i_current_price#35,i_class#40,i_category#42] Batched: true, DataFilters: [i_category#42 IN (Sports,Books,Home), isnotnull(i_item_sk#30)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(i_category, [Books,Home,Sports]), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string,i_current_price:double,i_class:string,i_...
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=78]
                                    +- Project [d_date_sk#52]
                                       +- Filter (((isnotnull(d_date#54) AND (cast(d_date#54 as date) >= 1999-02-22)) AND (cast(d_date#54 as date) <= 1999-03-24)) AND isnotnull(d_date_sk#52))
                                          +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#52,d_date#54] Batched: true, DataFilters: [isnotnull(d_date#54), (cast(d_date#54 as date) >= 1999-02-22), (cast(d_date#54 as date) <= 1999-..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>

Time taken: 2.779 seconds, Fetched 1 row(s)
