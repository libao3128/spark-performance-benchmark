Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579909178
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_category ASC NULLS FIRST, 'i_class ASC NULLS FIRST, 'i_item_id ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST, 'revenueratio ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id, 'i_item_desc, 'i_category, 'i_class, 'i_current_price], ['i_item_id, 'i_item_desc, 'i_category, 'i_class, 'i_current_price, 'sum('ws_ext_sales_price) AS itemrevenue#0, (('sum('ws_ext_sales_price) * 100) / 'sum('sum('ws_ext_sales_price)) windowspecdefinition('i_class, unspecifiedframe$())) AS revenueratio#1]
         +- 'Filter ((('ws_item_sk = 'i_item_sk) AND 'i_category IN (Sports,Books,Home)) AND (('ws_sold_date_sk = 'd_date_sk) AND (('d_date >= cast(1999-02-22 as date)) AND ('d_date <= 'date_add(cast(1999-02-22 as date), 30)))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation [web_sales], [], false
               :  +- 'UnresolvedRelation [item], [], false
               +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
i_item_id: string, i_item_desc: string, i_category: string, i_class: string, i_current_price: double, itemrevenue: double, revenueratio: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_category#53 ASC NULLS FIRST, i_class#51 ASC NULLS FIRST, i_item_id#42 ASC NULLS FIRST, i_item_desc#45 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true
      +- Project [i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46, itemrevenue#0, revenueratio#1]
         +- Project [i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46, itemrevenue#0, _w0#98, _we0#99, ((_w0#98 * cast(100 as double)) / _we0#99) AS revenueratio#1]
            +- Window [sum(_w0#98) windowspecdefinition(i_class#51, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#99], [i_class#51]
               +- Aggregate [i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46], [i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46, sum(ws_ext_sales_price#30) AS itemrevenue#0, sum(ws_ext_sales_price#30) AS _w0#98]
                  +- Filter (((ws_item_sk#10 = i_item_sk#41) AND i_category#53 IN (Sports,Books,Home)) AND ((ws_sold_date_sk#7 = d_date_sk#63) AND ((cast(d_date#65 as date) >= cast(1999-02-22 as date)) AND (cast(d_date#65 as date) <= date_add(cast(1999-02-22 as date), 30)))))
                     +- Join Inner
                        :- Join Inner
                        :  :- SubqueryAlias spark_catalog.tpcds.web_sales
                        :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
                        :  +- SubqueryAlias spark_catalog.tpcds.item
                        :     +- Relation spark_catalog.tpcds.item[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
                        +- SubqueryAlias spark_catalog.tpcds.date_dim
                           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_category#53 ASC NULLS FIRST, i_class#51 ASC NULLS FIRST, i_item_id#42 ASC NULLS FIRST, i_item_desc#45 ASC NULLS FIRST, revenueratio#1 ASC NULLS FIRST], true
      +- Project [i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46, itemrevenue#0, ((_w0#98 * 100.0) / _we0#99) AS revenueratio#1]
         +- Window [sum(_w0#98) windowspecdefinition(i_class#51, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#99], [i_class#51]
            +- Aggregate [i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46], [i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46, sum(ws_ext_sales_price#30) AS itemrevenue#0, sum(ws_ext_sales_price#30) AS _w0#98]
               +- Project [ws_ext_sales_price#30, i_item_id#42, i_item_desc#45, i_current_price#46, i_class#51, i_category#53]
                  +- Join Inner, (ws_sold_date_sk#7 = d_date_sk#63)
                     :- Project [ws_sold_date_sk#7, ws_ext_sales_price#30, i_item_id#42, i_item_desc#45, i_current_price#46, i_class#51, i_category#53]
                     :  +- Join Inner, (ws_item_sk#10 = i_item_sk#41)
                     :     :- Project [ws_sold_date_sk#7, ws_item_sk#10, ws_ext_sales_price#30]
                     :     :  +- Filter (isnotnull(ws_item_sk#10) AND isnotnull(ws_sold_date_sk#7))
                     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#7,ws_sold_time_sk#8,ws_ship_date_sk#9,ws_item_sk#10,ws_bill_customer_sk#11,ws_bill_cdemo_sk#12,ws_bill_hdemo_sk#13,ws_bill_addr_sk#14,ws_ship_customer_sk#15,ws_ship_cdemo_sk#16,ws_ship_hdemo_sk#17,ws_ship_addr_sk#18,ws_web_page_sk#19,ws_web_site_sk#20,ws_ship_mode_sk#21,ws_warehouse_sk#22,ws_promo_sk#23,ws_order_number#24,ws_quantity#25,ws_wholesale_cost#26,ws_list_price#27,ws_sales_price#28,ws_ext_discount_amt#29,ws_ext_sales_price#30,... 10 more fields] parquet
                     :     +- Project [i_item_sk#41, i_item_id#42, i_item_desc#45, i_current_price#46, i_class#51, i_category#53]
                     :        +- Filter (i_category#53 IN (Sports,Books,Home) AND isnotnull(i_item_sk#41))
                     :           +- Relation spark_catalog.tpcds.item[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
                     +- Project [d_date_sk#63]
                        +- Filter ((isnotnull(d_date#65) AND ((cast(d_date#65 as date) >= 1999-02-22) AND (cast(d_date#65 as date) <= 1999-03-24))) AND isnotnull(d_date_sk#63))
                           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[i_category#53 ASC NULLS FIRST,i_class#51 ASC NULLS FIRST,i_item_id#42 ASC NULLS FIRST,i_item_desc#45 ASC NULLS FIRST,revenueratio#1 ASC NULLS FIRST], output=[i_item_id#42,i_item_desc#45,i_category#53,i_class#51,i_current_price#46,itemrevenue#0,revenueratio#1])
   +- Project [i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46, itemrevenue#0, ((_w0#98 * 100.0) / _we0#99) AS revenueratio#1]
      +- Window [sum(_w0#98) windowspecdefinition(i_class#51, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _we0#99], [i_class#51]
         +- Sort [i_class#51 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(i_class#51, 200), ENSURE_REQUIREMENTS, [plan_id=85]
               +- HashAggregate(keys=[i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46], functions=[sum(ws_ext_sales_price#30)], output=[i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46, itemrevenue#0, _w0#98])
                  +- Exchange hashpartitioning(i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46, 200), ENSURE_REQUIREMENTS, [plan_id=82]
                     +- HashAggregate(keys=[i_item_id#42, i_item_desc#45, i_category#53, i_class#51, knownfloatingpointnormalized(normalizenanandzero(i_current_price#46)) AS i_current_price#46], functions=[partial_sum(ws_ext_sales_price#30)], output=[i_item_id#42, i_item_desc#45, i_category#53, i_class#51, i_current_price#46, sum#101])
                        +- Project [ws_ext_sales_price#30, i_item_id#42, i_item_desc#45, i_current_price#46, i_class#51, i_category#53]
                           +- BroadcastHashJoin [ws_sold_date_sk#7], [d_date_sk#63], Inner, BuildRight, false
                              :- Project [ws_sold_date_sk#7, ws_ext_sales_price#30, i_item_id#42, i_item_desc#45, i_current_price#46, i_class#51, i_category#53]
                              :  +- BroadcastHashJoin [ws_item_sk#10], [i_item_sk#41], Inner, BuildRight, false
                              :     :- Filter (isnotnull(ws_item_sk#10) AND isnotnull(ws_sold_date_sk#7))
                              :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#7,ws_item_sk#10,ws_ext_sales_price#30] Batched: true, DataFilters: [isnotnull(ws_item_sk#10), isnotnull(ws_sold_date_sk#7)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_ext_sales_price:double>
                              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=73]
                              :        +- Filter (i_category#53 IN (Sports,Books,Home) AND isnotnull(i_item_sk#41))
                              :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#41,i_item_id#42,i_item_desc#45,i_current_price#46,i_class#51,i_category#53] Batched: true, DataFilters: [i_category#53 IN (Sports,Books,Home), isnotnull(i_item_sk#41)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(i_category, [Books,Home,Sports]), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string,i_current_price:double,i_class:string,i_...
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=77]
                                 +- Project [d_date_sk#63]
                                    +- Filter (((isnotnull(d_date#65) AND (cast(d_date#65 as date) >= 1999-02-22)) AND (cast(d_date#65 as date) <= 1999-03-24)) AND isnotnull(d_date_sk#63))
                                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#63,d_date#65] Batched: true, DataFilters: [isnotnull(d_date#65), (cast(d_date#65 as date) >= 1999-02-22), (cast(d_date#65 as date) <= 1999-..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>

Time taken: 2.463 seconds, Fetched 1 row(s)
