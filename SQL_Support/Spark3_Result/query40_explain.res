Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581123837
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['w_state ASC NULLS FIRST, 'i_item_id ASC NULLS FIRST], true
      +- 'Aggregate ['w_state, 'i_item_id], ['w_state, 'i_item_id, 'sum(CASE WHEN (cast('d_date as date) < cast(2000-03-11 as date)) THEN ('cs_sales_price - 'coalesce('cr_refunded_cash, 0)) ELSE 0 END) AS sales_before#0, 'sum(CASE WHEN (cast('d_date as date) >= cast(2000-03-11 as date)) THEN ('cs_sales_price - 'coalesce('cr_refunded_cash, 0)) ELSE 0 END) AS sales_after#1]
         +- 'Filter ((((('i_current_price >= 0.99) AND ('i_current_price <= 1.49)) AND ('i_item_sk = 'cs_item_sk)) AND ('cs_warehouse_sk = 'w_warehouse_sk)) AND (('cs_sold_date_sk = 'd_date_sk) AND (('d_date >= 'date_sub(cast(2000-03-11 as date), 30)) AND ('d_date <= 'date_add(cast(2000-03-11 as date), 30)))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join LeftOuter, (('cs_order_number = 'cr_order_number) AND ('cs_item_sk = 'cr_item_sk))
               :  :  :  :- 'UnresolvedRelation [catalog_sales], [], false
               :  :  :  +- 'UnresolvedRelation [catalog_returns], [], false
               :  :  +- 'UnresolvedRelation [warehouse], [], false
               :  +- 'UnresolvedRelation [item], [], false
               +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
w_state: string, i_item_id: string, sales_before: double, sales_after: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [w_state#78 ASC NULLS FIRST, i_item_id#83 ASC NULLS FIRST], true
      +- Aggregate [w_state#78, i_item_id#83], [w_state#78, i_item_id#83, sum(CASE WHEN (cast(d_date#106 as date) < cast(2000-03-11 as date)) THEN (cs_sales_price#28 - coalesce(cr_refunded_cash#64, cast(0 as double))) ELSE cast(0 as double) END) AS sales_before#0, sum(CASE WHEN (cast(d_date#106 as date) >= cast(2000-03-11 as date)) THEN (cs_sales_price#28 - coalesce(cr_refunded_cash#64, cast(0 as double))) ELSE cast(0 as double) END) AS sales_after#1]
         +- Filter (((((i_current_price#87 >= cast(0.99 as double)) AND (i_current_price#87 <= cast(1.49 as double))) AND (i_item_sk#82 = cs_item_sk#22)) AND (cs_warehouse_sk#21 = w_warehouse_sk#68)) AND ((cs_sold_date_sk#7 = d_date_sk#104) AND ((cast(d_date#106 as date) >= date_sub(cast(2000-03-11 as date), 30)) AND (cast(d_date#106 as date) <= date_add(cast(2000-03-11 as date), 30)))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join LeftOuter, ((cs_order_number#24 = cr_order_number#57) AND (cs_item_sk#22 = cr_item_sk#43))
               :  :  :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
               :  :  :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
               :  :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_returns
               :  :  :     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#41,cr_returned_time_sk#42,cr_item_sk#43,cr_refunded_customer_sk#44,cr_refunded_cdemo_sk#45,cr_refunded_hdemo_sk#46,cr_refunded_addr_sk#47,cr_returning_customer_sk#48,cr_returning_cdemo_sk#49,cr_returning_hdemo_sk#50,cr_returning_addr_sk#51,cr_call_center_sk#52,cr_catalog_page_sk#53,cr_ship_mode_sk#54,cr_warehouse_sk#55,cr_reason_sk#56,cr_order_number#57,cr_return_quantity#58,cr_return_amount#59,cr_return_tax#60,cr_return_amt_inc_tax#61,cr_fee#62,cr_return_ship_cost#63,cr_refunded_cash#64,... 3 more fields] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.warehouse
               :  :     +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#68,w_warehouse_id#69,w_warehouse_name#70,w_warehouse_sq_ft#71,w_street_number#72,w_street_name#73,w_street_type#74,w_suite_number#75,w_city#76,w_county#77,w_state#78,w_zip#79,w_country#80,w_gmt_offset#81] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.item
               :     +- Relation spark_catalog.tpcds.item[i_item_sk#82,i_item_id#83,i_rec_start_date#84,i_rec_end_date#85,i_item_desc#86,i_current_price#87,i_wholesale_cost#88,i_brand_id#89,i_brand#90,i_class_id#91,i_class#92,i_category_id#93,i_category#94,i_manufact_id#95,i_manufact#96,i_size#97,i_formulation#98,i_color#99,i_units#100,i_container#101,i_manager_id#102,i_product_name#103] parquet
               +- SubqueryAlias spark_catalog.tpcds.date_dim
                  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [w_state#78 ASC NULLS FIRST, i_item_id#83 ASC NULLS FIRST], true
      +- Aggregate [w_state#78, i_item_id#83], [w_state#78, i_item_id#83, sum(CASE WHEN (cast(d_date#106 as date) < 2000-03-11) THEN (cs_sales_price#28 - coalesce(cr_refunded_cash#64, 0.0)) ELSE 0.0 END) AS sales_before#0, sum(CASE WHEN (cast(d_date#106 as date) >= 2000-03-11) THEN (cs_sales_price#28 - coalesce(cr_refunded_cash#64, 0.0)) ELSE 0.0 END) AS sales_after#1]
         +- Project [cs_sales_price#28, cr_refunded_cash#64, w_state#78, i_item_id#83, d_date#106]
            +- Join Inner, (cs_sold_date_sk#7 = d_date_sk#104)
               :- Project [cs_sold_date_sk#7, cs_sales_price#28, cr_refunded_cash#64, w_state#78, i_item_id#83]
               :  +- Join Inner, (i_item_sk#82 = cs_item_sk#22)
               :     :- Project [cs_sold_date_sk#7, cs_item_sk#22, cs_sales_price#28, cr_refunded_cash#64, w_state#78]
               :     :  +- Join Inner, (cs_warehouse_sk#21 = w_warehouse_sk#68)
               :     :     :- Project [cs_sold_date_sk#7, cs_warehouse_sk#21, cs_item_sk#22, cs_sales_price#28, cr_refunded_cash#64]
               :     :     :  +- Join LeftOuter, ((cs_order_number#24 = cr_order_number#57) AND (cs_item_sk#22 = cr_item_sk#43))
               :     :     :     :- Project [cs_sold_date_sk#7, cs_warehouse_sk#21, cs_item_sk#22, cs_order_number#24, cs_sales_price#28]
               :     :     :     :  +- Filter (isnotnull(cs_warehouse_sk#21) AND (isnotnull(cs_item_sk#22) AND isnotnull(cs_sold_date_sk#7)))
               :     :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#7,cs_sold_time_sk#8,cs_ship_date_sk#9,cs_bill_customer_sk#10,cs_bill_cdemo_sk#11,cs_bill_hdemo_sk#12,cs_bill_addr_sk#13,cs_ship_customer_sk#14,cs_ship_cdemo_sk#15,cs_ship_hdemo_sk#16,cs_ship_addr_sk#17,cs_call_center_sk#18,cs_catalog_page_sk#19,cs_ship_mode_sk#20,cs_warehouse_sk#21,cs_item_sk#22,cs_promo_sk#23,cs_order_number#24,cs_quantity#25,cs_wholesale_cost#26,cs_list_price#27,cs_sales_price#28,cs_ext_discount_amt#29,cs_ext_sales_price#30,... 10 more fields] parquet
               :     :     :     +- Project [cr_item_sk#43, cr_order_number#57, cr_refunded_cash#64]
               :     :     :        +- Filter (isnotnull(cr_order_number#57) AND isnotnull(cr_item_sk#43))
               :     :     :           +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#41,cr_returned_time_sk#42,cr_item_sk#43,cr_refunded_customer_sk#44,cr_refunded_cdemo_sk#45,cr_refunded_hdemo_sk#46,cr_refunded_addr_sk#47,cr_returning_customer_sk#48,cr_returning_cdemo_sk#49,cr_returning_hdemo_sk#50,cr_returning_addr_sk#51,cr_call_center_sk#52,cr_catalog_page_sk#53,cr_ship_mode_sk#54,cr_warehouse_sk#55,cr_reason_sk#56,cr_order_number#57,cr_return_quantity#58,cr_return_amount#59,cr_return_tax#60,cr_return_amt_inc_tax#61,cr_fee#62,cr_return_ship_cost#63,cr_refunded_cash#64,... 3 more fields] parquet
               :     :     +- Project [w_warehouse_sk#68, w_state#78]
               :     :        +- Filter isnotnull(w_warehouse_sk#68)
               :     :           +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#68,w_warehouse_id#69,w_warehouse_name#70,w_warehouse_sq_ft#71,w_street_number#72,w_street_name#73,w_street_type#74,w_suite_number#75,w_city#76,w_county#77,w_state#78,w_zip#79,w_country#80,w_gmt_offset#81] parquet
               :     +- Project [i_item_sk#82, i_item_id#83]
               :        +- Filter ((isnotnull(i_current_price#87) AND ((i_current_price#87 >= 0.99) AND (i_current_price#87 <= 1.49))) AND isnotnull(i_item_sk#82))
               :           +- Relation spark_catalog.tpcds.item[i_item_sk#82,i_item_id#83,i_rec_start_date#84,i_rec_end_date#85,i_item_desc#86,i_current_price#87,i_wholesale_cost#88,i_brand_id#89,i_brand#90,i_class_id#91,i_class#92,i_category_id#93,i_category#94,i_manufact_id#95,i_manufact#96,i_size#97,i_formulation#98,i_color#99,i_units#100,i_container#101,i_manager_id#102,i_product_name#103] parquet
               +- Project [d_date_sk#104, d_date#106]
                  +- Filter ((isnotnull(d_date#106) AND ((cast(d_date#106 as date) >= 2000-02-10) AND (cast(d_date#106 as date) <= 2000-04-10))) AND isnotnull(d_date_sk#104))
                     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[w_state#78 ASC NULLS FIRST,i_item_id#83 ASC NULLS FIRST], output=[w_state#78,i_item_id#83,sales_before#0,sales_after#1])
   +- HashAggregate(keys=[w_state#78, i_item_id#83], functions=[sum(CASE WHEN (cast(d_date#106 as date) < 2000-03-11) THEN (cs_sales_price#28 - coalesce(cr_refunded_cash#64, 0.0)) ELSE 0.0 END), sum(CASE WHEN (cast(d_date#106 as date) >= 2000-03-11) THEN (cs_sales_price#28 - coalesce(cr_refunded_cash#64, 0.0)) ELSE 0.0 END)], output=[w_state#78, i_item_id#83, sales_before#0, sales_after#1])
      +- Exchange hashpartitioning(w_state#78, i_item_id#83, 200), ENSURE_REQUIREMENTS, [plan_id=116]
         +- HashAggregate(keys=[w_state#78, i_item_id#83], functions=[partial_sum(CASE WHEN (cast(d_date#106 as date) < 2000-03-11) THEN (cs_sales_price#28 - coalesce(cr_refunded_cash#64, 0.0)) ELSE 0.0 END), partial_sum(CASE WHEN (cast(d_date#106 as date) >= 2000-03-11) THEN (cs_sales_price#28 - coalesce(cr_refunded_cash#64, 0.0)) ELSE 0.0 END)], output=[w_state#78, i_item_id#83, sum#141, sum#142])
            +- Project [cs_sales_price#28, cr_refunded_cash#64, w_state#78, i_item_id#83, d_date#106]
               +- BroadcastHashJoin [cs_sold_date_sk#7], [d_date_sk#104], Inner, BuildRight, false
                  :- Project [cs_sold_date_sk#7, cs_sales_price#28, cr_refunded_cash#64, w_state#78, i_item_id#83]
                  :  +- BroadcastHashJoin [cs_item_sk#22], [i_item_sk#82], Inner, BuildRight, false
                  :     :- Project [cs_sold_date_sk#7, cs_item_sk#22, cs_sales_price#28, cr_refunded_cash#64, w_state#78]
                  :     :  +- BroadcastHashJoin [cs_warehouse_sk#21], [w_warehouse_sk#68], Inner, BuildRight, false
                  :     :     :- Project [cs_sold_date_sk#7, cs_warehouse_sk#21, cs_item_sk#22, cs_sales_price#28, cr_refunded_cash#64]
                  :     :     :  +- BroadcastHashJoin [cs_order_number#24, cs_item_sk#22], [cr_order_number#57, cr_item_sk#43], LeftOuter, BuildRight, false
                  :     :     :     :- Filter ((isnotnull(cs_warehouse_sk#21) AND isnotnull(cs_item_sk#22)) AND isnotnull(cs_sold_date_sk#7))
                  :     :     :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#7,cs_warehouse_sk#21,cs_item_sk#22,cs_order_number#24,cs_sales_price#28] Batched: true, DataFilters: [isnotnull(cs_warehouse_sk#21), isnotnull(cs_item_sk#22), isnotnull(cs_sold_date_sk#7)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_warehouse_sk), IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_warehouse_sk:int,cs_item_sk:int,cs_order_number:int,cs_sales_price:...
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=99]
                  :     :     :        +- Filter (isnotnull(cr_order_number#57) AND isnotnull(cr_item_sk#43))
                  :     :     :           +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_item_sk#43,cr_order_number#57,cr_refunded_cash#64] Batched: true, DataFilters: [isnotnull(cr_order_number#57), isnotnull(cr_item_sk#43)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_order_number), IsNotNull(cr_item_sk)], ReadSchema: struct<cr_item_sk:int,cr_order_number:int,cr_refunded_cash:double>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=103]
                  :     :        +- Filter isnotnull(w_warehouse_sk#68)
                  :     :           +- FileScan parquet spark_catalog.tpcds.warehouse[w_warehouse_sk#68,w_state#78] Batched: true, DataFilters: [isnotnull(w_warehouse_sk#68)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_state:string>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=107]
                  :        +- Project [i_item_sk#82, i_item_id#83]
                  :           +- Filter (((isnotnull(i_current_price#87) AND (i_current_price#87 >= 0.99)) AND (i_current_price#87 <= 1.49)) AND isnotnull(i_item_sk#82))
                  :              +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#82,i_item_id#83,i_current_price#87] Batched: true, DataFilters: [isnotnull(i_current_price#87), (i_current_price#87 >= 0.99), (i_current_price#87 <= 1.49), isnot..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThanOrEqual(i_current_price,0.99), LessThanOrEqual(i_current_..., ReadSchema: struct<i_item_sk:int,i_item_id:string,i_current_price:double>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=111]
                     +- Filter (((isnotnull(d_date#106) AND (cast(d_date#106 as date) >= 2000-02-10)) AND (cast(d_date#106 as date) <= 2000-04-10)) AND isnotnull(d_date_sk#104))
                        +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#104,d_date#106] Batched: true, DataFilters: [isnotnull(d_date#106), (cast(d_date#106 as date) >= 2000-02-10), (cast(d_date#106 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>

Time taken: 2.748 seconds, Fetched 1 row(s)
