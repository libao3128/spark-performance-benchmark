Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580109522
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['count(distinct 'cs_order_number) ASC NULLS FIRST], true
      +- 'Project ['count(distinct 'cs_order_number) AS order_count#0, 'sum('cs_ext_ship_cost) AS total_shipping_cost#1, 'sum('cs_net_profit) AS total_net_profit#2]
         +- 'Filter ((((('d_date >= cast(2002-2-01 as date)) AND ('d_date <= 'date_add(cast(2002-2-01 as date), 60))) AND ('cs1.cs_ship_date_sk = 'd_date_sk)) AND (('cs1.cs_ship_addr_sk = 'ca_address_sk) AND ('ca_state = GA))) AND ((('cs1.cs_call_center_sk = 'cc_call_center_sk) AND 'cc_county IN (Williamson County,Williamson County,Williamson County,Williamson County,Williamson County)) AND (exists#3 [] AND NOT exists#4 [])))
            :  :- 'Project [*]
            :  :  +- 'Filter (('cs1.cs_order_number = 'cs2.cs_order_number) AND NOT ('cs1.cs_warehouse_sk = 'cs2.cs_warehouse_sk))
            :  :     +- 'SubqueryAlias cs2
            :  :        +- 'UnresolvedRelation [catalog_sales], [], false
            :  +- 'Project [*]
            :     +- 'Filter ('cs1.cs_order_number = 'cr1.cr_order_number)
            :        +- 'SubqueryAlias cr1
            :           +- 'UnresolvedRelation [catalog_returns], [], false
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'SubqueryAlias cs1
               :  :  :  +- 'UnresolvedRelation [catalog_sales], [], false
               :  :  +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [customer_address], [], false
               +- 'UnresolvedRelation [call_center], [], false

== Analyzed Logical Plan ==
order_count: bigint, total_shipping_cost: double, total_net_profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [order_count#0L ASC NULLS FIRST], true
      +- Aggregate [count(distinct cs_order_number#27) AS order_count#0L, sum(cs_ext_ship_cost#38) AS total_shipping_cost#1, sum(cs_net_profit#43) AS total_net_profit#2]
         +- Filter (((((cast(d_date#46 as date) >= cast(2002-2-01 as date)) AND (cast(d_date#46 as date) <= date_add(cast(2002-2-01 as date), 60))) AND (cs_ship_date_sk#12 = d_date_sk#44)) AND ((cs_ship_addr_sk#20 = ca_address_sk#72) AND (ca_state#80 = GA))) AND (((cs_call_center_sk#21 = cc_call_center_sk#85) AND cc_county#110 IN (Williamson County,Williamson County,Williamson County,Williamson County,Williamson County)) AND (exists#3 [cs_order_number#27 && cs_warehouse_sk#24] AND NOT exists#4 [cs_order_number#27])))
            :  :- Project [cs_sold_date_sk#145, cs_sold_time_sk#146, cs_ship_date_sk#147, cs_bill_customer_sk#148, cs_bill_cdemo_sk#149, cs_bill_hdemo_sk#150, cs_bill_addr_sk#151, cs_ship_customer_sk#152, cs_ship_cdemo_sk#153, cs_ship_hdemo_sk#154, cs_ship_addr_sk#155, cs_call_center_sk#156, cs_catalog_page_sk#157, cs_ship_mode_sk#158, cs_warehouse_sk#159, cs_item_sk#160, cs_promo_sk#161, cs_order_number#162, cs_quantity#163, cs_wholesale_cost#164, cs_list_price#165, cs_sales_price#166, cs_ext_discount_amt#167, cs_ext_sales_price#168, ... 10 more fields]
            :  :  +- Filter ((outer(cs_order_number#27) = cs_order_number#162) AND NOT (outer(cs_warehouse_sk#24) = cs_warehouse_sk#159))
            :  :     +- SubqueryAlias cs2
            :  :        +- SubqueryAlias spark_catalog.tpcds.catalog_sales
            :  :           +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#145,cs_sold_time_sk#146,cs_ship_date_sk#147,cs_bill_customer_sk#148,cs_bill_cdemo_sk#149,cs_bill_hdemo_sk#150,cs_bill_addr_sk#151,cs_ship_customer_sk#152,cs_ship_cdemo_sk#153,cs_ship_hdemo_sk#154,cs_ship_addr_sk#155,cs_call_center_sk#156,cs_catalog_page_sk#157,cs_ship_mode_sk#158,cs_warehouse_sk#159,cs_item_sk#160,cs_promo_sk#161,cs_order_number#162,cs_quantity#163,cs_wholesale_cost#164,cs_list_price#165,cs_sales_price#166,cs_ext_discount_amt#167,cs_ext_sales_price#168,... 10 more fields] parquet
            :  +- Project [cr_returned_date_sk#117, cr_returned_time_sk#118, cr_item_sk#119, cr_refunded_customer_sk#120, cr_refunded_cdemo_sk#121, cr_refunded_hdemo_sk#122, cr_refunded_addr_sk#123, cr_returning_customer_sk#124, cr_returning_cdemo_sk#125, cr_returning_hdemo_sk#126, cr_returning_addr_sk#127, cr_call_center_sk#128, cr_catalog_page_sk#129, cr_ship_mode_sk#130, cr_warehouse_sk#131, cr_reason_sk#132, cr_order_number#133, cr_return_quantity#134, cr_return_amount#135, cr_return_tax#136, cr_return_amt_inc_tax#137, cr_fee#138, cr_return_ship_cost#139, cr_refunded_cash#140, ... 3 more fields]
            :     +- Filter (outer(cs_order_number#27) = cr_order_number#133)
            :        +- SubqueryAlias cr1
            :           +- SubqueryAlias spark_catalog.tpcds.catalog_returns
            :              +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#117,cr_returned_time_sk#118,cr_item_sk#119,cr_refunded_customer_sk#120,cr_refunded_cdemo_sk#121,cr_refunded_hdemo_sk#122,cr_refunded_addr_sk#123,cr_returning_customer_sk#124,cr_returning_cdemo_sk#125,cr_returning_hdemo_sk#126,cr_returning_addr_sk#127,cr_call_center_sk#128,cr_catalog_page_sk#129,cr_ship_mode_sk#130,cr_warehouse_sk#131,cr_reason_sk#132,cr_order_number#133,cr_return_quantity#134,cr_return_amount#135,cr_return_tax#136,cr_return_amt_inc_tax#137,cr_fee#138,cr_return_ship_cost#139,cr_refunded_cash#140,... 3 more fields] parquet
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias cs1
               :  :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_sales
               :  :  :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#10,cs_sold_time_sk#11,cs_ship_date_sk#12,cs_bill_customer_sk#13,cs_bill_cdemo_sk#14,cs_bill_hdemo_sk#15,cs_bill_addr_sk#16,cs_ship_customer_sk#17,cs_ship_cdemo_sk#18,cs_ship_hdemo_sk#19,cs_ship_addr_sk#20,cs_call_center_sk#21,cs_catalog_page_sk#22,cs_ship_mode_sk#23,cs_warehouse_sk#24,cs_item_sk#25,cs_promo_sk#26,cs_order_number#27,cs_quantity#28,cs_wholesale_cost#29,cs_list_price#30,cs_sales_price#31,cs_ext_discount_amt#32,cs_ext_sales_price#33,... 10 more fields] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.customer_address
               :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#72,ca_address_id#73,ca_street_number#74,ca_street_name#75,ca_street_type#76,ca_suite_number#77,ca_city#78,ca_county#79,ca_state#80,ca_zip#81,ca_country#82,ca_gmt_offset#83,ca_location_type#84] parquet
               +- SubqueryAlias spark_catalog.tpcds.call_center
                  +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#85,cc_call_center_id#86,cc_rec_start_date#87,cc_rec_end_date#88,cc_closed_date_sk#89,cc_open_date_sk#90,cc_name#91,cc_class#92,cc_employees#93,cc_sq_ft#94,cc_hours#95,cc_manager#96,cc_mkt_id#97,cc_mkt_class#98,cc_mkt_desc#99,cc_market_manager#100,cc_division#101,cc_division_name#102,cc_company#103,cc_company_name#104,cc_street_number#105,cc_street_name#106,cc_street_type#107,cc_suite_number#108,... 7 more fields] parquet

== Optimized Logical Plan ==
Aggregate [count(distinct cs_order_number#27) AS order_count#0L, sum(cs_ext_ship_cost#38) AS total_shipping_cost#1, sum(cs_net_profit#43) AS total_net_profit#2]
+- Project [cs_order_number#27, cs_ext_ship_cost#38, cs_net_profit#43]
   +- Join Inner, (cs_call_center_sk#21 = cc_call_center_sk#85)
      :- Project [cs_call_center_sk#21, cs_order_number#27, cs_ext_ship_cost#38, cs_net_profit#43]
      :  +- Join Inner, (cs_ship_addr_sk#20 = ca_address_sk#72)
      :     :- Project [cs_ship_addr_sk#20, cs_call_center_sk#21, cs_order_number#27, cs_ext_ship_cost#38, cs_net_profit#43]
      :     :  +- Join Inner, (cs_ship_date_sk#12 = d_date_sk#44)
      :     :     :- Join LeftAnti, (cs_order_number#27 = cr_order_number#133)
      :     :     :  :- Project [cs_ship_date_sk#12, cs_ship_addr_sk#20, cs_call_center_sk#21, cs_order_number#27, cs_ext_ship_cost#38, cs_net_profit#43]
      :     :     :  :  +- Join LeftSemi, ((cs_order_number#27 = cs_order_number#162) AND NOT (cs_warehouse_sk#24 = cs_warehouse_sk#159))
      :     :     :  :     :- Project [cs_ship_date_sk#12, cs_ship_addr_sk#20, cs_call_center_sk#21, cs_warehouse_sk#24, cs_order_number#27, cs_ext_ship_cost#38, cs_net_profit#43]
      :     :     :  :     :  +- Filter ((isnotnull(cs_ship_date_sk#12) AND isnotnull(cs_ship_addr_sk#20)) AND isnotnull(cs_call_center_sk#21))
      :     :     :  :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#10,cs_sold_time_sk#11,cs_ship_date_sk#12,cs_bill_customer_sk#13,cs_bill_cdemo_sk#14,cs_bill_hdemo_sk#15,cs_bill_addr_sk#16,cs_ship_customer_sk#17,cs_ship_cdemo_sk#18,cs_ship_hdemo_sk#19,cs_ship_addr_sk#20,cs_call_center_sk#21,cs_catalog_page_sk#22,cs_ship_mode_sk#23,cs_warehouse_sk#24,cs_item_sk#25,cs_promo_sk#26,cs_order_number#27,cs_quantity#28,cs_wholesale_cost#29,cs_list_price#30,cs_sales_price#31,cs_ext_discount_amt#32,cs_ext_sales_price#33,... 10 more fields] parquet
      :     :     :  :     +- Project [cs_warehouse_sk#159, cs_order_number#162]
      :     :     :  :        +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#145,cs_sold_time_sk#146,cs_ship_date_sk#147,cs_bill_customer_sk#148,cs_bill_cdemo_sk#149,cs_bill_hdemo_sk#150,cs_bill_addr_sk#151,cs_ship_customer_sk#152,cs_ship_cdemo_sk#153,cs_ship_hdemo_sk#154,cs_ship_addr_sk#155,cs_call_center_sk#156,cs_catalog_page_sk#157,cs_ship_mode_sk#158,cs_warehouse_sk#159,cs_item_sk#160,cs_promo_sk#161,cs_order_number#162,cs_quantity#163,cs_wholesale_cost#164,cs_list_price#165,cs_sales_price#166,cs_ext_discount_amt#167,cs_ext_sales_price#168,... 10 more fields] parquet
      :     :     :  +- Project [cr_order_number#133]
      :     :     :     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#117,cr_returned_time_sk#118,cr_item_sk#119,cr_refunded_customer_sk#120,cr_refunded_cdemo_sk#121,cr_refunded_hdemo_sk#122,cr_refunded_addr_sk#123,cr_returning_customer_sk#124,cr_returning_cdemo_sk#125,cr_returning_hdemo_sk#126,cr_returning_addr_sk#127,cr_call_center_sk#128,cr_catalog_page_sk#129,cr_ship_mode_sk#130,cr_warehouse_sk#131,cr_reason_sk#132,cr_order_number#133,cr_return_quantity#134,cr_return_amount#135,cr_return_tax#136,cr_return_amt_inc_tax#137,cr_fee#138,cr_return_ship_cost#139,cr_refunded_cash#140,... 3 more fields] parquet
      :     :     +- Project [d_date_sk#44]
      :     :        +- Filter ((isnotnull(d_date#46) AND ((cast(d_date#46 as date) >= 2002-02-01) AND (cast(d_date#46 as date) <= 2002-04-02))) AND isnotnull(d_date_sk#44))
      :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet
      :     +- Project [ca_address_sk#72]
      :        +- Filter ((isnotnull(ca_state#80) AND (ca_state#80 = GA)) AND isnotnull(ca_address_sk#72))
      :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#72,ca_address_id#73,ca_street_number#74,ca_street_name#75,ca_street_type#76,ca_suite_number#77,ca_city#78,ca_county#79,ca_state#80,ca_zip#81,ca_country#82,ca_gmt_offset#83,ca_location_type#84] parquet
      +- Project [cc_call_center_sk#85]
         +- Filter ((isnotnull(cc_county#110) AND (cc_county#110 = Williamson County)) AND isnotnull(cc_call_center_sk#85))
            +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#85,cc_call_center_id#86,cc_rec_start_date#87,cc_rec_end_date#88,cc_closed_date_sk#89,cc_open_date_sk#90,cc_name#91,cc_class#92,cc_employees#93,cc_sq_ft#94,cc_hours#95,cc_manager#96,cc_mkt_id#97,cc_mkt_class#98,cc_mkt_desc#99,cc_market_manager#100,cc_division#101,cc_division_name#102,cc_company#103,cc_company_name#104,cc_street_number#105,cc_street_name#106,cc_street_type#107,cc_suite_number#108,... 7 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[sum(cs_ext_ship_cost#38), sum(cs_net_profit#43), count(distinct cs_order_number#27)], output=[order_count#0L, total_shipping_cost#1, total_net_profit#2])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=132]
      +- HashAggregate(keys=[], functions=[merge_sum(cs_ext_ship_cost#38), merge_sum(cs_net_profit#43), partial_count(distinct cs_order_number#27)], output=[sum#187, sum#189, count#192L])
         +- HashAggregate(keys=[cs_order_number#27], functions=[merge_sum(cs_ext_ship_cost#38), merge_sum(cs_net_profit#43)], output=[cs_order_number#27, sum#187, sum#189])
            +- Exchange hashpartitioning(cs_order_number#27, 200), ENSURE_REQUIREMENTS, [plan_id=128]
               +- HashAggregate(keys=[cs_order_number#27], functions=[partial_sum(cs_ext_ship_cost#38), partial_sum(cs_net_profit#43)], output=[cs_order_number#27, sum#187, sum#189])
                  +- Project [cs_order_number#27, cs_ext_ship_cost#38, cs_net_profit#43]
                     +- BroadcastHashJoin [cs_call_center_sk#21], [cc_call_center_sk#85], Inner, BuildRight, false
                        :- Project [cs_call_center_sk#21, cs_order_number#27, cs_ext_ship_cost#38, cs_net_profit#43]
                        :  +- BroadcastHashJoin [cs_ship_addr_sk#20], [ca_address_sk#72], Inner, BuildRight, false
                        :     :- Project [cs_ship_addr_sk#20, cs_call_center_sk#21, cs_order_number#27, cs_ext_ship_cost#38, cs_net_profit#43]
                        :     :  +- BroadcastHashJoin [cs_ship_date_sk#12], [d_date_sk#44], Inner, BuildRight, false
                        :     :     :- BroadcastHashJoin [cs_order_number#27], [cr_order_number#133], LeftAnti, BuildRight, false
                        :     :     :  :- Project [cs_ship_date_sk#12, cs_ship_addr_sk#20, cs_call_center_sk#21, cs_order_number#27, cs_ext_ship_cost#38, cs_net_profit#43]
                        :     :     :  :  +- BroadcastHashJoin [cs_order_number#27], [cs_order_number#162], LeftSemi, BuildRight, NOT (cs_warehouse_sk#24 = cs_warehouse_sk#159), false
                        :     :     :  :     :- Filter ((isnotnull(cs_ship_date_sk#12) AND isnotnull(cs_ship_addr_sk#20)) AND isnotnull(cs_call_center_sk#21))
                        :     :     :  :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_ship_date_sk#12,cs_ship_addr_sk#20,cs_call_center_sk#21,cs_warehouse_sk#24,cs_order_number#27,cs_ext_ship_cost#38,cs_net_profit#43] Batched: true, DataFilters: [isnotnull(cs_ship_date_sk#12), isnotnull(cs_ship_addr_sk#20), isnotnull(cs_call_center_sk#21)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_ship_date_sk), IsNotNull(cs_ship_addr_sk), IsNotNull(cs_call_center_sk)], ReadSchema: struct<cs_ship_date_sk:int,cs_ship_addr_sk:int,cs_call_center_sk:int,cs_warehouse_sk:int,cs_order...
                        :     :     :  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, int, true] as bigint)),false), [plan_id=108]
                        :     :     :  :        +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_warehouse_sk#159,cs_order_number#162] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<cs_warehouse_sk:int,cs_order_number:int>
                        :     :     :  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=112]
                        :     :     :     +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_order_number#133] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<cr_order_number:int>
                        :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=115]
                        :     :        +- Project [d_date_sk#44]
                        :     :           +- Filter (((isnotnull(d_date#46) AND (cast(d_date#46 as date) >= 2002-02-01)) AND (cast(d_date#46 as date) <= 2002-04-02)) AND isnotnull(d_date_sk#44))
                        :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#44,d_date#46] Batched: true, DataFilters: [isnotnull(d_date#46), (cast(d_date#46 as date) >= 2002-02-01), (cast(d_date#46 as date) <= 2002-..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=119]
                        :        +- Project [ca_address_sk#72]
                        :           +- Filter ((isnotnull(ca_state#80) AND (ca_state#80 = GA)) AND isnotnull(ca_address_sk#72))
                        :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#72,ca_state#80] Batched: true, DataFilters: [isnotnull(ca_state#80), (ca_state#80 = GA), isnotnull(ca_address_sk#72)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,GA), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
                        +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=123]
                           +- Project [cc_call_center_sk#85]
                              +- Filter ((isnotnull(cc_county#110) AND (cc_county#110 = Williamson County)) AND isnotnull(cc_call_center_sk#85))
                                 +- FileScan parquet spark_catalog.tpcds.call_center[cc_call_center_sk#85,cc_county#110] Batched: true, DataFilters: [isnotnull(cc_county#110), (cc_county#110 = Williamson County), isnotnull(cc_call_center_sk#85)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_county), EqualTo(cc_county,Williamson County), IsNotNull(cc_call_center_sk)], ReadSchema: struct<cc_call_center_sk:int,cc_county:string>

Time taken: 2.812 seconds, Fetched 1 row(s)
