Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4041
Spark master: local[*], Application Id: local-1741583243093
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['count(distinct 'ws_order_number) ASC NULLS FIRST], true
      +- 'Project ['count(distinct 'ws_order_number) AS order_count#0, 'sum('ws_ext_ship_cost) AS total_shipping_cost#1, 'sum('ws_net_profit) AS total_net_profit#2]
         +- 'Filter ((((('d_date >= cast(1999-2-01 as date)) AND ('d_date <= 'date_add(cast(1999-2-01 as date), 60))) AND ('ws1.ws_ship_date_sk = 'd_date_sk)) AND (('ws1.ws_ship_addr_sk = 'ca_address_sk) AND ('ca_state = IL))) AND ((('ws1.ws_web_site_sk = 'web_site_sk) AND ('web_company_name = pri)) AND (exists#3 [] AND NOT exists#4 [])))
            :  :- 'Project [*]
            :  :  +- 'Filter (('ws1.ws_order_number = 'ws2.ws_order_number) AND NOT ('ws1.ws_warehouse_sk = 'ws2.ws_warehouse_sk))
            :  :     +- 'SubqueryAlias ws2
            :  :        +- 'UnresolvedRelation [web_sales], [], false
            :  +- 'Project [*]
            :     +- 'Filter ('ws1.ws_order_number = 'wr1.wr_order_number)
            :        +- 'SubqueryAlias wr1
            :           +- 'UnresolvedRelation [web_returns], [], false
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'SubqueryAlias ws1
               :  :  :  +- 'UnresolvedRelation [web_sales], [], false
               :  :  +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [customer_address], [], false
               +- 'UnresolvedRelation [web_site], [], false

== Analyzed Logical Plan ==
order_count: bigint, total_shipping_cost: double, total_net_profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [order_count#0L ASC NULLS FIRST], true
      +- Aggregate [count(distinct ws_order_number#27) AS order_count#0L, sum(ws_ext_ship_cost#38) AS total_shipping_cost#1, sum(ws_net_profit#43) AS total_net_profit#2]
         +- Filter (((((cast(d_date#46 as date) >= cast(1999-2-01 as date)) AND (cast(d_date#46 as date) <= date_add(cast(1999-2-01 as date), 60))) AND (ws_ship_date_sk#12 = d_date_sk#44)) AND ((ws_ship_addr_sk#21 = ca_address_sk#72) AND (ca_state#80 = IL))) AND (((ws_web_site_sk#23 = web_site_sk#85) AND (web_company_name#99 = pri)) AND (exists#3 [ws_order_number#27 && ws_warehouse_sk#25] AND NOT exists#4 [ws_order_number#27])))
            :  :- Project [ws_sold_date_sk#137, ws_sold_time_sk#138, ws_ship_date_sk#139, ws_item_sk#140, ws_bill_customer_sk#141, ws_bill_cdemo_sk#142, ws_bill_hdemo_sk#143, ws_bill_addr_sk#144, ws_ship_customer_sk#145, ws_ship_cdemo_sk#146, ws_ship_hdemo_sk#147, ws_ship_addr_sk#148, ws_web_page_sk#149, ws_web_site_sk#150, ws_ship_mode_sk#151, ws_warehouse_sk#152, ws_promo_sk#153, ws_order_number#154, ws_quantity#155, ws_wholesale_cost#156, ws_list_price#157, ws_sales_price#158, ws_ext_discount_amt#159, ws_ext_sales_price#160, ... 10 more fields]
            :  :  +- Filter ((outer(ws_order_number#27) = ws_order_number#154) AND NOT (outer(ws_warehouse_sk#25) = ws_warehouse_sk#152))
            :  :     +- SubqueryAlias ws2
            :  :        +- SubqueryAlias spark_catalog.tpcds.web_sales
            :  :           +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#137,ws_sold_time_sk#138,ws_ship_date_sk#139,ws_item_sk#140,ws_bill_customer_sk#141,ws_bill_cdemo_sk#142,ws_bill_hdemo_sk#143,ws_bill_addr_sk#144,ws_ship_customer_sk#145,ws_ship_cdemo_sk#146,ws_ship_hdemo_sk#147,ws_ship_addr_sk#148,ws_web_page_sk#149,ws_web_site_sk#150,ws_ship_mode_sk#151,ws_warehouse_sk#152,ws_promo_sk#153,ws_order_number#154,ws_quantity#155,ws_wholesale_cost#156,ws_list_price#157,ws_sales_price#158,ws_ext_discount_amt#159,ws_ext_sales_price#160,... 10 more fields] parquet
            :  +- Project [wr_returned_date_sk#112, wr_returned_time_sk#113, wr_item_sk#114, wr_refunded_customer_sk#115, wr_refunded_cdemo_sk#116, wr_refunded_hdemo_sk#117, wr_refunded_addr_sk#118, wr_returning_customer_sk#119, wr_returning_cdemo_sk#120, wr_returning_hdemo_sk#121, wr_returning_addr_sk#122, wr_web_page_sk#123, wr_reason_sk#124, wr_order_number#125, wr_return_quantity#126, wr_return_amt#127, wr_return_tax#128, wr_return_amt_inc_tax#129, wr_fee#130, wr_return_ship_cost#131, wr_refunded_cash#132, wr_reversed_charge#133, wr_account_credit#134, wr_net_loss#135]
            :     +- Filter (outer(ws_order_number#27) = wr_order_number#125)
            :        +- SubqueryAlias wr1
            :           +- SubqueryAlias spark_catalog.tpcds.web_returns
            :              +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#112,wr_returned_time_sk#113,wr_item_sk#114,wr_refunded_customer_sk#115,wr_refunded_cdemo_sk#116,wr_refunded_hdemo_sk#117,wr_refunded_addr_sk#118,wr_returning_customer_sk#119,wr_returning_cdemo_sk#120,wr_returning_hdemo_sk#121,wr_returning_addr_sk#122,wr_web_page_sk#123,wr_reason_sk#124,wr_order_number#125,wr_return_quantity#126,wr_return_amt#127,wr_return_tax#128,wr_return_amt_inc_tax#129,wr_fee#130,wr_return_ship_cost#131,wr_refunded_cash#132,wr_reversed_charge#133,wr_account_credit#134,wr_net_loss#135] parquet
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias ws1
               :  :  :  +- SubqueryAlias spark_catalog.tpcds.web_sales
               :  :  :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_sold_time_sk#11,ws_ship_date_sk#12,ws_item_sk#13,ws_bill_customer_sk#14,ws_bill_cdemo_sk#15,ws_bill_hdemo_sk#16,ws_bill_addr_sk#17,ws_ship_customer_sk#18,ws_ship_cdemo_sk#19,ws_ship_hdemo_sk#20,ws_ship_addr_sk#21,ws_web_page_sk#22,ws_web_site_sk#23,ws_ship_mode_sk#24,ws_warehouse_sk#25,ws_promo_sk#26,ws_order_number#27,ws_quantity#28,ws_wholesale_cost#29,ws_list_price#30,ws_sales_price#31,ws_ext_discount_amt#32,ws_ext_sales_price#33,... 10 more fields] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.customer_address
               :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#72,ca_address_id#73,ca_street_number#74,ca_street_name#75,ca_street_type#76,ca_suite_number#77,ca_city#78,ca_county#79,ca_state#80,ca_zip#81,ca_country#82,ca_gmt_offset#83,ca_location_type#84] parquet
               +- SubqueryAlias spark_catalog.tpcds.web_site
                  +- Relation spark_catalog.tpcds.web_site[web_site_sk#85,web_site_id#86,web_rec_start_date#87,web_rec_end_date#88,web_name#89,web_open_date_sk#90,web_close_date_sk#91,web_class#92,web_manager#93,web_mkt_id#94,web_mkt_class#95,web_mkt_desc#96,web_market_manager#97,web_company_id#98,web_company_name#99,web_street_number#100,web_street_name#101,web_street_type#102,web_suite_number#103,web_city#104,web_county#105,web_state#106,web_zip#107,web_country#108,... 2 more fields] parquet

== Optimized Logical Plan ==
Aggregate [count(distinct ws_order_number#27) AS order_count#0L, sum(ws_ext_ship_cost#38) AS total_shipping_cost#1, sum(ws_net_profit#43) AS total_net_profit#2]
+- Project [ws_order_number#27, ws_ext_ship_cost#38, ws_net_profit#43]
   +- Join Inner, (ws_web_site_sk#23 = web_site_sk#85)
      :- Project [ws_web_site_sk#23, ws_order_number#27, ws_ext_ship_cost#38, ws_net_profit#43]
      :  +- Join Inner, (ws_ship_addr_sk#21 = ca_address_sk#72)
      :     :- Project [ws_ship_addr_sk#21, ws_web_site_sk#23, ws_order_number#27, ws_ext_ship_cost#38, ws_net_profit#43]
      :     :  +- Join Inner, (ws_ship_date_sk#12 = d_date_sk#44)
      :     :     :- Join LeftAnti, (ws_order_number#27 = wr_order_number#125)
      :     :     :  :- Project [ws_ship_date_sk#12, ws_ship_addr_sk#21, ws_web_site_sk#23, ws_order_number#27, ws_ext_ship_cost#38, ws_net_profit#43]
      :     :     :  :  +- Join LeftSemi, ((ws_order_number#27 = ws_order_number#154) AND NOT (ws_warehouse_sk#25 = ws_warehouse_sk#152))
      :     :     :  :     :- Project [ws_ship_date_sk#12, ws_ship_addr_sk#21, ws_web_site_sk#23, ws_warehouse_sk#25, ws_order_number#27, ws_ext_ship_cost#38, ws_net_profit#43]
      :     :     :  :     :  +- Filter ((isnotnull(ws_ship_date_sk#12) AND isnotnull(ws_ship_addr_sk#21)) AND isnotnull(ws_web_site_sk#23))
      :     :     :  :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#10,ws_sold_time_sk#11,ws_ship_date_sk#12,ws_item_sk#13,ws_bill_customer_sk#14,ws_bill_cdemo_sk#15,ws_bill_hdemo_sk#16,ws_bill_addr_sk#17,ws_ship_customer_sk#18,ws_ship_cdemo_sk#19,ws_ship_hdemo_sk#20,ws_ship_addr_sk#21,ws_web_page_sk#22,ws_web_site_sk#23,ws_ship_mode_sk#24,ws_warehouse_sk#25,ws_promo_sk#26,ws_order_number#27,ws_quantity#28,ws_wholesale_cost#29,ws_list_price#30,ws_sales_price#31,ws_ext_discount_amt#32,ws_ext_sales_price#33,... 10 more fields] parquet
      :     :     :  :     +- Project [ws_warehouse_sk#152, ws_order_number#154]
      :     :     :  :        +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#137,ws_sold_time_sk#138,ws_ship_date_sk#139,ws_item_sk#140,ws_bill_customer_sk#141,ws_bill_cdemo_sk#142,ws_bill_hdemo_sk#143,ws_bill_addr_sk#144,ws_ship_customer_sk#145,ws_ship_cdemo_sk#146,ws_ship_hdemo_sk#147,ws_ship_addr_sk#148,ws_web_page_sk#149,ws_web_site_sk#150,ws_ship_mode_sk#151,ws_warehouse_sk#152,ws_promo_sk#153,ws_order_number#154,ws_quantity#155,ws_wholesale_cost#156,ws_list_price#157,ws_sales_price#158,ws_ext_discount_amt#159,ws_ext_sales_price#160,... 10 more fields] parquet
      :     :     :  +- Project [wr_order_number#125]
      :     :     :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#112,wr_returned_time_sk#113,wr_item_sk#114,wr_refunded_customer_sk#115,wr_refunded_cdemo_sk#116,wr_refunded_hdemo_sk#117,wr_refunded_addr_sk#118,wr_returning_customer_sk#119,wr_returning_cdemo_sk#120,wr_returning_hdemo_sk#121,wr_returning_addr_sk#122,wr_web_page_sk#123,wr_reason_sk#124,wr_order_number#125,wr_return_quantity#126,wr_return_amt#127,wr_return_tax#128,wr_return_amt_inc_tax#129,wr_fee#130,wr_return_ship_cost#131,wr_refunded_cash#132,wr_reversed_charge#133,wr_account_credit#134,wr_net_loss#135] parquet
      :     :     +- Project [d_date_sk#44]
      :     :        +- Filter ((isnotnull(d_date#46) AND ((cast(d_date#46 as date) >= 1999-02-01) AND (cast(d_date#46 as date) <= 1999-04-02))) AND isnotnull(d_date_sk#44))
      :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#44,d_date_id#45,d_date#46,d_month_seq#47,d_week_seq#48,d_quarter_seq#49,d_year#50,d_dow#51,d_moy#52,d_dom#53,d_qoy#54,d_fy_year#55,d_fy_quarter_seq#56,d_fy_week_seq#57,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63,d_last_dom#64,d_same_day_ly#65,d_same_day_lq#66,d_current_day#67,... 4 more fields] parquet
      :     +- Project [ca_address_sk#72]
      :        +- Filter ((isnotnull(ca_state#80) AND (ca_state#80 = IL)) AND isnotnull(ca_address_sk#72))
      :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#72,ca_address_id#73,ca_street_number#74,ca_street_name#75,ca_street_type#76,ca_suite_number#77,ca_city#78,ca_county#79,ca_state#80,ca_zip#81,ca_country#82,ca_gmt_offset#83,ca_location_type#84] parquet
      +- Project [web_site_sk#85]
         +- Filter ((isnotnull(web_company_name#99) AND (web_company_name#99 = pri)) AND isnotnull(web_site_sk#85))
            +- Relation spark_catalog.tpcds.web_site[web_site_sk#85,web_site_id#86,web_rec_start_date#87,web_rec_end_date#88,web_name#89,web_open_date_sk#90,web_close_date_sk#91,web_class#92,web_manager#93,web_mkt_id#94,web_mkt_class#95,web_mkt_desc#96,web_market_manager#97,web_company_id#98,web_company_name#99,web_street_number#100,web_street_name#101,web_street_type#102,web_suite_number#103,web_city#104,web_county#105,web_state#106,web_zip#107,web_country#108,... 2 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[sum(ws_ext_ship_cost#38), sum(ws_net_profit#43), count(distinct ws_order_number#27)], output=[order_count#0L, total_shipping_cost#1, total_net_profit#2])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=132]
      +- HashAggregate(keys=[], functions=[merge_sum(ws_ext_ship_cost#38), merge_sum(ws_net_profit#43), partial_count(distinct ws_order_number#27)], output=[sum#179, sum#181, count#184L])
         +- HashAggregate(keys=[ws_order_number#27], functions=[merge_sum(ws_ext_ship_cost#38), merge_sum(ws_net_profit#43)], output=[ws_order_number#27, sum#179, sum#181])
            +- Exchange hashpartitioning(ws_order_number#27, 200), ENSURE_REQUIREMENTS, [plan_id=128]
               +- HashAggregate(keys=[ws_order_number#27], functions=[partial_sum(ws_ext_ship_cost#38), partial_sum(ws_net_profit#43)], output=[ws_order_number#27, sum#179, sum#181])
                  +- Project [ws_order_number#27, ws_ext_ship_cost#38, ws_net_profit#43]
                     +- BroadcastHashJoin [ws_web_site_sk#23], [web_site_sk#85], Inner, BuildRight, false
                        :- Project [ws_web_site_sk#23, ws_order_number#27, ws_ext_ship_cost#38, ws_net_profit#43]
                        :  +- BroadcastHashJoin [ws_ship_addr_sk#21], [ca_address_sk#72], Inner, BuildRight, false
                        :     :- Project [ws_ship_addr_sk#21, ws_web_site_sk#23, ws_order_number#27, ws_ext_ship_cost#38, ws_net_profit#43]
                        :     :  +- BroadcastHashJoin [ws_ship_date_sk#12], [d_date_sk#44], Inner, BuildRight, false
                        :     :     :- BroadcastHashJoin [ws_order_number#27], [wr_order_number#125], LeftAnti, BuildRight, false
                        :     :     :  :- Project [ws_ship_date_sk#12, ws_ship_addr_sk#21, ws_web_site_sk#23, ws_order_number#27, ws_ext_ship_cost#38, ws_net_profit#43]
                        :     :     :  :  +- BroadcastHashJoin [ws_order_number#27], [ws_order_number#154], LeftSemi, BuildRight, NOT (ws_warehouse_sk#25 = ws_warehouse_sk#152), false
                        :     :     :  :     :- Filter ((isnotnull(ws_ship_date_sk#12) AND isnotnull(ws_ship_addr_sk#21)) AND isnotnull(ws_web_site_sk#23))
                        :     :     :  :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_ship_date_sk#12,ws_ship_addr_sk#21,ws_web_site_sk#23,ws_warehouse_sk#25,ws_order_number#27,ws_ext_ship_cost#38,ws_net_profit#43] Batched: true, DataFilters: [isnotnull(ws_ship_date_sk#12), isnotnull(ws_ship_addr_sk#21), isnotnull(ws_web_site_sk#23)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_ship_date_sk), IsNotNull(ws_ship_addr_sk), IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_ship_date_sk:int,ws_ship_addr_sk:int,ws_web_site_sk:int,ws_warehouse_sk:int,ws_order_nu...
                        :     :     :  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, int, true] as bigint)),false), [plan_id=108]
                        :     :     :  :        +- FileScan parquet spark_catalog.tpcds.web_sales[ws_warehouse_sk#152,ws_order_number#154] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ws_warehouse_sk:int,ws_order_number:int>
                        :     :     :  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=112]
                        :     :     :     +- FileScan parquet spark_catalog.tpcds.web_returns[wr_order_number#125] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<wr_order_number:int>
                        :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=115]
                        :     :        +- Project [d_date_sk#44]
                        :     :           +- Filter (((isnotnull(d_date#46) AND (cast(d_date#46 as date) >= 1999-02-01)) AND (cast(d_date#46 as date) <= 1999-04-02)) AND isnotnull(d_date_sk#44))
                        :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#44,d_date#46] Batched: true, DataFilters: [isnotnull(d_date#46), (cast(d_date#46 as date) >= 1999-02-01), (cast(d_date#46 as date) <= 1999-..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=119]
                        :        +- Project [ca_address_sk#72]
                        :           +- Filter ((isnotnull(ca_state#80) AND (ca_state#80 = IL)) AND isnotnull(ca_address_sk#72))
                        :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#72,ca_state#80] Batched: true, DataFilters: [isnotnull(ca_state#80), (ca_state#80 = IL), isnotnull(ca_address_sk#72)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,IL), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
                        +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=123]
                           +- Project [web_site_sk#85]
                              +- Filter ((isnotnull(web_company_name#99) AND (web_company_name#99 = pri)) AND isnotnull(web_site_sk#85))
                                 +- FileScan parquet spark_catalog.tpcds.web_site[web_site_sk#85,web_company_name#99] Batched: true, DataFilters: [isnotnull(web_company_name#99), (web_company_name#99 = pri), isnotnull(web_site_sk#85)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(web_company_name), EqualTo(web_company_name,pri), IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_company_name:string>

Time taken: 3.227 seconds, Fetched 1 row(s)
