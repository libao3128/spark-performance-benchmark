Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4041
Spark master: local[*], Application Id: local-1741583288330
== Parsed Logical Plan ==
CTE [ws_wh]
:  +- 'SubqueryAlias ws_wh
:     +- 'Project ['ws1.ws_order_number, 'ws1.ws_warehouse_sk AS wh1#5, 'ws2.ws_warehouse_sk AS wh2#6]
:        +- 'Filter (('ws1.ws_order_number = 'ws2.ws_order_number) AND NOT ('ws1.ws_warehouse_sk = 'ws2.ws_warehouse_sk))
:           +- 'Join Inner
:              :- 'SubqueryAlias ws1
:              :  +- 'UnresolvedRelation [web_sales], [], false
:              +- 'SubqueryAlias ws2
:                 +- 'UnresolvedRelation [web_sales], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['count(distinct 'ws_order_number) ASC NULLS FIRST], true
         +- 'Project ['count(distinct 'ws_order_number) AS order_count#0, 'sum('ws_ext_ship_cost) AS total_shipping_cost#1, 'sum('ws_net_profit) AS total_net_profit#2]
            +- 'Filter ((((('d_date >= 1999-2-01) AND ('d_date <= 'date_add(cast(1999-2-01 as date), 60))) AND ('ws1.ws_ship_date_sk = 'd_date_sk)) AND (('ws1.ws_ship_addr_sk = 'ca_address_sk) AND ('ca_state = IL))) AND ((('ws1.ws_web_site_sk = 'web_site_sk) AND ('web_company_name = pri)) AND ('ws1.ws_order_number IN (list#3 []) AND 'ws1.ws_order_number IN (list#4 []))))
               :  :- 'Project ['ws_order_number]
               :  :  +- 'UnresolvedRelation [ws_wh], [], false
               :  +- 'Project ['wr_order_number]
               :     +- 'Filter ('wr_order_number = 'ws_wh.ws_order_number)
               :        +- 'Join Inner
               :           :- 'UnresolvedRelation [web_returns], [], false
               :           +- 'UnresolvedRelation [ws_wh], [], false
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
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ws_wh
:     +- Project [ws_order_number#29, ws_warehouse_sk#27 AS wh1#5, ws_warehouse_sk#128 AS wh2#6]
:        +- Filter ((ws_order_number#29 = ws_order_number#130) AND NOT (ws_warehouse_sk#27 = ws_warehouse_sk#128))
:           +- Join Inner
:              :- SubqueryAlias ws1
:              :  +- SubqueryAlias spark_catalog.tpcds.web_sales
:              :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#12,ws_sold_time_sk#13,ws_ship_date_sk#14,ws_item_sk#15,ws_bill_customer_sk#16,ws_bill_cdemo_sk#17,ws_bill_hdemo_sk#18,ws_bill_addr_sk#19,ws_ship_customer_sk#20,ws_ship_cdemo_sk#21,ws_ship_hdemo_sk#22,ws_ship_addr_sk#23,ws_web_page_sk#24,ws_web_site_sk#25,ws_ship_mode_sk#26,ws_warehouse_sk#27,ws_promo_sk#28,ws_order_number#29,ws_quantity#30,ws_wholesale_cost#31,ws_list_price#32,ws_sales_price#33,ws_ext_discount_amt#34,ws_ext_sales_price#35,... 10 more fields] parquet
:              +- SubqueryAlias ws2
:                 +- SubqueryAlias spark_catalog.tpcds.web_sales
:                    +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#113,ws_sold_time_sk#114,ws_ship_date_sk#115,ws_item_sk#116,ws_bill_customer_sk#117,ws_bill_cdemo_sk#118,ws_bill_hdemo_sk#119,ws_bill_addr_sk#120,ws_ship_customer_sk#121,ws_ship_cdemo_sk#122,ws_ship_hdemo_sk#123,ws_ship_addr_sk#124,ws_web_page_sk#125,ws_web_site_sk#126,ws_ship_mode_sk#127,ws_warehouse_sk#128,ws_promo_sk#129,ws_order_number#130,ws_quantity#131,ws_wholesale_cost#132,ws_list_price#133,ws_sales_price#134,ws_ext_discount_amt#135,ws_ext_sales_price#136,... 10 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [order_count#0L ASC NULLS FIRST], true
         +- Aggregate [count(distinct ws_order_number#164) AS order_count#0L, sum(ws_ext_ship_cost#175) AS total_shipping_cost#1, sum(ws_net_profit#180) AS total_net_profit#2]
            +- Filter (((((d_date#48 >= 1999-2-01) AND (cast(d_date#48 as date) <= date_add(cast(1999-2-01 as date), 60))) AND (ws_ship_date_sk#149 = d_date_sk#46)) AND ((ws_ship_addr_sk#158 = ca_address_sk#74) AND (ca_state#82 = IL))) AND (((ws_web_site_sk#160 = web_site_sk#87) AND (web_company_name#101 = pri)) AND (ws_order_number#164 IN (list#3 []) AND ws_order_number#164 IN (list#4 []))))
               :  :- Project [ws_order_number#29]
               :  :  +- SubqueryAlias ws_wh
               :  :     +- CTERelationRef 0, true, [ws_order_number#29, wh1#5, wh2#6], false
               :  +- Project [wr_order_number#194]
               :     +- Filter (wr_order_number#194 = ws_order_number#207)
               :        +- Join Inner
               :           :- SubqueryAlias spark_catalog.tpcds.web_returns
               :           :  +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#181,wr_returned_time_sk#182,wr_item_sk#183,wr_refunded_customer_sk#184,wr_refunded_cdemo_sk#185,wr_refunded_hdemo_sk#186,wr_refunded_addr_sk#187,wr_returning_customer_sk#188,wr_returning_cdemo_sk#189,wr_returning_hdemo_sk#190,wr_returning_addr_sk#191,wr_web_page_sk#192,wr_reason_sk#193,wr_order_number#194,wr_return_quantity#195,wr_return_amt#196,wr_return_tax#197,wr_return_amt_inc_tax#198,wr_fee#199,wr_return_ship_cost#200,wr_refunded_cash#201,wr_reversed_charge#202,wr_account_credit#203,wr_net_loss#204] parquet
               :           +- SubqueryAlias ws_wh
               :              +- CTERelationRef 0, true, [ws_order_number#207, wh1#208, wh2#209], false
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- SubqueryAlias ws1
                  :  :  :  +- SubqueryAlias spark_catalog.tpcds.web_sales
                  :  :  :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#147,ws_sold_time_sk#148,ws_ship_date_sk#149,ws_item_sk#150,ws_bill_customer_sk#151,ws_bill_cdemo_sk#152,ws_bill_hdemo_sk#153,ws_bill_addr_sk#154,ws_ship_customer_sk#155,ws_ship_cdemo_sk#156,ws_ship_hdemo_sk#157,ws_ship_addr_sk#158,ws_web_page_sk#159,ws_web_site_sk#160,ws_ship_mode_sk#161,ws_warehouse_sk#162,ws_promo_sk#163,ws_order_number#164,ws_quantity#165,ws_wholesale_cost#166,ws_list_price#167,ws_sales_price#168,ws_ext_discount_amt#169,ws_ext_sales_price#170,... 10 more fields] parquet
                  :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
                  :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#46,d_date_id#47,d_date#48,d_month_seq#49,d_week_seq#50,d_quarter_seq#51,d_year#52,d_dow#53,d_moy#54,d_dom#55,d_qoy#56,d_fy_year#57,d_fy_quarter_seq#58,d_fy_week_seq#59,d_day_name#60,d_quarter_name#61,d_holiday#62,d_weekend#63,d_following_holiday#64,d_first_dom#65,d_last_dom#66,d_same_day_ly#67,d_same_day_lq#68,d_current_day#69,... 4 more fields] parquet
                  :  +- SubqueryAlias spark_catalog.tpcds.customer_address
                  :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#74,ca_address_id#75,ca_street_number#76,ca_street_name#77,ca_street_type#78,ca_suite_number#79,ca_city#80,ca_county#81,ca_state#82,ca_zip#83,ca_country#84,ca_gmt_offset#85,ca_location_type#86] parquet
                  +- SubqueryAlias spark_catalog.tpcds.web_site
                     +- Relation spark_catalog.tpcds.web_site[web_site_sk#87,web_site_id#88,web_rec_start_date#89,web_rec_end_date#90,web_name#91,web_open_date_sk#92,web_close_date_sk#93,web_class#94,web_manager#95,web_mkt_id#96,web_mkt_class#97,web_mkt_desc#98,web_market_manager#99,web_company_id#100,web_company_name#101,web_street_number#102,web_street_name#103,web_street_type#104,web_suite_number#105,web_city#106,web_county#107,web_state#108,web_zip#109,web_country#110,... 2 more fields] parquet

== Optimized Logical Plan ==
Aggregate [count(distinct ws_order_number#164) AS order_count#0L, sum(ws_ext_ship_cost#175) AS total_shipping_cost#1, sum(ws_net_profit#180) AS total_net_profit#2]
+- Project [ws_order_number#164, ws_ext_ship_cost#175, ws_net_profit#180]
   +- Join Inner, (ws_web_site_sk#160 = web_site_sk#87)
      :- Project [ws_web_site_sk#160, ws_order_number#164, ws_ext_ship_cost#175, ws_net_profit#180]
      :  +- Join Inner, (ws_ship_addr_sk#158 = ca_address_sk#74)
      :     :- Project [ws_ship_addr_sk#158, ws_web_site_sk#160, ws_order_number#164, ws_ext_ship_cost#175, ws_net_profit#180]
      :     :  +- Join Inner, (ws_ship_date_sk#149 = d_date_sk#46)
      :     :     :- Join LeftSemi, (ws_order_number#164 = wr_order_number#194)
      :     :     :  :- Join LeftSemi, (ws_order_number#164 = ws_order_number#29)
      :     :     :  :  :- Project [ws_ship_date_sk#149, ws_ship_addr_sk#158, ws_web_site_sk#160, ws_order_number#164, ws_ext_ship_cost#175, ws_net_profit#180]
      :     :     :  :  :  +- Filter ((isnotnull(ws_ship_date_sk#149) AND isnotnull(ws_ship_addr_sk#158)) AND isnotnull(ws_web_site_sk#160))
      :     :     :  :  :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#147,ws_sold_time_sk#148,ws_ship_date_sk#149,ws_item_sk#150,ws_bill_customer_sk#151,ws_bill_cdemo_sk#152,ws_bill_hdemo_sk#153,ws_bill_addr_sk#154,ws_ship_customer_sk#155,ws_ship_cdemo_sk#156,ws_ship_hdemo_sk#157,ws_ship_addr_sk#158,ws_web_page_sk#159,ws_web_site_sk#160,ws_ship_mode_sk#161,ws_warehouse_sk#162,ws_promo_sk#163,ws_order_number#164,ws_quantity#165,ws_wholesale_cost#166,ws_list_price#167,ws_sales_price#168,ws_ext_discount_amt#169,ws_ext_sales_price#170,... 10 more fields] parquet
      :     :     :  :  +- Project [ws_order_number#29]
      :     :     :  :     +- Join Inner, ((ws_order_number#29 = ws_order_number#130) AND NOT (ws_warehouse_sk#27 = ws_warehouse_sk#128))
      :     :     :  :        :- Project [ws_warehouse_sk#27, ws_order_number#29]
      :     :     :  :        :  +- Filter (isnotnull(ws_order_number#29) AND isnotnull(ws_warehouse_sk#27))
      :     :     :  :        :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#12,ws_sold_time_sk#13,ws_ship_date_sk#14,ws_item_sk#15,ws_bill_customer_sk#16,ws_bill_cdemo_sk#17,ws_bill_hdemo_sk#18,ws_bill_addr_sk#19,ws_ship_customer_sk#20,ws_ship_cdemo_sk#21,ws_ship_hdemo_sk#22,ws_ship_addr_sk#23,ws_web_page_sk#24,ws_web_site_sk#25,ws_ship_mode_sk#26,ws_warehouse_sk#27,ws_promo_sk#28,ws_order_number#29,ws_quantity#30,ws_wholesale_cost#31,ws_list_price#32,ws_sales_price#33,ws_ext_discount_amt#34,ws_ext_sales_price#35,... 10 more fields] parquet
      :     :     :  :        +- Project [ws_warehouse_sk#128, ws_order_number#130]
      :     :     :  :           +- Filter (isnotnull(ws_order_number#130) AND isnotnull(ws_warehouse_sk#128))
      :     :     :  :              +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#113,ws_sold_time_sk#114,ws_ship_date_sk#115,ws_item_sk#116,ws_bill_customer_sk#117,ws_bill_cdemo_sk#118,ws_bill_hdemo_sk#119,ws_bill_addr_sk#120,ws_ship_customer_sk#121,ws_ship_cdemo_sk#122,ws_ship_hdemo_sk#123,ws_ship_addr_sk#124,ws_web_page_sk#125,ws_web_site_sk#126,ws_ship_mode_sk#127,ws_warehouse_sk#128,ws_promo_sk#129,ws_order_number#130,ws_quantity#131,ws_wholesale_cost#132,ws_list_price#133,ws_sales_price#134,ws_ext_discount_amt#135,ws_ext_sales_price#136,... 10 more fields] parquet
      :     :     :  +- Project [wr_order_number#194]
      :     :     :     +- Join Inner, (wr_order_number#194 = ws_order_number#306)
      :     :     :        :- Project [wr_order_number#194]
      :     :     :        :  +- Filter isnotnull(wr_order_number#194)
      :     :     :        :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#181,wr_returned_time_sk#182,wr_item_sk#183,wr_refunded_customer_sk#184,wr_refunded_cdemo_sk#185,wr_refunded_hdemo_sk#186,wr_refunded_addr_sk#187,wr_returning_customer_sk#188,wr_returning_cdemo_sk#189,wr_returning_hdemo_sk#190,wr_returning_addr_sk#191,wr_web_page_sk#192,wr_reason_sk#193,wr_order_number#194,wr_return_quantity#195,wr_return_amt#196,wr_return_tax#197,wr_return_amt_inc_tax#198,wr_fee#199,wr_return_ship_cost#200,wr_refunded_cash#201,wr_reversed_charge#202,wr_account_credit#203,wr_net_loss#204] parquet
      :     :     :        +- Project [ws_order_number#306]
      :     :     :           +- Join Inner, ((ws_order_number#306 = ws_order_number#340) AND NOT (ws_warehouse_sk#304 = ws_warehouse_sk#338))
      :     :     :              :- Project [ws_warehouse_sk#304, ws_order_number#306]
      :     :     :              :  +- Filter (isnotnull(ws_order_number#306) AND isnotnull(ws_warehouse_sk#304))
      :     :     :              :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#289,ws_sold_time_sk#290,ws_ship_date_sk#291,ws_item_sk#292,ws_bill_customer_sk#293,ws_bill_cdemo_sk#294,ws_bill_hdemo_sk#295,ws_bill_addr_sk#296,ws_ship_customer_sk#297,ws_ship_cdemo_sk#298,ws_ship_hdemo_sk#299,ws_ship_addr_sk#300,ws_web_page_sk#301,ws_web_site_sk#302,ws_ship_mode_sk#303,ws_warehouse_sk#304,ws_promo_sk#305,ws_order_number#306,ws_quantity#307,ws_wholesale_cost#308,ws_list_price#309,ws_sales_price#310,ws_ext_discount_amt#311,ws_ext_sales_price#312,... 10 more fields] parquet
      :     :     :              +- Project [ws_warehouse_sk#338, ws_order_number#340]
      :     :     :                 +- Filter (isnotnull(ws_order_number#340) AND isnotnull(ws_warehouse_sk#338))
      :     :     :                    +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#323,ws_sold_time_sk#324,ws_ship_date_sk#325,ws_item_sk#326,ws_bill_customer_sk#327,ws_bill_cdemo_sk#328,ws_bill_hdemo_sk#329,ws_bill_addr_sk#330,ws_ship_customer_sk#331,ws_ship_cdemo_sk#332,ws_ship_hdemo_sk#333,ws_ship_addr_sk#334,ws_web_page_sk#335,ws_web_site_sk#336,ws_ship_mode_sk#337,ws_warehouse_sk#338,ws_promo_sk#339,ws_order_number#340,ws_quantity#341,ws_wholesale_cost#342,ws_list_price#343,ws_sales_price#344,ws_ext_discount_amt#345,ws_ext_sales_price#346,... 10 more fields] parquet
      :     :     +- Project [d_date_sk#46]
      :     :        +- Filter ((isnotnull(d_date#48) AND ((d_date#48 >= 1999-2-01) AND (cast(d_date#48 as date) <= 1999-04-02))) AND isnotnull(d_date_sk#46))
      :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#46,d_date_id#47,d_date#48,d_month_seq#49,d_week_seq#50,d_quarter_seq#51,d_year#52,d_dow#53,d_moy#54,d_dom#55,d_qoy#56,d_fy_year#57,d_fy_quarter_seq#58,d_fy_week_seq#59,d_day_name#60,d_quarter_name#61,d_holiday#62,d_weekend#63,d_following_holiday#64,d_first_dom#65,d_last_dom#66,d_same_day_ly#67,d_same_day_lq#68,d_current_day#69,... 4 more fields] parquet
      :     +- Project [ca_address_sk#74]
      :        +- Filter ((isnotnull(ca_state#82) AND (ca_state#82 = IL)) AND isnotnull(ca_address_sk#74))
      :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#74,ca_address_id#75,ca_street_number#76,ca_street_name#77,ca_street_type#78,ca_suite_number#79,ca_city#80,ca_county#81,ca_state#82,ca_zip#83,ca_country#84,ca_gmt_offset#85,ca_location_type#86] parquet
      +- Project [web_site_sk#87]
         +- Filter ((isnotnull(web_company_name#101) AND (web_company_name#101 = pri)) AND isnotnull(web_site_sk#87))
            +- Relation spark_catalog.tpcds.web_site[web_site_sk#87,web_site_id#88,web_rec_start_date#89,web_rec_end_date#90,web_name#91,web_open_date_sk#92,web_close_date_sk#93,web_class#94,web_manager#95,web_mkt_id#96,web_mkt_class#97,web_mkt_desc#98,web_market_manager#99,web_company_id#100,web_company_name#101,web_street_number#102,web_street_name#103,web_street_type#104,web_suite_number#105,web_city#106,web_county#107,web_state#108,web_zip#109,web_country#110,... 2 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[sum(ws_ext_ship_cost#175), sum(ws_net_profit#180), count(distinct ws_order_number#164)], output=[order_count#0L, total_shipping_cost#1, total_net_profit#2])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=203]
      +- HashAggregate(keys=[], functions=[merge_sum(ws_ext_ship_cost#175), merge_sum(ws_net_profit#180), partial_count(distinct ws_order_number#164)], output=[sum#360, sum#362, count#365L])
         +- HashAggregate(keys=[ws_order_number#164], functions=[merge_sum(ws_ext_ship_cost#175), merge_sum(ws_net_profit#180)], output=[ws_order_number#164, sum#360, sum#362])
            +- HashAggregate(keys=[ws_order_number#164], functions=[partial_sum(ws_ext_ship_cost#175), partial_sum(ws_net_profit#180)], output=[ws_order_number#164, sum#360, sum#362])
               +- Project [ws_order_number#164, ws_ext_ship_cost#175, ws_net_profit#180]
                  +- BroadcastHashJoin [ws_web_site_sk#160], [web_site_sk#87], Inner, BuildRight, false
                     :- Project [ws_web_site_sk#160, ws_order_number#164, ws_ext_ship_cost#175, ws_net_profit#180]
                     :  +- BroadcastHashJoin [ws_ship_addr_sk#158], [ca_address_sk#74], Inner, BuildRight, false
                     :     :- Project [ws_ship_addr_sk#158, ws_web_site_sk#160, ws_order_number#164, ws_ext_ship_cost#175, ws_net_profit#180]
                     :     :  +- BroadcastHashJoin [ws_ship_date_sk#149], [d_date_sk#46], Inner, BuildRight, false
                     :     :     :- SortMergeJoin [ws_order_number#164], [wr_order_number#194], LeftSemi
                     :     :     :  :- SortMergeJoin [ws_order_number#164], [ws_order_number#29], LeftSemi
                     :     :     :  :  :- Sort [ws_order_number#164 ASC NULLS FIRST], false, 0
                     :     :     :  :  :  +- Exchange hashpartitioning(ws_order_number#164, 200), ENSURE_REQUIREMENTS, [plan_id=170]
                     :     :     :  :  :     +- Filter ((isnotnull(ws_ship_date_sk#149) AND isnotnull(ws_ship_addr_sk#158)) AND isnotnull(ws_web_site_sk#160))
                     :     :     :  :  :        +- FileScan parquet spark_catalog.tpcds.web_sales[ws_ship_date_sk#149,ws_ship_addr_sk#158,ws_web_site_sk#160,ws_order_number#164,ws_ext_ship_cost#175,ws_net_profit#180] Batched: true, DataFilters: [isnotnull(ws_ship_date_sk#149), isnotnull(ws_ship_addr_sk#158), isnotnull(ws_web_site_sk#160)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_ship_date_sk), IsNotNull(ws_ship_addr_sk), IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_ship_date_sk:int,ws_ship_addr_sk:int,ws_web_site_sk:int,ws_order_number:int,ws_ext_ship...
                     :     :     :  :  +- Sort [ws_order_number#29 ASC NULLS FIRST], false, 0
                     :     :     :  :     +- Exchange hashpartitioning(ws_order_number#29, 200), ENSURE_REQUIREMENTS, [plan_id=171]
                     :     :     :  :        +- Project [ws_order_number#29]
                     :     :     :  :           +- BroadcastHashJoin [ws_order_number#29], [ws_order_number#130], Inner, BuildRight, NOT (ws_warehouse_sk#27 = ws_warehouse_sk#128), false
                     :     :     :  :              :- Filter (isnotnull(ws_order_number#29) AND isnotnull(ws_warehouse_sk#27))
                     :     :     :  :              :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_warehouse_sk#27,ws_order_number#29] Batched: true, DataFilters: [isnotnull(ws_order_number#29), isnotnull(ws_warehouse_sk#27)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_order_number), IsNotNull(ws_warehouse_sk)], ReadSchema: struct<ws_warehouse_sk:int,ws_order_number:int>
                     :     :     :  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, int, false] as bigint)),false), [plan_id=165]
                     :     :     :  :                 +- Filter (isnotnull(ws_order_number#130) AND isnotnull(ws_warehouse_sk#128))
                     :     :     :  :                    +- FileScan parquet spark_catalog.tpcds.web_sales[ws_warehouse_sk#128,ws_order_number#130] Batched: true, DataFilters: [isnotnull(ws_order_number#130), isnotnull(ws_warehouse_sk#128)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_order_number), IsNotNull(ws_warehouse_sk)], ReadSchema: struct<ws_warehouse_sk:int,ws_order_number:int>
                     :     :     :  +- Sort [wr_order_number#194 ASC NULLS FIRST], false, 0
                     :     :     :     +- Exchange hashpartitioning(wr_order_number#194, 200), ENSURE_REQUIREMENTS, [plan_id=184]
                     :     :     :        +- Project [wr_order_number#194]
                     :     :     :           +- BroadcastHashJoin [wr_order_number#194], [ws_order_number#306], Inner, BuildLeft, false
                     :     :     :              :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=179]
                     :     :     :              :  +- Filter isnotnull(wr_order_number#194)
                     :     :     :              :     +- FileScan parquet spark_catalog.tpcds.web_returns[wr_order_number#194] Batched: true, DataFilters: [isnotnull(wr_order_number#194)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_order_number)], ReadSchema: struct<wr_order_number:int>
                     :     :     :              +- Project [ws_order_number#306]
                     :     :     :                 +- BroadcastHashJoin [ws_order_number#306], [ws_order_number#340], Inner, BuildRight, NOT (ws_warehouse_sk#304 = ws_warehouse_sk#338), false
                     :     :     :                    :- Filter (isnotnull(ws_order_number#306) AND isnotnull(ws_warehouse_sk#304))
                     :     :     :                    :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_warehouse_sk#304,ws_order_number#306] Batched: true, DataFilters: [isnotnull(ws_order_number#306), isnotnull(ws_warehouse_sk#304)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_order_number), IsNotNull(ws_warehouse_sk)], ReadSchema: struct<ws_warehouse_sk:int,ws_order_number:int>
                     :     :     :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, int, false] as bigint)),false), [plan_id=175]
                     :     :     :                       +- Filter (isnotnull(ws_order_number#340) AND isnotnull(ws_warehouse_sk#338))
                     :     :     :                          +- FileScan parquet spark_catalog.tpcds.web_sales[ws_warehouse_sk#338,ws_order_number#340] Batched: true, DataFilters: [isnotnull(ws_order_number#340), isnotnull(ws_warehouse_sk#338)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_order_number), IsNotNull(ws_warehouse_sk)], ReadSchema: struct<ws_warehouse_sk:int,ws_order_number:int>
                     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=188]
                     :     :        +- Project [d_date_sk#46]
                     :     :           +- Filter (((isnotnull(d_date#48) AND (d_date#48 >= 1999-2-01)) AND (cast(d_date#48 as date) <= 1999-04-02)) AND isnotnull(d_date_sk#46))
                     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#46,d_date#48] Batched: true, DataFilters: [isnotnull(d_date#48), (d_date#48 >= 1999-2-01), (cast(d_date#48 as date) <= 1999-04-02), isnotnu..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,1999-2-01), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=192]
                     :        +- Project [ca_address_sk#74]
                     :           +- Filter ((isnotnull(ca_state#82) AND (ca_state#82 = IL)) AND isnotnull(ca_address_sk#74))
                     :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#74,ca_state#82] Batched: true, DataFilters: [isnotnull(ca_state#82), (ca_state#82 = IL), isnotnull(ca_address_sk#74)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,IL), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=196]
                        +- Project [web_site_sk#87]
                           +- Filter ((isnotnull(web_company_name#101) AND (web_company_name#101 = pri)) AND isnotnull(web_site_sk#87))
                              +- FileScan parquet spark_catalog.tpcds.web_site[web_site_sk#87,web_company_name#101] Batched: true, DataFilters: [isnotnull(web_company_name#101), (web_company_name#101 = pri), isnotnull(web_site_sk#87)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(web_company_name), EqualTo(web_company_name,pri), IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_company_name:string>

Time taken: 5.3 seconds, Fetched 1 row(s)
