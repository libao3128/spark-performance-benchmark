Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579629322
== Parsed Logical Plan ==
CTE [ssr, csr, wsr]
:  :- 'SubqueryAlias ssr
:  :  +- 'Aggregate ['s_store_id], ['s_store_id, 'sum('sales_price) AS sales#24, 'sum('profit) AS profit#25, 'sum('return_amt) AS returns#26, 'sum('net_loss) AS profit_loss#27]
:  :     +- 'Filter ((('date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 14)))) AND ('store_sk = 's_store_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'SubqueryAlias salesreturns
:  :           :  :  +- 'Union false, false
:  :           :  :     :- 'Project ['ss_store_sk AS store_sk#12, 'ss_sold_date_sk AS date_sk#13, 'ss_ext_sales_price AS sales_price#14, 'ss_net_profit AS profit#15, cast(0 as decimal(7,2)) AS return_amt#16, cast(0 as decimal(7,2)) AS net_loss#17]
:  :           :  :     :  +- 'UnresolvedRelation [store_sales], [], false
:  :           :  :     +- 'Project ['sr_store_sk AS store_sk#18, 'sr_returned_date_sk AS date_sk#19, cast(0 as decimal(7,2)) AS sales_price#20, cast(0 as decimal(7,2)) AS profit#21, 'sr_return_amt AS return_amt#22, 'sr_net_loss AS net_loss#23]
:  :           :  :        +- 'UnresolvedRelation [store_returns], [], false
:  :           :  +- 'UnresolvedRelation [date_dim], [], false
:  :           +- 'UnresolvedRelation [store], [], false
:  :- 'SubqueryAlias csr
:  :  +- 'Aggregate ['cp_catalog_page_id], ['cp_catalog_page_id, 'sum('sales_price) AS sales#40, 'sum('profit) AS profit#41, 'sum('return_amt) AS returns#42, 'sum('net_loss) AS profit_loss#43]
:  :     +- 'Filter ((('date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 14)))) AND ('page_sk = 'cp_catalog_page_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'SubqueryAlias salesreturns
:  :           :  :  +- 'Union false, false
:  :           :  :     :- 'Project ['cs_catalog_page_sk AS page_sk#28, 'cs_sold_date_sk AS date_sk#29, 'cs_ext_sales_price AS sales_price#30, 'cs_net_profit AS profit#31, cast(0 as decimal(7,2)) AS return_amt#32, cast(0 as decimal(7,2)) AS net_loss#33]
:  :           :  :     :  +- 'UnresolvedRelation [catalog_sales], [], false
:  :           :  :     +- 'Project ['cr_catalog_page_sk AS page_sk#34, 'cr_returned_date_sk AS date_sk#35, cast(0 as decimal(7,2)) AS sales_price#36, cast(0 as decimal(7,2)) AS profit#37, 'cr_return_amount AS return_amt#38, 'cr_net_loss AS net_loss#39]
:  :           :  :        +- 'UnresolvedRelation [catalog_returns], [], false
:  :           :  +- 'UnresolvedRelation [date_dim], [], false
:  :           +- 'UnresolvedRelation [catalog_page], [], false
:  +- 'SubqueryAlias wsr
:     +- 'Aggregate ['web_site_id], ['web_site_id, 'sum('sales_price) AS sales#56, 'sum('profit) AS profit#57, 'sum('return_amt) AS returns#58, 'sum('net_loss) AS profit_loss#59]
:        +- 'Filter ((('date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 14)))) AND ('wsr_web_site_sk = 'web_site_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'SubqueryAlias salesreturns
:              :  :  +- 'Union false, false
:              :  :     :- 'Project ['ws_web_site_sk AS wsr_web_site_sk#44, 'ws_sold_date_sk AS date_sk#45, 'ws_ext_sales_price AS sales_price#46, 'ws_net_profit AS profit#47, cast(0 as decimal(7,2)) AS return_amt#48, cast(0 as decimal(7,2)) AS net_loss#49]
:              :  :     :  +- 'UnresolvedRelation [web_sales], [], false
:              :  :     +- 'Project ['ws_web_site_sk AS wsr_web_site_sk#50, 'wr_returned_date_sk AS date_sk#51, cast(0 as decimal(7,2)) AS sales_price#52, cast(0 as decimal(7,2)) AS profit#53, 'wr_return_amt AS return_amt#54, 'wr_net_loss AS net_loss#55]
:              :  :        +- 'Join LeftOuter, (('wr_item_sk = 'ws_item_sk) AND ('wr_order_number = 'ws_order_number))
:              :  :           :- 'UnresolvedRelation [web_returns], [], false
:              :  :           +- 'UnresolvedRelation [web_sales], [], false
:              :  +- 'UnresolvedRelation [date_dim], [], false
:              +- 'UnresolvedRelation [web_site], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['channel ASC NULLS FIRST, 'id ASC NULLS FIRST], true
         +- 'Aggregate [rollup(Vector(0), Vector(1), 'channel, 'id)], ['channel, 'id, 'sum('sales) AS sales#9, 'sum('returns) AS returns#10, 'sum('profit) AS profit#11]
            +- 'SubqueryAlias x
               +- 'Union false, false
                  :- 'Union false, false
                  :  :- 'Project [store channel AS channel#0, 'concat(store, 's_store_id) AS id#1, 'sales, 'returns, ('profit - 'profit_loss) AS profit#2]
                  :  :  +- 'UnresolvedRelation [ssr], [], false
                  :  +- 'Project [catalog channel AS channel#3, 'concat(catalog_page, 'cp_catalog_page_id) AS id#4, 'sales, 'returns, ('profit - 'profit_loss) AS profit#5]
                  :     +- 'UnresolvedRelation [csr], [], false
                  +- 'Project [web channel AS channel#6, 'concat(web_site, 'web_site_id) AS id#7, 'sales, 'returns, ('profit - 'profit_loss) AS profit#8]
                     +- 'UnresolvedRelation [wsr], [], false

== Analyzed Logical Plan ==
channel: string, id: string, sales: double, returns: double, profit: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ssr
:     +- Aggregate [s_store_id#137], [s_store_id#137, sum(sales_price#14) AS sales#24, sum(profit#15) AS profit#25, sum(return_amt#409) AS returns#26, sum(net_loss#410) AS profit_loss#27]
:        +- Filter (((date_sk#13 = d_date_sk#108) AND ((cast(d_date#110 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#110 as date) <= date_add(cast(2000-08-23 as date), 14)))) AND (store_sk#12 = s_store_sk#136))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias salesreturns
:              :  :  +- Union false, false
:              :  :     :- Project [store_sk#12, date_sk#13, sales_price#14, profit#15, cast(return_amt#16 as double) AS return_amt#409, cast(net_loss#17 as double) AS net_loss#410]
:              :  :     :  +- Project [ss_store_sk#72 AS store_sk#12, ss_sold_date_sk#65 AS date_sk#13, ss_ext_sales_price#80 AS sales_price#14, ss_net_profit#87 AS profit#15, cast(0 as decimal(7,2)) AS return_amt#16, cast(0 as decimal(7,2)) AS net_loss#17]
:              :  :     :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :     :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#65,ss_sold_time_sk#66,ss_item_sk#67,ss_customer_sk#68,ss_cdemo_sk#69,ss_hdemo_sk#70,ss_addr_sk#71,ss_store_sk#72,ss_promo_sk#73,ss_ticket_number#74,ss_quantity#75,ss_wholesale_cost#76,ss_list_price#77,ss_sales_price#78,ss_ext_discount_amt#79,ss_ext_sales_price#80,ss_ext_wholesale_cost#81,ss_ext_list_price#82,ss_ext_tax#83,ss_coupon_amt#84,ss_net_paid#85,ss_net_paid_inc_tax#86,ss_net_profit#87] parquet
:              :  :     +- Project [store_sk#18, date_sk#19, cast(sales_price#20 as double) AS sales_price#411, cast(profit#21 as double) AS profit#412, return_amt#22, net_loss#23]
:              :  :        +- Project [sr_store_sk#95 AS store_sk#18, sr_returned_date_sk#88 AS date_sk#19, cast(0 as decimal(7,2)) AS sales_price#20, cast(0 as decimal(7,2)) AS profit#21, sr_return_amt#99 AS return_amt#22, sr_net_loss#107 AS net_loss#23]
:              :  :           +- SubqueryAlias spark_catalog.tpcds.store_returns
:              :  :              +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#88,sr_return_time_sk#89,sr_item_sk#90,sr_customer_sk#91,sr_cdemo_sk#92,sr_hdemo_sk#93,sr_addr_sk#94,sr_store_sk#95,sr_reason_sk#96,sr_ticket_number#97,sr_return_quantity#98,sr_return_amt#99,sr_return_tax#100,sr_return_amt_inc_tax#101,sr_fee#102,sr_return_ship_cost#103,sr_refunded_cash#104,sr_reversed_charge#105,sr_store_credit#106,sr_net_loss#107] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#108,d_date_id#109,d_date#110,d_month_seq#111,d_week_seq#112,d_quarter_seq#113,d_year#114,d_dow#115,d_moy#116,d_dom#117,d_qoy#118,d_fy_year#119,d_fy_quarter_seq#120,d_fy_week_seq#121,d_day_name#122,d_quarter_name#123,d_holiday#124,d_weekend#125,d_following_holiday#126,d_first_dom#127,d_last_dom#128,d_same_day_ly#129,d_same_day_lq#130,d_current_day#131,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.store
:                 +- Relation spark_catalog.tpcds.store[s_store_sk#136,s_store_id#137,s_rec_start_date#138,s_rec_end_date#139,s_closed_date_sk#140,s_store_name#141,s_number_employees#142,s_floor_space#143,s_hours#144,s_manager#145,s_market_id#146,s_geography_class#147,s_market_desc#148,s_market_manager#149,s_division_id#150,s_division_name#151,s_company_id#152,s_company_name#153,s_street_number#154,s_street_name#155,s_street_type#156,s_suite_number#157,s_city#158,s_county#159,... 5 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias csr
:     +- Aggregate [cp_catalog_page_id#227], [cp_catalog_page_id#227, sum(sales_price#30) AS sales#40, sum(profit#31) AS profit#41, sum(return_amt#413) AS returns#42, sum(net_loss#414) AS profit_loss#43]
:        +- Filter (((date_sk#29 = d_date_sk#319) AND ((cast(d_date#321 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#321 as date) <= date_add(cast(2000-08-23 as date), 14)))) AND (page_sk#28 = cp_catalog_page_sk#226))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias salesreturns
:              :  :  +- Union false, false
:              :  :     :- Project [page_sk#28, date_sk#29, sales_price#30, profit#31, cast(return_amt#32 as double) AS return_amt#413, cast(net_loss#33 as double) AS net_loss#414]
:              :  :     :  +- Project [cs_catalog_page_sk#177 AS page_sk#28, cs_sold_date_sk#165 AS date_sk#29, cs_ext_sales_price#188 AS sales_price#30, cs_net_profit#198 AS profit#31, cast(0 as decimal(7,2)) AS return_amt#32, cast(0 as decimal(7,2)) AS net_loss#33]
:              :  :     :     +- SubqueryAlias spark_catalog.tpcds.catalog_sales
:              :  :     :        +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#165,cs_sold_time_sk#166,cs_ship_date_sk#167,cs_bill_customer_sk#168,cs_bill_cdemo_sk#169,cs_bill_hdemo_sk#170,cs_bill_addr_sk#171,cs_ship_customer_sk#172,cs_ship_cdemo_sk#173,cs_ship_hdemo_sk#174,cs_ship_addr_sk#175,cs_call_center_sk#176,cs_catalog_page_sk#177,cs_ship_mode_sk#178,cs_warehouse_sk#179,cs_item_sk#180,cs_promo_sk#181,cs_order_number#182,cs_quantity#183,cs_wholesale_cost#184,cs_list_price#185,cs_sales_price#186,cs_ext_discount_amt#187,cs_ext_sales_price#188,... 10 more fields] parquet
:              :  :     +- Project [page_sk#34, date_sk#35, cast(sales_price#36 as double) AS sales_price#415, cast(profit#37 as double) AS profit#416, return_amt#38, net_loss#39]
:              :  :        +- Project [cr_catalog_page_sk#211 AS page_sk#34, cr_returned_date_sk#199 AS date_sk#35, cast(0 as decimal(7,2)) AS sales_price#36, cast(0 as decimal(7,2)) AS profit#37, cr_return_amount#217 AS return_amt#38, cr_net_loss#225 AS net_loss#39]
:              :  :           +- SubqueryAlias spark_catalog.tpcds.catalog_returns
:              :  :              +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#199,cr_returned_time_sk#200,cr_item_sk#201,cr_refunded_customer_sk#202,cr_refunded_cdemo_sk#203,cr_refunded_hdemo_sk#204,cr_refunded_addr_sk#205,cr_returning_customer_sk#206,cr_returning_cdemo_sk#207,cr_returning_hdemo_sk#208,cr_returning_addr_sk#209,cr_call_center_sk#210,cr_catalog_page_sk#211,cr_ship_mode_sk#212,cr_warehouse_sk#213,cr_reason_sk#214,cr_order_number#215,cr_return_quantity#216,cr_return_amount#217,cr_return_tax#218,cr_return_amt_inc_tax#219,cr_fee#220,cr_return_ship_cost#221,cr_refunded_cash#222,... 3 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#319,d_date_id#320,d_date#321,d_month_seq#322,d_week_seq#323,d_quarter_seq#324,d_year#325,d_dow#326,d_moy#327,d_dom#328,d_qoy#329,d_fy_year#330,d_fy_quarter_seq#331,d_fy_week_seq#332,d_day_name#333,d_quarter_name#334,d_holiday#335,d_weekend#336,d_following_holiday#337,d_first_dom#338,d_last_dom#339,d_same_day_ly#340,d_same_day_lq#341,d_current_day#342,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.catalog_page
:                 +- Relation spark_catalog.tpcds.catalog_page[cp_catalog_page_sk#226,cp_catalog_page_id#227,cp_start_date_sk#228,cp_end_date_sk#229,cp_department#230,cp_catalog_number#231,cp_catalog_page_number#232,cp_description#233,cp_type#234] parquet
:- CTERelationDef 2, false
:  +- SubqueryAlias wsr
:     +- Aggregate [web_site_id#294], [web_site_id#294, sum(sales_price#46) AS sales#56, sum(profit#47) AS profit#57, sum(return_amt#417) AS returns#58, sum(net_loss#418) AS profit_loss#59]
:        +- Filter (((date_sk#45 = d_date_sk#381) AND ((cast(d_date#383 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#383 as date) <= date_add(cast(2000-08-23 as date), 14)))) AND (wsr_web_site_sk#44 = web_site_sk#293))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias salesreturns
:              :  :  +- Union false, false
:              :  :     :- Project [wsr_web_site_sk#44, date_sk#45, sales_price#46, profit#47, cast(return_amt#48 as double) AS return_amt#417, cast(net_loss#49 as double) AS net_loss#418]
:              :  :     :  +- Project [ws_web_site_sk#248 AS wsr_web_site_sk#44, ws_sold_date_sk#235 AS date_sk#45, ws_ext_sales_price#258 AS sales_price#46, ws_net_profit#268 AS profit#47, cast(0 as decimal(7,2)) AS return_amt#48, cast(0 as decimal(7,2)) AS net_loss#49]
:              :  :     :     +- SubqueryAlias spark_catalog.tpcds.web_sales
:              :  :     :        +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#235,ws_sold_time_sk#236,ws_ship_date_sk#237,ws_item_sk#238,ws_bill_customer_sk#239,ws_bill_cdemo_sk#240,ws_bill_hdemo_sk#241,ws_bill_addr_sk#242,ws_ship_customer_sk#243,ws_ship_cdemo_sk#244,ws_ship_hdemo_sk#245,ws_ship_addr_sk#246,ws_web_page_sk#247,ws_web_site_sk#248,ws_ship_mode_sk#249,ws_warehouse_sk#250,ws_promo_sk#251,ws_order_number#252,ws_quantity#253,ws_wholesale_cost#254,ws_list_price#255,ws_sales_price#256,ws_ext_discount_amt#257,ws_ext_sales_price#258,... 10 more fields] parquet
:              :  :     +- Project [wsr_web_site_sk#50, date_sk#51, cast(sales_price#52 as double) AS sales_price#419, cast(profit#53 as double) AS profit#420, return_amt#54, net_loss#55]
:              :  :        +- Project [ws_web_site_sk#360 AS wsr_web_site_sk#50, wr_returned_date_sk#269 AS date_sk#51, cast(0 as decimal(7,2)) AS sales_price#52, cast(0 as decimal(7,2)) AS profit#53, wr_return_amt#284 AS return_amt#54, wr_net_loss#292 AS net_loss#55]
:              :  :           +- Join LeftOuter, ((wr_item_sk#271 = ws_item_sk#350) AND (wr_order_number#282 = ws_order_number#364))
:              :  :              :- SubqueryAlias spark_catalog.tpcds.web_returns
:              :  :              :  +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#269,wr_returned_time_sk#270,wr_item_sk#271,wr_refunded_customer_sk#272,wr_refunded_cdemo_sk#273,wr_refunded_hdemo_sk#274,wr_refunded_addr_sk#275,wr_returning_customer_sk#276,wr_returning_cdemo_sk#277,wr_returning_hdemo_sk#278,wr_returning_addr_sk#279,wr_web_page_sk#280,wr_reason_sk#281,wr_order_number#282,wr_return_quantity#283,wr_return_amt#284,wr_return_tax#285,wr_return_amt_inc_tax#286,wr_fee#287,wr_return_ship_cost#288,wr_refunded_cash#289,wr_reversed_charge#290,wr_account_credit#291,wr_net_loss#292] parquet
:              :  :              +- SubqueryAlias spark_catalog.tpcds.web_sales
:              :  :                 +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#347,ws_sold_time_sk#348,ws_ship_date_sk#349,ws_item_sk#350,ws_bill_customer_sk#351,ws_bill_cdemo_sk#352,ws_bill_hdemo_sk#353,ws_bill_addr_sk#354,ws_ship_customer_sk#355,ws_ship_cdemo_sk#356,ws_ship_hdemo_sk#357,ws_ship_addr_sk#358,ws_web_page_sk#359,ws_web_site_sk#360,ws_ship_mode_sk#361,ws_warehouse_sk#362,ws_promo_sk#363,ws_order_number#364,ws_quantity#365,ws_wholesale_cost#366,ws_list_price#367,ws_sales_price#368,ws_ext_discount_amt#369,ws_ext_sales_price#370,... 10 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#381,d_date_id#382,d_date#383,d_month_seq#384,d_week_seq#385,d_quarter_seq#386,d_year#387,d_dow#388,d_moy#389,d_dom#390,d_qoy#391,d_fy_year#392,d_fy_quarter_seq#393,d_fy_week_seq#394,d_day_name#395,d_quarter_name#396,d_holiday#397,d_weekend#398,d_following_holiday#399,d_first_dom#400,d_last_dom#401,d_same_day_ly#402,d_same_day_lq#403,d_current_day#404,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.web_site
:                 +- Relation spark_catalog.tpcds.web_site[web_site_sk#293,web_site_id#294,web_rec_start_date#295,web_rec_end_date#296,web_name#297,web_open_date_sk#298,web_close_date_sk#299,web_class#300,web_manager#301,web_mkt_id#302,web_mkt_class#303,web_mkt_desc#304,web_market_manager#305,web_company_id#306,web_company_name#307,web_street_number#308,web_street_name#309,web_street_type#310,web_suite_number#311,web_city#312,web_county#313,web_state#314,web_zip#315,web_country#316,... 2 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [channel#452 ASC NULLS FIRST, id#453 ASC NULLS FIRST], true
         +- Aggregate [channel#452, id#453, spark_grouping_id#451L], [channel#452, id#453, sum(sales#24) AS sales#9, sum(returns#26) AS returns#10, sum(profit#2) AS profit#11]
            +- Expand [[channel#0, id#1, sales#24, returns#26, profit#2, channel#449, id#450, 0], [channel#0, id#1, sales#24, returns#26, profit#2, channel#449, null, 1], [channel#0, id#1, sales#24, returns#26, profit#2, null, null, 3]], [channel#0, id#1, sales#24, returns#26, profit#2, channel#452, id#453, spark_grouping_id#451L]
               +- Project [channel#0, id#1, sales#24, returns#26, profit#2, channel#0 AS channel#449, id#1 AS id#450]
                  +- SubqueryAlias x
                     +- Union false, false
                        :- Union false, false
                        :  :- Project [store channel AS channel#0, concat(store, s_store_id#137) AS id#1, sales#24, returns#26, (profit#25 - profit_loss#27) AS profit#2]
                        :  :  +- SubqueryAlias ssr
                        :  :     +- CTERelationRef 0, true, [s_store_id#137, sales#24, profit#25, returns#26, profit_loss#27], false
                        :  +- Project [catalog channel AS channel#3, concat(catalog_page, cp_catalog_page_id#227) AS id#4, sales#40, returns#42, (profit#41 - profit_loss#43) AS profit#5]
                        :     +- SubqueryAlias csr
                        :        +- CTERelationRef 1, true, [cp_catalog_page_id#227, sales#40, profit#41, returns#42, profit_loss#43], false
                        +- Project [web channel AS channel#6, concat(web_site, web_site_id#294) AS id#7, sales#56, returns#58, (profit#57 - profit_loss#59) AS profit#8]
                           +- SubqueryAlias wsr
                              +- CTERelationRef 2, true, [web_site_id#294, sales#56, profit#57, returns#58, profit_loss#59], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#452 ASC NULLS FIRST, id#453 ASC NULLS FIRST], true
      +- Aggregate [channel#452, id#453, spark_grouping_id#451L], [channel#452, id#453, sum(sales#24) AS sales#9, sum(returns#26) AS returns#10, sum(profit#2) AS profit#11]
         +- Expand [[sales#24, returns#26, profit#2, channel#449, id#450, 0], [sales#24, returns#26, profit#2, channel#449, null, 1], [sales#24, returns#26, profit#2, null, null, 3]], [sales#24, returns#26, profit#2, channel#452, id#453, spark_grouping_id#451L]
            +- Union false, false
               :- Aggregate [s_store_id#137], [sum(sales_price#14) AS sales#24, sum(return_amt#409) AS returns#26, (sum(profit#15) - sum(net_loss#410)) AS profit#2, store channel AS channel#449, concat(store, s_store_id#137) AS id#450]
               :  +- Project [sales_price#14, profit#15, return_amt#409, net_loss#410, s_store_id#137]
               :     +- Join Inner, (store_sk#12 = s_store_sk#136)
               :        :- Project [store_sk#12, sales_price#14, profit#15, return_amt#409, net_loss#410]
               :        :  +- Join Inner, (date_sk#13 = d_date_sk#108)
               :        :     :- Union false, false
               :        :     :  :- Project [ss_store_sk#72 AS store_sk#12, ss_sold_date_sk#65 AS date_sk#13, ss_ext_sales_price#80 AS sales_price#14, ss_net_profit#87 AS profit#15, 0.0 AS return_amt#409, 0.0 AS net_loss#410]
               :        :     :  :  +- Filter (isnotnull(ss_sold_date_sk#65) AND isnotnull(ss_store_sk#72))
               :        :     :  :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#65,ss_sold_time_sk#66,ss_item_sk#67,ss_customer_sk#68,ss_cdemo_sk#69,ss_hdemo_sk#70,ss_addr_sk#71,ss_store_sk#72,ss_promo_sk#73,ss_ticket_number#74,ss_quantity#75,ss_wholesale_cost#76,ss_list_price#77,ss_sales_price#78,ss_ext_discount_amt#79,ss_ext_sales_price#80,ss_ext_wholesale_cost#81,ss_ext_list_price#82,ss_ext_tax#83,ss_coupon_amt#84,ss_net_paid#85,ss_net_paid_inc_tax#86,ss_net_profit#87] parquet
               :        :     :  +- Project [sr_store_sk#95 AS store_sk#18, sr_returned_date_sk#88 AS date_sk#19, 0.0 AS sales_price#411, 0.0 AS profit#412, sr_return_amt#99 AS return_amt#22, sr_net_loss#107 AS net_loss#23]
               :        :     :     +- Filter (isnotnull(sr_returned_date_sk#88) AND isnotnull(sr_store_sk#95))
               :        :     :        +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#88,sr_return_time_sk#89,sr_item_sk#90,sr_customer_sk#91,sr_cdemo_sk#92,sr_hdemo_sk#93,sr_addr_sk#94,sr_store_sk#95,sr_reason_sk#96,sr_ticket_number#97,sr_return_quantity#98,sr_return_amt#99,sr_return_tax#100,sr_return_amt_inc_tax#101,sr_fee#102,sr_return_ship_cost#103,sr_refunded_cash#104,sr_reversed_charge#105,sr_store_credit#106,sr_net_loss#107] parquet
               :        :     +- Project [d_date_sk#108]
               :        :        +- Filter ((isnotnull(d_date#110) AND ((cast(d_date#110 as date) >= 2000-08-23) AND (cast(d_date#110 as date) <= 2000-09-06))) AND isnotnull(d_date_sk#108))
               :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#108,d_date_id#109,d_date#110,d_month_seq#111,d_week_seq#112,d_quarter_seq#113,d_year#114,d_dow#115,d_moy#116,d_dom#117,d_qoy#118,d_fy_year#119,d_fy_quarter_seq#120,d_fy_week_seq#121,d_day_name#122,d_quarter_name#123,d_holiday#124,d_weekend#125,d_following_holiday#126,d_first_dom#127,d_last_dom#128,d_same_day_ly#129,d_same_day_lq#130,d_current_day#131,... 4 more fields] parquet
               :        +- Project [s_store_sk#136, s_store_id#137]
               :           +- Filter isnotnull(s_store_sk#136)
               :              +- Relation spark_catalog.tpcds.store[s_store_sk#136,s_store_id#137,s_rec_start_date#138,s_rec_end_date#139,s_closed_date_sk#140,s_store_name#141,s_number_employees#142,s_floor_space#143,s_hours#144,s_manager#145,s_market_id#146,s_geography_class#147,s_market_desc#148,s_market_manager#149,s_division_id#150,s_division_name#151,s_company_id#152,s_company_name#153,s_street_number#154,s_street_name#155,s_street_type#156,s_suite_number#157,s_city#158,s_county#159,... 5 more fields] parquet
               :- Aggregate [cp_catalog_page_id#227], [sum(sales_price#30) AS sales#40, sum(return_amt#413) AS returns#42, (sum(profit#31) - sum(net_loss#414)) AS profit#5, catalog channel AS channel#454, concat(catalog_page, cp_catalog_page_id#227) AS id#455]
               :  +- Project [sales_price#30, profit#31, return_amt#413, net_loss#414, cp_catalog_page_id#227]
               :     +- Join Inner, (page_sk#28 = cp_catalog_page_sk#226)
               :        :- Project [page_sk#28, sales_price#30, profit#31, return_amt#413, net_loss#414]
               :        :  +- Join Inner, (date_sk#29 = d_date_sk#319)
               :        :     :- Union false, false
               :        :     :  :- Project [cs_catalog_page_sk#177 AS page_sk#28, cs_sold_date_sk#165 AS date_sk#29, cs_ext_sales_price#188 AS sales_price#30, cs_net_profit#198 AS profit#31, 0.0 AS return_amt#413, 0.0 AS net_loss#414]
               :        :     :  :  +- Filter (isnotnull(cs_sold_date_sk#165) AND isnotnull(cs_catalog_page_sk#177))
               :        :     :  :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#165,cs_sold_time_sk#166,cs_ship_date_sk#167,cs_bill_customer_sk#168,cs_bill_cdemo_sk#169,cs_bill_hdemo_sk#170,cs_bill_addr_sk#171,cs_ship_customer_sk#172,cs_ship_cdemo_sk#173,cs_ship_hdemo_sk#174,cs_ship_addr_sk#175,cs_call_center_sk#176,cs_catalog_page_sk#177,cs_ship_mode_sk#178,cs_warehouse_sk#179,cs_item_sk#180,cs_promo_sk#181,cs_order_number#182,cs_quantity#183,cs_wholesale_cost#184,cs_list_price#185,cs_sales_price#186,cs_ext_discount_amt#187,cs_ext_sales_price#188,... 10 more fields] parquet
               :        :     :  +- Project [cr_catalog_page_sk#211 AS page_sk#34, cr_returned_date_sk#199 AS date_sk#35, 0.0 AS sales_price#415, 0.0 AS profit#416, cr_return_amount#217 AS return_amt#38, cr_net_loss#225 AS net_loss#39]
               :        :     :     +- Filter (isnotnull(cr_returned_date_sk#199) AND isnotnull(cr_catalog_page_sk#211))
               :        :     :        +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#199,cr_returned_time_sk#200,cr_item_sk#201,cr_refunded_customer_sk#202,cr_refunded_cdemo_sk#203,cr_refunded_hdemo_sk#204,cr_refunded_addr_sk#205,cr_returning_customer_sk#206,cr_returning_cdemo_sk#207,cr_returning_hdemo_sk#208,cr_returning_addr_sk#209,cr_call_center_sk#210,cr_catalog_page_sk#211,cr_ship_mode_sk#212,cr_warehouse_sk#213,cr_reason_sk#214,cr_order_number#215,cr_return_quantity#216,cr_return_amount#217,cr_return_tax#218,cr_return_amt_inc_tax#219,cr_fee#220,cr_return_ship_cost#221,cr_refunded_cash#222,... 3 more fields] parquet
               :        :     +- Project [d_date_sk#319]
               :        :        +- Filter ((isnotnull(d_date#321) AND ((cast(d_date#321 as date) >= 2000-08-23) AND (cast(d_date#321 as date) <= 2000-09-06))) AND isnotnull(d_date_sk#319))
               :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#319,d_date_id#320,d_date#321,d_month_seq#322,d_week_seq#323,d_quarter_seq#324,d_year#325,d_dow#326,d_moy#327,d_dom#328,d_qoy#329,d_fy_year#330,d_fy_quarter_seq#331,d_fy_week_seq#332,d_day_name#333,d_quarter_name#334,d_holiday#335,d_weekend#336,d_following_holiday#337,d_first_dom#338,d_last_dom#339,d_same_day_ly#340,d_same_day_lq#341,d_current_day#342,... 4 more fields] parquet
               :        +- Project [cp_catalog_page_sk#226, cp_catalog_page_id#227]
               :           +- Filter isnotnull(cp_catalog_page_sk#226)
               :              +- Relation spark_catalog.tpcds.catalog_page[cp_catalog_page_sk#226,cp_catalog_page_id#227,cp_start_date_sk#228,cp_end_date_sk#229,cp_department#230,cp_catalog_number#231,cp_catalog_page_number#232,cp_description#233,cp_type#234] parquet
               +- Aggregate [web_site_id#294], [sum(sales_price#46) AS sales#56, sum(return_amt#417) AS returns#58, (sum(profit#47) - sum(net_loss#418)) AS profit#8, web channel AS channel#456, concat(web_site, web_site_id#294) AS id#457]
                  +- Project [sales_price#46, profit#47, return_amt#417, net_loss#418, web_site_id#294]
                     +- Join Inner, (wsr_web_site_sk#44 = web_site_sk#293)
                        :- Project [wsr_web_site_sk#44, sales_price#46, profit#47, return_amt#417, net_loss#418]
                        :  +- Join Inner, (date_sk#45 = d_date_sk#381)
                        :     :- Union false, false
                        :     :  :- Project [ws_web_site_sk#248 AS wsr_web_site_sk#44, ws_sold_date_sk#235 AS date_sk#45, ws_ext_sales_price#258 AS sales_price#46, ws_net_profit#268 AS profit#47, 0.0 AS return_amt#417, 0.0 AS net_loss#418]
                        :     :  :  +- Filter (isnotnull(ws_sold_date_sk#235) AND isnotnull(ws_web_site_sk#248))
                        :     :  :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#235,ws_sold_time_sk#236,ws_ship_date_sk#237,ws_item_sk#238,ws_bill_customer_sk#239,ws_bill_cdemo_sk#240,ws_bill_hdemo_sk#241,ws_bill_addr_sk#242,ws_ship_customer_sk#243,ws_ship_cdemo_sk#244,ws_ship_hdemo_sk#245,ws_ship_addr_sk#246,ws_web_page_sk#247,ws_web_site_sk#248,ws_ship_mode_sk#249,ws_warehouse_sk#250,ws_promo_sk#251,ws_order_number#252,ws_quantity#253,ws_wholesale_cost#254,ws_list_price#255,ws_sales_price#256,ws_ext_discount_amt#257,ws_ext_sales_price#258,... 10 more fields] parquet
                        :     :  +- Project [ws_web_site_sk#360 AS wsr_web_site_sk#50, wr_returned_date_sk#269 AS date_sk#51, 0.0 AS sales_price#419, 0.0 AS profit#420, wr_return_amt#284 AS return_amt#54, wr_net_loss#292 AS net_loss#55]
                        :     :     +- Join Inner, ((wr_item_sk#271 = ws_item_sk#350) AND (wr_order_number#282 = ws_order_number#364))
                        :     :        :- Project [wr_returned_date_sk#269, wr_item_sk#271, wr_order_number#282, wr_return_amt#284, wr_net_loss#292]
                        :     :        :  +- Filter isnotnull(wr_returned_date_sk#269)
                        :     :        :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#269,wr_returned_time_sk#270,wr_item_sk#271,wr_refunded_customer_sk#272,wr_refunded_cdemo_sk#273,wr_refunded_hdemo_sk#274,wr_refunded_addr_sk#275,wr_returning_customer_sk#276,wr_returning_cdemo_sk#277,wr_returning_hdemo_sk#278,wr_returning_addr_sk#279,wr_web_page_sk#280,wr_reason_sk#281,wr_order_number#282,wr_return_quantity#283,wr_return_amt#284,wr_return_tax#285,wr_return_amt_inc_tax#286,wr_fee#287,wr_return_ship_cost#288,wr_refunded_cash#289,wr_reversed_charge#290,wr_account_credit#291,wr_net_loss#292] parquet
                        :     :        +- Project [ws_item_sk#350, ws_web_site_sk#360, ws_order_number#364]
                        :     :           +- Filter ((isnotnull(ws_item_sk#350) AND isnotnull(ws_order_number#364)) AND isnotnull(ws_web_site_sk#360))
                        :     :              +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#347,ws_sold_time_sk#348,ws_ship_date_sk#349,ws_item_sk#350,ws_bill_customer_sk#351,ws_bill_cdemo_sk#352,ws_bill_hdemo_sk#353,ws_bill_addr_sk#354,ws_ship_customer_sk#355,ws_ship_cdemo_sk#356,ws_ship_hdemo_sk#357,ws_ship_addr_sk#358,ws_web_page_sk#359,ws_web_site_sk#360,ws_ship_mode_sk#361,ws_warehouse_sk#362,ws_promo_sk#363,ws_order_number#364,ws_quantity#365,ws_wholesale_cost#366,ws_list_price#367,ws_sales_price#368,ws_ext_discount_amt#369,ws_ext_sales_price#370,... 10 more fields] parquet
                        :     +- Project [d_date_sk#381]
                        :        +- Filter ((isnotnull(d_date#383) AND ((cast(d_date#383 as date) >= 2000-08-23) AND (cast(d_date#383 as date) <= 2000-09-06))) AND isnotnull(d_date_sk#381))
                        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#381,d_date_id#382,d_date#383,d_month_seq#384,d_week_seq#385,d_quarter_seq#386,d_year#387,d_dow#388,d_moy#389,d_dom#390,d_qoy#391,d_fy_year#392,d_fy_quarter_seq#393,d_fy_week_seq#394,d_day_name#395,d_quarter_name#396,d_holiday#397,d_weekend#398,d_following_holiday#399,d_first_dom#400,d_last_dom#401,d_same_day_ly#402,d_same_day_lq#403,d_current_day#404,... 4 more fields] parquet
                        +- Project [web_site_sk#293, web_site_id#294]
                           +- Filter isnotnull(web_site_sk#293)
                              +- Relation spark_catalog.tpcds.web_site[web_site_sk#293,web_site_id#294,web_rec_start_date#295,web_rec_end_date#296,web_name#297,web_open_date_sk#298,web_close_date_sk#299,web_class#300,web_manager#301,web_mkt_id#302,web_mkt_class#303,web_mkt_desc#304,web_market_manager#305,web_company_id#306,web_company_name#307,web_street_number#308,web_street_name#309,web_street_type#310,web_suite_number#311,web_city#312,web_county#313,web_state#314,web_zip#315,web_country#316,... 2 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[channel#452 ASC NULLS FIRST,id#453 ASC NULLS FIRST], output=[channel#452,id#453,sales#9,returns#10,profit#11])
   +- HashAggregate(keys=[channel#452, id#453, spark_grouping_id#451L], functions=[sum(sales#24), sum(returns#26), sum(profit#2)], output=[channel#452, id#453, sales#9, returns#10, profit#11])
      +- Exchange hashpartitioning(channel#452, id#453, spark_grouping_id#451L, 200), ENSURE_REQUIREMENTS, [plan_id=281]
         +- HashAggregate(keys=[channel#452, id#453, spark_grouping_id#451L], functions=[partial_sum(sales#24), partial_sum(returns#26), partial_sum(profit#2)], output=[channel#452, id#453, spark_grouping_id#451L, sum#461, sum#462, sum#463])
            +- Expand [[sales#24, returns#26, profit#2, channel#449, id#450, 0], [sales#24, returns#26, profit#2, channel#449, null, 1], [sales#24, returns#26, profit#2, null, null, 3]], [sales#24, returns#26, profit#2, channel#452, id#453, spark_grouping_id#451L]
               +- Union
                  :- HashAggregate(keys=[s_store_id#137], functions=[sum(sales_price#14), sum(return_amt#409), sum(profit#15), sum(net_loss#410)], output=[sales#24, returns#26, profit#2, channel#449, id#450])
                  :  +- Exchange hashpartitioning(s_store_id#137, 200), ENSURE_REQUIREMENTS, [plan_id=248]
                  :     +- HashAggregate(keys=[s_store_id#137], functions=[partial_sum(sales_price#14), partial_sum(return_amt#409), partial_sum(profit#15), partial_sum(net_loss#410)], output=[s_store_id#137, sum#468, sum#469, sum#470, sum#471])
                  :        +- Project [sales_price#14, profit#15, return_amt#409, net_loss#410, s_store_id#137]
                  :           +- BroadcastHashJoin [store_sk#12], [s_store_sk#136], Inner, BuildRight, false
                  :              :- Project [store_sk#12, sales_price#14, profit#15, return_amt#409, net_loss#410]
                  :              :  +- BroadcastHashJoin [date_sk#13], [d_date_sk#108], Inner, BuildRight, false
                  :              :     :- Union
                  :              :     :  :- Project [ss_store_sk#72 AS store_sk#12, ss_sold_date_sk#65 AS date_sk#13, ss_ext_sales_price#80 AS sales_price#14, ss_net_profit#87 AS profit#15, 0.0 AS return_amt#409, 0.0 AS net_loss#410]
                  :              :     :  :  +- Filter (isnotnull(ss_sold_date_sk#65) AND isnotnull(ss_store_sk#72))
                  :              :     :  :     +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#65,ss_store_sk#72,ss_ext_sales_price#80,ss_net_profit#87] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#65), isnotnull(ss_store_sk#72)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_ext_sales_price:double,ss_net_profit:double>
                  :              :     :  +- Project [sr_store_sk#95 AS store_sk#18, sr_returned_date_sk#88 AS date_sk#19, 0.0 AS sales_price#411, 0.0 AS profit#412, sr_return_amt#99 AS return_amt#22, sr_net_loss#107 AS net_loss#23]
                  :              :     :     +- Filter (isnotnull(sr_returned_date_sk#88) AND isnotnull(sr_store_sk#95))
                  :              :     :        +- FileScan parquet spark_catalog.tpcds.store_returns[sr_returned_date_sk#88,sr_store_sk#95,sr_return_amt#99,sr_net_loss#107] Batched: true, DataFilters: [isnotnull(sr_returned_date_sk#88), isnotnull(sr_store_sk#95)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_returned_date_sk), IsNotNull(sr_store_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_store_sk:int,sr_return_amt:double,sr_net_loss:double>
                  :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=239]
                  :              :        +- Project [d_date_sk#108]
                  :              :           +- Filter (((isnotnull(d_date#110) AND (cast(d_date#110 as date) >= 2000-08-23)) AND (cast(d_date#110 as date) <= 2000-09-06)) AND isnotnull(d_date_sk#108))
                  :              :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#108,d_date#110] Batched: true, DataFilters: [isnotnull(d_date#110), (cast(d_date#110 as date) >= 2000-08-23), (cast(d_date#110 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=243]
                  :                 +- Filter isnotnull(s_store_sk#136)
                  :                    +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#136,s_store_id#137] Batched: true, DataFilters: [isnotnull(s_store_sk#136)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_id:string>
                  :- HashAggregate(keys=[cp_catalog_page_id#227], functions=[sum(sales_price#30), sum(return_amt#413), sum(profit#31), sum(net_loss#414)], output=[sales#40, returns#42, profit#5, channel#454, id#455])
                  :  +- Exchange hashpartitioning(cp_catalog_page_id#227, 200), ENSURE_REQUIREMENTS, [plan_id=259]
                  :     +- HashAggregate(keys=[cp_catalog_page_id#227], functions=[partial_sum(sales_price#30), partial_sum(return_amt#413), partial_sum(profit#31), partial_sum(net_loss#414)], output=[cp_catalog_page_id#227, sum#476, sum#477, sum#478, sum#479])
                  :        +- Project [sales_price#30, profit#31, return_amt#413, net_loss#414, cp_catalog_page_id#227]
                  :           +- BroadcastHashJoin [page_sk#28], [cp_catalog_page_sk#226], Inner, BuildRight, false
                  :              :- Project [page_sk#28, sales_price#30, profit#31, return_amt#413, net_loss#414]
                  :              :  +- BroadcastHashJoin [date_sk#29], [d_date_sk#319], Inner, BuildRight, false
                  :              :     :- Union
                  :              :     :  :- Project [cs_catalog_page_sk#177 AS page_sk#28, cs_sold_date_sk#165 AS date_sk#29, cs_ext_sales_price#188 AS sales_price#30, cs_net_profit#198 AS profit#31, 0.0 AS return_amt#413, 0.0 AS net_loss#414]
                  :              :     :  :  +- Filter (isnotnull(cs_sold_date_sk#165) AND isnotnull(cs_catalog_page_sk#177))
                  :              :     :  :     +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#165,cs_catalog_page_sk#177,cs_ext_sales_price#188,cs_net_profit#198] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#165), isnotnull(cs_catalog_page_sk#177)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_catalog_page_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_catalog_page_sk:int,cs_ext_sales_price:double,cs_net_profit:double>
                  :              :     :  +- Project [cr_catalog_page_sk#211 AS page_sk#34, cr_returned_date_sk#199 AS date_sk#35, 0.0 AS sales_price#415, 0.0 AS profit#416, cr_return_amount#217 AS return_amt#38, cr_net_loss#225 AS net_loss#39]
                  :              :     :     +- Filter (isnotnull(cr_returned_date_sk#199) AND isnotnull(cr_catalog_page_sk#211))
                  :              :     :        +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#199,cr_catalog_page_sk#211,cr_return_amount#217,cr_net_loss#225] Batched: true, DataFilters: [isnotnull(cr_returned_date_sk#199), isnotnull(cr_catalog_page_sk#211)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_returned_date_sk), IsNotNull(cr_catalog_page_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_catalog_page_sk:int,cr_return_amount:double,cr_net_loss:double>
                  :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=250]
                  :              :        +- Project [d_date_sk#319]
                  :              :           +- Filter (((isnotnull(d_date#321) AND (cast(d_date#321 as date) >= 2000-08-23)) AND (cast(d_date#321 as date) <= 2000-09-06)) AND isnotnull(d_date_sk#319))
                  :              :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#319,d_date#321] Batched: true, DataFilters: [isnotnull(d_date#321), (cast(d_date#321 as date) >= 2000-08-23), (cast(d_date#321 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=254]
                  :                 +- Filter isnotnull(cp_catalog_page_sk#226)
                  :                    +- FileScan parquet spark_catalog.tpcds.catalog_page[cp_catalog_page_sk#226,cp_catalog_page_id#227] Batched: true, DataFilters: [isnotnull(cp_catalog_page_sk#226)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cp_catalog_page_sk)], ReadSchema: struct<cp_catalog_page_sk:int,cp_catalog_page_id:string>
                  +- HashAggregate(keys=[web_site_id#294], functions=[sum(sales_price#46), sum(return_amt#417), sum(profit#47), sum(net_loss#418)], output=[sales#56, returns#58, profit#8, channel#456, id#457])
                     +- Exchange hashpartitioning(web_site_id#294, 200), ENSURE_REQUIREMENTS, [plan_id=275]
                        +- HashAggregate(keys=[web_site_id#294], functions=[partial_sum(sales_price#46), partial_sum(return_amt#417), partial_sum(profit#47), partial_sum(net_loss#418)], output=[web_site_id#294, sum#484, sum#485, sum#486, sum#487])
                           +- Project [sales_price#46, profit#47, return_amt#417, net_loss#418, web_site_id#294]
                              +- BroadcastHashJoin [wsr_web_site_sk#44], [web_site_sk#293], Inner, BuildRight, false
                                 :- Project [wsr_web_site_sk#44, sales_price#46, profit#47, return_amt#417, net_loss#418]
                                 :  +- BroadcastHashJoin [date_sk#45], [d_date_sk#381], Inner, BuildRight, false
                                 :     :- Union
                                 :     :  :- Project [ws_web_site_sk#248 AS wsr_web_site_sk#44, ws_sold_date_sk#235 AS date_sk#45, ws_ext_sales_price#258 AS sales_price#46, ws_net_profit#268 AS profit#47, 0.0 AS return_amt#417, 0.0 AS net_loss#418]
                                 :     :  :  +- Filter (isnotnull(ws_sold_date_sk#235) AND isnotnull(ws_web_site_sk#248))
                                 :     :  :     +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#235,ws_web_site_sk#248,ws_ext_sales_price#258,ws_net_profit#268] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#235), isnotnull(ws_web_site_sk#248)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_web_site_sk:int,ws_ext_sales_price:double,ws_net_profit:double>
                                 :     :  +- Project [ws_web_site_sk#360 AS wsr_web_site_sk#50, wr_returned_date_sk#269 AS date_sk#51, 0.0 AS sales_price#419, 0.0 AS profit#420, wr_return_amt#284 AS return_amt#54, wr_net_loss#292 AS net_loss#55]
                                 :     :     +- BroadcastHashJoin [wr_item_sk#271, wr_order_number#282], [ws_item_sk#350, ws_order_number#364], Inner, BuildLeft, false
                                 :     :        :- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[2, int, true] as bigint) & 4294967295))),false), [plan_id=261]
                                 :     :        :  +- Filter isnotnull(wr_returned_date_sk#269)
                                 :     :        :     +- FileScan parquet spark_catalog.tpcds.web_returns[wr_returned_date_sk#269,wr_item_sk#271,wr_order_number#282,wr_return_amt#284,wr_net_loss#292] Batched: true, DataFilters: [isnotnull(wr_returned_date_sk#269)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_returned_date_sk)], ReadSchema: struct<wr_returned_date_sk:int,wr_item_sk:int,wr_order_number:int,wr_return_amt:double,wr_net_los...
                                 :     :        +- Filter ((isnotnull(ws_item_sk#350) AND isnotnull(ws_order_number#364)) AND isnotnull(ws_web_site_sk#360))
                                 :     :           +- FileScan parquet spark_catalog.tpcds.web_sales[ws_item_sk#350,ws_web_site_sk#360,ws_order_number#364] Batched: true, DataFilters: [isnotnull(ws_item_sk#350), isnotnull(ws_order_number#364), isnotnull(ws_web_site_sk#360)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_order_number), IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_item_sk:int,ws_web_site_sk:int,ws_order_number:int>
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=266]
                                 :        +- Project [d_date_sk#381]
                                 :           +- Filter (((isnotnull(d_date#383) AND (cast(d_date#383 as date) >= 2000-08-23)) AND (cast(d_date#383 as date) <= 2000-09-06)) AND isnotnull(d_date_sk#381))
                                 :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#381,d_date#383] Batched: true, DataFilters: [isnotnull(d_date#383), (cast(d_date#383 as date) >= 2000-08-23), (cast(d_date#383 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=270]
                                    +- Filter isnotnull(web_site_sk#293)
                                       +- FileScan parquet spark_catalog.tpcds.web_site[web_site_sk#293,web_site_id#294] Batched: true, DataFilters: [isnotnull(web_site_sk#293)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_site_id:string>

Time taken: 4.306 seconds, Fetched 1 row(s)
