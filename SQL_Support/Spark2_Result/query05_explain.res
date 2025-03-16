== Parsed Logical Plan ==
CTE [ssr, csr, wsr]
:  :- 'SubqueryAlias `ssr`
:  :  +- 'Aggregate ['s_store_id], ['s_store_id, 'sum('sales_price) AS sales#24, 'sum('profit) AS profit#25, 'sum('return_amt) AS returns#26, 'sum('net_loss) AS profit_loss#27]
:  :     +- 'Filter ((('date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 14)))) && ('store_sk = 's_store_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'SubqueryAlias `salesreturns`
:  :           :  :  +- 'Union
:  :           :  :     :- 'Project ['ss_store_sk AS store_sk#12, 'ss_sold_date_sk AS date_sk#13, 'ss_ext_sales_price AS sales_price#14, 'ss_net_profit AS profit#15, cast(0 as decimal(7,2)) AS return_amt#16, cast(0 as decimal(7,2)) AS net_loss#17]
:  :           :  :     :  +- 'UnresolvedRelation `store_sales`
:  :           :  :     +- 'Project ['sr_store_sk AS store_sk#18, 'sr_returned_date_sk AS date_sk#19, cast(0 as decimal(7,2)) AS sales_price#20, cast(0 as decimal(7,2)) AS profit#21, 'sr_return_amt AS return_amt#22, 'sr_net_loss AS net_loss#23]
:  :           :  :        +- 'UnresolvedRelation `store_returns`
:  :           :  +- 'UnresolvedRelation `date_dim`
:  :           +- 'UnresolvedRelation `store`
:  :- 'SubqueryAlias `csr`
:  :  +- 'Aggregate ['cp_catalog_page_id], ['cp_catalog_page_id, 'sum('sales_price) AS sales#40, 'sum('profit) AS profit#41, 'sum('return_amt) AS returns#42, 'sum('net_loss) AS profit_loss#43]
:  :     +- 'Filter ((('date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 14)))) && ('page_sk = 'cp_catalog_page_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'SubqueryAlias `salesreturns`
:  :           :  :  +- 'Union
:  :           :  :     :- 'Project ['cs_catalog_page_sk AS page_sk#28, 'cs_sold_date_sk AS date_sk#29, 'cs_ext_sales_price AS sales_price#30, 'cs_net_profit AS profit#31, cast(0 as decimal(7,2)) AS return_amt#32, cast(0 as decimal(7,2)) AS net_loss#33]
:  :           :  :     :  +- 'UnresolvedRelation `catalog_sales`
:  :           :  :     +- 'Project ['cr_catalog_page_sk AS page_sk#34, 'cr_returned_date_sk AS date_sk#35, cast(0 as decimal(7,2)) AS sales_price#36, cast(0 as decimal(7,2)) AS profit#37, 'cr_return_amount AS return_amt#38, 'cr_net_loss AS net_loss#39]
:  :           :  :        +- 'UnresolvedRelation `catalog_returns`
:  :           :  +- 'UnresolvedRelation `date_dim`
:  :           +- 'UnresolvedRelation `catalog_page`
:  +- 'SubqueryAlias `wsr`
:     +- 'Aggregate ['web_site_id], ['web_site_id, 'sum('sales_price) AS sales#56, 'sum('profit) AS profit#57, 'sum('return_amt) AS returns#58, 'sum('net_loss) AS profit_loss#59]
:        +- 'Filter ((('date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 14)))) && ('wsr_web_site_sk = 'web_site_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'SubqueryAlias `salesreturns`
:              :  :  +- 'Union
:              :  :     :- 'Project ['ws_web_site_sk AS wsr_web_site_sk#44, 'ws_sold_date_sk AS date_sk#45, 'ws_ext_sales_price AS sales_price#46, 'ws_net_profit AS profit#47, cast(0 as decimal(7,2)) AS return_amt#48, cast(0 as decimal(7,2)) AS net_loss#49]
:              :  :     :  +- 'UnresolvedRelation `web_sales`
:              :  :     +- 'Project ['ws_web_site_sk AS wsr_web_site_sk#50, 'wr_returned_date_sk AS date_sk#51, cast(0 as decimal(7,2)) AS sales_price#52, cast(0 as decimal(7,2)) AS profit#53, 'wr_return_amt AS return_amt#54, 'wr_net_loss AS net_loss#55]
:              :  :        +- 'Join LeftOuter, (('wr_item_sk = 'ws_item_sk) && ('wr_order_number = 'ws_order_number))
:              :  :           :- 'UnresolvedRelation `web_returns`
:              :  :           +- 'UnresolvedRelation `web_sales`
:              :  +- 'UnresolvedRelation `date_dim`
:              +- 'UnresolvedRelation `web_site`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['channel ASC NULLS FIRST, 'id ASC NULLS FIRST], true
         +- 'Aggregate ['rollup('channel, 'id)], ['channel, 'id, 'sum('sales) AS sales#9, 'sum('returns) AS returns#10, 'sum('profit) AS profit#11]
            +- 'SubqueryAlias `x`
               +- 'Union
                  :- 'Union
                  :  :- 'Project [store channel AS channel#0, 'concat(store, 's_store_id) AS id#1, 'sales, 'returns, ('profit - 'profit_loss) AS profit#2]
                  :  :  +- 'UnresolvedRelation `ssr`
                  :  +- 'Project [catalog channel AS channel#3, 'concat(catalog_page, 'cp_catalog_page_id) AS id#4, 'sales, 'returns, ('profit - 'profit_loss) AS profit#5]
                  :     +- 'UnresolvedRelation `csr`
                  +- 'Project [web channel AS channel#6, 'concat(web_site, 'web_site_id) AS id#7, 'sales, 'returns, ('profit - 'profit_loss) AS profit#8]
                     +- 'UnresolvedRelation `wsr`

== Analyzed Logical Plan ==
channel: string, id: string, sales: double, returns: double, profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#346 ASC NULLS FIRST, id#347 ASC NULLS FIRST], true
      +- Aggregate [channel#346, id#347, spark_grouping_id#343], [channel#346, id#347, sum(sales#24) AS sales#9, sum(returns#26) AS returns#10, sum(profit#2) AS profit#11]
         +- Expand [List(channel#0, id#1, sales#24, returns#26, profit#2, channel#344, id#345, 0), List(channel#0, id#1, sales#24, returns#26, profit#2, channel#344, null, 1), List(channel#0, id#1, sales#24, returns#26, profit#2, null, null, 3)], [channel#0, id#1, sales#24, returns#26, profit#2, channel#346, id#347, spark_grouping_id#343]
            +- Project [channel#0, id#1, sales#24, returns#26, profit#2, channel#0 AS channel#344, id#1 AS id#345]
               +- SubqueryAlias `x`
                  +- Union
                     :- Union
                     :  :- Project [store channel AS channel#0, concat(store, s_store_id#134) AS id#1, sales#24, returns#26, (profit#25 - profit_loss#27) AS profit#2]
                     :  :  +- SubqueryAlias `ssr`
                     :  :     +- Aggregate [s_store_id#134], [s_store_id#134, sum(sales_price#14) AS sales#24, sum(profit#15) AS profit#25, sum(return_amt#162) AS returns#26, sum(net_loss#163) AS profit_loss#27]
                     :  :        +- Filter (((date_sk#13 = d_date_sk#105) && ((d_date#107 >= cast(cast(2000-08-23 as date) as string)) && (d_date#107 <= cast(date_add(cast(2000-08-23 as date), 14) as string)))) && (store_sk#12 = s_store_sk#133))
                     :  :           +- Join Inner
                     :  :              :- Join Inner
                     :  :              :  :- SubqueryAlias `salesreturns`
                     :  :              :  :  +- Union
                     :  :              :  :     :- Project [store_sk#12, date_sk#13, sales_price#14, profit#15, cast(return_amt#16 as double) AS return_amt#162, cast(net_loss#17 as double) AS net_loss#163]
                     :  :              :  :     :  +- Project [ss_store_sk#69 AS store_sk#12, ss_sold_date_sk#62 AS date_sk#13, ss_ext_sales_price#77 AS sales_price#14, ss_net_profit#84 AS profit#15, cast(0 as decimal(7,2)) AS return_amt#16, cast(0 as decimal(7,2)) AS net_loss#17]
                     :  :              :  :     :     +- SubqueryAlias `tpcds`.`store_sales`
                     :  :              :  :     :        +- Relation[ss_sold_date_sk#62,ss_sold_time_sk#63,ss_item_sk#64,ss_customer_sk#65,ss_cdemo_sk#66,ss_hdemo_sk#67,ss_addr_sk#68,ss_store_sk#69,ss_promo_sk#70,ss_ticket_number#71,ss_quantity#72,ss_wholesale_cost#73,ss_list_price#74,ss_sales_price#75,ss_ext_discount_amt#76,ss_ext_sales_price#77,ss_ext_wholesale_cost#78,ss_ext_list_price#79,ss_ext_tax#80,ss_coupon_amt#81,ss_net_paid#82,ss_net_paid_inc_tax#83,ss_net_profit#84] parquet
                     :  :              :  :     +- Project [store_sk#18, date_sk#19, cast(sales_price#20 as double) AS sales_price#164, cast(profit#21 as double) AS profit#165, return_amt#22, net_loss#23]
                     :  :              :  :        +- Project [sr_store_sk#92 AS store_sk#18, sr_returned_date_sk#85 AS date_sk#19, cast(0 as decimal(7,2)) AS sales_price#20, cast(0 as decimal(7,2)) AS profit#21, sr_return_amt#96 AS return_amt#22, sr_net_loss#104 AS net_loss#23]
                     :  :              :  :           +- SubqueryAlias `tpcds`.`store_returns`
                     :  :              :  :              +- Relation[sr_returned_date_sk#85,sr_return_time_sk#86,sr_item_sk#87,sr_customer_sk#88,sr_cdemo_sk#89,sr_hdemo_sk#90,sr_addr_sk#91,sr_store_sk#92,sr_reason_sk#93,sr_ticket_number#94,sr_return_quantity#95,sr_return_amt#96,sr_return_tax#97,sr_return_amt_inc_tax#98,sr_fee#99,sr_return_ship_cost#100,sr_refunded_cash#101,sr_reversed_charge#102,sr_store_credit#103,sr_net_loss#104] parquet
                     :  :              :  +- SubqueryAlias `tpcds`.`date_dim`
                     :  :              :     +- Relation[d_date_sk#105,d_date_id#106,d_date#107,d_month_seq#108,d_week_seq#109,d_quarter_seq#110,d_year#111,d_dow#112,d_moy#113,d_dom#114,d_qoy#115,d_fy_year#116,d_fy_quarter_seq#117,d_fy_week_seq#118,d_day_name#119,d_quarter_name#120,d_holiday#121,d_weekend#122,d_following_holiday#123,d_first_dom#124,d_last_dom#125,d_same_day_ly#126,d_same_day_lq#127,d_current_day#128,... 4 more fields] parquet
                     :  :              +- SubqueryAlias `tpcds`.`store`
                     :  :                 +- Relation[s_store_sk#133,s_store_id#134,s_rec_start_date#135,s_rec_end_date#136,s_closed_date_sk#137,s_store_name#138,s_number_employees#139,s_floor_space#140,s_hours#141,s_manager#142,s_market_id#143,s_geography_class#144,s_market_desc#145,s_market_manager#146,s_division_id#147,s_division_name#148,s_company_id#149,s_company_name#150,s_street_number#151,s_street_name#152,s_street_type#153,s_suite_number#154,s_city#155,s_county#156,... 5 more fields] parquet
                     :  +- Project [catalog channel AS channel#3, concat(catalog_page, cp_catalog_page_id#232) AS id#4, sales#40, returns#42, (profit#41 - profit_loss#43) AS profit#5]
                     :     +- SubqueryAlias `csr`
                     :        +- Aggregate [cp_catalog_page_id#232], [cp_catalog_page_id#232, sum(sales_price#30) AS sales#40, sum(profit#31) AS profit#41, sum(return_amt#240) AS returns#42, sum(net_loss#241) AS profit_loss#43]
                     :           +- Filter (((date_sk#29 = d_date_sk#105) && ((d_date#107 >= cast(cast(2000-08-23 as date) as string)) && (d_date#107 <= cast(date_add(cast(2000-08-23 as date), 14) as string)))) && (page_sk#28 = cp_catalog_page_sk#231))
                     :              +- Join Inner
                     :                 :- Join Inner
                     :                 :  :- SubqueryAlias `salesreturns`
                     :                 :  :  +- Union
                     :                 :  :     :- Project [page_sk#28, date_sk#29, sales_price#30, profit#31, cast(return_amt#32 as double) AS return_amt#240, cast(net_loss#33 as double) AS net_loss#241]
                     :                 :  :     :  +- Project [cs_catalog_page_sk#182 AS page_sk#28, cs_sold_date_sk#170 AS date_sk#29, cs_ext_sales_price#193 AS sales_price#30, cs_net_profit#203 AS profit#31, cast(0 as decimal(7,2)) AS return_amt#32, cast(0 as decimal(7,2)) AS net_loss#33]
                     :                 :  :     :     +- SubqueryAlias `tpcds`.`catalog_sales`
                     :                 :  :     :        +- Relation[cs_sold_date_sk#170,cs_sold_time_sk#171,cs_ship_date_sk#172,cs_bill_customer_sk#173,cs_bill_cdemo_sk#174,cs_bill_hdemo_sk#175,cs_bill_addr_sk#176,cs_ship_customer_sk#177,cs_ship_cdemo_sk#178,cs_ship_hdemo_sk#179,cs_ship_addr_sk#180,cs_call_center_sk#181,cs_catalog_page_sk#182,cs_ship_mode_sk#183,cs_warehouse_sk#184,cs_item_sk#185,cs_promo_sk#186,cs_order_number#187,cs_quantity#188,cs_wholesale_cost#189,cs_list_price#190,cs_sales_price#191,cs_ext_discount_amt#192,cs_ext_sales_price#193,... 10 more fields] parquet
                     :                 :  :     +- Project [page_sk#34, date_sk#35, cast(sales_price#36 as double) AS sales_price#242, cast(profit#37 as double) AS profit#243, return_amt#38, net_loss#39]
                     :                 :  :        +- Project [cr_catalog_page_sk#216 AS page_sk#34, cr_returned_date_sk#204 AS date_sk#35, cast(0 as decimal(7,2)) AS sales_price#36, cast(0 as decimal(7,2)) AS profit#37, cr_return_amount#222 AS return_amt#38, cr_net_loss#230 AS net_loss#39]
                     :                 :  :           +- SubqueryAlias `tpcds`.`catalog_returns`
                     :                 :  :              +- Relation[cr_returned_date_sk#204,cr_returned_time_sk#205,cr_item_sk#206,cr_refunded_customer_sk#207,cr_refunded_cdemo_sk#208,cr_refunded_hdemo_sk#209,cr_refunded_addr_sk#210,cr_returning_customer_sk#211,cr_returning_cdemo_sk#212,cr_returning_hdemo_sk#213,cr_returning_addr_sk#214,cr_call_center_sk#215,cr_catalog_page_sk#216,cr_ship_mode_sk#217,cr_warehouse_sk#218,cr_reason_sk#219,cr_order_number#220,cr_return_quantity#221,cr_return_amount#222,cr_return_tax#223,cr_return_amt_inc_tax#224,cr_fee#225,cr_return_ship_cost#226,cr_refunded_cash#227,... 3 more fields] parquet
                     :                 :  +- SubqueryAlias `tpcds`.`date_dim`
                     :                 :     +- Relation[d_date_sk#105,d_date_id#106,d_date#107,d_month_seq#108,d_week_seq#109,d_quarter_seq#110,d_year#111,d_dow#112,d_moy#113,d_dom#114,d_qoy#115,d_fy_year#116,d_fy_quarter_seq#117,d_fy_week_seq#118,d_day_name#119,d_quarter_name#120,d_holiday#121,d_weekend#122,d_following_holiday#123,d_first_dom#124,d_last_dom#125,d_same_day_ly#126,d_same_day_lq#127,d_current_day#128,... 4 more fields] parquet
                     :                 +- SubqueryAlias `tpcds`.`catalog_page`
                     :                    +- Relation[cp_catalog_page_sk#231,cp_catalog_page_id#232,cp_start_date_sk#233,cp_end_date_sk#234,cp_department#235,cp_catalog_number#236,cp_catalog_page_number#237,cp_description#238,cp_type#239] parquet
                     +- Project [web channel AS channel#6, concat(web_site, web_site_id#307) AS id#7, sales#56, returns#58, (profit#57 - profit_loss#59) AS profit#8]
                        +- SubqueryAlias `wsr`
                           +- Aggregate [web_site_id#307], [web_site_id#307, sum(sales_price#46) AS sales#56, sum(profit#47) AS profit#57, sum(return_amt#332) AS returns#58, sum(net_loss#333) AS profit_loss#59]
                              +- Filter (((date_sk#45 = d_date_sk#105) && ((d_date#107 >= cast(cast(2000-08-23 as date) as string)) && (d_date#107 <= cast(date_add(cast(2000-08-23 as date), 14) as string)))) && (wsr_web_site_sk#44 = web_site_sk#306))
                                 +- Join Inner
                                    :- Join Inner
                                    :  :- SubqueryAlias `salesreturns`
                                    :  :  +- Union
                                    :  :     :- Project [wsr_web_site_sk#44, date_sk#45, sales_price#46, profit#47, cast(return_amt#48 as double) AS return_amt#332, cast(net_loss#49 as double) AS net_loss#333]
                                    :  :     :  +- Project [ws_web_site_sk#261 AS wsr_web_site_sk#44, ws_sold_date_sk#248 AS date_sk#45, ws_ext_sales_price#271 AS sales_price#46, ws_net_profit#281 AS profit#47, cast(0 as decimal(7,2)) AS return_amt#48, cast(0 as decimal(7,2)) AS net_loss#49]
                                    :  :     :     +- SubqueryAlias `tpcds`.`web_sales`
                                    :  :     :        +- Relation[ws_sold_date_sk#248,ws_sold_time_sk#249,ws_ship_date_sk#250,ws_item_sk#251,ws_bill_customer_sk#252,ws_bill_cdemo_sk#253,ws_bill_hdemo_sk#254,ws_bill_addr_sk#255,ws_ship_customer_sk#256,ws_ship_cdemo_sk#257,ws_ship_hdemo_sk#258,ws_ship_addr_sk#259,ws_web_page_sk#260,ws_web_site_sk#261,ws_ship_mode_sk#262,ws_warehouse_sk#263,ws_promo_sk#264,ws_order_number#265,ws_quantity#266,ws_wholesale_cost#267,ws_list_price#268,ws_sales_price#269,ws_ext_discount_amt#270,ws_ext_sales_price#271,... 10 more fields] parquet
                                    :  :     +- Project [wsr_web_site_sk#50, date_sk#51, cast(sales_price#52 as double) AS sales_price#334, cast(profit#53 as double) AS profit#335, return_amt#54, net_loss#55]
                                    :  :        +- Project [ws_web_site_sk#261 AS wsr_web_site_sk#50, wr_returned_date_sk#282 AS date_sk#51, cast(0 as decimal(7,2)) AS sales_price#52, cast(0 as decimal(7,2)) AS profit#53, wr_return_amt#297 AS return_amt#54, wr_net_loss#305 AS net_loss#55]
                                    :  :           +- Join LeftOuter, ((wr_item_sk#284 = ws_item_sk#251) && (wr_order_number#295 = ws_order_number#265))
                                    :  :              :- SubqueryAlias `tpcds`.`web_returns`
                                    :  :              :  +- Relation[wr_returned_date_sk#282,wr_returned_time_sk#283,wr_item_sk#284,wr_refunded_customer_sk#285,wr_refunded_cdemo_sk#286,wr_refunded_hdemo_sk#287,wr_refunded_addr_sk#288,wr_returning_customer_sk#289,wr_returning_cdemo_sk#290,wr_returning_hdemo_sk#291,wr_returning_addr_sk#292,wr_web_page_sk#293,wr_reason_sk#294,wr_order_number#295,wr_return_quantity#296,wr_return_amt#297,wr_return_tax#298,wr_return_amt_inc_tax#299,wr_fee#300,wr_return_ship_cost#301,wr_refunded_cash#302,wr_reversed_charge#303,wr_account_credit#304,wr_net_loss#305] parquet
                                    :  :              +- SubqueryAlias `tpcds`.`web_sales`
                                    :  :                 +- Relation[ws_sold_date_sk#248,ws_sold_time_sk#249,ws_ship_date_sk#250,ws_item_sk#251,ws_bill_customer_sk#252,ws_bill_cdemo_sk#253,ws_bill_hdemo_sk#254,ws_bill_addr_sk#255,ws_ship_customer_sk#256,ws_ship_cdemo_sk#257,ws_ship_hdemo_sk#258,ws_ship_addr_sk#259,ws_web_page_sk#260,ws_web_site_sk#261,ws_ship_mode_sk#262,ws_warehouse_sk#263,ws_promo_sk#264,ws_order_number#265,ws_quantity#266,ws_wholesale_cost#267,ws_list_price#268,ws_sales_price#269,ws_ext_discount_amt#270,ws_ext_sales_price#271,... 10 more fields] parquet
                                    :  +- SubqueryAlias `tpcds`.`date_dim`
                                    :     +- Relation[d_date_sk#105,d_date_id#106,d_date#107,d_month_seq#108,d_week_seq#109,d_quarter_seq#110,d_year#111,d_dow#112,d_moy#113,d_dom#114,d_qoy#115,d_fy_year#116,d_fy_quarter_seq#117,d_fy_week_seq#118,d_day_name#119,d_quarter_name#120,d_holiday#121,d_weekend#122,d_following_holiday#123,d_first_dom#124,d_last_dom#125,d_same_day_ly#126,d_same_day_lq#127,d_current_day#128,... 4 more fields] parquet
                                    +- SubqueryAlias `tpcds`.`web_site`
                                       +- Relation[web_site_sk#306,web_site_id#307,web_rec_start_date#308,web_rec_end_date#309,web_name#310,web_open_date_sk#311,web_close_date_sk#312,web_class#313,web_manager#314,web_mkt_id#315,web_mkt_class#316,web_mkt_desc#317,web_market_manager#318,web_company_id#319,web_company_name#320,web_street_number#321,web_street_name#322,web_street_type#323,web_suite_number#324,web_city#325,web_county#326,web_state#327,web_zip#328,web_country#329,... 2 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#346 ASC NULLS FIRST, id#347 ASC NULLS FIRST], true
      +- Aggregate [channel#346, id#347, spark_grouping_id#343], [channel#346, id#347, sum(sales#24) AS sales#9, sum(returns#26) AS returns#10, sum(profit#2) AS profit#11]
         +- Expand [List(sales#24, returns#26, profit#2, channel#344, id#345, 0), List(sales#24, returns#26, profit#2, channel#344, null, 1), List(sales#24, returns#26, profit#2, null, null, 3)], [sales#24, returns#26, profit#2, channel#346, id#347, spark_grouping_id#343]
            +- Union
               :- Aggregate [s_store_id#134], [sum(sales_price#14) AS sales#24, sum(return_amt#162) AS returns#26, (sum(profit#15) - sum(net_loss#163)) AS profit#2, store channel AS channel#344, concat(store, s_store_id#134) AS id#345]
               :  +- Project [sales_price#14, profit#15, return_amt#162, net_loss#163, s_store_id#134]
               :     +- Join Inner, (store_sk#12 = s_store_sk#133)
               :        :- Project [store_sk#12, sales_price#14, profit#15, return_amt#162, net_loss#163]
               :        :  +- Join Inner, (date_sk#13 = d_date_sk#105)
               :        :     :- Union
               :        :     :  :- Project [ss_store_sk#69 AS store_sk#12, ss_sold_date_sk#62 AS date_sk#13, ss_ext_sales_price#77 AS sales_price#14, ss_net_profit#84 AS profit#15, 0.0 AS return_amt#162, 0.0 AS net_loss#163]
               :        :     :  :  +- Filter (isnotnull(ss_sold_date_sk#62) && isnotnull(ss_store_sk#69))
               :        :     :  :     +- Relation[ss_sold_date_sk#62,ss_sold_time_sk#63,ss_item_sk#64,ss_customer_sk#65,ss_cdemo_sk#66,ss_hdemo_sk#67,ss_addr_sk#68,ss_store_sk#69,ss_promo_sk#70,ss_ticket_number#71,ss_quantity#72,ss_wholesale_cost#73,ss_list_price#74,ss_sales_price#75,ss_ext_discount_amt#76,ss_ext_sales_price#77,ss_ext_wholesale_cost#78,ss_ext_list_price#79,ss_ext_tax#80,ss_coupon_amt#81,ss_net_paid#82,ss_net_paid_inc_tax#83,ss_net_profit#84] parquet
               :        :     :  +- Project [sr_store_sk#92 AS store_sk#18, sr_returned_date_sk#85 AS date_sk#19, 0.0 AS sales_price#164, 0.0 AS profit#165, sr_return_amt#96 AS return_amt#22, sr_net_loss#104 AS net_loss#23]
               :        :     :     +- Filter (isnotnull(sr_returned_date_sk#85) && isnotnull(sr_store_sk#92))
               :        :     :        +- Relation[sr_returned_date_sk#85,sr_return_time_sk#86,sr_item_sk#87,sr_customer_sk#88,sr_cdemo_sk#89,sr_hdemo_sk#90,sr_addr_sk#91,sr_store_sk#92,sr_reason_sk#93,sr_ticket_number#94,sr_return_quantity#95,sr_return_amt#96,sr_return_tax#97,sr_return_amt_inc_tax#98,sr_fee#99,sr_return_ship_cost#100,sr_refunded_cash#101,sr_reversed_charge#102,sr_store_credit#103,sr_net_loss#104] parquet
               :        :     +- Project [d_date_sk#105]
               :        :        +- Filter (((isnotnull(d_date#107) && (d_date#107 >= 2000-08-23)) && (d_date#107 <= 2000-09-06)) && isnotnull(d_date_sk#105))
               :        :           +- Relation[d_date_sk#105,d_date_id#106,d_date#107,d_month_seq#108,d_week_seq#109,d_quarter_seq#110,d_year#111,d_dow#112,d_moy#113,d_dom#114,d_qoy#115,d_fy_year#116,d_fy_quarter_seq#117,d_fy_week_seq#118,d_day_name#119,d_quarter_name#120,d_holiday#121,d_weekend#122,d_following_holiday#123,d_first_dom#124,d_last_dom#125,d_same_day_ly#126,d_same_day_lq#127,d_current_day#128,... 4 more fields] parquet
               :        +- Project [s_store_sk#133, s_store_id#134]
               :           +- Filter isnotnull(s_store_sk#133)
               :              +- Relation[s_store_sk#133,s_store_id#134,s_rec_start_date#135,s_rec_end_date#136,s_closed_date_sk#137,s_store_name#138,s_number_employees#139,s_floor_space#140,s_hours#141,s_manager#142,s_market_id#143,s_geography_class#144,s_market_desc#145,s_market_manager#146,s_division_id#147,s_division_name#148,s_company_id#149,s_company_name#150,s_street_number#151,s_street_name#152,s_street_type#153,s_suite_number#154,s_city#155,s_county#156,... 5 more fields] parquet
               :- Aggregate [cp_catalog_page_id#232], [sum(sales_price#30) AS sales#40, sum(return_amt#240) AS returns#42, (sum(profit#31) - sum(net_loss#241)) AS profit#5, catalog channel AS channel#350, concat(catalog_page, cp_catalog_page_id#232) AS id#351]
               :  +- Project [sales_price#30, profit#31, return_amt#240, net_loss#241, cp_catalog_page_id#232]
               :     +- Join Inner, (page_sk#28 = cp_catalog_page_sk#231)
               :        :- Project [page_sk#28, sales_price#30, profit#31, return_amt#240, net_loss#241]
               :        :  +- Join Inner, (date_sk#29 = d_date_sk#105)
               :        :     :- Union
               :        :     :  :- Project [cs_catalog_page_sk#182 AS page_sk#28, cs_sold_date_sk#170 AS date_sk#29, cs_ext_sales_price#193 AS sales_price#30, cs_net_profit#203 AS profit#31, 0.0 AS return_amt#240, 0.0 AS net_loss#241]
               :        :     :  :  +- Filter (isnotnull(cs_sold_date_sk#170) && isnotnull(cs_catalog_page_sk#182))
               :        :     :  :     +- Relation[cs_sold_date_sk#170,cs_sold_time_sk#171,cs_ship_date_sk#172,cs_bill_customer_sk#173,cs_bill_cdemo_sk#174,cs_bill_hdemo_sk#175,cs_bill_addr_sk#176,cs_ship_customer_sk#177,cs_ship_cdemo_sk#178,cs_ship_hdemo_sk#179,cs_ship_addr_sk#180,cs_call_center_sk#181,cs_catalog_page_sk#182,cs_ship_mode_sk#183,cs_warehouse_sk#184,cs_item_sk#185,cs_promo_sk#186,cs_order_number#187,cs_quantity#188,cs_wholesale_cost#189,cs_list_price#190,cs_sales_price#191,cs_ext_discount_amt#192,cs_ext_sales_price#193,... 10 more fields] parquet
               :        :     :  +- Project [cr_catalog_page_sk#216 AS page_sk#34, cr_returned_date_sk#204 AS date_sk#35, 0.0 AS sales_price#242, 0.0 AS profit#243, cr_return_amount#222 AS return_amt#38, cr_net_loss#230 AS net_loss#39]
               :        :     :     +- Filter (isnotnull(cr_returned_date_sk#204) && isnotnull(cr_catalog_page_sk#216))
               :        :     :        +- Relation[cr_returned_date_sk#204,cr_returned_time_sk#205,cr_item_sk#206,cr_refunded_customer_sk#207,cr_refunded_cdemo_sk#208,cr_refunded_hdemo_sk#209,cr_refunded_addr_sk#210,cr_returning_customer_sk#211,cr_returning_cdemo_sk#212,cr_returning_hdemo_sk#213,cr_returning_addr_sk#214,cr_call_center_sk#215,cr_catalog_page_sk#216,cr_ship_mode_sk#217,cr_warehouse_sk#218,cr_reason_sk#219,cr_order_number#220,cr_return_quantity#221,cr_return_amount#222,cr_return_tax#223,cr_return_amt_inc_tax#224,cr_fee#225,cr_return_ship_cost#226,cr_refunded_cash#227,... 3 more fields] parquet
               :        :     +- Project [d_date_sk#105]
               :        :        +- Filter (((isnotnull(d_date#107) && (d_date#107 >= 2000-08-23)) && (d_date#107 <= 2000-09-06)) && isnotnull(d_date_sk#105))
               :        :           +- Relation[d_date_sk#105,d_date_id#106,d_date#107,d_month_seq#108,d_week_seq#109,d_quarter_seq#110,d_year#111,d_dow#112,d_moy#113,d_dom#114,d_qoy#115,d_fy_year#116,d_fy_quarter_seq#117,d_fy_week_seq#118,d_day_name#119,d_quarter_name#120,d_holiday#121,d_weekend#122,d_following_holiday#123,d_first_dom#124,d_last_dom#125,d_same_day_ly#126,d_same_day_lq#127,d_current_day#128,... 4 more fields] parquet
               :        +- Project [cp_catalog_page_sk#231, cp_catalog_page_id#232]
               :           +- Filter isnotnull(cp_catalog_page_sk#231)
               :              +- Relation[cp_catalog_page_sk#231,cp_catalog_page_id#232,cp_start_date_sk#233,cp_end_date_sk#234,cp_department#235,cp_catalog_number#236,cp_catalog_page_number#237,cp_description#238,cp_type#239] parquet
               +- Aggregate [web_site_id#307], [sum(sales_price#46) AS sales#56, sum(return_amt#332) AS returns#58, (sum(profit#47) - sum(net_loss#333)) AS profit#8, web channel AS channel#352, concat(web_site, web_site_id#307) AS id#353]
                  +- Project [sales_price#46, profit#47, return_amt#332, net_loss#333, web_site_id#307]
                     +- Join Inner, (wsr_web_site_sk#44 = web_site_sk#306)
                        :- Project [wsr_web_site_sk#44, sales_price#46, profit#47, return_amt#332, net_loss#333]
                        :  +- Join Inner, (date_sk#45 = d_date_sk#105)
                        :     :- Union
                        :     :  :- Project [ws_web_site_sk#261 AS wsr_web_site_sk#44, ws_sold_date_sk#248 AS date_sk#45, ws_ext_sales_price#271 AS sales_price#46, ws_net_profit#281 AS profit#47, 0.0 AS return_amt#332, 0.0 AS net_loss#333]
                        :     :  :  +- Filter (isnotnull(ws_sold_date_sk#248) && isnotnull(ws_web_site_sk#261))
                        :     :  :     +- Relation[ws_sold_date_sk#248,ws_sold_time_sk#249,ws_ship_date_sk#250,ws_item_sk#251,ws_bill_customer_sk#252,ws_bill_cdemo_sk#253,ws_bill_hdemo_sk#254,ws_bill_addr_sk#255,ws_ship_customer_sk#256,ws_ship_cdemo_sk#257,ws_ship_hdemo_sk#258,ws_ship_addr_sk#259,ws_web_page_sk#260,ws_web_site_sk#261,ws_ship_mode_sk#262,ws_warehouse_sk#263,ws_promo_sk#264,ws_order_number#265,ws_quantity#266,ws_wholesale_cost#267,ws_list_price#268,ws_sales_price#269,ws_ext_discount_amt#270,ws_ext_sales_price#271,... 10 more fields] parquet
                        :     :  +- Project [ws_web_site_sk#261 AS wsr_web_site_sk#50, wr_returned_date_sk#282 AS date_sk#51, 0.0 AS sales_price#334, 0.0 AS profit#335, wr_return_amt#297 AS return_amt#54, wr_net_loss#305 AS net_loss#55]
                        :     :     +- Join Inner, ((wr_item_sk#284 = ws_item_sk#251) && (wr_order_number#295 = ws_order_number#265))
                        :     :        :- Project [wr_returned_date_sk#282, wr_item_sk#284, wr_order_number#295, wr_return_amt#297, wr_net_loss#305]
                        :     :        :  +- Filter isnotnull(wr_returned_date_sk#282)
                        :     :        :     +- Relation[wr_returned_date_sk#282,wr_returned_time_sk#283,wr_item_sk#284,wr_refunded_customer_sk#285,wr_refunded_cdemo_sk#286,wr_refunded_hdemo_sk#287,wr_refunded_addr_sk#288,wr_returning_customer_sk#289,wr_returning_cdemo_sk#290,wr_returning_hdemo_sk#291,wr_returning_addr_sk#292,wr_web_page_sk#293,wr_reason_sk#294,wr_order_number#295,wr_return_quantity#296,wr_return_amt#297,wr_return_tax#298,wr_return_amt_inc_tax#299,wr_fee#300,wr_return_ship_cost#301,wr_refunded_cash#302,wr_reversed_charge#303,wr_account_credit#304,wr_net_loss#305] parquet
                        :     :        +- Project [ws_item_sk#251, ws_web_site_sk#261, ws_order_number#265]
                        :     :           +- Filter ((isnotnull(ws_item_sk#251) && isnotnull(ws_order_number#265)) && isnotnull(ws_web_site_sk#261))
                        :     :              +- Relation[ws_sold_date_sk#248,ws_sold_time_sk#249,ws_ship_date_sk#250,ws_item_sk#251,ws_bill_customer_sk#252,ws_bill_cdemo_sk#253,ws_bill_hdemo_sk#254,ws_bill_addr_sk#255,ws_ship_customer_sk#256,ws_ship_cdemo_sk#257,ws_ship_hdemo_sk#258,ws_ship_addr_sk#259,ws_web_page_sk#260,ws_web_site_sk#261,ws_ship_mode_sk#262,ws_warehouse_sk#263,ws_promo_sk#264,ws_order_number#265,ws_quantity#266,ws_wholesale_cost#267,ws_list_price#268,ws_sales_price#269,ws_ext_discount_amt#270,ws_ext_sales_price#271,... 10 more fields] parquet
                        :     +- Project [d_date_sk#105]
                        :        +- Filter (((isnotnull(d_date#107) && (d_date#107 >= 2000-08-23)) && (d_date#107 <= 2000-09-06)) && isnotnull(d_date_sk#105))
                        :           +- Relation[d_date_sk#105,d_date_id#106,d_date#107,d_month_seq#108,d_week_seq#109,d_quarter_seq#110,d_year#111,d_dow#112,d_moy#113,d_dom#114,d_qoy#115,d_fy_year#116,d_fy_quarter_seq#117,d_fy_week_seq#118,d_day_name#119,d_quarter_name#120,d_holiday#121,d_weekend#122,d_following_holiday#123,d_first_dom#124,d_last_dom#125,d_same_day_ly#126,d_same_day_lq#127,d_current_day#128,... 4 more fields] parquet
                        +- Project [web_site_sk#306, web_site_id#307]
                           +- Filter isnotnull(web_site_sk#306)
                              +- Relation[web_site_sk#306,web_site_id#307,web_rec_start_date#308,web_rec_end_date#309,web_name#310,web_open_date_sk#311,web_close_date_sk#312,web_class#313,web_manager#314,web_mkt_id#315,web_mkt_class#316,web_mkt_desc#317,web_market_manager#318,web_company_id#319,web_company_name#320,web_street_number#321,web_street_name#322,web_street_type#323,web_suite_number#324,web_city#325,web_county#326,web_state#327,web_zip#328,web_country#329,... 2 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[channel#346 ASC NULLS FIRST,id#347 ASC NULLS FIRST], output=[channel#346,id#347,sales#9,returns#10,profit#11])
+- *(21) HashAggregate(keys=[channel#346, id#347, spark_grouping_id#343], functions=[sum(sales#24), sum(returns#26), sum(profit#2)], output=[channel#346, id#347, sales#9, returns#10, profit#11])
   +- Exchange hashpartitioning(channel#346, id#347, spark_grouping_id#343, 200)
      +- *(20) HashAggregate(keys=[channel#346, id#347, spark_grouping_id#343], functions=[partial_sum(sales#24), partial_sum(returns#26), partial_sum(profit#2)], output=[channel#346, id#347, spark_grouping_id#343, sum#357, sum#358, sum#359])
         +- *(20) Expand [List(sales#24, returns#26, profit#2, channel#344, id#345, 0), List(sales#24, returns#26, profit#2, channel#344, null, 1), List(sales#24, returns#26, profit#2, null, null, 3)], [sales#24, returns#26, profit#2, channel#346, id#347, spark_grouping_id#343]
            +- Union
               :- *(6) HashAggregate(keys=[s_store_id#134], functions=[sum(sales_price#14), sum(return_amt#162), sum(profit#15), sum(net_loss#163)], output=[sales#24, returns#26, profit#2, channel#344, id#345])
               :  +- Exchange hashpartitioning(s_store_id#134, 200)
               :     +- *(5) HashAggregate(keys=[s_store_id#134], functions=[partial_sum(sales_price#14), partial_sum(return_amt#162), partial_sum(profit#15), partial_sum(net_loss#163)], output=[s_store_id#134, sum#364, sum#365, sum#366, sum#367])
               :        +- *(5) Project [sales_price#14, profit#15, return_amt#162, net_loss#163, s_store_id#134]
               :           +- *(5) BroadcastHashJoin [store_sk#12], [s_store_sk#133], Inner, BuildRight
               :              :- *(5) Project [store_sk#12, sales_price#14, profit#15, return_amt#162, net_loss#163]
               :              :  +- *(5) BroadcastHashJoin [date_sk#13], [d_date_sk#105], Inner, BuildRight
               :              :     :- Union
               :              :     :  :- *(1) Project [ss_store_sk#69 AS store_sk#12, ss_sold_date_sk#62 AS date_sk#13, ss_ext_sales_price#77 AS sales_price#14, ss_net_profit#84 AS profit#15, 0.0 AS return_amt#162, 0.0 AS net_loss#163]
               :              :     :  :  +- *(1) Filter (isnotnull(ss_sold_date_sk#62) && isnotnull(ss_store_sk#69))
               :              :     :  :     +- *(1) FileScan parquet tpcds.store_sales[ss_sold_date_sk#62,ss_store_sk#69,ss_ext_sales_price#77,ss_net_profit#84] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_ext_sales_price:double,ss_net_profit:double>
               :              :     :  +- *(2) Project [sr_store_sk#92 AS store_sk#18, sr_returned_date_sk#85 AS date_sk#19, 0.0 AS sales_price#164, 0.0 AS profit#165, sr_return_amt#96 AS return_amt#22, sr_net_loss#104 AS net_loss#23]
               :              :     :     +- *(2) Filter (isnotnull(sr_returned_date_sk#85) && isnotnull(sr_store_sk#92))
               :              :     :        +- *(2) FileScan parquet tpcds.store_returns[sr_returned_date_sk#85,sr_store_sk#92,sr_return_amt#96,sr_net_loss#104] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_returned_date_sk), IsNotNull(sr_store_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_store_sk:int,sr_return_amt:double,sr_net_loss:double>
               :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :              :        +- *(3) Project [d_date_sk#105]
               :              :           +- *(3) Filter (((isnotnull(d_date#107) && (d_date#107 >= 2000-08-23)) && (d_date#107 <= 2000-09-06)) && isnotnull(d_date_sk#105))
               :              :              +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#105,d_date#107] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-08-23), LessThanOrEqual(d_date,2000-09-06), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
               :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                 +- *(4) Project [s_store_sk#133, s_store_id#134]
               :                    +- *(4) Filter isnotnull(s_store_sk#133)
               :                       +- *(4) FileScan parquet tpcds.store[s_store_sk#133,s_store_id#134] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_id:string>
               :- *(12) HashAggregate(keys=[cp_catalog_page_id#232], functions=[sum(sales_price#30), sum(return_amt#240), sum(profit#31), sum(net_loss#241)], output=[sales#40, returns#42, profit#5, channel#350, id#351])
               :  +- Exchange hashpartitioning(cp_catalog_page_id#232, 200)
               :     +- *(11) HashAggregate(keys=[cp_catalog_page_id#232], functions=[partial_sum(sales_price#30), partial_sum(return_amt#240), partial_sum(profit#31), partial_sum(net_loss#241)], output=[cp_catalog_page_id#232, sum#372, sum#373, sum#374, sum#375])
               :        +- *(11) Project [sales_price#30, profit#31, return_amt#240, net_loss#241, cp_catalog_page_id#232]
               :           +- *(11) BroadcastHashJoin [page_sk#28], [cp_catalog_page_sk#231], Inner, BuildRight
               :              :- *(11) Project [page_sk#28, sales_price#30, profit#31, return_amt#240, net_loss#241]
               :              :  +- *(11) BroadcastHashJoin [date_sk#29], [d_date_sk#105], Inner, BuildRight
               :              :     :- Union
               :              :     :  :- *(7) Project [cs_catalog_page_sk#182 AS page_sk#28, cs_sold_date_sk#170 AS date_sk#29, cs_ext_sales_price#193 AS sales_price#30, cs_net_profit#203 AS profit#31, 0.0 AS return_amt#240, 0.0 AS net_loss#241]
               :              :     :  :  +- *(7) Filter (isnotnull(cs_sold_date_sk#170) && isnotnull(cs_catalog_page_sk#182))
               :              :     :  :     +- *(7) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#170,cs_catalog_page_sk#182,cs_ext_sales_price#193,cs_net_profit#203] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_catalog_page_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_catalog_page_sk:int,cs_ext_sales_price:double,cs_net_profit:double>
               :              :     :  +- *(8) Project [cr_catalog_page_sk#216 AS page_sk#34, cr_returned_date_sk#204 AS date_sk#35, 0.0 AS sales_price#242, 0.0 AS profit#243, cr_return_amount#222 AS return_amt#38, cr_net_loss#230 AS net_loss#39]
               :              :     :     +- *(8) Filter (isnotnull(cr_returned_date_sk#204) && isnotnull(cr_catalog_page_sk#216))
               :              :     :        +- *(8) FileScan parquet tpcds.catalog_returns[cr_returned_date_sk#204,cr_catalog_page_sk#216,cr_return_amount#222,cr_net_loss#230] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_returned_date_sk), IsNotNull(cr_catalog_page_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_catalog_page_sk:int,cr_return_amount:double,cr_net_loss:double>
               :              :     +- ReusedExchange [d_date_sk#105], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                 +- *(10) Project [cp_catalog_page_sk#231, cp_catalog_page_id#232]
               :                    +- *(10) Filter isnotnull(cp_catalog_page_sk#231)
               :                       +- *(10) FileScan parquet tpcds.catalog_page[cp_catalog_page_sk#231,cp_catalog_page_id#232] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cp_catalog_page_sk)], ReadSchema: struct<cp_catalog_page_sk:int,cp_catalog_page_id:string>
               +- *(19) HashAggregate(keys=[web_site_id#307], functions=[sum(sales_price#46), sum(return_amt#332), sum(profit#47), sum(net_loss#333)], output=[sales#56, returns#58, profit#8, channel#352, id#353])
                  +- Exchange hashpartitioning(web_site_id#307, 200)
                     +- *(18) HashAggregate(keys=[web_site_id#307], functions=[partial_sum(sales_price#46), partial_sum(return_amt#332), partial_sum(profit#47), partial_sum(net_loss#333)], output=[web_site_id#307, sum#380, sum#381, sum#382, sum#383])
                        +- *(18) Project [sales_price#46, profit#47, return_amt#332, net_loss#333, web_site_id#307]
                           +- *(18) BroadcastHashJoin [wsr_web_site_sk#44], [web_site_sk#306], Inner, BuildRight
                              :- *(18) Project [wsr_web_site_sk#44, sales_price#46, profit#47, return_amt#332, net_loss#333]
                              :  +- *(18) BroadcastHashJoin [date_sk#45], [d_date_sk#105], Inner, BuildRight
                              :     :- Union
                              :     :  :- *(13) Project [ws_web_site_sk#261 AS wsr_web_site_sk#44, ws_sold_date_sk#248 AS date_sk#45, ws_ext_sales_price#271 AS sales_price#46, ws_net_profit#281 AS profit#47, 0.0 AS return_amt#332, 0.0 AS net_loss#333]
                              :     :  :  +- *(13) Filter (isnotnull(ws_sold_date_sk#248) && isnotnull(ws_web_site_sk#261))
                              :     :  :     +- *(13) FileScan parquet tpcds.web_sales[ws_sold_date_sk#248,ws_web_site_sk#261,ws_ext_sales_price#271,ws_net_profit#281] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_web_site_sk:int,ws_ext_sales_price:double,ws_net_profit:double>
                              :     :  +- *(15) Project [ws_web_site_sk#261 AS wsr_web_site_sk#50, wr_returned_date_sk#282 AS date_sk#51, 0.0 AS sales_price#334, 0.0 AS profit#335, wr_return_amt#297 AS return_amt#54, wr_net_loss#305 AS net_loss#55]
                              :     :     +- *(15) BroadcastHashJoin [wr_item_sk#284, wr_order_number#295], [ws_item_sk#251, ws_order_number#265], Inner, BuildLeft
                              :     :        :- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[2, int, true] as bigint) & 4294967295))))
                              :     :        :  +- *(14) Project [wr_returned_date_sk#282, wr_item_sk#284, wr_order_number#295, wr_return_amt#297, wr_net_loss#305]
                              :     :        :     +- *(14) Filter isnotnull(wr_returned_date_sk#282)
                              :     :        :        +- *(14) FileScan parquet tpcds.web_returns[wr_returned_date_sk#282,wr_item_sk#284,wr_order_number#295,wr_return_amt#297,wr_net_loss#305] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_returned_date_sk)], ReadSchema: struct<wr_returned_date_sk:int,wr_item_sk:int,wr_order_number:int,wr_return_amt:double,wr_net_los...
                              :     :        +- *(15) Project [ws_item_sk#251, ws_web_site_sk#261, ws_order_number#265]
                              :     :           +- *(15) Filter ((isnotnull(ws_item_sk#251) && isnotnull(ws_order_number#265)) && isnotnull(ws_web_site_sk#261))
                              :     :              +- *(15) FileScan parquet tpcds.web_sales[ws_item_sk#251,ws_web_site_sk#261,ws_order_number#265] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_order_number), IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_item_sk:int,ws_web_site_sk:int,ws_order_number:int>
                              :     +- ReusedExchange [d_date_sk#105], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 +- *(17) Project [web_site_sk#306, web_site_id#307]
                                    +- *(17) Filter isnotnull(web_site_sk#306)
                                       +- *(17) FileScan parquet tpcds.web_site[web_site_sk#306,web_site_id#307] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_site_id:string>
Time taken: 5.405 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 52)

== SQL ==

-- end query 5 in stream 0 using template query5.tpl
----------------------------------------------------^^^

