== Parsed Logical Plan ==
CTE [ssr, csr, wsr]
:  :- 'SubqueryAlias `ssr`
:  :  +- 'Aggregate ['s_store_id], ['s_store_id AS store_id#9, 'sum('ss_ext_sales_price) AS sales#10, 'sum('coalesce('sr_return_amt, 0)) AS returns#11, 'sum(('ss_net_profit - 'coalesce('sr_net_loss, 0))) AS profit#12]
:  :     +- 'Filter (((('ss_sold_date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) && (('ss_store_sk = 's_store_sk) && ('ss_item_sk = 'i_item_sk))) && ((('i_current_price > 50) && ('ss_promo_sk = 'p_promo_sk)) && ('p_channel_tv = N)))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'Join Inner
:  :           :  :  :  :- 'Join LeftOuter, (('ss_item_sk = 'sr_item_sk) && ('ss_ticket_number = 'sr_ticket_number))
:  :           :  :  :  :  :- 'UnresolvedRelation `store_sales`
:  :           :  :  :  :  +- 'UnresolvedRelation `store_returns`
:  :           :  :  :  +- 'UnresolvedRelation `date_dim`
:  :           :  :  +- 'UnresolvedRelation `store`
:  :           :  +- 'UnresolvedRelation `item`
:  :           +- 'UnresolvedRelation `promotion`
:  :- 'SubqueryAlias `csr`
:  :  +- 'Aggregate ['cp_catalog_page_id], ['cp_catalog_page_id AS catalog_page_id#13, 'sum('cs_ext_sales_price) AS sales#14, 'sum('coalesce('cr_return_amount, 0)) AS returns#15, 'sum(('cs_net_profit - 'coalesce('cr_net_loss, 0))) AS profit#16]
:  :     +- 'Filter (((('cs_sold_date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) && (('cs_catalog_page_sk = 'cp_catalog_page_sk) && ('cs_item_sk = 'i_item_sk))) && ((('i_current_price > 50) && ('cs_promo_sk = 'p_promo_sk)) && ('p_channel_tv = N)))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'Join Inner
:  :           :  :  :  :- 'Join LeftOuter, (('cs_item_sk = 'cr_item_sk) && ('cs_order_number = 'cr_order_number))
:  :           :  :  :  :  :- 'UnresolvedRelation `catalog_sales`
:  :           :  :  :  :  +- 'UnresolvedRelation `catalog_returns`
:  :           :  :  :  +- 'UnresolvedRelation `date_dim`
:  :           :  :  +- 'UnresolvedRelation `catalog_page`
:  :           :  +- 'UnresolvedRelation `item`
:  :           +- 'UnresolvedRelation `promotion`
:  +- 'SubqueryAlias `wsr`
:     +- 'Aggregate ['web_site_id], ['web_site_id, 'sum('ws_ext_sales_price) AS sales#17, 'sum('coalesce('wr_return_amt, 0)) AS returns#18, 'sum(('ws_net_profit - 'coalesce('wr_net_loss, 0))) AS profit#19]
:        +- 'Filter (((('ws_sold_date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) && (('ws_web_site_sk = 'web_site_sk) && ('ws_item_sk = 'i_item_sk))) && ((('i_current_price > 50) && ('ws_promo_sk = 'p_promo_sk)) && ('p_channel_tv = N)))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'Join Inner
:              :  :  :- 'Join Inner
:              :  :  :  :- 'Join LeftOuter, (('ws_item_sk = 'wr_item_sk) && ('ws_order_number = 'wr_order_number))
:              :  :  :  :  :- 'UnresolvedRelation `web_sales`
:              :  :  :  :  +- 'UnresolvedRelation `web_returns`
:              :  :  :  +- 'UnresolvedRelation `date_dim`
:              :  :  +- 'UnresolvedRelation `web_site`
:              :  +- 'UnresolvedRelation `item`
:              +- 'UnresolvedRelation `promotion`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['channel ASC NULLS FIRST, 'id ASC NULLS FIRST], true
         +- 'Aggregate ['rollup('channel, 'id)], ['channel, 'id, 'sum('sales) AS sales#6, 'sum('returns) AS returns#7, 'sum('profit) AS profit#8]
            +- 'SubqueryAlias `x`
               +- 'Union
                  :- 'Union
                  :  :- 'Project [store channel AS channel#0, 'concat(store, 'store_id) AS id#1, 'sales, 'returns, 'profit]
                  :  :  +- 'UnresolvedRelation `ssr`
                  :  +- 'Project [catalog channel AS channel#2, 'concat(catalog_page, 'catalog_page_id) AS id#3, 'sales, 'returns, 'profit]
                  :     +- 'UnresolvedRelation `csr`
                  +- 'Project [web channel AS channel#4, 'concat(web_site, 'web_site_id) AS id#5, 'sales, 'returns, 'profit]
                     +- 'UnresolvedRelation `wsr`

== Analyzed Logical Plan ==
channel: string, id: string, sales: double, returns: double, profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#332 ASC NULLS FIRST, id#333 ASC NULLS FIRST], true
      +- Aggregate [channel#332, id#333, spark_grouping_id#329], [channel#332, id#333, sum(sales#10) AS sales#6, sum(returns#11) AS returns#7, sum(profit#12) AS profit#8]
         +- Expand [List(channel#0, id#1, sales#10, returns#11, profit#12, channel#330, id#331, 0), List(channel#0, id#1, sales#10, returns#11, profit#12, channel#330, null, 1), List(channel#0, id#1, sales#10, returns#11, profit#12, null, null, 3)], [channel#0, id#1, sales#10, returns#11, profit#12, channel#332, id#333, spark_grouping_id#329]
            +- Project [channel#0, id#1, sales#10, returns#11, profit#12, channel#0 AS channel#330, id#1 AS id#331]
               +- SubqueryAlias `x`
                  +- Union
                     :- Union
                     :  :- Project [store channel AS channel#0, concat(store, store_id#9) AS id#1, sales#10, returns#11, profit#12]
                     :  :  +- SubqueryAlias `ssr`
                     :  :     +- Aggregate [s_store_id#94], [s_store_id#94 AS store_id#9, sum(ss_ext_sales_price#37) AS sales#10, sum(coalesce(sr_return_amt#56, cast(0 as double))) AS returns#11, sum((ss_net_profit#44 - coalesce(sr_net_loss#64, cast(0 as double)))) AS profit#12]
                     :  :        +- Filter ((((ss_sold_date_sk#22 = d_date_sk#65) && ((d_date#67 >= cast(cast(2000-08-23 as date) as string)) && (d_date#67 <= cast(date_add(cast(2000-08-23 as date), 30) as string)))) && ((ss_store_sk#29 = s_store_sk#93) && (ss_item_sk#24 = i_item_sk#122))) && (((i_current_price#127 > cast(50 as double)) && (ss_promo_sk#30 = p_promo_sk#144)) && (p_channel_tv#155 = N)))
                     :  :           +- Join Inner
                     :  :              :- Join Inner
                     :  :              :  :- Join Inner
                     :  :              :  :  :- Join Inner
                     :  :              :  :  :  :- Join LeftOuter, ((ss_item_sk#24 = sr_item_sk#47) && (ss_ticket_number#31 = sr_ticket_number#54))
                     :  :              :  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
                     :  :              :  :  :  :  :  +- Relation[ss_sold_date_sk#22,ss_sold_time_sk#23,ss_item_sk#24,ss_customer_sk#25,ss_cdemo_sk#26,ss_hdemo_sk#27,ss_addr_sk#28,ss_store_sk#29,ss_promo_sk#30,ss_ticket_number#31,ss_quantity#32,ss_wholesale_cost#33,ss_list_price#34,ss_sales_price#35,ss_ext_discount_amt#36,ss_ext_sales_price#37,ss_ext_wholesale_cost#38,ss_ext_list_price#39,ss_ext_tax#40,ss_coupon_amt#41,ss_net_paid#42,ss_net_paid_inc_tax#43,ss_net_profit#44] parquet
                     :  :              :  :  :  :  +- SubqueryAlias `tpcds`.`store_returns`
                     :  :              :  :  :  :     +- Relation[sr_returned_date_sk#45,sr_return_time_sk#46,sr_item_sk#47,sr_customer_sk#48,sr_cdemo_sk#49,sr_hdemo_sk#50,sr_addr_sk#51,sr_store_sk#52,sr_reason_sk#53,sr_ticket_number#54,sr_return_quantity#55,sr_return_amt#56,sr_return_tax#57,sr_return_amt_inc_tax#58,sr_fee#59,sr_return_ship_cost#60,sr_refunded_cash#61,sr_reversed_charge#62,sr_store_credit#63,sr_net_loss#64] parquet
                     :  :              :  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                     :  :              :  :  :     +- Relation[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
                     :  :              :  :  +- SubqueryAlias `tpcds`.`store`
                     :  :              :  :     +- Relation[s_store_sk#93,s_store_id#94,s_rec_start_date#95,s_rec_end_date#96,s_closed_date_sk#97,s_store_name#98,s_number_employees#99,s_floor_space#100,s_hours#101,s_manager#102,s_market_id#103,s_geography_class#104,s_market_desc#105,s_market_manager#106,s_division_id#107,s_division_name#108,s_company_id#109,s_company_name#110,s_street_number#111,s_street_name#112,s_street_type#113,s_suite_number#114,s_city#115,s_county#116,... 5 more fields] parquet
                     :  :              :  +- SubqueryAlias `tpcds`.`item`
                     :  :              :     +- Relation[i_item_sk#122,i_item_id#123,i_rec_start_date#124,i_rec_end_date#125,i_item_desc#126,i_current_price#127,i_wholesale_cost#128,i_brand_id#129,i_brand#130,i_class_id#131,i_class#132,i_category_id#133,i_category#134,i_manufact_id#135,i_manufact#136,i_size#137,i_formulation#138,i_color#139,i_units#140,i_container#141,i_manager_id#142,i_product_name#143] parquet
                     :  :              +- SubqueryAlias `tpcds`.`promotion`
                     :  :                 +- Relation[p_promo_sk#144,p_promo_id#145,p_start_date_sk#146,p_end_date_sk#147,p_item_sk#148,p_cost#149,p_response_target#150,p_promo_name#151,p_channel_dmail#152,p_channel_email#153,p_channel_catalog#154,p_channel_tv#155,p_channel_radio#156,p_channel_press#157,p_channel_event#158,p_channel_demo#159,p_channel_details#160,p_purpose#161,p_discount_active#162] parquet
                     :  +- Project [catalog channel AS channel#2, concat(catalog_page, catalog_page_id#13) AS id#3, sales#14, returns#15, profit#16]
                     :     +- SubqueryAlias `csr`
                     :        +- Aggregate [cp_catalog_page_id#228], [cp_catalog_page_id#228 AS catalog_page_id#13, sum(cs_ext_sales_price#189) AS sales#14, sum(coalesce(cr_return_amount#218, cast(0 as double))) AS returns#15, sum((cs_net_profit#199 - coalesce(cr_net_loss#226, cast(0 as double)))) AS profit#16]
                     :           +- Filter ((((cs_sold_date_sk#166 = d_date_sk#65) && ((d_date#67 >= cast(cast(2000-08-23 as date) as string)) && (d_date#67 <= cast(date_add(cast(2000-08-23 as date), 30) as string)))) && ((cs_catalog_page_sk#178 = cp_catalog_page_sk#227) && (cs_item_sk#181 = i_item_sk#122))) && (((i_current_price#127 > cast(50 as double)) && (cs_promo_sk#182 = p_promo_sk#144)) && (p_channel_tv#155 = N)))
                     :              +- Join Inner
                     :                 :- Join Inner
                     :                 :  :- Join Inner
                     :                 :  :  :- Join Inner
                     :                 :  :  :  :- Join LeftOuter, ((cs_item_sk#181 = cr_item_sk#202) && (cs_order_number#183 = cr_order_number#216))
                     :                 :  :  :  :  :- SubqueryAlias `tpcds`.`catalog_sales`
                     :                 :  :  :  :  :  +- Relation[cs_sold_date_sk#166,cs_sold_time_sk#167,cs_ship_date_sk#168,cs_bill_customer_sk#169,cs_bill_cdemo_sk#170,cs_bill_hdemo_sk#171,cs_bill_addr_sk#172,cs_ship_customer_sk#173,cs_ship_cdemo_sk#174,cs_ship_hdemo_sk#175,cs_ship_addr_sk#176,cs_call_center_sk#177,cs_catalog_page_sk#178,cs_ship_mode_sk#179,cs_warehouse_sk#180,cs_item_sk#181,cs_promo_sk#182,cs_order_number#183,cs_quantity#184,cs_wholesale_cost#185,cs_list_price#186,cs_sales_price#187,cs_ext_discount_amt#188,cs_ext_sales_price#189,... 10 more fields] parquet
                     :                 :  :  :  :  +- SubqueryAlias `tpcds`.`catalog_returns`
                     :                 :  :  :  :     +- Relation[cr_returned_date_sk#200,cr_returned_time_sk#201,cr_item_sk#202,cr_refunded_customer_sk#203,cr_refunded_cdemo_sk#204,cr_refunded_hdemo_sk#205,cr_refunded_addr_sk#206,cr_returning_customer_sk#207,cr_returning_cdemo_sk#208,cr_returning_hdemo_sk#209,cr_returning_addr_sk#210,cr_call_center_sk#211,cr_catalog_page_sk#212,cr_ship_mode_sk#213,cr_warehouse_sk#214,cr_reason_sk#215,cr_order_number#216,cr_return_quantity#217,cr_return_amount#218,cr_return_tax#219,cr_return_amt_inc_tax#220,cr_fee#221,cr_return_ship_cost#222,cr_refunded_cash#223,... 3 more fields] parquet
                     :                 :  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                     :                 :  :  :     +- Relation[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
                     :                 :  :  +- SubqueryAlias `tpcds`.`catalog_page`
                     :                 :  :     +- Relation[cp_catalog_page_sk#227,cp_catalog_page_id#228,cp_start_date_sk#229,cp_end_date_sk#230,cp_department#231,cp_catalog_number#232,cp_catalog_page_number#233,cp_description#234,cp_type#235] parquet
                     :                 :  +- SubqueryAlias `tpcds`.`item`
                     :                 :     +- Relation[i_item_sk#122,i_item_id#123,i_rec_start_date#124,i_rec_end_date#125,i_item_desc#126,i_current_price#127,i_wholesale_cost#128,i_brand_id#129,i_brand#130,i_class_id#131,i_class#132,i_category_id#133,i_category#134,i_manufact_id#135,i_manufact#136,i_size#137,i_formulation#138,i_color#139,i_units#140,i_container#141,i_manager_id#142,i_product_name#143] parquet
                     :                 +- SubqueryAlias `tpcds`.`promotion`
                     :                    +- Relation[p_promo_sk#144,p_promo_id#145,p_start_date_sk#146,p_end_date_sk#147,p_item_sk#148,p_cost#149,p_response_target#150,p_promo_name#151,p_channel_dmail#152,p_channel_email#153,p_channel_catalog#154,p_channel_tv#155,p_channel_radio#156,p_channel_press#157,p_channel_event#158,p_channel_demo#159,p_channel_details#160,p_purpose#161,p_discount_active#162] parquet
                     +- Project [web channel AS channel#4, concat(web_site, web_site_id#298) AS id#5, sales#17, returns#18, profit#19]
                        +- SubqueryAlias `wsr`
                           +- Aggregate [web_site_id#298], [web_site_id#298, sum(ws_ext_sales_price#262) AS sales#17, sum(coalesce(wr_return_amt#288, cast(0 as double))) AS returns#18, sum((ws_net_profit#272 - coalesce(wr_net_loss#296, cast(0 as double)))) AS profit#19]
                              +- Filter ((((ws_sold_date_sk#239 = d_date_sk#65) && ((d_date#67 >= cast(cast(2000-08-23 as date) as string)) && (d_date#67 <= cast(date_add(cast(2000-08-23 as date), 30) as string)))) && ((ws_web_site_sk#252 = web_site_sk#297) && (ws_item_sk#242 = i_item_sk#122))) && (((i_current_price#127 > cast(50 as double)) && (ws_promo_sk#255 = p_promo_sk#144)) && (p_channel_tv#155 = N)))
                                 +- Join Inner
                                    :- Join Inner
                                    :  :- Join Inner
                                    :  :  :- Join Inner
                                    :  :  :  :- Join LeftOuter, ((ws_item_sk#242 = wr_item_sk#275) && (ws_order_number#256 = wr_order_number#286))
                                    :  :  :  :  :- SubqueryAlias `tpcds`.`web_sales`
                                    :  :  :  :  :  +- Relation[ws_sold_date_sk#239,ws_sold_time_sk#240,ws_ship_date_sk#241,ws_item_sk#242,ws_bill_customer_sk#243,ws_bill_cdemo_sk#244,ws_bill_hdemo_sk#245,ws_bill_addr_sk#246,ws_ship_customer_sk#247,ws_ship_cdemo_sk#248,ws_ship_hdemo_sk#249,ws_ship_addr_sk#250,ws_web_page_sk#251,ws_web_site_sk#252,ws_ship_mode_sk#253,ws_warehouse_sk#254,ws_promo_sk#255,ws_order_number#256,ws_quantity#257,ws_wholesale_cost#258,ws_list_price#259,ws_sales_price#260,ws_ext_discount_amt#261,ws_ext_sales_price#262,... 10 more fields] parquet
                                    :  :  :  :  +- SubqueryAlias `tpcds`.`web_returns`
                                    :  :  :  :     +- Relation[wr_returned_date_sk#273,wr_returned_time_sk#274,wr_item_sk#275,wr_refunded_customer_sk#276,wr_refunded_cdemo_sk#277,wr_refunded_hdemo_sk#278,wr_refunded_addr_sk#279,wr_returning_customer_sk#280,wr_returning_cdemo_sk#281,wr_returning_hdemo_sk#282,wr_returning_addr_sk#283,wr_web_page_sk#284,wr_reason_sk#285,wr_order_number#286,wr_return_quantity#287,wr_return_amt#288,wr_return_tax#289,wr_return_amt_inc_tax#290,wr_fee#291,wr_return_ship_cost#292,wr_refunded_cash#293,wr_reversed_charge#294,wr_account_credit#295,wr_net_loss#296] parquet
                                    :  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                                    :  :  :     +- Relation[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
                                    :  :  +- SubqueryAlias `tpcds`.`web_site`
                                    :  :     +- Relation[web_site_sk#297,web_site_id#298,web_rec_start_date#299,web_rec_end_date#300,web_name#301,web_open_date_sk#302,web_close_date_sk#303,web_class#304,web_manager#305,web_mkt_id#306,web_mkt_class#307,web_mkt_desc#308,web_market_manager#309,web_company_id#310,web_company_name#311,web_street_number#312,web_street_name#313,web_street_type#314,web_suite_number#315,web_city#316,web_county#317,web_state#318,web_zip#319,web_country#320,... 2 more fields] parquet
                                    :  +- SubqueryAlias `tpcds`.`item`
                                    :     +- Relation[i_item_sk#122,i_item_id#123,i_rec_start_date#124,i_rec_end_date#125,i_item_desc#126,i_current_price#127,i_wholesale_cost#128,i_brand_id#129,i_brand#130,i_class_id#131,i_class#132,i_category_id#133,i_category#134,i_manufact_id#135,i_manufact#136,i_size#137,i_formulation#138,i_color#139,i_units#140,i_container#141,i_manager_id#142,i_product_name#143] parquet
                                    +- SubqueryAlias `tpcds`.`promotion`
                                       +- Relation[p_promo_sk#144,p_promo_id#145,p_start_date_sk#146,p_end_date_sk#147,p_item_sk#148,p_cost#149,p_response_target#150,p_promo_name#151,p_channel_dmail#152,p_channel_email#153,p_channel_catalog#154,p_channel_tv#155,p_channel_radio#156,p_channel_press#157,p_channel_event#158,p_channel_demo#159,p_channel_details#160,p_purpose#161,p_discount_active#162] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#332 ASC NULLS FIRST, id#333 ASC NULLS FIRST], true
      +- Aggregate [channel#332, id#333, spark_grouping_id#329], [channel#332, id#333, sum(sales#10) AS sales#6, sum(returns#11) AS returns#7, sum(profit#12) AS profit#8]
         +- Expand [List(sales#10, returns#11, profit#12, channel#330, id#331, 0), List(sales#10, returns#11, profit#12, channel#330, null, 1), List(sales#10, returns#11, profit#12, null, null, 3)], [sales#10, returns#11, profit#12, channel#332, id#333, spark_grouping_id#329]
            +- Union
               :- Aggregate [s_store_id#94], [sum(ss_ext_sales_price#37) AS sales#10, sum(coalesce(sr_return_amt#56, 0.0)) AS returns#11, sum((ss_net_profit#44 - coalesce(sr_net_loss#64, 0.0))) AS profit#12, store channel AS channel#330, concat(store, s_store_id#94) AS id#331]
               :  +- Project [ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64, s_store_id#94]
               :     +- Join Inner, (ss_promo_sk#30 = p_promo_sk#144)
               :        :- Project [ss_promo_sk#30, ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64, s_store_id#94]
               :        :  +- Join Inner, (ss_item_sk#24 = i_item_sk#122)
               :        :     :- Project [ss_item_sk#24, ss_promo_sk#30, ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64, s_store_id#94]
               :        :     :  +- Join Inner, (ss_store_sk#29 = s_store_sk#93)
               :        :     :     :- Project [ss_item_sk#24, ss_store_sk#29, ss_promo_sk#30, ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64]
               :        :     :     :  +- Join Inner, (ss_sold_date_sk#22 = d_date_sk#65)
               :        :     :     :     :- Project [ss_sold_date_sk#22, ss_item_sk#24, ss_store_sk#29, ss_promo_sk#30, ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64]
               :        :     :     :     :  +- Join LeftOuter, ((ss_item_sk#24 = sr_item_sk#47) && (ss_ticket_number#31 = sr_ticket_number#54))
               :        :     :     :     :     :- Project [ss_sold_date_sk#22, ss_item_sk#24, ss_store_sk#29, ss_promo_sk#30, ss_ticket_number#31, ss_ext_sales_price#37, ss_net_profit#44]
               :        :     :     :     :     :  +- Filter (((isnotnull(ss_sold_date_sk#22) && isnotnull(ss_store_sk#29)) && isnotnull(ss_item_sk#24)) && isnotnull(ss_promo_sk#30))
               :        :     :     :     :     :     +- Relation[ss_sold_date_sk#22,ss_sold_time_sk#23,ss_item_sk#24,ss_customer_sk#25,ss_cdemo_sk#26,ss_hdemo_sk#27,ss_addr_sk#28,ss_store_sk#29,ss_promo_sk#30,ss_ticket_number#31,ss_quantity#32,ss_wholesale_cost#33,ss_list_price#34,ss_sales_price#35,ss_ext_discount_amt#36,ss_ext_sales_price#37,ss_ext_wholesale_cost#38,ss_ext_list_price#39,ss_ext_tax#40,ss_coupon_amt#41,ss_net_paid#42,ss_net_paid_inc_tax#43,ss_net_profit#44] parquet
               :        :     :     :     :     +- Project [sr_item_sk#47, sr_ticket_number#54, sr_return_amt#56, sr_net_loss#64]
               :        :     :     :     :        +- Filter (isnotnull(sr_item_sk#47) && isnotnull(sr_ticket_number#54))
               :        :     :     :     :           +- Relation[sr_returned_date_sk#45,sr_return_time_sk#46,sr_item_sk#47,sr_customer_sk#48,sr_cdemo_sk#49,sr_hdemo_sk#50,sr_addr_sk#51,sr_store_sk#52,sr_reason_sk#53,sr_ticket_number#54,sr_return_quantity#55,sr_return_amt#56,sr_return_tax#57,sr_return_amt_inc_tax#58,sr_fee#59,sr_return_ship_cost#60,sr_refunded_cash#61,sr_reversed_charge#62,sr_store_credit#63,sr_net_loss#64] parquet
               :        :     :     :     +- Project [d_date_sk#65]
               :        :     :     :        +- Filter (((isnotnull(d_date#67) && (d_date#67 >= 2000-08-23)) && (d_date#67 <= 2000-09-22)) && isnotnull(d_date_sk#65))
               :        :     :     :           +- Relation[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
               :        :     :     +- Project [s_store_sk#93, s_store_id#94]
               :        :     :        +- Filter isnotnull(s_store_sk#93)
               :        :     :           +- Relation[s_store_sk#93,s_store_id#94,s_rec_start_date#95,s_rec_end_date#96,s_closed_date_sk#97,s_store_name#98,s_number_employees#99,s_floor_space#100,s_hours#101,s_manager#102,s_market_id#103,s_geography_class#104,s_market_desc#105,s_market_manager#106,s_division_id#107,s_division_name#108,s_company_id#109,s_company_name#110,s_street_number#111,s_street_name#112,s_street_type#113,s_suite_number#114,s_city#115,s_county#116,... 5 more fields] parquet
               :        :     +- Project [i_item_sk#122]
               :        :        +- Filter ((isnotnull(i_current_price#127) && (i_current_price#127 > 50.0)) && isnotnull(i_item_sk#122))
               :        :           +- Relation[i_item_sk#122,i_item_id#123,i_rec_start_date#124,i_rec_end_date#125,i_item_desc#126,i_current_price#127,i_wholesale_cost#128,i_brand_id#129,i_brand#130,i_class_id#131,i_class#132,i_category_id#133,i_category#134,i_manufact_id#135,i_manufact#136,i_size#137,i_formulation#138,i_color#139,i_units#140,i_container#141,i_manager_id#142,i_product_name#143] parquet
               :        +- Project [p_promo_sk#144]
               :           +- Filter ((isnotnull(p_channel_tv#155) && (p_channel_tv#155 = N)) && isnotnull(p_promo_sk#144))
               :              +- Relation[p_promo_sk#144,p_promo_id#145,p_start_date_sk#146,p_end_date_sk#147,p_item_sk#148,p_cost#149,p_response_target#150,p_promo_name#151,p_channel_dmail#152,p_channel_email#153,p_channel_catalog#154,p_channel_tv#155,p_channel_radio#156,p_channel_press#157,p_channel_event#158,p_channel_demo#159,p_channel_details#160,p_purpose#161,p_discount_active#162] parquet
               :- Aggregate [cp_catalog_page_id#228], [sum(cs_ext_sales_price#189) AS sales#14, sum(coalesce(cr_return_amount#218, 0.0)) AS returns#15, sum((cs_net_profit#199 - coalesce(cr_net_loss#226, 0.0))) AS profit#16, catalog channel AS channel#336, concat(catalog_page, cp_catalog_page_id#228) AS id#337]
               :  +- Project [cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
               :     +- Join Inner, (cs_promo_sk#182 = p_promo_sk#144)
               :        :- Project [cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
               :        :  +- Join Inner, (cs_item_sk#181 = i_item_sk#122)
               :        :     :- Project [cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
               :        :     :  +- Join Inner, (cs_catalog_page_sk#178 = cp_catalog_page_sk#227)
               :        :     :     :- Project [cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226]
               :        :     :     :  +- Join Inner, (cs_sold_date_sk#166 = d_date_sk#65)
               :        :     :     :     :- Project [cs_sold_date_sk#166, cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226]
               :        :     :     :     :  +- Join LeftOuter, ((cs_item_sk#181 = cr_item_sk#202) && (cs_order_number#183 = cr_order_number#216))
               :        :     :     :     :     :- Project [cs_sold_date_sk#166, cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_order_number#183, cs_ext_sales_price#189, cs_net_profit#199]
               :        :     :     :     :     :  +- Filter (((isnotnull(cs_sold_date_sk#166) && isnotnull(cs_catalog_page_sk#178)) && isnotnull(cs_item_sk#181)) && isnotnull(cs_promo_sk#182))
               :        :     :     :     :     :     +- Relation[cs_sold_date_sk#166,cs_sold_time_sk#167,cs_ship_date_sk#168,cs_bill_customer_sk#169,cs_bill_cdemo_sk#170,cs_bill_hdemo_sk#171,cs_bill_addr_sk#172,cs_ship_customer_sk#173,cs_ship_cdemo_sk#174,cs_ship_hdemo_sk#175,cs_ship_addr_sk#176,cs_call_center_sk#177,cs_catalog_page_sk#178,cs_ship_mode_sk#179,cs_warehouse_sk#180,cs_item_sk#181,cs_promo_sk#182,cs_order_number#183,cs_quantity#184,cs_wholesale_cost#185,cs_list_price#186,cs_sales_price#187,cs_ext_discount_amt#188,cs_ext_sales_price#189,... 10 more fields] parquet
               :        :     :     :     :     +- Project [cr_item_sk#202, cr_order_number#216, cr_return_amount#218, cr_net_loss#226]
               :        :     :     :     :        +- Filter (isnotnull(cr_item_sk#202) && isnotnull(cr_order_number#216))
               :        :     :     :     :           +- Relation[cr_returned_date_sk#200,cr_returned_time_sk#201,cr_item_sk#202,cr_refunded_customer_sk#203,cr_refunded_cdemo_sk#204,cr_refunded_hdemo_sk#205,cr_refunded_addr_sk#206,cr_returning_customer_sk#207,cr_returning_cdemo_sk#208,cr_returning_hdemo_sk#209,cr_returning_addr_sk#210,cr_call_center_sk#211,cr_catalog_page_sk#212,cr_ship_mode_sk#213,cr_warehouse_sk#214,cr_reason_sk#215,cr_order_number#216,cr_return_quantity#217,cr_return_amount#218,cr_return_tax#219,cr_return_amt_inc_tax#220,cr_fee#221,cr_return_ship_cost#222,cr_refunded_cash#223,... 3 more fields] parquet
               :        :     :     :     +- Project [d_date_sk#65]
               :        :     :     :        +- Filter (((isnotnull(d_date#67) && (d_date#67 >= 2000-08-23)) && (d_date#67 <= 2000-09-22)) && isnotnull(d_date_sk#65))
               :        :     :     :           +- Relation[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
               :        :     :     +- Project [cp_catalog_page_sk#227, cp_catalog_page_id#228]
               :        :     :        +- Filter isnotnull(cp_catalog_page_sk#227)
               :        :     :           +- Relation[cp_catalog_page_sk#227,cp_catalog_page_id#228,cp_start_date_sk#229,cp_end_date_sk#230,cp_department#231,cp_catalog_number#232,cp_catalog_page_number#233,cp_description#234,cp_type#235] parquet
               :        :     +- Project [i_item_sk#122]
               :        :        +- Filter ((isnotnull(i_current_price#127) && (i_current_price#127 > 50.0)) && isnotnull(i_item_sk#122))
               :        :           +- Relation[i_item_sk#122,i_item_id#123,i_rec_start_date#124,i_rec_end_date#125,i_item_desc#126,i_current_price#127,i_wholesale_cost#128,i_brand_id#129,i_brand#130,i_class_id#131,i_class#132,i_category_id#133,i_category#134,i_manufact_id#135,i_manufact#136,i_size#137,i_formulation#138,i_color#139,i_units#140,i_container#141,i_manager_id#142,i_product_name#143] parquet
               :        +- Project [p_promo_sk#144]
               :           +- Filter ((isnotnull(p_channel_tv#155) && (p_channel_tv#155 = N)) && isnotnull(p_promo_sk#144))
               :              +- Relation[p_promo_sk#144,p_promo_id#145,p_start_date_sk#146,p_end_date_sk#147,p_item_sk#148,p_cost#149,p_response_target#150,p_promo_name#151,p_channel_dmail#152,p_channel_email#153,p_channel_catalog#154,p_channel_tv#155,p_channel_radio#156,p_channel_press#157,p_channel_event#158,p_channel_demo#159,p_channel_details#160,p_purpose#161,p_discount_active#162] parquet
               +- Aggregate [web_site_id#298], [sum(ws_ext_sales_price#262) AS sales#17, sum(coalesce(wr_return_amt#288, 0.0)) AS returns#18, sum((ws_net_profit#272 - coalesce(wr_net_loss#296, 0.0))) AS profit#19, web channel AS channel#338, concat(web_site, web_site_id#298) AS id#339]
                  +- Project [ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296, web_site_id#298]
                     +- Join Inner, (ws_promo_sk#255 = p_promo_sk#144)
                        :- Project [ws_promo_sk#255, ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296, web_site_id#298]
                        :  +- Join Inner, (ws_item_sk#242 = i_item_sk#122)
                        :     :- Project [ws_item_sk#242, ws_promo_sk#255, ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296, web_site_id#298]
                        :     :  +- Join Inner, (ws_web_site_sk#252 = web_site_sk#297)
                        :     :     :- Project [ws_item_sk#242, ws_web_site_sk#252, ws_promo_sk#255, ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296]
                        :     :     :  +- Join Inner, (ws_sold_date_sk#239 = d_date_sk#65)
                        :     :     :     :- Project [ws_sold_date_sk#239, ws_item_sk#242, ws_web_site_sk#252, ws_promo_sk#255, ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296]
                        :     :     :     :  +- Join LeftOuter, ((ws_item_sk#242 = wr_item_sk#275) && (ws_order_number#256 = wr_order_number#286))
                        :     :     :     :     :- Project [ws_sold_date_sk#239, ws_item_sk#242, ws_web_site_sk#252, ws_promo_sk#255, ws_order_number#256, ws_ext_sales_price#262, ws_net_profit#272]
                        :     :     :     :     :  +- Filter (((isnotnull(ws_sold_date_sk#239) && isnotnull(ws_web_site_sk#252)) && isnotnull(ws_item_sk#242)) && isnotnull(ws_promo_sk#255))
                        :     :     :     :     :     +- Relation[ws_sold_date_sk#239,ws_sold_time_sk#240,ws_ship_date_sk#241,ws_item_sk#242,ws_bill_customer_sk#243,ws_bill_cdemo_sk#244,ws_bill_hdemo_sk#245,ws_bill_addr_sk#246,ws_ship_customer_sk#247,ws_ship_cdemo_sk#248,ws_ship_hdemo_sk#249,ws_ship_addr_sk#250,ws_web_page_sk#251,ws_web_site_sk#252,ws_ship_mode_sk#253,ws_warehouse_sk#254,ws_promo_sk#255,ws_order_number#256,ws_quantity#257,ws_wholesale_cost#258,ws_list_price#259,ws_sales_price#260,ws_ext_discount_amt#261,ws_ext_sales_price#262,... 10 more fields] parquet
                        :     :     :     :     +- Project [wr_item_sk#275, wr_order_number#286, wr_return_amt#288, wr_net_loss#296]
                        :     :     :     :        +- Filter (isnotnull(wr_item_sk#275) && isnotnull(wr_order_number#286))
                        :     :     :     :           +- Relation[wr_returned_date_sk#273,wr_returned_time_sk#274,wr_item_sk#275,wr_refunded_customer_sk#276,wr_refunded_cdemo_sk#277,wr_refunded_hdemo_sk#278,wr_refunded_addr_sk#279,wr_returning_customer_sk#280,wr_returning_cdemo_sk#281,wr_returning_hdemo_sk#282,wr_returning_addr_sk#283,wr_web_page_sk#284,wr_reason_sk#285,wr_order_number#286,wr_return_quantity#287,wr_return_amt#288,wr_return_tax#289,wr_return_amt_inc_tax#290,wr_fee#291,wr_return_ship_cost#292,wr_refunded_cash#293,wr_reversed_charge#294,wr_account_credit#295,wr_net_loss#296] parquet
                        :     :     :     +- Project [d_date_sk#65]
                        :     :     :        +- Filter (((isnotnull(d_date#67) && (d_date#67 >= 2000-08-23)) && (d_date#67 <= 2000-09-22)) && isnotnull(d_date_sk#65))
                        :     :     :           +- Relation[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
                        :     :     +- Project [web_site_sk#297, web_site_id#298]
                        :     :        +- Filter isnotnull(web_site_sk#297)
                        :     :           +- Relation[web_site_sk#297,web_site_id#298,web_rec_start_date#299,web_rec_end_date#300,web_name#301,web_open_date_sk#302,web_close_date_sk#303,web_class#304,web_manager#305,web_mkt_id#306,web_mkt_class#307,web_mkt_desc#308,web_market_manager#309,web_company_id#310,web_company_name#311,web_street_number#312,web_street_name#313,web_street_type#314,web_suite_number#315,web_city#316,web_county#317,web_state#318,web_zip#319,web_country#320,... 2 more fields] parquet
                        :     +- Project [i_item_sk#122]
                        :        +- Filter ((isnotnull(i_current_price#127) && (i_current_price#127 > 50.0)) && isnotnull(i_item_sk#122))
                        :           +- Relation[i_item_sk#122,i_item_id#123,i_rec_start_date#124,i_rec_end_date#125,i_item_desc#126,i_current_price#127,i_wholesale_cost#128,i_brand_id#129,i_brand#130,i_class_id#131,i_class#132,i_category_id#133,i_category#134,i_manufact_id#135,i_manufact#136,i_size#137,i_formulation#138,i_color#139,i_units#140,i_container#141,i_manager_id#142,i_product_name#143] parquet
                        +- Project [p_promo_sk#144]
                           +- Filter ((isnotnull(p_channel_tv#155) && (p_channel_tv#155 = N)) && isnotnull(p_promo_sk#144))
                              +- Relation[p_promo_sk#144,p_promo_id#145,p_start_date_sk#146,p_end_date_sk#147,p_item_sk#148,p_cost#149,p_response_target#150,p_promo_name#151,p_channel_dmail#152,p_channel_email#153,p_channel_catalog#154,p_channel_tv#155,p_channel_radio#156,p_channel_press#157,p_channel_event#158,p_channel_demo#159,p_channel_details#160,p_purpose#161,p_discount_active#162] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[channel#332 ASC NULLS FIRST,id#333 ASC NULLS FIRST], output=[channel#332,id#333,sales#6,returns#7,profit#8])
+- *(23) HashAggregate(keys=[channel#332, id#333, spark_grouping_id#329], functions=[sum(sales#10), sum(returns#11), sum(profit#12)], output=[channel#332, id#333, sales#6, returns#7, profit#8])
   +- Exchange hashpartitioning(channel#332, id#333, spark_grouping_id#329, 200)
      +- *(22) HashAggregate(keys=[channel#332, id#333, spark_grouping_id#329], functions=[partial_sum(sales#10), partial_sum(returns#11), partial_sum(profit#12)], output=[channel#332, id#333, spark_grouping_id#329, sum#343, sum#344, sum#345])
         +- *(22) Expand [List(sales#10, returns#11, profit#12, channel#330, id#331, 0), List(sales#10, returns#11, profit#12, channel#330, null, 1), List(sales#10, returns#11, profit#12, null, null, 3)], [sales#10, returns#11, profit#12, channel#332, id#333, spark_grouping_id#329]
            +- Union
               :- *(7) HashAggregate(keys=[s_store_id#94], functions=[sum(ss_ext_sales_price#37), sum(coalesce(sr_return_amt#56, 0.0)), sum((ss_net_profit#44 - coalesce(sr_net_loss#64, 0.0)))], output=[sales#10, returns#11, profit#12, channel#330, id#331])
               :  +- Exchange hashpartitioning(s_store_id#94, 200)
               :     +- *(6) HashAggregate(keys=[s_store_id#94], functions=[partial_sum(ss_ext_sales_price#37), partial_sum(coalesce(sr_return_amt#56, 0.0)), partial_sum((ss_net_profit#44 - coalesce(sr_net_loss#64, 0.0)))], output=[s_store_id#94, sum#349, sum#350, sum#351])
               :        +- *(6) Project [ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64, s_store_id#94]
               :           +- *(6) BroadcastHashJoin [ss_promo_sk#30], [p_promo_sk#144], Inner, BuildRight
               :              :- *(6) Project [ss_promo_sk#30, ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64, s_store_id#94]
               :              :  +- *(6) BroadcastHashJoin [ss_item_sk#24], [i_item_sk#122], Inner, BuildRight
               :              :     :- *(6) Project [ss_item_sk#24, ss_promo_sk#30, ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64, s_store_id#94]
               :              :     :  +- *(6) BroadcastHashJoin [ss_store_sk#29], [s_store_sk#93], Inner, BuildRight
               :              :     :     :- *(6) Project [ss_item_sk#24, ss_store_sk#29, ss_promo_sk#30, ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64]
               :              :     :     :  +- *(6) BroadcastHashJoin [ss_sold_date_sk#22], [d_date_sk#65], Inner, BuildRight
               :              :     :     :     :- *(6) Project [ss_sold_date_sk#22, ss_item_sk#24, ss_store_sk#29, ss_promo_sk#30, ss_ext_sales_price#37, ss_net_profit#44, sr_return_amt#56, sr_net_loss#64]
               :              :     :     :     :  +- *(6) BroadcastHashJoin [ss_item_sk#24, ss_ticket_number#31], [sr_item_sk#47, sr_ticket_number#54], LeftOuter, BuildRight
               :              :     :     :     :     :- *(6) Project [ss_sold_date_sk#22, ss_item_sk#24, ss_store_sk#29, ss_promo_sk#30, ss_ticket_number#31, ss_ext_sales_price#37, ss_net_profit#44]
               :              :     :     :     :     :  +- *(6) Filter (((isnotnull(ss_sold_date_sk#22) && isnotnull(ss_store_sk#29)) && isnotnull(ss_item_sk#24)) && isnotnull(ss_promo_sk#30))
               :              :     :     :     :     :     +- *(6) FileScan parquet tpcds.store_sales[ss_sold_date_sk#22,ss_item_sk#24,ss_store_sk#29,ss_promo_sk#30,ss_ticket_number#31,ss_ext_sales_price#37,ss_net_profit#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_item_sk), IsNotNull(ss_promo_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_promo_sk:int,ss_ticket_number:int,ss...
               :              :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, true] as bigint), 32) | (cast(input[1, int, true] as bigint) & 4294967295))))
               :              :     :     :     :        +- *(1) Project [sr_item_sk#47, sr_ticket_number#54, sr_return_amt#56, sr_net_loss#64]
               :              :     :     :     :           +- *(1) Filter (isnotnull(sr_item_sk#47) && isnotnull(sr_ticket_number#54))
               :              :     :     :     :              +- *(1) FileScan parquet tpcds.store_returns[sr_item_sk#47,sr_ticket_number#54,sr_return_amt#56,sr_net_loss#64] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_ticket_number)], ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int,sr_return_amt:double,sr_net_loss:double>
               :              :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :              :     :     :        +- *(2) Project [d_date_sk#65]
               :              :     :     :           +- *(2) Filter (((isnotnull(d_date#67) && (d_date#67 >= 2000-08-23)) && (d_date#67 <= 2000-09-22)) && isnotnull(d_date_sk#65))
               :              :     :     :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#65,d_date#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-08-23), LessThanOrEqual(d_date,2000-09-22), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
               :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :              :     :        +- *(3) Project [s_store_sk#93, s_store_id#94]
               :              :     :           +- *(3) Filter isnotnull(s_store_sk#93)
               :              :     :              +- *(3) FileScan parquet tpcds.store[s_store_sk#93,s_store_id#94] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_id:string>
               :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :              :        +- *(4) Project [i_item_sk#122]
               :              :           +- *(4) Filter ((isnotnull(i_current_price#127) && (i_current_price#127 > 50.0)) && isnotnull(i_item_sk#122))
               :              :              +- *(4) FileScan parquet tpcds.item[i_item_sk#122,i_current_price#127] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThan(i_current_price,50.0), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double>
               :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                 +- *(5) Project [p_promo_sk#144]
               :                    +- *(5) Filter ((isnotnull(p_channel_tv#155) && (p_channel_tv#155 = N)) && isnotnull(p_promo_sk#144))
               :                       +- *(5) FileScan parquet tpcds.promotion[p_promo_sk#144,p_channel_tv#155] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/p..., PartitionFilters: [], PushedFilters: [IsNotNull(p_channel_tv), EqualTo(p_channel_tv,N), IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int,p_channel_tv:string>
               :- *(14) HashAggregate(keys=[cp_catalog_page_id#228], functions=[sum(cs_ext_sales_price#189), sum(coalesce(cr_return_amount#218, 0.0)), sum((cs_net_profit#199 - coalesce(cr_net_loss#226, 0.0)))], output=[sales#14, returns#15, profit#16, channel#336, id#337])
               :  +- Exchange hashpartitioning(cp_catalog_page_id#228, 200)
               :     +- *(13) HashAggregate(keys=[cp_catalog_page_id#228], functions=[partial_sum(cs_ext_sales_price#189), partial_sum(coalesce(cr_return_amount#218, 0.0)), partial_sum((cs_net_profit#199 - coalesce(cr_net_loss#226, 0.0)))], output=[cp_catalog_page_id#228, sum#355, sum#356, sum#357])
               :        +- *(13) Project [cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
               :           +- *(13) BroadcastHashJoin [cs_promo_sk#182], [p_promo_sk#144], Inner, BuildRight
               :              :- *(13) Project [cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
               :              :  +- *(13) BroadcastHashJoin [cs_item_sk#181], [i_item_sk#122], Inner, BuildRight
               :              :     :- *(13) Project [cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
               :              :     :  +- *(13) BroadcastHashJoin [cs_catalog_page_sk#178], [cp_catalog_page_sk#227], Inner, BuildRight
               :              :     :     :- *(13) Project [cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226]
               :              :     :     :  +- *(13) BroadcastHashJoin [cs_sold_date_sk#166], [d_date_sk#65], Inner, BuildRight
               :              :     :     :     :- *(13) Project [cs_sold_date_sk#166, cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226]
               :              :     :     :     :  +- *(13) BroadcastHashJoin [cs_item_sk#181, cs_order_number#183], [cr_item_sk#202, cr_order_number#216], LeftOuter, BuildRight
               :              :     :     :     :     :- *(13) Project [cs_sold_date_sk#166, cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_order_number#183, cs_ext_sales_price#189, cs_net_profit#199]
               :              :     :     :     :     :  +- *(13) Filter (((isnotnull(cs_sold_date_sk#166) && isnotnull(cs_catalog_page_sk#178)) && isnotnull(cs_item_sk#181)) && isnotnull(cs_promo_sk#182))
               :              :     :     :     :     :     +- *(13) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#166,cs_catalog_page_sk#178,cs_item_sk#181,cs_promo_sk#182,cs_order_number#183,cs_ext_sales_price#189,cs_net_profit#199] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_catalog_page_sk), IsNotNull(cs_item_sk), IsNotNull(cs_p..., ReadSchema: struct<cs_sold_date_sk:int,cs_catalog_page_sk:int,cs_item_sk:int,cs_promo_sk:int,cs_order_number:...
               :              :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, true] as bigint), 32) | (cast(input[1, int, true] as bigint) & 4294967295))))
               :              :     :     :     :        +- *(8) Project [cr_item_sk#202, cr_order_number#216, cr_return_amount#218, cr_net_loss#226]
               :              :     :     :     :           +- *(8) Filter (isnotnull(cr_item_sk#202) && isnotnull(cr_order_number#216))
               :              :     :     :     :              +- *(8) FileScan parquet tpcds.catalog_returns[cr_item_sk#202,cr_order_number#216,cr_return_amount#218,cr_net_loss#226] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_item_sk), IsNotNull(cr_order_number)], ReadSchema: struct<cr_item_sk:int,cr_order_number:int,cr_return_amount:double,cr_net_loss:double>
               :              :     :     :     +- ReusedExchange [d_date_sk#65], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :              :     :        +- *(10) Project [cp_catalog_page_sk#227, cp_catalog_page_id#228]
               :              :     :           +- *(10) Filter isnotnull(cp_catalog_page_sk#227)
               :              :     :              +- *(10) FileScan parquet tpcds.catalog_page[cp_catalog_page_sk#227,cp_catalog_page_id#228] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cp_catalog_page_sk)], ReadSchema: struct<cp_catalog_page_sk:int,cp_catalog_page_id:string>
               :              :     +- ReusedExchange [i_item_sk#122], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :              +- ReusedExchange [p_promo_sk#144], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               +- *(21) HashAggregate(keys=[web_site_id#298], functions=[sum(ws_ext_sales_price#262), sum(coalesce(wr_return_amt#288, 0.0)), sum((ws_net_profit#272 - coalesce(wr_net_loss#296, 0.0)))], output=[sales#17, returns#18, profit#19, channel#338, id#339])
                  +- Exchange hashpartitioning(web_site_id#298, 200)
                     +- *(20) HashAggregate(keys=[web_site_id#298], functions=[partial_sum(ws_ext_sales_price#262), partial_sum(coalesce(wr_return_amt#288, 0.0)), partial_sum((ws_net_profit#272 - coalesce(wr_net_loss#296, 0.0)))], output=[web_site_id#298, sum#361, sum#362, sum#363])
                        +- *(20) Project [ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296, web_site_id#298]
                           +- *(20) BroadcastHashJoin [ws_promo_sk#255], [p_promo_sk#144], Inner, BuildRight
                              :- *(20) Project [ws_promo_sk#255, ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296, web_site_id#298]
                              :  +- *(20) BroadcastHashJoin [ws_item_sk#242], [i_item_sk#122], Inner, BuildRight
                              :     :- *(20) Project [ws_item_sk#242, ws_promo_sk#255, ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296, web_site_id#298]
                              :     :  +- *(20) BroadcastHashJoin [ws_web_site_sk#252], [web_site_sk#297], Inner, BuildRight
                              :     :     :- *(20) Project [ws_item_sk#242, ws_web_site_sk#252, ws_promo_sk#255, ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296]
                              :     :     :  +- *(20) BroadcastHashJoin [ws_sold_date_sk#239], [d_date_sk#65], Inner, BuildRight
                              :     :     :     :- *(20) Project [ws_sold_date_sk#239, ws_item_sk#242, ws_web_site_sk#252, ws_promo_sk#255, ws_ext_sales_price#262, ws_net_profit#272, wr_return_amt#288, wr_net_loss#296]
                              :     :     :     :  +- *(20) BroadcastHashJoin [ws_item_sk#242, ws_order_number#256], [wr_item_sk#275, wr_order_number#286], LeftOuter, BuildRight
                              :     :     :     :     :- *(20) Project [ws_sold_date_sk#239, ws_item_sk#242, ws_web_site_sk#252, ws_promo_sk#255, ws_order_number#256, ws_ext_sales_price#262, ws_net_profit#272]
                              :     :     :     :     :  +- *(20) Filter (((isnotnull(ws_sold_date_sk#239) && isnotnull(ws_web_site_sk#252)) && isnotnull(ws_item_sk#242)) && isnotnull(ws_promo_sk#255))
                              :     :     :     :     :     +- *(20) FileScan parquet tpcds.web_sales[ws_sold_date_sk#239,ws_item_sk#242,ws_web_site_sk#252,ws_promo_sk#255,ws_order_number#256,ws_ext_sales_price#262,ws_net_profit#272] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_web_site_sk), IsNotNull(ws_item_sk), IsNotNull(ws_promo..., ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_web_site_sk:int,ws_promo_sk:int,ws_order_number:int,...
                              :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, true] as bigint), 32) | (cast(input[1, int, true] as bigint) & 4294967295))))
                              :     :     :     :        +- *(15) Project [wr_item_sk#275, wr_order_number#286, wr_return_amt#288, wr_net_loss#296]
                              :     :     :     :           +- *(15) Filter (isnotnull(wr_item_sk#275) && isnotnull(wr_order_number#286))
                              :     :     :     :              +- *(15) FileScan parquet tpcds.web_returns[wr_item_sk#275,wr_order_number#286,wr_return_amt#288,wr_net_loss#296] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_item_sk), IsNotNull(wr_order_number)], ReadSchema: struct<wr_item_sk:int,wr_order_number:int,wr_return_amt:double,wr_net_loss:double>
                              :     :     :     +- ReusedExchange [d_date_sk#65], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              :     :        +- *(17) Project [web_site_sk#297, web_site_id#298]
                              :     :           +- *(17) Filter isnotnull(web_site_sk#297)
                              :     :              +- *(17) FileScan parquet tpcds.web_site[web_site_sk#297,web_site_id#298] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_site_id:string>
                              :     +- ReusedExchange [i_item_sk#122], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              +- ReusedExchange [p_promo_sk#144], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.756 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 80 in stream 0 using template query80.tpl
------------------------------------------------------^^^

