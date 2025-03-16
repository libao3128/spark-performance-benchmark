== Parsed Logical Plan ==
CTE [ss, sr, cs, cr, ws, wr]
:  :- 'SubqueryAlias `ss`
:  :  +- 'Aggregate ['s_store_sk], ['s_store_sk, 'sum('ss_ext_sales_price) AS sales#14, 'sum('ss_net_profit) AS profit#15]
:  :     +- 'Filter ((('ss_sold_date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) && ('ss_store_sk = 's_store_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation `store_sales`
:  :           :  +- 'UnresolvedRelation `date_dim`
:  :           +- 'UnresolvedRelation `store`
:  :- 'SubqueryAlias `sr`
:  :  +- 'Aggregate ['s_store_sk], ['s_store_sk, 'sum('sr_return_amt) AS returns#16, 'sum('sr_net_loss) AS profit_loss#17]
:  :     +- 'Filter ((('sr_returned_date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) && ('sr_store_sk = 's_store_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation `store_returns`
:  :           :  +- 'UnresolvedRelation `date_dim`
:  :           +- 'UnresolvedRelation `store`
:  :- 'SubqueryAlias `cs`
:  :  +- 'Aggregate ['cs_call_center_sk], ['cs_call_center_sk, 'sum('cs_ext_sales_price) AS sales#18, 'sum('cs_net_profit) AS profit#19]
:  :     +- 'Filter (('cs_sold_date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 30))))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation `catalog_sales`
:  :           +- 'UnresolvedRelation `date_dim`
:  :- 'SubqueryAlias `cr`
:  :  +- 'Aggregate ['cr_call_center_sk], ['cr_call_center_sk, 'sum('cr_return_amount) AS returns#20, 'sum('cr_net_loss) AS profit_loss#21]
:  :     +- 'Filter (('cr_returned_date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 30))))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation `catalog_returns`
:  :           +- 'UnresolvedRelation `date_dim`
:  :- 'SubqueryAlias `ws`
:  :  +- 'Aggregate ['wp_web_page_sk], ['wp_web_page_sk, 'sum('ws_ext_sales_price) AS sales#22, 'sum('ws_net_profit) AS profit#23]
:  :     +- 'Filter ((('ws_sold_date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) && ('ws_web_page_sk = 'wp_web_page_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation `web_sales`
:  :           :  +- 'UnresolvedRelation `date_dim`
:  :           +- 'UnresolvedRelation `web_page`
:  +- 'SubqueryAlias `wr`
:     +- 'Aggregate ['wp_web_page_sk], ['wp_web_page_sk, 'sum('wr_return_amt) AS returns#24, 'sum('wr_net_loss) AS profit_loss#25]
:        +- 'Filter ((('wr_returned_date_sk = 'd_date_sk) && (('d_date >= cast(2000-08-23 as date)) && ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) && ('wr_web_page_sk = 'wp_web_page_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation `web_returns`
:              :  +- 'UnresolvedRelation `date_dim`
:              +- 'UnresolvedRelation `web_page`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['channel ASC NULLS FIRST, 'id ASC NULLS FIRST], true
         +- 'Aggregate ['rollup('channel, 'id)], ['channel, 'id, 'sum('sales) AS sales#11, 'sum('returns) AS returns#12, 'sum('profit) AS profit#13]
            +- 'SubqueryAlias `x`
               +- 'Union
                  :- 'Union
                  :  :- 'Project [store channel AS channel#0, 'ss.s_store_sk AS id#1, 'sales, 'coalesce('returns, 0) AS returns#2, ('profit - 'coalesce('profit_loss, 0)) AS profit#3]
                  :  :  +- 'Join LeftOuter, ('ss.s_store_sk = 'sr.s_store_sk)
                  :  :     :- 'UnresolvedRelation `ss`
                  :  :     +- 'UnresolvedRelation `sr`
                  :  +- 'Project [catalog channel AS channel#4, 'cs_call_center_sk AS id#5, 'sales, 'returns, ('profit - 'profit_loss) AS profit#6]
                  :     +- 'Join Inner
                  :        :- 'UnresolvedRelation `cs`
                  :        +- 'UnresolvedRelation `cr`
                  +- 'Project [web channel AS channel#7, 'ws.wp_web_page_sk AS id#8, 'sales, 'coalesce('returns, 0) AS returns#9, ('profit - 'coalesce('profit_loss, 0)) AS profit#10]
                     +- 'Join LeftOuter, ('ws.wp_web_page_sk = 'wr.wp_web_page_sk)
                        :- 'UnresolvedRelation `ws`
                        +- 'UnresolvedRelation `wr`

== Analyzed Logical Plan ==
channel: string, id: int, sales: double, returns: double, profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#322 ASC NULLS FIRST, id#323 ASC NULLS FIRST], true
      +- Aggregate [channel#322, id#323, spark_grouping_id#319], [channel#322, id#323, sum(sales#14) AS sales#11, sum(returns#2) AS returns#12, sum(profit#3) AS profit#13]
         +- Expand [List(channel#0, id#1, sales#14, returns#2, profit#3, channel#320, id#321, 0), List(channel#0, id#1, sales#14, returns#2, profit#3, channel#320, null, 1), List(channel#0, id#1, sales#14, returns#2, profit#3, null, null, 3)], [channel#0, id#1, sales#14, returns#2, profit#3, channel#322, id#323, spark_grouping_id#319]
            +- Project [channel#0, id#1, sales#14, returns#2, profit#3, channel#0 AS channel#320, id#1 AS id#321]
               +- SubqueryAlias `x`
                  +- Union
                     :- Union
                     :  :- Project [store channel AS channel#0, s_store_sk#79 AS id#1, sales#14, coalesce(returns#16, cast(0 as double)) AS returns#2, (profit#15 - coalesce(profit_loss#17, cast(0 as double))) AS profit#3]
                     :  :  +- Join LeftOuter, (s_store_sk#79 = s_store_sk#273)
                     :  :     :- SubqueryAlias `ss`
                     :  :     :  +- Aggregate [s_store_sk#79], [s_store_sk#79, sum(ss_ext_sales_price#43) AS sales#14, sum(ss_net_profit#50) AS profit#15]
                     :  :     :     +- Filter (((ss_sold_date_sk#28 = d_date_sk#51) && ((d_date#53 >= cast(cast(2000-08-23 as date) as string)) && (d_date#53 <= cast(date_add(cast(2000-08-23 as date), 30) as string)))) && (ss_store_sk#35 = s_store_sk#79))
                     :  :     :        +- Join Inner
                     :  :     :           :- Join Inner
                     :  :     :           :  :- SubqueryAlias `tpcds`.`store_sales`
                     :  :     :           :  :  +- Relation[ss_sold_date_sk#28,ss_sold_time_sk#29,ss_item_sk#30,ss_customer_sk#31,ss_cdemo_sk#32,ss_hdemo_sk#33,ss_addr_sk#34,ss_store_sk#35,ss_promo_sk#36,ss_ticket_number#37,ss_quantity#38,ss_wholesale_cost#39,ss_list_price#40,ss_sales_price#41,ss_ext_discount_amt#42,ss_ext_sales_price#43,ss_ext_wholesale_cost#44,ss_ext_list_price#45,ss_ext_tax#46,ss_coupon_amt#47,ss_net_paid#48,ss_net_paid_inc_tax#49,ss_net_profit#50] parquet
                     :  :     :           :  +- SubqueryAlias `tpcds`.`date_dim`
                     :  :     :           :     +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
                     :  :     :           +- SubqueryAlias `tpcds`.`store`
                     :  :     :              +- Relation[s_store_sk#79,s_store_id#80,s_rec_start_date#81,s_rec_end_date#82,s_closed_date_sk#83,s_store_name#84,s_number_employees#85,s_floor_space#86,s_hours#87,s_manager#88,s_market_id#89,s_geography_class#90,s_market_desc#91,s_market_manager#92,s_division_id#93,s_division_name#94,s_company_id#95,s_company_name#96,s_street_number#97,s_street_name#98,s_street_type#99,s_suite_number#100,s_city#101,s_county#102,... 5 more fields] parquet
                     :  :     +- SubqueryAlias `sr`
                     :  :        +- Aggregate [s_store_sk#273], [s_store_sk#273, sum(sr_return_amt#121) AS returns#16, sum(sr_net_loss#129) AS profit_loss#17]
                     :  :           +- Filter (((sr_returned_date_sk#110 = d_date_sk#51) && ((d_date#53 >= cast(cast(2000-08-23 as date) as string)) && (d_date#53 <= cast(date_add(cast(2000-08-23 as date), 30) as string)))) && (sr_store_sk#117 = s_store_sk#273))
                     :  :              +- Join Inner
                     :  :                 :- Join Inner
                     :  :                 :  :- SubqueryAlias `tpcds`.`store_returns`
                     :  :                 :  :  +- Relation[sr_returned_date_sk#110,sr_return_time_sk#111,sr_item_sk#112,sr_customer_sk#113,sr_cdemo_sk#114,sr_hdemo_sk#115,sr_addr_sk#116,sr_store_sk#117,sr_reason_sk#118,sr_ticket_number#119,sr_return_quantity#120,sr_return_amt#121,sr_return_tax#122,sr_return_amt_inc_tax#123,sr_fee#124,sr_return_ship_cost#125,sr_refunded_cash#126,sr_reversed_charge#127,sr_store_credit#128,sr_net_loss#129] parquet
                     :  :                 :  +- SubqueryAlias `tpcds`.`date_dim`
                     :  :                 :     +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
                     :  :                 +- SubqueryAlias `tpcds`.`store`
                     :  :                    +- Relation[s_store_sk#273,s_store_id#274,s_rec_start_date#275,s_rec_end_date#276,s_closed_date_sk#277,s_store_name#278,s_number_employees#279,s_floor_space#280,s_hours#281,s_manager#282,s_market_id#283,s_geography_class#284,s_market_desc#285,s_market_manager#286,s_division_id#287,s_division_name#288,s_company_id#289,s_company_name#290,s_street_number#291,s_street_name#292,s_street_type#293,s_suite_number#294,s_city#295,s_county#296,... 5 more fields] parquet
                     :  +- Project [catalog channel AS channel#4, cs_call_center_sk#143 AS id#5, sales#18, returns#20, (profit#19 - profit_loss#21) AS profit#6]
                     :     +- Join Inner
                     :        :- SubqueryAlias `cs`
                     :        :  +- Aggregate [cs_call_center_sk#143], [cs_call_center_sk#143, sum(cs_ext_sales_price#155) AS sales#18, sum(cs_net_profit#165) AS profit#19]
                     :        :     +- Filter ((cs_sold_date_sk#132 = d_date_sk#51) && ((d_date#53 >= cast(cast(2000-08-23 as date) as string)) && (d_date#53 <= cast(date_add(cast(2000-08-23 as date), 30) as string))))
                     :        :        +- Join Inner
                     :        :           :- SubqueryAlias `tpcds`.`catalog_sales`
                     :        :           :  +- Relation[cs_sold_date_sk#132,cs_sold_time_sk#133,cs_ship_date_sk#134,cs_bill_customer_sk#135,cs_bill_cdemo_sk#136,cs_bill_hdemo_sk#137,cs_bill_addr_sk#138,cs_ship_customer_sk#139,cs_ship_cdemo_sk#140,cs_ship_hdemo_sk#141,cs_ship_addr_sk#142,cs_call_center_sk#143,cs_catalog_page_sk#144,cs_ship_mode_sk#145,cs_warehouse_sk#146,cs_item_sk#147,cs_promo_sk#148,cs_order_number#149,cs_quantity#150,cs_wholesale_cost#151,cs_list_price#152,cs_sales_price#153,cs_ext_discount_amt#154,cs_ext_sales_price#155,... 10 more fields] parquet
                     :        :           +- SubqueryAlias `tpcds`.`date_dim`
                     :        :              +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
                     :        +- SubqueryAlias `cr`
                     :           +- Aggregate [cr_call_center_sk#179], [cr_call_center_sk#179, sum(cr_return_amount#186) AS returns#20, sum(cr_net_loss#194) AS profit_loss#21]
                     :              +- Filter ((cr_returned_date_sk#168 = d_date_sk#51) && ((d_date#53 >= cast(cast(2000-08-23 as date) as string)) && (d_date#53 <= cast(date_add(cast(2000-08-23 as date), 30) as string))))
                     :                 +- Join Inner
                     :                    :- SubqueryAlias `tpcds`.`catalog_returns`
                     :                    :  +- Relation[cr_returned_date_sk#168,cr_returned_time_sk#169,cr_item_sk#170,cr_refunded_customer_sk#171,cr_refunded_cdemo_sk#172,cr_refunded_hdemo_sk#173,cr_refunded_addr_sk#174,cr_returning_customer_sk#175,cr_returning_cdemo_sk#176,cr_returning_hdemo_sk#177,cr_returning_addr_sk#178,cr_call_center_sk#179,cr_catalog_page_sk#180,cr_ship_mode_sk#181,cr_warehouse_sk#182,cr_reason_sk#183,cr_order_number#184,cr_return_quantity#185,cr_return_amount#186,cr_return_tax#187,cr_return_amt_inc_tax#188,cr_fee#189,cr_return_ship_cost#190,cr_refunded_cash#191,... 3 more fields] parquet
                     :                    +- SubqueryAlias `tpcds`.`date_dim`
                     :                       +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
                     +- Project [web channel AS channel#7, wp_web_page_sk#231 AS id#8, sales#22, coalesce(returns#24, cast(0 as double)) AS returns#9, (profit#23 - coalesce(profit_loss#25, cast(0 as double))) AS profit#10]
                        +- Join LeftOuter, (wp_web_page_sk#231 = wp_web_page_sk#302)
                           :- SubqueryAlias `ws`
                           :  +- Aggregate [wp_web_page_sk#231], [wp_web_page_sk#231, sum(ws_ext_sales_price#220) AS sales#22, sum(ws_net_profit#230) AS profit#23]
                           :     +- Filter (((ws_sold_date_sk#197 = d_date_sk#51) && ((d_date#53 >= cast(cast(2000-08-23 as date) as string)) && (d_date#53 <= cast(date_add(cast(2000-08-23 as date), 30) as string)))) && (ws_web_page_sk#209 = wp_web_page_sk#231))
                           :        +- Join Inner
                           :           :- Join Inner
                           :           :  :- SubqueryAlias `tpcds`.`web_sales`
                           :           :  :  +- Relation[ws_sold_date_sk#197,ws_sold_time_sk#198,ws_ship_date_sk#199,ws_item_sk#200,ws_bill_customer_sk#201,ws_bill_cdemo_sk#202,ws_bill_hdemo_sk#203,ws_bill_addr_sk#204,ws_ship_customer_sk#205,ws_ship_cdemo_sk#206,ws_ship_hdemo_sk#207,ws_ship_addr_sk#208,ws_web_page_sk#209,ws_web_site_sk#210,ws_ship_mode_sk#211,ws_warehouse_sk#212,ws_promo_sk#213,ws_order_number#214,ws_quantity#215,ws_wholesale_cost#216,ws_list_price#217,ws_sales_price#218,ws_ext_discount_amt#219,ws_ext_sales_price#220,... 10 more fields] parquet
                           :           :  +- SubqueryAlias `tpcds`.`date_dim`
                           :           :     +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
                           :           +- SubqueryAlias `tpcds`.`web_page`
                           :              +- Relation[wp_web_page_sk#231,wp_web_page_id#232,wp_rec_start_date#233,wp_rec_end_date#234,wp_creation_date_sk#235,wp_access_date_sk#236,wp_autogen_flag#237,wp_customer_sk#238,wp_url#239,wp_type#240,wp_char_count#241,wp_link_count#242,wp_image_count#243,wp_max_ad_count#244] parquet
                           +- SubqueryAlias `wr`
                              +- Aggregate [wp_web_page_sk#302], [wp_web_page_sk#302, sum(wr_return_amt#262) AS returns#24, sum(wr_net_loss#270) AS profit_loss#25]
                                 +- Filter (((wr_returned_date_sk#247 = d_date_sk#51) && ((d_date#53 >= cast(cast(2000-08-23 as date) as string)) && (d_date#53 <= cast(date_add(cast(2000-08-23 as date), 30) as string)))) && (wr_web_page_sk#258 = wp_web_page_sk#302))
                                    +- Join Inner
                                       :- Join Inner
                                       :  :- SubqueryAlias `tpcds`.`web_returns`
                                       :  :  +- Relation[wr_returned_date_sk#247,wr_returned_time_sk#248,wr_item_sk#249,wr_refunded_customer_sk#250,wr_refunded_cdemo_sk#251,wr_refunded_hdemo_sk#252,wr_refunded_addr_sk#253,wr_returning_customer_sk#254,wr_returning_cdemo_sk#255,wr_returning_hdemo_sk#256,wr_returning_addr_sk#257,wr_web_page_sk#258,wr_reason_sk#259,wr_order_number#260,wr_return_quantity#261,wr_return_amt#262,wr_return_tax#263,wr_return_amt_inc_tax#264,wr_fee#265,wr_return_ship_cost#266,wr_refunded_cash#267,wr_reversed_charge#268,wr_account_credit#269,wr_net_loss#270] parquet
                                       :  +- SubqueryAlias `tpcds`.`date_dim`
                                       :     +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
                                       +- SubqueryAlias `tpcds`.`web_page`
                                          +- Relation[wp_web_page_sk#302,wp_web_page_id#303,wp_rec_start_date#304,wp_rec_end_date#305,wp_creation_date_sk#306,wp_access_date_sk#307,wp_autogen_flag#308,wp_customer_sk#309,wp_url#310,wp_type#311,wp_char_count#312,wp_link_count#313,wp_image_count#314,wp_max_ad_count#315] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#322 ASC NULLS FIRST, id#323 ASC NULLS FIRST], true
      +- Aggregate [channel#322, id#323, spark_grouping_id#319], [channel#322, id#323, sum(sales#14) AS sales#11, sum(returns#2) AS returns#12, sum(profit#3) AS profit#13]
         +- Expand [List(sales#14, returns#2, profit#3, channel#320, id#321, 0), List(sales#14, returns#2, profit#3, channel#320, null, 1), List(sales#14, returns#2, profit#3, null, null, 3)], [sales#14, returns#2, profit#3, channel#322, id#323, spark_grouping_id#319]
            +- Union
               :- Project [sales#14, coalesce(returns#16, 0.0) AS returns#2, (profit#15 - coalesce(profit_loss#17, 0.0)) AS profit#3, store channel AS channel#320, s_store_sk#79 AS id#321]
               :  +- Join LeftOuter, (s_store_sk#79 = s_store_sk#273)
               :     :- Aggregate [s_store_sk#79], [s_store_sk#79, sum(ss_ext_sales_price#43) AS sales#14, sum(ss_net_profit#50) AS profit#15]
               :     :  +- Project [ss_ext_sales_price#43, ss_net_profit#50, s_store_sk#79]
               :     :     +- Join Inner, (ss_store_sk#35 = s_store_sk#79)
               :     :        :- Project [ss_store_sk#35, ss_ext_sales_price#43, ss_net_profit#50]
               :     :        :  +- Join Inner, (ss_sold_date_sk#28 = d_date_sk#51)
               :     :        :     :- Project [ss_sold_date_sk#28, ss_store_sk#35, ss_ext_sales_price#43, ss_net_profit#50]
               :     :        :     :  +- Filter (isnotnull(ss_sold_date_sk#28) && isnotnull(ss_store_sk#35))
               :     :        :     :     +- Relation[ss_sold_date_sk#28,ss_sold_time_sk#29,ss_item_sk#30,ss_customer_sk#31,ss_cdemo_sk#32,ss_hdemo_sk#33,ss_addr_sk#34,ss_store_sk#35,ss_promo_sk#36,ss_ticket_number#37,ss_quantity#38,ss_wholesale_cost#39,ss_list_price#40,ss_sales_price#41,ss_ext_discount_amt#42,ss_ext_sales_price#43,ss_ext_wholesale_cost#44,ss_ext_list_price#45,ss_ext_tax#46,ss_coupon_amt#47,ss_net_paid#48,ss_net_paid_inc_tax#49,ss_net_profit#50] parquet
               :     :        :     +- Project [d_date_sk#51]
               :     :        :        +- Filter (((isnotnull(d_date#53) && (d_date#53 >= 2000-08-23)) && (d_date#53 <= 2000-09-22)) && isnotnull(d_date_sk#51))
               :     :        :           +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
               :     :        +- Project [s_store_sk#79]
               :     :           +- Filter isnotnull(s_store_sk#79)
               :     :              +- Relation[s_store_sk#79,s_store_id#80,s_rec_start_date#81,s_rec_end_date#82,s_closed_date_sk#83,s_store_name#84,s_number_employees#85,s_floor_space#86,s_hours#87,s_manager#88,s_market_id#89,s_geography_class#90,s_market_desc#91,s_market_manager#92,s_division_id#93,s_division_name#94,s_company_id#95,s_company_name#96,s_street_number#97,s_street_name#98,s_street_type#99,s_suite_number#100,s_city#101,s_county#102,... 5 more fields] parquet
               :     +- Aggregate [s_store_sk#273], [s_store_sk#273, sum(sr_return_amt#121) AS returns#16, sum(sr_net_loss#129) AS profit_loss#17]
               :        +- Project [sr_return_amt#121, sr_net_loss#129, s_store_sk#273]
               :           +- Join Inner, (sr_store_sk#117 = s_store_sk#273)
               :              :- Project [sr_store_sk#117, sr_return_amt#121, sr_net_loss#129]
               :              :  +- Join Inner, (sr_returned_date_sk#110 = d_date_sk#51)
               :              :     :- Project [sr_returned_date_sk#110, sr_store_sk#117, sr_return_amt#121, sr_net_loss#129]
               :              :     :  +- Filter (isnotnull(sr_returned_date_sk#110) && isnotnull(sr_store_sk#117))
               :              :     :     +- Relation[sr_returned_date_sk#110,sr_return_time_sk#111,sr_item_sk#112,sr_customer_sk#113,sr_cdemo_sk#114,sr_hdemo_sk#115,sr_addr_sk#116,sr_store_sk#117,sr_reason_sk#118,sr_ticket_number#119,sr_return_quantity#120,sr_return_amt#121,sr_return_tax#122,sr_return_amt_inc_tax#123,sr_fee#124,sr_return_ship_cost#125,sr_refunded_cash#126,sr_reversed_charge#127,sr_store_credit#128,sr_net_loss#129] parquet
               :              :     +- Project [d_date_sk#51]
               :              :        +- Filter (((isnotnull(d_date#53) && (d_date#53 >= 2000-08-23)) && (d_date#53 <= 2000-09-22)) && isnotnull(d_date_sk#51))
               :              :           +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
               :              +- Project [s_store_sk#273]
               :                 +- Filter isnotnull(s_store_sk#273)
               :                    +- Relation[s_store_sk#273,s_store_id#274,s_rec_start_date#275,s_rec_end_date#276,s_closed_date_sk#277,s_store_name#278,s_number_employees#279,s_floor_space#280,s_hours#281,s_manager#282,s_market_id#283,s_geography_class#284,s_market_desc#285,s_market_manager#286,s_division_id#287,s_division_name#288,s_company_id#289,s_company_name#290,s_street_number#291,s_street_name#292,s_street_type#293,s_suite_number#294,s_city#295,s_county#296,... 5 more fields] parquet
               :- Project [sales#18, returns#20, (profit#19 - profit_loss#21) AS profit#6, catalog channel AS channel#326, cs_call_center_sk#143 AS id#327]
               :  +- Join Inner
               :     :- Aggregate [cs_call_center_sk#143], [cs_call_center_sk#143, sum(cs_ext_sales_price#155) AS sales#18, sum(cs_net_profit#165) AS profit#19]
               :     :  +- Project [cs_call_center_sk#143, cs_ext_sales_price#155, cs_net_profit#165]
               :     :     +- Join Inner, (cs_sold_date_sk#132 = d_date_sk#51)
               :     :        :- Project [cs_sold_date_sk#132, cs_call_center_sk#143, cs_ext_sales_price#155, cs_net_profit#165]
               :     :        :  +- Filter isnotnull(cs_sold_date_sk#132)
               :     :        :     +- Relation[cs_sold_date_sk#132,cs_sold_time_sk#133,cs_ship_date_sk#134,cs_bill_customer_sk#135,cs_bill_cdemo_sk#136,cs_bill_hdemo_sk#137,cs_bill_addr_sk#138,cs_ship_customer_sk#139,cs_ship_cdemo_sk#140,cs_ship_hdemo_sk#141,cs_ship_addr_sk#142,cs_call_center_sk#143,cs_catalog_page_sk#144,cs_ship_mode_sk#145,cs_warehouse_sk#146,cs_item_sk#147,cs_promo_sk#148,cs_order_number#149,cs_quantity#150,cs_wholesale_cost#151,cs_list_price#152,cs_sales_price#153,cs_ext_discount_amt#154,cs_ext_sales_price#155,... 10 more fields] parquet
               :     :        +- Project [d_date_sk#51]
               :     :           +- Filter (((isnotnull(d_date#53) && (d_date#53 >= 2000-08-23)) && (d_date#53 <= 2000-09-22)) && isnotnull(d_date_sk#51))
               :     :              +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
               :     +- Aggregate [cr_call_center_sk#179], [sum(cr_return_amount#186) AS returns#20, sum(cr_net_loss#194) AS profit_loss#21]
               :        +- Project [cr_call_center_sk#179, cr_return_amount#186, cr_net_loss#194]
               :           +- Join Inner, (cr_returned_date_sk#168 = d_date_sk#51)
               :              :- Project [cr_returned_date_sk#168, cr_call_center_sk#179, cr_return_amount#186, cr_net_loss#194]
               :              :  +- Filter isnotnull(cr_returned_date_sk#168)
               :              :     +- Relation[cr_returned_date_sk#168,cr_returned_time_sk#169,cr_item_sk#170,cr_refunded_customer_sk#171,cr_refunded_cdemo_sk#172,cr_refunded_hdemo_sk#173,cr_refunded_addr_sk#174,cr_returning_customer_sk#175,cr_returning_cdemo_sk#176,cr_returning_hdemo_sk#177,cr_returning_addr_sk#178,cr_call_center_sk#179,cr_catalog_page_sk#180,cr_ship_mode_sk#181,cr_warehouse_sk#182,cr_reason_sk#183,cr_order_number#184,cr_return_quantity#185,cr_return_amount#186,cr_return_tax#187,cr_return_amt_inc_tax#188,cr_fee#189,cr_return_ship_cost#190,cr_refunded_cash#191,... 3 more fields] parquet
               :              +- Project [d_date_sk#51]
               :                 +- Filter (((isnotnull(d_date#53) && (d_date#53 >= 2000-08-23)) && (d_date#53 <= 2000-09-22)) && isnotnull(d_date_sk#51))
               :                    +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
               +- Project [sales#22, coalesce(returns#24, 0.0) AS returns#9, (profit#23 - coalesce(profit_loss#25, 0.0)) AS profit#10, web channel AS channel#328, wp_web_page_sk#231 AS id#329]
                  +- Join LeftOuter, (wp_web_page_sk#231 = wp_web_page_sk#302)
                     :- Aggregate [wp_web_page_sk#231], [wp_web_page_sk#231, sum(ws_ext_sales_price#220) AS sales#22, sum(ws_net_profit#230) AS profit#23]
                     :  +- Project [ws_ext_sales_price#220, ws_net_profit#230, wp_web_page_sk#231]
                     :     +- Join Inner, (ws_web_page_sk#209 = wp_web_page_sk#231)
                     :        :- Project [ws_web_page_sk#209, ws_ext_sales_price#220, ws_net_profit#230]
                     :        :  +- Join Inner, (ws_sold_date_sk#197 = d_date_sk#51)
                     :        :     :- Project [ws_sold_date_sk#197, ws_web_page_sk#209, ws_ext_sales_price#220, ws_net_profit#230]
                     :        :     :  +- Filter (isnotnull(ws_sold_date_sk#197) && isnotnull(ws_web_page_sk#209))
                     :        :     :     +- Relation[ws_sold_date_sk#197,ws_sold_time_sk#198,ws_ship_date_sk#199,ws_item_sk#200,ws_bill_customer_sk#201,ws_bill_cdemo_sk#202,ws_bill_hdemo_sk#203,ws_bill_addr_sk#204,ws_ship_customer_sk#205,ws_ship_cdemo_sk#206,ws_ship_hdemo_sk#207,ws_ship_addr_sk#208,ws_web_page_sk#209,ws_web_site_sk#210,ws_ship_mode_sk#211,ws_warehouse_sk#212,ws_promo_sk#213,ws_order_number#214,ws_quantity#215,ws_wholesale_cost#216,ws_list_price#217,ws_sales_price#218,ws_ext_discount_amt#219,ws_ext_sales_price#220,... 10 more fields] parquet
                     :        :     +- Project [d_date_sk#51]
                     :        :        +- Filter (((isnotnull(d_date#53) && (d_date#53 >= 2000-08-23)) && (d_date#53 <= 2000-09-22)) && isnotnull(d_date_sk#51))
                     :        :           +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
                     :        +- Project [wp_web_page_sk#231]
                     :           +- Filter isnotnull(wp_web_page_sk#231)
                     :              +- Relation[wp_web_page_sk#231,wp_web_page_id#232,wp_rec_start_date#233,wp_rec_end_date#234,wp_creation_date_sk#235,wp_access_date_sk#236,wp_autogen_flag#237,wp_customer_sk#238,wp_url#239,wp_type#240,wp_char_count#241,wp_link_count#242,wp_image_count#243,wp_max_ad_count#244] parquet
                     +- Aggregate [wp_web_page_sk#302], [wp_web_page_sk#302, sum(wr_return_amt#262) AS returns#24, sum(wr_net_loss#270) AS profit_loss#25]
                        +- Project [wr_return_amt#262, wr_net_loss#270, wp_web_page_sk#302]
                           +- Join Inner, (wr_web_page_sk#258 = wp_web_page_sk#302)
                              :- Project [wr_web_page_sk#258, wr_return_amt#262, wr_net_loss#270]
                              :  +- Join Inner, (wr_returned_date_sk#247 = d_date_sk#51)
                              :     :- Project [wr_returned_date_sk#247, wr_web_page_sk#258, wr_return_amt#262, wr_net_loss#270]
                              :     :  +- Filter (isnotnull(wr_returned_date_sk#247) && isnotnull(wr_web_page_sk#258))
                              :     :     +- Relation[wr_returned_date_sk#247,wr_returned_time_sk#248,wr_item_sk#249,wr_refunded_customer_sk#250,wr_refunded_cdemo_sk#251,wr_refunded_hdemo_sk#252,wr_refunded_addr_sk#253,wr_returning_customer_sk#254,wr_returning_cdemo_sk#255,wr_returning_hdemo_sk#256,wr_returning_addr_sk#257,wr_web_page_sk#258,wr_reason_sk#259,wr_order_number#260,wr_return_quantity#261,wr_return_amt#262,wr_return_tax#263,wr_return_amt_inc_tax#264,wr_fee#265,wr_return_ship_cost#266,wr_refunded_cash#267,wr_reversed_charge#268,wr_account_credit#269,wr_net_loss#270] parquet
                              :     +- Project [d_date_sk#51]
                              :        +- Filter (((isnotnull(d_date#53) && (d_date#53 >= 2000-08-23)) && (d_date#53 <= 2000-09-22)) && isnotnull(d_date_sk#51))
                              :           +- Relation[d_date_sk#51,d_date_id#52,d_date#53,d_month_seq#54,d_week_seq#55,d_quarter_seq#56,d_year#57,d_dow#58,d_moy#59,d_dom#60,d_qoy#61,d_fy_year#62,d_fy_quarter_seq#63,d_fy_week_seq#64,d_day_name#65,d_quarter_name#66,d_holiday#67,d_weekend#68,d_following_holiday#69,d_first_dom#70,d_last_dom#71,d_same_day_ly#72,d_same_day_lq#73,d_current_day#74,... 4 more fields] parquet
                              +- Project [wp_web_page_sk#302]
                                 +- Filter isnotnull(wp_web_page_sk#302)
                                    +- Relation[wp_web_page_sk#302,wp_web_page_id#303,wp_rec_start_date#304,wp_rec_end_date#305,wp_creation_date_sk#306,wp_access_date_sk#307,wp_autogen_flag#308,wp_customer_sk#309,wp_url#310,wp_type#311,wp_char_count#312,wp_link_count#313,wp_image_count#314,wp_max_ad_count#315] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[channel#322 ASC NULLS FIRST,id#323 ASC NULLS FIRST], output=[channel#322,id#323,sales#11,returns#12,profit#13])
+- *(27) HashAggregate(keys=[channel#322, id#323, spark_grouping_id#319], functions=[sum(sales#14), sum(returns#2), sum(profit#3)], output=[channel#322, id#323, sales#11, returns#12, profit#13])
   +- Exchange hashpartitioning(channel#322, id#323, spark_grouping_id#319, 200)
      +- *(26) HashAggregate(keys=[channel#322, id#323, spark_grouping_id#319], functions=[partial_sum(sales#14), partial_sum(returns#2), partial_sum(profit#3)], output=[channel#322, id#323, spark_grouping_id#319, sum#333, sum#334, sum#335])
         +- *(26) Expand [List(sales#14, returns#2, profit#3, channel#320, id#321, 0), List(sales#14, returns#2, profit#3, channel#320, null, 1), List(sales#14, returns#2, profit#3, null, null, 3)], [sales#14, returns#2, profit#3, channel#322, id#323, spark_grouping_id#319]
            +- Union
               :- *(9) Project [sales#14, coalesce(returns#16, 0.0) AS returns#2, (profit#15 - coalesce(profit_loss#17, 0.0)) AS profit#3, store channel AS channel#320, s_store_sk#79 AS id#321]
               :  +- SortMergeJoin [s_store_sk#79], [s_store_sk#273], LeftOuter
               :     :- *(4) Sort [s_store_sk#79 ASC NULLS FIRST], false, 0
               :     :  +- *(4) HashAggregate(keys=[s_store_sk#79], functions=[sum(ss_ext_sales_price#43), sum(ss_net_profit#50)], output=[s_store_sk#79, sales#14, profit#15])
               :     :     +- Exchange hashpartitioning(s_store_sk#79, 200)
               :     :        +- *(3) HashAggregate(keys=[s_store_sk#79], functions=[partial_sum(ss_ext_sales_price#43), partial_sum(ss_net_profit#50)], output=[s_store_sk#79, sum#338, sum#339])
               :     :           +- *(3) Project [ss_ext_sales_price#43, ss_net_profit#50, s_store_sk#79]
               :     :              +- *(3) BroadcastHashJoin [ss_store_sk#35], [s_store_sk#79], Inner, BuildRight
               :     :                 :- *(3) Project [ss_store_sk#35, ss_ext_sales_price#43, ss_net_profit#50]
               :     :                 :  +- *(3) BroadcastHashJoin [ss_sold_date_sk#28], [d_date_sk#51], Inner, BuildRight
               :     :                 :     :- *(3) Project [ss_sold_date_sk#28, ss_store_sk#35, ss_ext_sales_price#43, ss_net_profit#50]
               :     :                 :     :  +- *(3) Filter (isnotnull(ss_sold_date_sk#28) && isnotnull(ss_store_sk#35))
               :     :                 :     :     +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#28,ss_store_sk#35,ss_ext_sales_price#43,ss_net_profit#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_ext_sales_price:double,ss_net_profit:double>
               :     :                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :                 :        +- *(1) Project [d_date_sk#51]
               :     :                 :           +- *(1) Filter (((isnotnull(d_date#53) && (d_date#53 >= 2000-08-23)) && (d_date#53 <= 2000-09-22)) && isnotnull(d_date_sk#51))
               :     :                 :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#51,d_date#53] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), GreaterThanOrEqual(d_date,2000-08-23), LessThanOrEqual(d_date,2000-09-22), Is..., ReadSchema: struct<d_date_sk:int,d_date:string>
               :     :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :                    +- *(2) Project [s_store_sk#79]
               :     :                       +- *(2) Filter isnotnull(s_store_sk#79)
               :     :                          +- *(2) FileScan parquet tpcds.store[s_store_sk#79] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int>
               :     +- *(8) Sort [s_store_sk#273 ASC NULLS FIRST], false, 0
               :        +- *(8) HashAggregate(keys=[s_store_sk#273], functions=[sum(sr_return_amt#121), sum(sr_net_loss#129)], output=[s_store_sk#273, returns#16, profit_loss#17])
               :           +- Exchange hashpartitioning(s_store_sk#273, 200)
               :              +- *(7) HashAggregate(keys=[s_store_sk#273], functions=[partial_sum(sr_return_amt#121), partial_sum(sr_net_loss#129)], output=[s_store_sk#273, sum#342, sum#343])
               :                 +- *(7) Project [sr_return_amt#121, sr_net_loss#129, s_store_sk#273]
               :                    +- *(7) BroadcastHashJoin [sr_store_sk#117], [s_store_sk#273], Inner, BuildRight
               :                       :- *(7) Project [sr_store_sk#117, sr_return_amt#121, sr_net_loss#129]
               :                       :  +- *(7) BroadcastHashJoin [sr_returned_date_sk#110], [d_date_sk#51], Inner, BuildRight
               :                       :     :- *(7) Project [sr_returned_date_sk#110, sr_store_sk#117, sr_return_amt#121, sr_net_loss#129]
               :                       :     :  +- *(7) Filter (isnotnull(sr_returned_date_sk#110) && isnotnull(sr_store_sk#117))
               :                       :     :     +- *(7) FileScan parquet tpcds.store_returns[sr_returned_date_sk#110,sr_store_sk#117,sr_return_amt#121,sr_net_loss#129] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_returned_date_sk), IsNotNull(sr_store_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_store_sk:int,sr_return_amt:double,sr_net_loss:double>
               :                       :     +- ReusedExchange [d_date_sk#51], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                       +- ReusedExchange [s_store_sk#273], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :- *(16) Project [sales#18, returns#20, (profit#19 - profit_loss#21) AS profit#6, catalog channel AS channel#326, cs_call_center_sk#143 AS id#327]
               :  +- CartesianProduct
               :     :- *(12) HashAggregate(keys=[cs_call_center_sk#143], functions=[sum(cs_ext_sales_price#155), sum(cs_net_profit#165)], output=[cs_call_center_sk#143, sales#18, profit#19])
               :     :  +- Exchange hashpartitioning(cs_call_center_sk#143, 200)
               :     :     +- *(11) HashAggregate(keys=[cs_call_center_sk#143], functions=[partial_sum(cs_ext_sales_price#155), partial_sum(cs_net_profit#165)], output=[cs_call_center_sk#143, sum#346, sum#347])
               :     :        +- *(11) Project [cs_call_center_sk#143, cs_ext_sales_price#155, cs_net_profit#165]
               :     :           +- *(11) BroadcastHashJoin [cs_sold_date_sk#132], [d_date_sk#51], Inner, BuildRight
               :     :              :- *(11) Project [cs_sold_date_sk#132, cs_call_center_sk#143, cs_ext_sales_price#155, cs_net_profit#165]
               :     :              :  +- *(11) Filter isnotnull(cs_sold_date_sk#132)
               :     :              :     +- *(11) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#132,cs_call_center_sk#143,cs_ext_sales_price#155,cs_net_profit#165] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_call_center_sk:int,cs_ext_sales_price:double,cs_net_profit:double>
               :     :              +- ReusedExchange [d_date_sk#51], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     +- *(15) HashAggregate(keys=[cr_call_center_sk#179], functions=[sum(cr_return_amount#186), sum(cr_net_loss#194)], output=[returns#20, profit_loss#21])
               :        +- Exchange hashpartitioning(cr_call_center_sk#179, 200)
               :           +- *(14) HashAggregate(keys=[cr_call_center_sk#179], functions=[partial_sum(cr_return_amount#186), partial_sum(cr_net_loss#194)], output=[cr_call_center_sk#179, sum#350, sum#351])
               :              +- *(14) Project [cr_call_center_sk#179, cr_return_amount#186, cr_net_loss#194]
               :                 +- *(14) BroadcastHashJoin [cr_returned_date_sk#168], [d_date_sk#51], Inner, BuildRight
               :                    :- *(14) Project [cr_returned_date_sk#168, cr_call_center_sk#179, cr_return_amount#186, cr_net_loss#194]
               :                    :  +- *(14) Filter isnotnull(cr_returned_date_sk#168)
               :                    :     +- *(14) FileScan parquet tpcds.catalog_returns[cr_returned_date_sk#168,cr_call_center_sk#179,cr_return_amount#186,cr_net_loss#194] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_returned_date_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_call_center_sk:int,cr_return_amount:double,cr_net_loss:double>
               :                    +- ReusedExchange [d_date_sk#51], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               +- *(25) Project [sales#22, coalesce(returns#24, 0.0) AS returns#9, (profit#23 - coalesce(profit_loss#25, 0.0)) AS profit#10, web channel AS channel#328, wp_web_page_sk#231 AS id#329]
                  +- SortMergeJoin [wp_web_page_sk#231], [wp_web_page_sk#302], LeftOuter
                     :- *(20) Sort [wp_web_page_sk#231 ASC NULLS FIRST], false, 0
                     :  +- *(20) HashAggregate(keys=[wp_web_page_sk#231], functions=[sum(ws_ext_sales_price#220), sum(ws_net_profit#230)], output=[wp_web_page_sk#231, sales#22, profit#23])
                     :     +- Exchange hashpartitioning(wp_web_page_sk#231, 200)
                     :        +- *(19) HashAggregate(keys=[wp_web_page_sk#231], functions=[partial_sum(ws_ext_sales_price#220), partial_sum(ws_net_profit#230)], output=[wp_web_page_sk#231, sum#354, sum#355])
                     :           +- *(19) Project [ws_ext_sales_price#220, ws_net_profit#230, wp_web_page_sk#231]
                     :              +- *(19) BroadcastHashJoin [ws_web_page_sk#209], [wp_web_page_sk#231], Inner, BuildRight
                     :                 :- *(19) Project [ws_web_page_sk#209, ws_ext_sales_price#220, ws_net_profit#230]
                     :                 :  +- *(19) BroadcastHashJoin [ws_sold_date_sk#197], [d_date_sk#51], Inner, BuildRight
                     :                 :     :- *(19) Project [ws_sold_date_sk#197, ws_web_page_sk#209, ws_ext_sales_price#220, ws_net_profit#230]
                     :                 :     :  +- *(19) Filter (isnotnull(ws_sold_date_sk#197) && isnotnull(ws_web_page_sk#209))
                     :                 :     :     +- *(19) FileScan parquet tpcds.web_sales[ws_sold_date_sk#197,ws_web_page_sk#209,ws_ext_sales_price#220,ws_net_profit#230] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_web_page_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_web_page_sk:int,ws_ext_sales_price:double,ws_net_profit:double>
                     :                 :     +- ReusedExchange [d_date_sk#51], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :                    +- *(18) Project [wp_web_page_sk#231]
                     :                       +- *(18) Filter isnotnull(wp_web_page_sk#231)
                     :                          +- *(18) FileScan parquet tpcds.web_page[wp_web_page_sk#231] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wp_web_page_sk)], ReadSchema: struct<wp_web_page_sk:int>
                     +- *(24) Sort [wp_web_page_sk#302 ASC NULLS FIRST], false, 0
                        +- *(24) HashAggregate(keys=[wp_web_page_sk#302], functions=[sum(wr_return_amt#262), sum(wr_net_loss#270)], output=[wp_web_page_sk#302, returns#24, profit_loss#25])
                           +- Exchange hashpartitioning(wp_web_page_sk#302, 200)
                              +- *(23) HashAggregate(keys=[wp_web_page_sk#302], functions=[partial_sum(wr_return_amt#262), partial_sum(wr_net_loss#270)], output=[wp_web_page_sk#302, sum#358, sum#359])
                                 +- *(23) Project [wr_return_amt#262, wr_net_loss#270, wp_web_page_sk#302]
                                    +- *(23) BroadcastHashJoin [wr_web_page_sk#258], [wp_web_page_sk#302], Inner, BuildRight
                                       :- *(23) Project [wr_web_page_sk#258, wr_return_amt#262, wr_net_loss#270]
                                       :  +- *(23) BroadcastHashJoin [wr_returned_date_sk#247], [d_date_sk#51], Inner, BuildRight
                                       :     :- *(23) Project [wr_returned_date_sk#247, wr_web_page_sk#258, wr_return_amt#262, wr_net_loss#270]
                                       :     :  +- *(23) Filter (isnotnull(wr_returned_date_sk#247) && isnotnull(wr_web_page_sk#258))
                                       :     :     +- *(23) FileScan parquet tpcds.web_returns[wr_returned_date_sk#247,wr_web_page_sk#258,wr_return_amt#262,wr_net_loss#270] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_returned_date_sk), IsNotNull(wr_web_page_sk)], ReadSchema: struct<wr_returned_date_sk:int,wr_web_page_sk:int,wr_return_amt:double,wr_net_loss:double>
                                       :     +- ReusedExchange [d_date_sk#51], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                       +- ReusedExchange [wp_web_page_sk#302], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.743 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 77 in stream 0 using template query77.tpl
------------------------------------------------------^^^

