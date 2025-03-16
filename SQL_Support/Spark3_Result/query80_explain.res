Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582643879
== Parsed Logical Plan ==
CTE [ssr, csr, wsr]
:  :- 'SubqueryAlias ssr
:  :  +- 'Aggregate ['s_store_id], ['s_store_id AS store_id#9, 'sum('ss_ext_sales_price) AS sales#10, 'sum('coalesce('sr_return_amt, 0)) AS returns#11, 'sum(('ss_net_profit - 'coalesce('sr_net_loss, 0))) AS profit#12]
:  :     +- 'Filter (((('ss_sold_date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) AND (('ss_store_sk = 's_store_sk) AND ('ss_item_sk = 'i_item_sk))) AND ((('i_current_price > 50) AND ('ss_promo_sk = 'p_promo_sk)) AND ('p_channel_tv = N)))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'Join Inner
:  :           :  :  :  :- 'Join LeftOuter, (('ss_item_sk = 'sr_item_sk) AND ('ss_ticket_number = 'sr_ticket_number))
:  :           :  :  :  :  :- 'UnresolvedRelation [store_sales], [], false
:  :           :  :  :  :  +- 'UnresolvedRelation [store_returns], [], false
:  :           :  :  :  +- 'UnresolvedRelation [date_dim], [], false
:  :           :  :  +- 'UnresolvedRelation [store], [], false
:  :           :  +- 'UnresolvedRelation [item], [], false
:  :           +- 'UnresolvedRelation [promotion], [], false
:  :- 'SubqueryAlias csr
:  :  +- 'Aggregate ['cp_catalog_page_id], ['cp_catalog_page_id AS catalog_page_id#13, 'sum('cs_ext_sales_price) AS sales#14, 'sum('coalesce('cr_return_amount, 0)) AS returns#15, 'sum(('cs_net_profit - 'coalesce('cr_net_loss, 0))) AS profit#16]
:  :     +- 'Filter (((('cs_sold_date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) AND (('cs_catalog_page_sk = 'cp_catalog_page_sk) AND ('cs_item_sk = 'i_item_sk))) AND ((('i_current_price > 50) AND ('cs_promo_sk = 'p_promo_sk)) AND ('p_channel_tv = N)))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'Join Inner
:  :           :  :  :  :- 'Join LeftOuter, (('cs_item_sk = 'cr_item_sk) AND ('cs_order_number = 'cr_order_number))
:  :           :  :  :  :  :- 'UnresolvedRelation [catalog_sales], [], false
:  :           :  :  :  :  +- 'UnresolvedRelation [catalog_returns], [], false
:  :           :  :  :  +- 'UnresolvedRelation [date_dim], [], false
:  :           :  :  +- 'UnresolvedRelation [catalog_page], [], false
:  :           :  +- 'UnresolvedRelation [item], [], false
:  :           +- 'UnresolvedRelation [promotion], [], false
:  +- 'SubqueryAlias wsr
:     +- 'Aggregate ['web_site_id], ['web_site_id, 'sum('ws_ext_sales_price) AS sales#17, 'sum('coalesce('wr_return_amt, 0)) AS returns#18, 'sum(('ws_net_profit - 'coalesce('wr_net_loss, 0))) AS profit#19]
:        +- 'Filter (((('ws_sold_date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) AND (('ws_web_site_sk = 'web_site_sk) AND ('ws_item_sk = 'i_item_sk))) AND ((('i_current_price > 50) AND ('ws_promo_sk = 'p_promo_sk)) AND ('p_channel_tv = N)))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'Join Inner
:              :  :  :- 'Join Inner
:              :  :  :  :- 'Join LeftOuter, (('ws_item_sk = 'wr_item_sk) AND ('ws_order_number = 'wr_order_number))
:              :  :  :  :  :- 'UnresolvedRelation [web_sales], [], false
:              :  :  :  :  +- 'UnresolvedRelation [web_returns], [], false
:              :  :  :  +- 'UnresolvedRelation [date_dim], [], false
:              :  :  +- 'UnresolvedRelation [web_site], [], false
:              :  +- 'UnresolvedRelation [item], [], false
:              +- 'UnresolvedRelation [promotion], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['channel ASC NULLS FIRST, 'id ASC NULLS FIRST], true
         +- 'Aggregate [rollup(Vector(0), Vector(1), 'channel, 'id)], ['channel, 'id, 'sum('sales) AS sales#6, 'sum('returns) AS returns#7, 'sum('profit) AS profit#8]
            +- 'SubqueryAlias x
               +- 'Union false, false
                  :- 'Union false, false
                  :  :- 'Project [store channel AS channel#0, 'concat(store, 'store_id) AS id#1, 'sales, 'returns, 'profit]
                  :  :  +- 'UnresolvedRelation [ssr], [], false
                  :  +- 'Project [catalog channel AS channel#2, 'concat(catalog_page, 'catalog_page_id) AS id#3, 'sales, 'returns, 'profit]
                  :     +- 'UnresolvedRelation [csr], [], false
                  +- 'Project [web channel AS channel#4, 'concat(web_site, 'web_site_id) AS id#5, 'sales, 'returns, 'profit]
                     +- 'UnresolvedRelation [wsr], [], false

== Analyzed Logical Plan ==
channel: string, id: string, sales: double, returns: double, profit: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ssr
:     +- Aggregate [s_store_id#97], [s_store_id#97 AS store_id#9, sum(ss_ext_sales_price#40) AS sales#10, sum(coalesce(sr_return_amt#59, cast(0 as double))) AS returns#11, sum((ss_net_profit#47 - coalesce(sr_net_loss#67, cast(0 as double)))) AS profit#12]
:        +- Filter ((((ss_sold_date_sk#25 = d_date_sk#68) AND ((cast(d_date#70 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#70 as date) <= date_add(cast(2000-08-23 as date), 30)))) AND ((ss_store_sk#32 = s_store_sk#96) AND (ss_item_sk#27 = i_item_sk#125))) AND (((i_current_price#130 > cast(50 as double)) AND (ss_promo_sk#33 = p_promo_sk#147)) AND (p_channel_tv#158 = N)))
:           +- Join Inner
:              :- Join Inner
:              :  :- Join Inner
:              :  :  :- Join Inner
:              :  :  :  :- Join LeftOuter, ((ss_item_sk#27 = sr_item_sk#50) AND (ss_ticket_number#34 = sr_ticket_number#57))
:              :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#25,ss_sold_time_sk#26,ss_item_sk#27,ss_customer_sk#28,ss_cdemo_sk#29,ss_hdemo_sk#30,ss_addr_sk#31,ss_store_sk#32,ss_promo_sk#33,ss_ticket_number#34,ss_quantity#35,ss_wholesale_cost#36,ss_list_price#37,ss_sales_price#38,ss_ext_discount_amt#39,ss_ext_sales_price#40,ss_ext_wholesale_cost#41,ss_ext_list_price#42,ss_ext_tax#43,ss_coupon_amt#44,ss_net_paid#45,ss_net_paid_inc_tax#46,ss_net_profit#47] parquet
:              :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.store_returns
:              :  :  :  :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#48,sr_return_time_sk#49,sr_item_sk#50,sr_customer_sk#51,sr_cdemo_sk#52,sr_hdemo_sk#53,sr_addr_sk#54,sr_store_sk#55,sr_reason_sk#56,sr_ticket_number#57,sr_return_quantity#58,sr_return_amt#59,sr_return_tax#60,sr_return_amt_inc_tax#61,sr_fee#62,sr_return_ship_cost#63,sr_refunded_cash#64,sr_reversed_charge#65,sr_store_credit#66,sr_net_loss#67] parquet
:              :  :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :  :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#68,d_date_id#69,d_date#70,d_month_seq#71,d_week_seq#72,d_quarter_seq#73,d_year#74,d_dow#75,d_moy#76,d_dom#77,d_qoy#78,d_fy_year#79,d_fy_quarter_seq#80,d_fy_week_seq#81,d_day_name#82,d_quarter_name#83,d_holiday#84,d_weekend#85,d_following_holiday#86,d_first_dom#87,d_last_dom#88,d_same_day_ly#89,d_same_day_lq#90,d_current_day#91,... 4 more fields] parquet
:              :  :  +- SubqueryAlias spark_catalog.tpcds.store
:              :  :     +- Relation spark_catalog.tpcds.store[s_store_sk#96,s_store_id#97,s_rec_start_date#98,s_rec_end_date#99,s_closed_date_sk#100,s_store_name#101,s_number_employees#102,s_floor_space#103,s_hours#104,s_manager#105,s_market_id#106,s_geography_class#107,s_market_desc#108,s_market_manager#109,s_division_id#110,s_division_name#111,s_company_id#112,s_company_name#113,s_street_number#114,s_street_name#115,s_street_type#116,s_suite_number#117,s_city#118,s_county#119,... 5 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.item
:              :     +- Relation spark_catalog.tpcds.item[i_item_sk#125,i_item_id#126,i_rec_start_date#127,i_rec_end_date#128,i_item_desc#129,i_current_price#130,i_wholesale_cost#131,i_brand_id#132,i_brand#133,i_class_id#134,i_class#135,i_category_id#136,i_category#137,i_manufact_id#138,i_manufact#139,i_size#140,i_formulation#141,i_color#142,i_units#143,i_container#144,i_manager_id#145,i_product_name#146] parquet
:              +- SubqueryAlias spark_catalog.tpcds.promotion
:                 +- Relation spark_catalog.tpcds.promotion[p_promo_sk#147,p_promo_id#148,p_start_date_sk#149,p_end_date_sk#150,p_item_sk#151,p_cost#152,p_response_target#153,p_promo_name#154,p_channel_dmail#155,p_channel_email#156,p_channel_catalog#157,p_channel_tv#158,p_channel_radio#159,p_channel_press#160,p_channel_event#161,p_channel_demo#162,p_channel_details#163,p_purpose#164,p_discount_active#165] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias csr
:     +- Aggregate [cp_catalog_page_id#228], [cp_catalog_page_id#228 AS catalog_page_id#13, sum(cs_ext_sales_price#189) AS sales#14, sum(coalesce(cr_return_amount#218, cast(0 as double))) AS returns#15, sum((cs_net_profit#199 - coalesce(cr_net_loss#226, cast(0 as double)))) AS profit#16]
:        +- Filter ((((cs_sold_date_sk#166 = d_date_sk#320) AND ((cast(d_date#322 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#322 as date) <= date_add(cast(2000-08-23 as date), 30)))) AND ((cs_catalog_page_sk#178 = cp_catalog_page_sk#227) AND (cs_item_sk#181 = i_item_sk#348))) AND (((i_current_price#353 > cast(50 as double)) AND (cs_promo_sk#182 = p_promo_sk#370)) AND (p_channel_tv#381 = N)))
:           +- Join Inner
:              :- Join Inner
:              :  :- Join Inner
:              :  :  :- Join Inner
:              :  :  :  :- Join LeftOuter, ((cs_item_sk#181 = cr_item_sk#202) AND (cs_order_number#183 = cr_order_number#216))
:              :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
:              :  :  :  :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#166,cs_sold_time_sk#167,cs_ship_date_sk#168,cs_bill_customer_sk#169,cs_bill_cdemo_sk#170,cs_bill_hdemo_sk#171,cs_bill_addr_sk#172,cs_ship_customer_sk#173,cs_ship_cdemo_sk#174,cs_ship_hdemo_sk#175,cs_ship_addr_sk#176,cs_call_center_sk#177,cs_catalog_page_sk#178,cs_ship_mode_sk#179,cs_warehouse_sk#180,cs_item_sk#181,cs_promo_sk#182,cs_order_number#183,cs_quantity#184,cs_wholesale_cost#185,cs_list_price#186,cs_sales_price#187,cs_ext_discount_amt#188,cs_ext_sales_price#189,... 10 more fields] parquet
:              :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_returns
:              :  :  :  :     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#200,cr_returned_time_sk#201,cr_item_sk#202,cr_refunded_customer_sk#203,cr_refunded_cdemo_sk#204,cr_refunded_hdemo_sk#205,cr_refunded_addr_sk#206,cr_returning_customer_sk#207,cr_returning_cdemo_sk#208,cr_returning_hdemo_sk#209,cr_returning_addr_sk#210,cr_call_center_sk#211,cr_catalog_page_sk#212,cr_ship_mode_sk#213,cr_warehouse_sk#214,cr_reason_sk#215,cr_order_number#216,cr_return_quantity#217,cr_return_amount#218,cr_return_tax#219,cr_return_amt_inc_tax#220,cr_fee#221,cr_return_ship_cost#222,cr_refunded_cash#223,... 3 more fields] parquet
:              :  :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :  :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#320,d_date_id#321,d_date#322,d_month_seq#323,d_week_seq#324,d_quarter_seq#325,d_year#326,d_dow#327,d_moy#328,d_dom#329,d_qoy#330,d_fy_year#331,d_fy_quarter_seq#332,d_fy_week_seq#333,d_day_name#334,d_quarter_name#335,d_holiday#336,d_weekend#337,d_following_holiday#338,d_first_dom#339,d_last_dom#340,d_same_day_ly#341,d_same_day_lq#342,d_current_day#343,... 4 more fields] parquet
:              :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_page
:              :  :     +- Relation spark_catalog.tpcds.catalog_page[cp_catalog_page_sk#227,cp_catalog_page_id#228,cp_start_date_sk#229,cp_end_date_sk#230,cp_department#231,cp_catalog_number#232,cp_catalog_page_number#233,cp_description#234,cp_type#235] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.item
:              :     +- Relation spark_catalog.tpcds.item[i_item_sk#348,i_item_id#349,i_rec_start_date#350,i_rec_end_date#351,i_item_desc#352,i_current_price#353,i_wholesale_cost#354,i_brand_id#355,i_brand#356,i_class_id#357,i_class#358,i_category_id#359,i_category#360,i_manufact_id#361,i_manufact#362,i_size#363,i_formulation#364,i_color#365,i_units#366,i_container#367,i_manager_id#368,i_product_name#369] parquet
:              +- SubqueryAlias spark_catalog.tpcds.promotion
:                 +- Relation spark_catalog.tpcds.promotion[p_promo_sk#370,p_promo_id#371,p_start_date_sk#372,p_end_date_sk#373,p_item_sk#374,p_cost#375,p_response_target#376,p_promo_name#377,p_channel_dmail#378,p_channel_email#379,p_channel_catalog#380,p_channel_tv#381,p_channel_radio#382,p_channel_press#383,p_channel_event#384,p_channel_demo#385,p_channel_details#386,p_purpose#387,p_discount_active#388] parquet
:- CTERelationDef 2, false
:  +- SubqueryAlias wsr
:     +- Aggregate [web_site_id#295], [web_site_id#295, sum(ws_ext_sales_price#259) AS sales#17, sum(coalesce(wr_return_amt#285, cast(0 as double))) AS returns#18, sum((ws_net_profit#269 - coalesce(wr_net_loss#293, cast(0 as double)))) AS profit#19]
:        +- Filter ((((ws_sold_date_sk#236 = d_date_sk#389) AND ((cast(d_date#391 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#391 as date) <= date_add(cast(2000-08-23 as date), 30)))) AND ((ws_web_site_sk#249 = web_site_sk#294) AND (ws_item_sk#239 = i_item_sk#417))) AND (((i_current_price#422 > cast(50 as double)) AND (ws_promo_sk#252 = p_promo_sk#439)) AND (p_channel_tv#450 = N)))
:           +- Join Inner
:              :- Join Inner
:              :  :- Join Inner
:              :  :  :- Join Inner
:              :  :  :  :- Join LeftOuter, ((ws_item_sk#239 = wr_item_sk#272) AND (ws_order_number#253 = wr_order_number#283))
:              :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.web_sales
:              :  :  :  :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#236,ws_sold_time_sk#237,ws_ship_date_sk#238,ws_item_sk#239,ws_bill_customer_sk#240,ws_bill_cdemo_sk#241,ws_bill_hdemo_sk#242,ws_bill_addr_sk#243,ws_ship_customer_sk#244,ws_ship_cdemo_sk#245,ws_ship_hdemo_sk#246,ws_ship_addr_sk#247,ws_web_page_sk#248,ws_web_site_sk#249,ws_ship_mode_sk#250,ws_warehouse_sk#251,ws_promo_sk#252,ws_order_number#253,ws_quantity#254,ws_wholesale_cost#255,ws_list_price#256,ws_sales_price#257,ws_ext_discount_amt#258,ws_ext_sales_price#259,... 10 more fields] parquet
:              :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.web_returns
:              :  :  :  :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#270,wr_returned_time_sk#271,wr_item_sk#272,wr_refunded_customer_sk#273,wr_refunded_cdemo_sk#274,wr_refunded_hdemo_sk#275,wr_refunded_addr_sk#276,wr_returning_customer_sk#277,wr_returning_cdemo_sk#278,wr_returning_hdemo_sk#279,wr_returning_addr_sk#280,wr_web_page_sk#281,wr_reason_sk#282,wr_order_number#283,wr_return_quantity#284,wr_return_amt#285,wr_return_tax#286,wr_return_amt_inc_tax#287,wr_fee#288,wr_return_ship_cost#289,wr_refunded_cash#290,wr_reversed_charge#291,wr_account_credit#292,wr_net_loss#293] parquet
:              :  :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :  :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#389,d_date_id#390,d_date#391,d_month_seq#392,d_week_seq#393,d_quarter_seq#394,d_year#395,d_dow#396,d_moy#397,d_dom#398,d_qoy#399,d_fy_year#400,d_fy_quarter_seq#401,d_fy_week_seq#402,d_day_name#403,d_quarter_name#404,d_holiday#405,d_weekend#406,d_following_holiday#407,d_first_dom#408,d_last_dom#409,d_same_day_ly#410,d_same_day_lq#411,d_current_day#412,... 4 more fields] parquet
:              :  :  +- SubqueryAlias spark_catalog.tpcds.web_site
:              :  :     +- Relation spark_catalog.tpcds.web_site[web_site_sk#294,web_site_id#295,web_rec_start_date#296,web_rec_end_date#297,web_name#298,web_open_date_sk#299,web_close_date_sk#300,web_class#301,web_manager#302,web_mkt_id#303,web_mkt_class#304,web_mkt_desc#305,web_market_manager#306,web_company_id#307,web_company_name#308,web_street_number#309,web_street_name#310,web_street_type#311,web_suite_number#312,web_city#313,web_county#314,web_state#315,web_zip#316,web_country#317,... 2 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.item
:              :     +- Relation spark_catalog.tpcds.item[i_item_sk#417,i_item_id#418,i_rec_start_date#419,i_rec_end_date#420,i_item_desc#421,i_current_price#422,i_wholesale_cost#423,i_brand_id#424,i_brand#425,i_class_id#426,i_class#427,i_category_id#428,i_category#429,i_manufact_id#430,i_manufact#431,i_size#432,i_formulation#433,i_color#434,i_units#435,i_container#436,i_manager_id#437,i_product_name#438] parquet
:              +- SubqueryAlias spark_catalog.tpcds.promotion
:                 +- Relation spark_catalog.tpcds.promotion[p_promo_sk#439,p_promo_id#440,p_start_date_sk#441,p_end_date_sk#442,p_item_sk#443,p_cost#444,p_response_target#445,p_promo_name#446,p_channel_dmail#447,p_channel_email#448,p_channel_catalog#449,p_channel_tv#450,p_channel_radio#451,p_channel_press#452,p_channel_event#453,p_channel_demo#454,p_channel_details#455,p_purpose#456,p_discount_active#457] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [channel#491 ASC NULLS FIRST, id#492 ASC NULLS FIRST], true
         +- Aggregate [channel#491, id#492, spark_grouping_id#490L], [channel#491, id#492, sum(sales#10) AS sales#6, sum(returns#11) AS returns#7, sum(profit#12) AS profit#8]
            +- Expand [[channel#0, id#1, sales#10, returns#11, profit#12, channel#488, id#489, 0], [channel#0, id#1, sales#10, returns#11, profit#12, channel#488, null, 1], [channel#0, id#1, sales#10, returns#11, profit#12, null, null, 3]], [channel#0, id#1, sales#10, returns#11, profit#12, channel#491, id#492, spark_grouping_id#490L]
               +- Project [channel#0, id#1, sales#10, returns#11, profit#12, channel#0 AS channel#488, id#1 AS id#489]
                  +- SubqueryAlias x
                     +- Union false, false
                        :- Union false, false
                        :  :- Project [store channel AS channel#0, concat(store, store_id#9) AS id#1, sales#10, returns#11, profit#12]
                        :  :  +- SubqueryAlias ssr
                        :  :     +- CTERelationRef 0, true, [store_id#9, sales#10, returns#11, profit#12], false
                        :  +- Project [catalog channel AS channel#2, concat(catalog_page, catalog_page_id#13) AS id#3, sales#14, returns#15, profit#16]
                        :     +- SubqueryAlias csr
                        :        +- CTERelationRef 1, true, [catalog_page_id#13, sales#14, returns#15, profit#16], false
                        +- Project [web channel AS channel#4, concat(web_site, web_site_id#295) AS id#5, sales#17, returns#18, profit#19]
                           +- SubqueryAlias wsr
                              +- CTERelationRef 2, true, [web_site_id#295, sales#17, returns#18, profit#19], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#491 ASC NULLS FIRST, id#492 ASC NULLS FIRST], true
      +- Aggregate [channel#491, id#492, spark_grouping_id#490L], [channel#491, id#492, sum(sales#10) AS sales#6, sum(returns#11) AS returns#7, sum(profit#12) AS profit#8]
         +- Expand [[sales#10, returns#11, profit#12, channel#488, id#489, 0], [sales#10, returns#11, profit#12, channel#488, null, 1], [sales#10, returns#11, profit#12, null, null, 3]], [sales#10, returns#11, profit#12, channel#491, id#492, spark_grouping_id#490L]
            +- Union false, false
               :- Aggregate [s_store_id#97], [sum(ss_ext_sales_price#40) AS sales#10, sum(coalesce(sr_return_amt#59, 0.0)) AS returns#11, sum((ss_net_profit#47 - coalesce(sr_net_loss#67, 0.0))) AS profit#12, store channel AS channel#488, concat(store, s_store_id#97) AS id#489]
               :  +- Project [ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67, s_store_id#97]
               :     +- Join Inner, (ss_promo_sk#33 = p_promo_sk#147)
               :        :- Project [ss_promo_sk#33, ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67, s_store_id#97]
               :        :  +- Join Inner, (ss_item_sk#27 = i_item_sk#125)
               :        :     :- Project [ss_item_sk#27, ss_promo_sk#33, ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67, s_store_id#97]
               :        :     :  +- Join Inner, (ss_store_sk#32 = s_store_sk#96)
               :        :     :     :- Project [ss_item_sk#27, ss_store_sk#32, ss_promo_sk#33, ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67]
               :        :     :     :  +- Join Inner, (ss_sold_date_sk#25 = d_date_sk#68)
               :        :     :     :     :- Project [ss_sold_date_sk#25, ss_item_sk#27, ss_store_sk#32, ss_promo_sk#33, ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67]
               :        :     :     :     :  +- Join LeftOuter, ((ss_item_sk#27 = sr_item_sk#50) AND (ss_ticket_number#34 = sr_ticket_number#57))
               :        :     :     :     :     :- Project [ss_sold_date_sk#25, ss_item_sk#27, ss_store_sk#32, ss_promo_sk#33, ss_ticket_number#34, ss_ext_sales_price#40, ss_net_profit#47]
               :        :     :     :     :     :  +- Filter (((isnotnull(ss_sold_date_sk#25) AND isnotnull(ss_store_sk#32)) AND isnotnull(ss_item_sk#27)) AND isnotnull(ss_promo_sk#33))
               :        :     :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#25,ss_sold_time_sk#26,ss_item_sk#27,ss_customer_sk#28,ss_cdemo_sk#29,ss_hdemo_sk#30,ss_addr_sk#31,ss_store_sk#32,ss_promo_sk#33,ss_ticket_number#34,ss_quantity#35,ss_wholesale_cost#36,ss_list_price#37,ss_sales_price#38,ss_ext_discount_amt#39,ss_ext_sales_price#40,ss_ext_wholesale_cost#41,ss_ext_list_price#42,ss_ext_tax#43,ss_coupon_amt#44,ss_net_paid#45,ss_net_paid_inc_tax#46,ss_net_profit#47] parquet
               :        :     :     :     :     +- Project [sr_item_sk#50, sr_ticket_number#57, sr_return_amt#59, sr_net_loss#67]
               :        :     :     :     :        +- Filter (isnotnull(sr_item_sk#50) AND isnotnull(sr_ticket_number#57))
               :        :     :     :     :           +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#48,sr_return_time_sk#49,sr_item_sk#50,sr_customer_sk#51,sr_cdemo_sk#52,sr_hdemo_sk#53,sr_addr_sk#54,sr_store_sk#55,sr_reason_sk#56,sr_ticket_number#57,sr_return_quantity#58,sr_return_amt#59,sr_return_tax#60,sr_return_amt_inc_tax#61,sr_fee#62,sr_return_ship_cost#63,sr_refunded_cash#64,sr_reversed_charge#65,sr_store_credit#66,sr_net_loss#67] parquet
               :        :     :     :     +- Project [d_date_sk#68]
               :        :     :     :        +- Filter ((isnotnull(d_date#70) AND ((cast(d_date#70 as date) >= 2000-08-23) AND (cast(d_date#70 as date) <= 2000-09-22))) AND isnotnull(d_date_sk#68))
               :        :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#68,d_date_id#69,d_date#70,d_month_seq#71,d_week_seq#72,d_quarter_seq#73,d_year#74,d_dow#75,d_moy#76,d_dom#77,d_qoy#78,d_fy_year#79,d_fy_quarter_seq#80,d_fy_week_seq#81,d_day_name#82,d_quarter_name#83,d_holiday#84,d_weekend#85,d_following_holiday#86,d_first_dom#87,d_last_dom#88,d_same_day_ly#89,d_same_day_lq#90,d_current_day#91,... 4 more fields] parquet
               :        :     :     +- Project [s_store_sk#96, s_store_id#97]
               :        :     :        +- Filter isnotnull(s_store_sk#96)
               :        :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#96,s_store_id#97,s_rec_start_date#98,s_rec_end_date#99,s_closed_date_sk#100,s_store_name#101,s_number_employees#102,s_floor_space#103,s_hours#104,s_manager#105,s_market_id#106,s_geography_class#107,s_market_desc#108,s_market_manager#109,s_division_id#110,s_division_name#111,s_company_id#112,s_company_name#113,s_street_number#114,s_street_name#115,s_street_type#116,s_suite_number#117,s_city#118,s_county#119,... 5 more fields] parquet
               :        :     +- Project [i_item_sk#125]
               :        :        +- Filter ((isnotnull(i_current_price#130) AND (i_current_price#130 > 50.0)) AND isnotnull(i_item_sk#125))
               :        :           +- Relation spark_catalog.tpcds.item[i_item_sk#125,i_item_id#126,i_rec_start_date#127,i_rec_end_date#128,i_item_desc#129,i_current_price#130,i_wholesale_cost#131,i_brand_id#132,i_brand#133,i_class_id#134,i_class#135,i_category_id#136,i_category#137,i_manufact_id#138,i_manufact#139,i_size#140,i_formulation#141,i_color#142,i_units#143,i_container#144,i_manager_id#145,i_product_name#146] parquet
               :        +- Project [p_promo_sk#147]
               :           +- Filter ((isnotnull(p_channel_tv#158) AND (p_channel_tv#158 = N)) AND isnotnull(p_promo_sk#147))
               :              +- Relation spark_catalog.tpcds.promotion[p_promo_sk#147,p_promo_id#148,p_start_date_sk#149,p_end_date_sk#150,p_item_sk#151,p_cost#152,p_response_target#153,p_promo_name#154,p_channel_dmail#155,p_channel_email#156,p_channel_catalog#157,p_channel_tv#158,p_channel_radio#159,p_channel_press#160,p_channel_event#161,p_channel_demo#162,p_channel_details#163,p_purpose#164,p_discount_active#165] parquet
               :- Aggregate [cp_catalog_page_id#228], [sum(cs_ext_sales_price#189) AS sales#14, sum(coalesce(cr_return_amount#218, 0.0)) AS returns#15, sum((cs_net_profit#199 - coalesce(cr_net_loss#226, 0.0))) AS profit#16, catalog channel AS channel#493, concat(catalog_page, cp_catalog_page_id#228) AS id#494]
               :  +- Project [cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
               :     +- Join Inner, (cs_promo_sk#182 = p_promo_sk#370)
               :        :- Project [cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
               :        :  +- Join Inner, (cs_item_sk#181 = i_item_sk#348)
               :        :     :- Project [cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
               :        :     :  +- Join Inner, (cs_catalog_page_sk#178 = cp_catalog_page_sk#227)
               :        :     :     :- Project [cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226]
               :        :     :     :  +- Join Inner, (cs_sold_date_sk#166 = d_date_sk#320)
               :        :     :     :     :- Project [cs_sold_date_sk#166, cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226]
               :        :     :     :     :  +- Join LeftOuter, ((cs_item_sk#181 = cr_item_sk#202) AND (cs_order_number#183 = cr_order_number#216))
               :        :     :     :     :     :- Project [cs_sold_date_sk#166, cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_order_number#183, cs_ext_sales_price#189, cs_net_profit#199]
               :        :     :     :     :     :  +- Filter (((isnotnull(cs_sold_date_sk#166) AND isnotnull(cs_catalog_page_sk#178)) AND isnotnull(cs_item_sk#181)) AND isnotnull(cs_promo_sk#182))
               :        :     :     :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#166,cs_sold_time_sk#167,cs_ship_date_sk#168,cs_bill_customer_sk#169,cs_bill_cdemo_sk#170,cs_bill_hdemo_sk#171,cs_bill_addr_sk#172,cs_ship_customer_sk#173,cs_ship_cdemo_sk#174,cs_ship_hdemo_sk#175,cs_ship_addr_sk#176,cs_call_center_sk#177,cs_catalog_page_sk#178,cs_ship_mode_sk#179,cs_warehouse_sk#180,cs_item_sk#181,cs_promo_sk#182,cs_order_number#183,cs_quantity#184,cs_wholesale_cost#185,cs_list_price#186,cs_sales_price#187,cs_ext_discount_amt#188,cs_ext_sales_price#189,... 10 more fields] parquet
               :        :     :     :     :     +- Project [cr_item_sk#202, cr_order_number#216, cr_return_amount#218, cr_net_loss#226]
               :        :     :     :     :        +- Filter (isnotnull(cr_item_sk#202) AND isnotnull(cr_order_number#216))
               :        :     :     :     :           +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#200,cr_returned_time_sk#201,cr_item_sk#202,cr_refunded_customer_sk#203,cr_refunded_cdemo_sk#204,cr_refunded_hdemo_sk#205,cr_refunded_addr_sk#206,cr_returning_customer_sk#207,cr_returning_cdemo_sk#208,cr_returning_hdemo_sk#209,cr_returning_addr_sk#210,cr_call_center_sk#211,cr_catalog_page_sk#212,cr_ship_mode_sk#213,cr_warehouse_sk#214,cr_reason_sk#215,cr_order_number#216,cr_return_quantity#217,cr_return_amount#218,cr_return_tax#219,cr_return_amt_inc_tax#220,cr_fee#221,cr_return_ship_cost#222,cr_refunded_cash#223,... 3 more fields] parquet
               :        :     :     :     +- Project [d_date_sk#320]
               :        :     :     :        +- Filter ((isnotnull(d_date#322) AND ((cast(d_date#322 as date) >= 2000-08-23) AND (cast(d_date#322 as date) <= 2000-09-22))) AND isnotnull(d_date_sk#320))
               :        :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#320,d_date_id#321,d_date#322,d_month_seq#323,d_week_seq#324,d_quarter_seq#325,d_year#326,d_dow#327,d_moy#328,d_dom#329,d_qoy#330,d_fy_year#331,d_fy_quarter_seq#332,d_fy_week_seq#333,d_day_name#334,d_quarter_name#335,d_holiday#336,d_weekend#337,d_following_holiday#338,d_first_dom#339,d_last_dom#340,d_same_day_ly#341,d_same_day_lq#342,d_current_day#343,... 4 more fields] parquet
               :        :     :     +- Project [cp_catalog_page_sk#227, cp_catalog_page_id#228]
               :        :     :        +- Filter isnotnull(cp_catalog_page_sk#227)
               :        :     :           +- Relation spark_catalog.tpcds.catalog_page[cp_catalog_page_sk#227,cp_catalog_page_id#228,cp_start_date_sk#229,cp_end_date_sk#230,cp_department#231,cp_catalog_number#232,cp_catalog_page_number#233,cp_description#234,cp_type#235] parquet
               :        :     +- Project [i_item_sk#348]
               :        :        +- Filter ((isnotnull(i_current_price#353) AND (i_current_price#353 > 50.0)) AND isnotnull(i_item_sk#348))
               :        :           +- Relation spark_catalog.tpcds.item[i_item_sk#348,i_item_id#349,i_rec_start_date#350,i_rec_end_date#351,i_item_desc#352,i_current_price#353,i_wholesale_cost#354,i_brand_id#355,i_brand#356,i_class_id#357,i_class#358,i_category_id#359,i_category#360,i_manufact_id#361,i_manufact#362,i_size#363,i_formulation#364,i_color#365,i_units#366,i_container#367,i_manager_id#368,i_product_name#369] parquet
               :        +- Project [p_promo_sk#370]
               :           +- Filter ((isnotnull(p_channel_tv#381) AND (p_channel_tv#381 = N)) AND isnotnull(p_promo_sk#370))
               :              +- Relation spark_catalog.tpcds.promotion[p_promo_sk#370,p_promo_id#371,p_start_date_sk#372,p_end_date_sk#373,p_item_sk#374,p_cost#375,p_response_target#376,p_promo_name#377,p_channel_dmail#378,p_channel_email#379,p_channel_catalog#380,p_channel_tv#381,p_channel_radio#382,p_channel_press#383,p_channel_event#384,p_channel_demo#385,p_channel_details#386,p_purpose#387,p_discount_active#388] parquet
               +- Aggregate [web_site_id#295], [sum(ws_ext_sales_price#259) AS sales#17, sum(coalesce(wr_return_amt#285, 0.0)) AS returns#18, sum((ws_net_profit#269 - coalesce(wr_net_loss#293, 0.0))) AS profit#19, web channel AS channel#495, concat(web_site, web_site_id#295) AS id#496]
                  +- Project [ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293, web_site_id#295]
                     +- Join Inner, (ws_promo_sk#252 = p_promo_sk#439)
                        :- Project [ws_promo_sk#252, ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293, web_site_id#295]
                        :  +- Join Inner, (ws_item_sk#239 = i_item_sk#417)
                        :     :- Project [ws_item_sk#239, ws_promo_sk#252, ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293, web_site_id#295]
                        :     :  +- Join Inner, (ws_web_site_sk#249 = web_site_sk#294)
                        :     :     :- Project [ws_item_sk#239, ws_web_site_sk#249, ws_promo_sk#252, ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293]
                        :     :     :  +- Join Inner, (ws_sold_date_sk#236 = d_date_sk#389)
                        :     :     :     :- Project [ws_sold_date_sk#236, ws_item_sk#239, ws_web_site_sk#249, ws_promo_sk#252, ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293]
                        :     :     :     :  +- Join LeftOuter, ((ws_item_sk#239 = wr_item_sk#272) AND (ws_order_number#253 = wr_order_number#283))
                        :     :     :     :     :- Project [ws_sold_date_sk#236, ws_item_sk#239, ws_web_site_sk#249, ws_promo_sk#252, ws_order_number#253, ws_ext_sales_price#259, ws_net_profit#269]
                        :     :     :     :     :  +- Filter (((isnotnull(ws_sold_date_sk#236) AND isnotnull(ws_web_site_sk#249)) AND isnotnull(ws_item_sk#239)) AND isnotnull(ws_promo_sk#252))
                        :     :     :     :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#236,ws_sold_time_sk#237,ws_ship_date_sk#238,ws_item_sk#239,ws_bill_customer_sk#240,ws_bill_cdemo_sk#241,ws_bill_hdemo_sk#242,ws_bill_addr_sk#243,ws_ship_customer_sk#244,ws_ship_cdemo_sk#245,ws_ship_hdemo_sk#246,ws_ship_addr_sk#247,ws_web_page_sk#248,ws_web_site_sk#249,ws_ship_mode_sk#250,ws_warehouse_sk#251,ws_promo_sk#252,ws_order_number#253,ws_quantity#254,ws_wholesale_cost#255,ws_list_price#256,ws_sales_price#257,ws_ext_discount_amt#258,ws_ext_sales_price#259,... 10 more fields] parquet
                        :     :     :     :     +- Project [wr_item_sk#272, wr_order_number#283, wr_return_amt#285, wr_net_loss#293]
                        :     :     :     :        +- Filter (isnotnull(wr_item_sk#272) AND isnotnull(wr_order_number#283))
                        :     :     :     :           +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#270,wr_returned_time_sk#271,wr_item_sk#272,wr_refunded_customer_sk#273,wr_refunded_cdemo_sk#274,wr_refunded_hdemo_sk#275,wr_refunded_addr_sk#276,wr_returning_customer_sk#277,wr_returning_cdemo_sk#278,wr_returning_hdemo_sk#279,wr_returning_addr_sk#280,wr_web_page_sk#281,wr_reason_sk#282,wr_order_number#283,wr_return_quantity#284,wr_return_amt#285,wr_return_tax#286,wr_return_amt_inc_tax#287,wr_fee#288,wr_return_ship_cost#289,wr_refunded_cash#290,wr_reversed_charge#291,wr_account_credit#292,wr_net_loss#293] parquet
                        :     :     :     +- Project [d_date_sk#389]
                        :     :     :        +- Filter ((isnotnull(d_date#391) AND ((cast(d_date#391 as date) >= 2000-08-23) AND (cast(d_date#391 as date) <= 2000-09-22))) AND isnotnull(d_date_sk#389))
                        :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#389,d_date_id#390,d_date#391,d_month_seq#392,d_week_seq#393,d_quarter_seq#394,d_year#395,d_dow#396,d_moy#397,d_dom#398,d_qoy#399,d_fy_year#400,d_fy_quarter_seq#401,d_fy_week_seq#402,d_day_name#403,d_quarter_name#404,d_holiday#405,d_weekend#406,d_following_holiday#407,d_first_dom#408,d_last_dom#409,d_same_day_ly#410,d_same_day_lq#411,d_current_day#412,... 4 more fields] parquet
                        :     :     +- Project [web_site_sk#294, web_site_id#295]
                        :     :        +- Filter isnotnull(web_site_sk#294)
                        :     :           +- Relation spark_catalog.tpcds.web_site[web_site_sk#294,web_site_id#295,web_rec_start_date#296,web_rec_end_date#297,web_name#298,web_open_date_sk#299,web_close_date_sk#300,web_class#301,web_manager#302,web_mkt_id#303,web_mkt_class#304,web_mkt_desc#305,web_market_manager#306,web_company_id#307,web_company_name#308,web_street_number#309,web_street_name#310,web_street_type#311,web_suite_number#312,web_city#313,web_county#314,web_state#315,web_zip#316,web_country#317,... 2 more fields] parquet
                        :     +- Project [i_item_sk#417]
                        :        +- Filter ((isnotnull(i_current_price#422) AND (i_current_price#422 > 50.0)) AND isnotnull(i_item_sk#417))
                        :           +- Relation spark_catalog.tpcds.item[i_item_sk#417,i_item_id#418,i_rec_start_date#419,i_rec_end_date#420,i_item_desc#421,i_current_price#422,i_wholesale_cost#423,i_brand_id#424,i_brand#425,i_class_id#426,i_class#427,i_category_id#428,i_category#429,i_manufact_id#430,i_manufact#431,i_size#432,i_formulation#433,i_color#434,i_units#435,i_container#436,i_manager_id#437,i_product_name#438] parquet
                        +- Project [p_promo_sk#439]
                           +- Filter ((isnotnull(p_channel_tv#450) AND (p_channel_tv#450 = N)) AND isnotnull(p_promo_sk#439))
                              +- Relation spark_catalog.tpcds.promotion[p_promo_sk#439,p_promo_id#440,p_start_date_sk#441,p_end_date_sk#442,p_item_sk#443,p_cost#444,p_response_target#445,p_promo_name#446,p_channel_dmail#447,p_channel_email#448,p_channel_catalog#449,p_channel_tv#450,p_channel_radio#451,p_channel_press#452,p_channel_event#453,p_channel_demo#454,p_channel_details#455,p_purpose#456,p_discount_active#457] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[channel#491 ASC NULLS FIRST,id#492 ASC NULLS FIRST], output=[channel#491,id#492,sales#6,returns#7,profit#8])
   +- HashAggregate(keys=[channel#491, id#492, spark_grouping_id#490L], functions=[sum(sales#10), sum(returns#11), sum(profit#12)], output=[channel#491, id#492, sales#6, returns#7, profit#8])
      +- Exchange hashpartitioning(channel#491, id#492, spark_grouping_id#490L, 200), ENSURE_REQUIREMENTS, [plan_id=423]
         +- HashAggregate(keys=[channel#491, id#492, spark_grouping_id#490L], functions=[partial_sum(sales#10), partial_sum(returns#11), partial_sum(profit#12)], output=[channel#491, id#492, spark_grouping_id#490L, sum#500, sum#501, sum#502])
            +- Expand [[sales#10, returns#11, profit#12, channel#488, id#489, 0], [sales#10, returns#11, profit#12, channel#488, null, 1], [sales#10, returns#11, profit#12, null, null, 3]], [sales#10, returns#11, profit#12, channel#491, id#492, spark_grouping_id#490L]
               +- Union
                  :- HashAggregate(keys=[s_store_id#97], functions=[sum(ss_ext_sales_price#40), sum(coalesce(sr_return_amt#59, 0.0)), sum((ss_net_profit#47 - coalesce(sr_net_loss#67, 0.0)))], output=[sales#10, returns#11, profit#12, channel#488, id#489])
                  :  +- Exchange hashpartitioning(s_store_id#97, 200), ENSURE_REQUIREMENTS, [plan_id=371]
                  :     +- HashAggregate(keys=[s_store_id#97], functions=[partial_sum(ss_ext_sales_price#40), partial_sum(coalesce(sr_return_amt#59, 0.0)), partial_sum((ss_net_profit#47 - coalesce(sr_net_loss#67, 0.0)))], output=[s_store_id#97, sum#506, sum#507, sum#508])
                  :        +- Project [ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67, s_store_id#97]
                  :           +- BroadcastHashJoin [ss_promo_sk#33], [p_promo_sk#147], Inner, BuildRight, false
                  :              :- Project [ss_promo_sk#33, ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67, s_store_id#97]
                  :              :  +- BroadcastHashJoin [ss_item_sk#27], [i_item_sk#125], Inner, BuildRight, false
                  :              :     :- Project [ss_item_sk#27, ss_promo_sk#33, ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67, s_store_id#97]
                  :              :     :  +- BroadcastHashJoin [ss_store_sk#32], [s_store_sk#96], Inner, BuildRight, false
                  :              :     :     :- Project [ss_item_sk#27, ss_store_sk#32, ss_promo_sk#33, ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67]
                  :              :     :     :  +- BroadcastHashJoin [ss_sold_date_sk#25], [d_date_sk#68], Inner, BuildRight, false
                  :              :     :     :     :- Project [ss_sold_date_sk#25, ss_item_sk#27, ss_store_sk#32, ss_promo_sk#33, ss_ext_sales_price#40, ss_net_profit#47, sr_return_amt#59, sr_net_loss#67]
                  :              :     :     :     :  +- BroadcastHashJoin [ss_item_sk#27, ss_ticket_number#34], [sr_item_sk#50, sr_ticket_number#57], LeftOuter, BuildRight, false
                  :              :     :     :     :     :- Filter (((isnotnull(ss_sold_date_sk#25) AND isnotnull(ss_store_sk#32)) AND isnotnull(ss_item_sk#27)) AND isnotnull(ss_promo_sk#33))
                  :              :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#25,ss_item_sk#27,ss_store_sk#32,ss_promo_sk#33,ss_ticket_number#34,ss_ext_sales_price#40,ss_net_profit#47] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#25), isnotnull(ss_store_sk#32), isnotnull(ss_item_sk#27), isnotnull(ss..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk), IsNotNull(ss_item_sk), IsNotNull(ss_promo_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_promo_sk:int,ss_ticket_number:int,ss...
                  :              :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, false] as bigint), 32) | (cast(input[1, int, false] as bigint) & 4294967295))),false), [plan_id=350]
                  :              :     :     :     :        +- Filter (isnotnull(sr_item_sk#50) AND isnotnull(sr_ticket_number#57))
                  :              :     :     :     :           +- FileScan parquet spark_catalog.tpcds.store_returns[sr_item_sk#50,sr_ticket_number#57,sr_return_amt#59,sr_net_loss#67] Batched: true, DataFilters: [isnotnull(sr_item_sk#50), isnotnull(sr_ticket_number#57)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_ticket_number)], ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int,sr_return_amt:double,sr_net_loss:double>
                  :              :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=354]
                  :              :     :     :        +- Project [d_date_sk#68]
                  :              :     :     :           +- Filter (((isnotnull(d_date#70) AND (cast(d_date#70 as date) >= 2000-08-23)) AND (cast(d_date#70 as date) <= 2000-09-22)) AND isnotnull(d_date_sk#68))
                  :              :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#68,d_date#70] Batched: true, DataFilters: [isnotnull(d_date#70), (cast(d_date#70 as date) >= 2000-08-23), (cast(d_date#70 as date) <= 2000-..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=358]
                  :              :     :        +- Filter isnotnull(s_store_sk#96)
                  :              :     :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#96,s_store_id#97] Batched: true, DataFilters: [isnotnull(s_store_sk#96)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_id:string>
                  :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=362]
                  :              :        +- Project [i_item_sk#125]
                  :              :           +- Filter ((isnotnull(i_current_price#130) AND (i_current_price#130 > 50.0)) AND isnotnull(i_item_sk#125))
                  :              :              +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#125,i_current_price#130] Batched: true, DataFilters: [isnotnull(i_current_price#130), (i_current_price#130 > 50.0), isnotnull(i_item_sk#125)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThan(i_current_price,50.0), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double>
                  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=366]
                  :                 +- Project [p_promo_sk#147]
                  :                    +- Filter ((isnotnull(p_channel_tv#158) AND (p_channel_tv#158 = N)) AND isnotnull(p_promo_sk#147))
                  :                       +- FileScan parquet spark_catalog.tpcds.promotion[p_promo_sk#147,p_channel_tv#158] Batched: true, DataFilters: [isnotnull(p_channel_tv#158), (p_channel_tv#158 = N), isnotnull(p_promo_sk#147)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(p_channel_tv), EqualTo(p_channel_tv,N), IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int,p_channel_tv:string>
                  :- HashAggregate(keys=[cp_catalog_page_id#228], functions=[sum(cs_ext_sales_price#189), sum(coalesce(cr_return_amount#218, 0.0)), sum((cs_net_profit#199 - coalesce(cr_net_loss#226, 0.0)))], output=[sales#14, returns#15, profit#16, channel#493, id#494])
                  :  +- Exchange hashpartitioning(cp_catalog_page_id#228, 200), ENSURE_REQUIREMENTS, [plan_id=394]
                  :     +- HashAggregate(keys=[cp_catalog_page_id#228], functions=[partial_sum(cs_ext_sales_price#189), partial_sum(coalesce(cr_return_amount#218, 0.0)), partial_sum((cs_net_profit#199 - coalesce(cr_net_loss#226, 0.0)))], output=[cp_catalog_page_id#228, sum#512, sum#513, sum#514])
                  :        +- Project [cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
                  :           +- BroadcastHashJoin [cs_promo_sk#182], [p_promo_sk#370], Inner, BuildRight, false
                  :              :- Project [cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
                  :              :  +- BroadcastHashJoin [cs_item_sk#181], [i_item_sk#348], Inner, BuildRight, false
                  :              :     :- Project [cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226, cp_catalog_page_id#228]
                  :              :     :  +- BroadcastHashJoin [cs_catalog_page_sk#178], [cp_catalog_page_sk#227], Inner, BuildRight, false
                  :              :     :     :- Project [cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226]
                  :              :     :     :  +- BroadcastHashJoin [cs_sold_date_sk#166], [d_date_sk#320], Inner, BuildRight, false
                  :              :     :     :     :- Project [cs_sold_date_sk#166, cs_catalog_page_sk#178, cs_item_sk#181, cs_promo_sk#182, cs_ext_sales_price#189, cs_net_profit#199, cr_return_amount#218, cr_net_loss#226]
                  :              :     :     :     :  +- BroadcastHashJoin [cs_item_sk#181, cs_order_number#183], [cr_item_sk#202, cr_order_number#216], LeftOuter, BuildRight, false
                  :              :     :     :     :     :- Filter (((isnotnull(cs_sold_date_sk#166) AND isnotnull(cs_catalog_page_sk#178)) AND isnotnull(cs_item_sk#181)) AND isnotnull(cs_promo_sk#182))
                  :              :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#166,cs_catalog_page_sk#178,cs_item_sk#181,cs_promo_sk#182,cs_order_number#183,cs_ext_sales_price#189,cs_net_profit#199] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#166), isnotnull(cs_catalog_page_sk#178), isnotnull(cs_item_sk#181), is..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_catalog_page_sk), IsNotNull(cs_item_sk), IsNotNull(cs_p..., ReadSchema: struct<cs_sold_date_sk:int,cs_catalog_page_sk:int,cs_item_sk:int,cs_promo_sk:int,cs_order_number:...
                  :              :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, false] as bigint), 32) | (cast(input[1, int, false] as bigint) & 4294967295))),false), [plan_id=373]
                  :              :     :     :     :        +- Filter (isnotnull(cr_item_sk#202) AND isnotnull(cr_order_number#216))
                  :              :     :     :     :           +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_item_sk#202,cr_order_number#216,cr_return_amount#218,cr_net_loss#226] Batched: true, DataFilters: [isnotnull(cr_item_sk#202), isnotnull(cr_order_number#216)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_item_sk), IsNotNull(cr_order_number)], ReadSchema: struct<cr_item_sk:int,cr_order_number:int,cr_return_amount:double,cr_net_loss:double>
                  :              :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=377]
                  :              :     :     :        +- Project [d_date_sk#320]
                  :              :     :     :           +- Filter (((isnotnull(d_date#322) AND (cast(d_date#322 as date) >= 2000-08-23)) AND (cast(d_date#322 as date) <= 2000-09-22)) AND isnotnull(d_date_sk#320))
                  :              :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#320,d_date#322] Batched: true, DataFilters: [isnotnull(d_date#322), (cast(d_date#322 as date) >= 2000-08-23), (cast(d_date#322 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=381]
                  :              :     :        +- Filter isnotnull(cp_catalog_page_sk#227)
                  :              :     :           +- FileScan parquet spark_catalog.tpcds.catalog_page[cp_catalog_page_sk#227,cp_catalog_page_id#228] Batched: true, DataFilters: [isnotnull(cp_catalog_page_sk#227)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cp_catalog_page_sk)], ReadSchema: struct<cp_catalog_page_sk:int,cp_catalog_page_id:string>
                  :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=385]
                  :              :        +- Project [i_item_sk#348]
                  :              :           +- Filter ((isnotnull(i_current_price#353) AND (i_current_price#353 > 50.0)) AND isnotnull(i_item_sk#348))
                  :              :              +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#348,i_current_price#353] Batched: true, DataFilters: [isnotnull(i_current_price#353), (i_current_price#353 > 50.0), isnotnull(i_item_sk#348)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThan(i_current_price,50.0), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double>
                  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=389]
                  :                 +- Project [p_promo_sk#370]
                  :                    +- Filter ((isnotnull(p_channel_tv#381) AND (p_channel_tv#381 = N)) AND isnotnull(p_promo_sk#370))
                  :                       +- FileScan parquet spark_catalog.tpcds.promotion[p_promo_sk#370,p_channel_tv#381] Batched: true, DataFilters: [isnotnull(p_channel_tv#381), (p_channel_tv#381 = N), isnotnull(p_promo_sk#370)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(p_channel_tv), EqualTo(p_channel_tv,N), IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int,p_channel_tv:string>
                  +- HashAggregate(keys=[web_site_id#295], functions=[sum(ws_ext_sales_price#259), sum(coalesce(wr_return_amt#285, 0.0)), sum((ws_net_profit#269 - coalesce(wr_net_loss#293, 0.0)))], output=[sales#17, returns#18, profit#19, channel#495, id#496])
                     +- Exchange hashpartitioning(web_site_id#295, 200), ENSURE_REQUIREMENTS, [plan_id=417]
                        +- HashAggregate(keys=[web_site_id#295], functions=[partial_sum(ws_ext_sales_price#259), partial_sum(coalesce(wr_return_amt#285, 0.0)), partial_sum((ws_net_profit#269 - coalesce(wr_net_loss#293, 0.0)))], output=[web_site_id#295, sum#518, sum#519, sum#520])
                           +- Project [ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293, web_site_id#295]
                              +- BroadcastHashJoin [ws_promo_sk#252], [p_promo_sk#439], Inner, BuildRight, false
                                 :- Project [ws_promo_sk#252, ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293, web_site_id#295]
                                 :  +- BroadcastHashJoin [ws_item_sk#239], [i_item_sk#417], Inner, BuildRight, false
                                 :     :- Project [ws_item_sk#239, ws_promo_sk#252, ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293, web_site_id#295]
                                 :     :  +- BroadcastHashJoin [ws_web_site_sk#249], [web_site_sk#294], Inner, BuildRight, false
                                 :     :     :- Project [ws_item_sk#239, ws_web_site_sk#249, ws_promo_sk#252, ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293]
                                 :     :     :  +- BroadcastHashJoin [ws_sold_date_sk#236], [d_date_sk#389], Inner, BuildRight, false
                                 :     :     :     :- Project [ws_sold_date_sk#236, ws_item_sk#239, ws_web_site_sk#249, ws_promo_sk#252, ws_ext_sales_price#259, ws_net_profit#269, wr_return_amt#285, wr_net_loss#293]
                                 :     :     :     :  +- BroadcastHashJoin [ws_item_sk#239, ws_order_number#253], [wr_item_sk#272, wr_order_number#283], LeftOuter, BuildRight, false
                                 :     :     :     :     :- Filter (((isnotnull(ws_sold_date_sk#236) AND isnotnull(ws_web_site_sk#249)) AND isnotnull(ws_item_sk#239)) AND isnotnull(ws_promo_sk#252))
                                 :     :     :     :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#236,ws_item_sk#239,ws_web_site_sk#249,ws_promo_sk#252,ws_order_number#253,ws_ext_sales_price#259,ws_net_profit#269] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#236), isnotnull(ws_web_site_sk#249), isnotnull(ws_item_sk#239), isnotn..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_web_site_sk), IsNotNull(ws_item_sk), IsNotNull(ws_promo..., ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_web_site_sk:int,ws_promo_sk:int,ws_order_number:int,...
                                 :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, false] as bigint), 32) | (cast(input[1, int, false] as bigint) & 4294967295))),false), [plan_id=396]
                                 :     :     :     :        +- Filter (isnotnull(wr_item_sk#272) AND isnotnull(wr_order_number#283))
                                 :     :     :     :           +- FileScan parquet spark_catalog.tpcds.web_returns[wr_item_sk#272,wr_order_number#283,wr_return_amt#285,wr_net_loss#293] Batched: true, DataFilters: [isnotnull(wr_item_sk#272), isnotnull(wr_order_number#283)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_item_sk), IsNotNull(wr_order_number)], ReadSchema: struct<wr_item_sk:int,wr_order_number:int,wr_return_amt:double,wr_net_loss:double>
                                 :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=400]
                                 :     :     :        +- Project [d_date_sk#389]
                                 :     :     :           +- Filter (((isnotnull(d_date#391) AND (cast(d_date#391 as date) >= 2000-08-23)) AND (cast(d_date#391 as date) <= 2000-09-22)) AND isnotnull(d_date_sk#389))
                                 :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#389,d_date#391] Batched: true, DataFilters: [isnotnull(d_date#391), (cast(d_date#391 as date) >= 2000-08-23), (cast(d_date#391 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                                 :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=404]
                                 :     :        +- Filter isnotnull(web_site_sk#294)
                                 :     :           +- FileScan parquet spark_catalog.tpcds.web_site[web_site_sk#294,web_site_id#295] Batched: true, DataFilters: [isnotnull(web_site_sk#294)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(web_site_sk)], ReadSchema: struct<web_site_sk:int,web_site_id:string>
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=408]
                                 :        +- Project [i_item_sk#417]
                                 :           +- Filter ((isnotnull(i_current_price#422) AND (i_current_price#422 > 50.0)) AND isnotnull(i_item_sk#417))
                                 :              +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#417,i_current_price#422] Batched: true, DataFilters: [isnotnull(i_current_price#422), (i_current_price#422 > 50.0), isnotnull(i_item_sk#417)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), GreaterThan(i_current_price,50.0), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=412]
                                    +- Project [p_promo_sk#439]
                                       +- Filter ((isnotnull(p_channel_tv#450) AND (p_channel_tv#450 = N)) AND isnotnull(p_promo_sk#439))
                                          +- FileScan parquet spark_catalog.tpcds.promotion[p_promo_sk#439,p_channel_tv#450] Batched: true, DataFilters: [isnotnull(p_channel_tv#450), (p_channel_tv#450 = N), isnotnull(p_promo_sk#439)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(p_channel_tv), EqualTo(p_channel_tv,N), IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int,p_channel_tv:string>

Time taken: 5.257 seconds, Fetched 1 row(s)
