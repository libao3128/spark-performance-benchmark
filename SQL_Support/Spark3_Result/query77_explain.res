Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582523900
== Parsed Logical Plan ==
CTE [ss, sr, cs, cr, ws, wr]
:  :- 'SubqueryAlias ss
:  :  +- 'Aggregate ['s_store_sk], ['s_store_sk, 'sum('ss_ext_sales_price) AS sales#14, 'sum('ss_net_profit) AS profit#15]
:  :     +- 'Filter ((('ss_sold_date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) AND ('ss_store_sk = 's_store_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation [store_sales], [], false
:  :           :  +- 'UnresolvedRelation [date_dim], [], false
:  :           +- 'UnresolvedRelation [store], [], false
:  :- 'SubqueryAlias sr
:  :  +- 'Aggregate ['s_store_sk], ['s_store_sk, 'sum('sr_return_amt) AS returns#16, 'sum('sr_net_loss) AS profit_loss#17]
:  :     +- 'Filter ((('sr_returned_date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) AND ('sr_store_sk = 's_store_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation [store_returns], [], false
:  :           :  +- 'UnresolvedRelation [date_dim], [], false
:  :           +- 'UnresolvedRelation [store], [], false
:  :- 'SubqueryAlias cs
:  :  +- 'Aggregate ['cs_call_center_sk], ['cs_call_center_sk, 'sum('cs_ext_sales_price) AS sales#18, 'sum('cs_net_profit) AS profit#19]
:  :     +- 'Filter (('cs_sold_date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 30))))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation [catalog_sales], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  :- 'SubqueryAlias cr
:  :  +- 'Aggregate ['cr_call_center_sk], ['cr_call_center_sk, 'sum('cr_return_amount) AS returns#20, 'sum('cr_net_loss) AS profit_loss#21]
:  :     +- 'Filter (('cr_returned_date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 30))))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation [catalog_returns], [], false
:  :           +- 'UnresolvedRelation [date_dim], [], false
:  :- 'SubqueryAlias ws
:  :  +- 'Aggregate ['wp_web_page_sk], ['wp_web_page_sk, 'sum('ws_ext_sales_price) AS sales#22, 'sum('ws_net_profit) AS profit#23]
:  :     +- 'Filter ((('ws_sold_date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) AND ('ws_web_page_sk = 'wp_web_page_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation [web_sales], [], false
:  :           :  +- 'UnresolvedRelation [date_dim], [], false
:  :           +- 'UnresolvedRelation [web_page], [], false
:  +- 'SubqueryAlias wr
:     +- 'Aggregate ['wp_web_page_sk], ['wp_web_page_sk, 'sum('wr_return_amt) AS returns#24, 'sum('wr_net_loss) AS profit_loss#25]
:        +- 'Filter ((('wr_returned_date_sk = 'd_date_sk) AND (('d_date >= cast(2000-08-23 as date)) AND ('d_date <= 'date_add(cast(2000-08-23 as date), 30)))) AND ('wr_web_page_sk = 'wp_web_page_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation [web_returns], [], false
:              :  +- 'UnresolvedRelation [date_dim], [], false
:              +- 'UnresolvedRelation [web_page], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['channel ASC NULLS FIRST, 'id ASC NULLS FIRST], true
         +- 'Aggregate [rollup(Vector(0), Vector(1), 'channel, 'id)], ['channel, 'id, 'sum('sales) AS sales#11, 'sum('returns) AS returns#12, 'sum('profit) AS profit#13]
            +- 'SubqueryAlias x
               +- 'Union false, false
                  :- 'Union false, false
                  :  :- 'Project [store channel AS channel#0, 'ss.s_store_sk AS id#1, 'sales, 'coalesce('returns, 0) AS returns#2, ('profit - 'coalesce('profit_loss, 0)) AS profit#3]
                  :  :  +- 'Join LeftOuter, ('ss.s_store_sk = 'sr.s_store_sk)
                  :  :     :- 'UnresolvedRelation [ss], [], false
                  :  :     +- 'UnresolvedRelation [sr], [], false
                  :  +- 'Project [catalog channel AS channel#4, 'cs_call_center_sk AS id#5, 'sales, 'returns, ('profit - 'profit_loss) AS profit#6]
                  :     +- 'Join Inner
                  :        :- 'UnresolvedRelation [cs], [], false
                  :        +- 'UnresolvedRelation [cr], [], false
                  +- 'Project [web channel AS channel#7, 'ws.wp_web_page_sk AS id#8, 'sales, 'coalesce('returns, 0) AS returns#9, ('profit - 'coalesce('profit_loss, 0)) AS profit#10]
                     +- 'Join LeftOuter, ('ws.wp_web_page_sk = 'wr.wp_web_page_sk)
                        :- 'UnresolvedRelation [ws], [], false
                        +- 'UnresolvedRelation [wr], [], false

== Analyzed Logical Plan ==
channel: string, id: int, sales: double, returns: double, profit: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ss
:     +- Aggregate [s_store_sk#82], [s_store_sk#82, sum(ss_ext_sales_price#46) AS sales#14, sum(ss_net_profit#53) AS profit#15]
:        +- Filter (((ss_sold_date_sk#31 = d_date_sk#54) AND ((cast(d_date#56 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#56 as date) <= date_add(cast(2000-08-23 as date), 30)))) AND (ss_store_sk#38 = s_store_sk#82))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#31,ss_sold_time_sk#32,ss_item_sk#33,ss_customer_sk#34,ss_cdemo_sk#35,ss_hdemo_sk#36,ss_addr_sk#37,ss_store_sk#38,ss_promo_sk#39,ss_ticket_number#40,ss_quantity#41,ss_wholesale_cost#42,ss_list_price#43,ss_sales_price#44,ss_ext_discount_amt#45,ss_ext_sales_price#46,ss_ext_wholesale_cost#47,ss_ext_list_price#48,ss_ext_tax#49,ss_coupon_amt#50,ss_net_paid#51,ss_net_paid_inc_tax#52,ss_net_profit#53] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#54,d_date_id#55,d_date#56,d_month_seq#57,d_week_seq#58,d_quarter_seq#59,d_year#60,d_dow#61,d_moy#62,d_dom#63,d_qoy#64,d_fy_year#65,d_fy_quarter_seq#66,d_fy_week_seq#67,d_day_name#68,d_quarter_name#69,d_holiday#70,d_weekend#71,d_following_holiday#72,d_first_dom#73,d_last_dom#74,d_same_day_ly#75,d_same_day_lq#76,d_current_day#77,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.store
:                 +- Relation spark_catalog.tpcds.store[s_store_sk#82,s_store_id#83,s_rec_start_date#84,s_rec_end_date#85,s_closed_date_sk#86,s_store_name#87,s_number_employees#88,s_floor_space#89,s_hours#90,s_manager#91,s_market_id#92,s_geography_class#93,s_market_desc#94,s_market_manager#95,s_division_id#96,s_division_name#97,s_company_id#98,s_company_name#99,s_street_number#100,s_street_name#101,s_street_type#102,s_suite_number#103,s_city#104,s_county#105,... 5 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias sr
:     +- Aggregate [s_store_sk#292], [s_store_sk#292, sum(sr_return_amt#122) AS returns#16, sum(sr_net_loss#130) AS profit_loss#17]
:        +- Filter (((sr_returned_date_sk#111 = d_date_sk#264) AND ((cast(d_date#266 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#266 as date) <= date_add(cast(2000-08-23 as date), 30)))) AND (sr_store_sk#118 = s_store_sk#292))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.store_returns
:              :  :  +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#111,sr_return_time_sk#112,sr_item_sk#113,sr_customer_sk#114,sr_cdemo_sk#115,sr_hdemo_sk#116,sr_addr_sk#117,sr_store_sk#118,sr_reason_sk#119,sr_ticket_number#120,sr_return_quantity#121,sr_return_amt#122,sr_return_tax#123,sr_return_amt_inc_tax#124,sr_fee#125,sr_return_ship_cost#126,sr_refunded_cash#127,sr_reversed_charge#128,sr_store_credit#129,sr_net_loss#130] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#264,d_date_id#265,d_date#266,d_month_seq#267,d_week_seq#268,d_quarter_seq#269,d_year#270,d_dow#271,d_moy#272,d_dom#273,d_qoy#274,d_fy_year#275,d_fy_quarter_seq#276,d_fy_week_seq#277,d_day_name#278,d_quarter_name#279,d_holiday#280,d_weekend#281,d_following_holiday#282,d_first_dom#283,d_last_dom#284,d_same_day_ly#285,d_same_day_lq#286,d_current_day#287,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.store
:                 +- Relation spark_catalog.tpcds.store[s_store_sk#292,s_store_id#293,s_rec_start_date#294,s_rec_end_date#295,s_closed_date_sk#296,s_store_name#297,s_number_employees#298,s_floor_space#299,s_hours#300,s_manager#301,s_market_id#302,s_geography_class#303,s_market_desc#304,s_market_manager#305,s_division_id#306,s_division_name#307,s_company_id#308,s_company_name#309,s_street_number#310,s_street_name#311,s_street_type#312,s_suite_number#313,s_city#314,s_county#315,... 5 more fields] parquet
:- CTERelationDef 2, false
:  +- SubqueryAlias cs
:     +- Aggregate [cs_call_center_sk#142], [cs_call_center_sk#142, sum(cs_ext_sales_price#154) AS sales#18, sum(cs_net_profit#164) AS profit#19]
:        +- Filter ((cs_sold_date_sk#131 = d_date_sk#321) AND ((cast(d_date#323 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#323 as date) <= date_add(cast(2000-08-23 as date), 30))))
:           +- Join Inner
:              :- SubqueryAlias spark_catalog.tpcds.catalog_sales
:              :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#131,cs_sold_time_sk#132,cs_ship_date_sk#133,cs_bill_customer_sk#134,cs_bill_cdemo_sk#135,cs_bill_hdemo_sk#136,cs_bill_addr_sk#137,cs_ship_customer_sk#138,cs_ship_cdemo_sk#139,cs_ship_hdemo_sk#140,cs_ship_addr_sk#141,cs_call_center_sk#142,cs_catalog_page_sk#143,cs_ship_mode_sk#144,cs_warehouse_sk#145,cs_item_sk#146,cs_promo_sk#147,cs_order_number#148,cs_quantity#149,cs_wholesale_cost#150,cs_list_price#151,cs_sales_price#152,cs_ext_discount_amt#153,cs_ext_sales_price#154,... 10 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#321,d_date_id#322,d_date#323,d_month_seq#324,d_week_seq#325,d_quarter_seq#326,d_year#327,d_dow#328,d_moy#329,d_dom#330,d_qoy#331,d_fy_year#332,d_fy_quarter_seq#333,d_fy_week_seq#334,d_day_name#335,d_quarter_name#336,d_holiday#337,d_weekend#338,d_following_holiday#339,d_first_dom#340,d_last_dom#341,d_same_day_ly#342,d_same_day_lq#343,d_current_day#344,... 4 more fields] parquet
:- CTERelationDef 3, false
:  +- SubqueryAlias cr
:     +- Aggregate [cr_call_center_sk#176], [cr_call_center_sk#176, sum(cr_return_amount#183) AS returns#20, sum(cr_net_loss#191) AS profit_loss#21]
:        +- Filter ((cr_returned_date_sk#165 = d_date_sk#349) AND ((cast(d_date#351 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#351 as date) <= date_add(cast(2000-08-23 as date), 30))))
:           +- Join Inner
:              :- SubqueryAlias spark_catalog.tpcds.catalog_returns
:              :  +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#165,cr_returned_time_sk#166,cr_item_sk#167,cr_refunded_customer_sk#168,cr_refunded_cdemo_sk#169,cr_refunded_hdemo_sk#170,cr_refunded_addr_sk#171,cr_returning_customer_sk#172,cr_returning_cdemo_sk#173,cr_returning_hdemo_sk#174,cr_returning_addr_sk#175,cr_call_center_sk#176,cr_catalog_page_sk#177,cr_ship_mode_sk#178,cr_warehouse_sk#179,cr_reason_sk#180,cr_order_number#181,cr_return_quantity#182,cr_return_amount#183,cr_return_tax#184,cr_return_amt_inc_tax#185,cr_fee#186,cr_return_ship_cost#187,cr_refunded_cash#188,... 3 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#349,d_date_id#350,d_date#351,d_month_seq#352,d_week_seq#353,d_quarter_seq#354,d_year#355,d_dow#356,d_moy#357,d_dom#358,d_qoy#359,d_fy_year#360,d_fy_quarter_seq#361,d_fy_week_seq#362,d_day_name#363,d_quarter_name#364,d_holiday#365,d_weekend#366,d_following_holiday#367,d_first_dom#368,d_last_dom#369,d_same_day_ly#370,d_same_day_lq#371,d_current_day#372,... 4 more fields] parquet
:- CTERelationDef 4, false
:  +- SubqueryAlias ws
:     +- Aggregate [wp_web_page_sk#226], [wp_web_page_sk#226, sum(ws_ext_sales_price#215) AS sales#22, sum(ws_net_profit#225) AS profit#23]
:        +- Filter (((ws_sold_date_sk#192 = d_date_sk#377) AND ((cast(d_date#379 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#379 as date) <= date_add(cast(2000-08-23 as date), 30)))) AND (ws_web_page_sk#204 = wp_web_page_sk#226))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.web_sales
:              :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#192,ws_sold_time_sk#193,ws_ship_date_sk#194,ws_item_sk#195,ws_bill_customer_sk#196,ws_bill_cdemo_sk#197,ws_bill_hdemo_sk#198,ws_bill_addr_sk#199,ws_ship_customer_sk#200,ws_ship_cdemo_sk#201,ws_ship_hdemo_sk#202,ws_ship_addr_sk#203,ws_web_page_sk#204,ws_web_site_sk#205,ws_ship_mode_sk#206,ws_warehouse_sk#207,ws_promo_sk#208,ws_order_number#209,ws_quantity#210,ws_wholesale_cost#211,ws_list_price#212,ws_sales_price#213,ws_ext_discount_amt#214,ws_ext_sales_price#215,... 10 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#377,d_date_id#378,d_date#379,d_month_seq#380,d_week_seq#381,d_quarter_seq#382,d_year#383,d_dow#384,d_moy#385,d_dom#386,d_qoy#387,d_fy_year#388,d_fy_quarter_seq#389,d_fy_week_seq#390,d_day_name#391,d_quarter_name#392,d_holiday#393,d_weekend#394,d_following_holiday#395,d_first_dom#396,d_last_dom#397,d_same_day_ly#398,d_same_day_lq#399,d_current_day#400,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.web_page
:                 +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#226,wp_web_page_id#227,wp_rec_start_date#228,wp_rec_end_date#229,wp_creation_date_sk#230,wp_access_date_sk#231,wp_autogen_flag#232,wp_customer_sk#233,wp_url#234,wp_type#235,wp_char_count#236,wp_link_count#237,wp_image_count#238,wp_max_ad_count#239] parquet
:- CTERelationDef 5, false
:  +- SubqueryAlias wr
:     +- Aggregate [wp_web_page_sk#433], [wp_web_page_sk#433, sum(wr_return_amt#255) AS returns#24, sum(wr_net_loss#263) AS profit_loss#25]
:        +- Filter (((wr_returned_date_sk#240 = d_date_sk#405) AND ((cast(d_date#407 as date) >= cast(2000-08-23 as date)) AND (cast(d_date#407 as date) <= date_add(cast(2000-08-23 as date), 30)))) AND (wr_web_page_sk#251 = wp_web_page_sk#433))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.web_returns
:              :  :  +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#240,wr_returned_time_sk#241,wr_item_sk#242,wr_refunded_customer_sk#243,wr_refunded_cdemo_sk#244,wr_refunded_hdemo_sk#245,wr_refunded_addr_sk#246,wr_returning_customer_sk#247,wr_returning_cdemo_sk#248,wr_returning_hdemo_sk#249,wr_returning_addr_sk#250,wr_web_page_sk#251,wr_reason_sk#252,wr_order_number#253,wr_return_quantity#254,wr_return_amt#255,wr_return_tax#256,wr_return_amt_inc_tax#257,wr_fee#258,wr_return_ship_cost#259,wr_refunded_cash#260,wr_reversed_charge#261,wr_account_credit#262,wr_net_loss#263] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#405,d_date_id#406,d_date#407,d_month_seq#408,d_week_seq#409,d_quarter_seq#410,d_year#411,d_dow#412,d_moy#413,d_dom#414,d_qoy#415,d_fy_year#416,d_fy_quarter_seq#417,d_fy_week_seq#418,d_day_name#419,d_quarter_name#420,d_holiday#421,d_weekend#422,d_following_holiday#423,d_first_dom#424,d_last_dom#425,d_same_day_ly#426,d_same_day_lq#427,d_current_day#428,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.web_page
:                 +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#433,wp_web_page_id#434,wp_rec_start_date#435,wp_rec_end_date#436,wp_creation_date_sk#437,wp_access_date_sk#438,wp_autogen_flag#439,wp_customer_sk#440,wp_url#441,wp_type#442,wp_char_count#443,wp_link_count#444,wp_image_count#445,wp_max_ad_count#446] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [channel#481 ASC NULLS FIRST, id#482 ASC NULLS FIRST], true
         +- Aggregate [channel#481, id#482, spark_grouping_id#480L], [channel#481, id#482, sum(sales#14) AS sales#11, sum(returns#2) AS returns#12, sum(profit#3) AS profit#13]
            +- Expand [[channel#0, id#1, sales#14, returns#2, profit#3, channel#478, id#479, 0], [channel#0, id#1, sales#14, returns#2, profit#3, channel#478, null, 1], [channel#0, id#1, sales#14, returns#2, profit#3, null, null, 3]], [channel#0, id#1, sales#14, returns#2, profit#3, channel#481, id#482, spark_grouping_id#480L]
               +- Project [channel#0, id#1, sales#14, returns#2, profit#3, channel#0 AS channel#478, id#1 AS id#479]
                  +- SubqueryAlias x
                     +- Union false, false
                        :- Union false, false
                        :  :- Project [store channel AS channel#0, s_store_sk#82 AS id#1, sales#14, coalesce(returns#16, cast(0 as double)) AS returns#2, (profit#15 - coalesce(profit_loss#17, cast(0 as double))) AS profit#3]
                        :  :  +- Join LeftOuter, (s_store_sk#82 = s_store_sk#292)
                        :  :     :- SubqueryAlias ss
                        :  :     :  +- CTERelationRef 0, true, [s_store_sk#82, sales#14, profit#15], false
                        :  :     +- SubqueryAlias sr
                        :  :        +- CTERelationRef 1, true, [s_store_sk#292, returns#16, profit_loss#17], false
                        :  +- Project [catalog channel AS channel#4, cs_call_center_sk#142 AS id#5, sales#18, returns#20, (profit#19 - profit_loss#21) AS profit#6]
                        :     +- Join Inner
                        :        :- SubqueryAlias cs
                        :        :  +- CTERelationRef 2, true, [cs_call_center_sk#142, sales#18, profit#19], false
                        :        +- SubqueryAlias cr
                        :           +- CTERelationRef 3, true, [cr_call_center_sk#176, returns#20, profit_loss#21], false
                        +- Project [web channel AS channel#7, wp_web_page_sk#226 AS id#8, sales#22, coalesce(returns#24, cast(0 as double)) AS returns#9, (profit#23 - coalesce(profit_loss#25, cast(0 as double))) AS profit#10]
                           +- Join LeftOuter, (wp_web_page_sk#226 = wp_web_page_sk#433)
                              :- SubqueryAlias ws
                              :  +- CTERelationRef 4, true, [wp_web_page_sk#226, sales#22, profit#23], false
                              +- SubqueryAlias wr
                                 +- CTERelationRef 5, true, [wp_web_page_sk#433, returns#24, profit_loss#25], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#481 ASC NULLS FIRST, id#482 ASC NULLS FIRST], true
      +- Aggregate [channel#481, id#482, spark_grouping_id#480L], [channel#481, id#482, sum(sales#14) AS sales#11, sum(returns#2) AS returns#12, sum(profit#3) AS profit#13]
         +- Expand [[sales#14, returns#2, profit#3, channel#478, id#479, 0], [sales#14, returns#2, profit#3, channel#478, null, 1], [sales#14, returns#2, profit#3, null, null, 3]], [sales#14, returns#2, profit#3, channel#481, id#482, spark_grouping_id#480L]
            +- Union false, false
               :- Project [sales#14, coalesce(returns#16, 0.0) AS returns#2, (profit#15 - coalesce(profit_loss#17, 0.0)) AS profit#3, store channel AS channel#478, s_store_sk#82 AS id#479]
               :  +- Join LeftOuter, (s_store_sk#82 = s_store_sk#292)
               :     :- Aggregate [s_store_sk#82], [s_store_sk#82, sum(ss_ext_sales_price#46) AS sales#14, sum(ss_net_profit#53) AS profit#15]
               :     :  +- Project [ss_ext_sales_price#46, ss_net_profit#53, s_store_sk#82]
               :     :     +- Join Inner, (ss_store_sk#38 = s_store_sk#82)
               :     :        :- Project [ss_store_sk#38, ss_ext_sales_price#46, ss_net_profit#53]
               :     :        :  +- Join Inner, (ss_sold_date_sk#31 = d_date_sk#54)
               :     :        :     :- Project [ss_sold_date_sk#31, ss_store_sk#38, ss_ext_sales_price#46, ss_net_profit#53]
               :     :        :     :  +- Filter (isnotnull(ss_sold_date_sk#31) AND isnotnull(ss_store_sk#38))
               :     :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#31,ss_sold_time_sk#32,ss_item_sk#33,ss_customer_sk#34,ss_cdemo_sk#35,ss_hdemo_sk#36,ss_addr_sk#37,ss_store_sk#38,ss_promo_sk#39,ss_ticket_number#40,ss_quantity#41,ss_wholesale_cost#42,ss_list_price#43,ss_sales_price#44,ss_ext_discount_amt#45,ss_ext_sales_price#46,ss_ext_wholesale_cost#47,ss_ext_list_price#48,ss_ext_tax#49,ss_coupon_amt#50,ss_net_paid#51,ss_net_paid_inc_tax#52,ss_net_profit#53] parquet
               :     :        :     +- Project [d_date_sk#54]
               :     :        :        +- Filter ((isnotnull(d_date#56) AND ((cast(d_date#56 as date) >= 2000-08-23) AND (cast(d_date#56 as date) <= 2000-09-22))) AND isnotnull(d_date_sk#54))
               :     :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#54,d_date_id#55,d_date#56,d_month_seq#57,d_week_seq#58,d_quarter_seq#59,d_year#60,d_dow#61,d_moy#62,d_dom#63,d_qoy#64,d_fy_year#65,d_fy_quarter_seq#66,d_fy_week_seq#67,d_day_name#68,d_quarter_name#69,d_holiday#70,d_weekend#71,d_following_holiday#72,d_first_dom#73,d_last_dom#74,d_same_day_ly#75,d_same_day_lq#76,d_current_day#77,... 4 more fields] parquet
               :     :        +- Project [s_store_sk#82]
               :     :           +- Filter isnotnull(s_store_sk#82)
               :     :              +- Relation spark_catalog.tpcds.store[s_store_sk#82,s_store_id#83,s_rec_start_date#84,s_rec_end_date#85,s_closed_date_sk#86,s_store_name#87,s_number_employees#88,s_floor_space#89,s_hours#90,s_manager#91,s_market_id#92,s_geography_class#93,s_market_desc#94,s_market_manager#95,s_division_id#96,s_division_name#97,s_company_id#98,s_company_name#99,s_street_number#100,s_street_name#101,s_street_type#102,s_suite_number#103,s_city#104,s_county#105,... 5 more fields] parquet
               :     +- Aggregate [s_store_sk#292], [s_store_sk#292, sum(sr_return_amt#122) AS returns#16, sum(sr_net_loss#130) AS profit_loss#17]
               :        +- Project [sr_return_amt#122, sr_net_loss#130, s_store_sk#292]
               :           +- Join Inner, (sr_store_sk#118 = s_store_sk#292)
               :              :- Project [sr_store_sk#118, sr_return_amt#122, sr_net_loss#130]
               :              :  +- Join Inner, (sr_returned_date_sk#111 = d_date_sk#264)
               :              :     :- Project [sr_returned_date_sk#111, sr_store_sk#118, sr_return_amt#122, sr_net_loss#130]
               :              :     :  +- Filter (isnotnull(sr_returned_date_sk#111) AND isnotnull(sr_store_sk#118))
               :              :     :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#111,sr_return_time_sk#112,sr_item_sk#113,sr_customer_sk#114,sr_cdemo_sk#115,sr_hdemo_sk#116,sr_addr_sk#117,sr_store_sk#118,sr_reason_sk#119,sr_ticket_number#120,sr_return_quantity#121,sr_return_amt#122,sr_return_tax#123,sr_return_amt_inc_tax#124,sr_fee#125,sr_return_ship_cost#126,sr_refunded_cash#127,sr_reversed_charge#128,sr_store_credit#129,sr_net_loss#130] parquet
               :              :     +- Project [d_date_sk#264]
               :              :        +- Filter ((isnotnull(d_date#266) AND ((cast(d_date#266 as date) >= 2000-08-23) AND (cast(d_date#266 as date) <= 2000-09-22))) AND isnotnull(d_date_sk#264))
               :              :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#264,d_date_id#265,d_date#266,d_month_seq#267,d_week_seq#268,d_quarter_seq#269,d_year#270,d_dow#271,d_moy#272,d_dom#273,d_qoy#274,d_fy_year#275,d_fy_quarter_seq#276,d_fy_week_seq#277,d_day_name#278,d_quarter_name#279,d_holiday#280,d_weekend#281,d_following_holiday#282,d_first_dom#283,d_last_dom#284,d_same_day_ly#285,d_same_day_lq#286,d_current_day#287,... 4 more fields] parquet
               :              +- Project [s_store_sk#292]
               :                 +- Filter isnotnull(s_store_sk#292)
               :                    +- Relation spark_catalog.tpcds.store[s_store_sk#292,s_store_id#293,s_rec_start_date#294,s_rec_end_date#295,s_closed_date_sk#296,s_store_name#297,s_number_employees#298,s_floor_space#299,s_hours#300,s_manager#301,s_market_id#302,s_geography_class#303,s_market_desc#304,s_market_manager#305,s_division_id#306,s_division_name#307,s_company_id#308,s_company_name#309,s_street_number#310,s_street_name#311,s_street_type#312,s_suite_number#313,s_city#314,s_county#315,... 5 more fields] parquet
               :- Project [sales#18, returns#20, (profit#19 - profit_loss#21) AS profit#6, catalog channel AS channel#483, cs_call_center_sk#142 AS id#484]
               :  +- Join Inner
               :     :- Aggregate [cs_call_center_sk#142], [cs_call_center_sk#142, sum(cs_ext_sales_price#154) AS sales#18, sum(cs_net_profit#164) AS profit#19]
               :     :  +- Project [cs_call_center_sk#142, cs_ext_sales_price#154, cs_net_profit#164]
               :     :     +- Join Inner, (cs_sold_date_sk#131 = d_date_sk#321)
               :     :        :- Project [cs_sold_date_sk#131, cs_call_center_sk#142, cs_ext_sales_price#154, cs_net_profit#164]
               :     :        :  +- Filter isnotnull(cs_sold_date_sk#131)
               :     :        :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#131,cs_sold_time_sk#132,cs_ship_date_sk#133,cs_bill_customer_sk#134,cs_bill_cdemo_sk#135,cs_bill_hdemo_sk#136,cs_bill_addr_sk#137,cs_ship_customer_sk#138,cs_ship_cdemo_sk#139,cs_ship_hdemo_sk#140,cs_ship_addr_sk#141,cs_call_center_sk#142,cs_catalog_page_sk#143,cs_ship_mode_sk#144,cs_warehouse_sk#145,cs_item_sk#146,cs_promo_sk#147,cs_order_number#148,cs_quantity#149,cs_wholesale_cost#150,cs_list_price#151,cs_sales_price#152,cs_ext_discount_amt#153,cs_ext_sales_price#154,... 10 more fields] parquet
               :     :        +- Project [d_date_sk#321]
               :     :           +- Filter ((isnotnull(d_date#323) AND ((cast(d_date#323 as date) >= 2000-08-23) AND (cast(d_date#323 as date) <= 2000-09-22))) AND isnotnull(d_date_sk#321))
               :     :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#321,d_date_id#322,d_date#323,d_month_seq#324,d_week_seq#325,d_quarter_seq#326,d_year#327,d_dow#328,d_moy#329,d_dom#330,d_qoy#331,d_fy_year#332,d_fy_quarter_seq#333,d_fy_week_seq#334,d_day_name#335,d_quarter_name#336,d_holiday#337,d_weekend#338,d_following_holiday#339,d_first_dom#340,d_last_dom#341,d_same_day_ly#342,d_same_day_lq#343,d_current_day#344,... 4 more fields] parquet
               :     +- Aggregate [cr_call_center_sk#176], [sum(cr_return_amount#183) AS returns#20, sum(cr_net_loss#191) AS profit_loss#21]
               :        +- Project [cr_call_center_sk#176, cr_return_amount#183, cr_net_loss#191]
               :           +- Join Inner, (cr_returned_date_sk#165 = d_date_sk#349)
               :              :- Project [cr_returned_date_sk#165, cr_call_center_sk#176, cr_return_amount#183, cr_net_loss#191]
               :              :  +- Filter isnotnull(cr_returned_date_sk#165)
               :              :     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#165,cr_returned_time_sk#166,cr_item_sk#167,cr_refunded_customer_sk#168,cr_refunded_cdemo_sk#169,cr_refunded_hdemo_sk#170,cr_refunded_addr_sk#171,cr_returning_customer_sk#172,cr_returning_cdemo_sk#173,cr_returning_hdemo_sk#174,cr_returning_addr_sk#175,cr_call_center_sk#176,cr_catalog_page_sk#177,cr_ship_mode_sk#178,cr_warehouse_sk#179,cr_reason_sk#180,cr_order_number#181,cr_return_quantity#182,cr_return_amount#183,cr_return_tax#184,cr_return_amt_inc_tax#185,cr_fee#186,cr_return_ship_cost#187,cr_refunded_cash#188,... 3 more fields] parquet
               :              +- Project [d_date_sk#349]
               :                 +- Filter ((isnotnull(d_date#351) AND ((cast(d_date#351 as date) >= 2000-08-23) AND (cast(d_date#351 as date) <= 2000-09-22))) AND isnotnull(d_date_sk#349))
               :                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#349,d_date_id#350,d_date#351,d_month_seq#352,d_week_seq#353,d_quarter_seq#354,d_year#355,d_dow#356,d_moy#357,d_dom#358,d_qoy#359,d_fy_year#360,d_fy_quarter_seq#361,d_fy_week_seq#362,d_day_name#363,d_quarter_name#364,d_holiday#365,d_weekend#366,d_following_holiday#367,d_first_dom#368,d_last_dom#369,d_same_day_ly#370,d_same_day_lq#371,d_current_day#372,... 4 more fields] parquet
               +- Project [sales#22, coalesce(returns#24, 0.0) AS returns#9, (profit#23 - coalesce(profit_loss#25, 0.0)) AS profit#10, web channel AS channel#485, wp_web_page_sk#226 AS id#486]
                  +- Join LeftOuter, (wp_web_page_sk#226 = wp_web_page_sk#433)
                     :- Aggregate [wp_web_page_sk#226], [wp_web_page_sk#226, sum(ws_ext_sales_price#215) AS sales#22, sum(ws_net_profit#225) AS profit#23]
                     :  +- Project [ws_ext_sales_price#215, ws_net_profit#225, wp_web_page_sk#226]
                     :     +- Join Inner, (ws_web_page_sk#204 = wp_web_page_sk#226)
                     :        :- Project [ws_web_page_sk#204, ws_ext_sales_price#215, ws_net_profit#225]
                     :        :  +- Join Inner, (ws_sold_date_sk#192 = d_date_sk#377)
                     :        :     :- Project [ws_sold_date_sk#192, ws_web_page_sk#204, ws_ext_sales_price#215, ws_net_profit#225]
                     :        :     :  +- Filter (isnotnull(ws_sold_date_sk#192) AND isnotnull(ws_web_page_sk#204))
                     :        :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#192,ws_sold_time_sk#193,ws_ship_date_sk#194,ws_item_sk#195,ws_bill_customer_sk#196,ws_bill_cdemo_sk#197,ws_bill_hdemo_sk#198,ws_bill_addr_sk#199,ws_ship_customer_sk#200,ws_ship_cdemo_sk#201,ws_ship_hdemo_sk#202,ws_ship_addr_sk#203,ws_web_page_sk#204,ws_web_site_sk#205,ws_ship_mode_sk#206,ws_warehouse_sk#207,ws_promo_sk#208,ws_order_number#209,ws_quantity#210,ws_wholesale_cost#211,ws_list_price#212,ws_sales_price#213,ws_ext_discount_amt#214,ws_ext_sales_price#215,... 10 more fields] parquet
                     :        :     +- Project [d_date_sk#377]
                     :        :        +- Filter ((isnotnull(d_date#379) AND ((cast(d_date#379 as date) >= 2000-08-23) AND (cast(d_date#379 as date) <= 2000-09-22))) AND isnotnull(d_date_sk#377))
                     :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#377,d_date_id#378,d_date#379,d_month_seq#380,d_week_seq#381,d_quarter_seq#382,d_year#383,d_dow#384,d_moy#385,d_dom#386,d_qoy#387,d_fy_year#388,d_fy_quarter_seq#389,d_fy_week_seq#390,d_day_name#391,d_quarter_name#392,d_holiday#393,d_weekend#394,d_following_holiday#395,d_first_dom#396,d_last_dom#397,d_same_day_ly#398,d_same_day_lq#399,d_current_day#400,... 4 more fields] parquet
                     :        +- Project [wp_web_page_sk#226]
                     :           +- Filter isnotnull(wp_web_page_sk#226)
                     :              +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#226,wp_web_page_id#227,wp_rec_start_date#228,wp_rec_end_date#229,wp_creation_date_sk#230,wp_access_date_sk#231,wp_autogen_flag#232,wp_customer_sk#233,wp_url#234,wp_type#235,wp_char_count#236,wp_link_count#237,wp_image_count#238,wp_max_ad_count#239] parquet
                     +- Aggregate [wp_web_page_sk#433], [wp_web_page_sk#433, sum(wr_return_amt#255) AS returns#24, sum(wr_net_loss#263) AS profit_loss#25]
                        +- Project [wr_return_amt#255, wr_net_loss#263, wp_web_page_sk#433]
                           +- Join Inner, (wr_web_page_sk#251 = wp_web_page_sk#433)
                              :- Project [wr_web_page_sk#251, wr_return_amt#255, wr_net_loss#263]
                              :  +- Join Inner, (wr_returned_date_sk#240 = d_date_sk#405)
                              :     :- Project [wr_returned_date_sk#240, wr_web_page_sk#251, wr_return_amt#255, wr_net_loss#263]
                              :     :  +- Filter (isnotnull(wr_returned_date_sk#240) AND isnotnull(wr_web_page_sk#251))
                              :     :     +- Relation spark_catalog.tpcds.web_returns[wr_returned_date_sk#240,wr_returned_time_sk#241,wr_item_sk#242,wr_refunded_customer_sk#243,wr_refunded_cdemo_sk#244,wr_refunded_hdemo_sk#245,wr_refunded_addr_sk#246,wr_returning_customer_sk#247,wr_returning_cdemo_sk#248,wr_returning_hdemo_sk#249,wr_returning_addr_sk#250,wr_web_page_sk#251,wr_reason_sk#252,wr_order_number#253,wr_return_quantity#254,wr_return_amt#255,wr_return_tax#256,wr_return_amt_inc_tax#257,wr_fee#258,wr_return_ship_cost#259,wr_refunded_cash#260,wr_reversed_charge#261,wr_account_credit#262,wr_net_loss#263] parquet
                              :     +- Project [d_date_sk#405]
                              :        +- Filter ((isnotnull(d_date#407) AND ((cast(d_date#407 as date) >= 2000-08-23) AND (cast(d_date#407 as date) <= 2000-09-22))) AND isnotnull(d_date_sk#405))
                              :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#405,d_date_id#406,d_date#407,d_month_seq#408,d_week_seq#409,d_quarter_seq#410,d_year#411,d_dow#412,d_moy#413,d_dom#414,d_qoy#415,d_fy_year#416,d_fy_quarter_seq#417,d_fy_week_seq#418,d_day_name#419,d_quarter_name#420,d_holiday#421,d_weekend#422,d_following_holiday#423,d_first_dom#424,d_last_dom#425,d_same_day_ly#426,d_same_day_lq#427,d_current_day#428,... 4 more fields] parquet
                              +- Project [wp_web_page_sk#433]
                                 +- Filter isnotnull(wp_web_page_sk#433)
                                    +- Relation spark_catalog.tpcds.web_page[wp_web_page_sk#433,wp_web_page_id#434,wp_rec_start_date#435,wp_rec_end_date#436,wp_creation_date_sk#437,wp_access_date_sk#438,wp_autogen_flag#439,wp_customer_sk#440,wp_url#441,wp_type#442,wp_char_count#443,wp_link_count#444,wp_image_count#445,wp_max_ad_count#446] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[channel#481 ASC NULLS FIRST,id#482 ASC NULLS FIRST], output=[channel#481,id#482,sales#11,returns#12,profit#13])
   +- HashAggregate(keys=[channel#481, id#482, spark_grouping_id#480L], functions=[sum(sales#14), sum(returns#2), sum(profit#3)], output=[channel#481, id#482, sales#11, returns#12, profit#13])
      +- Exchange hashpartitioning(channel#481, id#482, spark_grouping_id#480L, 200), ENSURE_REQUIREMENTS, [plan_id=417]
         +- HashAggregate(keys=[channel#481, id#482, spark_grouping_id#480L], functions=[partial_sum(sales#14), partial_sum(returns#2), partial_sum(profit#3)], output=[channel#481, id#482, spark_grouping_id#480L, sum#490, sum#491, sum#492])
            +- Expand [[sales#14, returns#2, profit#3, channel#478, id#479, 0], [sales#14, returns#2, profit#3, channel#478, null, 1], [sales#14, returns#2, profit#3, null, null, 3]], [sales#14, returns#2, profit#3, channel#481, id#482, spark_grouping_id#480L]
               +- Union
                  :- Project [sales#14, coalesce(returns#16, 0.0) AS returns#2, (profit#15 - coalesce(profit_loss#17, 0.0)) AS profit#3, store channel AS channel#478, s_store_sk#82 AS id#479]
                  :  +- SortMergeJoin [s_store_sk#82], [s_store_sk#292], LeftOuter
                  :     :- Sort [s_store_sk#82 ASC NULLS FIRST], false, 0
                  :     :  +- HashAggregate(keys=[s_store_sk#82], functions=[sum(ss_ext_sales_price#46), sum(ss_net_profit#53)], output=[s_store_sk#82, sales#14, profit#15])
                  :     :     +- Exchange hashpartitioning(s_store_sk#82, 200), ENSURE_REQUIREMENTS, [plan_id=350]
                  :     :        +- HashAggregate(keys=[s_store_sk#82], functions=[partial_sum(ss_ext_sales_price#46), partial_sum(ss_net_profit#53)], output=[s_store_sk#82, sum#495, sum#496])
                  :     :           +- Project [ss_ext_sales_price#46, ss_net_profit#53, s_store_sk#82]
                  :     :              +- BroadcastHashJoin [ss_store_sk#38], [s_store_sk#82], Inner, BuildRight, false
                  :     :                 :- Project [ss_store_sk#38, ss_ext_sales_price#46, ss_net_profit#53]
                  :     :                 :  +- BroadcastHashJoin [ss_sold_date_sk#31], [d_date_sk#54], Inner, BuildRight, false
                  :     :                 :     :- Filter (isnotnull(ss_sold_date_sk#31) AND isnotnull(ss_store_sk#38))
                  :     :                 :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#31,ss_store_sk#38,ss_ext_sales_price#46,ss_net_profit#53] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#31), isnotnull(ss_store_sk#38)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_ext_sales_price:double,ss_net_profit:double>
                  :     :                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=341]
                  :     :                 :        +- Project [d_date_sk#54]
                  :     :                 :           +- Filter (((isnotnull(d_date#56) AND (cast(d_date#56 as date) >= 2000-08-23)) AND (cast(d_date#56 as date) <= 2000-09-22)) AND isnotnull(d_date_sk#54))
                  :     :                 :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#54,d_date#56] Batched: true, DataFilters: [isnotnull(d_date#56), (cast(d_date#56 as date) >= 2000-08-23), (cast(d_date#56 as date) <= 2000-..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  :     :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=345]
                  :     :                    +- Filter isnotnull(s_store_sk#82)
                  :     :                       +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#82] Batched: true, DataFilters: [isnotnull(s_store_sk#82)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int>
                  :     +- Sort [s_store_sk#292 ASC NULLS FIRST], false, 0
                  :        +- HashAggregate(keys=[s_store_sk#292], functions=[sum(sr_return_amt#122), sum(sr_net_loss#130)], output=[s_store_sk#292, returns#16, profit_loss#17])
                  :           +- Exchange hashpartitioning(s_store_sk#292, 200), ENSURE_REQUIREMENTS, [plan_id=361]
                  :              +- HashAggregate(keys=[s_store_sk#292], functions=[partial_sum(sr_return_amt#122), partial_sum(sr_net_loss#130)], output=[s_store_sk#292, sum#499, sum#500])
                  :                 +- Project [sr_return_amt#122, sr_net_loss#130, s_store_sk#292]
                  :                    +- BroadcastHashJoin [sr_store_sk#118], [s_store_sk#292], Inner, BuildRight, false
                  :                       :- Project [sr_store_sk#118, sr_return_amt#122, sr_net_loss#130]
                  :                       :  +- BroadcastHashJoin [sr_returned_date_sk#111], [d_date_sk#264], Inner, BuildRight, false
                  :                       :     :- Filter (isnotnull(sr_returned_date_sk#111) AND isnotnull(sr_store_sk#118))
                  :                       :     :  +- FileScan parquet spark_catalog.tpcds.store_returns[sr_returned_date_sk#111,sr_store_sk#118,sr_return_amt#122,sr_net_loss#130] Batched: true, DataFilters: [isnotnull(sr_returned_date_sk#111), isnotnull(sr_store_sk#118)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_returned_date_sk), IsNotNull(sr_store_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_store_sk:int,sr_return_amt:double,sr_net_loss:double>
                  :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=352]
                  :                       :        +- Project [d_date_sk#264]
                  :                       :           +- Filter (((isnotnull(d_date#266) AND (cast(d_date#266 as date) >= 2000-08-23)) AND (cast(d_date#266 as date) <= 2000-09-22)) AND isnotnull(d_date_sk#264))
                  :                       :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#264,d_date#266] Batched: true, DataFilters: [isnotnull(d_date#266), (cast(d_date#266 as date) >= 2000-08-23), (cast(d_date#266 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=356]
                  :                          +- Filter isnotnull(s_store_sk#292)
                  :                             +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#292] Batched: true, DataFilters: [isnotnull(s_store_sk#292)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int>
                  :- Project [sales#18, returns#20, (profit#19 - profit_loss#21) AS profit#6, catalog channel AS channel#483, cs_call_center_sk#142 AS id#484]
                  :  +- CartesianProduct
                  :     :- HashAggregate(keys=[cs_call_center_sk#142], functions=[sum(cs_ext_sales_price#154), sum(cs_net_profit#164)], output=[cs_call_center_sk#142, sales#18, profit#19])
                  :     :  +- Exchange hashpartitioning(cs_call_center_sk#142, 200), ENSURE_REQUIREMENTS, [plan_id=374]
                  :     :     +- HashAggregate(keys=[cs_call_center_sk#142], functions=[partial_sum(cs_ext_sales_price#154), partial_sum(cs_net_profit#164)], output=[cs_call_center_sk#142, sum#503, sum#504])
                  :     :        +- Project [cs_call_center_sk#142, cs_ext_sales_price#154, cs_net_profit#164]
                  :     :           +- BroadcastHashJoin [cs_sold_date_sk#131], [d_date_sk#321], Inner, BuildRight, false
                  :     :              :- Filter isnotnull(cs_sold_date_sk#131)
                  :     :              :  +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#131,cs_call_center_sk#142,cs_ext_sales_price#154,cs_net_profit#164] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#131)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_call_center_sk:int,cs_ext_sales_price:double,cs_net_profit:double>
                  :     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=369]
                  :     :                 +- Project [d_date_sk#321]
                  :     :                    +- Filter (((isnotnull(d_date#323) AND (cast(d_date#323 as date) >= 2000-08-23)) AND (cast(d_date#323 as date) <= 2000-09-22)) AND isnotnull(d_date_sk#321))
                  :     :                       +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#321,d_date#323] Batched: true, DataFilters: [isnotnull(d_date#323), (cast(d_date#323 as date) >= 2000-08-23), (cast(d_date#323 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  :     +- HashAggregate(keys=[cr_call_center_sk#176], functions=[sum(cr_return_amount#183), sum(cr_net_loss#191)], output=[returns#20, profit_loss#21])
                  :        +- Exchange hashpartitioning(cr_call_center_sk#176, 200), ENSURE_REQUIREMENTS, [plan_id=381]
                  :           +- HashAggregate(keys=[cr_call_center_sk#176], functions=[partial_sum(cr_return_amount#183), partial_sum(cr_net_loss#191)], output=[cr_call_center_sk#176, sum#507, sum#508])
                  :              +- Project [cr_call_center_sk#176, cr_return_amount#183, cr_net_loss#191]
                  :                 +- BroadcastHashJoin [cr_returned_date_sk#165], [d_date_sk#349], Inner, BuildRight, false
                  :                    :- Filter isnotnull(cr_returned_date_sk#165)
                  :                    :  +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#165,cr_call_center_sk#176,cr_return_amount#183,cr_net_loss#191] Batched: true, DataFilters: [isnotnull(cr_returned_date_sk#165)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_returned_date_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_call_center_sk:int,cr_return_amount:double,cr_net_loss:double>
                  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=376]
                  :                       +- Project [d_date_sk#349]
                  :                          +- Filter (((isnotnull(d_date#351) AND (cast(d_date#351 as date) >= 2000-08-23)) AND (cast(d_date#351 as date) <= 2000-09-22)) AND isnotnull(d_date_sk#349))
                  :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#349,d_date#351] Batched: true, DataFilters: [isnotnull(d_date#351), (cast(d_date#351 as date) >= 2000-08-23), (cast(d_date#351 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  +- Project [sales#22, coalesce(returns#24, 0.0) AS returns#9, (profit#23 - coalesce(profit_loss#25, 0.0)) AS profit#10, web channel AS channel#485, wp_web_page_sk#226 AS id#486]
                     +- SortMergeJoin [wp_web_page_sk#226], [wp_web_page_sk#433], LeftOuter
                        :- Sort [wp_web_page_sk#226 ASC NULLS FIRST], false, 0
                        :  +- HashAggregate(keys=[wp_web_page_sk#226], functions=[sum(ws_ext_sales_price#215), sum(ws_net_profit#225)], output=[wp_web_page_sk#226, sales#22, profit#23])
                        :     +- Exchange hashpartitioning(wp_web_page_sk#226, 200), ENSURE_REQUIREMENTS, [plan_id=394]
                        :        +- HashAggregate(keys=[wp_web_page_sk#226], functions=[partial_sum(ws_ext_sales_price#215), partial_sum(ws_net_profit#225)], output=[wp_web_page_sk#226, sum#511, sum#512])
                        :           +- Project [ws_ext_sales_price#215, ws_net_profit#225, wp_web_page_sk#226]
                        :              +- BroadcastHashJoin [ws_web_page_sk#204], [wp_web_page_sk#226], Inner, BuildRight, false
                        :                 :- Project [ws_web_page_sk#204, ws_ext_sales_price#215, ws_net_profit#225]
                        :                 :  +- BroadcastHashJoin [ws_sold_date_sk#192], [d_date_sk#377], Inner, BuildRight, false
                        :                 :     :- Filter (isnotnull(ws_sold_date_sk#192) AND isnotnull(ws_web_page_sk#204))
                        :                 :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#192,ws_web_page_sk#204,ws_ext_sales_price#215,ws_net_profit#225] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#192), isnotnull(ws_web_page_sk#204)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_web_page_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_web_page_sk:int,ws_ext_sales_price:double,ws_net_profit:double>
                        :                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=385]
                        :                 :        +- Project [d_date_sk#377]
                        :                 :           +- Filter (((isnotnull(d_date#379) AND (cast(d_date#379 as date) >= 2000-08-23)) AND (cast(d_date#379 as date) <= 2000-09-22)) AND isnotnull(d_date_sk#377))
                        :                 :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#377,d_date#379] Batched: true, DataFilters: [isnotnull(d_date#379), (cast(d_date#379 as date) >= 2000-08-23), (cast(d_date#379 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                        :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=389]
                        :                    +- Filter isnotnull(wp_web_page_sk#226)
                        :                       +- FileScan parquet spark_catalog.tpcds.web_page[wp_web_page_sk#226] Batched: true, DataFilters: [isnotnull(wp_web_page_sk#226)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wp_web_page_sk)], ReadSchema: struct<wp_web_page_sk:int>
                        +- Sort [wp_web_page_sk#433 ASC NULLS FIRST], false, 0
                           +- HashAggregate(keys=[wp_web_page_sk#433], functions=[sum(wr_return_amt#255), sum(wr_net_loss#263)], output=[wp_web_page_sk#433, returns#24, profit_loss#25])
                              +- Exchange hashpartitioning(wp_web_page_sk#433, 200), ENSURE_REQUIREMENTS, [plan_id=405]
                                 +- HashAggregate(keys=[wp_web_page_sk#433], functions=[partial_sum(wr_return_amt#255), partial_sum(wr_net_loss#263)], output=[wp_web_page_sk#433, sum#515, sum#516])
                                    +- Project [wr_return_amt#255, wr_net_loss#263, wp_web_page_sk#433]
                                       +- BroadcastHashJoin [wr_web_page_sk#251], [wp_web_page_sk#433], Inner, BuildRight, false
                                          :- Project [wr_web_page_sk#251, wr_return_amt#255, wr_net_loss#263]
                                          :  +- BroadcastHashJoin [wr_returned_date_sk#240], [d_date_sk#405], Inner, BuildRight, false
                                          :     :- Filter (isnotnull(wr_returned_date_sk#240) AND isnotnull(wr_web_page_sk#251))
                                          :     :  +- FileScan parquet spark_catalog.tpcds.web_returns[wr_returned_date_sk#240,wr_web_page_sk#251,wr_return_amt#255,wr_net_loss#263] Batched: true, DataFilters: [isnotnull(wr_returned_date_sk#240), isnotnull(wr_web_page_sk#251)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_returned_date_sk), IsNotNull(wr_web_page_sk)], ReadSchema: struct<wr_returned_date_sk:int,wr_web_page_sk:int,wr_return_amt:double,wr_net_loss:double>
                                          :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=396]
                                          :        +- Project [d_date_sk#405]
                                          :           +- Filter (((isnotnull(d_date#407) AND (cast(d_date#407 as date) >= 2000-08-23)) AND (cast(d_date#407 as date) <= 2000-09-22)) AND isnotnull(d_date_sk#405))
                                          :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#405,d_date#407] Batched: true, DataFilters: [isnotnull(d_date#407), (cast(d_date#407 as date) >= 2000-08-23), (cast(d_date#407 as date) <= 20..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=400]
                                             +- Filter isnotnull(wp_web_page_sk#433)
                                                +- FileScan parquet spark_catalog.tpcds.web_page[wp_web_page_sk#433] Batched: true, DataFilters: [isnotnull(wp_web_page_sk#433)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(wp_web_page_sk)], ReadSchema: struct<wp_web_page_sk:int>

Time taken: 3.968 seconds, Fetched 1 row(s)
