Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580740142
== Parsed Logical Plan ==
CTE [ss, ws]
:  :- 'SubqueryAlias ss
:  :  +- 'Aggregate ['ca_county, 'd_qoy, 'd_year], ['ca_county, 'd_qoy, 'd_year, 'sum('ss_ext_sales_price) AS store_sales#4]
:  :     +- 'Filter (('ss_sold_date_sk = 'd_date_sk) AND ('ss_addr_sk = 'ca_address_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation [store_sales], [], false
:  :           :  +- 'UnresolvedRelation [date_dim], [], false
:  :           +- 'UnresolvedRelation [customer_address], [], false
:  +- 'SubqueryAlias ws
:     +- 'Aggregate ['ca_county, 'd_qoy, 'd_year], ['ca_county, 'd_qoy, 'd_year, 'sum('ws_ext_sales_price) AS web_sales#5]
:        +- 'Filter (('ws_sold_date_sk = 'd_date_sk) AND ('ws_bill_addr_sk = 'ca_address_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation [web_sales], [], false
:              :  +- 'UnresolvedRelation [date_dim], [], false
:              +- 'UnresolvedRelation [customer_address], [], false
+- 'Sort ['ss1.ca_county ASC NULLS FIRST], true
   +- 'Project ['ss1.ca_county, 'ss1.d_year, ('ws2.web_sales / 'ws1.web_sales) AS web_q1_q2_increase#0, ('ss2.store_sales / 'ss1.store_sales) AS store_q1_q2_increase#1, ('ws3.web_sales / 'ws2.web_sales) AS web_q2_q3_increase#2, ('ss3.store_sales / 'ss2.store_sales) AS store_q2_q3_increase#3]
      +- 'Filter (((((('ss1.d_qoy = 1) AND ('ss1.d_year = 2000)) AND ('ss1.ca_county = 'ss2.ca_county)) AND (('ss2.d_qoy = 2) AND ('ss2.d_year = 2000))) AND (((('ss2.ca_county = 'ss3.ca_county) AND ('ss3.d_qoy = 3)) AND ('ss3.d_year = 2000)) AND (('ss1.ca_county = 'ws1.ca_county) AND ('ws1.d_qoy = 1)))) AND ((((('ws1.d_year = 2000) AND ('ws1.ca_county = 'ws2.ca_county)) AND ('ws2.d_qoy = 2)) AND (('ws2.d_year = 2000) AND ('ws1.ca_county = 'ws3.ca_county))) AND ((('ws3.d_qoy = 3) AND ('ws3.d_year = 2000)) AND ((CASE WHEN ('ws1.web_sales > 0) THEN ('ws2.web_sales / 'ws1.web_sales) ELSE null END > CASE WHEN ('ss1.store_sales > 0) THEN ('ss2.store_sales / 'ss1.store_sales) ELSE null END) AND (CASE WHEN ('ws2.web_sales > 0) THEN ('ws3.web_sales / 'ws2.web_sales) ELSE null END > CASE WHEN ('ss2.store_sales > 0) THEN ('ss3.store_sales / 'ss2.store_sales) ELSE null END)))))
         +- 'Join Inner
            :- 'Join Inner
            :  :- 'Join Inner
            :  :  :- 'Join Inner
            :  :  :  :- 'Join Inner
            :  :  :  :  :- 'SubqueryAlias ss1
            :  :  :  :  :  +- 'UnresolvedRelation [ss], [], false
            :  :  :  :  +- 'SubqueryAlias ss2
            :  :  :  :     +- 'UnresolvedRelation [ss], [], false
            :  :  :  +- 'SubqueryAlias ss3
            :  :  :     +- 'UnresolvedRelation [ss], [], false
            :  :  +- 'SubqueryAlias ws1
            :  :     +- 'UnresolvedRelation [ws], [], false
            :  +- 'SubqueryAlias ws2
            :     +- 'UnresolvedRelation [ws], [], false
            +- 'SubqueryAlias ws3
               +- 'UnresolvedRelation [ws], [], false

== Analyzed Logical Plan ==
ca_county: string, d_year: int, web_q1_q2_increase: double, store_q1_q2_increase: double, web_q2_q3_increase: double, store_q2_q3_increase: double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias ss
:     +- Aggregate [ca_county#69, d_qoy#44, d_year#40], [ca_county#69, d_qoy#44, d_year#40, sum(ss_ext_sales_price#26) AS store_sales#4]
:        +- Filter ((ss_sold_date_sk#11 = d_date_sk#34) AND (ss_addr_sk#17 = ca_address_sk#62))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#11,ss_sold_time_sk#12,ss_item_sk#13,ss_customer_sk#14,ss_cdemo_sk#15,ss_hdemo_sk#16,ss_addr_sk#17,ss_store_sk#18,ss_promo_sk#19,ss_ticket_number#20,ss_quantity#21,ss_wholesale_cost#22,ss_list_price#23,ss_sales_price#24,ss_ext_discount_amt#25,ss_ext_sales_price#26,ss_ext_wholesale_cost#27,ss_ext_list_price#28,ss_ext_tax#29,ss_coupon_amt#30,ss_net_paid#31,ss_net_paid_inc_tax#32,ss_net_profit#33] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#34,d_date_id#35,d_date#36,d_month_seq#37,d_week_seq#38,d_quarter_seq#39,d_year#40,d_dow#41,d_moy#42,d_dom#43,d_qoy#44,d_fy_year#45,d_fy_quarter_seq#46,d_fy_week_seq#47,d_day_name#48,d_quarter_name#49,d_holiday#50,d_weekend#51,d_following_holiday#52,d_first_dom#53,d_last_dom#54,d_same_day_ly#55,d_same_day_lq#56,d_current_day#57,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.customer_address
:                 +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#62,ca_address_id#63,ca_street_number#64,ca_street_name#65,ca_street_type#66,ca_suite_number#67,ca_city#68,ca_county#69,ca_state#70,ca_zip#71,ca_country#72,ca_gmt_offset#73,ca_location_type#74] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias ws
:     +- Aggregate [ca_county#144, d_qoy#119, d_year#115], [ca_county#144, d_qoy#119, d_year#115, sum(ws_ext_sales_price#98) AS web_sales#5]
:        +- Filter ((ws_sold_date_sk#75 = d_date_sk#109) AND (ws_bill_addr_sk#82 = ca_address_sk#137))
:           +- Join Inner
:              :- Join Inner
:              :  :- SubqueryAlias spark_catalog.tpcds.web_sales
:              :  :  +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#75,ws_sold_time_sk#76,ws_ship_date_sk#77,ws_item_sk#78,ws_bill_customer_sk#79,ws_bill_cdemo_sk#80,ws_bill_hdemo_sk#81,ws_bill_addr_sk#82,ws_ship_customer_sk#83,ws_ship_cdemo_sk#84,ws_ship_hdemo_sk#85,ws_ship_addr_sk#86,ws_web_page_sk#87,ws_web_site_sk#88,ws_ship_mode_sk#89,ws_warehouse_sk#90,ws_promo_sk#91,ws_order_number#92,ws_quantity#93,ws_wholesale_cost#94,ws_list_price#95,ws_sales_price#96,ws_ext_discount_amt#97,ws_ext_sales_price#98,... 10 more fields] parquet
:              :  +- SubqueryAlias spark_catalog.tpcds.date_dim
:              :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#109,d_date_id#110,d_date#111,d_month_seq#112,d_week_seq#113,d_quarter_seq#114,d_year#115,d_dow#116,d_moy#117,d_dom#118,d_qoy#119,d_fy_year#120,d_fy_quarter_seq#121,d_fy_week_seq#122,d_day_name#123,d_quarter_name#124,d_holiday#125,d_weekend#126,d_following_holiday#127,d_first_dom#128,d_last_dom#129,d_same_day_ly#130,d_same_day_lq#131,d_current_day#132,... 4 more fields] parquet
:              +- SubqueryAlias spark_catalog.tpcds.customer_address
:                 +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#137,ca_address_id#138,ca_street_number#139,ca_street_name#140,ca_street_type#141,ca_suite_number#142,ca_city#143,ca_county#144,ca_state#145,ca_zip#146,ca_country#147,ca_gmt_offset#148,ca_location_type#149] parquet
+- Sort [ca_county#69 ASC NULLS FIRST], true
   +- Project [ca_county#69, d_year#40, (web_sales#169 / web_sales#5) AS web_q1_q2_increase#0, (store_sales#161 / store_sales#4) AS store_q1_q2_increase#1, (web_sales#173 / web_sales#169) AS web_q2_q3_increase#2, (store_sales#165 / store_sales#161) AS store_q2_q3_increase#3]
      +- Filter ((((((d_qoy#44 = 1) AND (d_year#40 = 2000)) AND (ca_county#69 = ca_county#158)) AND ((d_qoy#159 = 2) AND (d_year#160 = 2000))) AND ((((ca_county#158 = ca_county#162) AND (d_qoy#163 = 3)) AND (d_year#164 = 2000)) AND ((ca_county#69 = ca_county#144) AND (d_qoy#119 = 1)))) AND (((((d_year#115 = 2000) AND (ca_county#144 = ca_county#166)) AND (d_qoy#167 = 2)) AND ((d_year#168 = 2000) AND (ca_county#144 = ca_county#170))) AND (((d_qoy#171 = 3) AND (d_year#172 = 2000)) AND ((CASE WHEN (web_sales#5 > cast(0 as double)) THEN (web_sales#169 / web_sales#5) ELSE cast(null as double) END > CASE WHEN (store_sales#4 > cast(0 as double)) THEN (store_sales#161 / store_sales#4) ELSE cast(null as double) END) AND (CASE WHEN (web_sales#169 > cast(0 as double)) THEN (web_sales#173 / web_sales#169) ELSE cast(null as double) END > CASE WHEN (store_sales#161 > cast(0 as double)) THEN (store_sales#165 / store_sales#161) ELSE cast(null as double) END)))))
         +- Join Inner
            :- Join Inner
            :  :- Join Inner
            :  :  :- Join Inner
            :  :  :  :- Join Inner
            :  :  :  :  :- SubqueryAlias ss1
            :  :  :  :  :  +- SubqueryAlias ss
            :  :  :  :  :     +- CTERelationRef 0, true, [ca_county#69, d_qoy#44, d_year#40, store_sales#4], false
            :  :  :  :  +- SubqueryAlias ss2
            :  :  :  :     +- SubqueryAlias ss
            :  :  :  :        +- CTERelationRef 0, true, [ca_county#158, d_qoy#159, d_year#160, store_sales#161], false
            :  :  :  +- SubqueryAlias ss3
            :  :  :     +- SubqueryAlias ss
            :  :  :        +- CTERelationRef 0, true, [ca_county#162, d_qoy#163, d_year#164, store_sales#165], false
            :  :  +- SubqueryAlias ws1
            :  :     +- SubqueryAlias ws
            :  :        +- CTERelationRef 1, true, [ca_county#144, d_qoy#119, d_year#115, web_sales#5], false
            :  +- SubqueryAlias ws2
            :     +- SubqueryAlias ws
            :        +- CTERelationRef 1, true, [ca_county#166, d_qoy#167, d_year#168, web_sales#169], false
            +- SubqueryAlias ws3
               +- SubqueryAlias ws
                  +- CTERelationRef 1, true, [ca_county#170, d_qoy#171, d_year#172, web_sales#173], false

== Optimized Logical Plan ==
Sort [ca_county#69 ASC NULLS FIRST], true
+- Project [ca_county#69, d_year#40, (web_sales#169 / web_sales#5) AS web_q1_q2_increase#0, (store_sales#161 / store_sales#4) AS store_q1_q2_increase#1, (web_sales#173 / web_sales#169) AS web_q2_q3_increase#2, (store_sales#165 / store_sales#161) AS store_q2_q3_increase#3]
   +- Join Inner, ((ca_county#144 = ca_county#731) AND (CASE WHEN (web_sales#169 > 0.0) THEN (web_sales#173 / web_sales#169) END > CASE WHEN (store_sales#161 > 0.0) THEN (store_sales#165 / store_sales#161) END))
      :- Project [ca_county#69, d_year#40, store_sales#4, store_sales#161, store_sales#165, ca_county#144, web_sales#5, web_sales#169]
      :  +- Join Inner, ((ca_county#144 = ca_county#655) AND (CASE WHEN (web_sales#5 > 0.0) THEN (web_sales#169 / web_sales#5) END > CASE WHEN (store_sales#4 > 0.0) THEN (store_sales#161 / store_sales#4) END))
      :     :- Join Inner, (ca_county#69 = ca_county#144)
      :     :  :- Project [ca_county#69, d_year#40, store_sales#4, store_sales#161, store_sales#165]
      :     :  :  +- Join Inner, (ca_county#514 = ca_county#579)
      :     :  :     :- Join Inner, (ca_county#69 = ca_county#514)
      :     :  :     :  :- Aggregate [ca_county#69, d_qoy#44, d_year#40], [ca_county#69, d_year#40, sum(ss_ext_sales_price#26) AS store_sales#4]
      :     :  :     :  :  +- Project [ss_ext_sales_price#26, d_year#40, d_qoy#44, ca_county#69]
      :     :  :     :  :     +- Join Inner, (ss_addr_sk#17 = ca_address_sk#62)
      :     :  :     :  :        :- Project [ss_addr_sk#17, ss_ext_sales_price#26, d_year#40, d_qoy#44]
      :     :  :     :  :        :  +- Join Inner, (ss_sold_date_sk#11 = d_date_sk#34)
      :     :  :     :  :        :     :- Project [ss_sold_date_sk#11, ss_addr_sk#17, ss_ext_sales_price#26]
      :     :  :     :  :        :     :  +- Filter (isnotnull(ss_sold_date_sk#11) AND isnotnull(ss_addr_sk#17))
      :     :  :     :  :        :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#11,ss_sold_time_sk#12,ss_item_sk#13,ss_customer_sk#14,ss_cdemo_sk#15,ss_hdemo_sk#16,ss_addr_sk#17,ss_store_sk#18,ss_promo_sk#19,ss_ticket_number#20,ss_quantity#21,ss_wholesale_cost#22,ss_list_price#23,ss_sales_price#24,ss_ext_discount_amt#25,ss_ext_sales_price#26,ss_ext_wholesale_cost#27,ss_ext_list_price#28,ss_ext_tax#29,ss_coupon_amt#30,ss_net_paid#31,ss_net_paid_inc_tax#32,ss_net_profit#33] parquet
      :     :  :     :  :        :     +- Project [d_date_sk#34, d_year#40, d_qoy#44]
      :     :  :     :  :        :        +- Filter (((isnotnull(d_qoy#44) AND isnotnull(d_year#40)) AND ((d_qoy#44 = 1) AND (d_year#40 = 2000))) AND isnotnull(d_date_sk#34))
      :     :  :     :  :        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#34,d_date_id#35,d_date#36,d_month_seq#37,d_week_seq#38,d_quarter_seq#39,d_year#40,d_dow#41,d_moy#42,d_dom#43,d_qoy#44,d_fy_year#45,d_fy_quarter_seq#46,d_fy_week_seq#47,d_day_name#48,d_quarter_name#49,d_holiday#50,d_weekend#51,d_following_holiday#52,d_first_dom#53,d_last_dom#54,d_same_day_ly#55,d_same_day_lq#56,d_current_day#57,... 4 more fields] parquet
      :     :  :     :  :        +- Project [ca_address_sk#62, ca_county#69]
      :     :  :     :  :           +- Filter (isnotnull(ca_address_sk#62) AND isnotnull(ca_county#69))
      :     :  :     :  :              +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#62,ca_address_id#63,ca_street_number#64,ca_street_name#65,ca_street_type#66,ca_suite_number#67,ca_city#68,ca_county#69,ca_state#70,ca_zip#71,ca_country#72,ca_gmt_offset#73,ca_location_type#74] parquet
      :     :  :     :  +- Aggregate [ca_county#514, d_qoy#489, d_year#485], [ca_county#514, sum(ss_ext_sales_price#471) AS store_sales#161]
      :     :  :     :     +- Project [ss_ext_sales_price#471, d_year#485, d_qoy#489, ca_county#514]
      :     :  :     :        +- Join Inner, (ss_addr_sk#462 = ca_address_sk#507)
      :     :  :     :           :- Project [ss_addr_sk#462, ss_ext_sales_price#471, d_year#485, d_qoy#489]
      :     :  :     :           :  +- Join Inner, (ss_sold_date_sk#456 = d_date_sk#479)
      :     :  :     :           :     :- Project [ss_sold_date_sk#456, ss_addr_sk#462, ss_ext_sales_price#471]
      :     :  :     :           :     :  +- Filter (isnotnull(ss_sold_date_sk#456) AND isnotnull(ss_addr_sk#462))
      :     :  :     :           :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#456,ss_sold_time_sk#457,ss_item_sk#458,ss_customer_sk#459,ss_cdemo_sk#460,ss_hdemo_sk#461,ss_addr_sk#462,ss_store_sk#463,ss_promo_sk#464,ss_ticket_number#465,ss_quantity#466,ss_wholesale_cost#467,ss_list_price#468,ss_sales_price#469,ss_ext_discount_amt#470,ss_ext_sales_price#471,ss_ext_wholesale_cost#472,ss_ext_list_price#473,ss_ext_tax#474,ss_coupon_amt#475,ss_net_paid#476,ss_net_paid_inc_tax#477,ss_net_profit#478] parquet
      :     :  :     :           :     +- Project [d_date_sk#479, d_year#485, d_qoy#489]
      :     :  :     :           :        +- Filter (((isnotnull(d_qoy#489) AND isnotnull(d_year#485)) AND ((d_qoy#489 = 2) AND (d_year#485 = 2000))) AND isnotnull(d_date_sk#479))
      :     :  :     :           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#479,d_date_id#480,d_date#481,d_month_seq#482,d_week_seq#483,d_quarter_seq#484,d_year#485,d_dow#486,d_moy#487,d_dom#488,d_qoy#489,d_fy_year#490,d_fy_quarter_seq#491,d_fy_week_seq#492,d_day_name#493,d_quarter_name#494,d_holiday#495,d_weekend#496,d_following_holiday#497,d_first_dom#498,d_last_dom#499,d_same_day_ly#500,d_same_day_lq#501,d_current_day#502,... 4 more fields] parquet
      :     :  :     :           +- Project [ca_address_sk#507, ca_county#514]
      :     :  :     :              +- Filter (isnotnull(ca_address_sk#507) AND isnotnull(ca_county#514))
      :     :  :     :                 +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#507,ca_address_id#508,ca_street_number#509,ca_street_name#510,ca_street_type#511,ca_suite_number#512,ca_city#513,ca_county#514,ca_state#515,ca_zip#516,ca_country#517,ca_gmt_offset#518,ca_location_type#519] parquet
      :     :  :     +- Aggregate [ca_county#579, d_qoy#554, d_year#550], [ca_county#579, sum(ss_ext_sales_price#536) AS store_sales#165]
      :     :  :        +- Project [ss_ext_sales_price#536, d_year#550, d_qoy#554, ca_county#579]
      :     :  :           +- Join Inner, (ss_addr_sk#527 = ca_address_sk#572)
      :     :  :              :- Project [ss_addr_sk#527, ss_ext_sales_price#536, d_year#550, d_qoy#554]
      :     :  :              :  +- Join Inner, (ss_sold_date_sk#521 = d_date_sk#544)
      :     :  :              :     :- Project [ss_sold_date_sk#521, ss_addr_sk#527, ss_ext_sales_price#536]
      :     :  :              :     :  +- Filter (isnotnull(ss_sold_date_sk#521) AND isnotnull(ss_addr_sk#527))
      :     :  :              :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#521,ss_sold_time_sk#522,ss_item_sk#523,ss_customer_sk#524,ss_cdemo_sk#525,ss_hdemo_sk#526,ss_addr_sk#527,ss_store_sk#528,ss_promo_sk#529,ss_ticket_number#530,ss_quantity#531,ss_wholesale_cost#532,ss_list_price#533,ss_sales_price#534,ss_ext_discount_amt#535,ss_ext_sales_price#536,ss_ext_wholesale_cost#537,ss_ext_list_price#538,ss_ext_tax#539,ss_coupon_amt#540,ss_net_paid#541,ss_net_paid_inc_tax#542,ss_net_profit#543] parquet
      :     :  :              :     +- Project [d_date_sk#544, d_year#550, d_qoy#554]
      :     :  :              :        +- Filter (((isnotnull(d_qoy#554) AND isnotnull(d_year#550)) AND ((d_qoy#554 = 3) AND (d_year#550 = 2000))) AND isnotnull(d_date_sk#544))
      :     :  :              :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#544,d_date_id#545,d_date#546,d_month_seq#547,d_week_seq#548,d_quarter_seq#549,d_year#550,d_dow#551,d_moy#552,d_dom#553,d_qoy#554,d_fy_year#555,d_fy_quarter_seq#556,d_fy_week_seq#557,d_day_name#558,d_quarter_name#559,d_holiday#560,d_weekend#561,d_following_holiday#562,d_first_dom#563,d_last_dom#564,d_same_day_ly#565,d_same_day_lq#566,d_current_day#567,... 4 more fields] parquet
      :     :  :              +- Project [ca_address_sk#572, ca_county#579]
      :     :  :                 +- Filter (isnotnull(ca_address_sk#572) AND isnotnull(ca_county#579))
      :     :  :                    +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#572,ca_address_id#573,ca_street_number#574,ca_street_name#575,ca_street_type#576,ca_suite_number#577,ca_city#578,ca_county#579,ca_state#580,ca_zip#581,ca_country#582,ca_gmt_offset#583,ca_location_type#584] parquet
      :     :  +- Aggregate [ca_county#144, d_qoy#119, d_year#115], [ca_county#144, sum(ws_ext_sales_price#98) AS web_sales#5]
      :     :     +- Project [ws_ext_sales_price#98, d_year#115, d_qoy#119, ca_county#144]
      :     :        +- Join Inner, (ws_bill_addr_sk#82 = ca_address_sk#137)
      :     :           :- Project [ws_bill_addr_sk#82, ws_ext_sales_price#98, d_year#115, d_qoy#119]
      :     :           :  +- Join Inner, (ws_sold_date_sk#75 = d_date_sk#109)
      :     :           :     :- Project [ws_sold_date_sk#75, ws_bill_addr_sk#82, ws_ext_sales_price#98]
      :     :           :     :  +- Filter (isnotnull(ws_sold_date_sk#75) AND isnotnull(ws_bill_addr_sk#82))
      :     :           :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#75,ws_sold_time_sk#76,ws_ship_date_sk#77,ws_item_sk#78,ws_bill_customer_sk#79,ws_bill_cdemo_sk#80,ws_bill_hdemo_sk#81,ws_bill_addr_sk#82,ws_ship_customer_sk#83,ws_ship_cdemo_sk#84,ws_ship_hdemo_sk#85,ws_ship_addr_sk#86,ws_web_page_sk#87,ws_web_site_sk#88,ws_ship_mode_sk#89,ws_warehouse_sk#90,ws_promo_sk#91,ws_order_number#92,ws_quantity#93,ws_wholesale_cost#94,ws_list_price#95,ws_sales_price#96,ws_ext_discount_amt#97,ws_ext_sales_price#98,... 10 more fields] parquet
      :     :           :     +- Project [d_date_sk#109, d_year#115, d_qoy#119]
      :     :           :        +- Filter (((isnotnull(d_qoy#119) AND isnotnull(d_year#115)) AND ((d_qoy#119 = 1) AND (d_year#115 = 2000))) AND isnotnull(d_date_sk#109))
      :     :           :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#109,d_date_id#110,d_date#111,d_month_seq#112,d_week_seq#113,d_quarter_seq#114,d_year#115,d_dow#116,d_moy#117,d_dom#118,d_qoy#119,d_fy_year#120,d_fy_quarter_seq#121,d_fy_week_seq#122,d_day_name#123,d_quarter_name#124,d_holiday#125,d_weekend#126,d_following_holiday#127,d_first_dom#128,d_last_dom#129,d_same_day_ly#130,d_same_day_lq#131,d_current_day#132,... 4 more fields] parquet
      :     :           +- Project [ca_address_sk#137, ca_county#144]
      :     :              +- Filter (isnotnull(ca_address_sk#137) AND isnotnull(ca_county#144))
      :     :                 +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#137,ca_address_id#138,ca_street_number#139,ca_street_name#140,ca_street_type#141,ca_suite_number#142,ca_city#143,ca_county#144,ca_state#145,ca_zip#146,ca_country#147,ca_gmt_offset#148,ca_location_type#149] parquet
      :     +- Aggregate [ca_county#655, d_qoy#630, d_year#626], [ca_county#655, sum(ws_ext_sales_price#609) AS web_sales#169]
      :        +- Project [ws_ext_sales_price#609, d_year#626, d_qoy#630, ca_county#655]
      :           +- Join Inner, (ws_bill_addr_sk#593 = ca_address_sk#648)
      :              :- Project [ws_bill_addr_sk#593, ws_ext_sales_price#609, d_year#626, d_qoy#630]
      :              :  +- Join Inner, (ws_sold_date_sk#586 = d_date_sk#620)
      :              :     :- Project [ws_sold_date_sk#586, ws_bill_addr_sk#593, ws_ext_sales_price#609]
      :              :     :  +- Filter (isnotnull(ws_sold_date_sk#586) AND isnotnull(ws_bill_addr_sk#593))
      :              :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#586,ws_sold_time_sk#587,ws_ship_date_sk#588,ws_item_sk#589,ws_bill_customer_sk#590,ws_bill_cdemo_sk#591,ws_bill_hdemo_sk#592,ws_bill_addr_sk#593,ws_ship_customer_sk#594,ws_ship_cdemo_sk#595,ws_ship_hdemo_sk#596,ws_ship_addr_sk#597,ws_web_page_sk#598,ws_web_site_sk#599,ws_ship_mode_sk#600,ws_warehouse_sk#601,ws_promo_sk#602,ws_order_number#603,ws_quantity#604,ws_wholesale_cost#605,ws_list_price#606,ws_sales_price#607,ws_ext_discount_amt#608,ws_ext_sales_price#609,... 10 more fields] parquet
      :              :     +- Project [d_date_sk#620, d_year#626, d_qoy#630]
      :              :        +- Filter (((isnotnull(d_qoy#630) AND isnotnull(d_year#626)) AND ((d_qoy#630 = 2) AND (d_year#626 = 2000))) AND isnotnull(d_date_sk#620))
      :              :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#620,d_date_id#621,d_date#622,d_month_seq#623,d_week_seq#624,d_quarter_seq#625,d_year#626,d_dow#627,d_moy#628,d_dom#629,d_qoy#630,d_fy_year#631,d_fy_quarter_seq#632,d_fy_week_seq#633,d_day_name#634,d_quarter_name#635,d_holiday#636,d_weekend#637,d_following_holiday#638,d_first_dom#639,d_last_dom#640,d_same_day_ly#641,d_same_day_lq#642,d_current_day#643,... 4 more fields] parquet
      :              +- Project [ca_address_sk#648, ca_county#655]
      :                 +- Filter (isnotnull(ca_address_sk#648) AND isnotnull(ca_county#655))
      :                    +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#648,ca_address_id#649,ca_street_number#650,ca_street_name#651,ca_street_type#652,ca_suite_number#653,ca_city#654,ca_county#655,ca_state#656,ca_zip#657,ca_country#658,ca_gmt_offset#659,ca_location_type#660] parquet
      +- Aggregate [ca_county#731, d_qoy#706, d_year#702], [ca_county#731, sum(ws_ext_sales_price#685) AS web_sales#173]
         +- Project [ws_ext_sales_price#685, d_year#702, d_qoy#706, ca_county#731]
            +- Join Inner, (ws_bill_addr_sk#669 = ca_address_sk#724)
               :- Project [ws_bill_addr_sk#669, ws_ext_sales_price#685, d_year#702, d_qoy#706]
               :  +- Join Inner, (ws_sold_date_sk#662 = d_date_sk#696)
               :     :- Project [ws_sold_date_sk#662, ws_bill_addr_sk#669, ws_ext_sales_price#685]
               :     :  +- Filter (isnotnull(ws_sold_date_sk#662) AND isnotnull(ws_bill_addr_sk#669))
               :     :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#662,ws_sold_time_sk#663,ws_ship_date_sk#664,ws_item_sk#665,ws_bill_customer_sk#666,ws_bill_cdemo_sk#667,ws_bill_hdemo_sk#668,ws_bill_addr_sk#669,ws_ship_customer_sk#670,ws_ship_cdemo_sk#671,ws_ship_hdemo_sk#672,ws_ship_addr_sk#673,ws_web_page_sk#674,ws_web_site_sk#675,ws_ship_mode_sk#676,ws_warehouse_sk#677,ws_promo_sk#678,ws_order_number#679,ws_quantity#680,ws_wholesale_cost#681,ws_list_price#682,ws_sales_price#683,ws_ext_discount_amt#684,ws_ext_sales_price#685,... 10 more fields] parquet
               :     +- Project [d_date_sk#696, d_year#702, d_qoy#706]
               :        +- Filter (((isnotnull(d_qoy#706) AND isnotnull(d_year#702)) AND ((d_qoy#706 = 3) AND (d_year#702 = 2000))) AND isnotnull(d_date_sk#696))
               :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#696,d_date_id#697,d_date#698,d_month_seq#699,d_week_seq#700,d_quarter_seq#701,d_year#702,d_dow#703,d_moy#704,d_dom#705,d_qoy#706,d_fy_year#707,d_fy_quarter_seq#708,d_fy_week_seq#709,d_day_name#710,d_quarter_name#711,d_holiday#712,d_weekend#713,d_following_holiday#714,d_first_dom#715,d_last_dom#716,d_same_day_ly#717,d_same_day_lq#718,d_current_day#719,... 4 more fields] parquet
               +- Project [ca_address_sk#724, ca_county#731]
                  +- Filter (isnotnull(ca_address_sk#724) AND isnotnull(ca_county#731))
                     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#724,ca_address_id#725,ca_street_number#726,ca_street_name#727,ca_street_type#728,ca_suite_number#729,ca_city#730,ca_county#731,ca_state#732,ca_zip#733,ca_country#734,ca_gmt_offset#735,ca_location_type#736] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [ca_county#69 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(ca_county#69 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=466]
      +- Project [ca_county#69, d_year#40, (web_sales#169 / web_sales#5) AS web_q1_q2_increase#0, (store_sales#161 / store_sales#4) AS store_q1_q2_increase#1, (web_sales#173 / web_sales#169) AS web_q2_q3_increase#2, (store_sales#165 / store_sales#161) AS store_q2_q3_increase#3]
         +- SortMergeJoin [ca_county#144], [ca_county#731], Inner, (CASE WHEN (web_sales#169 > 0.0) THEN (web_sales#173 / web_sales#169) END > CASE WHEN (store_sales#161 > 0.0) THEN (store_sales#165 / store_sales#161) END)
            :- Project [ca_county#69, d_year#40, store_sales#4, store_sales#161, store_sales#165, ca_county#144, web_sales#5, web_sales#169]
            :  +- SortMergeJoin [ca_county#144], [ca_county#655], Inner, (CASE WHEN (web_sales#5 > 0.0) THEN (web_sales#169 / web_sales#5) END > CASE WHEN (store_sales#4 > 0.0) THEN (store_sales#161 / store_sales#4) END)
            :     :- SortMergeJoin [ca_county#69], [ca_county#144], Inner
            :     :  :- Project [ca_county#69, d_year#40, store_sales#4, store_sales#161, store_sales#165]
            :     :  :  +- SortMergeJoin [ca_county#514], [ca_county#579], Inner
            :     :  :     :- SortMergeJoin [ca_county#69], [ca_county#514], Inner
            :     :  :     :  :- Sort [ca_county#69 ASC NULLS FIRST], false, 0
            :     :  :     :  :  +- Exchange hashpartitioning(ca_county#69, 200), ENSURE_REQUIREMENTS, [plan_id=393]
            :     :  :     :  :     +- HashAggregate(keys=[ca_county#69, d_qoy#44, d_year#40], functions=[sum(ss_ext_sales_price#26)], output=[ca_county#69, d_year#40, store_sales#4])
            :     :  :     :  :        +- Exchange hashpartitioning(ca_county#69, d_qoy#44, d_year#40, 200), ENSURE_REQUIREMENTS, [plan_id=378]
            :     :  :     :  :           +- HashAggregate(keys=[ca_county#69, d_qoy#44, d_year#40], functions=[partial_sum(ss_ext_sales_price#26)], output=[ca_county#69, d_qoy#44, d_year#40, sum#739])
            :     :  :     :  :              +- Project [ss_ext_sales_price#26, d_year#40, d_qoy#44, ca_county#69]
            :     :  :     :  :                 +- BroadcastHashJoin [ss_addr_sk#17], [ca_address_sk#62], Inner, BuildRight, false
            :     :  :     :  :                    :- Project [ss_addr_sk#17, ss_ext_sales_price#26, d_year#40, d_qoy#44]
            :     :  :     :  :                    :  +- BroadcastHashJoin [ss_sold_date_sk#11], [d_date_sk#34], Inner, BuildRight, false
            :     :  :     :  :                    :     :- Filter (isnotnull(ss_sold_date_sk#11) AND isnotnull(ss_addr_sk#17))
            :     :  :     :  :                    :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#11,ss_addr_sk#17,ss_ext_sales_price#26] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#11), isnotnull(ss_addr_sk#17)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_addr_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_addr_sk:int,ss_ext_sales_price:double>
            :     :  :     :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=369]
            :     :  :     :  :                    :        +- Filter ((((isnotnull(d_qoy#44) AND isnotnull(d_year#40)) AND (d_qoy#44 = 1)) AND (d_year#40 = 2000)) AND isnotnull(d_date_sk#34))
            :     :  :     :  :                    :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#34,d_year#40,d_qoy#44] Batched: true, DataFilters: [isnotnull(d_qoy#44), isnotnull(d_year#40), (d_qoy#44 = 1), (d_year#40 = 2000), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,1), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
            :     :  :     :  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=373]
            :     :  :     :  :                       +- Filter (isnotnull(ca_address_sk#62) AND isnotnull(ca_county#69))
            :     :  :     :  :                          +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#62,ca_county#69] Batched: true, DataFilters: [isnotnull(ca_address_sk#62), isnotnull(ca_county#69)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_county)], ReadSchema: struct<ca_address_sk:int,ca_county:string>
            :     :  :     :  +- Sort [ca_county#514 ASC NULLS FIRST], false, 0
            :     :  :     :     +- Exchange hashpartitioning(ca_county#514, 200), ENSURE_REQUIREMENTS, [plan_id=394]
            :     :  :     :        +- HashAggregate(keys=[ca_county#514, d_qoy#489, d_year#485], functions=[sum(ss_ext_sales_price#471)], output=[ca_county#514, store_sales#161])
            :     :  :     :           +- Exchange hashpartitioning(ca_county#514, d_qoy#489, d_year#485, 200), ENSURE_REQUIREMENTS, [plan_id=389]
            :     :  :     :              +- HashAggregate(keys=[ca_county#514, d_qoy#489, d_year#485], functions=[partial_sum(ss_ext_sales_price#471)], output=[ca_county#514, d_qoy#489, d_year#485, sum#741])
            :     :  :     :                 +- Project [ss_ext_sales_price#471, d_year#485, d_qoy#489, ca_county#514]
            :     :  :     :                    +- BroadcastHashJoin [ss_addr_sk#462], [ca_address_sk#507], Inner, BuildRight, false
            :     :  :     :                       :- Project [ss_addr_sk#462, ss_ext_sales_price#471, d_year#485, d_qoy#489]
            :     :  :     :                       :  +- BroadcastHashJoin [ss_sold_date_sk#456], [d_date_sk#479], Inner, BuildRight, false
            :     :  :     :                       :     :- Filter (isnotnull(ss_sold_date_sk#456) AND isnotnull(ss_addr_sk#462))
            :     :  :     :                       :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#456,ss_addr_sk#462,ss_ext_sales_price#471] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#456), isnotnull(ss_addr_sk#462)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_addr_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_addr_sk:int,ss_ext_sales_price:double>
            :     :  :     :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=380]
            :     :  :     :                       :        +- Filter ((((isnotnull(d_qoy#489) AND isnotnull(d_year#485)) AND (d_qoy#489 = 2)) AND (d_year#485 = 2000)) AND isnotnull(d_date_sk#479))
            :     :  :     :                       :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#479,d_year#485,d_qoy#489] Batched: true, DataFilters: [isnotnull(d_qoy#489), isnotnull(d_year#485), (d_qoy#489 = 2), (d_year#485 = 2000), isnotnull(d_d..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,2), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
            :     :  :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=384]
            :     :  :     :                          +- Filter (isnotnull(ca_address_sk#507) AND isnotnull(ca_county#514))
            :     :  :     :                             +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#507,ca_county#514] Batched: true, DataFilters: [isnotnull(ca_address_sk#507), isnotnull(ca_county#514)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_county)], ReadSchema: struct<ca_address_sk:int,ca_county:string>
            :     :  :     +- Sort [ca_county#579 ASC NULLS FIRST], false, 0
            :     :  :        +- Exchange hashpartitioning(ca_county#579, 200), ENSURE_REQUIREMENTS, [plan_id=411]
            :     :  :           +- HashAggregate(keys=[ca_county#579, d_qoy#554, d_year#550], functions=[sum(ss_ext_sales_price#536)], output=[ca_county#579, store_sales#165])
            :     :  :              +- Exchange hashpartitioning(ca_county#579, d_qoy#554, d_year#550, 200), ENSURE_REQUIREMENTS, [plan_id=407]
            :     :  :                 +- HashAggregate(keys=[ca_county#579, d_qoy#554, d_year#550], functions=[partial_sum(ss_ext_sales_price#536)], output=[ca_county#579, d_qoy#554, d_year#550, sum#743])
            :     :  :                    +- Project [ss_ext_sales_price#536, d_year#550, d_qoy#554, ca_county#579]
            :     :  :                       +- BroadcastHashJoin [ss_addr_sk#527], [ca_address_sk#572], Inner, BuildRight, false
            :     :  :                          :- Project [ss_addr_sk#527, ss_ext_sales_price#536, d_year#550, d_qoy#554]
            :     :  :                          :  +- BroadcastHashJoin [ss_sold_date_sk#521], [d_date_sk#544], Inner, BuildRight, false
            :     :  :                          :     :- Filter (isnotnull(ss_sold_date_sk#521) AND isnotnull(ss_addr_sk#527))
            :     :  :                          :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#521,ss_addr_sk#527,ss_ext_sales_price#536] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#521), isnotnull(ss_addr_sk#527)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_addr_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_addr_sk:int,ss_ext_sales_price:double>
            :     :  :                          :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=398]
            :     :  :                          :        +- Filter ((((isnotnull(d_qoy#554) AND isnotnull(d_year#550)) AND (d_qoy#554 = 3)) AND (d_year#550 = 2000)) AND isnotnull(d_date_sk#544))
            :     :  :                          :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#544,d_year#550,d_qoy#554] Batched: true, DataFilters: [isnotnull(d_qoy#554), isnotnull(d_year#550), (d_qoy#554 = 3), (d_year#550 = 2000), isnotnull(d_d..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,3), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
            :     :  :                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=402]
            :     :  :                             +- Filter (isnotnull(ca_address_sk#572) AND isnotnull(ca_county#579))
            :     :  :                                +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#572,ca_county#579] Batched: true, DataFilters: [isnotnull(ca_address_sk#572), isnotnull(ca_county#579)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_county)], ReadSchema: struct<ca_address_sk:int,ca_county:string>
            :     :  +- Sort [ca_county#144 ASC NULLS FIRST], false, 0
            :     :     +- Exchange hashpartitioning(ca_county#144, 200), ENSURE_REQUIREMENTS, [plan_id=428]
            :     :        +- HashAggregate(keys=[ca_county#144, d_qoy#119, d_year#115], functions=[sum(ws_ext_sales_price#98)], output=[ca_county#144, web_sales#5])
            :     :           +- Exchange hashpartitioning(ca_county#144, d_qoy#119, d_year#115, 200), ENSURE_REQUIREMENTS, [plan_id=424]
            :     :              +- HashAggregate(keys=[ca_county#144, d_qoy#119, d_year#115], functions=[partial_sum(ws_ext_sales_price#98)], output=[ca_county#144, d_qoy#119, d_year#115, sum#745])
            :     :                 +- Project [ws_ext_sales_price#98, d_year#115, d_qoy#119, ca_county#144]
            :     :                    +- BroadcastHashJoin [ws_bill_addr_sk#82], [ca_address_sk#137], Inner, BuildRight, false
            :     :                       :- Project [ws_bill_addr_sk#82, ws_ext_sales_price#98, d_year#115, d_qoy#119]
            :     :                       :  +- BroadcastHashJoin [ws_sold_date_sk#75], [d_date_sk#109], Inner, BuildRight, false
            :     :                       :     :- Filter (isnotnull(ws_sold_date_sk#75) AND isnotnull(ws_bill_addr_sk#82))
            :     :                       :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#75,ws_bill_addr_sk#82,ws_ext_sales_price#98] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#75), isnotnull(ws_bill_addr_sk#82)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_addr_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_addr_sk:int,ws_ext_sales_price:double>
            :     :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=415]
            :     :                       :        +- Filter ((((isnotnull(d_qoy#119) AND isnotnull(d_year#115)) AND (d_qoy#119 = 1)) AND (d_year#115 = 2000)) AND isnotnull(d_date_sk#109))
            :     :                       :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#109,d_year#115,d_qoy#119] Batched: true, DataFilters: [isnotnull(d_qoy#119), isnotnull(d_year#115), (d_qoy#119 = 1), (d_year#115 = 2000), isnotnull(d_d..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,1), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
            :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=419]
            :     :                          +- Filter (isnotnull(ca_address_sk#137) AND isnotnull(ca_county#144))
            :     :                             +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#137,ca_county#144] Batched: true, DataFilters: [isnotnull(ca_address_sk#137), isnotnull(ca_county#144)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_county)], ReadSchema: struct<ca_address_sk:int,ca_county:string>
            :     +- Sort [ca_county#655 ASC NULLS FIRST], false, 0
            :        +- Exchange hashpartitioning(ca_county#655, 200), ENSURE_REQUIREMENTS, [plan_id=444]
            :           +- HashAggregate(keys=[ca_county#655, d_qoy#630, d_year#626], functions=[sum(ws_ext_sales_price#609)], output=[ca_county#655, web_sales#169])
            :              +- Exchange hashpartitioning(ca_county#655, d_qoy#630, d_year#626, 200), ENSURE_REQUIREMENTS, [plan_id=440]
            :                 +- HashAggregate(keys=[ca_county#655, d_qoy#630, d_year#626], functions=[partial_sum(ws_ext_sales_price#609)], output=[ca_county#655, d_qoy#630, d_year#626, sum#747])
            :                    +- Project [ws_ext_sales_price#609, d_year#626, d_qoy#630, ca_county#655]
            :                       +- BroadcastHashJoin [ws_bill_addr_sk#593], [ca_address_sk#648], Inner, BuildRight, false
            :                          :- Project [ws_bill_addr_sk#593, ws_ext_sales_price#609, d_year#626, d_qoy#630]
            :                          :  +- BroadcastHashJoin [ws_sold_date_sk#586], [d_date_sk#620], Inner, BuildRight, false
            :                          :     :- Filter (isnotnull(ws_sold_date_sk#586) AND isnotnull(ws_bill_addr_sk#593))
            :                          :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#586,ws_bill_addr_sk#593,ws_ext_sales_price#609] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#586), isnotnull(ws_bill_addr_sk#593)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_addr_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_addr_sk:int,ws_ext_sales_price:double>
            :                          :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=431]
            :                          :        +- Filter ((((isnotnull(d_qoy#630) AND isnotnull(d_year#626)) AND (d_qoy#630 = 2)) AND (d_year#626 = 2000)) AND isnotnull(d_date_sk#620))
            :                          :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#620,d_year#626,d_qoy#630] Batched: true, DataFilters: [isnotnull(d_qoy#630), isnotnull(d_year#626), (d_qoy#630 = 2), (d_year#626 = 2000), isnotnull(d_d..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,2), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
            :                          +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=435]
            :                             +- Filter (isnotnull(ca_address_sk#648) AND isnotnull(ca_county#655))
            :                                +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#648,ca_county#655] Batched: true, DataFilters: [isnotnull(ca_address_sk#648), isnotnull(ca_county#655)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_county)], ReadSchema: struct<ca_address_sk:int,ca_county:string>
            +- Sort [ca_county#731 ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning(ca_county#731, 200), ENSURE_REQUIREMENTS, [plan_id=461]
                  +- HashAggregate(keys=[ca_county#731, d_qoy#706, d_year#702], functions=[sum(ws_ext_sales_price#685)], output=[ca_county#731, web_sales#173])
                     +- Exchange hashpartitioning(ca_county#731, d_qoy#706, d_year#702, 200), ENSURE_REQUIREMENTS, [plan_id=457]
                        +- HashAggregate(keys=[ca_county#731, d_qoy#706, d_year#702], functions=[partial_sum(ws_ext_sales_price#685)], output=[ca_county#731, d_qoy#706, d_year#702, sum#749])
                           +- Project [ws_ext_sales_price#685, d_year#702, d_qoy#706, ca_county#731]
                              +- BroadcastHashJoin [ws_bill_addr_sk#669], [ca_address_sk#724], Inner, BuildRight, false
                                 :- Project [ws_bill_addr_sk#669, ws_ext_sales_price#685, d_year#702, d_qoy#706]
                                 :  +- BroadcastHashJoin [ws_sold_date_sk#662], [d_date_sk#696], Inner, BuildRight, false
                                 :     :- Filter (isnotnull(ws_sold_date_sk#662) AND isnotnull(ws_bill_addr_sk#669))
                                 :     :  +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#662,ws_bill_addr_sk#669,ws_ext_sales_price#685] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#662), isnotnull(ws_bill_addr_sk#669)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_addr_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_addr_sk:int,ws_ext_sales_price:double>
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=448]
                                 :        +- Filter ((((isnotnull(d_qoy#706) AND isnotnull(d_year#702)) AND (d_qoy#706 = 3)) AND (d_year#702 = 2000)) AND isnotnull(d_date_sk#696))
                                 :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#696,d_year#702,d_qoy#706] Batched: true, DataFilters: [isnotnull(d_qoy#706), isnotnull(d_year#702), (d_qoy#706 = 3), (d_year#702 = 2000), isnotnull(d_d..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,3), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=452]
                                    +- Filter (isnotnull(ca_address_sk#724) AND isnotnull(ca_county#731))
                                       +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#724,ca_county#731] Batched: true, DataFilters: [isnotnull(ca_address_sk#724), isnotnull(ca_county#731)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_county)], ReadSchema: struct<ca_address_sk:int,ca_county:string>

Time taken: 3.417 seconds, Fetched 1 row(s)
