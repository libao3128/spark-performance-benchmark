== Parsed Logical Plan ==
CTE [ss, ws]
:  :- 'SubqueryAlias `ss`
:  :  +- 'Aggregate ['ca_county, 'd_qoy, 'd_year], ['ca_county, 'd_qoy, 'd_year, 'sum('ss_ext_sales_price) AS store_sales#4]
:  :     +- 'Filter (('ss_sold_date_sk = 'd_date_sk) && ('ss_addr_sk = 'ca_address_sk))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation `store_sales`
:  :           :  +- 'UnresolvedRelation `date_dim`
:  :           +- 'UnresolvedRelation `customer_address`
:  +- 'SubqueryAlias `ws`
:     +- 'Aggregate ['ca_county, 'd_qoy, 'd_year], ['ca_county, 'd_qoy, 'd_year, 'sum('ws_ext_sales_price) AS web_sales#5]
:        +- 'Filter (('ws_sold_date_sk = 'd_date_sk) && ('ws_bill_addr_sk = 'ca_address_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation `web_sales`
:              :  +- 'UnresolvedRelation `date_dim`
:              +- 'UnresolvedRelation `customer_address`
+- 'Sort ['ss1.ca_county ASC NULLS FIRST], true
   +- 'Project ['ss1.ca_county, 'ss1.d_year, ('ws2.web_sales / 'ws1.web_sales) AS web_q1_q2_increase#0, ('ss2.store_sales / 'ss1.store_sales) AS store_q1_q2_increase#1, ('ws3.web_sales / 'ws2.web_sales) AS web_q2_q3_increase#2, ('ss3.store_sales / 'ss2.store_sales) AS store_q2_q3_increase#3]
      +- 'Filter (((((('ss1.d_qoy = 1) && ('ss1.d_year = 2000)) && ('ss1.ca_county = 'ss2.ca_county)) && (('ss2.d_qoy = 2) && ('ss2.d_year = 2000))) && (((('ss2.ca_county = 'ss3.ca_county) && ('ss3.d_qoy = 3)) && ('ss3.d_year = 2000)) && (('ss1.ca_county = 'ws1.ca_county) && ('ws1.d_qoy = 1)))) && ((((('ws1.d_year = 2000) && ('ws1.ca_county = 'ws2.ca_county)) && ('ws2.d_qoy = 2)) && (('ws2.d_year = 2000) && ('ws1.ca_county = 'ws3.ca_county))) && ((('ws3.d_qoy = 3) && ('ws3.d_year = 2000)) && ((CASE WHEN ('ws1.web_sales > 0) THEN ('ws2.web_sales / 'ws1.web_sales) ELSE null END > CASE WHEN ('ss1.store_sales > 0) THEN ('ss2.store_sales / 'ss1.store_sales) ELSE null END) && (CASE WHEN ('ws2.web_sales > 0) THEN ('ws3.web_sales / 'ws2.web_sales) ELSE null END > CASE WHEN ('ss2.store_sales > 0) THEN ('ss3.store_sales / 'ss2.store_sales) ELSE null END)))))
         +- 'Join Inner
            :- 'Join Inner
            :  :- 'Join Inner
            :  :  :- 'Join Inner
            :  :  :  :- 'Join Inner
            :  :  :  :  :- 'SubqueryAlias `ss1`
            :  :  :  :  :  +- 'UnresolvedRelation `ss`
            :  :  :  :  +- 'SubqueryAlias `ss2`
            :  :  :  :     +- 'UnresolvedRelation `ss`
            :  :  :  +- 'SubqueryAlias `ss3`
            :  :  :     +- 'UnresolvedRelation `ss`
            :  :  +- 'SubqueryAlias `ws1`
            :  :     +- 'UnresolvedRelation `ws`
            :  +- 'SubqueryAlias `ws2`
            :     +- 'UnresolvedRelation `ws`
            +- 'SubqueryAlias `ws3`
               +- 'UnresolvedRelation `ws`

== Analyzed Logical Plan ==
ca_county: string, d_year: int, web_q1_q2_increase: double, store_q1_q2_increase: double, web_q2_q3_increase: double, store_q2_q3_increase: double
Sort [ca_county#66 ASC NULLS FIRST], true
+- Project [ca_county#66, d_year#37, (web_sales#354 / web_sales#5) AS web_q1_q2_increase#0, (store_sales#108 / store_sales#4) AS store_q1_q2_increase#1, (web_sales#450 / web_sales#354) AS web_q2_q3_increase#2, (store_sales#204 / store_sales#108) AS store_q2_q3_increase#3]
   +- Filter ((((((d_qoy#41 = 1) && (d_year#37 = 2000)) && (ca_county#66 = ca_county#198)) && ((d_qoy#160 = 2) && (d_year#156 = 2000))) && ((((ca_county#198 = ca_county#294) && (d_qoy#256 = 3)) && (d_year#252 = 2000)) && ((ca_county#66 = ca_county#348) && (d_qoy#310 = 1)))) && (((((d_year#306 = 2000) && (ca_county#348 = ca_county#444)) && (d_qoy#406 = 2)) && ((d_year#402 = 2000) && (ca_county#348 = ca_county#540))) && (((d_qoy#502 = 3) && (d_year#498 = 2000)) && ((CASE WHEN (web_sales#5 > cast(0 as double)) THEN (web_sales#354 / web_sales#5) ELSE cast(null as double) END > CASE WHEN (store_sales#4 > cast(0 as double)) THEN (store_sales#108 / store_sales#4) ELSE cast(null as double) END) && (CASE WHEN (web_sales#354 > cast(0 as double)) THEN (web_sales#450 / web_sales#354) ELSE cast(null as double) END > CASE WHEN (store_sales#108 > cast(0 as double)) THEN (store_sales#204 / store_sales#108) ELSE cast(null as double) END)))))
      +- Join Inner
         :- Join Inner
         :  :- Join Inner
         :  :  :- Join Inner
         :  :  :  :- Join Inner
         :  :  :  :  :- SubqueryAlias `ss1`
         :  :  :  :  :  +- SubqueryAlias `ss`
         :  :  :  :  :     +- Aggregate [ca_county#66, d_qoy#41, d_year#37], [ca_county#66, d_qoy#41, d_year#37, sum(ss_ext_sales_price#23) AS store_sales#4]
         :  :  :  :  :        +- Filter ((ss_sold_date_sk#8 = d_date_sk#31) && (ss_addr_sk#14 = ca_address_sk#59))
         :  :  :  :  :           +- Join Inner
         :  :  :  :  :              :- Join Inner
         :  :  :  :  :              :  :- SubqueryAlias `tpcds`.`store_sales`
         :  :  :  :  :              :  :  +- Relation[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
         :  :  :  :  :              :  +- SubqueryAlias `tpcds`.`date_dim`
         :  :  :  :  :              :     +- Relation[d_date_sk#31,d_date_id#32,d_date#33,d_month_seq#34,d_week_seq#35,d_quarter_seq#36,d_year#37,d_dow#38,d_moy#39,d_dom#40,d_qoy#41,d_fy_year#42,d_fy_quarter_seq#43,d_fy_week_seq#44,d_day_name#45,d_quarter_name#46,d_holiday#47,d_weekend#48,d_following_holiday#49,d_first_dom#50,d_last_dom#51,d_same_day_ly#52,d_same_day_lq#53,d_current_day#54,... 4 more fields] parquet
         :  :  :  :  :              +- SubqueryAlias `tpcds`.`customer_address`
         :  :  :  :  :                 +- Relation[ca_address_sk#59,ca_address_id#60,ca_street_number#61,ca_street_name#62,ca_street_type#63,ca_suite_number#64,ca_city#65,ca_county#66,ca_state#67,ca_zip#68,ca_country#69,ca_gmt_offset#70,ca_location_type#71] parquet
         :  :  :  :  +- SubqueryAlias `ss2`
         :  :  :  :     +- SubqueryAlias `ss`
         :  :  :  :        +- Aggregate [ca_county#198, d_qoy#160, d_year#156], [ca_county#198, d_qoy#160, d_year#156, sum(ss_ext_sales_price#23) AS store_sales#108]
         :  :  :  :           +- Filter ((ss_sold_date_sk#8 = d_date_sk#150) && (ss_addr_sk#14 = ca_address_sk#191))
         :  :  :  :              +- Join Inner
         :  :  :  :                 :- Join Inner
         :  :  :  :                 :  :- SubqueryAlias `tpcds`.`store_sales`
         :  :  :  :                 :  :  +- Relation[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
         :  :  :  :                 :  +- SubqueryAlias `tpcds`.`date_dim`
         :  :  :  :                 :     +- Relation[d_date_sk#150,d_date_id#151,d_date#152,d_month_seq#153,d_week_seq#154,d_quarter_seq#155,d_year#156,d_dow#157,d_moy#158,d_dom#159,d_qoy#160,d_fy_year#161,d_fy_quarter_seq#162,d_fy_week_seq#163,d_day_name#164,d_quarter_name#165,d_holiday#166,d_weekend#167,d_following_holiday#168,d_first_dom#169,d_last_dom#170,d_same_day_ly#171,d_same_day_lq#172,d_current_day#173,... 4 more fields] parquet
         :  :  :  :                 +- SubqueryAlias `tpcds`.`customer_address`
         :  :  :  :                    +- Relation[ca_address_sk#191,ca_address_id#192,ca_street_number#193,ca_street_name#194,ca_street_type#195,ca_suite_number#196,ca_city#197,ca_county#198,ca_state#199,ca_zip#200,ca_country#201,ca_gmt_offset#202,ca_location_type#203] parquet
         :  :  :  +- SubqueryAlias `ss3`
         :  :  :     +- SubqueryAlias `ss`
         :  :  :        +- Aggregate [ca_county#294, d_qoy#256, d_year#252], [ca_county#294, d_qoy#256, d_year#252, sum(ss_ext_sales_price#23) AS store_sales#204]
         :  :  :           +- Filter ((ss_sold_date_sk#8 = d_date_sk#246) && (ss_addr_sk#14 = ca_address_sk#287))
         :  :  :              +- Join Inner
         :  :  :                 :- Join Inner
         :  :  :                 :  :- SubqueryAlias `tpcds`.`store_sales`
         :  :  :                 :  :  +- Relation[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
         :  :  :                 :  +- SubqueryAlias `tpcds`.`date_dim`
         :  :  :                 :     +- Relation[d_date_sk#246,d_date_id#247,d_date#248,d_month_seq#249,d_week_seq#250,d_quarter_seq#251,d_year#252,d_dow#253,d_moy#254,d_dom#255,d_qoy#256,d_fy_year#257,d_fy_quarter_seq#258,d_fy_week_seq#259,d_day_name#260,d_quarter_name#261,d_holiday#262,d_weekend#263,d_following_holiday#264,d_first_dom#265,d_last_dom#266,d_same_day_ly#267,d_same_day_lq#268,d_current_day#269,... 4 more fields] parquet
         :  :  :                 +- SubqueryAlias `tpcds`.`customer_address`
         :  :  :                    +- Relation[ca_address_sk#287,ca_address_id#288,ca_street_number#289,ca_street_name#290,ca_street_type#291,ca_suite_number#292,ca_city#293,ca_county#294,ca_state#295,ca_zip#296,ca_country#297,ca_gmt_offset#298,ca_location_type#299] parquet
         :  :  +- SubqueryAlias `ws1`
         :  :     +- SubqueryAlias `ws`
         :  :        +- Aggregate [ca_county#348, d_qoy#310, d_year#306], [ca_county#348, d_qoy#310, d_year#306, sum(ws_ext_sales_price#96) AS web_sales#5]
         :  :           +- Filter ((ws_sold_date_sk#73 = d_date_sk#300) && (ws_bill_addr_sk#80 = ca_address_sk#341))
         :  :              +- Join Inner
         :  :                 :- Join Inner
         :  :                 :  :- SubqueryAlias `tpcds`.`web_sales`
         :  :                 :  :  +- Relation[ws_sold_date_sk#73,ws_sold_time_sk#74,ws_ship_date_sk#75,ws_item_sk#76,ws_bill_customer_sk#77,ws_bill_cdemo_sk#78,ws_bill_hdemo_sk#79,ws_bill_addr_sk#80,ws_ship_customer_sk#81,ws_ship_cdemo_sk#82,ws_ship_hdemo_sk#83,ws_ship_addr_sk#84,ws_web_page_sk#85,ws_web_site_sk#86,ws_ship_mode_sk#87,ws_warehouse_sk#88,ws_promo_sk#89,ws_order_number#90,ws_quantity#91,ws_wholesale_cost#92,ws_list_price#93,ws_sales_price#94,ws_ext_discount_amt#95,ws_ext_sales_price#96,... 10 more fields] parquet
         :  :                 :  +- SubqueryAlias `tpcds`.`date_dim`
         :  :                 :     +- Relation[d_date_sk#300,d_date_id#301,d_date#302,d_month_seq#303,d_week_seq#304,d_quarter_seq#305,d_year#306,d_dow#307,d_moy#308,d_dom#309,d_qoy#310,d_fy_year#311,d_fy_quarter_seq#312,d_fy_week_seq#313,d_day_name#314,d_quarter_name#315,d_holiday#316,d_weekend#317,d_following_holiday#318,d_first_dom#319,d_last_dom#320,d_same_day_ly#321,d_same_day_lq#322,d_current_day#323,... 4 more fields] parquet
         :  :                 +- SubqueryAlias `tpcds`.`customer_address`
         :  :                    +- Relation[ca_address_sk#341,ca_address_id#342,ca_street_number#343,ca_street_name#344,ca_street_type#345,ca_suite_number#346,ca_city#347,ca_county#348,ca_state#349,ca_zip#350,ca_country#351,ca_gmt_offset#352,ca_location_type#353] parquet
         :  +- SubqueryAlias `ws2`
         :     +- SubqueryAlias `ws`
         :        +- Aggregate [ca_county#444, d_qoy#406, d_year#402], [ca_county#444, d_qoy#406, d_year#402, sum(ws_ext_sales_price#96) AS web_sales#354]
         :           +- Filter ((ws_sold_date_sk#73 = d_date_sk#396) && (ws_bill_addr_sk#80 = ca_address_sk#437))
         :              +- Join Inner
         :                 :- Join Inner
         :                 :  :- SubqueryAlias `tpcds`.`web_sales`
         :                 :  :  +- Relation[ws_sold_date_sk#73,ws_sold_time_sk#74,ws_ship_date_sk#75,ws_item_sk#76,ws_bill_customer_sk#77,ws_bill_cdemo_sk#78,ws_bill_hdemo_sk#79,ws_bill_addr_sk#80,ws_ship_customer_sk#81,ws_ship_cdemo_sk#82,ws_ship_hdemo_sk#83,ws_ship_addr_sk#84,ws_web_page_sk#85,ws_web_site_sk#86,ws_ship_mode_sk#87,ws_warehouse_sk#88,ws_promo_sk#89,ws_order_number#90,ws_quantity#91,ws_wholesale_cost#92,ws_list_price#93,ws_sales_price#94,ws_ext_discount_amt#95,ws_ext_sales_price#96,... 10 more fields] parquet
         :                 :  +- SubqueryAlias `tpcds`.`date_dim`
         :                 :     +- Relation[d_date_sk#396,d_date_id#397,d_date#398,d_month_seq#399,d_week_seq#400,d_quarter_seq#401,d_year#402,d_dow#403,d_moy#404,d_dom#405,d_qoy#406,d_fy_year#407,d_fy_quarter_seq#408,d_fy_week_seq#409,d_day_name#410,d_quarter_name#411,d_holiday#412,d_weekend#413,d_following_holiday#414,d_first_dom#415,d_last_dom#416,d_same_day_ly#417,d_same_day_lq#418,d_current_day#419,... 4 more fields] parquet
         :                 +- SubqueryAlias `tpcds`.`customer_address`
         :                    +- Relation[ca_address_sk#437,ca_address_id#438,ca_street_number#439,ca_street_name#440,ca_street_type#441,ca_suite_number#442,ca_city#443,ca_county#444,ca_state#445,ca_zip#446,ca_country#447,ca_gmt_offset#448,ca_location_type#449] parquet
         +- SubqueryAlias `ws3`
            +- SubqueryAlias `ws`
               +- Aggregate [ca_county#540, d_qoy#502, d_year#498], [ca_county#540, d_qoy#502, d_year#498, sum(ws_ext_sales_price#96) AS web_sales#450]
                  +- Filter ((ws_sold_date_sk#73 = d_date_sk#492) && (ws_bill_addr_sk#80 = ca_address_sk#533))
                     +- Join Inner
                        :- Join Inner
                        :  :- SubqueryAlias `tpcds`.`web_sales`
                        :  :  +- Relation[ws_sold_date_sk#73,ws_sold_time_sk#74,ws_ship_date_sk#75,ws_item_sk#76,ws_bill_customer_sk#77,ws_bill_cdemo_sk#78,ws_bill_hdemo_sk#79,ws_bill_addr_sk#80,ws_ship_customer_sk#81,ws_ship_cdemo_sk#82,ws_ship_hdemo_sk#83,ws_ship_addr_sk#84,ws_web_page_sk#85,ws_web_site_sk#86,ws_ship_mode_sk#87,ws_warehouse_sk#88,ws_promo_sk#89,ws_order_number#90,ws_quantity#91,ws_wholesale_cost#92,ws_list_price#93,ws_sales_price#94,ws_ext_discount_amt#95,ws_ext_sales_price#96,... 10 more fields] parquet
                        :  +- SubqueryAlias `tpcds`.`date_dim`
                        :     +- Relation[d_date_sk#492,d_date_id#493,d_date#494,d_month_seq#495,d_week_seq#496,d_quarter_seq#497,d_year#498,d_dow#499,d_moy#500,d_dom#501,d_qoy#502,d_fy_year#503,d_fy_quarter_seq#504,d_fy_week_seq#505,d_day_name#506,d_quarter_name#507,d_holiday#508,d_weekend#509,d_following_holiday#510,d_first_dom#511,d_last_dom#512,d_same_day_ly#513,d_same_day_lq#514,d_current_day#515,... 4 more fields] parquet
                        +- SubqueryAlias `tpcds`.`customer_address`
                           +- Relation[ca_address_sk#533,ca_address_id#534,ca_street_number#535,ca_street_name#536,ca_street_type#537,ca_suite_number#538,ca_city#539,ca_county#540,ca_state#541,ca_zip#542,ca_country#543,ca_gmt_offset#544,ca_location_type#545] parquet

== Optimized Logical Plan ==
Sort [ca_county#66 ASC NULLS FIRST], true
+- Project [ca_county#66, d_year#37, (web_sales#354 / web_sales#5) AS web_q1_q2_increase#0, (store_sales#108 / store_sales#4) AS store_q1_q2_increase#1, (web_sales#450 / web_sales#354) AS web_q2_q3_increase#2, (store_sales#204 / store_sales#108) AS store_q2_q3_increase#3]
   +- Join Inner, ((ca_county#348 = ca_county#540) && (CASE WHEN (web_sales#354 > 0.0) THEN (web_sales#450 / web_sales#354) ELSE null END > CASE WHEN (store_sales#108 > 0.0) THEN (store_sales#204 / store_sales#108) ELSE null END))
      :- Project [ca_county#66, d_year#37, store_sales#4, store_sales#108, store_sales#204, ca_county#348, web_sales#5, web_sales#354]
      :  +- Join Inner, ((ca_county#348 = ca_county#444) && (CASE WHEN (web_sales#5 > 0.0) THEN (web_sales#354 / web_sales#5) ELSE null END > CASE WHEN (store_sales#4 > 0.0) THEN (store_sales#108 / store_sales#4) ELSE null END))
      :     :- Join Inner, (ca_county#66 = ca_county#348)
      :     :  :- Project [ca_county#66, d_year#37, store_sales#4, store_sales#108, store_sales#204]
      :     :  :  +- Join Inner, (ca_county#198 = ca_county#294)
      :     :  :     :- Join Inner, (ca_county#66 = ca_county#198)
      :     :  :     :  :- Aggregate [ca_county#66, d_qoy#41, d_year#37], [ca_county#66, d_year#37, sum(ss_ext_sales_price#23) AS store_sales#4]
      :     :  :     :  :  +- Project [ss_ext_sales_price#23, d_year#37, d_qoy#41, ca_county#66]
      :     :  :     :  :     +- Join Inner, (ss_addr_sk#14 = ca_address_sk#59)
      :     :  :     :  :        :- Project [ss_addr_sk#14, ss_ext_sales_price#23, d_year#37, d_qoy#41]
      :     :  :     :  :        :  +- Join Inner, (ss_sold_date_sk#8 = d_date_sk#31)
      :     :  :     :  :        :     :- Project [ss_sold_date_sk#8, ss_addr_sk#14, ss_ext_sales_price#23]
      :     :  :     :  :        :     :  +- Filter (isnotnull(ss_sold_date_sk#8) && isnotnull(ss_addr_sk#14))
      :     :  :     :  :        :     :     +- Relation[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
      :     :  :     :  :        :     +- Project [d_date_sk#31, d_year#37, d_qoy#41]
      :     :  :     :  :        :        +- Filter ((((isnotnull(d_qoy#41) && isnotnull(d_year#37)) && (d_qoy#41 = 1)) && (d_year#37 = 2000)) && isnotnull(d_date_sk#31))
      :     :  :     :  :        :           +- Relation[d_date_sk#31,d_date_id#32,d_date#33,d_month_seq#34,d_week_seq#35,d_quarter_seq#36,d_year#37,d_dow#38,d_moy#39,d_dom#40,d_qoy#41,d_fy_year#42,d_fy_quarter_seq#43,d_fy_week_seq#44,d_day_name#45,d_quarter_name#46,d_holiday#47,d_weekend#48,d_following_holiday#49,d_first_dom#50,d_last_dom#51,d_same_day_ly#52,d_same_day_lq#53,d_current_day#54,... 4 more fields] parquet
      :     :  :     :  :        +- Project [ca_address_sk#59, ca_county#66]
      :     :  :     :  :           +- Filter (isnotnull(ca_address_sk#59) && isnotnull(ca_county#66))
      :     :  :     :  :              +- Relation[ca_address_sk#59,ca_address_id#60,ca_street_number#61,ca_street_name#62,ca_street_type#63,ca_suite_number#64,ca_city#65,ca_county#66,ca_state#67,ca_zip#68,ca_country#69,ca_gmt_offset#70,ca_location_type#71] parquet
      :     :  :     :  +- Aggregate [ca_county#198, d_qoy#160, d_year#156], [ca_county#198, sum(ss_ext_sales_price#23) AS store_sales#108]
      :     :  :     :     +- Project [ss_ext_sales_price#23, d_year#156, d_qoy#160, ca_county#198]
      :     :  :     :        +- Join Inner, (ss_addr_sk#14 = ca_address_sk#191)
      :     :  :     :           :- Project [ss_addr_sk#14, ss_ext_sales_price#23, d_year#156, d_qoy#160]
      :     :  :     :           :  +- Join Inner, (ss_sold_date_sk#8 = d_date_sk#150)
      :     :  :     :           :     :- Project [ss_sold_date_sk#8, ss_addr_sk#14, ss_ext_sales_price#23]
      :     :  :     :           :     :  +- Filter (isnotnull(ss_sold_date_sk#8) && isnotnull(ss_addr_sk#14))
      :     :  :     :           :     :     +- Relation[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
      :     :  :     :           :     +- Project [d_date_sk#150, d_year#156, d_qoy#160]
      :     :  :     :           :        +- Filter ((((isnotnull(d_qoy#160) && isnotnull(d_year#156)) && (d_qoy#160 = 2)) && (d_year#156 = 2000)) && isnotnull(d_date_sk#150))
      :     :  :     :           :           +- Relation[d_date_sk#150,d_date_id#151,d_date#152,d_month_seq#153,d_week_seq#154,d_quarter_seq#155,d_year#156,d_dow#157,d_moy#158,d_dom#159,d_qoy#160,d_fy_year#161,d_fy_quarter_seq#162,d_fy_week_seq#163,d_day_name#164,d_quarter_name#165,d_holiday#166,d_weekend#167,d_following_holiday#168,d_first_dom#169,d_last_dom#170,d_same_day_ly#171,d_same_day_lq#172,d_current_day#173,... 4 more fields] parquet
      :     :  :     :           +- Project [ca_address_sk#191, ca_county#198]
      :     :  :     :              +- Filter (isnotnull(ca_address_sk#191) && isnotnull(ca_county#198))
      :     :  :     :                 +- Relation[ca_address_sk#191,ca_address_id#192,ca_street_number#193,ca_street_name#194,ca_street_type#195,ca_suite_number#196,ca_city#197,ca_county#198,ca_state#199,ca_zip#200,ca_country#201,ca_gmt_offset#202,ca_location_type#203] parquet
      :     :  :     +- Aggregate [ca_county#294, d_qoy#256, d_year#252], [ca_county#294, sum(ss_ext_sales_price#23) AS store_sales#204]
      :     :  :        +- Project [ss_ext_sales_price#23, d_year#252, d_qoy#256, ca_county#294]
      :     :  :           +- Join Inner, (ss_addr_sk#14 = ca_address_sk#287)
      :     :  :              :- Project [ss_addr_sk#14, ss_ext_sales_price#23, d_year#252, d_qoy#256]
      :     :  :              :  +- Join Inner, (ss_sold_date_sk#8 = d_date_sk#246)
      :     :  :              :     :- Project [ss_sold_date_sk#8, ss_addr_sk#14, ss_ext_sales_price#23]
      :     :  :              :     :  +- Filter (isnotnull(ss_sold_date_sk#8) && isnotnull(ss_addr_sk#14))
      :     :  :              :     :     +- Relation[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
      :     :  :              :     +- Project [d_date_sk#246, d_year#252, d_qoy#256]
      :     :  :              :        +- Filter ((((isnotnull(d_qoy#256) && isnotnull(d_year#252)) && (d_qoy#256 = 3)) && (d_year#252 = 2000)) && isnotnull(d_date_sk#246))
      :     :  :              :           +- Relation[d_date_sk#246,d_date_id#247,d_date#248,d_month_seq#249,d_week_seq#250,d_quarter_seq#251,d_year#252,d_dow#253,d_moy#254,d_dom#255,d_qoy#256,d_fy_year#257,d_fy_quarter_seq#258,d_fy_week_seq#259,d_day_name#260,d_quarter_name#261,d_holiday#262,d_weekend#263,d_following_holiday#264,d_first_dom#265,d_last_dom#266,d_same_day_ly#267,d_same_day_lq#268,d_current_day#269,... 4 more fields] parquet
      :     :  :              +- Project [ca_address_sk#287, ca_county#294]
      :     :  :                 +- Filter (isnotnull(ca_address_sk#287) && isnotnull(ca_county#294))
      :     :  :                    +- Relation[ca_address_sk#287,ca_address_id#288,ca_street_number#289,ca_street_name#290,ca_street_type#291,ca_suite_number#292,ca_city#293,ca_county#294,ca_state#295,ca_zip#296,ca_country#297,ca_gmt_offset#298,ca_location_type#299] parquet
      :     :  +- Aggregate [ca_county#348, d_qoy#310, d_year#306], [ca_county#348, sum(ws_ext_sales_price#96) AS web_sales#5]
      :     :     +- Project [ws_ext_sales_price#96, d_year#306, d_qoy#310, ca_county#348]
      :     :        +- Join Inner, (ws_bill_addr_sk#80 = ca_address_sk#341)
      :     :           :- Project [ws_bill_addr_sk#80, ws_ext_sales_price#96, d_year#306, d_qoy#310]
      :     :           :  +- Join Inner, (ws_sold_date_sk#73 = d_date_sk#300)
      :     :           :     :- Project [ws_sold_date_sk#73, ws_bill_addr_sk#80, ws_ext_sales_price#96]
      :     :           :     :  +- Filter (isnotnull(ws_sold_date_sk#73) && isnotnull(ws_bill_addr_sk#80))
      :     :           :     :     +- Relation[ws_sold_date_sk#73,ws_sold_time_sk#74,ws_ship_date_sk#75,ws_item_sk#76,ws_bill_customer_sk#77,ws_bill_cdemo_sk#78,ws_bill_hdemo_sk#79,ws_bill_addr_sk#80,ws_ship_customer_sk#81,ws_ship_cdemo_sk#82,ws_ship_hdemo_sk#83,ws_ship_addr_sk#84,ws_web_page_sk#85,ws_web_site_sk#86,ws_ship_mode_sk#87,ws_warehouse_sk#88,ws_promo_sk#89,ws_order_number#90,ws_quantity#91,ws_wholesale_cost#92,ws_list_price#93,ws_sales_price#94,ws_ext_discount_amt#95,ws_ext_sales_price#96,... 10 more fields] parquet
      :     :           :     +- Project [d_date_sk#300, d_year#306, d_qoy#310]
      :     :           :        +- Filter ((((isnotnull(d_qoy#310) && isnotnull(d_year#306)) && (d_qoy#310 = 1)) && (d_year#306 = 2000)) && isnotnull(d_date_sk#300))
      :     :           :           +- Relation[d_date_sk#300,d_date_id#301,d_date#302,d_month_seq#303,d_week_seq#304,d_quarter_seq#305,d_year#306,d_dow#307,d_moy#308,d_dom#309,d_qoy#310,d_fy_year#311,d_fy_quarter_seq#312,d_fy_week_seq#313,d_day_name#314,d_quarter_name#315,d_holiday#316,d_weekend#317,d_following_holiday#318,d_first_dom#319,d_last_dom#320,d_same_day_ly#321,d_same_day_lq#322,d_current_day#323,... 4 more fields] parquet
      :     :           +- Project [ca_address_sk#341, ca_county#348]
      :     :              +- Filter (isnotnull(ca_address_sk#341) && isnotnull(ca_county#348))
      :     :                 +- Relation[ca_address_sk#341,ca_address_id#342,ca_street_number#343,ca_street_name#344,ca_street_type#345,ca_suite_number#346,ca_city#347,ca_county#348,ca_state#349,ca_zip#350,ca_country#351,ca_gmt_offset#352,ca_location_type#353] parquet
      :     +- Aggregate [ca_county#444, d_qoy#406, d_year#402], [ca_county#444, sum(ws_ext_sales_price#96) AS web_sales#354]
      :        +- Project [ws_ext_sales_price#96, d_year#402, d_qoy#406, ca_county#444]
      :           +- Join Inner, (ws_bill_addr_sk#80 = ca_address_sk#437)
      :              :- Project [ws_bill_addr_sk#80, ws_ext_sales_price#96, d_year#402, d_qoy#406]
      :              :  +- Join Inner, (ws_sold_date_sk#73 = d_date_sk#396)
      :              :     :- Project [ws_sold_date_sk#73, ws_bill_addr_sk#80, ws_ext_sales_price#96]
      :              :     :  +- Filter (isnotnull(ws_sold_date_sk#73) && isnotnull(ws_bill_addr_sk#80))
      :              :     :     +- Relation[ws_sold_date_sk#73,ws_sold_time_sk#74,ws_ship_date_sk#75,ws_item_sk#76,ws_bill_customer_sk#77,ws_bill_cdemo_sk#78,ws_bill_hdemo_sk#79,ws_bill_addr_sk#80,ws_ship_customer_sk#81,ws_ship_cdemo_sk#82,ws_ship_hdemo_sk#83,ws_ship_addr_sk#84,ws_web_page_sk#85,ws_web_site_sk#86,ws_ship_mode_sk#87,ws_warehouse_sk#88,ws_promo_sk#89,ws_order_number#90,ws_quantity#91,ws_wholesale_cost#92,ws_list_price#93,ws_sales_price#94,ws_ext_discount_amt#95,ws_ext_sales_price#96,... 10 more fields] parquet
      :              :     +- Project [d_date_sk#396, d_year#402, d_qoy#406]
      :              :        +- Filter ((((isnotnull(d_qoy#406) && isnotnull(d_year#402)) && (d_qoy#406 = 2)) && (d_year#402 = 2000)) && isnotnull(d_date_sk#396))
      :              :           +- Relation[d_date_sk#396,d_date_id#397,d_date#398,d_month_seq#399,d_week_seq#400,d_quarter_seq#401,d_year#402,d_dow#403,d_moy#404,d_dom#405,d_qoy#406,d_fy_year#407,d_fy_quarter_seq#408,d_fy_week_seq#409,d_day_name#410,d_quarter_name#411,d_holiday#412,d_weekend#413,d_following_holiday#414,d_first_dom#415,d_last_dom#416,d_same_day_ly#417,d_same_day_lq#418,d_current_day#419,... 4 more fields] parquet
      :              +- Project [ca_address_sk#437, ca_county#444]
      :                 +- Filter (isnotnull(ca_address_sk#437) && isnotnull(ca_county#444))
      :                    +- Relation[ca_address_sk#437,ca_address_id#438,ca_street_number#439,ca_street_name#440,ca_street_type#441,ca_suite_number#442,ca_city#443,ca_county#444,ca_state#445,ca_zip#446,ca_country#447,ca_gmt_offset#448,ca_location_type#449] parquet
      +- Aggregate [ca_county#540, d_qoy#502, d_year#498], [ca_county#540, sum(ws_ext_sales_price#96) AS web_sales#450]
         +- Project [ws_ext_sales_price#96, d_year#498, d_qoy#502, ca_county#540]
            +- Join Inner, (ws_bill_addr_sk#80 = ca_address_sk#533)
               :- Project [ws_bill_addr_sk#80, ws_ext_sales_price#96, d_year#498, d_qoy#502]
               :  +- Join Inner, (ws_sold_date_sk#73 = d_date_sk#492)
               :     :- Project [ws_sold_date_sk#73, ws_bill_addr_sk#80, ws_ext_sales_price#96]
               :     :  +- Filter (isnotnull(ws_sold_date_sk#73) && isnotnull(ws_bill_addr_sk#80))
               :     :     +- Relation[ws_sold_date_sk#73,ws_sold_time_sk#74,ws_ship_date_sk#75,ws_item_sk#76,ws_bill_customer_sk#77,ws_bill_cdemo_sk#78,ws_bill_hdemo_sk#79,ws_bill_addr_sk#80,ws_ship_customer_sk#81,ws_ship_cdemo_sk#82,ws_ship_hdemo_sk#83,ws_ship_addr_sk#84,ws_web_page_sk#85,ws_web_site_sk#86,ws_ship_mode_sk#87,ws_warehouse_sk#88,ws_promo_sk#89,ws_order_number#90,ws_quantity#91,ws_wholesale_cost#92,ws_list_price#93,ws_sales_price#94,ws_ext_discount_amt#95,ws_ext_sales_price#96,... 10 more fields] parquet
               :     +- Project [d_date_sk#492, d_year#498, d_qoy#502]
               :        +- Filter ((((isnotnull(d_qoy#502) && isnotnull(d_year#498)) && (d_qoy#502 = 3)) && (d_year#498 = 2000)) && isnotnull(d_date_sk#492))
               :           +- Relation[d_date_sk#492,d_date_id#493,d_date#494,d_month_seq#495,d_week_seq#496,d_quarter_seq#497,d_year#498,d_dow#499,d_moy#500,d_dom#501,d_qoy#502,d_fy_year#503,d_fy_quarter_seq#504,d_fy_week_seq#505,d_day_name#506,d_quarter_name#507,d_holiday#508,d_weekend#509,d_following_holiday#510,d_first_dom#511,d_last_dom#512,d_same_day_ly#513,d_same_day_lq#514,d_current_day#515,... 4 more fields] parquet
               +- Project [ca_address_sk#533, ca_county#540]
                  +- Filter (isnotnull(ca_address_sk#533) && isnotnull(ca_county#540))
                     +- Relation[ca_address_sk#533,ca_address_id#534,ca_street_number#535,ca_street_name#536,ca_street_type#537,ca_suite_number#538,ca_city#539,ca_county#540,ca_state#541,ca_zip#542,ca_country#543,ca_gmt_offset#544,ca_location_type#545] parquet

== Physical Plan ==
*(36) Sort [ca_county#66 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(ca_county#66 ASC NULLS FIRST, 200)
   +- *(35) Project [ca_county#66, d_year#37, (web_sales#354 / web_sales#5) AS web_q1_q2_increase#0, (store_sales#108 / store_sales#4) AS store_q1_q2_increase#1, (web_sales#450 / web_sales#354) AS web_q2_q3_increase#2, (store_sales#204 / store_sales#108) AS store_q2_q3_increase#3]
      +- *(35) SortMergeJoin [ca_county#348], [ca_county#540], Inner, (CASE WHEN (web_sales#354 > 0.0) THEN (web_sales#450 / web_sales#354) ELSE null END > CASE WHEN (store_sales#108 > 0.0) THEN (store_sales#204 / store_sales#108) ELSE null END)
         :- *(29) Project [ca_county#66, d_year#37, store_sales#4, store_sales#108, store_sales#204, ca_county#348, web_sales#5, web_sales#354]
         :  +- *(29) SortMergeJoin [ca_county#348], [ca_county#444], Inner, (CASE WHEN (web_sales#5 > 0.0) THEN (web_sales#354 / web_sales#5) ELSE null END > CASE WHEN (store_sales#4 > 0.0) THEN (store_sales#108 / store_sales#4) ELSE null END)
         :     :- *(23) SortMergeJoin [ca_county#66], [ca_county#348], Inner
         :     :  :- *(17) Project [ca_county#66, d_year#37, store_sales#4, store_sales#108, store_sales#204]
         :     :  :  +- *(17) SortMergeJoin [ca_county#198], [ca_county#294], Inner
         :     :  :     :- *(11) SortMergeJoin [ca_county#66], [ca_county#198], Inner
         :     :  :     :  :- *(5) Sort [ca_county#66 ASC NULLS FIRST], false, 0
         :     :  :     :  :  +- Exchange hashpartitioning(ca_county#66, 200)
         :     :  :     :  :     +- *(4) HashAggregate(keys=[ca_county#66, d_qoy#41, d_year#37], functions=[sum(ss_ext_sales_price#23)], output=[ca_county#66, d_year#37, store_sales#4])
         :     :  :     :  :        +- Exchange hashpartitioning(ca_county#66, d_qoy#41, d_year#37, 200)
         :     :  :     :  :           +- *(3) HashAggregate(keys=[ca_county#66, d_qoy#41, d_year#37], functions=[partial_sum(ss_ext_sales_price#23)], output=[ca_county#66, d_qoy#41, d_year#37, sum#547])
         :     :  :     :  :              +- *(3) Project [ss_ext_sales_price#23, d_year#37, d_qoy#41, ca_county#66]
         :     :  :     :  :                 +- *(3) BroadcastHashJoin [ss_addr_sk#14], [ca_address_sk#59], Inner, BuildRight
         :     :  :     :  :                    :- *(3) Project [ss_addr_sk#14, ss_ext_sales_price#23, d_year#37, d_qoy#41]
         :     :  :     :  :                    :  +- *(3) BroadcastHashJoin [ss_sold_date_sk#8], [d_date_sk#31], Inner, BuildRight
         :     :  :     :  :                    :     :- *(3) Project [ss_sold_date_sk#8, ss_addr_sk#14, ss_ext_sales_price#23]
         :     :  :     :  :                    :     :  +- *(3) Filter (isnotnull(ss_sold_date_sk#8) && isnotnull(ss_addr_sk#14))
         :     :  :     :  :                    :     :     +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#8,ss_addr_sk#14,ss_ext_sales_price#23] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_addr_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_addr_sk:int,ss_ext_sales_price:double>
         :     :  :     :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :     :  :     :  :                    :        +- *(1) Project [d_date_sk#31, d_year#37, d_qoy#41]
         :     :  :     :  :                    :           +- *(1) Filter ((((isnotnull(d_qoy#41) && isnotnull(d_year#37)) && (d_qoy#41 = 1)) && (d_year#37 = 2000)) && isnotnull(d_date_sk#31))
         :     :  :     :  :                    :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#31,d_year#37,d_qoy#41] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,1), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
         :     :  :     :  :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :     :  :     :  :                       +- *(2) Project [ca_address_sk#59, ca_county#66]
         :     :  :     :  :                          +- *(2) Filter (isnotnull(ca_address_sk#59) && isnotnull(ca_county#66))
         :     :  :     :  :                             +- *(2) FileScan parquet tpcds.customer_address[ca_address_sk#59,ca_county#66] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_county)], ReadSchema: struct<ca_address_sk:int,ca_county:string>
         :     :  :     :  +- *(10) Sort [ca_county#198 ASC NULLS FIRST], false, 0
         :     :  :     :     +- Exchange hashpartitioning(ca_county#198, 200)
         :     :  :     :        +- *(9) HashAggregate(keys=[ca_county#198, d_qoy#160, d_year#156], functions=[sum(ss_ext_sales_price#23)], output=[ca_county#198, store_sales#108])
         :     :  :     :           +- Exchange hashpartitioning(ca_county#198, d_qoy#160, d_year#156, 200)
         :     :  :     :              +- *(8) HashAggregate(keys=[ca_county#198, d_qoy#160, d_year#156], functions=[partial_sum(ss_ext_sales_price#23)], output=[ca_county#198, d_qoy#160, d_year#156, sum#547])
         :     :  :     :                 +- *(8) Project [ss_ext_sales_price#23, d_year#156, d_qoy#160, ca_county#198]
         :     :  :     :                    +- *(8) BroadcastHashJoin [ss_addr_sk#14], [ca_address_sk#191], Inner, BuildRight
         :     :  :     :                       :- *(8) Project [ss_addr_sk#14, ss_ext_sales_price#23, d_year#156, d_qoy#160]
         :     :  :     :                       :  +- *(8) BroadcastHashJoin [ss_sold_date_sk#8], [d_date_sk#150], Inner, BuildRight
         :     :  :     :                       :     :- *(8) Project [ss_sold_date_sk#8, ss_addr_sk#14, ss_ext_sales_price#23]
         :     :  :     :                       :     :  +- *(8) Filter (isnotnull(ss_sold_date_sk#8) && isnotnull(ss_addr_sk#14))
         :     :  :     :                       :     :     +- *(8) FileScan parquet tpcds.store_sales[ss_sold_date_sk#8,ss_addr_sk#14,ss_ext_sales_price#23] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_addr_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_addr_sk:int,ss_ext_sales_price:double>
         :     :  :     :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :     :  :     :                       :        +- *(6) Project [d_date_sk#150, d_year#156, d_qoy#160]
         :     :  :     :                       :           +- *(6) Filter ((((isnotnull(d_qoy#160) && isnotnull(d_year#156)) && (d_qoy#160 = 2)) && (d_year#156 = 2000)) && isnotnull(d_date_sk#150))
         :     :  :     :                       :              +- *(6) FileScan parquet tpcds.date_dim[d_date_sk#150,d_year#156,d_qoy#160] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,2), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
         :     :  :     :                       +- ReusedExchange [ca_address_sk#191, ca_county#198], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :     :  :     +- *(16) Sort [ca_county#294 ASC NULLS FIRST], false, 0
         :     :  :        +- Exchange hashpartitioning(ca_county#294, 200)
         :     :  :           +- *(15) HashAggregate(keys=[ca_county#294, d_qoy#256, d_year#252], functions=[sum(ss_ext_sales_price#23)], output=[ca_county#294, store_sales#204])
         :     :  :              +- Exchange hashpartitioning(ca_county#294, d_qoy#256, d_year#252, 200)
         :     :  :                 +- *(14) HashAggregate(keys=[ca_county#294, d_qoy#256, d_year#252], functions=[partial_sum(ss_ext_sales_price#23)], output=[ca_county#294, d_qoy#256, d_year#252, sum#547])
         :     :  :                    +- *(14) Project [ss_ext_sales_price#23, d_year#252, d_qoy#256, ca_county#294]
         :     :  :                       +- *(14) BroadcastHashJoin [ss_addr_sk#14], [ca_address_sk#287], Inner, BuildRight
         :     :  :                          :- *(14) Project [ss_addr_sk#14, ss_ext_sales_price#23, d_year#252, d_qoy#256]
         :     :  :                          :  +- *(14) BroadcastHashJoin [ss_sold_date_sk#8], [d_date_sk#246], Inner, BuildRight
         :     :  :                          :     :- *(14) Project [ss_sold_date_sk#8, ss_addr_sk#14, ss_ext_sales_price#23]
         :     :  :                          :     :  +- *(14) Filter (isnotnull(ss_sold_date_sk#8) && isnotnull(ss_addr_sk#14))
         :     :  :                          :     :     +- *(14) FileScan parquet tpcds.store_sales[ss_sold_date_sk#8,ss_addr_sk#14,ss_ext_sales_price#23] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_addr_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_addr_sk:int,ss_ext_sales_price:double>
         :     :  :                          :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :     :  :                          :        +- *(12) Project [d_date_sk#246, d_year#252, d_qoy#256]
         :     :  :                          :           +- *(12) Filter ((((isnotnull(d_qoy#256) && isnotnull(d_year#252)) && (d_qoy#256 = 3)) && (d_year#252 = 2000)) && isnotnull(d_date_sk#246))
         :     :  :                          :              +- *(12) FileScan parquet tpcds.date_dim[d_date_sk#246,d_year#252,d_qoy#256] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,3), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
         :     :  :                          +- ReusedExchange [ca_address_sk#287, ca_county#294], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :     :  +- *(22) Sort [ca_county#348 ASC NULLS FIRST], false, 0
         :     :     +- Exchange hashpartitioning(ca_county#348, 200)
         :     :        +- *(21) HashAggregate(keys=[ca_county#348, d_qoy#310, d_year#306], functions=[sum(ws_ext_sales_price#96)], output=[ca_county#348, web_sales#5])
         :     :           +- Exchange hashpartitioning(ca_county#348, d_qoy#310, d_year#306, 200)
         :     :              +- *(20) HashAggregate(keys=[ca_county#348, d_qoy#310, d_year#306], functions=[partial_sum(ws_ext_sales_price#96)], output=[ca_county#348, d_qoy#310, d_year#306, sum#549])
         :     :                 +- *(20) Project [ws_ext_sales_price#96, d_year#306, d_qoy#310, ca_county#348]
         :     :                    +- *(20) BroadcastHashJoin [ws_bill_addr_sk#80], [ca_address_sk#341], Inner, BuildRight
         :     :                       :- *(20) Project [ws_bill_addr_sk#80, ws_ext_sales_price#96, d_year#306, d_qoy#310]
         :     :                       :  +- *(20) BroadcastHashJoin [ws_sold_date_sk#73], [d_date_sk#300], Inner, BuildRight
         :     :                       :     :- *(20) Project [ws_sold_date_sk#73, ws_bill_addr_sk#80, ws_ext_sales_price#96]
         :     :                       :     :  +- *(20) Filter (isnotnull(ws_sold_date_sk#73) && isnotnull(ws_bill_addr_sk#80))
         :     :                       :     :     +- *(20) FileScan parquet tpcds.web_sales[ws_sold_date_sk#73,ws_bill_addr_sk#80,ws_ext_sales_price#96] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_addr_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_addr_sk:int,ws_ext_sales_price:double>
         :     :                       :     +- ReusedExchange [d_date_sk#300, d_year#306, d_qoy#310], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :     :                       +- ReusedExchange [ca_address_sk#341, ca_county#348], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :     +- *(28) Sort [ca_county#444 ASC NULLS FIRST], false, 0
         :        +- Exchange hashpartitioning(ca_county#444, 200)
         :           +- *(27) HashAggregate(keys=[ca_county#444, d_qoy#406, d_year#402], functions=[sum(ws_ext_sales_price#96)], output=[ca_county#444, web_sales#354])
         :              +- Exchange hashpartitioning(ca_county#444, d_qoy#406, d_year#402, 200)
         :                 +- *(26) HashAggregate(keys=[ca_county#444, d_qoy#406, d_year#402], functions=[partial_sum(ws_ext_sales_price#96)], output=[ca_county#444, d_qoy#406, d_year#402, sum#549])
         :                    +- *(26) Project [ws_ext_sales_price#96, d_year#402, d_qoy#406, ca_county#444]
         :                       +- *(26) BroadcastHashJoin [ws_bill_addr_sk#80], [ca_address_sk#437], Inner, BuildRight
         :                          :- *(26) Project [ws_bill_addr_sk#80, ws_ext_sales_price#96, d_year#402, d_qoy#406]
         :                          :  +- *(26) BroadcastHashJoin [ws_sold_date_sk#73], [d_date_sk#396], Inner, BuildRight
         :                          :     :- *(26) Project [ws_sold_date_sk#73, ws_bill_addr_sk#80, ws_ext_sales_price#96]
         :                          :     :  +- *(26) Filter (isnotnull(ws_sold_date_sk#73) && isnotnull(ws_bill_addr_sk#80))
         :                          :     :     +- *(26) FileScan parquet tpcds.web_sales[ws_sold_date_sk#73,ws_bill_addr_sk#80,ws_ext_sales_price#96] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_addr_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_addr_sk:int,ws_ext_sales_price:double>
         :                          :     +- ReusedExchange [d_date_sk#396, d_year#402, d_qoy#406], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                          +- ReusedExchange [ca_address_sk#437, ca_county#444], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         +- *(34) Sort [ca_county#540 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(ca_county#540, 200)
               +- *(33) HashAggregate(keys=[ca_county#540, d_qoy#502, d_year#498], functions=[sum(ws_ext_sales_price#96)], output=[ca_county#540, web_sales#450])
                  +- Exchange hashpartitioning(ca_county#540, d_qoy#502, d_year#498, 200)
                     +- *(32) HashAggregate(keys=[ca_county#540, d_qoy#502, d_year#498], functions=[partial_sum(ws_ext_sales_price#96)], output=[ca_county#540, d_qoy#502, d_year#498, sum#549])
                        +- *(32) Project [ws_ext_sales_price#96, d_year#498, d_qoy#502, ca_county#540]
                           +- *(32) BroadcastHashJoin [ws_bill_addr_sk#80], [ca_address_sk#533], Inner, BuildRight
                              :- *(32) Project [ws_bill_addr_sk#80, ws_ext_sales_price#96, d_year#498, d_qoy#502]
                              :  +- *(32) BroadcastHashJoin [ws_sold_date_sk#73], [d_date_sk#492], Inner, BuildRight
                              :     :- *(32) Project [ws_sold_date_sk#73, ws_bill_addr_sk#80, ws_ext_sales_price#96]
                              :     :  +- *(32) Filter (isnotnull(ws_sold_date_sk#73) && isnotnull(ws_bill_addr_sk#80))
                              :     :     +- *(32) FileScan parquet tpcds.web_sales[ws_sold_date_sk#73,ws_bill_addr_sk#80,ws_ext_sales_price#96] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_addr_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_addr_sk:int,ws_ext_sales_price:double>
                              :     +- ReusedExchange [d_date_sk#492, d_year#498, d_qoy#502], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              +- ReusedExchange [ca_address_sk#533, ca_county#540], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.645 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 31 in stream 0 using template query31.tpl
------------------------------------------------------^^^

