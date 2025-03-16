Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582400960
== Parsed Logical Plan ==
CTE [year_total]
:  +- 'SubqueryAlias year_total
:     +- 'Union false, false
:        :- 'Aggregate ['c_customer_id, 'c_first_name, 'c_last_name, 'd_year], ['c_customer_id AS customer_id#0, 'c_first_name AS customer_first_name#1, 'c_last_name AS customer_last_name#2, 'd_year AS year#3, 'sum('ss_net_paid) AS year_total#4, s AS sale_type#5]
:        :  +- 'Filter ((('c_customer_sk = 'ss_customer_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND 'd_year IN (2001,(2001 + 1)))
:        :     +- 'Join Inner
:        :        :- 'Join Inner
:        :        :  :- 'UnresolvedRelation [customer], [], false
:        :        :  +- 'UnresolvedRelation [store_sales], [], false
:        :        +- 'UnresolvedRelation [date_dim], [], false
:        +- 'Aggregate ['c_customer_id, 'c_first_name, 'c_last_name, 'd_year], ['c_customer_id AS customer_id#6, 'c_first_name AS customer_first_name#7, 'c_last_name AS customer_last_name#8, 'd_year AS year#9, 'sum('ws_net_paid) AS year_total#10, w AS sale_type#11]
:           +- 'Filter ((('c_customer_sk = 'ws_bill_customer_sk) AND ('ws_sold_date_sk = 'd_date_sk)) AND 'd_year IN (2001,(2001 + 1)))
:              +- 'Join Inner
:                 :- 'Join Inner
:                 :  :- 'UnresolvedRelation [customer], [], false
:                 :  +- 'UnresolvedRelation [web_sales], [], false
:                 +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort [1 ASC NULLS FIRST, 1 ASC NULLS FIRST, 1 ASC NULLS FIRST], true
         +- 'Project ['t_s_secyear.customer_id, 't_s_secyear.customer_first_name, 't_s_secyear.customer_last_name]
            +- 'Filter ((((('t_s_secyear.customer_id = 't_s_firstyear.customer_id) AND ('t_s_firstyear.customer_id = 't_w_secyear.customer_id)) AND (('t_s_firstyear.customer_id = 't_w_firstyear.customer_id) AND ('t_s_firstyear.sale_type = s))) AND ((('t_w_firstyear.sale_type = w) AND ('t_s_secyear.sale_type = s)) AND ('t_w_secyear.sale_type = w))) AND (((('t_s_firstyear.year = 2001) AND ('t_s_secyear.year = (2001 + 1))) AND (('t_w_firstyear.year = 2001) AND ('t_w_secyear.year = (2001 + 1)))) AND ((('t_s_firstyear.year_total > 0) AND ('t_w_firstyear.year_total > 0)) AND (CASE WHEN ('t_w_firstyear.year_total > 0) THEN ('t_w_secyear.year_total / 't_w_firstyear.year_total) ELSE null END > CASE WHEN ('t_s_firstyear.year_total > 0) THEN ('t_s_secyear.year_total / 't_s_firstyear.year_total) ELSE null END))))
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'Join Inner
                  :  :  :- 'SubqueryAlias t_s_firstyear
                  :  :  :  +- 'UnresolvedRelation [year_total], [], false
                  :  :  +- 'SubqueryAlias t_s_secyear
                  :  :     +- 'UnresolvedRelation [year_total], [], false
                  :  +- 'SubqueryAlias t_w_firstyear
                  :     +- 'UnresolvedRelation [year_total], [], false
                  +- 'SubqueryAlias t_w_secyear
                     +- 'UnresolvedRelation [year_total], [], false

== Analyzed Logical Plan ==
customer_id: string, customer_first_name: string, customer_last_name: string
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias year_total
:     +- Union false, false
:        :- Aggregate [c_customer_id#18, c_first_name#25, c_last_name#26, d_year#64], [c_customer_id#18 AS customer_id#0, c_first_name#25 AS customer_first_name#1, c_last_name#26 AS customer_last_name#2, d_year#64 AS year#3, sum(ss_net_paid#55) AS year_total#4, s AS sale_type#5]
:        :  +- Filter (((c_customer_sk#17 = ss_customer_sk#38) AND (ss_sold_date_sk#35 = d_date_sk#58)) AND d_year#64 IN (2001,(2001 + 1)))
:        :     +- Join Inner
:        :        :- Join Inner
:        :        :  :- SubqueryAlias spark_catalog.tpcds.customer
:        :        :  :  +- Relation spark_catalog.tpcds.customer[c_customer_sk#17,c_customer_id#18,c_current_cdemo_sk#19,c_current_hdemo_sk#20,c_current_addr_sk#21,c_first_shipto_date_sk#22,c_first_sales_date_sk#23,c_salutation#24,c_first_name#25,c_last_name#26,c_preferred_cust_flag#27,c_birth_day#28,c_birth_month#29,c_birth_year#30,c_birth_country#31,c_login#32,c_email_address#33,c_last_review_date#34] parquet
:        :        :  +- SubqueryAlias spark_catalog.tpcds.store_sales
:        :        :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#35,ss_sold_time_sk#36,ss_item_sk#37,ss_customer_sk#38,ss_cdemo_sk#39,ss_hdemo_sk#40,ss_addr_sk#41,ss_store_sk#42,ss_promo_sk#43,ss_ticket_number#44,ss_quantity#45,ss_wholesale_cost#46,ss_list_price#47,ss_sales_price#48,ss_ext_discount_amt#49,ss_ext_sales_price#50,ss_ext_wholesale_cost#51,ss_ext_list_price#52,ss_ext_tax#53,ss_coupon_amt#54,ss_net_paid#55,ss_net_paid_inc_tax#56,ss_net_profit#57] parquet
:        :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#58,d_date_id#59,d_date#60,d_month_seq#61,d_week_seq#62,d_quarter_seq#63,d_year#64,d_dow#65,d_moy#66,d_dom#67,d_qoy#68,d_fy_year#69,d_fy_quarter_seq#70,d_fy_week_seq#71,d_day_name#72,d_quarter_name#73,d_holiday#74,d_weekend#75,d_following_holiday#76,d_first_dom#77,d_last_dom#78,d_same_day_ly#79,d_same_day_lq#80,d_current_day#81,... 4 more fields] parquet
:        +- Aggregate [c_customer_id#121, c_first_name#128, c_last_name#129, d_year#144], [c_customer_id#121 AS customer_id#6, c_first_name#128 AS customer_first_name#7, c_last_name#129 AS customer_last_name#8, d_year#144 AS year#9, sum(ws_net_paid#115) AS year_total#10, w AS sale_type#11]
:           +- Filter (((c_customer_sk#120 = ws_bill_customer_sk#90) AND (ws_sold_date_sk#86 = d_date_sk#138)) AND d_year#144 IN (2001,(2001 + 1)))
:              +- Join Inner
:                 :- Join Inner
:                 :  :- SubqueryAlias spark_catalog.tpcds.customer
:                 :  :  +- Relation spark_catalog.tpcds.customer[c_customer_sk#120,c_customer_id#121,c_current_cdemo_sk#122,c_current_hdemo_sk#123,c_current_addr_sk#124,c_first_shipto_date_sk#125,c_first_sales_date_sk#126,c_salutation#127,c_first_name#128,c_last_name#129,c_preferred_cust_flag#130,c_birth_day#131,c_birth_month#132,c_birth_year#133,c_birth_country#134,c_login#135,c_email_address#136,c_last_review_date#137] parquet
:                 :  +- SubqueryAlias spark_catalog.tpcds.web_sales
:                 :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#86,ws_sold_time_sk#87,ws_ship_date_sk#88,ws_item_sk#89,ws_bill_customer_sk#90,ws_bill_cdemo_sk#91,ws_bill_hdemo_sk#92,ws_bill_addr_sk#93,ws_ship_customer_sk#94,ws_ship_cdemo_sk#95,ws_ship_hdemo_sk#96,ws_ship_addr_sk#97,ws_web_page_sk#98,ws_web_site_sk#99,ws_ship_mode_sk#100,ws_warehouse_sk#101,ws_promo_sk#102,ws_order_number#103,ws_quantity#104,ws_wholesale_cost#105,ws_list_price#106,ws_sales_price#107,ws_ext_discount_amt#108,ws_ext_sales_price#109,... 10 more fields] parquet
:                 +- SubqueryAlias spark_catalog.tpcds.date_dim
:                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#138,d_date_id#139,d_date#140,d_month_seq#141,d_week_seq#142,d_quarter_seq#143,d_year#144,d_dow#145,d_moy#146,d_dom#147,d_qoy#148,d_fy_year#149,d_fy_quarter_seq#150,d_fy_week_seq#151,d_day_name#152,d_quarter_name#153,d_holiday#154,d_weekend#155,d_following_holiday#156,d_first_dom#157,d_last_dom#158,d_same_day_ly#159,d_same_day_lq#160,d_current_day#161,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [customer_id#174 ASC NULLS FIRST, customer_id#174 ASC NULLS FIRST, customer_id#174 ASC NULLS FIRST], true
         +- Project [customer_id#174, customer_first_name#175, customer_last_name#176]
            +- Filter (((((customer_id#174 = customer_id#0) AND (customer_id#0 = customer_id#186)) AND ((customer_id#0 = customer_id#180) AND (sale_type#5 = s))) AND (((sale_type#185 = w) AND (sale_type#179 = s)) AND (sale_type#191 = w))) AND ((((year#3 = 2001) AND (year#177 = (2001 + 1))) AND ((year#183 = 2001) AND (year#189 = (2001 + 1)))) AND (((year_total#4 > cast(0 as double)) AND (year_total#184 > cast(0 as double))) AND (CASE WHEN (year_total#184 > cast(0 as double)) THEN (year_total#190 / year_total#184) ELSE cast(null as double) END > CASE WHEN (year_total#4 > cast(0 as double)) THEN (year_total#178 / year_total#4) ELSE cast(null as double) END))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- SubqueryAlias t_s_firstyear
                  :  :  :  +- SubqueryAlias year_total
                  :  :  :     +- CTERelationRef 0, true, [customer_id#0, customer_first_name#1, customer_last_name#2, year#3, year_total#4, sale_type#5], false
                  :  :  +- SubqueryAlias t_s_secyear
                  :  :     +- SubqueryAlias year_total
                  :  :        +- CTERelationRef 0, true, [customer_id#174, customer_first_name#175, customer_last_name#176, year#177, year_total#178, sale_type#179], false
                  :  +- SubqueryAlias t_w_firstyear
                  :     +- SubqueryAlias year_total
                  :        +- CTERelationRef 0, true, [customer_id#180, customer_first_name#181, customer_last_name#182, year#183, year_total#184, sale_type#185], false
                  +- SubqueryAlias t_w_secyear
                     +- SubqueryAlias year_total
                        +- CTERelationRef 0, true, [customer_id#186, customer_first_name#187, customer_last_name#188, year#189, year_total#190, sale_type#191], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [customer_id#174 ASC NULLS FIRST, customer_id#174 ASC NULLS FIRST, customer_id#174 ASC NULLS FIRST], true
      +- Project [customer_id#174, customer_first_name#175, customer_last_name#176]
         +- Join Inner, ((customer_id#0 = customer_id#186) AND (CASE WHEN (year_total#10 > 0.0) THEN (year_total#190 / year_total#10) END > CASE WHEN (year_total#4 > 0.0) THEN (year_total#178 / year_total#4) END))
            :- Project [customer_id#0, year_total#4, customer_id#174, customer_first_name#175, customer_last_name#176, year_total#178, year_total#10]
            :  +- Join Inner, (customer_id#0 = customer_id#6)
            :     :- Join Inner, (customer_id#174 = customer_id#0)
            :     :  :- Filter (isnotnull(year_total#4) AND (year_total#4 > 0.0))
            :     :  :  +- Aggregate [c_customer_id#18, c_first_name#25, c_last_name#26, d_year#64], [c_customer_id#18 AS customer_id#0, sum(ss_net_paid#55) AS year_total#4]
            :     :  :     +- Project [c_customer_id#18, c_first_name#25, c_last_name#26, ss_net_paid#55, d_year#64]
            :     :  :        +- Join Inner, (ss_sold_date_sk#35 = d_date_sk#58)
            :     :  :           :- Project [c_customer_id#18, c_first_name#25, c_last_name#26, ss_sold_date_sk#35, ss_net_paid#55]
            :     :  :           :  +- Join Inner, (c_customer_sk#17 = ss_customer_sk#38)
            :     :  :           :     :- Project [c_customer_sk#17, c_customer_id#18, c_first_name#25, c_last_name#26]
            :     :  :           :     :  +- Filter (isnotnull(c_customer_sk#17) AND isnotnull(c_customer_id#18))
            :     :  :           :     :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#17,c_customer_id#18,c_current_cdemo_sk#19,c_current_hdemo_sk#20,c_current_addr_sk#21,c_first_shipto_date_sk#22,c_first_sales_date_sk#23,c_salutation#24,c_first_name#25,c_last_name#26,c_preferred_cust_flag#27,c_birth_day#28,c_birth_month#29,c_birth_year#30,c_birth_country#31,c_login#32,c_email_address#33,c_last_review_date#34] parquet
            :     :  :           :     +- Project [ss_sold_date_sk#35, ss_customer_sk#38, ss_net_paid#55]
            :     :  :           :        +- Filter (isnotnull(ss_customer_sk#38) AND isnotnull(ss_sold_date_sk#35))
            :     :  :           :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#35,ss_sold_time_sk#36,ss_item_sk#37,ss_customer_sk#38,ss_cdemo_sk#39,ss_hdemo_sk#40,ss_addr_sk#41,ss_store_sk#42,ss_promo_sk#43,ss_ticket_number#44,ss_quantity#45,ss_wholesale_cost#46,ss_list_price#47,ss_sales_price#48,ss_ext_discount_amt#49,ss_ext_sales_price#50,ss_ext_wholesale_cost#51,ss_ext_list_price#52,ss_ext_tax#53,ss_coupon_amt#54,ss_net_paid#55,ss_net_paid_inc_tax#56,ss_net_profit#57] parquet
            :     :  :           +- Project [d_date_sk#58, d_year#64]
            :     :  :              +- Filter ((isnotnull(d_year#64) AND ((d_year#64 = 2001) AND d_year#64 IN (2001,2002))) AND isnotnull(d_date_sk#58))
            :     :  :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#58,d_date_id#59,d_date#60,d_month_seq#61,d_week_seq#62,d_quarter_seq#63,d_year#64,d_dow#65,d_moy#66,d_dom#67,d_qoy#68,d_fy_year#69,d_fy_quarter_seq#70,d_fy_week_seq#71,d_day_name#72,d_quarter_name#73,d_holiday#74,d_weekend#75,d_following_holiday#76,d_first_dom#77,d_last_dom#78,d_same_day_ly#79,d_same_day_lq#80,d_current_day#81,... 4 more fields] parquet
            :     :  +- Aggregate [c_customer_id#658, c_first_name#665, c_last_name#666, d_year#704], [c_customer_id#658 AS customer_id#174, c_first_name#665 AS customer_first_name#175, c_last_name#666 AS customer_last_name#176, sum(ss_net_paid#695) AS year_total#178]
            :     :     +- Project [c_customer_id#658, c_first_name#665, c_last_name#666, ss_net_paid#695, d_year#704]
            :     :        +- Join Inner, (ss_sold_date_sk#675 = d_date_sk#698)
            :     :           :- Project [c_customer_id#658, c_first_name#665, c_last_name#666, ss_sold_date_sk#675, ss_net_paid#695]
            :     :           :  +- Join Inner, (c_customer_sk#657 = ss_customer_sk#678)
            :     :           :     :- Project [c_customer_sk#657, c_customer_id#658, c_first_name#665, c_last_name#666]
            :     :           :     :  +- Filter (isnotnull(c_customer_sk#657) AND isnotnull(c_customer_id#658))
            :     :           :     :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#657,c_customer_id#658,c_current_cdemo_sk#659,c_current_hdemo_sk#660,c_current_addr_sk#661,c_first_shipto_date_sk#662,c_first_sales_date_sk#663,c_salutation#664,c_first_name#665,c_last_name#666,c_preferred_cust_flag#667,c_birth_day#668,c_birth_month#669,c_birth_year#670,c_birth_country#671,c_login#672,c_email_address#673,c_last_review_date#674] parquet
            :     :           :     +- Project [ss_sold_date_sk#675, ss_customer_sk#678, ss_net_paid#695]
            :     :           :        +- Filter (isnotnull(ss_customer_sk#678) AND isnotnull(ss_sold_date_sk#675))
            :     :           :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#675,ss_sold_time_sk#676,ss_item_sk#677,ss_customer_sk#678,ss_cdemo_sk#679,ss_hdemo_sk#680,ss_addr_sk#681,ss_store_sk#682,ss_promo_sk#683,ss_ticket_number#684,ss_quantity#685,ss_wholesale_cost#686,ss_list_price#687,ss_sales_price#688,ss_ext_discount_amt#689,ss_ext_sales_price#690,ss_ext_wholesale_cost#691,ss_ext_list_price#692,ss_ext_tax#693,ss_coupon_amt#694,ss_net_paid#695,ss_net_paid_inc_tax#696,ss_net_profit#697] parquet
            :     :           +- Project [d_date_sk#698, d_year#704]
            :     :              +- Filter ((isnotnull(d_year#704) AND ((d_year#704 = 2002) AND d_year#704 IN (2001,2002))) AND isnotnull(d_date_sk#698))
            :     :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#698,d_date_id#699,d_date#700,d_month_seq#701,d_week_seq#702,d_quarter_seq#703,d_year#704,d_dow#705,d_moy#706,d_dom#707,d_qoy#708,d_fy_year#709,d_fy_quarter_seq#710,d_fy_week_seq#711,d_day_name#712,d_quarter_name#713,d_holiday#714,d_weekend#715,d_following_holiday#716,d_first_dom#717,d_last_dom#718,d_same_day_ly#719,d_same_day_lq#720,d_current_day#721,... 4 more fields] parquet
            :     +- Filter (isnotnull(year_total#10) AND (year_total#10 > 0.0))
            :        +- Aggregate [c_customer_id#882, c_first_name#889, c_last_name#890, d_year#939], [c_customer_id#882 AS customer_id#6, sum(ws_net_paid#928) AS year_total#10]
            :           +- Project [c_customer_id#882, c_first_name#889, c_last_name#890, ws_net_paid#928, d_year#939]
            :              +- Join Inner, (ws_sold_date_sk#899 = d_date_sk#933)
            :                 :- Project [c_customer_id#882, c_first_name#889, c_last_name#890, ws_sold_date_sk#899, ws_net_paid#928]
            :                 :  +- Join Inner, (c_customer_sk#881 = ws_bill_customer_sk#903)
            :                 :     :- Project [c_customer_sk#881, c_customer_id#882, c_first_name#889, c_last_name#890]
            :                 :     :  +- Filter (isnotnull(c_customer_sk#881) AND isnotnull(c_customer_id#882))
            :                 :     :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#881,c_customer_id#882,c_current_cdemo_sk#883,c_current_hdemo_sk#884,c_current_addr_sk#885,c_first_shipto_date_sk#886,c_first_sales_date_sk#887,c_salutation#888,c_first_name#889,c_last_name#890,c_preferred_cust_flag#891,c_birth_day#892,c_birth_month#893,c_birth_year#894,c_birth_country#895,c_login#896,c_email_address#897,c_last_review_date#898] parquet
            :                 :     +- Project [ws_sold_date_sk#899, ws_bill_customer_sk#903, ws_net_paid#928]
            :                 :        +- Filter (isnotnull(ws_bill_customer_sk#903) AND isnotnull(ws_sold_date_sk#899))
            :                 :           +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#899,ws_sold_time_sk#900,ws_ship_date_sk#901,ws_item_sk#902,ws_bill_customer_sk#903,ws_bill_cdemo_sk#904,ws_bill_hdemo_sk#905,ws_bill_addr_sk#906,ws_ship_customer_sk#907,ws_ship_cdemo_sk#908,ws_ship_hdemo_sk#909,ws_ship_addr_sk#910,ws_web_page_sk#911,ws_web_site_sk#912,ws_ship_mode_sk#913,ws_warehouse_sk#914,ws_promo_sk#915,ws_order_number#916,ws_quantity#917,ws_wholesale_cost#918,ws_list_price#919,ws_sales_price#920,ws_ext_discount_amt#921,ws_ext_sales_price#922,... 10 more fields] parquet
            :                 +- Project [d_date_sk#933, d_year#939]
            :                    +- Filter ((isnotnull(d_year#939) AND ((d_year#939 = 2001) AND d_year#939 IN (2001,2002))) AND isnotnull(d_date_sk#933))
            :                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#933,d_date_id#934,d_date#935,d_month_seq#936,d_week_seq#937,d_quarter_seq#938,d_year#939,d_dow#940,d_moy#941,d_dom#942,d_qoy#943,d_fy_year#944,d_fy_quarter_seq#945,d_fy_week_seq#946,d_day_name#947,d_quarter_name#948,d_holiday#949,d_weekend#950,d_following_holiday#951,d_first_dom#952,d_last_dom#953,d_same_day_ly#954,d_same_day_lq#955,d_current_day#956,... 4 more fields] parquet
            +- Aggregate [c_customer_id#1037, c_first_name#1044, c_last_name#1045, d_year#1094], [c_customer_id#1037 AS customer_id#186, sum(ws_net_paid#1083) AS year_total#190]
               +- Project [c_customer_id#1037, c_first_name#1044, c_last_name#1045, ws_net_paid#1083, d_year#1094]
                  +- Join Inner, (ws_sold_date_sk#1054 = d_date_sk#1088)
                     :- Project [c_customer_id#1037, c_first_name#1044, c_last_name#1045, ws_sold_date_sk#1054, ws_net_paid#1083]
                     :  +- Join Inner, (c_customer_sk#1036 = ws_bill_customer_sk#1058)
                     :     :- Project [c_customer_sk#1036, c_customer_id#1037, c_first_name#1044, c_last_name#1045]
                     :     :  +- Filter (isnotnull(c_customer_sk#1036) AND isnotnull(c_customer_id#1037))
                     :     :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#1036,c_customer_id#1037,c_current_cdemo_sk#1038,c_current_hdemo_sk#1039,c_current_addr_sk#1040,c_first_shipto_date_sk#1041,c_first_sales_date_sk#1042,c_salutation#1043,c_first_name#1044,c_last_name#1045,c_preferred_cust_flag#1046,c_birth_day#1047,c_birth_month#1048,c_birth_year#1049,c_birth_country#1050,c_login#1051,c_email_address#1052,c_last_review_date#1053] parquet
                     :     +- Project [ws_sold_date_sk#1054, ws_bill_customer_sk#1058, ws_net_paid#1083]
                     :        +- Filter (isnotnull(ws_bill_customer_sk#1058) AND isnotnull(ws_sold_date_sk#1054))
                     :           +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#1054,ws_sold_time_sk#1055,ws_ship_date_sk#1056,ws_item_sk#1057,ws_bill_customer_sk#1058,ws_bill_cdemo_sk#1059,ws_bill_hdemo_sk#1060,ws_bill_addr_sk#1061,ws_ship_customer_sk#1062,ws_ship_cdemo_sk#1063,ws_ship_hdemo_sk#1064,ws_ship_addr_sk#1065,ws_web_page_sk#1066,ws_web_site_sk#1067,ws_ship_mode_sk#1068,ws_warehouse_sk#1069,ws_promo_sk#1070,ws_order_number#1071,ws_quantity#1072,ws_wholesale_cost#1073,ws_list_price#1074,ws_sales_price#1075,ws_ext_discount_amt#1076,ws_ext_sales_price#1077,... 10 more fields] parquet
                     +- Project [d_date_sk#1088, d_year#1094]
                        +- Filter ((isnotnull(d_year#1094) AND ((d_year#1094 = 2002) AND d_year#1094 IN (2001,2002))) AND isnotnull(d_date_sk#1088))
                           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#1088,d_date_id#1089,d_date#1090,d_month_seq#1091,d_week_seq#1092,d_quarter_seq#1093,d_year#1094,d_dow#1095,d_moy#1096,d_dom#1097,d_qoy#1098,d_fy_year#1099,d_fy_quarter_seq#1100,d_fy_week_seq#1101,d_day_name#1102,d_quarter_name#1103,d_holiday#1104,d_weekend#1105,d_following_holiday#1106,d_first_dom#1107,d_last_dom#1108,d_same_day_ly#1109,d_same_day_lq#1110,d_current_day#1111,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[customer_id#174 ASC NULLS FIRST,customer_id#174 ASC NULLS FIRST,customer_id#174 ASC NULLS FIRST], output=[customer_id#174,customer_first_name#175,customer_last_name#176])
   +- Project [customer_id#174, customer_first_name#175, customer_last_name#176]
      +- SortMergeJoin [customer_id#0], [customer_id#186], Inner, (CASE WHEN (year_total#10 > 0.0) THEN (year_total#190 / year_total#10) END > CASE WHEN (year_total#4 > 0.0) THEN (year_total#178 / year_total#4) END)
         :- Project [customer_id#0, year_total#4, customer_id#174, customer_first_name#175, customer_last_name#176, year_total#178, year_total#10]
         :  +- SortMergeJoin [customer_id#0], [customer_id#6], Inner
         :     :- SortMergeJoin [customer_id#0], [customer_id#174], Inner
         :     :  :- Sort [customer_id#0 ASC NULLS FIRST], false, 0
         :     :  :  +- Exchange hashpartitioning(customer_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=282]
         :     :  :     +- Filter (isnotnull(year_total#4) AND (year_total#4 > 0.0))
         :     :  :        +- HashAggregate(keys=[c_customer_id#18, c_first_name#25, c_last_name#26, d_year#64], functions=[sum(ss_net_paid#55)], output=[customer_id#0, year_total#4])
         :     :  :           +- Exchange hashpartitioning(c_customer_id#18, c_first_name#25, c_last_name#26, d_year#64, 200), ENSURE_REQUIREMENTS, [plan_id=266]
         :     :  :              +- HashAggregate(keys=[c_customer_id#18, c_first_name#25, c_last_name#26, d_year#64], functions=[partial_sum(ss_net_paid#55)], output=[c_customer_id#18, c_first_name#25, c_last_name#26, d_year#64, sum#1141])
         :     :  :                 +- Project [c_customer_id#18, c_first_name#25, c_last_name#26, ss_net_paid#55, d_year#64]
         :     :  :                    +- BroadcastHashJoin [ss_sold_date_sk#35], [d_date_sk#58], Inner, BuildRight, false
         :     :  :                       :- Project [c_customer_id#18, c_first_name#25, c_last_name#26, ss_sold_date_sk#35, ss_net_paid#55]
         :     :  :                       :  +- BroadcastHashJoin [c_customer_sk#17], [ss_customer_sk#38], Inner, BuildLeft, false
         :     :  :                       :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=257]
         :     :  :                       :     :  +- Filter (isnotnull(c_customer_sk#17) AND isnotnull(c_customer_id#18))
         :     :  :                       :     :     +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#17,c_customer_id#18,c_first_name#25,c_last_name#26] Batched: true, DataFilters: [isnotnull(c_customer_sk#17), isnotnull(c_customer_id#18)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string>
         :     :  :                       :     +- Filter (isnotnull(ss_customer_sk#38) AND isnotnull(ss_sold_date_sk#35))
         :     :  :                       :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#35,ss_customer_sk#38,ss_net_paid#55] Batched: true, DataFilters: [isnotnull(ss_customer_sk#38), isnotnull(ss_sold_date_sk#35)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_net_paid:double>
         :     :  :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=261]
         :     :  :                          +- Filter (((isnotnull(d_year#64) AND (d_year#64 = 2001)) AND d_year#64 IN (2001,2002)) AND isnotnull(d_date_sk#58))
         :     :  :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#58,d_year#64] Batched: true, DataFilters: [isnotnull(d_year#64), (d_year#64 = 2001), d_year#64 IN (2001,2002), isnotnull(d_date_sk#58)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), In(d_year, [2001,2002]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     :  +- Sort [customer_id#174 ASC NULLS FIRST], false, 0
         :     :     +- Exchange hashpartitioning(customer_id#174, 200), ENSURE_REQUIREMENTS, [plan_id=283]
         :     :        +- HashAggregate(keys=[c_customer_id#658, c_first_name#665, c_last_name#666, d_year#704], functions=[sum(ss_net_paid#695)], output=[customer_id#174, customer_first_name#175, customer_last_name#176, year_total#178])
         :     :           +- Exchange hashpartitioning(c_customer_id#658, c_first_name#665, c_last_name#666, d_year#704, 200), ENSURE_REQUIREMENTS, [plan_id=278]
         :     :              +- HashAggregate(keys=[c_customer_id#658, c_first_name#665, c_last_name#666, d_year#704], functions=[partial_sum(ss_net_paid#695)], output=[c_customer_id#658, c_first_name#665, c_last_name#666, d_year#704, sum#1143])
         :     :                 +- Project [c_customer_id#658, c_first_name#665, c_last_name#666, ss_net_paid#695, d_year#704]
         :     :                    +- BroadcastHashJoin [ss_sold_date_sk#675], [d_date_sk#698], Inner, BuildRight, false
         :     :                       :- Project [c_customer_id#658, c_first_name#665, c_last_name#666, ss_sold_date_sk#675, ss_net_paid#695]
         :     :                       :  +- BroadcastHashJoin [c_customer_sk#657], [ss_customer_sk#678], Inner, BuildLeft, false
         :     :                       :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=269]
         :     :                       :     :  +- Filter (isnotnull(c_customer_sk#657) AND isnotnull(c_customer_id#658))
         :     :                       :     :     +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#657,c_customer_id#658,c_first_name#665,c_last_name#666] Batched: true, DataFilters: [isnotnull(c_customer_sk#657), isnotnull(c_customer_id#658)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string>
         :     :                       :     +- Filter (isnotnull(ss_customer_sk#678) AND isnotnull(ss_sold_date_sk#675))
         :     :                       :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#675,ss_customer_sk#678,ss_net_paid#695] Batched: true, DataFilters: [isnotnull(ss_customer_sk#678), isnotnull(ss_sold_date_sk#675)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_net_paid:double>
         :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=273]
         :     :                          +- Filter (((isnotnull(d_year#704) AND (d_year#704 = 2002)) AND d_year#704 IN (2001,2002)) AND isnotnull(d_date_sk#698))
         :     :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#698,d_year#704] Batched: true, DataFilters: [isnotnull(d_year#704), (d_year#704 = 2002), d_year#704 IN (2001,2002), isnotnull(d_date_sk#698)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), In(d_year, [2001,2002]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     +- Sort [customer_id#6 ASC NULLS FIRST], false, 0
         :        +- Exchange hashpartitioning(customer_id#6, 200), ENSURE_REQUIREMENTS, [plan_id=301]
         :           +- Filter (isnotnull(year_total#10) AND (year_total#10 > 0.0))
         :              +- HashAggregate(keys=[c_customer_id#882, c_first_name#889, c_last_name#890, d_year#939], functions=[sum(ws_net_paid#928)], output=[customer_id#6, year_total#10])
         :                 +- Exchange hashpartitioning(c_customer_id#882, c_first_name#889, c_last_name#890, d_year#939, 200), ENSURE_REQUIREMENTS, [plan_id=296]
         :                    +- HashAggregate(keys=[c_customer_id#882, c_first_name#889, c_last_name#890, d_year#939], functions=[partial_sum(ws_net_paid#928)], output=[c_customer_id#882, c_first_name#889, c_last_name#890, d_year#939, sum#1145])
         :                       +- Project [c_customer_id#882, c_first_name#889, c_last_name#890, ws_net_paid#928, d_year#939]
         :                          +- BroadcastHashJoin [ws_sold_date_sk#899], [d_date_sk#933], Inner, BuildRight, false
         :                             :- Project [c_customer_id#882, c_first_name#889, c_last_name#890, ws_sold_date_sk#899, ws_net_paid#928]
         :                             :  +- BroadcastHashJoin [c_customer_sk#881], [ws_bill_customer_sk#903], Inner, BuildLeft, false
         :                             :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=287]
         :                             :     :  +- Filter (isnotnull(c_customer_sk#881) AND isnotnull(c_customer_id#882))
         :                             :     :     +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#881,c_customer_id#882,c_first_name#889,c_last_name#890] Batched: true, DataFilters: [isnotnull(c_customer_sk#881), isnotnull(c_customer_id#882)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string>
         :                             :     +- Filter (isnotnull(ws_bill_customer_sk#903) AND isnotnull(ws_sold_date_sk#899))
         :                             :        +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#899,ws_bill_customer_sk#903,ws_net_paid#928] Batched: true, DataFilters: [isnotnull(ws_bill_customer_sk#903), isnotnull(ws_sold_date_sk#899)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int,ws_net_paid:double>
         :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=291]
         :                                +- Filter (((isnotnull(d_year#939) AND (d_year#939 = 2001)) AND d_year#939 IN (2001,2002)) AND isnotnull(d_date_sk#933))
         :                                   +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#933,d_year#939] Batched: true, DataFilters: [isnotnull(d_year#939), (d_year#939 = 2001), d_year#939 IN (2001,2002), isnotnull(d_date_sk#933)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), In(d_year, [2001,2002]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         +- Sort [customer_id#186 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(customer_id#186, 200), ENSURE_REQUIREMENTS, [plan_id=318]
               +- HashAggregate(keys=[c_customer_id#1037, c_first_name#1044, c_last_name#1045, d_year#1094], functions=[sum(ws_net_paid#1083)], output=[customer_id#186, year_total#190])
                  +- Exchange hashpartitioning(c_customer_id#1037, c_first_name#1044, c_last_name#1045, d_year#1094, 200), ENSURE_REQUIREMENTS, [plan_id=314]
                     +- HashAggregate(keys=[c_customer_id#1037, c_first_name#1044, c_last_name#1045, d_year#1094], functions=[partial_sum(ws_net_paid#1083)], output=[c_customer_id#1037, c_first_name#1044, c_last_name#1045, d_year#1094, sum#1147])
                        +- Project [c_customer_id#1037, c_first_name#1044, c_last_name#1045, ws_net_paid#1083, d_year#1094]
                           +- BroadcastHashJoin [ws_sold_date_sk#1054], [d_date_sk#1088], Inner, BuildRight, false
                              :- Project [c_customer_id#1037, c_first_name#1044, c_last_name#1045, ws_sold_date_sk#1054, ws_net_paid#1083]
                              :  +- BroadcastHashJoin [c_customer_sk#1036], [ws_bill_customer_sk#1058], Inner, BuildLeft, false
                              :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=305]
                              :     :  +- Filter (isnotnull(c_customer_sk#1036) AND isnotnull(c_customer_id#1037))
                              :     :     +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#1036,c_customer_id#1037,c_first_name#1044,c_last_name#1045] Batched: true, DataFilters: [isnotnull(c_customer_sk#1036), isnotnull(c_customer_id#1037)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string>
                              :     +- Filter (isnotnull(ws_bill_customer_sk#1058) AND isnotnull(ws_sold_date_sk#1054))
                              :        +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#1054,ws_bill_customer_sk#1058,ws_net_paid#1083] Batched: true, DataFilters: [isnotnull(ws_bill_customer_sk#1058), isnotnull(ws_sold_date_sk#1054)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int,ws_net_paid:double>
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=309]
                                 +- Filter (((isnotnull(d_year#1094) AND (d_year#1094 = 2002)) AND d_year#1094 IN (2001,2002)) AND isnotnull(d_date_sk#1088))
                                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#1088,d_year#1094] Batched: true, DataFilters: [isnotnull(d_year#1094), (d_year#1094 = 2002), d_year#1094 IN (2001,2002), isnotnull(d_date_sk#10..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), In(d_year, [2001,2002]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>

Time taken: 3.699 seconds, Fetched 1 row(s)
