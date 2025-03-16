Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579863718
== Parsed Logical Plan ==
CTE [year_total]
:  +- 'SubqueryAlias year_total
:     +- 'Union false, false
:        :- 'Aggregate ['c_customer_id, 'c_first_name, 'c_last_name, 'c_preferred_cust_flag, 'c_birth_country, 'c_login, 'c_email_address, 'd_year], ['c_customer_id AS customer_id#0, 'c_first_name AS customer_first_name#1, 'c_last_name AS customer_last_name#2, 'c_preferred_cust_flag AS customer_preferred_cust_flag#3, 'c_birth_country AS customer_birth_country#4, 'c_login AS customer_login#5, 'c_email_address AS customer_email_address#6, 'd_year AS dyear#7, 'sum(('ss_ext_list_price - 'ss_ext_discount_amt)) AS year_total#8, s AS sale_type#9]
:        :  +- 'Filter (('c_customer_sk = 'ss_customer_sk) AND ('ss_sold_date_sk = 'd_date_sk))
:        :     +- 'Join Inner
:        :        :- 'Join Inner
:        :        :  :- 'UnresolvedRelation [customer], [], false
:        :        :  +- 'UnresolvedRelation [store_sales], [], false
:        :        +- 'UnresolvedRelation [date_dim], [], false
:        +- 'Aggregate ['c_customer_id, 'c_first_name, 'c_last_name, 'c_preferred_cust_flag, 'c_birth_country, 'c_login, 'c_email_address, 'd_year], ['c_customer_id AS customer_id#10, 'c_first_name AS customer_first_name#11, 'c_last_name AS customer_last_name#12, 'c_preferred_cust_flag AS customer_preferred_cust_flag#13, 'c_birth_country AS customer_birth_country#14, 'c_login AS customer_login#15, 'c_email_address AS customer_email_address#16, 'd_year AS dyear#17, 'sum(('ws_ext_list_price - 'ws_ext_discount_amt)) AS year_total#18, w AS sale_type#19]
:           +- 'Filter (('c_customer_sk = 'ws_bill_customer_sk) AND ('ws_sold_date_sk = 'd_date_sk))
:              +- 'Join Inner
:                 :- 'Join Inner
:                 :  :- 'UnresolvedRelation [customer], [], false
:                 :  +- 'UnresolvedRelation [web_sales], [], false
:                 +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['t_s_secyear.customer_id ASC NULLS FIRST, 't_s_secyear.customer_first_name ASC NULLS FIRST, 't_s_secyear.customer_last_name ASC NULLS FIRST, 't_s_secyear.customer_preferred_cust_flag ASC NULLS FIRST], true
         +- 'Project ['t_s_secyear.customer_id, 't_s_secyear.customer_first_name, 't_s_secyear.customer_last_name, 't_s_secyear.customer_preferred_cust_flag]
            +- 'Filter ((((('t_s_secyear.customer_id = 't_s_firstyear.customer_id) AND ('t_s_firstyear.customer_id = 't_w_secyear.customer_id)) AND (('t_s_firstyear.customer_id = 't_w_firstyear.customer_id) AND ('t_s_firstyear.sale_type = s))) AND ((('t_w_firstyear.sale_type = w) AND ('t_s_secyear.sale_type = s)) AND ('t_w_secyear.sale_type = w))) AND (((('t_s_firstyear.dyear = 2001) AND ('t_s_secyear.dyear = (2001 + 1))) AND (('t_w_firstyear.dyear = 2001) AND ('t_w_secyear.dyear = (2001 + 1)))) AND ((('t_s_firstyear.year_total > 0) AND ('t_w_firstyear.year_total > 0)) AND (CASE WHEN ('t_w_firstyear.year_total > 0) THEN ('t_w_secyear.year_total / 't_w_firstyear.year_total) ELSE 0.0 END > CASE WHEN ('t_s_firstyear.year_total > 0) THEN ('t_s_secyear.year_total / 't_s_firstyear.year_total) ELSE 0.0 END))))
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
customer_id: string, customer_first_name: string, customer_last_name: string, customer_preferred_cust_flag: string
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias year_total
:     +- Union false, false
:        :- Aggregate [c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, d_year#72], [c_customer_id#26 AS customer_id#0, c_first_name#33 AS customer_first_name#1, c_last_name#34 AS customer_last_name#2, c_preferred_cust_flag#35 AS customer_preferred_cust_flag#3, c_birth_country#39 AS customer_birth_country#4, c_login#40 AS customer_login#5, c_email_address#41 AS customer_email_address#6, d_year#72 AS dyear#7, sum((ss_ext_list_price#60 - ss_ext_discount_amt#57)) AS year_total#8, s AS sale_type#9]
:        :  +- Filter ((c_customer_sk#25 = ss_customer_sk#46) AND (ss_sold_date_sk#43 = d_date_sk#66))
:        :     +- Join Inner
:        :        :- Join Inner
:        :        :  :- SubqueryAlias spark_catalog.tpcds.customer
:        :        :  :  +- Relation spark_catalog.tpcds.customer[c_customer_sk#25,c_customer_id#26,c_current_cdemo_sk#27,c_current_hdemo_sk#28,c_current_addr_sk#29,c_first_shipto_date_sk#30,c_first_sales_date_sk#31,c_salutation#32,c_first_name#33,c_last_name#34,c_preferred_cust_flag#35,c_birth_day#36,c_birth_month#37,c_birth_year#38,c_birth_country#39,c_login#40,c_email_address#41,c_last_review_date#42] parquet
:        :        :  +- SubqueryAlias spark_catalog.tpcds.store_sales
:        :        :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#43,ss_sold_time_sk#44,ss_item_sk#45,ss_customer_sk#46,ss_cdemo_sk#47,ss_hdemo_sk#48,ss_addr_sk#49,ss_store_sk#50,ss_promo_sk#51,ss_ticket_number#52,ss_quantity#53,ss_wholesale_cost#54,ss_list_price#55,ss_sales_price#56,ss_ext_discount_amt#57,ss_ext_sales_price#58,ss_ext_wholesale_cost#59,ss_ext_list_price#60,ss_ext_tax#61,ss_coupon_amt#62,ss_net_paid#63,ss_net_paid_inc_tax#64,ss_net_profit#65] parquet
:        :        +- SubqueryAlias spark_catalog.tpcds.date_dim
:        :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#66,d_date_id#67,d_date#68,d_month_seq#69,d_week_seq#70,d_quarter_seq#71,d_year#72,d_dow#73,d_moy#74,d_dom#75,d_qoy#76,d_fy_year#77,d_fy_quarter_seq#78,d_fy_week_seq#79,d_day_name#80,d_quarter_name#81,d_holiday#82,d_weekend#83,d_following_holiday#84,d_first_dom#85,d_last_dom#86,d_same_day_ly#87,d_same_day_lq#88,d_current_day#89,... 4 more fields] parquet
:        +- Aggregate [c_customer_id#129, c_first_name#136, c_last_name#137, c_preferred_cust_flag#138, c_birth_country#142, c_login#143, c_email_address#144, d_year#152], [c_customer_id#129 AS customer_id#10, c_first_name#136 AS customer_first_name#11, c_last_name#137 AS customer_last_name#12, c_preferred_cust_flag#138 AS customer_preferred_cust_flag#13, c_birth_country#142 AS customer_birth_country#14, c_login#143 AS customer_login#15, c_email_address#144 AS customer_email_address#16, d_year#152 AS dyear#17, sum((ws_ext_list_price#119 - ws_ext_discount_amt#116)) AS year_total#18, w AS sale_type#19]
:           +- Filter ((c_customer_sk#128 = ws_bill_customer_sk#98) AND (ws_sold_date_sk#94 = d_date_sk#146))
:              +- Join Inner
:                 :- Join Inner
:                 :  :- SubqueryAlias spark_catalog.tpcds.customer
:                 :  :  +- Relation spark_catalog.tpcds.customer[c_customer_sk#128,c_customer_id#129,c_current_cdemo_sk#130,c_current_hdemo_sk#131,c_current_addr_sk#132,c_first_shipto_date_sk#133,c_first_sales_date_sk#134,c_salutation#135,c_first_name#136,c_last_name#137,c_preferred_cust_flag#138,c_birth_day#139,c_birth_month#140,c_birth_year#141,c_birth_country#142,c_login#143,c_email_address#144,c_last_review_date#145] parquet
:                 :  +- SubqueryAlias spark_catalog.tpcds.web_sales
:                 :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#94,ws_sold_time_sk#95,ws_ship_date_sk#96,ws_item_sk#97,ws_bill_customer_sk#98,ws_bill_cdemo_sk#99,ws_bill_hdemo_sk#100,ws_bill_addr_sk#101,ws_ship_customer_sk#102,ws_ship_cdemo_sk#103,ws_ship_hdemo_sk#104,ws_ship_addr_sk#105,ws_web_page_sk#106,ws_web_site_sk#107,ws_ship_mode_sk#108,ws_warehouse_sk#109,ws_promo_sk#110,ws_order_number#111,ws_quantity#112,ws_wholesale_cost#113,ws_list_price#114,ws_sales_price#115,ws_ext_discount_amt#116,ws_ext_sales_price#117,... 10 more fields] parquet
:                 +- SubqueryAlias spark_catalog.tpcds.date_dim
:                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#146,d_date_id#147,d_date#148,d_month_seq#149,d_week_seq#150,d_quarter_seq#151,d_year#152,d_dow#153,d_moy#154,d_dom#155,d_qoy#156,d_fy_year#157,d_fy_quarter_seq#158,d_fy_week_seq#159,d_day_name#160,d_quarter_name#161,d_holiday#162,d_weekend#163,d_following_holiday#164,d_first_dom#165,d_last_dom#166,d_same_day_ly#167,d_same_day_lq#168,d_current_day#169,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [customer_id#182 ASC NULLS FIRST, customer_first_name#183 ASC NULLS FIRST, customer_last_name#184 ASC NULLS FIRST, customer_preferred_cust_flag#185 ASC NULLS FIRST], true
         +- Project [customer_id#182, customer_first_name#183, customer_last_name#184, customer_preferred_cust_flag#185]
            +- Filter (((((customer_id#182 = customer_id#0) AND (customer_id#0 = customer_id#202)) AND ((customer_id#0 = customer_id#192) AND (sale_type#9 = s))) AND (((sale_type#201 = w) AND (sale_type#191 = s)) AND (sale_type#211 = w))) AND ((((dyear#7 = 2001) AND (dyear#189 = (2001 + 1))) AND ((dyear#199 = 2001) AND (dyear#209 = (2001 + 1)))) AND (((year_total#8 > cast(0 as double)) AND (year_total#200 > cast(0 as double))) AND (CASE WHEN (year_total#200 > cast(0 as double)) THEN (year_total#210 / year_total#200) ELSE cast(0.0 as double) END > CASE WHEN (year_total#8 > cast(0 as double)) THEN (year_total#190 / year_total#8) ELSE cast(0.0 as double) END))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- SubqueryAlias t_s_firstyear
                  :  :  :  +- SubqueryAlias year_total
                  :  :  :     +- CTERelationRef 0, true, [customer_id#0, customer_first_name#1, customer_last_name#2, customer_preferred_cust_flag#3, customer_birth_country#4, customer_login#5, customer_email_address#6, dyear#7, year_total#8, sale_type#9], false
                  :  :  +- SubqueryAlias t_s_secyear
                  :  :     +- SubqueryAlias year_total
                  :  :        +- CTERelationRef 0, true, [customer_id#182, customer_first_name#183, customer_last_name#184, customer_preferred_cust_flag#185, customer_birth_country#186, customer_login#187, customer_email_address#188, dyear#189, year_total#190, sale_type#191], false
                  :  +- SubqueryAlias t_w_firstyear
                  :     +- SubqueryAlias year_total
                  :        +- CTERelationRef 0, true, [customer_id#192, customer_first_name#193, customer_last_name#194, customer_preferred_cust_flag#195, customer_birth_country#196, customer_login#197, customer_email_address#198, dyear#199, year_total#200, sale_type#201], false
                  +- SubqueryAlias t_w_secyear
                     +- SubqueryAlias year_total
                        +- CTERelationRef 0, true, [customer_id#202, customer_first_name#203, customer_last_name#204, customer_preferred_cust_flag#205, customer_birth_country#206, customer_login#207, customer_email_address#208, dyear#209, year_total#210, sale_type#211], false

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [customer_id#182 ASC NULLS FIRST, customer_first_name#183 ASC NULLS FIRST, customer_last_name#184 ASC NULLS FIRST, customer_preferred_cust_flag#185 ASC NULLS FIRST], true
      +- Project [customer_id#182, customer_first_name#183, customer_last_name#184, customer_preferred_cust_flag#185]
         +- Join Inner, ((customer_id#0 = customer_id#202) AND (CASE WHEN (year_total#18 > 0.0) THEN (year_total#210 / year_total#18) ELSE 0.0 END > CASE WHEN (year_total#8 > 0.0) THEN (year_total#190 / year_total#8) ELSE 0.0 END))
            :- Project [customer_id#0, year_total#8, customer_id#182, customer_first_name#183, customer_last_name#184, customer_preferred_cust_flag#185, year_total#190, year_total#18]
            :  +- Join Inner, (customer_id#0 = customer_id#10)
            :     :- Join Inner, (customer_id#182 = customer_id#0)
            :     :  :- Filter (isnotnull(year_total#8) AND (year_total#8 > 0.0))
            :     :  :  +- Aggregate [c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, d_year#72], [c_customer_id#26 AS customer_id#0, sum((ss_ext_list_price#60 - ss_ext_discount_amt#57)) AS year_total#8]
            :     :  :     +- Project [c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, ss_ext_discount_amt#57, ss_ext_list_price#60, d_year#72]
            :     :  :        +- Join Inner, (ss_sold_date_sk#43 = d_date_sk#66)
            :     :  :           :- Project [c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, ss_sold_date_sk#43, ss_ext_discount_amt#57, ss_ext_list_price#60]
            :     :  :           :  +- Join Inner, (c_customer_sk#25 = ss_customer_sk#46)
            :     :  :           :     :- Project [c_customer_sk#25, c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41]
            :     :  :           :     :  +- Filter (isnotnull(c_customer_sk#25) AND isnotnull(c_customer_id#26))
            :     :  :           :     :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#25,c_customer_id#26,c_current_cdemo_sk#27,c_current_hdemo_sk#28,c_current_addr_sk#29,c_first_shipto_date_sk#30,c_first_sales_date_sk#31,c_salutation#32,c_first_name#33,c_last_name#34,c_preferred_cust_flag#35,c_birth_day#36,c_birth_month#37,c_birth_year#38,c_birth_country#39,c_login#40,c_email_address#41,c_last_review_date#42] parquet
            :     :  :           :     +- Project [ss_sold_date_sk#43, ss_customer_sk#46, ss_ext_discount_amt#57, ss_ext_list_price#60]
            :     :  :           :        +- Filter (isnotnull(ss_customer_sk#46) AND isnotnull(ss_sold_date_sk#43))
            :     :  :           :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#43,ss_sold_time_sk#44,ss_item_sk#45,ss_customer_sk#46,ss_cdemo_sk#47,ss_hdemo_sk#48,ss_addr_sk#49,ss_store_sk#50,ss_promo_sk#51,ss_ticket_number#52,ss_quantity#53,ss_wholesale_cost#54,ss_list_price#55,ss_sales_price#56,ss_ext_discount_amt#57,ss_ext_sales_price#58,ss_ext_wholesale_cost#59,ss_ext_list_price#60,ss_ext_tax#61,ss_coupon_amt#62,ss_net_paid#63,ss_net_paid_inc_tax#64,ss_net_profit#65] parquet
            :     :  :           +- Project [d_date_sk#66, d_year#72]
            :     :  :              +- Filter ((isnotnull(d_year#72) AND (d_year#72 = 2001)) AND isnotnull(d_date_sk#66))
            :     :  :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#66,d_date_id#67,d_date#68,d_month_seq#69,d_week_seq#70,d_quarter_seq#71,d_year#72,d_dow#73,d_moy#74,d_dom#75,d_qoy#76,d_fy_year#77,d_fy_quarter_seq#78,d_fy_week_seq#79,d_day_name#80,d_quarter_name#81,d_holiday#82,d_weekend#83,d_following_holiday#84,d_first_dom#85,d_last_dom#86,d_same_day_ly#87,d_same_day_lq#88,d_current_day#89,... 4 more fields] parquet
            :     :  +- Aggregate [c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705, d_year#736], [c_customer_id#690 AS customer_id#182, c_first_name#697 AS customer_first_name#183, c_last_name#698 AS customer_last_name#184, c_preferred_cust_flag#699 AS customer_preferred_cust_flag#185, sum((ss_ext_list_price#724 - ss_ext_discount_amt#721)) AS year_total#190]
            :     :     +- Project [c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705, ss_ext_discount_amt#721, ss_ext_list_price#724, d_year#736]
            :     :        +- Join Inner, (ss_sold_date_sk#707 = d_date_sk#730)
            :     :           :- Project [c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705, ss_sold_date_sk#707, ss_ext_discount_amt#721, ss_ext_list_price#724]
            :     :           :  +- Join Inner, (c_customer_sk#689 = ss_customer_sk#710)
            :     :           :     :- Project [c_customer_sk#689, c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705]
            :     :           :     :  +- Filter (isnotnull(c_customer_sk#689) AND isnotnull(c_customer_id#690))
            :     :           :     :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#689,c_customer_id#690,c_current_cdemo_sk#691,c_current_hdemo_sk#692,c_current_addr_sk#693,c_first_shipto_date_sk#694,c_first_sales_date_sk#695,c_salutation#696,c_first_name#697,c_last_name#698,c_preferred_cust_flag#699,c_birth_day#700,c_birth_month#701,c_birth_year#702,c_birth_country#703,c_login#704,c_email_address#705,c_last_review_date#706] parquet
            :     :           :     +- Project [ss_sold_date_sk#707, ss_customer_sk#710, ss_ext_discount_amt#721, ss_ext_list_price#724]
            :     :           :        +- Filter (isnotnull(ss_customer_sk#710) AND isnotnull(ss_sold_date_sk#707))
            :     :           :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#707,ss_sold_time_sk#708,ss_item_sk#709,ss_customer_sk#710,ss_cdemo_sk#711,ss_hdemo_sk#712,ss_addr_sk#713,ss_store_sk#714,ss_promo_sk#715,ss_ticket_number#716,ss_quantity#717,ss_wholesale_cost#718,ss_list_price#719,ss_sales_price#720,ss_ext_discount_amt#721,ss_ext_sales_price#722,ss_ext_wholesale_cost#723,ss_ext_list_price#724,ss_ext_tax#725,ss_coupon_amt#726,ss_net_paid#727,ss_net_paid_inc_tax#728,ss_net_profit#729] parquet
            :     :           +- Project [d_date_sk#730, d_year#736]
            :     :              +- Filter ((isnotnull(d_year#736) AND (d_year#736 = 2002)) AND isnotnull(d_date_sk#730))
            :     :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#730,d_date_id#731,d_date#732,d_month_seq#733,d_week_seq#734,d_quarter_seq#735,d_year#736,d_dow#737,d_moy#738,d_dom#739,d_qoy#740,d_fy_year#741,d_fy_quarter_seq#742,d_fy_week_seq#743,d_day_name#744,d_quarter_name#745,d_holiday#746,d_weekend#747,d_following_holiday#748,d_first_dom#749,d_last_dom#750,d_same_day_ly#751,d_same_day_lq#752,d_current_day#753,... 4 more fields] parquet
            :     +- Filter (isnotnull(year_total#18) AND (year_total#18 > 0.0))
            :        +- Aggregate [c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933, d_year#975], [c_customer_id#918 AS customer_id#10, sum((ws_ext_list_price#960 - ws_ext_discount_amt#957)) AS year_total#18]
            :           +- Project [c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933, ws_ext_discount_amt#957, ws_ext_list_price#960, d_year#975]
            :              +- Join Inner, (ws_sold_date_sk#935 = d_date_sk#969)
            :                 :- Project [c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933, ws_sold_date_sk#935, ws_ext_discount_amt#957, ws_ext_list_price#960]
            :                 :  +- Join Inner, (c_customer_sk#917 = ws_bill_customer_sk#939)
            :                 :     :- Project [c_customer_sk#917, c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933]
            :                 :     :  +- Filter (isnotnull(c_customer_sk#917) AND isnotnull(c_customer_id#918))
            :                 :     :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#917,c_customer_id#918,c_current_cdemo_sk#919,c_current_hdemo_sk#920,c_current_addr_sk#921,c_first_shipto_date_sk#922,c_first_sales_date_sk#923,c_salutation#924,c_first_name#925,c_last_name#926,c_preferred_cust_flag#927,c_birth_day#928,c_birth_month#929,c_birth_year#930,c_birth_country#931,c_login#932,c_email_address#933,c_last_review_date#934] parquet
            :                 :     +- Project [ws_sold_date_sk#935, ws_bill_customer_sk#939, ws_ext_discount_amt#957, ws_ext_list_price#960]
            :                 :        +- Filter (isnotnull(ws_bill_customer_sk#939) AND isnotnull(ws_sold_date_sk#935))
            :                 :           +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#935,ws_sold_time_sk#936,ws_ship_date_sk#937,ws_item_sk#938,ws_bill_customer_sk#939,ws_bill_cdemo_sk#940,ws_bill_hdemo_sk#941,ws_bill_addr_sk#942,ws_ship_customer_sk#943,ws_ship_cdemo_sk#944,ws_ship_hdemo_sk#945,ws_ship_addr_sk#946,ws_web_page_sk#947,ws_web_site_sk#948,ws_ship_mode_sk#949,ws_warehouse_sk#950,ws_promo_sk#951,ws_order_number#952,ws_quantity#953,ws_wholesale_cost#954,ws_list_price#955,ws_sales_price#956,ws_ext_discount_amt#957,ws_ext_sales_price#958,... 10 more fields] parquet
            :                 +- Project [d_date_sk#969, d_year#975]
            :                    +- Filter ((isnotnull(d_year#975) AND (d_year#975 = 2001)) AND isnotnull(d_date_sk#969))
            :                       +- Relation spark_catalog.tpcds.date_dim[d_date_sk#969,d_date_id#970,d_date#971,d_month_seq#972,d_week_seq#973,d_quarter_seq#974,d_year#975,d_dow#976,d_moy#977,d_dom#978,d_qoy#979,d_fy_year#980,d_fy_quarter_seq#981,d_fy_week_seq#982,d_day_name#983,d_quarter_name#984,d_holiday#985,d_weekend#986,d_following_holiday#987,d_first_dom#988,d_last_dom#989,d_same_day_ly#990,d_same_day_lq#991,d_current_day#992,... 4 more fields] parquet
            +- Aggregate [c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092, d_year#1134], [c_customer_id#1077 AS customer_id#202, sum((ws_ext_list_price#1119 - ws_ext_discount_amt#1116)) AS year_total#210]
               +- Project [c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092, ws_ext_discount_amt#1116, ws_ext_list_price#1119, d_year#1134]
                  +- Join Inner, (ws_sold_date_sk#1094 = d_date_sk#1128)
                     :- Project [c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092, ws_sold_date_sk#1094, ws_ext_discount_amt#1116, ws_ext_list_price#1119]
                     :  +- Join Inner, (c_customer_sk#1076 = ws_bill_customer_sk#1098)
                     :     :- Project [c_customer_sk#1076, c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092]
                     :     :  +- Filter (isnotnull(c_customer_sk#1076) AND isnotnull(c_customer_id#1077))
                     :     :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#1076,c_customer_id#1077,c_current_cdemo_sk#1078,c_current_hdemo_sk#1079,c_current_addr_sk#1080,c_first_shipto_date_sk#1081,c_first_sales_date_sk#1082,c_salutation#1083,c_first_name#1084,c_last_name#1085,c_preferred_cust_flag#1086,c_birth_day#1087,c_birth_month#1088,c_birth_year#1089,c_birth_country#1090,c_login#1091,c_email_address#1092,c_last_review_date#1093] parquet
                     :     +- Project [ws_sold_date_sk#1094, ws_bill_customer_sk#1098, ws_ext_discount_amt#1116, ws_ext_list_price#1119]
                     :        +- Filter (isnotnull(ws_bill_customer_sk#1098) AND isnotnull(ws_sold_date_sk#1094))
                     :           +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#1094,ws_sold_time_sk#1095,ws_ship_date_sk#1096,ws_item_sk#1097,ws_bill_customer_sk#1098,ws_bill_cdemo_sk#1099,ws_bill_hdemo_sk#1100,ws_bill_addr_sk#1101,ws_ship_customer_sk#1102,ws_ship_cdemo_sk#1103,ws_ship_hdemo_sk#1104,ws_ship_addr_sk#1105,ws_web_page_sk#1106,ws_web_site_sk#1107,ws_ship_mode_sk#1108,ws_warehouse_sk#1109,ws_promo_sk#1110,ws_order_number#1111,ws_quantity#1112,ws_wholesale_cost#1113,ws_list_price#1114,ws_sales_price#1115,ws_ext_discount_amt#1116,ws_ext_sales_price#1117,... 10 more fields] parquet
                     +- Project [d_date_sk#1128, d_year#1134]
                        +- Filter ((isnotnull(d_year#1134) AND (d_year#1134 = 2002)) AND isnotnull(d_date_sk#1128))
                           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#1128,d_date_id#1129,d_date#1130,d_month_seq#1131,d_week_seq#1132,d_quarter_seq#1133,d_year#1134,d_dow#1135,d_moy#1136,d_dom#1137,d_qoy#1138,d_fy_year#1139,d_fy_quarter_seq#1140,d_fy_week_seq#1141,d_day_name#1142,d_quarter_name#1143,d_holiday#1144,d_weekend#1145,d_following_holiday#1146,d_first_dom#1147,d_last_dom#1148,d_same_day_ly#1149,d_same_day_lq#1150,d_current_day#1151,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[customer_id#182 ASC NULLS FIRST,customer_first_name#183 ASC NULLS FIRST,customer_last_name#184 ASC NULLS FIRST,customer_preferred_cust_flag#185 ASC NULLS FIRST], output=[customer_id#182,customer_first_name#183,customer_last_name#184,customer_preferred_cust_flag#185])
   +- Project [customer_id#182, customer_first_name#183, customer_last_name#184, customer_preferred_cust_flag#185]
      +- SortMergeJoin [customer_id#0], [customer_id#202], Inner, (CASE WHEN (year_total#18 > 0.0) THEN (year_total#210 / year_total#18) ELSE 0.0 END > CASE WHEN (year_total#8 > 0.0) THEN (year_total#190 / year_total#8) ELSE 0.0 END)
         :- Project [customer_id#0, year_total#8, customer_id#182, customer_first_name#183, customer_last_name#184, customer_preferred_cust_flag#185, year_total#190, year_total#18]
         :  +- SortMergeJoin [customer_id#0], [customer_id#10], Inner
         :     :- SortMergeJoin [customer_id#0], [customer_id#182], Inner
         :     :  :- Sort [customer_id#0 ASC NULLS FIRST], false, 0
         :     :  :  +- Exchange hashpartitioning(customer_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=282]
         :     :  :     +- Filter (isnotnull(year_total#8) AND (year_total#8 > 0.0))
         :     :  :        +- HashAggregate(keys=[c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, d_year#72], functions=[sum((ss_ext_list_price#60 - ss_ext_discount_amt#57))], output=[customer_id#0, year_total#8])
         :     :  :           +- Exchange hashpartitioning(c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, d_year#72, 200), ENSURE_REQUIREMENTS, [plan_id=266]
         :     :  :              +- HashAggregate(keys=[c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, d_year#72], functions=[partial_sum((ss_ext_list_price#60 - ss_ext_discount_amt#57))], output=[c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, d_year#72, sum#1197])
         :     :  :                 +- Project [c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, ss_ext_discount_amt#57, ss_ext_list_price#60, d_year#72]
         :     :  :                    +- BroadcastHashJoin [ss_sold_date_sk#43], [d_date_sk#66], Inner, BuildRight, false
         :     :  :                       :- Project [c_customer_id#26, c_first_name#33, c_last_name#34, c_preferred_cust_flag#35, c_birth_country#39, c_login#40, c_email_address#41, ss_sold_date_sk#43, ss_ext_discount_amt#57, ss_ext_list_price#60]
         :     :  :                       :  +- BroadcastHashJoin [c_customer_sk#25], [ss_customer_sk#46], Inner, BuildLeft, false
         :     :  :                       :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=257]
         :     :  :                       :     :  +- Filter (isnotnull(c_customer_sk#25) AND isnotnull(c_customer_id#26))
         :     :  :                       :     :     +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#25,c_customer_id#26,c_first_name#33,c_last_name#34,c_preferred_cust_flag#35,c_birth_country#39,c_login#40,c_email_address#41] Batched: true, DataFilters: [isnotnull(c_customer_sk#25), isnotnull(c_customer_id#26)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string,c_preferred_...
         :     :  :                       :     +- Filter (isnotnull(ss_customer_sk#46) AND isnotnull(ss_sold_date_sk#43))
         :     :  :                       :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#43,ss_customer_sk#46,ss_ext_discount_amt#57,ss_ext_list_price#60] Batched: true, DataFilters: [isnotnull(ss_customer_sk#46), isnotnull(ss_sold_date_sk#43)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_ext_discount_amt:double,ss_ext_list_price:double>
         :     :  :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=261]
         :     :  :                          +- Filter ((isnotnull(d_year#72) AND (d_year#72 = 2001)) AND isnotnull(d_date_sk#66))
         :     :  :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#66,d_year#72] Batched: true, DataFilters: [isnotnull(d_year#72), (d_year#72 = 2001), isnotnull(d_date_sk#66)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     :  +- Sort [customer_id#182 ASC NULLS FIRST], false, 0
         :     :     +- Exchange hashpartitioning(customer_id#182, 200), ENSURE_REQUIREMENTS, [plan_id=283]
         :     :        +- HashAggregate(keys=[c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705, d_year#736], functions=[sum((ss_ext_list_price#724 - ss_ext_discount_amt#721))], output=[customer_id#182, customer_first_name#183, customer_last_name#184, customer_preferred_cust_flag#185, year_total#190])
         :     :           +- Exchange hashpartitioning(c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705, d_year#736, 200), ENSURE_REQUIREMENTS, [plan_id=278]
         :     :              +- HashAggregate(keys=[c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705, d_year#736], functions=[partial_sum((ss_ext_list_price#724 - ss_ext_discount_amt#721))], output=[c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705, d_year#736, sum#1199])
         :     :                 +- Project [c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705, ss_ext_discount_amt#721, ss_ext_list_price#724, d_year#736]
         :     :                    +- BroadcastHashJoin [ss_sold_date_sk#707], [d_date_sk#730], Inner, BuildRight, false
         :     :                       :- Project [c_customer_id#690, c_first_name#697, c_last_name#698, c_preferred_cust_flag#699, c_birth_country#703, c_login#704, c_email_address#705, ss_sold_date_sk#707, ss_ext_discount_amt#721, ss_ext_list_price#724]
         :     :                       :  +- BroadcastHashJoin [c_customer_sk#689], [ss_customer_sk#710], Inner, BuildLeft, false
         :     :                       :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=269]
         :     :                       :     :  +- Filter (isnotnull(c_customer_sk#689) AND isnotnull(c_customer_id#690))
         :     :                       :     :     +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#689,c_customer_id#690,c_first_name#697,c_last_name#698,c_preferred_cust_flag#699,c_birth_country#703,c_login#704,c_email_address#705] Batched: true, DataFilters: [isnotnull(c_customer_sk#689), isnotnull(c_customer_id#690)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string,c_preferred_...
         :     :                       :     +- Filter (isnotnull(ss_customer_sk#710) AND isnotnull(ss_sold_date_sk#707))
         :     :                       :        +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#707,ss_customer_sk#710,ss_ext_discount_amt#721,ss_ext_list_price#724] Batched: true, DataFilters: [isnotnull(ss_customer_sk#710), isnotnull(ss_sold_date_sk#707)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_ext_discount_amt:double,ss_ext_list_price:double>
         :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=273]
         :     :                          +- Filter ((isnotnull(d_year#736) AND (d_year#736 = 2002)) AND isnotnull(d_date_sk#730))
         :     :                             +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#730,d_year#736] Batched: true, DataFilters: [isnotnull(d_year#736), (d_year#736 = 2002), isnotnull(d_date_sk#730)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         :     +- Sort [customer_id#10 ASC NULLS FIRST], false, 0
         :        +- Exchange hashpartitioning(customer_id#10, 200), ENSURE_REQUIREMENTS, [plan_id=301]
         :           +- Filter (isnotnull(year_total#18) AND (year_total#18 > 0.0))
         :              +- HashAggregate(keys=[c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933, d_year#975], functions=[sum((ws_ext_list_price#960 - ws_ext_discount_amt#957))], output=[customer_id#10, year_total#18])
         :                 +- Exchange hashpartitioning(c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933, d_year#975, 200), ENSURE_REQUIREMENTS, [plan_id=296]
         :                    +- HashAggregate(keys=[c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933, d_year#975], functions=[partial_sum((ws_ext_list_price#960 - ws_ext_discount_amt#957))], output=[c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933, d_year#975, sum#1201])
         :                       +- Project [c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933, ws_ext_discount_amt#957, ws_ext_list_price#960, d_year#975]
         :                          +- BroadcastHashJoin [ws_sold_date_sk#935], [d_date_sk#969], Inner, BuildRight, false
         :                             :- Project [c_customer_id#918, c_first_name#925, c_last_name#926, c_preferred_cust_flag#927, c_birth_country#931, c_login#932, c_email_address#933, ws_sold_date_sk#935, ws_ext_discount_amt#957, ws_ext_list_price#960]
         :                             :  +- BroadcastHashJoin [c_customer_sk#917], [ws_bill_customer_sk#939], Inner, BuildLeft, false
         :                             :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=287]
         :                             :     :  +- Filter (isnotnull(c_customer_sk#917) AND isnotnull(c_customer_id#918))
         :                             :     :     +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#917,c_customer_id#918,c_first_name#925,c_last_name#926,c_preferred_cust_flag#927,c_birth_country#931,c_login#932,c_email_address#933] Batched: true, DataFilters: [isnotnull(c_customer_sk#917), isnotnull(c_customer_id#918)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string,c_preferred_...
         :                             :     +- Filter (isnotnull(ws_bill_customer_sk#939) AND isnotnull(ws_sold_date_sk#935))
         :                             :        +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#935,ws_bill_customer_sk#939,ws_ext_discount_amt#957,ws_ext_list_price#960] Batched: true, DataFilters: [isnotnull(ws_bill_customer_sk#939), isnotnull(ws_sold_date_sk#935)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int,ws_ext_discount_amt:double,ws_ext_list_price:d...
         :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=291]
         :                                +- Filter ((isnotnull(d_year#975) AND (d_year#975 = 2001)) AND isnotnull(d_date_sk#969))
         :                                   +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#969,d_year#975] Batched: true, DataFilters: [isnotnull(d_year#975), (d_year#975 = 2001), isnotnull(d_date_sk#969)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
         +- Sort [customer_id#202 ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(customer_id#202, 200), ENSURE_REQUIREMENTS, [plan_id=318]
               +- HashAggregate(keys=[c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092, d_year#1134], functions=[sum((ws_ext_list_price#1119 - ws_ext_discount_amt#1116))], output=[customer_id#202, year_total#210])
                  +- Exchange hashpartitioning(c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092, d_year#1134, 200), ENSURE_REQUIREMENTS, [plan_id=314]
                     +- HashAggregate(keys=[c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092, d_year#1134], functions=[partial_sum((ws_ext_list_price#1119 - ws_ext_discount_amt#1116))], output=[c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092, d_year#1134, sum#1203])
                        +- Project [c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092, ws_ext_discount_amt#1116, ws_ext_list_price#1119, d_year#1134]
                           +- BroadcastHashJoin [ws_sold_date_sk#1094], [d_date_sk#1128], Inner, BuildRight, false
                              :- Project [c_customer_id#1077, c_first_name#1084, c_last_name#1085, c_preferred_cust_flag#1086, c_birth_country#1090, c_login#1091, c_email_address#1092, ws_sold_date_sk#1094, ws_ext_discount_amt#1116, ws_ext_list_price#1119]
                              :  +- BroadcastHashJoin [c_customer_sk#1076], [ws_bill_customer_sk#1098], Inner, BuildLeft, false
                              :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=305]
                              :     :  +- Filter (isnotnull(c_customer_sk#1076) AND isnotnull(c_customer_id#1077))
                              :     :     +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#1076,c_customer_id#1077,c_first_name#1084,c_last_name#1085,c_preferred_cust_flag#1086,c_birth_country#1090,c_login#1091,c_email_address#1092] Batched: true, DataFilters: [isnotnull(c_customer_sk#1076), isnotnull(c_customer_id#1077)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string,c_preferred_...
                              :     +- Filter (isnotnull(ws_bill_customer_sk#1098) AND isnotnull(ws_sold_date_sk#1094))
                              :        +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#1094,ws_bill_customer_sk#1098,ws_ext_discount_amt#1116,ws_ext_list_price#1119] Batched: true, DataFilters: [isnotnull(ws_bill_customer_sk#1098), isnotnull(ws_sold_date_sk#1094)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int,ws_ext_discount_amt:double,ws_ext_list_price:d...
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=309]
                                 +- Filter ((isnotnull(d_year#1134) AND (d_year#1134 = 2002)) AND isnotnull(d_date_sk#1128))
                                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#1128,d_year#1134] Batched: true, DataFilters: [isnotnull(d_year#1134), (d_year#1134 = 2002), isnotnull(d_date_sk#1128)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>

Time taken: 3.937 seconds, Fetched 1 row(s)
