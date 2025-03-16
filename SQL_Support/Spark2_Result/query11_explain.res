== Parsed Logical Plan ==
CTE [year_total]
:  +- 'SubqueryAlias `year_total`
:     +- 'Union
:        :- 'Aggregate ['c_customer_id, 'c_first_name, 'c_last_name, 'c_preferred_cust_flag, 'c_birth_country, 'c_login, 'c_email_address, 'd_year], ['c_customer_id AS customer_id#0, 'c_first_name AS customer_first_name#1, 'c_last_name AS customer_last_name#2, 'c_preferred_cust_flag AS customer_preferred_cust_flag#3, 'c_birth_country AS customer_birth_country#4, 'c_login AS customer_login#5, 'c_email_address AS customer_email_address#6, 'd_year AS dyear#7, 'sum(('ss_ext_list_price - 'ss_ext_discount_amt)) AS year_total#8, s AS sale_type#9]
:        :  +- 'Filter (('c_customer_sk = 'ss_customer_sk) && ('ss_sold_date_sk = 'd_date_sk))
:        :     +- 'Join Inner
:        :        :- 'Join Inner
:        :        :  :- 'UnresolvedRelation `customer`
:        :        :  +- 'UnresolvedRelation `store_sales`
:        :        +- 'UnresolvedRelation `date_dim`
:        +- 'Aggregate ['c_customer_id, 'c_first_name, 'c_last_name, 'c_preferred_cust_flag, 'c_birth_country, 'c_login, 'c_email_address, 'd_year], ['c_customer_id AS customer_id#10, 'c_first_name AS customer_first_name#11, 'c_last_name AS customer_last_name#12, 'c_preferred_cust_flag AS customer_preferred_cust_flag#13, 'c_birth_country AS customer_birth_country#14, 'c_login AS customer_login#15, 'c_email_address AS customer_email_address#16, 'd_year AS dyear#17, 'sum(('ws_ext_list_price - 'ws_ext_discount_amt)) AS year_total#18, w AS sale_type#19]
:           +- 'Filter (('c_customer_sk = 'ws_bill_customer_sk) && ('ws_sold_date_sk = 'd_date_sk))
:              +- 'Join Inner
:                 :- 'Join Inner
:                 :  :- 'UnresolvedRelation `customer`
:                 :  +- 'UnresolvedRelation `web_sales`
:                 +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['t_s_secyear.customer_id ASC NULLS FIRST, 't_s_secyear.customer_first_name ASC NULLS FIRST, 't_s_secyear.customer_last_name ASC NULLS FIRST, 't_s_secyear.customer_preferred_cust_flag ASC NULLS FIRST], true
         +- 'Project ['t_s_secyear.customer_id, 't_s_secyear.customer_first_name, 't_s_secyear.customer_last_name, 't_s_secyear.customer_preferred_cust_flag]
            +- 'Filter ((((('t_s_secyear.customer_id = 't_s_firstyear.customer_id) && ('t_s_firstyear.customer_id = 't_w_secyear.customer_id)) && (('t_s_firstyear.customer_id = 't_w_firstyear.customer_id) && ('t_s_firstyear.sale_type = s))) && ((('t_w_firstyear.sale_type = w) && ('t_s_secyear.sale_type = s)) && ('t_w_secyear.sale_type = w))) && (((('t_s_firstyear.dyear = 2001) && ('t_s_secyear.dyear = (2001 + 1))) && (('t_w_firstyear.dyear = 2001) && ('t_w_secyear.dyear = (2001 + 1)))) && ((('t_s_firstyear.year_total > 0) && ('t_w_firstyear.year_total > 0)) && (CASE WHEN ('t_w_firstyear.year_total > 0) THEN ('t_w_secyear.year_total / 't_w_firstyear.year_total) ELSE 0.0 END > CASE WHEN ('t_s_firstyear.year_total > 0) THEN ('t_s_secyear.year_total / 't_s_firstyear.year_total) ELSE 0.0 END))))
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'Join Inner
                  :  :  :- 'SubqueryAlias `t_s_firstyear`
                  :  :  :  +- 'UnresolvedRelation `year_total`
                  :  :  +- 'SubqueryAlias `t_s_secyear`
                  :  :     +- 'UnresolvedRelation `year_total`
                  :  +- 'SubqueryAlias `t_w_firstyear`
                  :     +- 'UnresolvedRelation `year_total`
                  +- 'SubqueryAlias `t_w_secyear`
                     +- 'UnresolvedRelation `year_total`

== Analyzed Logical Plan ==
customer_id: string, customer_first_name: string, customer_last_name: string, customer_preferred_cust_flag: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [customer_id#127 ASC NULLS FIRST, customer_first_name#128 ASC NULLS FIRST, customer_last_name#129 ASC NULLS FIRST, customer_preferred_cust_flag#130 ASC NULLS FIRST], true
      +- Project [customer_id#127, customer_first_name#128, customer_last_name#129, customer_preferred_cust_flag#130]
         +- Filter (((((customer_id#127 = customer_id#0) && (customer_id#0 = customer_id#147)) && ((customer_id#0 = customer_id#137) && (sale_type#9 = s))) && (((sale_type#146 = w) && (sale_type#136 = s)) && (sale_type#156 = w))) && ((((dyear#7 = 2001) && (dyear#134 = (2001 + 1))) && ((dyear#144 = 2001) && (dyear#154 = (2001 + 1)))) && (((year_total#8 > cast(0 as double)) && (year_total#145 > cast(0 as double))) && (CASE WHEN (year_total#145 > cast(0 as double)) THEN (year_total#155 / year_total#145) ELSE cast(0.0 as double) END > CASE WHEN (year_total#8 > cast(0 as double)) THEN (year_total#135 / year_total#8) ELSE cast(0.0 as double) END))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias `t_s_firstyear`
               :  :  :  +- SubqueryAlias `year_total`
               :  :  :     +- Union
               :  :  :        :- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#0, c_first_name#30 AS customer_first_name#1, c_last_name#31 AS customer_last_name#2, c_preferred_cust_flag#32 AS customer_preferred_cust_flag#3, c_birth_country#36 AS customer_birth_country#4, c_login#37 AS customer_login#5, c_email_address#38 AS customer_email_address#6, d_year#69 AS dyear#7, sum((ss_ext_list_price#57 - ss_ext_discount_amt#54)) AS year_total#8, s AS sale_type#9]
               :  :  :        :  +- Filter ((c_customer_sk#22 = ss_customer_sk#43) && (ss_sold_date_sk#40 = d_date_sk#63))
               :  :  :        :     +- Join Inner
               :  :  :        :        :- Join Inner
               :  :  :        :        :  :- SubqueryAlias `tpcds`.`customer`
               :  :  :        :        :  :  +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
               :  :  :        :        :  +- SubqueryAlias `tpcds`.`store_sales`
               :  :  :        :        :     +- Relation[ss_sold_date_sk#40,ss_sold_time_sk#41,ss_item_sk#42,ss_customer_sk#43,ss_cdemo_sk#44,ss_hdemo_sk#45,ss_addr_sk#46,ss_store_sk#47,ss_promo_sk#48,ss_ticket_number#49,ss_quantity#50,ss_wholesale_cost#51,ss_list_price#52,ss_sales_price#53,ss_ext_discount_amt#54,ss_ext_sales_price#55,ss_ext_wholesale_cost#56,ss_ext_list_price#57,ss_ext_tax#58,ss_coupon_amt#59,ss_net_paid#60,ss_net_paid_inc_tax#61,ss_net_profit#62] parquet
               :  :  :        :        +- SubqueryAlias `tpcds`.`date_dim`
               :  :  :        :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :  :  :        +- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#10, c_first_name#30 AS customer_first_name#11, c_last_name#31 AS customer_last_name#12, c_preferred_cust_flag#32 AS customer_preferred_cust_flag#13, c_birth_country#36 AS customer_birth_country#14, c_login#37 AS customer_login#15, c_email_address#38 AS customer_email_address#16, d_year#69 AS dyear#17, sum((ws_ext_list_price#116 - ws_ext_discount_amt#113)) AS year_total#18, w AS sale_type#19]
               :  :  :           +- Filter ((c_customer_sk#22 = ws_bill_customer_sk#95) && (ws_sold_date_sk#91 = d_date_sk#63))
               :  :  :              +- Join Inner
               :  :  :                 :- Join Inner
               :  :  :                 :  :- SubqueryAlias `tpcds`.`customer`
               :  :  :                 :  :  +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
               :  :  :                 :  +- SubqueryAlias `tpcds`.`web_sales`
               :  :  :                 :     +- Relation[ws_sold_date_sk#91,ws_sold_time_sk#92,ws_ship_date_sk#93,ws_item_sk#94,ws_bill_customer_sk#95,ws_bill_cdemo_sk#96,ws_bill_hdemo_sk#97,ws_bill_addr_sk#98,ws_ship_customer_sk#99,ws_ship_cdemo_sk#100,ws_ship_hdemo_sk#101,ws_ship_addr_sk#102,ws_web_page_sk#103,ws_web_site_sk#104,ws_ship_mode_sk#105,ws_warehouse_sk#106,ws_promo_sk#107,ws_order_number#108,ws_quantity#109,ws_wholesale_cost#110,ws_list_price#111,ws_sales_price#112,ws_ext_discount_amt#113,ws_ext_sales_price#114,... 10 more fields] parquet
               :  :  :                 +- SubqueryAlias `tpcds`.`date_dim`
               :  :  :                    +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :  :  +- SubqueryAlias `t_s_secyear`
               :  :     +- SubqueryAlias `year_total`
               :  :        +- Union
               :  :           :- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#127, c_first_name#30 AS customer_first_name#128, c_last_name#31 AS customer_last_name#129, c_preferred_cust_flag#32 AS customer_preferred_cust_flag#130, c_birth_country#36 AS customer_birth_country#131, c_login#37 AS customer_login#132, c_email_address#38 AS customer_email_address#133, d_year#69 AS dyear#134, sum((ss_ext_list_price#57 - ss_ext_discount_amt#54)) AS year_total#135, s AS sale_type#136]
               :  :           :  +- Filter ((c_customer_sk#22 = ss_customer_sk#43) && (ss_sold_date_sk#40 = d_date_sk#63))
               :  :           :     +- Join Inner
               :  :           :        :- Join Inner
               :  :           :        :  :- SubqueryAlias `tpcds`.`customer`
               :  :           :        :  :  +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
               :  :           :        :  +- SubqueryAlias `tpcds`.`store_sales`
               :  :           :        :     +- Relation[ss_sold_date_sk#40,ss_sold_time_sk#41,ss_item_sk#42,ss_customer_sk#43,ss_cdemo_sk#44,ss_hdemo_sk#45,ss_addr_sk#46,ss_store_sk#47,ss_promo_sk#48,ss_ticket_number#49,ss_quantity#50,ss_wholesale_cost#51,ss_list_price#52,ss_sales_price#53,ss_ext_discount_amt#54,ss_ext_sales_price#55,ss_ext_wholesale_cost#56,ss_ext_list_price#57,ss_ext_tax#58,ss_coupon_amt#59,ss_net_paid#60,ss_net_paid_inc_tax#61,ss_net_profit#62] parquet
               :  :           :        +- SubqueryAlias `tpcds`.`date_dim`
               :  :           :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :  :           +- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#10, c_first_name#30 AS customer_first_name#11, c_last_name#31 AS customer_last_name#12, c_preferred_cust_flag#32 AS customer_preferred_cust_flag#13, c_birth_country#36 AS customer_birth_country#14, c_login#37 AS customer_login#15, c_email_address#38 AS customer_email_address#16, d_year#69 AS dyear#17, sum((ws_ext_list_price#116 - ws_ext_discount_amt#113)) AS year_total#18, w AS sale_type#19]
               :  :              +- Filter ((c_customer_sk#22 = ws_bill_customer_sk#95) && (ws_sold_date_sk#91 = d_date_sk#63))
               :  :                 +- Join Inner
               :  :                    :- Join Inner
               :  :                    :  :- SubqueryAlias `tpcds`.`customer`
               :  :                    :  :  +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
               :  :                    :  +- SubqueryAlias `tpcds`.`web_sales`
               :  :                    :     +- Relation[ws_sold_date_sk#91,ws_sold_time_sk#92,ws_ship_date_sk#93,ws_item_sk#94,ws_bill_customer_sk#95,ws_bill_cdemo_sk#96,ws_bill_hdemo_sk#97,ws_bill_addr_sk#98,ws_ship_customer_sk#99,ws_ship_cdemo_sk#100,ws_ship_hdemo_sk#101,ws_ship_addr_sk#102,ws_web_page_sk#103,ws_web_site_sk#104,ws_ship_mode_sk#105,ws_warehouse_sk#106,ws_promo_sk#107,ws_order_number#108,ws_quantity#109,ws_wholesale_cost#110,ws_list_price#111,ws_sales_price#112,ws_ext_discount_amt#113,ws_ext_sales_price#114,... 10 more fields] parquet
               :  :                    +- SubqueryAlias `tpcds`.`date_dim`
               :  :                       +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :  +- SubqueryAlias `t_w_firstyear`
               :     +- SubqueryAlias `year_total`
               :        +- Union
               :           :- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#137, c_first_name#30 AS customer_first_name#138, c_last_name#31 AS customer_last_name#139, c_preferred_cust_flag#32 AS customer_preferred_cust_flag#140, c_birth_country#36 AS customer_birth_country#141, c_login#37 AS customer_login#142, c_email_address#38 AS customer_email_address#143, d_year#69 AS dyear#144, sum((ss_ext_list_price#57 - ss_ext_discount_amt#54)) AS year_total#145, s AS sale_type#146]
               :           :  +- Filter ((c_customer_sk#22 = ss_customer_sk#43) && (ss_sold_date_sk#40 = d_date_sk#63))
               :           :     +- Join Inner
               :           :        :- Join Inner
               :           :        :  :- SubqueryAlias `tpcds`.`customer`
               :           :        :  :  +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
               :           :        :  +- SubqueryAlias `tpcds`.`store_sales`
               :           :        :     +- Relation[ss_sold_date_sk#40,ss_sold_time_sk#41,ss_item_sk#42,ss_customer_sk#43,ss_cdemo_sk#44,ss_hdemo_sk#45,ss_addr_sk#46,ss_store_sk#47,ss_promo_sk#48,ss_ticket_number#49,ss_quantity#50,ss_wholesale_cost#51,ss_list_price#52,ss_sales_price#53,ss_ext_discount_amt#54,ss_ext_sales_price#55,ss_ext_wholesale_cost#56,ss_ext_list_price#57,ss_ext_tax#58,ss_coupon_amt#59,ss_net_paid#60,ss_net_paid_inc_tax#61,ss_net_profit#62] parquet
               :           :        +- SubqueryAlias `tpcds`.`date_dim`
               :           :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :           +- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#10, c_first_name#30 AS customer_first_name#11, c_last_name#31 AS customer_last_name#12, c_preferred_cust_flag#32 AS customer_preferred_cust_flag#13, c_birth_country#36 AS customer_birth_country#14, c_login#37 AS customer_login#15, c_email_address#38 AS customer_email_address#16, d_year#69 AS dyear#17, sum((ws_ext_list_price#116 - ws_ext_discount_amt#113)) AS year_total#18, w AS sale_type#19]
               :              +- Filter ((c_customer_sk#22 = ws_bill_customer_sk#95) && (ws_sold_date_sk#91 = d_date_sk#63))
               :                 +- Join Inner
               :                    :- Join Inner
               :                    :  :- SubqueryAlias `tpcds`.`customer`
               :                    :  :  +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
               :                    :  +- SubqueryAlias `tpcds`.`web_sales`
               :                    :     +- Relation[ws_sold_date_sk#91,ws_sold_time_sk#92,ws_ship_date_sk#93,ws_item_sk#94,ws_bill_customer_sk#95,ws_bill_cdemo_sk#96,ws_bill_hdemo_sk#97,ws_bill_addr_sk#98,ws_ship_customer_sk#99,ws_ship_cdemo_sk#100,ws_ship_hdemo_sk#101,ws_ship_addr_sk#102,ws_web_page_sk#103,ws_web_site_sk#104,ws_ship_mode_sk#105,ws_warehouse_sk#106,ws_promo_sk#107,ws_order_number#108,ws_quantity#109,ws_wholesale_cost#110,ws_list_price#111,ws_sales_price#112,ws_ext_discount_amt#113,ws_ext_sales_price#114,... 10 more fields] parquet
               :                    +- SubqueryAlias `tpcds`.`date_dim`
               :                       +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               +- SubqueryAlias `t_w_secyear`
                  +- SubqueryAlias `year_total`
                     +- Union
                        :- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#147, c_first_name#30 AS customer_first_name#148, c_last_name#31 AS customer_last_name#149, c_preferred_cust_flag#32 AS customer_preferred_cust_flag#150, c_birth_country#36 AS customer_birth_country#151, c_login#37 AS customer_login#152, c_email_address#38 AS customer_email_address#153, d_year#69 AS dyear#154, sum((ss_ext_list_price#57 - ss_ext_discount_amt#54)) AS year_total#155, s AS sale_type#156]
                        :  +- Filter ((c_customer_sk#22 = ss_customer_sk#43) && (ss_sold_date_sk#40 = d_date_sk#63))
                        :     +- Join Inner
                        :        :- Join Inner
                        :        :  :- SubqueryAlias `tpcds`.`customer`
                        :        :  :  +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
                        :        :  +- SubqueryAlias `tpcds`.`store_sales`
                        :        :     +- Relation[ss_sold_date_sk#40,ss_sold_time_sk#41,ss_item_sk#42,ss_customer_sk#43,ss_cdemo_sk#44,ss_hdemo_sk#45,ss_addr_sk#46,ss_store_sk#47,ss_promo_sk#48,ss_ticket_number#49,ss_quantity#50,ss_wholesale_cost#51,ss_list_price#52,ss_sales_price#53,ss_ext_discount_amt#54,ss_ext_sales_price#55,ss_ext_wholesale_cost#56,ss_ext_list_price#57,ss_ext_tax#58,ss_coupon_amt#59,ss_net_paid#60,ss_net_paid_inc_tax#61,ss_net_profit#62] parquet
                        :        +- SubqueryAlias `tpcds`.`date_dim`
                        :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
                        +- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#10, c_first_name#30 AS customer_first_name#11, c_last_name#31 AS customer_last_name#12, c_preferred_cust_flag#32 AS customer_preferred_cust_flag#13, c_birth_country#36 AS customer_birth_country#14, c_login#37 AS customer_login#15, c_email_address#38 AS customer_email_address#16, d_year#69 AS dyear#17, sum((ws_ext_list_price#116 - ws_ext_discount_amt#113)) AS year_total#18, w AS sale_type#19]
                           +- Filter ((c_customer_sk#22 = ws_bill_customer_sk#95) && (ws_sold_date_sk#91 = d_date_sk#63))
                              +- Join Inner
                                 :- Join Inner
                                 :  :- SubqueryAlias `tpcds`.`customer`
                                 :  :  +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
                                 :  +- SubqueryAlias `tpcds`.`web_sales`
                                 :     +- Relation[ws_sold_date_sk#91,ws_sold_time_sk#92,ws_ship_date_sk#93,ws_item_sk#94,ws_bill_customer_sk#95,ws_bill_cdemo_sk#96,ws_bill_hdemo_sk#97,ws_bill_addr_sk#98,ws_ship_customer_sk#99,ws_ship_cdemo_sk#100,ws_ship_hdemo_sk#101,ws_ship_addr_sk#102,ws_web_page_sk#103,ws_web_site_sk#104,ws_ship_mode_sk#105,ws_warehouse_sk#106,ws_promo_sk#107,ws_order_number#108,ws_quantity#109,ws_wholesale_cost#110,ws_list_price#111,ws_sales_price#112,ws_ext_discount_amt#113,ws_ext_sales_price#114,... 10 more fields] parquet
                                 +- SubqueryAlias `tpcds`.`date_dim`
                                    +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [customer_id#127 ASC NULLS FIRST, customer_first_name#128 ASC NULLS FIRST, customer_last_name#129 ASC NULLS FIRST, customer_preferred_cust_flag#130 ASC NULLS FIRST], true
      +- Project [customer_id#127, customer_first_name#128, customer_last_name#129, customer_preferred_cust_flag#130]
         +- Join Inner, ((customer_id#0 = customer_id#147) && (CASE WHEN (year_total#145 > 0.0) THEN (year_total#155 / year_total#145) ELSE 0.0 END > CASE WHEN (year_total#8 > 0.0) THEN (year_total#135 / year_total#8) ELSE 0.0 END))
            :- Project [customer_id#0, year_total#8, customer_id#127, customer_first_name#128, customer_last_name#129, customer_preferred_cust_flag#130, year_total#135, year_total#145]
            :  +- Join Inner, (customer_id#0 = customer_id#137)
            :     :- Join Inner, (customer_id#127 = customer_id#0)
            :     :  :- Union
            :     :  :  :- Filter (isnotnull(year_total#8) && (year_total#8 > 0.0))
            :     :  :  :  +- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#0, sum((ss_ext_list_price#57 - ss_ext_discount_amt#54)) AS year_total#8]
            :     :  :  :     +- Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#54, ss_ext_list_price#57, d_year#69]
            :     :  :  :        +- Join Inner, (ss_sold_date_sk#40 = d_date_sk#63)
            :     :  :  :           :- Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_sold_date_sk#40, ss_ext_discount_amt#54, ss_ext_list_price#57]
            :     :  :  :           :  +- Join Inner, (c_customer_sk#22 = ss_customer_sk#43)
            :     :  :  :           :     :- Project [c_customer_sk#22, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38]
            :     :  :  :           :     :  +- Filter (isnotnull(c_customer_sk#22) && isnotnull(c_customer_id#23))
            :     :  :  :           :     :     +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
            :     :  :  :           :     +- Project [ss_sold_date_sk#40, ss_customer_sk#43, ss_ext_discount_amt#54, ss_ext_list_price#57]
            :     :  :  :           :        +- Filter (isnotnull(ss_customer_sk#43) && isnotnull(ss_sold_date_sk#40))
            :     :  :  :           :           +- Relation[ss_sold_date_sk#40,ss_sold_time_sk#41,ss_item_sk#42,ss_customer_sk#43,ss_cdemo_sk#44,ss_hdemo_sk#45,ss_addr_sk#46,ss_store_sk#47,ss_promo_sk#48,ss_ticket_number#49,ss_quantity#50,ss_wholesale_cost#51,ss_list_price#52,ss_sales_price#53,ss_ext_discount_amt#54,ss_ext_sales_price#55,ss_ext_wholesale_cost#56,ss_ext_list_price#57,ss_ext_tax#58,ss_coupon_amt#59,ss_net_paid#60,ss_net_paid_inc_tax#61,ss_net_profit#62] parquet
            :     :  :  :           +- Project [d_date_sk#63, d_year#69]
            :     :  :  :              +- Filter ((isnotnull(d_year#69) && (d_year#69 = 2001)) && isnotnull(d_date_sk#63))
            :     :  :  :                 +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            :     :  :  +- LocalRelation <empty>, [customer_id#10, year_total#18]
            :     :  +- Union
            :     :     :- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#127, c_first_name#30 AS customer_first_name#128, c_last_name#31 AS customer_last_name#129, c_preferred_cust_flag#32 AS customer_preferred_cust_flag#130, sum((ss_ext_list_price#57 - ss_ext_discount_amt#54)) AS year_total#135]
            :     :     :  +- Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#54, ss_ext_list_price#57, d_year#69]
            :     :     :     +- Join Inner, (ss_sold_date_sk#40 = d_date_sk#63)
            :     :     :        :- Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_sold_date_sk#40, ss_ext_discount_amt#54, ss_ext_list_price#57]
            :     :     :        :  +- Join Inner, (c_customer_sk#22 = ss_customer_sk#43)
            :     :     :        :     :- Project [c_customer_sk#22, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38]
            :     :     :        :     :  +- Filter (isnotnull(c_customer_sk#22) && isnotnull(c_customer_id#23))
            :     :     :        :     :     +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
            :     :     :        :     +- Project [ss_sold_date_sk#40, ss_customer_sk#43, ss_ext_discount_amt#54, ss_ext_list_price#57]
            :     :     :        :        +- Filter (isnotnull(ss_customer_sk#43) && isnotnull(ss_sold_date_sk#40))
            :     :     :        :           +- Relation[ss_sold_date_sk#40,ss_sold_time_sk#41,ss_item_sk#42,ss_customer_sk#43,ss_cdemo_sk#44,ss_hdemo_sk#45,ss_addr_sk#46,ss_store_sk#47,ss_promo_sk#48,ss_ticket_number#49,ss_quantity#50,ss_wholesale_cost#51,ss_list_price#52,ss_sales_price#53,ss_ext_discount_amt#54,ss_ext_sales_price#55,ss_ext_wholesale_cost#56,ss_ext_list_price#57,ss_ext_tax#58,ss_coupon_amt#59,ss_net_paid#60,ss_net_paid_inc_tax#61,ss_net_profit#62] parquet
            :     :     :        +- Project [d_date_sk#63, d_year#69]
            :     :     :           +- Filter ((isnotnull(d_year#69) && (d_year#69 = 2002)) && isnotnull(d_date_sk#63))
            :     :     :              +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            :     :     +- LocalRelation <empty>, [customer_id#10, customer_first_name#11, customer_last_name#12, customer_preferred_cust_flag#13, year_total#18]
            :     +- Union
            :        :- LocalRelation <empty>, [customer_id#137, year_total#145]
            :        +- Filter (isnotnull(year_total#18) && (year_total#18 > 0.0))
            :           +- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#10, sum((ws_ext_list_price#116 - ws_ext_discount_amt#113)) AS year_total#18]
            :              +- Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ws_ext_discount_amt#113, ws_ext_list_price#116, d_year#69]
            :                 +- Join Inner, (ws_sold_date_sk#91 = d_date_sk#63)
            :                    :- Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ws_sold_date_sk#91, ws_ext_discount_amt#113, ws_ext_list_price#116]
            :                    :  +- Join Inner, (c_customer_sk#22 = ws_bill_customer_sk#95)
            :                    :     :- Project [c_customer_sk#22, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38]
            :                    :     :  +- Filter (isnotnull(c_customer_sk#22) && isnotnull(c_customer_id#23))
            :                    :     :     +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
            :                    :     +- Project [ws_sold_date_sk#91, ws_bill_customer_sk#95, ws_ext_discount_amt#113, ws_ext_list_price#116]
            :                    :        +- Filter (isnotnull(ws_bill_customer_sk#95) && isnotnull(ws_sold_date_sk#91))
            :                    :           +- Relation[ws_sold_date_sk#91,ws_sold_time_sk#92,ws_ship_date_sk#93,ws_item_sk#94,ws_bill_customer_sk#95,ws_bill_cdemo_sk#96,ws_bill_hdemo_sk#97,ws_bill_addr_sk#98,ws_ship_customer_sk#99,ws_ship_cdemo_sk#100,ws_ship_hdemo_sk#101,ws_ship_addr_sk#102,ws_web_page_sk#103,ws_web_site_sk#104,ws_ship_mode_sk#105,ws_warehouse_sk#106,ws_promo_sk#107,ws_order_number#108,ws_quantity#109,ws_wholesale_cost#110,ws_list_price#111,ws_sales_price#112,ws_ext_discount_amt#113,ws_ext_sales_price#114,... 10 more fields] parquet
            :                    +- Project [d_date_sk#63, d_year#69]
            :                       +- Filter ((isnotnull(d_year#69) && (d_year#69 = 2001)) && isnotnull(d_date_sk#63))
            :                          +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            +- Union
               :- LocalRelation <empty>, [customer_id#147, year_total#155]
               +- Aggregate [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], [c_customer_id#23 AS customer_id#10, sum((ws_ext_list_price#116 - ws_ext_discount_amt#113)) AS year_total#18]
                  +- Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ws_ext_discount_amt#113, ws_ext_list_price#116, d_year#69]
                     +- Join Inner, (ws_sold_date_sk#91 = d_date_sk#63)
                        :- Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ws_sold_date_sk#91, ws_ext_discount_amt#113, ws_ext_list_price#116]
                        :  +- Join Inner, (c_customer_sk#22 = ws_bill_customer_sk#95)
                        :     :- Project [c_customer_sk#22, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38]
                        :     :  +- Filter (isnotnull(c_customer_sk#22) && isnotnull(c_customer_id#23))
                        :     :     +- Relation[c_customer_sk#22,c_customer_id#23,c_current_cdemo_sk#24,c_current_hdemo_sk#25,c_current_addr_sk#26,c_first_shipto_date_sk#27,c_first_sales_date_sk#28,c_salutation#29,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_day#33,c_birth_month#34,c_birth_year#35,c_birth_country#36,c_login#37,c_email_address#38,c_last_review_date#39] parquet
                        :     +- Project [ws_sold_date_sk#91, ws_bill_customer_sk#95, ws_ext_discount_amt#113, ws_ext_list_price#116]
                        :        +- Filter (isnotnull(ws_bill_customer_sk#95) && isnotnull(ws_sold_date_sk#91))
                        :           +- Relation[ws_sold_date_sk#91,ws_sold_time_sk#92,ws_ship_date_sk#93,ws_item_sk#94,ws_bill_customer_sk#95,ws_bill_cdemo_sk#96,ws_bill_hdemo_sk#97,ws_bill_addr_sk#98,ws_ship_customer_sk#99,ws_ship_cdemo_sk#100,ws_ship_hdemo_sk#101,ws_ship_addr_sk#102,ws_web_page_sk#103,ws_web_site_sk#104,ws_ship_mode_sk#105,ws_warehouse_sk#106,ws_promo_sk#107,ws_order_number#108,ws_quantity#109,ws_wholesale_cost#110,ws_list_price#111,ws_sales_price#112,ws_ext_discount_amt#113,ws_ext_sales_price#114,... 10 more fields] parquet
                        +- Project [d_date_sk#63, d_year#69]
                           +- Filter ((isnotnull(d_year#69) && (d_year#69 = 2002)) && isnotnull(d_date_sk#63))
                              +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[customer_id#127 ASC NULLS FIRST,customer_first_name#128 ASC NULLS FIRST,customer_last_name#129 ASC NULLS FIRST,customer_preferred_cust_flag#130 ASC NULLS FIRST], output=[customer_id#127,customer_first_name#128,customer_last_name#129,customer_preferred_cust_flag#130])
+- *(23) Project [customer_id#127, customer_first_name#128, customer_last_name#129, customer_preferred_cust_flag#130]
   +- *(23) SortMergeJoin [customer_id#0], [customer_id#147], Inner, (CASE WHEN (year_total#145 > 0.0) THEN (year_total#155 / year_total#145) ELSE 0.0 END > CASE WHEN (year_total#8 > 0.0) THEN (year_total#135 / year_total#8) ELSE 0.0 END)
      :- *(17) Project [customer_id#0, year_total#8, customer_id#127, customer_first_name#128, customer_last_name#129, customer_preferred_cust_flag#130, year_total#135, year_total#145]
      :  +- *(17) SortMergeJoin [customer_id#0], [customer_id#137], Inner
      :     :- *(11) SortMergeJoin [customer_id#0], [customer_id#127], Inner
      :     :  :- *(5) Sort [customer_id#0 ASC NULLS FIRST], false, 0
      :     :  :  +- Exchange hashpartitioning(customer_id#0, 200)
      :     :  :     +- Union
      :     :  :        :- *(4) Filter (isnotnull(year_total#8) && (year_total#8 > 0.0))
      :     :  :        :  +- *(4) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], functions=[sum((ss_ext_list_price#57 - ss_ext_discount_amt#54))], output=[customer_id#0, year_total#8])
      :     :  :        :     +- Exchange hashpartitioning(c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69, 200)
      :     :  :        :        +- *(3) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], functions=[partial_sum((ss_ext_list_price#57 - ss_ext_discount_amt#54))], output=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69, sum#158])
      :     :  :        :           +- *(3) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#54, ss_ext_list_price#57, d_year#69]
      :     :  :        :              +- *(3) BroadcastHashJoin [ss_sold_date_sk#40], [d_date_sk#63], Inner, BuildRight
      :     :  :        :                 :- *(3) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_sold_date_sk#40, ss_ext_discount_amt#54, ss_ext_list_price#57]
      :     :  :        :                 :  +- *(3) BroadcastHashJoin [c_customer_sk#22], [ss_customer_sk#43], Inner, BuildLeft
      :     :  :        :                 :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :  :        :                 :     :  +- *(1) Project [c_customer_sk#22, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38]
      :     :  :        :                 :     :     +- *(1) Filter (isnotnull(c_customer_sk#22) && isnotnull(c_customer_id#23))
      :     :  :        :                 :     :        +- *(1) FileScan parquet tpcds.customer[c_customer_sk#22,c_customer_id#23,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_country#36,c_login#37,c_email_address#38] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string,c_preferred_...
      :     :  :        :                 :     +- *(3) Project [ss_sold_date_sk#40, ss_customer_sk#43, ss_ext_discount_amt#54, ss_ext_list_price#57]
      :     :  :        :                 :        +- *(3) Filter (isnotnull(ss_customer_sk#43) && isnotnull(ss_sold_date_sk#40))
      :     :  :        :                 :           +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#40,ss_customer_sk#43,ss_ext_discount_amt#54,ss_ext_list_price#57] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_ext_discount_amt:double,ss_ext_list_price:double>
      :     :  :        :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :  :        :                    +- *(2) Project [d_date_sk#63, d_year#69]
      :     :  :        :                       +- *(2) Filter ((isnotnull(d_year#69) && (d_year#69 = 2001)) && isnotnull(d_date_sk#63))
      :     :  :        :                          +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#63,d_year#69] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
      :     :  :        +- LocalTableScan <empty>, [customer_id#10, year_total#18]
      :     :  +- *(10) Sort [customer_id#127 ASC NULLS FIRST], false, 0
      :     :     +- Exchange hashpartitioning(customer_id#127, 200)
      :     :        +- Union
      :     :           :- *(9) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], functions=[sum((ss_ext_list_price#57 - ss_ext_discount_amt#54))], output=[customer_id#127, customer_first_name#128, customer_last_name#129, customer_preferred_cust_flag#130, year_total#135])
      :     :           :  +- Exchange hashpartitioning(c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69, 200)
      :     :           :     +- *(8) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], functions=[partial_sum((ss_ext_list_price#57 - ss_ext_discount_amt#54))], output=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69, sum#158])
      :     :           :        +- *(8) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#54, ss_ext_list_price#57, d_year#69]
      :     :           :           +- *(8) BroadcastHashJoin [ss_sold_date_sk#40], [d_date_sk#63], Inner, BuildRight
      :     :           :              :- *(8) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_sold_date_sk#40, ss_ext_discount_amt#54, ss_ext_list_price#57]
      :     :           :              :  +- *(8) BroadcastHashJoin [c_customer_sk#22], [ss_customer_sk#43], Inner, BuildLeft
      :     :           :              :     :- ReusedExchange [c_customer_sk#22, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :           :              :     +- *(8) Project [ss_sold_date_sk#40, ss_customer_sk#43, ss_ext_discount_amt#54, ss_ext_list_price#57]
      :     :           :              :        +- *(8) Filter (isnotnull(ss_customer_sk#43) && isnotnull(ss_sold_date_sk#40))
      :     :           :              :           +- *(8) FileScan parquet tpcds.store_sales[ss_sold_date_sk#40,ss_customer_sk#43,ss_ext_discount_amt#54,ss_ext_list_price#57] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_ext_discount_amt:double,ss_ext_list_price:double>
      :     :           :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :           :                 +- *(7) Project [d_date_sk#63, d_year#69]
      :     :           :                    +- *(7) Filter ((isnotnull(d_year#69) && (d_year#69 = 2002)) && isnotnull(d_date_sk#63))
      :     :           :                       +- *(7) FileScan parquet tpcds.date_dim[d_date_sk#63,d_year#69] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
      :     :           +- LocalTableScan <empty>, [customer_id#10, customer_first_name#11, customer_last_name#12, customer_preferred_cust_flag#13, year_total#18]
      :     +- *(16) Sort [customer_id#137 ASC NULLS FIRST], false, 0
      :        +- Exchange hashpartitioning(customer_id#137, 200)
      :           +- Union
      :              :- LocalTableScan <empty>, [customer_id#137, year_total#145]
      :              +- *(15) Filter (isnotnull(year_total#18) && (year_total#18 > 0.0))
      :                 +- *(15) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], functions=[sum((ws_ext_list_price#116 - ws_ext_discount_amt#113))], output=[customer_id#10, year_total#18])
      :                    +- Exchange hashpartitioning(c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69, 200)
      :                       +- *(14) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], functions=[partial_sum((ws_ext_list_price#116 - ws_ext_discount_amt#113))], output=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69, sum#160])
      :                          +- *(14) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ws_ext_discount_amt#113, ws_ext_list_price#116, d_year#69]
      :                             +- *(14) BroadcastHashJoin [ws_sold_date_sk#91], [d_date_sk#63], Inner, BuildRight
      :                                :- *(14) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ws_sold_date_sk#91, ws_ext_discount_amt#113, ws_ext_list_price#116]
      :                                :  +- *(14) BroadcastHashJoin [c_customer_sk#22], [ws_bill_customer_sk#95], Inner, BuildLeft
      :                                :     :- ReusedExchange [c_customer_sk#22, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                                :     +- *(14) Project [ws_sold_date_sk#91, ws_bill_customer_sk#95, ws_ext_discount_amt#113, ws_ext_list_price#116]
      :                                :        +- *(14) Filter (isnotnull(ws_bill_customer_sk#95) && isnotnull(ws_sold_date_sk#91))
      :                                :           +- *(14) FileScan parquet tpcds.web_sales[ws_sold_date_sk#91,ws_bill_customer_sk#95,ws_ext_discount_amt#113,ws_ext_list_price#116] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int,ws_ext_discount_amt:double,ws_ext_list_price:d...
      :                                +- ReusedExchange [d_date_sk#63, d_year#69], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      +- *(22) Sort [customer_id#147 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(customer_id#147, 200)
            +- Union
               :- LocalTableScan <empty>, [customer_id#147, year_total#155]
               +- *(21) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], functions=[sum((ws_ext_list_price#116 - ws_ext_discount_amt#113))], output=[customer_id#10, year_total#18])
                  +- Exchange hashpartitioning(c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69, 200)
                     +- *(20) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69], functions=[partial_sum((ws_ext_list_price#116 - ws_ext_discount_amt#113))], output=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69, sum#160])
                        +- *(20) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ws_ext_discount_amt#113, ws_ext_list_price#116, d_year#69]
                           +- *(20) BroadcastHashJoin [ws_sold_date_sk#91], [d_date_sk#63], Inner, BuildRight
                              :- *(20) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ws_sold_date_sk#91, ws_ext_discount_amt#113, ws_ext_list_price#116]
                              :  +- *(20) BroadcastHashJoin [c_customer_sk#22], [ws_bill_customer_sk#95], Inner, BuildLeft
                              :     :- ReusedExchange [c_customer_sk#22, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              :     +- *(20) Project [ws_sold_date_sk#91, ws_bill_customer_sk#95, ws_ext_discount_amt#113, ws_ext_list_price#116]
                              :        +- *(20) Filter (isnotnull(ws_bill_customer_sk#95) && isnotnull(ws_sold_date_sk#91))
                              :           +- *(20) FileScan parquet tpcds.web_sales[ws_sold_date_sk#91,ws_bill_customer_sk#95,ws_ext_discount_amt#113,ws_ext_list_price#116] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int,ws_ext_discount_amt:double,ws_ext_list_price:d...
                              +- ReusedExchange [d_date_sk#63, d_year#69], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.476 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 11 in stream 0 using template query11.tpl
------------------------------------------------------^^^

