== Parsed Logical Plan ==
CTE [year_total]
:  +- 'SubqueryAlias `year_total`
:     +- 'Union
:        :- 'Aggregate ['c_customer_id, 'c_first_name, 'c_last_name, 'd_year], ['c_customer_id AS customer_id#0, 'c_first_name AS customer_first_name#1, 'c_last_name AS customer_last_name#2, 'd_year AS year#3, 'sum('ss_net_paid) AS year_total#4, s AS sale_type#5]
:        :  +- 'Filter ((('c_customer_sk = 'ss_customer_sk) && ('ss_sold_date_sk = 'd_date_sk)) && 'd_year IN (2001,(2001 + 1)))
:        :     +- 'Join Inner
:        :        :- 'Join Inner
:        :        :  :- 'UnresolvedRelation `customer`
:        :        :  +- 'UnresolvedRelation `store_sales`
:        :        +- 'UnresolvedRelation `date_dim`
:        +- 'Aggregate ['c_customer_id, 'c_first_name, 'c_last_name, 'd_year], ['c_customer_id AS customer_id#6, 'c_first_name AS customer_first_name#7, 'c_last_name AS customer_last_name#8, 'd_year AS year#9, 'sum('ws_net_paid) AS year_total#10, w AS sale_type#11]
:           +- 'Filter ((('c_customer_sk = 'ws_bill_customer_sk) && ('ws_sold_date_sk = 'd_date_sk)) && 'd_year IN (2001,(2001 + 1)))
:              +- 'Join Inner
:                 :- 'Join Inner
:                 :  :- 'UnresolvedRelation `customer`
:                 :  +- 'UnresolvedRelation `web_sales`
:                 +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort [1 ASC NULLS FIRST, 1 ASC NULLS FIRST, 1 ASC NULLS FIRST], true
         +- 'Project ['t_s_secyear.customer_id, 't_s_secyear.customer_first_name, 't_s_secyear.customer_last_name]
            +- 'Filter ((((('t_s_secyear.customer_id = 't_s_firstyear.customer_id) && ('t_s_firstyear.customer_id = 't_w_secyear.customer_id)) && (('t_s_firstyear.customer_id = 't_w_firstyear.customer_id) && ('t_s_firstyear.sale_type = s))) && ((('t_w_firstyear.sale_type = w) && ('t_s_secyear.sale_type = s)) && ('t_w_secyear.sale_type = w))) && (((('t_s_firstyear.year = 2001) && ('t_s_secyear.year = (2001 + 1))) && (('t_w_firstyear.year = 2001) && ('t_w_secyear.year = (2001 + 1)))) && ((('t_s_firstyear.year_total > 0) && ('t_w_firstyear.year_total > 0)) && (CASE WHEN ('t_w_firstyear.year_total > 0) THEN ('t_w_secyear.year_total / 't_w_firstyear.year_total) ELSE null END > CASE WHEN ('t_s_firstyear.year_total > 0) THEN ('t_s_secyear.year_total / 't_s_firstyear.year_total) ELSE null END))))
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
customer_id: string, customer_first_name: string, customer_last_name: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [customer_id#119 ASC NULLS FIRST, customer_id#119 ASC NULLS FIRST, customer_id#119 ASC NULLS FIRST], true
      +- Project [customer_id#119, customer_first_name#120, customer_last_name#121]
         +- Filter (((((customer_id#119 = customer_id#0) && (customer_id#0 = customer_id#131)) && ((customer_id#0 = customer_id#125) && (sale_type#5 = s))) && (((sale_type#130 = w) && (sale_type#124 = s)) && (sale_type#136 = w))) && ((((year#3 = 2001) && (year#122 = (2001 + 1))) && ((year#128 = 2001) && (year#134 = (2001 + 1)))) && (((year_total#4 > cast(0 as double)) && (year_total#129 > cast(0 as double))) && (CASE WHEN (year_total#129 > cast(0 as double)) THEN (year_total#135 / year_total#129) ELSE cast(null as double) END > CASE WHEN (year_total#4 > cast(0 as double)) THEN (year_total#123 / year_total#4) ELSE cast(null as double) END))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias `t_s_firstyear`
               :  :  :  +- SubqueryAlias `year_total`
               :  :  :     +- Union
               :  :  :        :- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#0, c_first_name#22 AS customer_first_name#1, c_last_name#23 AS customer_last_name#2, d_year#61 AS year#3, sum(ss_net_paid#52) AS year_total#4, s AS sale_type#5]
               :  :  :        :  +- Filter (((c_customer_sk#14 = ss_customer_sk#35) && (ss_sold_date_sk#32 = d_date_sk#55)) && d_year#61 IN (2001,(2001 + 1)))
               :  :  :        :     +- Join Inner
               :  :  :        :        :- Join Inner
               :  :  :        :        :  :- SubqueryAlias `tpcds`.`customer`
               :  :  :        :        :  :  +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
               :  :  :        :        :  +- SubqueryAlias `tpcds`.`store_sales`
               :  :  :        :        :     +- Relation[ss_sold_date_sk#32,ss_sold_time_sk#33,ss_item_sk#34,ss_customer_sk#35,ss_cdemo_sk#36,ss_hdemo_sk#37,ss_addr_sk#38,ss_store_sk#39,ss_promo_sk#40,ss_ticket_number#41,ss_quantity#42,ss_wholesale_cost#43,ss_list_price#44,ss_sales_price#45,ss_ext_discount_amt#46,ss_ext_sales_price#47,ss_ext_wholesale_cost#48,ss_ext_list_price#49,ss_ext_tax#50,ss_coupon_amt#51,ss_net_paid#52,ss_net_paid_inc_tax#53,ss_net_profit#54] parquet
               :  :  :        :        +- SubqueryAlias `tpcds`.`date_dim`
               :  :  :        :           +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
               :  :  :        +- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#6, c_first_name#22 AS customer_first_name#7, c_last_name#23 AS customer_last_name#8, d_year#61 AS year#9, sum(ws_net_paid#112) AS year_total#10, w AS sale_type#11]
               :  :  :           +- Filter (((c_customer_sk#14 = ws_bill_customer_sk#87) && (ws_sold_date_sk#83 = d_date_sk#55)) && d_year#61 IN (2001,(2001 + 1)))
               :  :  :              +- Join Inner
               :  :  :                 :- Join Inner
               :  :  :                 :  :- SubqueryAlias `tpcds`.`customer`
               :  :  :                 :  :  +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
               :  :  :                 :  +- SubqueryAlias `tpcds`.`web_sales`
               :  :  :                 :     +- Relation[ws_sold_date_sk#83,ws_sold_time_sk#84,ws_ship_date_sk#85,ws_item_sk#86,ws_bill_customer_sk#87,ws_bill_cdemo_sk#88,ws_bill_hdemo_sk#89,ws_bill_addr_sk#90,ws_ship_customer_sk#91,ws_ship_cdemo_sk#92,ws_ship_hdemo_sk#93,ws_ship_addr_sk#94,ws_web_page_sk#95,ws_web_site_sk#96,ws_ship_mode_sk#97,ws_warehouse_sk#98,ws_promo_sk#99,ws_order_number#100,ws_quantity#101,ws_wholesale_cost#102,ws_list_price#103,ws_sales_price#104,ws_ext_discount_amt#105,ws_ext_sales_price#106,... 10 more fields] parquet
               :  :  :                 +- SubqueryAlias `tpcds`.`date_dim`
               :  :  :                    +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
               :  :  +- SubqueryAlias `t_s_secyear`
               :  :     +- SubqueryAlias `year_total`
               :  :        +- Union
               :  :           :- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#119, c_first_name#22 AS customer_first_name#120, c_last_name#23 AS customer_last_name#121, d_year#61 AS year#122, sum(ss_net_paid#52) AS year_total#123, s AS sale_type#124]
               :  :           :  +- Filter (((c_customer_sk#14 = ss_customer_sk#35) && (ss_sold_date_sk#32 = d_date_sk#55)) && d_year#61 IN (2001,(2001 + 1)))
               :  :           :     +- Join Inner
               :  :           :        :- Join Inner
               :  :           :        :  :- SubqueryAlias `tpcds`.`customer`
               :  :           :        :  :  +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
               :  :           :        :  +- SubqueryAlias `tpcds`.`store_sales`
               :  :           :        :     +- Relation[ss_sold_date_sk#32,ss_sold_time_sk#33,ss_item_sk#34,ss_customer_sk#35,ss_cdemo_sk#36,ss_hdemo_sk#37,ss_addr_sk#38,ss_store_sk#39,ss_promo_sk#40,ss_ticket_number#41,ss_quantity#42,ss_wholesale_cost#43,ss_list_price#44,ss_sales_price#45,ss_ext_discount_amt#46,ss_ext_sales_price#47,ss_ext_wholesale_cost#48,ss_ext_list_price#49,ss_ext_tax#50,ss_coupon_amt#51,ss_net_paid#52,ss_net_paid_inc_tax#53,ss_net_profit#54] parquet
               :  :           :        +- SubqueryAlias `tpcds`.`date_dim`
               :  :           :           +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
               :  :           +- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#6, c_first_name#22 AS customer_first_name#7, c_last_name#23 AS customer_last_name#8, d_year#61 AS year#9, sum(ws_net_paid#112) AS year_total#10, w AS sale_type#11]
               :  :              +- Filter (((c_customer_sk#14 = ws_bill_customer_sk#87) && (ws_sold_date_sk#83 = d_date_sk#55)) && d_year#61 IN (2001,(2001 + 1)))
               :  :                 +- Join Inner
               :  :                    :- Join Inner
               :  :                    :  :- SubqueryAlias `tpcds`.`customer`
               :  :                    :  :  +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
               :  :                    :  +- SubqueryAlias `tpcds`.`web_sales`
               :  :                    :     +- Relation[ws_sold_date_sk#83,ws_sold_time_sk#84,ws_ship_date_sk#85,ws_item_sk#86,ws_bill_customer_sk#87,ws_bill_cdemo_sk#88,ws_bill_hdemo_sk#89,ws_bill_addr_sk#90,ws_ship_customer_sk#91,ws_ship_cdemo_sk#92,ws_ship_hdemo_sk#93,ws_ship_addr_sk#94,ws_web_page_sk#95,ws_web_site_sk#96,ws_ship_mode_sk#97,ws_warehouse_sk#98,ws_promo_sk#99,ws_order_number#100,ws_quantity#101,ws_wholesale_cost#102,ws_list_price#103,ws_sales_price#104,ws_ext_discount_amt#105,ws_ext_sales_price#106,... 10 more fields] parquet
               :  :                    +- SubqueryAlias `tpcds`.`date_dim`
               :  :                       +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
               :  +- SubqueryAlias `t_w_firstyear`
               :     +- SubqueryAlias `year_total`
               :        +- Union
               :           :- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#125, c_first_name#22 AS customer_first_name#126, c_last_name#23 AS customer_last_name#127, d_year#61 AS year#128, sum(ss_net_paid#52) AS year_total#129, s AS sale_type#130]
               :           :  +- Filter (((c_customer_sk#14 = ss_customer_sk#35) && (ss_sold_date_sk#32 = d_date_sk#55)) && d_year#61 IN (2001,(2001 + 1)))
               :           :     +- Join Inner
               :           :        :- Join Inner
               :           :        :  :- SubqueryAlias `tpcds`.`customer`
               :           :        :  :  +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
               :           :        :  +- SubqueryAlias `tpcds`.`store_sales`
               :           :        :     +- Relation[ss_sold_date_sk#32,ss_sold_time_sk#33,ss_item_sk#34,ss_customer_sk#35,ss_cdemo_sk#36,ss_hdemo_sk#37,ss_addr_sk#38,ss_store_sk#39,ss_promo_sk#40,ss_ticket_number#41,ss_quantity#42,ss_wholesale_cost#43,ss_list_price#44,ss_sales_price#45,ss_ext_discount_amt#46,ss_ext_sales_price#47,ss_ext_wholesale_cost#48,ss_ext_list_price#49,ss_ext_tax#50,ss_coupon_amt#51,ss_net_paid#52,ss_net_paid_inc_tax#53,ss_net_profit#54] parquet
               :           :        +- SubqueryAlias `tpcds`.`date_dim`
               :           :           +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
               :           +- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#6, c_first_name#22 AS customer_first_name#7, c_last_name#23 AS customer_last_name#8, d_year#61 AS year#9, sum(ws_net_paid#112) AS year_total#10, w AS sale_type#11]
               :              +- Filter (((c_customer_sk#14 = ws_bill_customer_sk#87) && (ws_sold_date_sk#83 = d_date_sk#55)) && d_year#61 IN (2001,(2001 + 1)))
               :                 +- Join Inner
               :                    :- Join Inner
               :                    :  :- SubqueryAlias `tpcds`.`customer`
               :                    :  :  +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
               :                    :  +- SubqueryAlias `tpcds`.`web_sales`
               :                    :     +- Relation[ws_sold_date_sk#83,ws_sold_time_sk#84,ws_ship_date_sk#85,ws_item_sk#86,ws_bill_customer_sk#87,ws_bill_cdemo_sk#88,ws_bill_hdemo_sk#89,ws_bill_addr_sk#90,ws_ship_customer_sk#91,ws_ship_cdemo_sk#92,ws_ship_hdemo_sk#93,ws_ship_addr_sk#94,ws_web_page_sk#95,ws_web_site_sk#96,ws_ship_mode_sk#97,ws_warehouse_sk#98,ws_promo_sk#99,ws_order_number#100,ws_quantity#101,ws_wholesale_cost#102,ws_list_price#103,ws_sales_price#104,ws_ext_discount_amt#105,ws_ext_sales_price#106,... 10 more fields] parquet
               :                    +- SubqueryAlias `tpcds`.`date_dim`
               :                       +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
               +- SubqueryAlias `t_w_secyear`
                  +- SubqueryAlias `year_total`
                     +- Union
                        :- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#131, c_first_name#22 AS customer_first_name#132, c_last_name#23 AS customer_last_name#133, d_year#61 AS year#134, sum(ss_net_paid#52) AS year_total#135, s AS sale_type#136]
                        :  +- Filter (((c_customer_sk#14 = ss_customer_sk#35) && (ss_sold_date_sk#32 = d_date_sk#55)) && d_year#61 IN (2001,(2001 + 1)))
                        :     +- Join Inner
                        :        :- Join Inner
                        :        :  :- SubqueryAlias `tpcds`.`customer`
                        :        :  :  +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
                        :        :  +- SubqueryAlias `tpcds`.`store_sales`
                        :        :     +- Relation[ss_sold_date_sk#32,ss_sold_time_sk#33,ss_item_sk#34,ss_customer_sk#35,ss_cdemo_sk#36,ss_hdemo_sk#37,ss_addr_sk#38,ss_store_sk#39,ss_promo_sk#40,ss_ticket_number#41,ss_quantity#42,ss_wholesale_cost#43,ss_list_price#44,ss_sales_price#45,ss_ext_discount_amt#46,ss_ext_sales_price#47,ss_ext_wholesale_cost#48,ss_ext_list_price#49,ss_ext_tax#50,ss_coupon_amt#51,ss_net_paid#52,ss_net_paid_inc_tax#53,ss_net_profit#54] parquet
                        :        +- SubqueryAlias `tpcds`.`date_dim`
                        :           +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
                        +- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#6, c_first_name#22 AS customer_first_name#7, c_last_name#23 AS customer_last_name#8, d_year#61 AS year#9, sum(ws_net_paid#112) AS year_total#10, w AS sale_type#11]
                           +- Filter (((c_customer_sk#14 = ws_bill_customer_sk#87) && (ws_sold_date_sk#83 = d_date_sk#55)) && d_year#61 IN (2001,(2001 + 1)))
                              +- Join Inner
                                 :- Join Inner
                                 :  :- SubqueryAlias `tpcds`.`customer`
                                 :  :  +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
                                 :  +- SubqueryAlias `tpcds`.`web_sales`
                                 :     +- Relation[ws_sold_date_sk#83,ws_sold_time_sk#84,ws_ship_date_sk#85,ws_item_sk#86,ws_bill_customer_sk#87,ws_bill_cdemo_sk#88,ws_bill_hdemo_sk#89,ws_bill_addr_sk#90,ws_ship_customer_sk#91,ws_ship_cdemo_sk#92,ws_ship_hdemo_sk#93,ws_ship_addr_sk#94,ws_web_page_sk#95,ws_web_site_sk#96,ws_ship_mode_sk#97,ws_warehouse_sk#98,ws_promo_sk#99,ws_order_number#100,ws_quantity#101,ws_wholesale_cost#102,ws_list_price#103,ws_sales_price#104,ws_ext_discount_amt#105,ws_ext_sales_price#106,... 10 more fields] parquet
                                 +- SubqueryAlias `tpcds`.`date_dim`
                                    +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [customer_id#119 ASC NULLS FIRST, customer_id#119 ASC NULLS FIRST, customer_id#119 ASC NULLS FIRST], true
      +- Project [customer_id#119, customer_first_name#120, customer_last_name#121]
         +- Join Inner, ((customer_id#0 = customer_id#131) && (CASE WHEN (year_total#129 > 0.0) THEN (year_total#135 / year_total#129) ELSE null END > CASE WHEN (year_total#4 > 0.0) THEN (year_total#123 / year_total#4) ELSE null END))
            :- Project [customer_id#0, year_total#4, customer_id#119, customer_first_name#120, customer_last_name#121, year_total#123, year_total#129]
            :  +- Join Inner, (customer_id#0 = customer_id#125)
            :     :- Join Inner, (customer_id#119 = customer_id#0)
            :     :  :- Union
            :     :  :  :- Filter (isnotnull(year_total#4) && (year_total#4 > 0.0))
            :     :  :  :  +- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#0, sum(ss_net_paid#52) AS year_total#4]
            :     :  :  :     +- Project [c_customer_id#15, c_first_name#22, c_last_name#23, ss_net_paid#52, d_year#61]
            :     :  :  :        +- Join Inner, (ss_sold_date_sk#32 = d_date_sk#55)
            :     :  :  :           :- Project [c_customer_id#15, c_first_name#22, c_last_name#23, ss_sold_date_sk#32, ss_net_paid#52]
            :     :  :  :           :  +- Join Inner, (c_customer_sk#14 = ss_customer_sk#35)
            :     :  :  :           :     :- Project [c_customer_sk#14, c_customer_id#15, c_first_name#22, c_last_name#23]
            :     :  :  :           :     :  +- Filter (isnotnull(c_customer_sk#14) && isnotnull(c_customer_id#15))
            :     :  :  :           :     :     +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
            :     :  :  :           :     +- Project [ss_sold_date_sk#32, ss_customer_sk#35, ss_net_paid#52]
            :     :  :  :           :        +- Filter (isnotnull(ss_customer_sk#35) && isnotnull(ss_sold_date_sk#32))
            :     :  :  :           :           +- Relation[ss_sold_date_sk#32,ss_sold_time_sk#33,ss_item_sk#34,ss_customer_sk#35,ss_cdemo_sk#36,ss_hdemo_sk#37,ss_addr_sk#38,ss_store_sk#39,ss_promo_sk#40,ss_ticket_number#41,ss_quantity#42,ss_wholesale_cost#43,ss_list_price#44,ss_sales_price#45,ss_ext_discount_amt#46,ss_ext_sales_price#47,ss_ext_wholesale_cost#48,ss_ext_list_price#49,ss_ext_tax#50,ss_coupon_amt#51,ss_net_paid#52,ss_net_paid_inc_tax#53,ss_net_profit#54] parquet
            :     :  :  :           +- Project [d_date_sk#55, d_year#61]
            :     :  :  :              +- Filter (((isnotnull(d_year#61) && d_year#61 IN (2001,2002)) && (d_year#61 = 2001)) && isnotnull(d_date_sk#55))
            :     :  :  :                 +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
            :     :  :  +- LocalRelation <empty>, [customer_id#6, year_total#10]
            :     :  +- Union
            :     :     :- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#119, c_first_name#22 AS customer_first_name#120, c_last_name#23 AS customer_last_name#121, sum(ss_net_paid#52) AS year_total#123]
            :     :     :  +- Project [c_customer_id#15, c_first_name#22, c_last_name#23, ss_net_paid#52, d_year#61]
            :     :     :     +- Join Inner, (ss_sold_date_sk#32 = d_date_sk#55)
            :     :     :        :- Project [c_customer_id#15, c_first_name#22, c_last_name#23, ss_sold_date_sk#32, ss_net_paid#52]
            :     :     :        :  +- Join Inner, (c_customer_sk#14 = ss_customer_sk#35)
            :     :     :        :     :- Project [c_customer_sk#14, c_customer_id#15, c_first_name#22, c_last_name#23]
            :     :     :        :     :  +- Filter (isnotnull(c_customer_sk#14) && isnotnull(c_customer_id#15))
            :     :     :        :     :     +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
            :     :     :        :     +- Project [ss_sold_date_sk#32, ss_customer_sk#35, ss_net_paid#52]
            :     :     :        :        +- Filter (isnotnull(ss_customer_sk#35) && isnotnull(ss_sold_date_sk#32))
            :     :     :        :           +- Relation[ss_sold_date_sk#32,ss_sold_time_sk#33,ss_item_sk#34,ss_customer_sk#35,ss_cdemo_sk#36,ss_hdemo_sk#37,ss_addr_sk#38,ss_store_sk#39,ss_promo_sk#40,ss_ticket_number#41,ss_quantity#42,ss_wholesale_cost#43,ss_list_price#44,ss_sales_price#45,ss_ext_discount_amt#46,ss_ext_sales_price#47,ss_ext_wholesale_cost#48,ss_ext_list_price#49,ss_ext_tax#50,ss_coupon_amt#51,ss_net_paid#52,ss_net_paid_inc_tax#53,ss_net_profit#54] parquet
            :     :     :        +- Project [d_date_sk#55, d_year#61]
            :     :     :           +- Filter (((isnotnull(d_year#61) && d_year#61 IN (2001,2002)) && (d_year#61 = 2002)) && isnotnull(d_date_sk#55))
            :     :     :              +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
            :     :     +- LocalRelation <empty>, [customer_id#6, customer_first_name#7, customer_last_name#8, year_total#10]
            :     +- Union
            :        :- LocalRelation <empty>, [customer_id#125, year_total#129]
            :        +- Filter (isnotnull(year_total#10) && (year_total#10 > 0.0))
            :           +- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#6, sum(ws_net_paid#112) AS year_total#10]
            :              +- Project [c_customer_id#15, c_first_name#22, c_last_name#23, ws_net_paid#112, d_year#61]
            :                 +- Join Inner, (ws_sold_date_sk#83 = d_date_sk#55)
            :                    :- Project [c_customer_id#15, c_first_name#22, c_last_name#23, ws_sold_date_sk#83, ws_net_paid#112]
            :                    :  +- Join Inner, (c_customer_sk#14 = ws_bill_customer_sk#87)
            :                    :     :- Project [c_customer_sk#14, c_customer_id#15, c_first_name#22, c_last_name#23]
            :                    :     :  +- Filter (isnotnull(c_customer_sk#14) && isnotnull(c_customer_id#15))
            :                    :     :     +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
            :                    :     +- Project [ws_sold_date_sk#83, ws_bill_customer_sk#87, ws_net_paid#112]
            :                    :        +- Filter (isnotnull(ws_bill_customer_sk#87) && isnotnull(ws_sold_date_sk#83))
            :                    :           +- Relation[ws_sold_date_sk#83,ws_sold_time_sk#84,ws_ship_date_sk#85,ws_item_sk#86,ws_bill_customer_sk#87,ws_bill_cdemo_sk#88,ws_bill_hdemo_sk#89,ws_bill_addr_sk#90,ws_ship_customer_sk#91,ws_ship_cdemo_sk#92,ws_ship_hdemo_sk#93,ws_ship_addr_sk#94,ws_web_page_sk#95,ws_web_site_sk#96,ws_ship_mode_sk#97,ws_warehouse_sk#98,ws_promo_sk#99,ws_order_number#100,ws_quantity#101,ws_wholesale_cost#102,ws_list_price#103,ws_sales_price#104,ws_ext_discount_amt#105,ws_ext_sales_price#106,... 10 more fields] parquet
            :                    +- Project [d_date_sk#55, d_year#61]
            :                       +- Filter (((isnotnull(d_year#61) && d_year#61 IN (2001,2002)) && (d_year#61 = 2001)) && isnotnull(d_date_sk#55))
            :                          +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
            +- Union
               :- LocalRelation <empty>, [customer_id#131, year_total#135]
               +- Aggregate [c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], [c_customer_id#15 AS customer_id#6, sum(ws_net_paid#112) AS year_total#10]
                  +- Project [c_customer_id#15, c_first_name#22, c_last_name#23, ws_net_paid#112, d_year#61]
                     +- Join Inner, (ws_sold_date_sk#83 = d_date_sk#55)
                        :- Project [c_customer_id#15, c_first_name#22, c_last_name#23, ws_sold_date_sk#83, ws_net_paid#112]
                        :  +- Join Inner, (c_customer_sk#14 = ws_bill_customer_sk#87)
                        :     :- Project [c_customer_sk#14, c_customer_id#15, c_first_name#22, c_last_name#23]
                        :     :  +- Filter (isnotnull(c_customer_sk#14) && isnotnull(c_customer_id#15))
                        :     :     +- Relation[c_customer_sk#14,c_customer_id#15,c_current_cdemo_sk#16,c_current_hdemo_sk#17,c_current_addr_sk#18,c_first_shipto_date_sk#19,c_first_sales_date_sk#20,c_salutation#21,c_first_name#22,c_last_name#23,c_preferred_cust_flag#24,c_birth_day#25,c_birth_month#26,c_birth_year#27,c_birth_country#28,c_login#29,c_email_address#30,c_last_review_date#31] parquet
                        :     +- Project [ws_sold_date_sk#83, ws_bill_customer_sk#87, ws_net_paid#112]
                        :        +- Filter (isnotnull(ws_bill_customer_sk#87) && isnotnull(ws_sold_date_sk#83))
                        :           +- Relation[ws_sold_date_sk#83,ws_sold_time_sk#84,ws_ship_date_sk#85,ws_item_sk#86,ws_bill_customer_sk#87,ws_bill_cdemo_sk#88,ws_bill_hdemo_sk#89,ws_bill_addr_sk#90,ws_ship_customer_sk#91,ws_ship_cdemo_sk#92,ws_ship_hdemo_sk#93,ws_ship_addr_sk#94,ws_web_page_sk#95,ws_web_site_sk#96,ws_ship_mode_sk#97,ws_warehouse_sk#98,ws_promo_sk#99,ws_order_number#100,ws_quantity#101,ws_wholesale_cost#102,ws_list_price#103,ws_sales_price#104,ws_ext_discount_amt#105,ws_ext_sales_price#106,... 10 more fields] parquet
                        +- Project [d_date_sk#55, d_year#61]
                           +- Filter (((isnotnull(d_year#61) && d_year#61 IN (2001,2002)) && (d_year#61 = 2002)) && isnotnull(d_date_sk#55))
                              +- Relation[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[customer_id#119 ASC NULLS FIRST,customer_id#119 ASC NULLS FIRST,customer_id#119 ASC NULLS FIRST], output=[customer_id#119,customer_first_name#120,customer_last_name#121])
+- *(23) Project [customer_id#119, customer_first_name#120, customer_last_name#121]
   +- *(23) SortMergeJoin [customer_id#0], [customer_id#131], Inner, (CASE WHEN (year_total#129 > 0.0) THEN (year_total#135 / year_total#129) ELSE null END > CASE WHEN (year_total#4 > 0.0) THEN (year_total#123 / year_total#4) ELSE null END)
      :- *(17) Project [customer_id#0, year_total#4, customer_id#119, customer_first_name#120, customer_last_name#121, year_total#123, year_total#129]
      :  +- *(17) SortMergeJoin [customer_id#0], [customer_id#125], Inner
      :     :- *(11) SortMergeJoin [customer_id#0], [customer_id#119], Inner
      :     :  :- *(5) Sort [customer_id#0 ASC NULLS FIRST], false, 0
      :     :  :  +- Exchange hashpartitioning(customer_id#0, 200)
      :     :  :     +- Union
      :     :  :        :- *(4) Filter (isnotnull(year_total#4) && (year_total#4 > 0.0))
      :     :  :        :  +- *(4) HashAggregate(keys=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], functions=[sum(ss_net_paid#52)], output=[customer_id#0, year_total#4])
      :     :  :        :     +- Exchange hashpartitioning(c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61, 200)
      :     :  :        :        +- *(3) HashAggregate(keys=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], functions=[partial_sum(ss_net_paid#52)], output=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61, sum#138])
      :     :  :        :           +- *(3) Project [c_customer_id#15, c_first_name#22, c_last_name#23, ss_net_paid#52, d_year#61]
      :     :  :        :              +- *(3) BroadcastHashJoin [ss_sold_date_sk#32], [d_date_sk#55], Inner, BuildRight
      :     :  :        :                 :- *(3) Project [c_customer_id#15, c_first_name#22, c_last_name#23, ss_sold_date_sk#32, ss_net_paid#52]
      :     :  :        :                 :  +- *(3) BroadcastHashJoin [c_customer_sk#14], [ss_customer_sk#35], Inner, BuildLeft
      :     :  :        :                 :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :  :        :                 :     :  +- *(1) Project [c_customer_sk#14, c_customer_id#15, c_first_name#22, c_last_name#23]
      :     :  :        :                 :     :     +- *(1) Filter (isnotnull(c_customer_sk#14) && isnotnull(c_customer_id#15))
      :     :  :        :                 :     :        +- *(1) FileScan parquet tpcds.customer[c_customer_sk#14,c_customer_id#15,c_first_name#22,c_last_name#23] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_first_name:string,c_last_name:string>
      :     :  :        :                 :     +- *(3) Project [ss_sold_date_sk#32, ss_customer_sk#35, ss_net_paid#52]
      :     :  :        :                 :        +- *(3) Filter (isnotnull(ss_customer_sk#35) && isnotnull(ss_sold_date_sk#32))
      :     :  :        :                 :           +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#32,ss_customer_sk#35,ss_net_paid#52] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_net_paid:double>
      :     :  :        :                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :  :        :                    +- *(2) Project [d_date_sk#55, d_year#61]
      :     :  :        :                       +- *(2) Filter (((isnotnull(d_year#61) && d_year#61 IN (2001,2002)) && (d_year#61 = 2001)) && isnotnull(d_date_sk#55))
      :     :  :        :                          +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#55,d_year#61] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), In(d_year, [2001,2002]), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
      :     :  :        +- LocalTableScan <empty>, [customer_id#6, year_total#10]
      :     :  +- *(10) Sort [customer_id#119 ASC NULLS FIRST], false, 0
      :     :     +- Exchange hashpartitioning(customer_id#119, 200)
      :     :        +- Union
      :     :           :- *(9) HashAggregate(keys=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], functions=[sum(ss_net_paid#52)], output=[customer_id#119, customer_first_name#120, customer_last_name#121, year_total#123])
      :     :           :  +- Exchange hashpartitioning(c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61, 200)
      :     :           :     +- *(8) HashAggregate(keys=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], functions=[partial_sum(ss_net_paid#52)], output=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61, sum#138])
      :     :           :        +- *(8) Project [c_customer_id#15, c_first_name#22, c_last_name#23, ss_net_paid#52, d_year#61]
      :     :           :           +- *(8) BroadcastHashJoin [ss_sold_date_sk#32], [d_date_sk#55], Inner, BuildRight
      :     :           :              :- *(8) Project [c_customer_id#15, c_first_name#22, c_last_name#23, ss_sold_date_sk#32, ss_net_paid#52]
      :     :           :              :  +- *(8) BroadcastHashJoin [c_customer_sk#14], [ss_customer_sk#35], Inner, BuildLeft
      :     :           :              :     :- ReusedExchange [c_customer_sk#14, c_customer_id#15, c_first_name#22, c_last_name#23], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :           :              :     +- *(8) Project [ss_sold_date_sk#32, ss_customer_sk#35, ss_net_paid#52]
      :     :           :              :        +- *(8) Filter (isnotnull(ss_customer_sk#35) && isnotnull(ss_sold_date_sk#32))
      :     :           :              :           +- *(8) FileScan parquet tpcds.store_sales[ss_sold_date_sk#32,ss_customer_sk#35,ss_net_paid#52] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_net_paid:double>
      :     :           :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :           :                 +- *(7) Project [d_date_sk#55, d_year#61]
      :     :           :                    +- *(7) Filter (((isnotnull(d_year#61) && d_year#61 IN (2001,2002)) && (d_year#61 = 2002)) && isnotnull(d_date_sk#55))
      :     :           :                       +- *(7) FileScan parquet tpcds.date_dim[d_date_sk#55,d_year#61] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), In(d_year, [2001,2002]), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
      :     :           +- LocalTableScan <empty>, [customer_id#6, customer_first_name#7, customer_last_name#8, year_total#10]
      :     +- *(16) Sort [customer_id#125 ASC NULLS FIRST], false, 0
      :        +- Exchange hashpartitioning(customer_id#125, 200)
      :           +- Union
      :              :- LocalTableScan <empty>, [customer_id#125, year_total#129]
      :              +- *(15) Filter (isnotnull(year_total#10) && (year_total#10 > 0.0))
      :                 +- *(15) HashAggregate(keys=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], functions=[sum(ws_net_paid#112)], output=[customer_id#6, year_total#10])
      :                    +- Exchange hashpartitioning(c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61, 200)
      :                       +- *(14) HashAggregate(keys=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], functions=[partial_sum(ws_net_paid#112)], output=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61, sum#140])
      :                          +- *(14) Project [c_customer_id#15, c_first_name#22, c_last_name#23, ws_net_paid#112, d_year#61]
      :                             +- *(14) BroadcastHashJoin [ws_sold_date_sk#83], [d_date_sk#55], Inner, BuildRight
      :                                :- *(14) Project [c_customer_id#15, c_first_name#22, c_last_name#23, ws_sold_date_sk#83, ws_net_paid#112]
      :                                :  +- *(14) BroadcastHashJoin [c_customer_sk#14], [ws_bill_customer_sk#87], Inner, BuildLeft
      :                                :     :- ReusedExchange [c_customer_sk#14, c_customer_id#15, c_first_name#22, c_last_name#23], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                                :     +- *(14) Project [ws_sold_date_sk#83, ws_bill_customer_sk#87, ws_net_paid#112]
      :                                :        +- *(14) Filter (isnotnull(ws_bill_customer_sk#87) && isnotnull(ws_sold_date_sk#83))
      :                                :           +- *(14) FileScan parquet tpcds.web_sales[ws_sold_date_sk#83,ws_bill_customer_sk#87,ws_net_paid#112] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int,ws_net_paid:double>
      :                                +- ReusedExchange [d_date_sk#55, d_year#61], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      +- *(22) Sort [customer_id#131 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(customer_id#131, 200)
            +- Union
               :- LocalTableScan <empty>, [customer_id#131, year_total#135]
               +- *(21) HashAggregate(keys=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], functions=[sum(ws_net_paid#112)], output=[customer_id#6, year_total#10])
                  +- Exchange hashpartitioning(c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61, 200)
                     +- *(20) HashAggregate(keys=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61], functions=[partial_sum(ws_net_paid#112)], output=[c_customer_id#15, c_first_name#22, c_last_name#23, d_year#61, sum#140])
                        +- *(20) Project [c_customer_id#15, c_first_name#22, c_last_name#23, ws_net_paid#112, d_year#61]
                           +- *(20) BroadcastHashJoin [ws_sold_date_sk#83], [d_date_sk#55], Inner, BuildRight
                              :- *(20) Project [c_customer_id#15, c_first_name#22, c_last_name#23, ws_sold_date_sk#83, ws_net_paid#112]
                              :  +- *(20) BroadcastHashJoin [c_customer_sk#14], [ws_bill_customer_sk#87], Inner, BuildLeft
                              :     :- ReusedExchange [c_customer_sk#14, c_customer_id#15, c_first_name#22, c_last_name#23], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              :     +- *(20) Project [ws_sold_date_sk#83, ws_bill_customer_sk#87, ws_net_paid#112]
                              :        +- *(20) Filter (isnotnull(ws_bill_customer_sk#87) && isnotnull(ws_sold_date_sk#83))
                              :           +- *(20) FileScan parquet tpcds.web_sales[ws_sold_date_sk#83,ws_bill_customer_sk#87,ws_net_paid#112] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_bill_customer_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int,ws_net_paid:double>
                              +- ReusedExchange [d_date_sk#55, d_year#61], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.427 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 74 in stream 0 using template query74.tpl
------------------------------------------------------^^^

