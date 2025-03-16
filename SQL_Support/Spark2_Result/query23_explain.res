== Parsed Logical Plan ==
CTE [frequent_ss_items, max_store_sales, best_ss_customer]
:  :- 'SubqueryAlias `frequent_ss_items`
:  :  +- 'UnresolvedHaving ('count(1) > 4)
:  :     +- 'Aggregate ['substr('i_item_desc, 1, 30), 'i_item_sk, 'd_date], ['substr('i_item_desc, 1, 30) AS itemdesc#6, 'i_item_sk AS item_sk#7, 'd_date AS solddate#8, 'count(1) AS cnt#9]
:  :        +- 'Filter ((('ss_sold_date_sk = 'd_date_sk) && ('ss_item_sk = 'i_item_sk)) && 'd_year IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
:  :           +- 'Join Inner
:  :              :- 'Join Inner
:  :              :  :- 'UnresolvedRelation `store_sales`
:  :              :  +- 'UnresolvedRelation `date_dim`
:  :              +- 'UnresolvedRelation `item`
:  :- 'SubqueryAlias `max_store_sales`
:  :  +- 'Project ['max('csales) AS tpcds_cmax#11]
:  :     +- 'SubqueryAlias `__auto_generated_subquery_name`
:  :        +- 'Aggregate ['c_customer_sk], ['c_customer_sk, 'sum(('ss_quantity * 'ss_sales_price)) AS csales#10]
:  :           +- 'Filter ((('ss_customer_sk = 'c_customer_sk) && ('ss_sold_date_sk = 'd_date_sk)) && 'd_year IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
:  :              +- 'Join Inner
:  :                 :- 'Join Inner
:  :                 :  :- 'UnresolvedRelation `store_sales`
:  :                 :  +- 'UnresolvedRelation `customer`
:  :                 +- 'UnresolvedRelation `date_dim`
:  +- 'SubqueryAlias `best_ss_customer`
:     +- 'UnresolvedHaving ('sum(('ss_quantity * 'ss_sales_price)) > ((50 / 100.0) * scalar-subquery#13 []))
:        :  +- 'Project [*]
:        :     +- 'UnresolvedRelation `max_store_sales`
:        +- 'Aggregate ['c_customer_sk], ['c_customer_sk, 'sum(('ss_quantity * 'ss_sales_price)) AS ssales#12]
:           +- 'Filter ('ss_customer_sk = 'c_customer_sk)
:              +- 'Join Inner
:                 :- 'UnresolvedRelation `store_sales`
:                 +- 'UnresolvedRelation `customer`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Project [unresolvedalias('sum('sales), None)]
         +- 'SubqueryAlias `__auto_generated_subquery_name`
            +- 'Union
               :- 'Project [('cs_quantity * 'cs_list_price) AS sales#0]
               :  +- 'Filter (((('d_year = 2000) && ('d_moy = 2)) && ('cs_sold_date_sk = 'd_date_sk)) && ('cs_item_sk IN (list#1 []) && 'cs_bill_customer_sk IN (list#2 [])))
               :     :  :- 'Project ['item_sk]
               :     :  :  +- 'UnresolvedRelation `frequent_ss_items`
               :     :  +- 'Project ['c_customer_sk]
               :     :     +- 'UnresolvedRelation `best_ss_customer`
               :     +- 'Join Inner
               :        :- 'UnresolvedRelation `catalog_sales`
               :        +- 'UnresolvedRelation `date_dim`
               +- 'Project [('ws_quantity * 'ws_list_price) AS sales#3]
                  +- 'Filter (((('d_year = 2000) && ('d_moy = 2)) && ('ws_sold_date_sk = 'd_date_sk)) && ('ws_item_sk IN (list#4 []) && 'ws_bill_customer_sk IN (list#5 [])))
                     :  :- 'Project ['item_sk]
                     :  :  +- 'UnresolvedRelation `frequent_ss_items`
                     :  +- 'Project ['c_customer_sk]
                     :     +- 'UnresolvedRelation `best_ss_customer`
                     +- 'Join Inner
                        :- 'UnresolvedRelation `web_sales`
                        +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
sum(sales): double
GlobalLimit 100
+- LocalLimit 100
   +- Aggregate [sum(sales#0) AS sum(sales)#196]
      +- SubqueryAlias `__auto_generated_subquery_name`
         +- Union
            :- Project [(cast(cs_quantity#137 as double) * cs_list_price#139) AS sales#0]
            :  +- Filter ((((d_year#47 = 2000) && (d_moy#49 = 2)) && (cs_sold_date_sk#119 = d_date_sk#41)) && (cs_item_sk#134 IN (list#1 []) && cs_bill_customer_sk#122 IN (list#2 [])))
            :     :  :- Project [item_sk#7]
            :     :  :  +- SubqueryAlias `frequent_ss_items`
            :     :  :     +- Project [itemdesc#6, item_sk#7, solddate#8, cnt#9L]
            :     :  :        +- Filter (count(1)#92L > cast(4 as bigint))
            :     :  :           +- Aggregate [substring(i_item_desc#73, 1, 30), i_item_sk#69, d_date#43], [substring(i_item_desc#73, 1, 30) AS itemdesc#6, i_item_sk#69 AS item_sk#7, d_date#43 AS solddate#8, count(1) AS cnt#9L, count(1) AS count(1)#92L]
            :     :  :              +- Filter (((ss_sold_date_sk#18 = d_date_sk#41) && (ss_item_sk#20 = i_item_sk#69)) && d_year#47 IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
            :     :  :                 +- Join Inner
            :     :  :                    :- Join Inner
            :     :  :                    :  :- SubqueryAlias `tpcds`.`store_sales`
            :     :  :                    :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
            :     :  :                    :  +- SubqueryAlias `tpcds`.`date_dim`
            :     :  :                    :     +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
            :     :  :                    +- SubqueryAlias `tpcds`.`item`
            :     :  :                       +- Relation[i_item_sk#69,i_item_id#70,i_rec_start_date#71,i_rec_end_date#72,i_item_desc#73,i_current_price#74,i_wholesale_cost#75,i_brand_id#76,i_brand#77,i_class_id#78,i_class#79,i_category_id#80,i_category#81,i_manufact_id#82,i_manufact#83,i_size#84,i_formulation#85,i_color#86,i_units#87,i_container#88,i_manager_id#89,i_product_name#90] parquet
            :     :  +- Project [c_customer_sk#94]
            :     :     +- SubqueryAlias `best_ss_customer`
            :     :        +- Project [c_customer_sk#94, ssales#12]
            :     :           +- Filter (sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117 > (cast(CheckOverflow((promote_precision(cast(cast(50 as decimal(2,0)) as decimal(4,1))) / promote_precision(cast(100.0 as decimal(4,1)))), DecimalType(9,6)) as double) * scalar-subquery#13 []))
            :     :              :  +- Project [tpcds_cmax#11]
            :     :              :     +- SubqueryAlias `max_store_sales`
            :     :              :        +- Aggregate [max(csales#10) AS tpcds_cmax#11]
            :     :              :           +- SubqueryAlias `__auto_generated_subquery_name`
            :     :              :              +- Aggregate [c_customer_sk#94], [c_customer_sk#94, sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS csales#10]
            :     :              :                 +- Filter (((ss_customer_sk#21 = c_customer_sk#94) && (ss_sold_date_sk#18 = d_date_sk#41)) && d_year#47 IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
            :     :              :                    +- Join Inner
            :     :              :                       :- Join Inner
            :     :              :                       :  :- SubqueryAlias `tpcds`.`store_sales`
            :     :              :                       :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
            :     :              :                       :  +- SubqueryAlias `tpcds`.`customer`
            :     :              :                       :     +- Relation[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
            :     :              :                       +- SubqueryAlias `tpcds`.`date_dim`
            :     :              :                          +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
            :     :              +- Aggregate [c_customer_sk#94], [c_customer_sk#94, sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS ssales#12, sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117]
            :     :                 +- Filter (ss_customer_sk#21 = c_customer_sk#94)
            :     :                    +- Join Inner
            :     :                       :- SubqueryAlias `tpcds`.`store_sales`
            :     :                       :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
            :     :                       +- SubqueryAlias `tpcds`.`customer`
            :     :                          +- Relation[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
            :     +- Join Inner
            :        :- SubqueryAlias `tpcds`.`catalog_sales`
            :        :  +- Relation[cs_sold_date_sk#119,cs_sold_time_sk#120,cs_ship_date_sk#121,cs_bill_customer_sk#122,cs_bill_cdemo_sk#123,cs_bill_hdemo_sk#124,cs_bill_addr_sk#125,cs_ship_customer_sk#126,cs_ship_cdemo_sk#127,cs_ship_hdemo_sk#128,cs_ship_addr_sk#129,cs_call_center_sk#130,cs_catalog_page_sk#131,cs_ship_mode_sk#132,cs_warehouse_sk#133,cs_item_sk#134,cs_promo_sk#135,cs_order_number#136,cs_quantity#137,cs_wholesale_cost#138,cs_list_price#139,cs_sales_price#140,cs_ext_discount_amt#141,cs_ext_sales_price#142,... 10 more fields] parquet
            :        +- SubqueryAlias `tpcds`.`date_dim`
            :           +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
            +- Project [(cast(ws_quantity#171 as double) * ws_list_price#173) AS sales#3]
               +- Filter ((((d_year#47 = 2000) && (d_moy#49 = 2)) && (ws_sold_date_sk#153 = d_date_sk#41)) && (ws_item_sk#156 IN (list#4 []) && ws_bill_customer_sk#157 IN (list#5 [])))
                  :  :- Project [item_sk#7]
                  :  :  +- SubqueryAlias `frequent_ss_items`
                  :  :     +- Project [itemdesc#6, item_sk#7, solddate#8, cnt#9L]
                  :  :        +- Filter (count(1)#92L > cast(4 as bigint))
                  :  :           +- Aggregate [substring(i_item_desc#73, 1, 30), i_item_sk#69, d_date#43], [substring(i_item_desc#73, 1, 30) AS itemdesc#6, i_item_sk#69 AS item_sk#7, d_date#43 AS solddate#8, count(1) AS cnt#9L, count(1) AS count(1)#92L]
                  :  :              +- Filter (((ss_sold_date_sk#18 = d_date_sk#41) && (ss_item_sk#20 = i_item_sk#69)) && d_year#47 IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
                  :  :                 +- Join Inner
                  :  :                    :- Join Inner
                  :  :                    :  :- SubqueryAlias `tpcds`.`store_sales`
                  :  :                    :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
                  :  :                    :  +- SubqueryAlias `tpcds`.`date_dim`
                  :  :                    :     +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
                  :  :                    +- SubqueryAlias `tpcds`.`item`
                  :  :                       +- Relation[i_item_sk#69,i_item_id#70,i_rec_start_date#71,i_rec_end_date#72,i_item_desc#73,i_current_price#74,i_wholesale_cost#75,i_brand_id#76,i_brand#77,i_class_id#78,i_class#79,i_category_id#80,i_category#81,i_manufact_id#82,i_manufact#83,i_size#84,i_formulation#85,i_color#86,i_units#87,i_container#88,i_manager_id#89,i_product_name#90] parquet
                  :  +- Project [c_customer_sk#94]
                  :     +- SubqueryAlias `best_ss_customer`
                  :        +- Project [c_customer_sk#94, ssales#12]
                  :           +- Filter (sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117 > (cast(CheckOverflow((promote_precision(cast(cast(50 as decimal(2,0)) as decimal(4,1))) / promote_precision(cast(100.0 as decimal(4,1)))), DecimalType(9,6)) as double) * scalar-subquery#13 []))
                  :              :  +- Project [tpcds_cmax#11]
                  :              :     +- SubqueryAlias `max_store_sales`
                  :              :        +- Aggregate [max(csales#10) AS tpcds_cmax#11]
                  :              :           +- SubqueryAlias `__auto_generated_subquery_name`
                  :              :              +- Aggregate [c_customer_sk#94], [c_customer_sk#94, sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS csales#10]
                  :              :                 +- Filter (((ss_customer_sk#21 = c_customer_sk#94) && (ss_sold_date_sk#18 = d_date_sk#41)) && d_year#47 IN (2000,(2000 + 1),(2000 + 2),(2000 + 3)))
                  :              :                    +- Join Inner
                  :              :                       :- Join Inner
                  :              :                       :  :- SubqueryAlias `tpcds`.`store_sales`
                  :              :                       :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
                  :              :                       :  +- SubqueryAlias `tpcds`.`customer`
                  :              :                       :     +- Relation[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
                  :              :                       +- SubqueryAlias `tpcds`.`date_dim`
                  :              :                          +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
                  :              +- Aggregate [c_customer_sk#94], [c_customer_sk#94, sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS ssales#12, sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117]
                  :                 +- Filter (ss_customer_sk#21 = c_customer_sk#94)
                  :                    +- Join Inner
                  :                       :- SubqueryAlias `tpcds`.`store_sales`
                  :                       :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
                  :                       +- SubqueryAlias `tpcds`.`customer`
                  :                          +- Relation[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
                  +- Join Inner
                     :- SubqueryAlias `tpcds`.`web_sales`
                     :  +- Relation[ws_sold_date_sk#153,ws_sold_time_sk#154,ws_ship_date_sk#155,ws_item_sk#156,ws_bill_customer_sk#157,ws_bill_cdemo_sk#158,ws_bill_hdemo_sk#159,ws_bill_addr_sk#160,ws_ship_customer_sk#161,ws_ship_cdemo_sk#162,ws_ship_hdemo_sk#163,ws_ship_addr_sk#164,ws_web_page_sk#165,ws_web_site_sk#166,ws_ship_mode_sk#167,ws_warehouse_sk#168,ws_promo_sk#169,ws_order_number#170,ws_quantity#171,ws_wholesale_cost#172,ws_list_price#173,ws_sales_price#174,ws_ext_discount_amt#175,ws_ext_sales_price#176,... 10 more fields] parquet
                     +- SubqueryAlias `tpcds`.`date_dim`
                        +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Aggregate [sum(sales#0) AS sum(sales)#196]
      +- Union
         :- Project [(cast(cs_quantity#137 as double) * cs_list_price#139) AS sales#0]
         :  +- Join Inner, (cs_sold_date_sk#119 = d_date_sk#41)
         :     :- Project [cs_sold_date_sk#119, cs_quantity#137, cs_list_price#139]
         :     :  +- Join LeftSemi, (cs_bill_customer_sk#122 = c_customer_sk#94)
         :     :     :- Project [cs_sold_date_sk#119, cs_bill_customer_sk#122, cs_quantity#137, cs_list_price#139]
         :     :     :  +- Join LeftSemi, (cs_item_sk#134 = item_sk#7)
         :     :     :     :- Project [cs_sold_date_sk#119, cs_bill_customer_sk#122, cs_item_sk#134, cs_quantity#137, cs_list_price#139]
         :     :     :     :  +- Filter isnotnull(cs_sold_date_sk#119)
         :     :     :     :     +- Relation[cs_sold_date_sk#119,cs_sold_time_sk#120,cs_ship_date_sk#121,cs_bill_customer_sk#122,cs_bill_cdemo_sk#123,cs_bill_hdemo_sk#124,cs_bill_addr_sk#125,cs_ship_customer_sk#126,cs_ship_cdemo_sk#127,cs_ship_hdemo_sk#128,cs_ship_addr_sk#129,cs_call_center_sk#130,cs_catalog_page_sk#131,cs_ship_mode_sk#132,cs_warehouse_sk#133,cs_item_sk#134,cs_promo_sk#135,cs_order_number#136,cs_quantity#137,cs_wholesale_cost#138,cs_list_price#139,cs_sales_price#140,cs_ext_discount_amt#141,cs_ext_sales_price#142,... 10 more fields] parquet
         :     :     :     +- Project [item_sk#7]
         :     :     :        +- Filter (count(1)#92L > 4)
         :     :     :           +- Aggregate [substring(i_item_desc#73, 1, 30), i_item_sk#69, d_date#43], [i_item_sk#69 AS item_sk#7, count(1) AS count(1)#92L]
         :     :     :              +- Project [d_date#43, i_item_sk#69, i_item_desc#73]
         :     :     :                 +- Join Inner, (ss_item_sk#20 = i_item_sk#69)
         :     :     :                    :- Project [ss_item_sk#20, d_date#43]
         :     :     :                    :  +- Join Inner, (ss_sold_date_sk#18 = d_date_sk#41)
         :     :     :                    :     :- Project [ss_sold_date_sk#18, ss_item_sk#20]
         :     :     :                    :     :  +- Filter (isnotnull(ss_sold_date_sk#18) && isnotnull(ss_item_sk#20))
         :     :     :                    :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
         :     :     :                    :     +- Project [d_date_sk#41, d_date#43]
         :     :     :                    :        +- Filter (d_year#47 IN (2000,2001,2002,2003) && isnotnull(d_date_sk#41))
         :     :     :                    :           +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
         :     :     :                    +- Project [i_item_sk#69, i_item_desc#73]
         :     :     :                       +- Filter isnotnull(i_item_sk#69)
         :     :     :                          +- Relation[i_item_sk#69,i_item_id#70,i_rec_start_date#71,i_rec_end_date#72,i_item_desc#73,i_current_price#74,i_wholesale_cost#75,i_brand_id#76,i_brand#77,i_class_id#78,i_class#79,i_category_id#80,i_category#81,i_manufact_id#82,i_manufact#83,i_size#84,i_formulation#85,i_color#86,i_units#87,i_container#88,i_manager_id#89,i_product_name#90] parquet
         :     :     +- Project [c_customer_sk#94]
         :     :        +- Filter (isnotnull(sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117) && (sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117 > (0.5 * scalar-subquery#13 [])))
         :     :           :  +- Aggregate [max(csales#10) AS tpcds_cmax#11]
         :     :           :     +- Aggregate [c_customer_sk#94], [sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS csales#10]
         :     :           :        +- Project [ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
         :     :           :           +- Join Inner, (ss_sold_date_sk#18 = d_date_sk#41)
         :     :           :              :- Project [ss_sold_date_sk#18, ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
         :     :           :              :  +- Join Inner, (ss_customer_sk#21 = c_customer_sk#94)
         :     :           :              :     :- Project [ss_sold_date_sk#18, ss_customer_sk#21, ss_quantity#28, ss_sales_price#31]
         :     :           :              :     :  +- Filter (isnotnull(ss_customer_sk#21) && isnotnull(ss_sold_date_sk#18))
         :     :           :              :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
         :     :           :              :     +- Project [c_customer_sk#94]
         :     :           :              :        +- Filter isnotnull(c_customer_sk#94)
         :     :           :              :           +- Relation[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
         :     :           :              +- Project [d_date_sk#41]
         :     :           :                 +- Filter (d_year#47 IN (2000,2001,2002,2003) && isnotnull(d_date_sk#41))
         :     :           :                    +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
         :     :           +- Aggregate [c_customer_sk#94], [c_customer_sk#94, sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117]
         :     :              +- Project [ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
         :     :                 +- Join Inner, (ss_customer_sk#21 = c_customer_sk#94)
         :     :                    :- Project [ss_customer_sk#21, ss_quantity#28, ss_sales_price#31]
         :     :                    :  +- Filter isnotnull(ss_customer_sk#21)
         :     :                    :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
         :     :                    +- Project [c_customer_sk#94]
         :     :                       +- Filter isnotnull(c_customer_sk#94)
         :     :                          +- Relation[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
         :     +- Project [d_date_sk#41]
         :        +- Filter ((((isnotnull(d_year#47) && isnotnull(d_moy#49)) && (d_year#47 = 2000)) && (d_moy#49 = 2)) && isnotnull(d_date_sk#41))
         :           +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
         +- Project [(cast(ws_quantity#171 as double) * ws_list_price#173) AS sales#3]
            +- Join Inner, (ws_sold_date_sk#153 = d_date_sk#41)
               :- Project [ws_sold_date_sk#153, ws_quantity#171, ws_list_price#173]
               :  +- Join LeftSemi, (ws_bill_customer_sk#157 = c_customer_sk#94)
               :     :- Project [ws_sold_date_sk#153, ws_bill_customer_sk#157, ws_quantity#171, ws_list_price#173]
               :     :  +- Join LeftSemi, (ws_item_sk#156 = item_sk#7)
               :     :     :- Project [ws_sold_date_sk#153, ws_item_sk#156, ws_bill_customer_sk#157, ws_quantity#171, ws_list_price#173]
               :     :     :  +- Filter isnotnull(ws_sold_date_sk#153)
               :     :     :     +- Relation[ws_sold_date_sk#153,ws_sold_time_sk#154,ws_ship_date_sk#155,ws_item_sk#156,ws_bill_customer_sk#157,ws_bill_cdemo_sk#158,ws_bill_hdemo_sk#159,ws_bill_addr_sk#160,ws_ship_customer_sk#161,ws_ship_cdemo_sk#162,ws_ship_hdemo_sk#163,ws_ship_addr_sk#164,ws_web_page_sk#165,ws_web_site_sk#166,ws_ship_mode_sk#167,ws_warehouse_sk#168,ws_promo_sk#169,ws_order_number#170,ws_quantity#171,ws_wholesale_cost#172,ws_list_price#173,ws_sales_price#174,ws_ext_discount_amt#175,ws_ext_sales_price#176,... 10 more fields] parquet
               :     :     +- Project [item_sk#7]
               :     :        +- Filter (count(1)#92L > 4)
               :     :           +- Aggregate [substring(i_item_desc#73, 1, 30), i_item_sk#69, d_date#43], [i_item_sk#69 AS item_sk#7, count(1) AS count(1)#92L]
               :     :              +- Project [d_date#43, i_item_sk#69, i_item_desc#73]
               :     :                 +- Join Inner, (ss_item_sk#20 = i_item_sk#69)
               :     :                    :- Project [ss_item_sk#20, d_date#43]
               :     :                    :  +- Join Inner, (ss_sold_date_sk#18 = d_date_sk#41)
               :     :                    :     :- Project [ss_sold_date_sk#18, ss_item_sk#20]
               :     :                    :     :  +- Filter (isnotnull(ss_sold_date_sk#18) && isnotnull(ss_item_sk#20))
               :     :                    :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
               :     :                    :     +- Project [d_date_sk#41, d_date#43]
               :     :                    :        +- Filter (d_year#47 IN (2000,2001,2002,2003) && isnotnull(d_date_sk#41))
               :     :                    :           +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
               :     :                    +- Project [i_item_sk#69, i_item_desc#73]
               :     :                       +- Filter isnotnull(i_item_sk#69)
               :     :                          +- Relation[i_item_sk#69,i_item_id#70,i_rec_start_date#71,i_rec_end_date#72,i_item_desc#73,i_current_price#74,i_wholesale_cost#75,i_brand_id#76,i_brand#77,i_class_id#78,i_class#79,i_category_id#80,i_category#81,i_manufact_id#82,i_manufact#83,i_size#84,i_formulation#85,i_color#86,i_units#87,i_container#88,i_manager_id#89,i_product_name#90] parquet
               :     +- Project [c_customer_sk#94]
               :        +- Filter (isnotnull(sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117) && (sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117 > (0.5 * scalar-subquery#13 [])))
               :           :  +- Aggregate [max(csales#10) AS tpcds_cmax#11]
               :           :     +- Aggregate [c_customer_sk#94], [sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS csales#10]
               :           :        +- Project [ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
               :           :           +- Join Inner, (ss_sold_date_sk#18 = d_date_sk#41)
               :           :              :- Project [ss_sold_date_sk#18, ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
               :           :              :  +- Join Inner, (ss_customer_sk#21 = c_customer_sk#94)
               :           :              :     :- Project [ss_sold_date_sk#18, ss_customer_sk#21, ss_quantity#28, ss_sales_price#31]
               :           :              :     :  +- Filter (isnotnull(ss_customer_sk#21) && isnotnull(ss_sold_date_sk#18))
               :           :              :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
               :           :              :     +- Project [c_customer_sk#94]
               :           :              :        +- Filter isnotnull(c_customer_sk#94)
               :           :              :           +- Relation[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
               :           :              +- Project [d_date_sk#41]
               :           :                 +- Filter (d_year#47 IN (2000,2001,2002,2003) && isnotnull(d_date_sk#41))
               :           :                    +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet
               :           +- Aggregate [c_customer_sk#94], [c_customer_sk#94, sum((cast(ss_quantity#28 as double) * ss_sales_price#31)) AS sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117]
               :              +- Project [ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
               :                 +- Join Inner, (ss_customer_sk#21 = c_customer_sk#94)
               :                    :- Project [ss_customer_sk#21, ss_quantity#28, ss_sales_price#31]
               :                    :  +- Filter isnotnull(ss_customer_sk#21)
               :                    :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
               :                    +- Project [c_customer_sk#94]
               :                       +- Filter isnotnull(c_customer_sk#94)
               :                          +- Relation[c_customer_sk#94,c_customer_id#95,c_current_cdemo_sk#96,c_current_hdemo_sk#97,c_current_addr_sk#98,c_first_shipto_date_sk#99,c_first_sales_date_sk#100,c_salutation#101,c_first_name#102,c_last_name#103,c_preferred_cust_flag#104,c_birth_day#105,c_birth_month#106,c_birth_year#107,c_birth_country#108,c_login#109,c_email_address#110,c_last_review_date#111] parquet
               +- Project [d_date_sk#41]
                  +- Filter ((((isnotnull(d_year#47) && isnotnull(d_moy#49)) && (d_year#47 = 2000)) && (d_moy#49 = 2)) && isnotnull(d_date_sk#41))
                     +- Relation[d_date_sk#41,d_date_id#42,d_date#43,d_month_seq#44,d_week_seq#45,d_quarter_seq#46,d_year#47,d_dow#48,d_moy#49,d_dom#50,d_qoy#51,d_fy_year#52,d_fy_quarter_seq#53,d_fy_week_seq#54,d_day_name#55,d_quarter_name#56,d_holiday#57,d_weekend#58,d_following_holiday#59,d_first_dom#60,d_last_dom#61,d_same_day_ly#62,d_same_day_lq#63,d_current_day#64,... 4 more fields] parquet

== Physical Plan ==
CollectLimit 100
+- *(30) HashAggregate(keys=[], functions=[sum(sales#0)], output=[sum(sales)#196])
   +- Exchange SinglePartition
      +- *(29) HashAggregate(keys=[], functions=[partial_sum(sales#0)], output=[sum#198])
         +- Union
            :- *(14) Project [(cast(cs_quantity#137 as double) * cs_list_price#139) AS sales#0]
            :  +- *(14) BroadcastHashJoin [cs_sold_date_sk#119], [d_date_sk#41], Inner, BuildRight
            :     :- *(14) Project [cs_sold_date_sk#119, cs_quantity#137, cs_list_price#139]
            :     :  +- SortMergeJoin [cs_bill_customer_sk#122], [c_customer_sk#94], LeftSemi
            :     :     :- *(9) Sort [cs_bill_customer_sk#122 ASC NULLS FIRST], false, 0
            :     :     :  +- Exchange hashpartitioning(cs_bill_customer_sk#122, 200)
            :     :     :     +- *(8) Project [cs_sold_date_sk#119, cs_bill_customer_sk#122, cs_quantity#137, cs_list_price#139]
            :     :     :        +- SortMergeJoin [cs_item_sk#134], [item_sk#7], LeftSemi
            :     :     :           :- *(2) Sort [cs_item_sk#134 ASC NULLS FIRST], false, 0
            :     :     :           :  +- Exchange hashpartitioning(cs_item_sk#134, 200)
            :     :     :           :     +- *(1) Project [cs_sold_date_sk#119, cs_bill_customer_sk#122, cs_item_sk#134, cs_quantity#137, cs_list_price#139]
            :     :     :           :        +- *(1) Filter isnotnull(cs_sold_date_sk#119)
            :     :     :           :           +- *(1) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#119,cs_bill_customer_sk#122,cs_item_sk#134,cs_quantity#137,cs_list_price#139] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int,cs_quantity:int,cs_list_price:d...
            :     :     :           +- *(7) Sort [item_sk#7 ASC NULLS FIRST], false, 0
            :     :     :              +- Exchange hashpartitioning(item_sk#7, 200)
            :     :     :                 +- *(6) Project [item_sk#7]
            :     :     :                    +- *(6) Filter (count(1)#92L > 4)
            :     :     :                       +- *(6) HashAggregate(keys=[substring(i_item_desc#73, 1, 30)#199, i_item_sk#69, d_date#43], functions=[count(1)], output=[item_sk#7, count(1)#92L])
            :     :     :                          +- Exchange hashpartitioning(substring(i_item_desc#73, 1, 30)#199, i_item_sk#69, d_date#43, 200)
            :     :     :                             +- *(5) HashAggregate(keys=[substring(i_item_desc#73, 1, 30) AS substring(i_item_desc#73, 1, 30)#199, i_item_sk#69, d_date#43], functions=[partial_count(1)], output=[substring(i_item_desc#73, 1, 30)#199, i_item_sk#69, d_date#43, count#201L])
            :     :     :                                +- *(5) Project [d_date#43, i_item_sk#69, i_item_desc#73]
            :     :     :                                   +- *(5) BroadcastHashJoin [ss_item_sk#20], [i_item_sk#69], Inner, BuildRight
            :     :     :                                      :- *(5) Project [ss_item_sk#20, d_date#43]
            :     :     :                                      :  +- *(5) BroadcastHashJoin [ss_sold_date_sk#18], [d_date_sk#41], Inner, BuildRight
            :     :     :                                      :     :- *(5) Project [ss_sold_date_sk#18, ss_item_sk#20]
            :     :     :                                      :     :  +- *(5) Filter (isnotnull(ss_sold_date_sk#18) && isnotnull(ss_item_sk#20))
            :     :     :                                      :     :     +- *(5) FileScan parquet tpcds.store_sales[ss_sold_date_sk#18,ss_item_sk#20] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
            :     :     :                                      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :     :                                      :        +- *(3) Project [d_date_sk#41, d_date#43]
            :     :     :                                      :           +- *(3) Filter (d_year#47 IN (2000,2001,2002,2003) && isnotnull(d_date_sk#41))
            :     :     :                                      :              +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#41,d_date#43,d_year#47] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [In(d_year, [2000,2001,2002,2003]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string,d_year:int>
            :     :     :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :     :                                         +- *(4) Project [i_item_sk#69, i_item_desc#73]
            :     :     :                                            +- *(4) Filter isnotnull(i_item_sk#69)
            :     :     :                                               +- *(4) FileScan parquet tpcds.item[i_item_sk#69,i_item_desc#73] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_desc:string>
            :     :     +- *(12) Sort [c_customer_sk#94 ASC NULLS FIRST], false, 0
            :     :        +- *(12) Project [c_customer_sk#94]
            :     :           +- *(12) Filter (isnotnull(sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117) && (sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117 > (0.5 * Subquery subquery13)))
            :     :              :  +- Subquery subquery13
            :     :              :     +- *(5) HashAggregate(keys=[], functions=[max(csales#10)], output=[tpcds_cmax#11])
            :     :              :        +- Exchange SinglePartition
            :     :              :           +- *(4) HashAggregate(keys=[], functions=[partial_max(csales#10)], output=[max#206])
            :     :              :              +- *(4) HashAggregate(keys=[c_customer_sk#94], functions=[sum((cast(ss_quantity#28 as double) * ss_sales_price#31))], output=[csales#10])
            :     :              :                 +- Exchange hashpartitioning(c_customer_sk#94, 200)
            :     :              :                    +- *(3) HashAggregate(keys=[c_customer_sk#94], functions=[partial_sum((cast(ss_quantity#28 as double) * ss_sales_price#31))], output=[c_customer_sk#94, sum#208])
            :     :              :                       +- *(3) Project [ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
            :     :              :                          +- *(3) BroadcastHashJoin [ss_sold_date_sk#18], [d_date_sk#41], Inner, BuildRight
            :     :              :                             :- *(3) Project [ss_sold_date_sk#18, ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
            :     :              :                             :  +- *(3) BroadcastHashJoin [ss_customer_sk#21], [c_customer_sk#94], Inner, BuildRight
            :     :              :                             :     :- *(3) Project [ss_sold_date_sk#18, ss_customer_sk#21, ss_quantity#28, ss_sales_price#31]
            :     :              :                             :     :  +- *(3) Filter (isnotnull(ss_customer_sk#21) && isnotnull(ss_sold_date_sk#18))
            :     :              :                             :     :     +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#18,ss_customer_sk#21,ss_quantity#28,ss_sales_price#31] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_quantity:int,ss_sales_price:double>
            :     :              :                             :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :              :                             :        +- *(1) Project [c_customer_sk#94]
            :     :              :                             :           +- *(1) Filter isnotnull(c_customer_sk#94)
            :     :              :                             :              +- *(1) FileScan parquet tpcds.customer[c_customer_sk#94] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int>
            :     :              :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :              :                                +- *(2) Project [d_date_sk#41]
            :     :              :                                   +- *(2) Filter (d_year#47 IN (2000,2001,2002,2003) && isnotnull(d_date_sk#41))
            :     :              :                                      +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#41,d_year#47] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [In(d_year, [2000,2001,2002,2003]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
            :     :              +- *(12) HashAggregate(keys=[c_customer_sk#94], functions=[sum((cast(ss_quantity#28 as double) * ss_sales_price#31))], output=[c_customer_sk#94, sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117])
            :     :                 +- Exchange hashpartitioning(c_customer_sk#94, 200)
            :     :                    +- *(11) HashAggregate(keys=[c_customer_sk#94], functions=[partial_sum((cast(ss_quantity#28 as double) * ss_sales_price#31))], output=[c_customer_sk#94, sum#203])
            :     :                       +- *(11) Project [ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
            :     :                          +- *(11) BroadcastHashJoin [ss_customer_sk#21], [c_customer_sk#94], Inner, BuildRight
            :     :                             :- *(11) Project [ss_customer_sk#21, ss_quantity#28, ss_sales_price#31]
            :     :                             :  +- *(11) Filter isnotnull(ss_customer_sk#21)
            :     :                             :     +- *(11) FileScan parquet tpcds.store_sales[ss_customer_sk#21,ss_quantity#28,ss_sales_price#31] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk)], ReadSchema: struct<ss_customer_sk:int,ss_quantity:int,ss_sales_price:double>
            :     :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :                                +- *(10) Project [c_customer_sk#94]
            :     :                                   +- *(10) Filter isnotnull(c_customer_sk#94)
            :     :                                      +- *(10) FileScan parquet tpcds.customer[c_customer_sk#94] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int>
            :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :        +- *(13) Project [d_date_sk#41]
            :           +- *(13) Filter ((((isnotnull(d_year#47) && isnotnull(d_moy#49)) && (d_year#47 = 2000)) && (d_moy#49 = 2)) && isnotnull(d_date_sk#41))
            :              +- *(13) FileScan parquet tpcds.date_dim[d_date_sk#41,d_year#47,d_moy#49] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2000), EqualTo(d_moy,2), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
            +- *(28) Project [(cast(ws_quantity#171 as double) * ws_list_price#173) AS sales#3]
               +- *(28) BroadcastHashJoin [ws_sold_date_sk#153], [d_date_sk#41], Inner, BuildRight
                  :- *(28) Project [ws_sold_date_sk#153, ws_quantity#171, ws_list_price#173]
                  :  +- SortMergeJoin [ws_bill_customer_sk#157], [c_customer_sk#94], LeftSemi
                  :     :- *(23) Sort [ws_bill_customer_sk#157 ASC NULLS FIRST], false, 0
                  :     :  +- Exchange hashpartitioning(ws_bill_customer_sk#157, 200)
                  :     :     +- *(22) Project [ws_sold_date_sk#153, ws_bill_customer_sk#157, ws_quantity#171, ws_list_price#173]
                  :     :        +- SortMergeJoin [ws_item_sk#156], [item_sk#7], LeftSemi
                  :     :           :- *(16) Sort [ws_item_sk#156 ASC NULLS FIRST], false, 0
                  :     :           :  +- Exchange hashpartitioning(ws_item_sk#156, 200)
                  :     :           :     +- *(15) Project [ws_sold_date_sk#153, ws_item_sk#156, ws_bill_customer_sk#157, ws_quantity#171, ws_list_price#173]
                  :     :           :        +- *(15) Filter isnotnull(ws_sold_date_sk#153)
                  :     :           :           +- *(15) FileScan parquet tpcds.web_sales[ws_sold_date_sk#153,ws_item_sk#156,ws_bill_customer_sk#157,ws_quantity#171,ws_list_price#173] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_customer_sk:int,ws_quantity:int,ws_list_price:d...
                  :     :           +- *(21) Sort [item_sk#7 ASC NULLS FIRST], false, 0
                  :     :              +- ReusedExchange [item_sk#7], Exchange hashpartitioning(item_sk#7, 200)
                  :     +- *(26) Sort [c_customer_sk#94 ASC NULLS FIRST], false, 0
                  :        +- *(26) Project [c_customer_sk#94]
                  :           +- *(26) Filter (isnotnull(sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117) && (sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117 > (0.5 * Subquery subquery13)))
                  :              :  +- Subquery subquery13
                  :              :     +- *(5) HashAggregate(keys=[], functions=[max(csales#10)], output=[tpcds_cmax#11])
                  :              :        +- Exchange SinglePartition
                  :              :           +- *(4) HashAggregate(keys=[], functions=[partial_max(csales#10)], output=[max#206])
                  :              :              +- *(4) HashAggregate(keys=[c_customer_sk#94], functions=[sum((cast(ss_quantity#28 as double) * ss_sales_price#31))], output=[csales#10])
                  :              :                 +- Exchange hashpartitioning(c_customer_sk#94, 200)
                  :              :                    +- *(3) HashAggregate(keys=[c_customer_sk#94], functions=[partial_sum((cast(ss_quantity#28 as double) * ss_sales_price#31))], output=[c_customer_sk#94, sum#208])
                  :              :                       +- *(3) Project [ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
                  :              :                          +- *(3) BroadcastHashJoin [ss_sold_date_sk#18], [d_date_sk#41], Inner, BuildRight
                  :              :                             :- *(3) Project [ss_sold_date_sk#18, ss_quantity#28, ss_sales_price#31, c_customer_sk#94]
                  :              :                             :  +- *(3) BroadcastHashJoin [ss_customer_sk#21], [c_customer_sk#94], Inner, BuildRight
                  :              :                             :     :- *(3) Project [ss_sold_date_sk#18, ss_customer_sk#21, ss_quantity#28, ss_sales_price#31]
                  :              :                             :     :  +- *(3) Filter (isnotnull(ss_customer_sk#21) && isnotnull(ss_sold_date_sk#18))
                  :              :                             :     :     +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#18,ss_customer_sk#21,ss_quantity#28,ss_sales_price#31] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_quantity:int,ss_sales_price:double>
                  :              :                             :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :              :                             :        +- *(1) Project [c_customer_sk#94]
                  :              :                             :           +- *(1) Filter isnotnull(c_customer_sk#94)
                  :              :                             :              +- *(1) FileScan parquet tpcds.customer[c_customer_sk#94] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int>
                  :              :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :              :                                +- *(2) Project [d_date_sk#41]
                  :              :                                   +- *(2) Filter (d_year#47 IN (2000,2001,2002,2003) && isnotnull(d_date_sk#41))
                  :              :                                      +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#41,d_year#47] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [In(d_year, [2000,2001,2002,2003]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
                  :              +- *(26) HashAggregate(keys=[c_customer_sk#94], functions=[sum((cast(ss_quantity#28 as double) * ss_sales_price#31))], output=[c_customer_sk#94, sum((cast(ss_quantity#28 as double) * ss_sales_price#31))#117])
                  :                 +- ReusedExchange [c_customer_sk#94, sum#203], Exchange hashpartitioning(c_customer_sk#94, 200)
                  +- ReusedExchange [d_date_sk#41], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.603 seconds, Fetched 1 row(s)
NULL	Robert	598.86
Brown	Monika	6031.52
Collins	Gordon	727.5699999999999
Green	Jesse	9672.960000000001
Time taken: 25.151 seconds, Fetched 4 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 23 in stream 0 using template query23.tpl
------------------------------------------------------^^^

