== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['promotions ASC NULLS FIRST, 'total ASC NULLS FIRST], true
      +- 'Project ['promotions, 'total, unresolvedalias(((cast('promotions as decimal(15,4)) / cast('total as decimal(15,4))) * 100), None)]
         +- 'Join Inner
            :- 'SubqueryAlias `promotional_sales`
            :  +- 'Project ['sum('ss_ext_sales_price) AS promotions#0]
            :     +- 'Filter ((((('ss_sold_date_sk = 'd_date_sk) && ('ss_store_sk = 's_store_sk)) && ('ss_promo_sk = 'p_promo_sk)) && ((('ss_customer_sk = 'c_customer_sk) && ('ca_address_sk = 'c_current_addr_sk)) && ('ss_item_sk = 'i_item_sk))) && (((('ca_gmt_offset = -5) && ('i_category = Jewelry)) && ((('p_channel_dmail = Y) || ('p_channel_email = Y)) || ('p_channel_tv = Y))) && ((('s_gmt_offset = -5) && ('d_year = 1998)) && ('d_moy = 11))))
            :        +- 'Join Inner
            :           :- 'Join Inner
            :           :  :- 'Join Inner
            :           :  :  :- 'Join Inner
            :           :  :  :  :- 'Join Inner
            :           :  :  :  :  :- 'Join Inner
            :           :  :  :  :  :  :- 'UnresolvedRelation `store_sales`
            :           :  :  :  :  :  +- 'UnresolvedRelation `store`
            :           :  :  :  :  +- 'UnresolvedRelation `promotion`
            :           :  :  :  +- 'UnresolvedRelation `date_dim`
            :           :  :  +- 'UnresolvedRelation `customer`
            :           :  +- 'UnresolvedRelation `customer_address`
            :           +- 'UnresolvedRelation `item`
            +- 'SubqueryAlias `all_sales`
               +- 'Project ['sum('ss_ext_sales_price) AS total#1]
                  +- 'Filter ((((('ss_sold_date_sk = 'd_date_sk) && ('ss_store_sk = 's_store_sk)) && ('ss_customer_sk = 'c_customer_sk)) && (('ca_address_sk = 'c_current_addr_sk) && ('ss_item_sk = 'i_item_sk))) && (((('ca_gmt_offset = -5) && ('i_category = Jewelry)) && ('s_gmt_offset = -5)) && (('d_year = 1998) && ('d_moy = 11))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'Join Inner
                        :  :  :- 'Join Inner
                        :  :  :  :- 'Join Inner
                        :  :  :  :  :- 'UnresolvedRelation `store_sales`
                        :  :  :  :  +- 'UnresolvedRelation `store`
                        :  :  :  +- 'UnresolvedRelation `date_dim`
                        :  :  +- 'UnresolvedRelation `customer`
                        :  +- 'UnresolvedRelation `customer_address`
                        +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
promotions: double, total: double, (CAST((CAST(CAST(promotions AS DECIMAL(15,4)) AS DECIMAL(15,4)) / CAST(CAST(total AS DECIMAL(15,4)) AS DECIMAL(15,4))) AS DECIMAL(35,20)) * CAST(CAST(100 AS DECIMAL(3,0)) AS DECIMAL(35,20))): decimal(38,19)
GlobalLimit 100
+- LocalLimit 100
   +- Sort [promotions#0 ASC NULLS FIRST, total#1 ASC NULLS FIRST], true
      +- Project [promotions#0, total#1, CheckOverflow((promote_precision(cast(CheckOverflow((promote_precision(cast(cast(promotions#0 as decimal(15,4)) as decimal(15,4))) / promote_precision(cast(cast(total#1 as decimal(15,4)) as decimal(15,4)))), DecimalType(35,20)) as decimal(35,20))) * promote_precision(cast(cast(100 as decimal(3,0)) as decimal(35,20)))), DecimalType(38,19)) AS (CAST((CAST(CAST(promotions AS DECIMAL(15,4)) AS DECIMAL(15,4)) / CAST(CAST(total AS DECIMAL(15,4)) AS DECIMAL(15,4))) AS DECIMAL(35,20)) * CAST(CAST(100 AS DECIMAL(3,0)) AS DECIMAL(35,20)))#158]
         +- Join Inner
            :- SubqueryAlias `promotional_sales`
            :  +- Aggregate [sum(ss_ext_sales_price#19) AS promotions#0]
            :     +- Filter (((((ss_sold_date_sk#4 = d_date_sk#75) && (ss_store_sk#11 = s_store_sk#27)) && (ss_promo_sk#12 = p_promo_sk#56)) && (((ss_customer_sk#7 = c_customer_sk#103) && (ca_address_sk#121 = c_current_addr_sk#107)) && (ss_item_sk#6 = i_item_sk#134))) && ((((ca_gmt_offset#132 = cast(-5 as double)) && (i_category#146 = Jewelry)) && (((p_channel_dmail#64 = Y) || (p_channel_email#65 = Y)) || (p_channel_tv#67 = Y))) && (((s_gmt_offset#54 = cast(-5 as double)) && (d_year#81 = 1998)) && (d_moy#83 = 11))))
            :        +- Join Inner
            :           :- Join Inner
            :           :  :- Join Inner
            :           :  :  :- Join Inner
            :           :  :  :  :- Join Inner
            :           :  :  :  :  :- Join Inner
            :           :  :  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
            :           :  :  :  :  :  :  +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
            :           :  :  :  :  :  +- SubqueryAlias `tpcds`.`store`
            :           :  :  :  :  :     +- Relation[s_store_sk#27,s_store_id#28,s_rec_start_date#29,s_rec_end_date#30,s_closed_date_sk#31,s_store_name#32,s_number_employees#33,s_floor_space#34,s_hours#35,s_manager#36,s_market_id#37,s_geography_class#38,s_market_desc#39,s_market_manager#40,s_division_id#41,s_division_name#42,s_company_id#43,s_company_name#44,s_street_number#45,s_street_name#46,s_street_type#47,s_suite_number#48,s_city#49,s_county#50,... 5 more fields] parquet
            :           :  :  :  :  +- SubqueryAlias `tpcds`.`promotion`
            :           :  :  :  :     +- Relation[p_promo_sk#56,p_promo_id#57,p_start_date_sk#58,p_end_date_sk#59,p_item_sk#60,p_cost#61,p_response_target#62,p_promo_name#63,p_channel_dmail#64,p_channel_email#65,p_channel_catalog#66,p_channel_tv#67,p_channel_radio#68,p_channel_press#69,p_channel_event#70,p_channel_demo#71,p_channel_details#72,p_purpose#73,p_discount_active#74] parquet
            :           :  :  :  +- SubqueryAlias `tpcds`.`date_dim`
            :           :  :  :     +- Relation[d_date_sk#75,d_date_id#76,d_date#77,d_month_seq#78,d_week_seq#79,d_quarter_seq#80,d_year#81,d_dow#82,d_moy#83,d_dom#84,d_qoy#85,d_fy_year#86,d_fy_quarter_seq#87,d_fy_week_seq#88,d_day_name#89,d_quarter_name#90,d_holiday#91,d_weekend#92,d_following_holiday#93,d_first_dom#94,d_last_dom#95,d_same_day_ly#96,d_same_day_lq#97,d_current_day#98,... 4 more fields] parquet
            :           :  :  +- SubqueryAlias `tpcds`.`customer`
            :           :  :     +- Relation[c_customer_sk#103,c_customer_id#104,c_current_cdemo_sk#105,c_current_hdemo_sk#106,c_current_addr_sk#107,c_first_shipto_date_sk#108,c_first_sales_date_sk#109,c_salutation#110,c_first_name#111,c_last_name#112,c_preferred_cust_flag#113,c_birth_day#114,c_birth_month#115,c_birth_year#116,c_birth_country#117,c_login#118,c_email_address#119,c_last_review_date#120] parquet
            :           :  +- SubqueryAlias `tpcds`.`customer_address`
            :           :     +- Relation[ca_address_sk#121,ca_address_id#122,ca_street_number#123,ca_street_name#124,ca_street_type#125,ca_suite_number#126,ca_city#127,ca_county#128,ca_state#129,ca_zip#130,ca_country#131,ca_gmt_offset#132,ca_location_type#133] parquet
            :           +- SubqueryAlias `tpcds`.`item`
            :              +- Relation[i_item_sk#134,i_item_id#135,i_rec_start_date#136,i_rec_end_date#137,i_item_desc#138,i_current_price#139,i_wholesale_cost#140,i_brand_id#141,i_brand#142,i_class_id#143,i_class#144,i_category_id#145,i_category#146,i_manufact_id#147,i_manufact#148,i_size#149,i_formulation#150,i_color#151,i_units#152,i_container#153,i_manager_id#154,i_product_name#155] parquet
            +- SubqueryAlias `all_sales`
               +- Aggregate [sum(ss_ext_sales_price#19) AS total#1]
                  +- Filter (((((ss_sold_date_sk#4 = d_date_sk#75) && (ss_store_sk#11 = s_store_sk#27)) && (ss_customer_sk#7 = c_customer_sk#103)) && ((ca_address_sk#121 = c_current_addr_sk#107) && (ss_item_sk#6 = i_item_sk#134))) && ((((ca_gmt_offset#132 = cast(-5 as double)) && (i_category#146 = Jewelry)) && (s_gmt_offset#54 = cast(-5 as double))) && ((d_year#81 = 1998) && (d_moy#83 = 11))))
                     +- Join Inner
                        :- Join Inner
                        :  :- Join Inner
                        :  :  :- Join Inner
                        :  :  :  :- Join Inner
                        :  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
                        :  :  :  :  :  +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
                        :  :  :  :  +- SubqueryAlias `tpcds`.`store`
                        :  :  :  :     +- Relation[s_store_sk#27,s_store_id#28,s_rec_start_date#29,s_rec_end_date#30,s_closed_date_sk#31,s_store_name#32,s_number_employees#33,s_floor_space#34,s_hours#35,s_manager#36,s_market_id#37,s_geography_class#38,s_market_desc#39,s_market_manager#40,s_division_id#41,s_division_name#42,s_company_id#43,s_company_name#44,s_street_number#45,s_street_name#46,s_street_type#47,s_suite_number#48,s_city#49,s_county#50,... 5 more fields] parquet
                        :  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                        :  :  :     +- Relation[d_date_sk#75,d_date_id#76,d_date#77,d_month_seq#78,d_week_seq#79,d_quarter_seq#80,d_year#81,d_dow#82,d_moy#83,d_dom#84,d_qoy#85,d_fy_year#86,d_fy_quarter_seq#87,d_fy_week_seq#88,d_day_name#89,d_quarter_name#90,d_holiday#91,d_weekend#92,d_following_holiday#93,d_first_dom#94,d_last_dom#95,d_same_day_ly#96,d_same_day_lq#97,d_current_day#98,... 4 more fields] parquet
                        :  :  +- SubqueryAlias `tpcds`.`customer`
                        :  :     +- Relation[c_customer_sk#103,c_customer_id#104,c_current_cdemo_sk#105,c_current_hdemo_sk#106,c_current_addr_sk#107,c_first_shipto_date_sk#108,c_first_sales_date_sk#109,c_salutation#110,c_first_name#111,c_last_name#112,c_preferred_cust_flag#113,c_birth_day#114,c_birth_month#115,c_birth_year#116,c_birth_country#117,c_login#118,c_email_address#119,c_last_review_date#120] parquet
                        :  +- SubqueryAlias `tpcds`.`customer_address`
                        :     +- Relation[ca_address_sk#121,ca_address_id#122,ca_street_number#123,ca_street_name#124,ca_street_type#125,ca_suite_number#126,ca_city#127,ca_county#128,ca_state#129,ca_zip#130,ca_country#131,ca_gmt_offset#132,ca_location_type#133] parquet
                        +- SubqueryAlias `tpcds`.`item`
                           +- Relation[i_item_sk#134,i_item_id#135,i_rec_start_date#136,i_rec_end_date#137,i_item_desc#138,i_current_price#139,i_wholesale_cost#140,i_brand_id#141,i_brand#142,i_class_id#143,i_class#144,i_category_id#145,i_category#146,i_manufact_id#147,i_manufact#148,i_size#149,i_formulation#150,i_color#151,i_units#152,i_container#153,i_manager_id#154,i_product_name#155] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [promotions#0 ASC NULLS FIRST, total#1 ASC NULLS FIRST], true
      +- Project [promotions#0, total#1, CheckOverflow((promote_precision(CheckOverflow((promote_precision(cast(promotions#0 as decimal(15,4))) / promote_precision(cast(total#1 as decimal(15,4)))), DecimalType(35,20))) * 100.00000000000000000000), DecimalType(38,19)) AS (CAST((CAST(CAST(promotions AS DECIMAL(15,4)) AS DECIMAL(15,4)) / CAST(CAST(total AS DECIMAL(15,4)) AS DECIMAL(15,4))) AS DECIMAL(35,20)) * CAST(CAST(100 AS DECIMAL(3,0)) AS DECIMAL(35,20)))#158]
         +- Join Inner
            :- Aggregate [sum(ss_ext_sales_price#19) AS promotions#0]
            :  +- Project [ss_ext_sales_price#19]
            :     +- Join Inner, (ss_item_sk#6 = i_item_sk#134)
            :        :- Project [ss_item_sk#6, ss_ext_sales_price#19]
            :        :  +- Join Inner, (ca_address_sk#121 = c_current_addr_sk#107)
            :        :     :- Project [ss_item_sk#6, ss_ext_sales_price#19, c_current_addr_sk#107]
            :        :     :  +- Join Inner, (ss_customer_sk#7 = c_customer_sk#103)
            :        :     :     :- Project [ss_item_sk#6, ss_customer_sk#7, ss_ext_sales_price#19]
            :        :     :     :  +- Join Inner, (ss_sold_date_sk#4 = d_date_sk#75)
            :        :     :     :     :- Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_ext_sales_price#19]
            :        :     :     :     :  +- Join Inner, (ss_promo_sk#12 = p_promo_sk#56)
            :        :     :     :     :     :- Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_promo_sk#12, ss_ext_sales_price#19]
            :        :     :     :     :     :  +- Join Inner, (ss_store_sk#11 = s_store_sk#27)
            :        :     :     :     :     :     :- Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_store_sk#11, ss_promo_sk#12, ss_ext_sales_price#19]
            :        :     :     :     :     :     :  +- Filter ((((isnotnull(ss_store_sk#11) && isnotnull(ss_promo_sk#12)) && isnotnull(ss_sold_date_sk#4)) && isnotnull(ss_customer_sk#7)) && isnotnull(ss_item_sk#6))
            :        :     :     :     :     :     :     +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
            :        :     :     :     :     :     +- Project [s_store_sk#27]
            :        :     :     :     :     :        +- Filter ((isnotnull(s_gmt_offset#54) && (s_gmt_offset#54 = -5.0)) && isnotnull(s_store_sk#27))
            :        :     :     :     :     :           +- Relation[s_store_sk#27,s_store_id#28,s_rec_start_date#29,s_rec_end_date#30,s_closed_date_sk#31,s_store_name#32,s_number_employees#33,s_floor_space#34,s_hours#35,s_manager#36,s_market_id#37,s_geography_class#38,s_market_desc#39,s_market_manager#40,s_division_id#41,s_division_name#42,s_company_id#43,s_company_name#44,s_street_number#45,s_street_name#46,s_street_type#47,s_suite_number#48,s_city#49,s_county#50,... 5 more fields] parquet
            :        :     :     :     :     +- Project [p_promo_sk#56]
            :        :     :     :     :        +- Filter ((((p_channel_dmail#64 = Y) || (p_channel_email#65 = Y)) || (p_channel_tv#67 = Y)) && isnotnull(p_promo_sk#56))
            :        :     :     :     :           +- Relation[p_promo_sk#56,p_promo_id#57,p_start_date_sk#58,p_end_date_sk#59,p_item_sk#60,p_cost#61,p_response_target#62,p_promo_name#63,p_channel_dmail#64,p_channel_email#65,p_channel_catalog#66,p_channel_tv#67,p_channel_radio#68,p_channel_press#69,p_channel_event#70,p_channel_demo#71,p_channel_details#72,p_purpose#73,p_discount_active#74] parquet
            :        :     :     :     +- Project [d_date_sk#75]
            :        :     :     :        +- Filter ((((isnotnull(d_year#81) && isnotnull(d_moy#83)) && (d_year#81 = 1998)) && (d_moy#83 = 11)) && isnotnull(d_date_sk#75))
            :        :     :     :           +- Relation[d_date_sk#75,d_date_id#76,d_date#77,d_month_seq#78,d_week_seq#79,d_quarter_seq#80,d_year#81,d_dow#82,d_moy#83,d_dom#84,d_qoy#85,d_fy_year#86,d_fy_quarter_seq#87,d_fy_week_seq#88,d_day_name#89,d_quarter_name#90,d_holiday#91,d_weekend#92,d_following_holiday#93,d_first_dom#94,d_last_dom#95,d_same_day_ly#96,d_same_day_lq#97,d_current_day#98,... 4 more fields] parquet
            :        :     :     +- Project [c_customer_sk#103, c_current_addr_sk#107]
            :        :     :        +- Filter (isnotnull(c_customer_sk#103) && isnotnull(c_current_addr_sk#107))
            :        :     :           +- Relation[c_customer_sk#103,c_customer_id#104,c_current_cdemo_sk#105,c_current_hdemo_sk#106,c_current_addr_sk#107,c_first_shipto_date_sk#108,c_first_sales_date_sk#109,c_salutation#110,c_first_name#111,c_last_name#112,c_preferred_cust_flag#113,c_birth_day#114,c_birth_month#115,c_birth_year#116,c_birth_country#117,c_login#118,c_email_address#119,c_last_review_date#120] parquet
            :        :     +- Project [ca_address_sk#121]
            :        :        +- Filter ((isnotnull(ca_gmt_offset#132) && (ca_gmt_offset#132 = -5.0)) && isnotnull(ca_address_sk#121))
            :        :           +- Relation[ca_address_sk#121,ca_address_id#122,ca_street_number#123,ca_street_name#124,ca_street_type#125,ca_suite_number#126,ca_city#127,ca_county#128,ca_state#129,ca_zip#130,ca_country#131,ca_gmt_offset#132,ca_location_type#133] parquet
            :        +- Project [i_item_sk#134]
            :           +- Filter ((isnotnull(i_category#146) && (i_category#146 = Jewelry)) && isnotnull(i_item_sk#134))
            :              +- Relation[i_item_sk#134,i_item_id#135,i_rec_start_date#136,i_rec_end_date#137,i_item_desc#138,i_current_price#139,i_wholesale_cost#140,i_brand_id#141,i_brand#142,i_class_id#143,i_class#144,i_category_id#145,i_category#146,i_manufact_id#147,i_manufact#148,i_size#149,i_formulation#150,i_color#151,i_units#152,i_container#153,i_manager_id#154,i_product_name#155] parquet
            +- Aggregate [sum(ss_ext_sales_price#19) AS total#1]
               +- Project [ss_ext_sales_price#19]
                  +- Join Inner, (ss_item_sk#6 = i_item_sk#134)
                     :- Project [ss_item_sk#6, ss_ext_sales_price#19]
                     :  +- Join Inner, (ca_address_sk#121 = c_current_addr_sk#107)
                     :     :- Project [ss_item_sk#6, ss_ext_sales_price#19, c_current_addr_sk#107]
                     :     :  +- Join Inner, (ss_customer_sk#7 = c_customer_sk#103)
                     :     :     :- Project [ss_item_sk#6, ss_customer_sk#7, ss_ext_sales_price#19]
                     :     :     :  +- Join Inner, (ss_sold_date_sk#4 = d_date_sk#75)
                     :     :     :     :- Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_ext_sales_price#19]
                     :     :     :     :  +- Join Inner, (ss_store_sk#11 = s_store_sk#27)
                     :     :     :     :     :- Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_store_sk#11, ss_ext_sales_price#19]
                     :     :     :     :     :  +- Filter (((isnotnull(ss_store_sk#11) && isnotnull(ss_sold_date_sk#4)) && isnotnull(ss_customer_sk#7)) && isnotnull(ss_item_sk#6))
                     :     :     :     :     :     +- Relation[ss_sold_date_sk#4,ss_sold_time_sk#5,ss_item_sk#6,ss_customer_sk#7,ss_cdemo_sk#8,ss_hdemo_sk#9,ss_addr_sk#10,ss_store_sk#11,ss_promo_sk#12,ss_ticket_number#13,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26] parquet
                     :     :     :     :     +- Project [s_store_sk#27]
                     :     :     :     :        +- Filter ((isnotnull(s_gmt_offset#54) && (s_gmt_offset#54 = -5.0)) && isnotnull(s_store_sk#27))
                     :     :     :     :           +- Relation[s_store_sk#27,s_store_id#28,s_rec_start_date#29,s_rec_end_date#30,s_closed_date_sk#31,s_store_name#32,s_number_employees#33,s_floor_space#34,s_hours#35,s_manager#36,s_market_id#37,s_geography_class#38,s_market_desc#39,s_market_manager#40,s_division_id#41,s_division_name#42,s_company_id#43,s_company_name#44,s_street_number#45,s_street_name#46,s_street_type#47,s_suite_number#48,s_city#49,s_county#50,... 5 more fields] parquet
                     :     :     :     +- Project [d_date_sk#75]
                     :     :     :        +- Filter ((((isnotnull(d_year#81) && isnotnull(d_moy#83)) && (d_year#81 = 1998)) && (d_moy#83 = 11)) && isnotnull(d_date_sk#75))
                     :     :     :           +- Relation[d_date_sk#75,d_date_id#76,d_date#77,d_month_seq#78,d_week_seq#79,d_quarter_seq#80,d_year#81,d_dow#82,d_moy#83,d_dom#84,d_qoy#85,d_fy_year#86,d_fy_quarter_seq#87,d_fy_week_seq#88,d_day_name#89,d_quarter_name#90,d_holiday#91,d_weekend#92,d_following_holiday#93,d_first_dom#94,d_last_dom#95,d_same_day_ly#96,d_same_day_lq#97,d_current_day#98,... 4 more fields] parquet
                     :     :     +- Project [c_customer_sk#103, c_current_addr_sk#107]
                     :     :        +- Filter (isnotnull(c_customer_sk#103) && isnotnull(c_current_addr_sk#107))
                     :     :           +- Relation[c_customer_sk#103,c_customer_id#104,c_current_cdemo_sk#105,c_current_hdemo_sk#106,c_current_addr_sk#107,c_first_shipto_date_sk#108,c_first_sales_date_sk#109,c_salutation#110,c_first_name#111,c_last_name#112,c_preferred_cust_flag#113,c_birth_day#114,c_birth_month#115,c_birth_year#116,c_birth_country#117,c_login#118,c_email_address#119,c_last_review_date#120] parquet
                     :     +- Project [ca_address_sk#121]
                     :        +- Filter ((isnotnull(ca_gmt_offset#132) && (ca_gmt_offset#132 = -5.0)) && isnotnull(ca_address_sk#121))
                     :           +- Relation[ca_address_sk#121,ca_address_id#122,ca_street_number#123,ca_street_name#124,ca_street_type#125,ca_suite_number#126,ca_city#127,ca_county#128,ca_state#129,ca_zip#130,ca_country#131,ca_gmt_offset#132,ca_location_type#133] parquet
                     +- Project [i_item_sk#134]
                        +- Filter ((isnotnull(i_category#146) && (i_category#146 = Jewelry)) && isnotnull(i_item_sk#134))
                           +- Relation[i_item_sk#134,i_item_id#135,i_rec_start_date#136,i_rec_end_date#137,i_item_desc#138,i_current_price#139,i_wholesale_cost#140,i_brand_id#141,i_brand#142,i_class_id#143,i_class#144,i_category_id#145,i_category#146,i_manufact_id#147,i_manufact#148,i_size#149,i_formulation#150,i_color#151,i_units#152,i_container#153,i_manager_id#154,i_product_name#155] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[promotions#0 ASC NULLS FIRST,total#1 ASC NULLS FIRST], output=[promotions#0,total#1,(CAST((CAST(CAST(promotions AS DECIMAL(15,4)) AS DECIMAL(15,4)) / CAST(CAST(total AS DECIMAL(15,4)) AS DECIMAL(15,4))) AS DECIMAL(35,20)) * CAST(CAST(100 AS DECIMAL(3,0)) AS DECIMAL(35,20)))#158])
+- *(16) Project [promotions#0, total#1, CheckOverflow((promote_precision(CheckOverflow((promote_precision(cast(promotions#0 as decimal(15,4))) / promote_precision(cast(total#1 as decimal(15,4)))), DecimalType(35,20))) * 100.00000000000000000000), DecimalType(38,19)) AS (CAST((CAST(CAST(promotions AS DECIMAL(15,4)) AS DECIMAL(15,4)) / CAST(CAST(total AS DECIMAL(15,4)) AS DECIMAL(15,4))) AS DECIMAL(35,20)) * CAST(CAST(100 AS DECIMAL(3,0)) AS DECIMAL(35,20)))#158]
   +- BroadcastNestedLoopJoin BuildRight, Inner
      :- *(8) HashAggregate(keys=[], functions=[sum(ss_ext_sales_price#19)], output=[promotions#0])
      :  +- Exchange SinglePartition
      :     +- *(7) HashAggregate(keys=[], functions=[partial_sum(ss_ext_sales_price#19)], output=[sum#160])
      :        +- *(7) Project [ss_ext_sales_price#19]
      :           +- *(7) BroadcastHashJoin [ss_item_sk#6], [i_item_sk#134], Inner, BuildRight
      :              :- *(7) Project [ss_item_sk#6, ss_ext_sales_price#19]
      :              :  +- *(7) BroadcastHashJoin [c_current_addr_sk#107], [ca_address_sk#121], Inner, BuildRight
      :              :     :- *(7) Project [ss_item_sk#6, ss_ext_sales_price#19, c_current_addr_sk#107]
      :              :     :  +- *(7) BroadcastHashJoin [ss_customer_sk#7], [c_customer_sk#103], Inner, BuildRight
      :              :     :     :- *(7) Project [ss_item_sk#6, ss_customer_sk#7, ss_ext_sales_price#19]
      :              :     :     :  +- *(7) BroadcastHashJoin [ss_sold_date_sk#4], [d_date_sk#75], Inner, BuildRight
      :              :     :     :     :- *(7) Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_ext_sales_price#19]
      :              :     :     :     :  +- *(7) BroadcastHashJoin [ss_promo_sk#12], [p_promo_sk#56], Inner, BuildRight
      :              :     :     :     :     :- *(7) Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_promo_sk#12, ss_ext_sales_price#19]
      :              :     :     :     :     :  +- *(7) BroadcastHashJoin [ss_store_sk#11], [s_store_sk#27], Inner, BuildRight
      :              :     :     :     :     :     :- *(7) Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_store_sk#11, ss_promo_sk#12, ss_ext_sales_price#19]
      :              :     :     :     :     :     :  +- *(7) Filter ((((isnotnull(ss_store_sk#11) && isnotnull(ss_promo_sk#12)) && isnotnull(ss_sold_date_sk#4)) && isnotnull(ss_customer_sk#7)) && isnotnull(ss_item_sk#6))
      :              :     :     :     :     :     :     +- *(7) FileScan parquet tpcds.store_sales[ss_sold_date_sk#4,ss_item_sk#6,ss_customer_sk#7,ss_store_sk#11,ss_promo_sk#12,ss_ext_sales_price#19] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_promo_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_custome..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_promo_sk:int,ss_e...
      :              :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              :     :     :     :     :        +- *(1) Project [s_store_sk#27]
      :              :     :     :     :     :           +- *(1) Filter ((isnotnull(s_gmt_offset#54) && (s_gmt_offset#54 = -5.0)) && isnotnull(s_store_sk#27))
      :              :     :     :     :     :              +- *(1) FileScan parquet tpcds.store[s_store_sk#27,s_gmt_offset#54] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_gmt_offset), EqualTo(s_gmt_offset,-5.0), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_gmt_offset:double>
      :              :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              :     :     :     :        +- *(2) Project [p_promo_sk#56]
      :              :     :     :     :           +- *(2) Filter ((((p_channel_dmail#64 = Y) || (p_channel_email#65 = Y)) || (p_channel_tv#67 = Y)) && isnotnull(p_promo_sk#56))
      :              :     :     :     :              +- *(2) FileScan parquet tpcds.promotion[p_promo_sk#56,p_channel_dmail#64,p_channel_email#65,p_channel_tv#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/p..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(p_channel_dmail,Y),EqualTo(p_channel_email,Y)),EqualTo(p_channel_tv,Y)), IsNotNull..., ReadSchema: struct<p_promo_sk:int,p_channel_dmail:string,p_channel_email:string,p_channel_tv:string>
      :              :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              :     :     :        +- *(3) Project [d_date_sk#75]
      :              :     :     :           +- *(3) Filter ((((isnotnull(d_year#81) && isnotnull(d_moy#83)) && (d_year#81 = 1998)) && (d_moy#83 = 11)) && isnotnull(d_date_sk#75))
      :              :     :     :              +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#75,d_year#81,d_moy#83] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
      :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              :     :        +- *(4) Project [c_customer_sk#103, c_current_addr_sk#107]
      :              :     :           +- *(4) Filter (isnotnull(c_customer_sk#103) && isnotnull(c_current_addr_sk#107))
      :              :     :              +- *(4) FileScan parquet tpcds.customer[c_customer_sk#103,c_current_addr_sk#107] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
      :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              :        +- *(5) Project [ca_address_sk#121]
      :              :           +- *(5) Filter ((isnotnull(ca_gmt_offset#132) && (ca_gmt_offset#132 = -5.0)) && isnotnull(ca_address_sk#121))
      :              :              +- *(5) FileScan parquet tpcds.customer_address[ca_address_sk#121,ca_gmt_offset#132] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_gmt_offset), EqualTo(ca_gmt_offset,-5.0), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_gmt_offset:double>
      :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                 +- *(6) Project [i_item_sk#134]
      :                    +- *(6) Filter ((isnotnull(i_category#146) && (i_category#146 = Jewelry)) && isnotnull(i_item_sk#134))
      :                       +- *(6) FileScan parquet tpcds.item[i_item_sk#134,i_category#146] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), EqualTo(i_category,Jewelry), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_category:string>
      +- BroadcastExchange IdentityBroadcastMode
         +- *(15) HashAggregate(keys=[], functions=[sum(ss_ext_sales_price#19)], output=[total#1])
            +- Exchange SinglePartition
               +- *(14) HashAggregate(keys=[], functions=[partial_sum(ss_ext_sales_price#19)], output=[sum#162])
                  +- *(14) Project [ss_ext_sales_price#19]
                     +- *(14) BroadcastHashJoin [ss_item_sk#6], [i_item_sk#134], Inner, BuildRight
                        :- *(14) Project [ss_item_sk#6, ss_ext_sales_price#19]
                        :  +- *(14) BroadcastHashJoin [c_current_addr_sk#107], [ca_address_sk#121], Inner, BuildRight
                        :     :- *(14) Project [ss_item_sk#6, ss_ext_sales_price#19, c_current_addr_sk#107]
                        :     :  +- *(14) BroadcastHashJoin [ss_customer_sk#7], [c_customer_sk#103], Inner, BuildRight
                        :     :     :- *(14) Project [ss_item_sk#6, ss_customer_sk#7, ss_ext_sales_price#19]
                        :     :     :  +- *(14) BroadcastHashJoin [ss_sold_date_sk#4], [d_date_sk#75], Inner, BuildRight
                        :     :     :     :- *(14) Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_ext_sales_price#19]
                        :     :     :     :  +- *(14) BroadcastHashJoin [ss_store_sk#11], [s_store_sk#27], Inner, BuildRight
                        :     :     :     :     :- *(14) Project [ss_sold_date_sk#4, ss_item_sk#6, ss_customer_sk#7, ss_store_sk#11, ss_ext_sales_price#19]
                        :     :     :     :     :  +- *(14) Filter (((isnotnull(ss_store_sk#11) && isnotnull(ss_sold_date_sk#4)) && isnotnull(ss_customer_sk#7)) && isnotnull(ss_item_sk#6))
                        :     :     :     :     :     +- *(14) FileScan parquet tpcds.store_sales[ss_sold_date_sk#4,ss_item_sk#6,ss_customer_sk#7,ss_store_sk#11,ss_ext_sales_price#19] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_customer_sk), IsNotNull(ss_item..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ext_sales_price:d...
                        :     :     :     :     +- ReusedExchange [s_store_sk#27], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :     :     :     +- ReusedExchange [d_date_sk#75], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :     :     +- ReusedExchange [c_customer_sk#103, c_current_addr_sk#107], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        :     +- ReusedExchange [ca_address_sk#121], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        +- ReusedExchange [i_item_sk#134], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.287 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 61 in stream 0 using template query61.tpl
------------------------------------------------------^^^

