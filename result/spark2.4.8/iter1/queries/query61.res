Error in query: Detected implicit cartesian product for INNER join between logical plans
Aggregate [sum(ss_ext_sales_price#17) AS promotions#0]
+- Project [ss_ext_sales_price#17]
   +- Join Inner, (ss_item_sk#4 = i_item_sk#132)
      :- Project [ss_item_sk#4, ss_ext_sales_price#17]
      :  +- Join Inner, (ca_address_sk#119 = c_current_addr_sk#105)
      :     :- Project [ss_item_sk#4, ss_ext_sales_price#17, c_current_addr_sk#105]
      :     :  +- Join Inner, (ss_customer_sk#5 = c_customer_sk#101)
      :     :     :- Project [ss_item_sk#4, ss_customer_sk#5, ss_ext_sales_price#17]
      :     :     :  +- Join Inner, (ss_sold_date_sk#2 = d_date_sk#73)
      :     :     :     :- Project [ss_sold_date_sk#2, ss_item_sk#4, ss_customer_sk#5, ss_ext_sales_price#17]
      :     :     :     :  +- Join Inner, (ss_promo_sk#10 = p_promo_sk#54)
      :     :     :     :     :- Project [ss_sold_date_sk#2, ss_item_sk#4, ss_customer_sk#5, ss_promo_sk#10, ss_ext_sales_price#17]
      :     :     :     :     :  +- Join Inner, (ss_store_sk#9 = s_store_sk#25)
      :     :     :     :     :     :- Project [ss_sold_date_sk#2, ss_item_sk#4, ss_customer_sk#5, ss_store_sk#9, ss_promo_sk#10, ss_ext_sales_price#17]
      :     :     :     :     :     :  +- Filter ((((isnotnull(ss_store_sk#9) && isnotnull(ss_promo_sk#10)) && isnotnull(ss_sold_date_sk#2)) && isnotnull(ss_customer_sk#5)) && isnotnull(ss_item_sk#4))
      :     :     :     :     :     :     +- Relation[ss_sold_date_sk#2,ss_sold_time_sk#3,ss_item_sk#4,ss_customer_sk#5,ss_cdemo_sk#6,ss_hdemo_sk#7,ss_addr_sk#8,ss_store_sk#9,ss_promo_sk#10,ss_ticket_number#11,ss_quantity#12,ss_wholesale_cost#13,ss_list_price#14,ss_sales_price#15,ss_ext_discount_amt#16,ss_ext_sales_price#17,ss_ext_wholesale_cost#18,ss_ext_list_price#19,ss_ext_tax#20,ss_coupon_amt#21,ss_net_paid#22,ss_net_paid_inc_tax#23,ss_net_profit#24] parquet
      :     :     :     :     :     +- Project [s_store_sk#25]
      :     :     :     :     :        +- Filter ((isnotnull(s_gmt_offset#52) && (s_gmt_offset#52 = -5.0)) && isnotnull(s_store_sk#25))
      :     :     :     :     :           +- Relation[s_store_sk#25,s_store_id#26,s_rec_start_date#27,s_rec_end_date#28,s_closed_date_sk#29,s_store_name#30,s_number_employees#31,s_floor_space#32,s_hours#33,s_manager#34,s_market_id#35,s_geography_class#36,s_market_desc#37,s_market_manager#38,s_division_id#39,s_division_name#40,s_company_id#41,s_company_name#42,s_street_number#43,s_street_name#44,s_street_type#45,s_suite_number#46,s_city#47,s_county#48,... 5 more fields] parquet
      :     :     :     :     +- Project [p_promo_sk#54]
      :     :     :     :        +- Filter ((((p_channel_dmail#62 = Y) || (p_channel_email#63 = Y)) || (p_channel_tv#65 = Y)) && isnotnull(p_promo_sk#54))
      :     :     :     :           +- Relation[p_promo_sk#54,p_promo_id#55,p_start_date_sk#56,p_end_date_sk#57,p_item_sk#58,p_cost#59,p_response_target#60,p_promo_name#61,p_channel_dmail#62,p_channel_email#63,p_channel_catalog#64,p_channel_tv#65,p_channel_radio#66,p_channel_press#67,p_channel_event#68,p_channel_demo#69,p_channel_details#70,p_purpose#71,p_discount_active#72] parquet
      :     :     :     +- Project [d_date_sk#73]
      :     :     :        +- Filter (((isnotnull(d_year#79) && isnotnull(d_moy#81)) && ((d_year#79 = 1998) && (d_moy#81 = 11))) && isnotnull(d_date_sk#73))
      :     :     :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
      :     :     +- Project [c_customer_sk#101, c_current_addr_sk#105]
      :     :        +- Filter (isnotnull(c_customer_sk#101) && isnotnull(c_current_addr_sk#105))
      :     :           +- Relation[c_customer_sk#101,c_customer_id#102,c_current_cdemo_sk#103,c_current_hdemo_sk#104,c_current_addr_sk#105,c_first_shipto_date_sk#106,c_first_sales_date_sk#107,c_salutation#108,c_first_name#109,c_last_name#110,c_preferred_cust_flag#111,c_birth_day#112,c_birth_month#113,c_birth_year#114,c_birth_country#115,c_login#116,c_email_address#117,c_last_review_date#118] parquet
      :     +- Project [ca_address_sk#119]
      :        +- Filter ((isnotnull(ca_gmt_offset#130) && (ca_gmt_offset#130 = -5.0)) && isnotnull(ca_address_sk#119))
      :           +- Relation[ca_address_sk#119,ca_address_id#120,ca_street_number#121,ca_street_name#122,ca_street_type#123,ca_suite_number#124,ca_city#125,ca_county#126,ca_state#127,ca_zip#128,ca_country#129,ca_gmt_offset#130,ca_location_type#131] parquet
      +- Project [i_item_sk#132]
         +- Filter ((isnotnull(i_category#144) && (i_category#144 = Jewelry)) && isnotnull(i_item_sk#132))
            +- Relation[i_item_sk#132,i_item_id#133,i_rec_start_date#134,i_rec_end_date#135,i_item_desc#136,i_current_price#137,i_wholesale_cost#138,i_brand_id#139,i_brand#140,i_class_id#141,i_class#142,i_category_id#143,i_category#144,i_manufact_id#145,i_manufact#146,i_size#147,i_formulation#148,i_color#149,i_units#150,i_container#151,i_manager_id#152,i_product_name#153] parquet
and
Aggregate [sum(ss_ext_sales_price#17) AS total#1]
+- Project [ss_ext_sales_price#17]
   +- Join Inner, (ss_item_sk#4 = i_item_sk#132)
      :- Project [ss_item_sk#4, ss_ext_sales_price#17]
      :  +- Join Inner, (ca_address_sk#119 = c_current_addr_sk#105)
      :     :- Project [ss_item_sk#4, ss_ext_sales_price#17, c_current_addr_sk#105]
      :     :  +- Join Inner, (ss_customer_sk#5 = c_customer_sk#101)
      :     :     :- Project [ss_item_sk#4, ss_customer_sk#5, ss_ext_sales_price#17]
      :     :     :  +- Join Inner, (ss_sold_date_sk#2 = d_date_sk#73)
      :     :     :     :- Project [ss_sold_date_sk#2, ss_item_sk#4, ss_customer_sk#5, ss_ext_sales_price#17]
      :     :     :     :  +- Join Inner, (ss_store_sk#9 = s_store_sk#25)
      :     :     :     :     :- Project [ss_sold_date_sk#2, ss_item_sk#4, ss_customer_sk#5, ss_store_sk#9, ss_ext_sales_price#17]
      :     :     :     :     :  +- Filter (((isnotnull(ss_store_sk#9) && isnotnull(ss_sold_date_sk#2)) && isnotnull(ss_customer_sk#5)) && isnotnull(ss_item_sk#4))
      :     :     :     :     :     +- Relation[ss_sold_date_sk#2,ss_sold_time_sk#3,ss_item_sk#4,ss_customer_sk#5,ss_cdemo_sk#6,ss_hdemo_sk#7,ss_addr_sk#8,ss_store_sk#9,ss_promo_sk#10,ss_ticket_number#11,ss_quantity#12,ss_wholesale_cost#13,ss_list_price#14,ss_sales_price#15,ss_ext_discount_amt#16,ss_ext_sales_price#17,ss_ext_wholesale_cost#18,ss_ext_list_price#19,ss_ext_tax#20,ss_coupon_amt#21,ss_net_paid#22,ss_net_paid_inc_tax#23,ss_net_profit#24] parquet
      :     :     :     :     +- Project [s_store_sk#25]
      :     :     :     :        +- Filter ((isnotnull(s_gmt_offset#52) && (s_gmt_offset#52 = -5.0)) && isnotnull(s_store_sk#25))
      :     :     :     :           +- Relation[s_store_sk#25,s_store_id#26,s_rec_start_date#27,s_rec_end_date#28,s_closed_date_sk#29,s_store_name#30,s_number_employees#31,s_floor_space#32,s_hours#33,s_manager#34,s_market_id#35,s_geography_class#36,s_market_desc#37,s_market_manager#38,s_division_id#39,s_division_name#40,s_company_id#41,s_company_name#42,s_street_number#43,s_street_name#44,s_street_type#45,s_suite_number#46,s_city#47,s_county#48,... 5 more fields] parquet
      :     :     :     +- Project [d_date_sk#73]
      :     :     :        +- Filter (((isnotnull(d_year#79) && isnotnull(d_moy#81)) && ((d_year#79 = 1998) && (d_moy#81 = 11))) && isnotnull(d_date_sk#73))
      :     :     :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
      :     :     +- Project [c_customer_sk#101, c_current_addr_sk#105]
      :     :        +- Filter (isnotnull(c_customer_sk#101) && isnotnull(c_current_addr_sk#105))
      :     :           +- Relation[c_customer_sk#101,c_customer_id#102,c_current_cdemo_sk#103,c_current_hdemo_sk#104,c_current_addr_sk#105,c_first_shipto_date_sk#106,c_first_sales_date_sk#107,c_salutation#108,c_first_name#109,c_last_name#110,c_preferred_cust_flag#111,c_birth_day#112,c_birth_month#113,c_birth_year#114,c_birth_country#115,c_login#116,c_email_address#117,c_last_review_date#118] parquet
      :     +- Project [ca_address_sk#119]
      :        +- Filter ((isnotnull(ca_gmt_offset#130) && (ca_gmt_offset#130 = -5.0)) && isnotnull(ca_address_sk#119))
      :           +- Relation[ca_address_sk#119,ca_address_id#120,ca_street_number#121,ca_street_name#122,ca_street_type#123,ca_suite_number#124,ca_city#125,ca_county#126,ca_state#127,ca_zip#128,ca_country#129,ca_gmt_offset#130,ca_location_type#131] parquet
      +- Project [i_item_sk#132]
         +- Filter ((isnotnull(i_category#144) && (i_category#144 = Jewelry)) && isnotnull(i_item_sk#132))
            +- Relation[i_item_sk#132,i_item_id#133,i_rec_start_date#134,i_rec_end_date#135,i_item_desc#136,i_current_price#137,i_wholesale_cost#138,i_brand_id#139,i_brand#140,i_class_id#141,i_class#142,i_category_id#143,i_category#144,i_manufact_id#145,i_manufact#146,i_size#147,i_formulation#148,i_color#149,i_units#150,i_container#151,i_manager_id#152,i_product_name#153] parquet
Join condition is missing or trivial.
Either: use the CROSS JOIN syntax to allow cartesian products between these
relations, or: enable implicit cartesian products by setting the configuration
variable spark.sql.crossJoin.enabled=true;
