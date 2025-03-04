Error in query: Detected implicit cartesian product for INNER join between logical plans
Join Inner
:- Join Inner
:  :- Join Inner
:  :  :- Join Inner
:  :  :  :- Join Inner
:  :  :  :  :- Join Inner
:  :  :  :  :  :- Aggregate [count(1) AS h8_30_to_9#0L]
:  :  :  :  :  :  +- Project
:  :  :  :  :  :     +- Join Inner, (ss_store_sk#23 = s_store_sk#54)
:  :  :  :  :  :        :- Project [ss_store_sk#23]
:  :  :  :  :  :        :  +- Join Inner, (ss_sold_time_sk#17 = t_time_sk#44)
:  :  :  :  :  :        :     :- Project [ss_sold_time_sk#17, ss_store_sk#23]
:  :  :  :  :  :        :     :  +- Join Inner, (ss_hdemo_sk#21 = hd_demo_sk#39)
:  :  :  :  :  :        :     :     :- Project [ss_sold_time_sk#17, ss_hdemo_sk#21, ss_store_sk#23]
:  :  :  :  :  :        :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#21) && isnotnull(ss_sold_time_sk#17)) && isnotnull(ss_store_sk#23))
:  :  :  :  :  :        :     :     :     +- Relation[ss_sold_date_sk#16,ss_sold_time_sk#17,ss_item_sk#18,ss_customer_sk#19,ss_cdemo_sk#20,ss_hdemo_sk#21,ss_addr_sk#22,ss_store_sk#23,ss_promo_sk#24,ss_ticket_number#25,ss_quantity#26,ss_wholesale_cost#27,ss_list_price#28,ss_sales_price#29,ss_ext_discount_amt#30,ss_ext_sales_price#31,ss_ext_wholesale_cost#32,ss_ext_list_price#33,ss_ext_tax#34,ss_coupon_amt#35,ss_net_paid#36,ss_net_paid_inc_tax#37,ss_net_profit#38] parquet
:  :  :  :  :  :        :     :     +- Project [hd_demo_sk#39]
:  :  :  :  :  :        :     :        +- Filter (((((hd_dep_count#42 = 4) && (hd_vehicle_count#43 <= 6)) || ((hd_dep_count#42 = 2) && (hd_vehicle_count#43 <= 4))) || ((hd_dep_count#42 = 0) && (hd_vehicle_count#43 <= 2))) && isnotnull(hd_demo_sk#39))
:  :  :  :  :  :        :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
:  :  :  :  :  :        :     +- Project [t_time_sk#44]
:  :  :  :  :  :        :        +- Filter (((isnotnull(t_hour#47) && isnotnull(t_minute#48)) && ((t_hour#47 = 8) && (t_minute#48 >= 30))) && isnotnull(t_time_sk#44))
:  :  :  :  :  :        :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
:  :  :  :  :  :        +- Project [s_store_sk#54]
:  :  :  :  :  :           +- Filter ((isnotnull(s_store_name#59) && (s_store_name#59 = ese)) && isnotnull(s_store_sk#54))
:  :  :  :  :  :              +- Relation[s_store_sk#54,s_store_id#55,s_rec_start_date#56,s_rec_end_date#57,s_closed_date_sk#58,s_store_name#59,s_number_employees#60,s_floor_space#61,s_hours#62,s_manager#63,s_market_id#64,s_geography_class#65,s_market_desc#66,s_market_manager#67,s_division_id#68,s_division_name#69,s_company_id#70,s_company_name#71,s_street_number#72,s_street_name#73,s_street_type#74,s_suite_number#75,s_city#76,s_county#77,... 5 more fields] parquet
:  :  :  :  :  +- Aggregate [count(1) AS h9_to_9_30#1L]
:  :  :  :  :     +- Project
:  :  :  :  :        +- Join Inner, (ss_store_sk#23 = s_store_sk#54)
:  :  :  :  :           :- Project [ss_store_sk#23]
:  :  :  :  :           :  +- Join Inner, (ss_sold_time_sk#17 = t_time_sk#44)
:  :  :  :  :           :     :- Project [ss_sold_time_sk#17, ss_store_sk#23]
:  :  :  :  :           :     :  +- Join Inner, (ss_hdemo_sk#21 = hd_demo_sk#39)
:  :  :  :  :           :     :     :- Project [ss_sold_time_sk#17, ss_hdemo_sk#21, ss_store_sk#23]
:  :  :  :  :           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#21) && isnotnull(ss_sold_time_sk#17)) && isnotnull(ss_store_sk#23))
:  :  :  :  :           :     :     :     +- Relation[ss_sold_date_sk#16,ss_sold_time_sk#17,ss_item_sk#18,ss_customer_sk#19,ss_cdemo_sk#20,ss_hdemo_sk#21,ss_addr_sk#22,ss_store_sk#23,ss_promo_sk#24,ss_ticket_number#25,ss_quantity#26,ss_wholesale_cost#27,ss_list_price#28,ss_sales_price#29,ss_ext_discount_amt#30,ss_ext_sales_price#31,ss_ext_wholesale_cost#32,ss_ext_list_price#33,ss_ext_tax#34,ss_coupon_amt#35,ss_net_paid#36,ss_net_paid_inc_tax#37,ss_net_profit#38] parquet
:  :  :  :  :           :     :     +- Project [hd_demo_sk#39]
:  :  :  :  :           :     :        +- Filter (((((hd_dep_count#42 = 4) && (hd_vehicle_count#43 <= 6)) || ((hd_dep_count#42 = 2) && (hd_vehicle_count#43 <= 4))) || ((hd_dep_count#42 = 0) && (hd_vehicle_count#43 <= 2))) && isnotnull(hd_demo_sk#39))
:  :  :  :  :           :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
:  :  :  :  :           :     +- Project [t_time_sk#44]
:  :  :  :  :           :        +- Filter (((isnotnull(t_hour#47) && isnotnull(t_minute#48)) && ((t_hour#47 = 9) && (t_minute#48 < 30))) && isnotnull(t_time_sk#44))
:  :  :  :  :           :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
:  :  :  :  :           +- Project [s_store_sk#54]
:  :  :  :  :              +- Filter ((isnotnull(s_store_name#59) && (s_store_name#59 = ese)) && isnotnull(s_store_sk#54))
:  :  :  :  :                 +- Relation[s_store_sk#54,s_store_id#55,s_rec_start_date#56,s_rec_end_date#57,s_closed_date_sk#58,s_store_name#59,s_number_employees#60,s_floor_space#61,s_hours#62,s_manager#63,s_market_id#64,s_geography_class#65,s_market_desc#66,s_market_manager#67,s_division_id#68,s_division_name#69,s_company_id#70,s_company_name#71,s_street_number#72,s_street_name#73,s_street_type#74,s_suite_number#75,s_city#76,s_county#77,... 5 more fields] parquet
:  :  :  :  +- Aggregate [count(1) AS h9_30_to_10#2L]
:  :  :  :     +- Project
:  :  :  :        +- Join Inner, (ss_store_sk#23 = s_store_sk#54)
:  :  :  :           :- Project [ss_store_sk#23]
:  :  :  :           :  +- Join Inner, (ss_sold_time_sk#17 = t_time_sk#44)
:  :  :  :           :     :- Project [ss_sold_time_sk#17, ss_store_sk#23]
:  :  :  :           :     :  +- Join Inner, (ss_hdemo_sk#21 = hd_demo_sk#39)
:  :  :  :           :     :     :- Project [ss_sold_time_sk#17, ss_hdemo_sk#21, ss_store_sk#23]
:  :  :  :           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#21) && isnotnull(ss_sold_time_sk#17)) && isnotnull(ss_store_sk#23))
:  :  :  :           :     :     :     +- Relation[ss_sold_date_sk#16,ss_sold_time_sk#17,ss_item_sk#18,ss_customer_sk#19,ss_cdemo_sk#20,ss_hdemo_sk#21,ss_addr_sk#22,ss_store_sk#23,ss_promo_sk#24,ss_ticket_number#25,ss_quantity#26,ss_wholesale_cost#27,ss_list_price#28,ss_sales_price#29,ss_ext_discount_amt#30,ss_ext_sales_price#31,ss_ext_wholesale_cost#32,ss_ext_list_price#33,ss_ext_tax#34,ss_coupon_amt#35,ss_net_paid#36,ss_net_paid_inc_tax#37,ss_net_profit#38] parquet
:  :  :  :           :     :     +- Project [hd_demo_sk#39]
:  :  :  :           :     :        +- Filter (((((hd_dep_count#42 = 4) && (hd_vehicle_count#43 <= 6)) || ((hd_dep_count#42 = 2) && (hd_vehicle_count#43 <= 4))) || ((hd_dep_count#42 = 0) && (hd_vehicle_count#43 <= 2))) && isnotnull(hd_demo_sk#39))
:  :  :  :           :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
:  :  :  :           :     +- Project [t_time_sk#44]
:  :  :  :           :        +- Filter (((isnotnull(t_hour#47) && isnotnull(t_minute#48)) && ((t_hour#47 = 9) && (t_minute#48 >= 30))) && isnotnull(t_time_sk#44))
:  :  :  :           :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
:  :  :  :           +- Project [s_store_sk#54]
:  :  :  :              +- Filter ((isnotnull(s_store_name#59) && (s_store_name#59 = ese)) && isnotnull(s_store_sk#54))
:  :  :  :                 +- Relation[s_store_sk#54,s_store_id#55,s_rec_start_date#56,s_rec_end_date#57,s_closed_date_sk#58,s_store_name#59,s_number_employees#60,s_floor_space#61,s_hours#62,s_manager#63,s_market_id#64,s_geography_class#65,s_market_desc#66,s_market_manager#67,s_division_id#68,s_division_name#69,s_company_id#70,s_company_name#71,s_street_number#72,s_street_name#73,s_street_type#74,s_suite_number#75,s_city#76,s_county#77,... 5 more fields] parquet
:  :  :  +- Aggregate [count(1) AS h10_to_10_30#3L]
:  :  :     +- Project
:  :  :        +- Join Inner, (ss_store_sk#23 = s_store_sk#54)
:  :  :           :- Project [ss_store_sk#23]
:  :  :           :  +- Join Inner, (ss_sold_time_sk#17 = t_time_sk#44)
:  :  :           :     :- Project [ss_sold_time_sk#17, ss_store_sk#23]
:  :  :           :     :  +- Join Inner, (ss_hdemo_sk#21 = hd_demo_sk#39)
:  :  :           :     :     :- Project [ss_sold_time_sk#17, ss_hdemo_sk#21, ss_store_sk#23]
:  :  :           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#21) && isnotnull(ss_sold_time_sk#17)) && isnotnull(ss_store_sk#23))
:  :  :           :     :     :     +- Relation[ss_sold_date_sk#16,ss_sold_time_sk#17,ss_item_sk#18,ss_customer_sk#19,ss_cdemo_sk#20,ss_hdemo_sk#21,ss_addr_sk#22,ss_store_sk#23,ss_promo_sk#24,ss_ticket_number#25,ss_quantity#26,ss_wholesale_cost#27,ss_list_price#28,ss_sales_price#29,ss_ext_discount_amt#30,ss_ext_sales_price#31,ss_ext_wholesale_cost#32,ss_ext_list_price#33,ss_ext_tax#34,ss_coupon_amt#35,ss_net_paid#36,ss_net_paid_inc_tax#37,ss_net_profit#38] parquet
:  :  :           :     :     +- Project [hd_demo_sk#39]
:  :  :           :     :        +- Filter (((((hd_dep_count#42 = 4) && (hd_vehicle_count#43 <= 6)) || ((hd_dep_count#42 = 2) && (hd_vehicle_count#43 <= 4))) || ((hd_dep_count#42 = 0) && (hd_vehicle_count#43 <= 2))) && isnotnull(hd_demo_sk#39))
:  :  :           :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
:  :  :           :     +- Project [t_time_sk#44]
:  :  :           :        +- Filter (((isnotnull(t_hour#47) && isnotnull(t_minute#48)) && ((t_hour#47 = 10) && (t_minute#48 < 30))) && isnotnull(t_time_sk#44))
:  :  :           :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
:  :  :           +- Project [s_store_sk#54]
:  :  :              +- Filter ((isnotnull(s_store_name#59) && (s_store_name#59 = ese)) && isnotnull(s_store_sk#54))
:  :  :                 +- Relation[s_store_sk#54,s_store_id#55,s_rec_start_date#56,s_rec_end_date#57,s_closed_date_sk#58,s_store_name#59,s_number_employees#60,s_floor_space#61,s_hours#62,s_manager#63,s_market_id#64,s_geography_class#65,s_market_desc#66,s_market_manager#67,s_division_id#68,s_division_name#69,s_company_id#70,s_company_name#71,s_street_number#72,s_street_name#73,s_street_type#74,s_suite_number#75,s_city#76,s_county#77,... 5 more fields] parquet
:  :  +- Aggregate [count(1) AS h10_30_to_11#4L]
:  :     +- Project
:  :        +- Join Inner, (ss_store_sk#23 = s_store_sk#54)
:  :           :- Project [ss_store_sk#23]
:  :           :  +- Join Inner, (ss_sold_time_sk#17 = t_time_sk#44)
:  :           :     :- Project [ss_sold_time_sk#17, ss_store_sk#23]
:  :           :     :  +- Join Inner, (ss_hdemo_sk#21 = hd_demo_sk#39)
:  :           :     :     :- Project [ss_sold_time_sk#17, ss_hdemo_sk#21, ss_store_sk#23]
:  :           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#21) && isnotnull(ss_sold_time_sk#17)) && isnotnull(ss_store_sk#23))
:  :           :     :     :     +- Relation[ss_sold_date_sk#16,ss_sold_time_sk#17,ss_item_sk#18,ss_customer_sk#19,ss_cdemo_sk#20,ss_hdemo_sk#21,ss_addr_sk#22,ss_store_sk#23,ss_promo_sk#24,ss_ticket_number#25,ss_quantity#26,ss_wholesale_cost#27,ss_list_price#28,ss_sales_price#29,ss_ext_discount_amt#30,ss_ext_sales_price#31,ss_ext_wholesale_cost#32,ss_ext_list_price#33,ss_ext_tax#34,ss_coupon_amt#35,ss_net_paid#36,ss_net_paid_inc_tax#37,ss_net_profit#38] parquet
:  :           :     :     +- Project [hd_demo_sk#39]
:  :           :     :        +- Filter (((((hd_dep_count#42 = 4) && (hd_vehicle_count#43 <= 6)) || ((hd_dep_count#42 = 2) && (hd_vehicle_count#43 <= 4))) || ((hd_dep_count#42 = 0) && (hd_vehicle_count#43 <= 2))) && isnotnull(hd_demo_sk#39))
:  :           :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
:  :           :     +- Project [t_time_sk#44]
:  :           :        +- Filter (((isnotnull(t_hour#47) && isnotnull(t_minute#48)) && ((t_hour#47 = 10) && (t_minute#48 >= 30))) && isnotnull(t_time_sk#44))
:  :           :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
:  :           +- Project [s_store_sk#54]
:  :              +- Filter ((isnotnull(s_store_name#59) && (s_store_name#59 = ese)) && isnotnull(s_store_sk#54))
:  :                 +- Relation[s_store_sk#54,s_store_id#55,s_rec_start_date#56,s_rec_end_date#57,s_closed_date_sk#58,s_store_name#59,s_number_employees#60,s_floor_space#61,s_hours#62,s_manager#63,s_market_id#64,s_geography_class#65,s_market_desc#66,s_market_manager#67,s_division_id#68,s_division_name#69,s_company_id#70,s_company_name#71,s_street_number#72,s_street_name#73,s_street_type#74,s_suite_number#75,s_city#76,s_county#77,... 5 more fields] parquet
:  +- Aggregate [count(1) AS h11_to_11_30#5L]
:     +- Project
:        +- Join Inner, (ss_store_sk#23 = s_store_sk#54)
:           :- Project [ss_store_sk#23]
:           :  +- Join Inner, (ss_sold_time_sk#17 = t_time_sk#44)
:           :     :- Project [ss_sold_time_sk#17, ss_store_sk#23]
:           :     :  +- Join Inner, (ss_hdemo_sk#21 = hd_demo_sk#39)
:           :     :     :- Project [ss_sold_time_sk#17, ss_hdemo_sk#21, ss_store_sk#23]
:           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#21) && isnotnull(ss_sold_time_sk#17)) && isnotnull(ss_store_sk#23))
:           :     :     :     +- Relation[ss_sold_date_sk#16,ss_sold_time_sk#17,ss_item_sk#18,ss_customer_sk#19,ss_cdemo_sk#20,ss_hdemo_sk#21,ss_addr_sk#22,ss_store_sk#23,ss_promo_sk#24,ss_ticket_number#25,ss_quantity#26,ss_wholesale_cost#27,ss_list_price#28,ss_sales_price#29,ss_ext_discount_amt#30,ss_ext_sales_price#31,ss_ext_wholesale_cost#32,ss_ext_list_price#33,ss_ext_tax#34,ss_coupon_amt#35,ss_net_paid#36,ss_net_paid_inc_tax#37,ss_net_profit#38] parquet
:           :     :     +- Project [hd_demo_sk#39]
:           :     :        +- Filter (((((hd_dep_count#42 = 4) && (hd_vehicle_count#43 <= 6)) || ((hd_dep_count#42 = 2) && (hd_vehicle_count#43 <= 4))) || ((hd_dep_count#42 = 0) && (hd_vehicle_count#43 <= 2))) && isnotnull(hd_demo_sk#39))
:           :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
:           :     +- Project [t_time_sk#44]
:           :        +- Filter (((isnotnull(t_hour#47) && isnotnull(t_minute#48)) && ((t_hour#47 = 11) && (t_minute#48 < 30))) && isnotnull(t_time_sk#44))
:           :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
:           +- Project [s_store_sk#54]
:              +- Filter ((isnotnull(s_store_name#59) && (s_store_name#59 = ese)) && isnotnull(s_store_sk#54))
:                 +- Relation[s_store_sk#54,s_store_id#55,s_rec_start_date#56,s_rec_end_date#57,s_closed_date_sk#58,s_store_name#59,s_number_employees#60,s_floor_space#61,s_hours#62,s_manager#63,s_market_id#64,s_geography_class#65,s_market_desc#66,s_market_manager#67,s_division_id#68,s_division_name#69,s_company_id#70,s_company_name#71,s_street_number#72,s_street_name#73,s_street_type#74,s_suite_number#75,s_city#76,s_county#77,... 5 more fields] parquet
+- Aggregate [count(1) AS h11_30_to_12#6L]
   +- Project
      +- Join Inner, (ss_store_sk#23 = s_store_sk#54)
         :- Project [ss_store_sk#23]
         :  +- Join Inner, (ss_sold_time_sk#17 = t_time_sk#44)
         :     :- Project [ss_sold_time_sk#17, ss_store_sk#23]
         :     :  +- Join Inner, (ss_hdemo_sk#21 = hd_demo_sk#39)
         :     :     :- Project [ss_sold_time_sk#17, ss_hdemo_sk#21, ss_store_sk#23]
         :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#21) && isnotnull(ss_sold_time_sk#17)) && isnotnull(ss_store_sk#23))
         :     :     :     +- Relation[ss_sold_date_sk#16,ss_sold_time_sk#17,ss_item_sk#18,ss_customer_sk#19,ss_cdemo_sk#20,ss_hdemo_sk#21,ss_addr_sk#22,ss_store_sk#23,ss_promo_sk#24,ss_ticket_number#25,ss_quantity#26,ss_wholesale_cost#27,ss_list_price#28,ss_sales_price#29,ss_ext_discount_amt#30,ss_ext_sales_price#31,ss_ext_wholesale_cost#32,ss_ext_list_price#33,ss_ext_tax#34,ss_coupon_amt#35,ss_net_paid#36,ss_net_paid_inc_tax#37,ss_net_profit#38] parquet
         :     :     +- Project [hd_demo_sk#39]
         :     :        +- Filter (((((hd_dep_count#42 = 4) && (hd_vehicle_count#43 <= 6)) || ((hd_dep_count#42 = 2) && (hd_vehicle_count#43 <= 4))) || ((hd_dep_count#42 = 0) && (hd_vehicle_count#43 <= 2))) && isnotnull(hd_demo_sk#39))
         :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
         :     +- Project [t_time_sk#44]
         :        +- Filter (((isnotnull(t_hour#47) && isnotnull(t_minute#48)) && ((t_hour#47 = 11) && (t_minute#48 >= 30))) && isnotnull(t_time_sk#44))
         :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
         +- Project [s_store_sk#54]
            +- Filter ((isnotnull(s_store_name#59) && (s_store_name#59 = ese)) && isnotnull(s_store_sk#54))
               +- Relation[s_store_sk#54,s_store_id#55,s_rec_start_date#56,s_rec_end_date#57,s_closed_date_sk#58,s_store_name#59,s_number_employees#60,s_floor_space#61,s_hours#62,s_manager#63,s_market_id#64,s_geography_class#65,s_market_desc#66,s_market_manager#67,s_division_id#68,s_division_name#69,s_company_id#70,s_company_name#71,s_street_number#72,s_street_name#73,s_street_type#74,s_suite_number#75,s_city#76,s_county#77,... 5 more fields] parquet
and
Aggregate [count(1) AS h12_to_12_30#7L]
+- Project
   +- Join Inner, (ss_store_sk#23 = s_store_sk#54)
      :- Project [ss_store_sk#23]
      :  +- Join Inner, (ss_sold_time_sk#17 = t_time_sk#44)
      :     :- Project [ss_sold_time_sk#17, ss_store_sk#23]
      :     :  +- Join Inner, (ss_hdemo_sk#21 = hd_demo_sk#39)
      :     :     :- Project [ss_sold_time_sk#17, ss_hdemo_sk#21, ss_store_sk#23]
      :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#21) && isnotnull(ss_sold_time_sk#17)) && isnotnull(ss_store_sk#23))
      :     :     :     +- Relation[ss_sold_date_sk#16,ss_sold_time_sk#17,ss_item_sk#18,ss_customer_sk#19,ss_cdemo_sk#20,ss_hdemo_sk#21,ss_addr_sk#22,ss_store_sk#23,ss_promo_sk#24,ss_ticket_number#25,ss_quantity#26,ss_wholesale_cost#27,ss_list_price#28,ss_sales_price#29,ss_ext_discount_amt#30,ss_ext_sales_price#31,ss_ext_wholesale_cost#32,ss_ext_list_price#33,ss_ext_tax#34,ss_coupon_amt#35,ss_net_paid#36,ss_net_paid_inc_tax#37,ss_net_profit#38] parquet
      :     :     +- Project [hd_demo_sk#39]
      :     :        +- Filter (((((hd_dep_count#42 = 4) && (hd_vehicle_count#43 <= 6)) || ((hd_dep_count#42 = 2) && (hd_vehicle_count#43 <= 4))) || ((hd_dep_count#42 = 0) && (hd_vehicle_count#43 <= 2))) && isnotnull(hd_demo_sk#39))
      :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
      :     +- Project [t_time_sk#44]
      :        +- Filter (((isnotnull(t_hour#47) && isnotnull(t_minute#48)) && ((t_hour#47 = 12) && (t_minute#48 < 30))) && isnotnull(t_time_sk#44))
      :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
      +- Project [s_store_sk#54]
         +- Filter ((isnotnull(s_store_name#59) && (s_store_name#59 = ese)) && isnotnull(s_store_sk#54))
            +- Relation[s_store_sk#54,s_store_id#55,s_rec_start_date#56,s_rec_end_date#57,s_closed_date_sk#58,s_store_name#59,s_number_employees#60,s_floor_space#61,s_hours#62,s_manager#63,s_market_id#64,s_geography_class#65,s_market_desc#66,s_market_manager#67,s_division_id#68,s_division_name#69,s_company_id#70,s_company_name#71,s_street_number#72,s_street_name#73,s_street_type#74,s_suite_number#75,s_city#76,s_county#77,... 5 more fields] parquet
Join condition is missing or trivial.
Either: use the CROSS JOIN syntax to allow cartesian products between these
relations, or: enable implicit cartesian products by setting the configuration
variable spark.sql.crossJoin.enabled=true;
