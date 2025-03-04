Error in query: Detected implicit cartesian product for INNER join between logical plans
Aggregate [count(1) AS amc#0L]
+- Project
   +- Join Inner, (ws_web_page_sk#17 = wp_web_page_sk#54)
      :- Project [ws_web_page_sk#17]
      :  +- Join Inner, (ws_sold_time_sk#6 = t_time_sk#44)
      :     :- Project [ws_sold_time_sk#6, ws_web_page_sk#17]
      :     :  +- Join Inner, (ws_ship_hdemo_sk#15 = hd_demo_sk#39)
      :     :     :- Project [ws_sold_time_sk#6, ws_ship_hdemo_sk#15, ws_web_page_sk#17]
      :     :     :  +- Filter ((isnotnull(ws_ship_hdemo_sk#15) && isnotnull(ws_sold_time_sk#6)) && isnotnull(ws_web_page_sk#17))
      :     :     :     +- Relation[ws_sold_date_sk#5,ws_sold_time_sk#6,ws_ship_date_sk#7,ws_item_sk#8,ws_bill_customer_sk#9,ws_bill_cdemo_sk#10,ws_bill_hdemo_sk#11,ws_bill_addr_sk#12,ws_ship_customer_sk#13,ws_ship_cdemo_sk#14,ws_ship_hdemo_sk#15,ws_ship_addr_sk#16,ws_web_page_sk#17,ws_web_site_sk#18,ws_ship_mode_sk#19,ws_warehouse_sk#20,ws_promo_sk#21,ws_order_number#22,ws_quantity#23,ws_wholesale_cost#24,ws_list_price#25,ws_sales_price#26,ws_ext_discount_amt#27,ws_ext_sales_price#28,... 10 more fields] parquet
      :     :     +- Project [hd_demo_sk#39]
      :     :        +- Filter ((isnotnull(hd_dep_count#42) && (hd_dep_count#42 = 6)) && isnotnull(hd_demo_sk#39))
      :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
      :     +- Project [t_time_sk#44]
      :        +- Filter ((isnotnull(t_hour#47) && ((t_hour#47 >= 8) && (t_hour#47 <= 9))) && isnotnull(t_time_sk#44))
      :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
      +- Project [wp_web_page_sk#54]
         +- Filter ((isnotnull(wp_char_count#64) && ((wp_char_count#64 >= 5000) && (wp_char_count#64 <= 5200))) && isnotnull(wp_web_page_sk#54))
            +- Relation[wp_web_page_sk#54,wp_web_page_id#55,wp_rec_start_date#56,wp_rec_end_date#57,wp_creation_date_sk#58,wp_access_date_sk#59,wp_autogen_flag#60,wp_customer_sk#61,wp_url#62,wp_type#63,wp_char_count#64,wp_link_count#65,wp_image_count#66,wp_max_ad_count#67] parquet
and
Aggregate [count(1) AS pmc#1L]
+- Project
   +- Join Inner, (ws_web_page_sk#17 = wp_web_page_sk#54)
      :- Project [ws_web_page_sk#17]
      :  +- Join Inner, (ws_sold_time_sk#6 = t_time_sk#44)
      :     :- Project [ws_sold_time_sk#6, ws_web_page_sk#17]
      :     :  +- Join Inner, (ws_ship_hdemo_sk#15 = hd_demo_sk#39)
      :     :     :- Project [ws_sold_time_sk#6, ws_ship_hdemo_sk#15, ws_web_page_sk#17]
      :     :     :  +- Filter ((isnotnull(ws_ship_hdemo_sk#15) && isnotnull(ws_sold_time_sk#6)) && isnotnull(ws_web_page_sk#17))
      :     :     :     +- Relation[ws_sold_date_sk#5,ws_sold_time_sk#6,ws_ship_date_sk#7,ws_item_sk#8,ws_bill_customer_sk#9,ws_bill_cdemo_sk#10,ws_bill_hdemo_sk#11,ws_bill_addr_sk#12,ws_ship_customer_sk#13,ws_ship_cdemo_sk#14,ws_ship_hdemo_sk#15,ws_ship_addr_sk#16,ws_web_page_sk#17,ws_web_site_sk#18,ws_ship_mode_sk#19,ws_warehouse_sk#20,ws_promo_sk#21,ws_order_number#22,ws_quantity#23,ws_wholesale_cost#24,ws_list_price#25,ws_sales_price#26,ws_ext_discount_amt#27,ws_ext_sales_price#28,... 10 more fields] parquet
      :     :     +- Project [hd_demo_sk#39]
      :     :        +- Filter ((isnotnull(hd_dep_count#42) && (hd_dep_count#42 = 6)) && isnotnull(hd_demo_sk#39))
      :     :           +- Relation[hd_demo_sk#39,hd_income_band_sk#40,hd_buy_potential#41,hd_dep_count#42,hd_vehicle_count#43] parquet
      :     +- Project [t_time_sk#44]
      :        +- Filter ((isnotnull(t_hour#47) && ((t_hour#47 >= 19) && (t_hour#47 <= 20))) && isnotnull(t_time_sk#44))
      :           +- Relation[t_time_sk#44,t_time_id#45,t_time#46,t_hour#47,t_minute#48,t_second#49,t_am_pm#50,t_shift#51,t_sub_shift#52,t_meal_time#53] parquet
      +- Project [wp_web_page_sk#54]
         +- Filter ((isnotnull(wp_char_count#64) && ((wp_char_count#64 >= 5000) && (wp_char_count#64 <= 5200))) && isnotnull(wp_web_page_sk#54))
            +- Relation[wp_web_page_sk#54,wp_web_page_id#55,wp_rec_start_date#56,wp_rec_end_date#57,wp_creation_date_sk#58,wp_access_date_sk#59,wp_autogen_flag#60,wp_customer_sk#61,wp_url#62,wp_type#63,wp_char_count#64,wp_link_count#65,wp_image_count#66,wp_max_ad_count#67] parquet
Join condition is missing or trivial.
Either: use the CROSS JOIN syntax to allow cartesian products between these
relations, or: enable implicit cartesian products by setting the configuration
variable spark.sql.crossJoin.enabled=true;
