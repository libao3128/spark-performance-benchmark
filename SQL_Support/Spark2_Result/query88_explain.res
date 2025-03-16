== Parsed Logical Plan ==
'Project [*]
+- 'Join Inner
   :- 'Join Inner
   :  :- 'Join Inner
   :  :  :- 'Join Inner
   :  :  :  :- 'Join Inner
   :  :  :  :  :- 'Join Inner
   :  :  :  :  :  :- 'Join Inner
   :  :  :  :  :  :  :- 'SubqueryAlias `s1`
   :  :  :  :  :  :  :  +- 'Project ['count(1) AS h8_30_to_9#0]
   :  :  :  :  :  :  :     +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) && ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (('ss_store_sk = 's_store_sk) && ('time_dim.t_hour = 8))) && ((('time_dim.t_minute >= 30) && (((('household_demographics.hd_dep_count = 4) && ('household_demographics.hd_vehicle_count <= (4 + 2))) || (('household_demographics.hd_dep_count = 2) && ('household_demographics.hd_vehicle_count <= (2 + 2)))) || (('household_demographics.hd_dep_count = 0) && ('household_demographics.hd_vehicle_count <= (0 + 2))))) && ('store.s_store_name = ese)))
   :  :  :  :  :  :  :        +- 'Join Inner
   :  :  :  :  :  :  :           :- 'Join Inner
   :  :  :  :  :  :  :           :  :- 'Join Inner
   :  :  :  :  :  :  :           :  :  :- 'UnresolvedRelation `store_sales`
   :  :  :  :  :  :  :           :  :  +- 'UnresolvedRelation `household_demographics`
   :  :  :  :  :  :  :           :  +- 'UnresolvedRelation `time_dim`
   :  :  :  :  :  :  :           +- 'UnresolvedRelation `store`
   :  :  :  :  :  :  +- 'SubqueryAlias `s2`
   :  :  :  :  :  :     +- 'Project ['count(1) AS h9_to_9_30#1]
   :  :  :  :  :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) && ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (('ss_store_sk = 's_store_sk) && ('time_dim.t_hour = 9))) && ((('time_dim.t_minute < 30) && (((('household_demographics.hd_dep_count = 4) && ('household_demographics.hd_vehicle_count <= (4 + 2))) || (('household_demographics.hd_dep_count = 2) && ('household_demographics.hd_vehicle_count <= (2 + 2)))) || (('household_demographics.hd_dep_count = 0) && ('household_demographics.hd_vehicle_count <= (0 + 2))))) && ('store.s_store_name = ese)))
   :  :  :  :  :  :           +- 'Join Inner
   :  :  :  :  :  :              :- 'Join Inner
   :  :  :  :  :  :              :  :- 'Join Inner
   :  :  :  :  :  :              :  :  :- 'UnresolvedRelation `store_sales`
   :  :  :  :  :  :              :  :  +- 'UnresolvedRelation `household_demographics`
   :  :  :  :  :  :              :  +- 'UnresolvedRelation `time_dim`
   :  :  :  :  :  :              +- 'UnresolvedRelation `store`
   :  :  :  :  :  +- 'SubqueryAlias `s3`
   :  :  :  :  :     +- 'Project ['count(1) AS h9_30_to_10#2]
   :  :  :  :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) && ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (('ss_store_sk = 's_store_sk) && ('time_dim.t_hour = 9))) && ((('time_dim.t_minute >= 30) && (((('household_demographics.hd_dep_count = 4) && ('household_demographics.hd_vehicle_count <= (4 + 2))) || (('household_demographics.hd_dep_count = 2) && ('household_demographics.hd_vehicle_count <= (2 + 2)))) || (('household_demographics.hd_dep_count = 0) && ('household_demographics.hd_vehicle_count <= (0 + 2))))) && ('store.s_store_name = ese)))
   :  :  :  :  :           +- 'Join Inner
   :  :  :  :  :              :- 'Join Inner
   :  :  :  :  :              :  :- 'Join Inner
   :  :  :  :  :              :  :  :- 'UnresolvedRelation `store_sales`
   :  :  :  :  :              :  :  +- 'UnresolvedRelation `household_demographics`
   :  :  :  :  :              :  +- 'UnresolvedRelation `time_dim`
   :  :  :  :  :              +- 'UnresolvedRelation `store`
   :  :  :  :  +- 'SubqueryAlias `s4`
   :  :  :  :     +- 'Project ['count(1) AS h10_to_10_30#3]
   :  :  :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) && ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (('ss_store_sk = 's_store_sk) && ('time_dim.t_hour = 10))) && ((('time_dim.t_minute < 30) && (((('household_demographics.hd_dep_count = 4) && ('household_demographics.hd_vehicle_count <= (4 + 2))) || (('household_demographics.hd_dep_count = 2) && ('household_demographics.hd_vehicle_count <= (2 + 2)))) || (('household_demographics.hd_dep_count = 0) && ('household_demographics.hd_vehicle_count <= (0 + 2))))) && ('store.s_store_name = ese)))
   :  :  :  :           +- 'Join Inner
   :  :  :  :              :- 'Join Inner
   :  :  :  :              :  :- 'Join Inner
   :  :  :  :              :  :  :- 'UnresolvedRelation `store_sales`
   :  :  :  :              :  :  +- 'UnresolvedRelation `household_demographics`
   :  :  :  :              :  +- 'UnresolvedRelation `time_dim`
   :  :  :  :              +- 'UnresolvedRelation `store`
   :  :  :  +- 'SubqueryAlias `s5`
   :  :  :     +- 'Project ['count(1) AS h10_30_to_11#4]
   :  :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) && ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (('ss_store_sk = 's_store_sk) && ('time_dim.t_hour = 10))) && ((('time_dim.t_minute >= 30) && (((('household_demographics.hd_dep_count = 4) && ('household_demographics.hd_vehicle_count <= (4 + 2))) || (('household_demographics.hd_dep_count = 2) && ('household_demographics.hd_vehicle_count <= (2 + 2)))) || (('household_demographics.hd_dep_count = 0) && ('household_demographics.hd_vehicle_count <= (0 + 2))))) && ('store.s_store_name = ese)))
   :  :  :           +- 'Join Inner
   :  :  :              :- 'Join Inner
   :  :  :              :  :- 'Join Inner
   :  :  :              :  :  :- 'UnresolvedRelation `store_sales`
   :  :  :              :  :  +- 'UnresolvedRelation `household_demographics`
   :  :  :              :  +- 'UnresolvedRelation `time_dim`
   :  :  :              +- 'UnresolvedRelation `store`
   :  :  +- 'SubqueryAlias `s6`
   :  :     +- 'Project ['count(1) AS h11_to_11_30#5]
   :  :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) && ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (('ss_store_sk = 's_store_sk) && ('time_dim.t_hour = 11))) && ((('time_dim.t_minute < 30) && (((('household_demographics.hd_dep_count = 4) && ('household_demographics.hd_vehicle_count <= (4 + 2))) || (('household_demographics.hd_dep_count = 2) && ('household_demographics.hd_vehicle_count <= (2 + 2)))) || (('household_demographics.hd_dep_count = 0) && ('household_demographics.hd_vehicle_count <= (0 + 2))))) && ('store.s_store_name = ese)))
   :  :           +- 'Join Inner
   :  :              :- 'Join Inner
   :  :              :  :- 'Join Inner
   :  :              :  :  :- 'UnresolvedRelation `store_sales`
   :  :              :  :  +- 'UnresolvedRelation `household_demographics`
   :  :              :  +- 'UnresolvedRelation `time_dim`
   :  :              +- 'UnresolvedRelation `store`
   :  +- 'SubqueryAlias `s7`
   :     +- 'Project ['count(1) AS h11_30_to_12#6]
   :        +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) && ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (('ss_store_sk = 's_store_sk) && ('time_dim.t_hour = 11))) && ((('time_dim.t_minute >= 30) && (((('household_demographics.hd_dep_count = 4) && ('household_demographics.hd_vehicle_count <= (4 + 2))) || (('household_demographics.hd_dep_count = 2) && ('household_demographics.hd_vehicle_count <= (2 + 2)))) || (('household_demographics.hd_dep_count = 0) && ('household_demographics.hd_vehicle_count <= (0 + 2))))) && ('store.s_store_name = ese)))
   :           +- 'Join Inner
   :              :- 'Join Inner
   :              :  :- 'Join Inner
   :              :  :  :- 'UnresolvedRelation `store_sales`
   :              :  :  +- 'UnresolvedRelation `household_demographics`
   :              :  +- 'UnresolvedRelation `time_dim`
   :              +- 'UnresolvedRelation `store`
   +- 'SubqueryAlias `s8`
      +- 'Project ['count(1) AS h12_to_12_30#7]
         +- 'Filter (((('ss_sold_time_sk = 'time_dim.t_time_sk) && ('ss_hdemo_sk = 'household_demographics.hd_demo_sk)) && (('ss_store_sk = 's_store_sk) && ('time_dim.t_hour = 12))) && ((('time_dim.t_minute < 30) && (((('household_demographics.hd_dep_count = 4) && ('household_demographics.hd_vehicle_count <= (4 + 2))) || (('household_demographics.hd_dep_count = 2) && ('household_demographics.hd_vehicle_count <= (2 + 2)))) || (('household_demographics.hd_dep_count = 0) && ('household_demographics.hd_vehicle_count <= (0 + 2))))) && ('store.s_store_name = ese)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation `store_sales`
               :  :  +- 'UnresolvedRelation `household_demographics`
               :  +- 'UnresolvedRelation `time_dim`
               +- 'UnresolvedRelation `store`

== Analyzed Logical Plan ==
h8_30_to_9: bigint, h9_to_9_30: bigint, h9_30_to_10: bigint, h10_to_10_30: bigint, h10_30_to_11: bigint, h11_to_11_30: bigint, h11_30_to_12: bigint, h12_to_12_30: bigint
Project [h8_30_to_9#0L, h9_to_9_30#1L, h9_30_to_10#2L, h10_to_10_30#3L, h10_30_to_11#4L, h11_to_11_30#5L, h11_30_to_12#6L, h12_to_12_30#7L]
+- Join Inner
   :- Join Inner
   :  :- Join Inner
   :  :  :- Join Inner
   :  :  :  :- Join Inner
   :  :  :  :  :- Join Inner
   :  :  :  :  :  :- Join Inner
   :  :  :  :  :  :  :- SubqueryAlias `s1`
   :  :  :  :  :  :  :  +- Aggregate [count(1) AS h8_30_to_9#0L]
   :  :  :  :  :  :  :     +- Filter ((((ss_sold_time_sk#19 = t_time_sk#46) && (ss_hdemo_sk#23 = hd_demo_sk#41)) && ((ss_store_sk#25 = s_store_sk#56) && (t_hour#49 = 8))) && (((t_minute#50 >= 30) && ((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= (4 + 2))) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= (2 + 2)))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= (0 + 2))))) && (s_store_name#61 = ese)))
   :  :  :  :  :  :  :        +- Join Inner
   :  :  :  :  :  :  :           :- Join Inner
   :  :  :  :  :  :  :           :  :- Join Inner
   :  :  :  :  :  :  :           :  :  :- SubqueryAlias `tpcds`.`store_sales`
   :  :  :  :  :  :  :           :  :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
   :  :  :  :  :  :  :           :  :  +- SubqueryAlias `tpcds`.`household_demographics`
   :  :  :  :  :  :  :           :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
   :  :  :  :  :  :  :           :  +- SubqueryAlias `tpcds`.`time_dim`
   :  :  :  :  :  :  :           :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
   :  :  :  :  :  :  :           +- SubqueryAlias `tpcds`.`store`
   :  :  :  :  :  :  :              +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
   :  :  :  :  :  :  +- SubqueryAlias `s2`
   :  :  :  :  :  :     +- Aggregate [count(1) AS h9_to_9_30#1L]
   :  :  :  :  :  :        +- Filter ((((ss_sold_time_sk#19 = t_time_sk#46) && (ss_hdemo_sk#23 = hd_demo_sk#41)) && ((ss_store_sk#25 = s_store_sk#56) && (t_hour#49 = 9))) && (((t_minute#50 < 30) && ((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= (4 + 2))) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= (2 + 2)))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= (0 + 2))))) && (s_store_name#61 = ese)))
   :  :  :  :  :  :           +- Join Inner
   :  :  :  :  :  :              :- Join Inner
   :  :  :  :  :  :              :  :- Join Inner
   :  :  :  :  :  :              :  :  :- SubqueryAlias `tpcds`.`store_sales`
   :  :  :  :  :  :              :  :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
   :  :  :  :  :  :              :  :  +- SubqueryAlias `tpcds`.`household_demographics`
   :  :  :  :  :  :              :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
   :  :  :  :  :  :              :  +- SubqueryAlias `tpcds`.`time_dim`
   :  :  :  :  :  :              :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
   :  :  :  :  :  :              +- SubqueryAlias `tpcds`.`store`
   :  :  :  :  :  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
   :  :  :  :  :  +- SubqueryAlias `s3`
   :  :  :  :  :     +- Aggregate [count(1) AS h9_30_to_10#2L]
   :  :  :  :  :        +- Filter ((((ss_sold_time_sk#19 = t_time_sk#46) && (ss_hdemo_sk#23 = hd_demo_sk#41)) && ((ss_store_sk#25 = s_store_sk#56) && (t_hour#49 = 9))) && (((t_minute#50 >= 30) && ((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= (4 + 2))) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= (2 + 2)))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= (0 + 2))))) && (s_store_name#61 = ese)))
   :  :  :  :  :           +- Join Inner
   :  :  :  :  :              :- Join Inner
   :  :  :  :  :              :  :- Join Inner
   :  :  :  :  :              :  :  :- SubqueryAlias `tpcds`.`store_sales`
   :  :  :  :  :              :  :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
   :  :  :  :  :              :  :  +- SubqueryAlias `tpcds`.`household_demographics`
   :  :  :  :  :              :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
   :  :  :  :  :              :  +- SubqueryAlias `tpcds`.`time_dim`
   :  :  :  :  :              :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
   :  :  :  :  :              +- SubqueryAlias `tpcds`.`store`
   :  :  :  :  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
   :  :  :  :  +- SubqueryAlias `s4`
   :  :  :  :     +- Aggregate [count(1) AS h10_to_10_30#3L]
   :  :  :  :        +- Filter ((((ss_sold_time_sk#19 = t_time_sk#46) && (ss_hdemo_sk#23 = hd_demo_sk#41)) && ((ss_store_sk#25 = s_store_sk#56) && (t_hour#49 = 10))) && (((t_minute#50 < 30) && ((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= (4 + 2))) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= (2 + 2)))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= (0 + 2))))) && (s_store_name#61 = ese)))
   :  :  :  :           +- Join Inner
   :  :  :  :              :- Join Inner
   :  :  :  :              :  :- Join Inner
   :  :  :  :              :  :  :- SubqueryAlias `tpcds`.`store_sales`
   :  :  :  :              :  :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
   :  :  :  :              :  :  +- SubqueryAlias `tpcds`.`household_demographics`
   :  :  :  :              :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
   :  :  :  :              :  +- SubqueryAlias `tpcds`.`time_dim`
   :  :  :  :              :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
   :  :  :  :              +- SubqueryAlias `tpcds`.`store`
   :  :  :  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
   :  :  :  +- SubqueryAlias `s5`
   :  :  :     +- Aggregate [count(1) AS h10_30_to_11#4L]
   :  :  :        +- Filter ((((ss_sold_time_sk#19 = t_time_sk#46) && (ss_hdemo_sk#23 = hd_demo_sk#41)) && ((ss_store_sk#25 = s_store_sk#56) && (t_hour#49 = 10))) && (((t_minute#50 >= 30) && ((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= (4 + 2))) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= (2 + 2)))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= (0 + 2))))) && (s_store_name#61 = ese)))
   :  :  :           +- Join Inner
   :  :  :              :- Join Inner
   :  :  :              :  :- Join Inner
   :  :  :              :  :  :- SubqueryAlias `tpcds`.`store_sales`
   :  :  :              :  :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
   :  :  :              :  :  +- SubqueryAlias `tpcds`.`household_demographics`
   :  :  :              :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
   :  :  :              :  +- SubqueryAlias `tpcds`.`time_dim`
   :  :  :              :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
   :  :  :              +- SubqueryAlias `tpcds`.`store`
   :  :  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
   :  :  +- SubqueryAlias `s6`
   :  :     +- Aggregate [count(1) AS h11_to_11_30#5L]
   :  :        +- Filter ((((ss_sold_time_sk#19 = t_time_sk#46) && (ss_hdemo_sk#23 = hd_demo_sk#41)) && ((ss_store_sk#25 = s_store_sk#56) && (t_hour#49 = 11))) && (((t_minute#50 < 30) && ((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= (4 + 2))) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= (2 + 2)))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= (0 + 2))))) && (s_store_name#61 = ese)))
   :  :           +- Join Inner
   :  :              :- Join Inner
   :  :              :  :- Join Inner
   :  :              :  :  :- SubqueryAlias `tpcds`.`store_sales`
   :  :              :  :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
   :  :              :  :  +- SubqueryAlias `tpcds`.`household_demographics`
   :  :              :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
   :  :              :  +- SubqueryAlias `tpcds`.`time_dim`
   :  :              :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
   :  :              +- SubqueryAlias `tpcds`.`store`
   :  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
   :  +- SubqueryAlias `s7`
   :     +- Aggregate [count(1) AS h11_30_to_12#6L]
   :        +- Filter ((((ss_sold_time_sk#19 = t_time_sk#46) && (ss_hdemo_sk#23 = hd_demo_sk#41)) && ((ss_store_sk#25 = s_store_sk#56) && (t_hour#49 = 11))) && (((t_minute#50 >= 30) && ((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= (4 + 2))) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= (2 + 2)))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= (0 + 2))))) && (s_store_name#61 = ese)))
   :           +- Join Inner
   :              :- Join Inner
   :              :  :- Join Inner
   :              :  :  :- SubqueryAlias `tpcds`.`store_sales`
   :              :  :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
   :              :  :  +- SubqueryAlias `tpcds`.`household_demographics`
   :              :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
   :              :  +- SubqueryAlias `tpcds`.`time_dim`
   :              :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
   :              +- SubqueryAlias `tpcds`.`store`
   :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
   +- SubqueryAlias `s8`
      +- Aggregate [count(1) AS h12_to_12_30#7L]
         +- Filter ((((ss_sold_time_sk#19 = t_time_sk#46) && (ss_hdemo_sk#23 = hd_demo_sk#41)) && ((ss_store_sk#25 = s_store_sk#56) && (t_hour#49 = 12))) && (((t_minute#50 < 30) && ((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= (4 + 2))) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= (2 + 2)))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= (0 + 2))))) && (s_store_name#61 = ese)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias `tpcds`.`store_sales`
               :  :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
               :  :  +- SubqueryAlias `tpcds`.`household_demographics`
               :  :     +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
               :  +- SubqueryAlias `tpcds`.`time_dim`
               :     +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
               +- SubqueryAlias `tpcds`.`store`
                  +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet

== Optimized Logical Plan ==
Join Inner
:- Join Inner
:  :- Join Inner
:  :  :- Join Inner
:  :  :  :- Join Inner
:  :  :  :  :- Join Inner
:  :  :  :  :  :- Join Inner
:  :  :  :  :  :  :- Aggregate [count(1) AS h8_30_to_9#0L]
:  :  :  :  :  :  :  +- Project
:  :  :  :  :  :  :     +- Join Inner, (ss_store_sk#25 = s_store_sk#56)
:  :  :  :  :  :  :        :- Project [ss_store_sk#25]
:  :  :  :  :  :  :        :  +- Join Inner, (ss_sold_time_sk#19 = t_time_sk#46)
:  :  :  :  :  :  :        :     :- Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :  :  :  :  :        :     :  +- Join Inner, (ss_hdemo_sk#23 = hd_demo_sk#41)
:  :  :  :  :  :  :        :     :     :- Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :  :  :  :  :        :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :  :  :  :  :        :     :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:  :  :  :  :  :  :        :     :     +- Project [hd_demo_sk#41]
:  :  :  :  :  :  :        :     :        +- Filter (((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= 6)) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= 4))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= 2))) && isnotnull(hd_demo_sk#41))
:  :  :  :  :  :  :        :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
:  :  :  :  :  :  :        :     +- Project [t_time_sk#46]
:  :  :  :  :  :  :        :        +- Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 8)) && (t_minute#50 >= 30)) && isnotnull(t_time_sk#46))
:  :  :  :  :  :  :        :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
:  :  :  :  :  :  :        +- Project [s_store_sk#56]
:  :  :  :  :  :  :           +- Filter ((isnotnull(s_store_name#61) && (s_store_name#61 = ese)) && isnotnull(s_store_sk#56))
:  :  :  :  :  :  :              +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
:  :  :  :  :  :  +- Aggregate [count(1) AS h9_to_9_30#1L]
:  :  :  :  :  :     +- Project
:  :  :  :  :  :        +- Join Inner, (ss_store_sk#25 = s_store_sk#56)
:  :  :  :  :  :           :- Project [ss_store_sk#25]
:  :  :  :  :  :           :  +- Join Inner, (ss_sold_time_sk#19 = t_time_sk#46)
:  :  :  :  :  :           :     :- Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :  :  :  :           :     :  +- Join Inner, (ss_hdemo_sk#23 = hd_demo_sk#41)
:  :  :  :  :  :           :     :     :- Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :  :  :  :           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :  :  :  :           :     :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:  :  :  :  :  :           :     :     +- Project [hd_demo_sk#41]
:  :  :  :  :  :           :     :        +- Filter (((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= 6)) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= 4))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= 2))) && isnotnull(hd_demo_sk#41))
:  :  :  :  :  :           :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
:  :  :  :  :  :           :     +- Project [t_time_sk#46]
:  :  :  :  :  :           :        +- Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 9)) && (t_minute#50 < 30)) && isnotnull(t_time_sk#46))
:  :  :  :  :  :           :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
:  :  :  :  :  :           +- Project [s_store_sk#56]
:  :  :  :  :  :              +- Filter ((isnotnull(s_store_name#61) && (s_store_name#61 = ese)) && isnotnull(s_store_sk#56))
:  :  :  :  :  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
:  :  :  :  :  +- Aggregate [count(1) AS h9_30_to_10#2L]
:  :  :  :  :     +- Project
:  :  :  :  :        +- Join Inner, (ss_store_sk#25 = s_store_sk#56)
:  :  :  :  :           :- Project [ss_store_sk#25]
:  :  :  :  :           :  +- Join Inner, (ss_sold_time_sk#19 = t_time_sk#46)
:  :  :  :  :           :     :- Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :  :  :           :     :  +- Join Inner, (ss_hdemo_sk#23 = hd_demo_sk#41)
:  :  :  :  :           :     :     :- Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :  :  :           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :  :  :           :     :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:  :  :  :  :           :     :     +- Project [hd_demo_sk#41]
:  :  :  :  :           :     :        +- Filter (((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= 6)) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= 4))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= 2))) && isnotnull(hd_demo_sk#41))
:  :  :  :  :           :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
:  :  :  :  :           :     +- Project [t_time_sk#46]
:  :  :  :  :           :        +- Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 9)) && (t_minute#50 >= 30)) && isnotnull(t_time_sk#46))
:  :  :  :  :           :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
:  :  :  :  :           +- Project [s_store_sk#56]
:  :  :  :  :              +- Filter ((isnotnull(s_store_name#61) && (s_store_name#61 = ese)) && isnotnull(s_store_sk#56))
:  :  :  :  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
:  :  :  :  +- Aggregate [count(1) AS h10_to_10_30#3L]
:  :  :  :     +- Project
:  :  :  :        +- Join Inner, (ss_store_sk#25 = s_store_sk#56)
:  :  :  :           :- Project [ss_store_sk#25]
:  :  :  :           :  +- Join Inner, (ss_sold_time_sk#19 = t_time_sk#46)
:  :  :  :           :     :- Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :  :           :     :  +- Join Inner, (ss_hdemo_sk#23 = hd_demo_sk#41)
:  :  :  :           :     :     :- Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :  :           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :  :           :     :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:  :  :  :           :     :     +- Project [hd_demo_sk#41]
:  :  :  :           :     :        +- Filter (((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= 6)) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= 4))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= 2))) && isnotnull(hd_demo_sk#41))
:  :  :  :           :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
:  :  :  :           :     +- Project [t_time_sk#46]
:  :  :  :           :        +- Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 10)) && (t_minute#50 < 30)) && isnotnull(t_time_sk#46))
:  :  :  :           :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
:  :  :  :           +- Project [s_store_sk#56]
:  :  :  :              +- Filter ((isnotnull(s_store_name#61) && (s_store_name#61 = ese)) && isnotnull(s_store_sk#56))
:  :  :  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
:  :  :  +- Aggregate [count(1) AS h10_30_to_11#4L]
:  :  :     +- Project
:  :  :        +- Join Inner, (ss_store_sk#25 = s_store_sk#56)
:  :  :           :- Project [ss_store_sk#25]
:  :  :           :  +- Join Inner, (ss_sold_time_sk#19 = t_time_sk#46)
:  :  :           :     :- Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :           :     :  +- Join Inner, (ss_hdemo_sk#23 = hd_demo_sk#41)
:  :  :           :     :     :- Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :           :     :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:  :  :           :     :     +- Project [hd_demo_sk#41]
:  :  :           :     :        +- Filter (((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= 6)) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= 4))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= 2))) && isnotnull(hd_demo_sk#41))
:  :  :           :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
:  :  :           :     +- Project [t_time_sk#46]
:  :  :           :        +- Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 10)) && (t_minute#50 >= 30)) && isnotnull(t_time_sk#46))
:  :  :           :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
:  :  :           +- Project [s_store_sk#56]
:  :  :              +- Filter ((isnotnull(s_store_name#61) && (s_store_name#61 = ese)) && isnotnull(s_store_sk#56))
:  :  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
:  :  +- Aggregate [count(1) AS h11_to_11_30#5L]
:  :     +- Project
:  :        +- Join Inner, (ss_store_sk#25 = s_store_sk#56)
:  :           :- Project [ss_store_sk#25]
:  :           :  +- Join Inner, (ss_sold_time_sk#19 = t_time_sk#46)
:  :           :     :- Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :           :     :  +- Join Inner, (ss_hdemo_sk#23 = hd_demo_sk#41)
:  :           :     :     :- Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :           :     :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:  :           :     :     +- Project [hd_demo_sk#41]
:  :           :     :        +- Filter (((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= 6)) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= 4))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= 2))) && isnotnull(hd_demo_sk#41))
:  :           :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
:  :           :     +- Project [t_time_sk#46]
:  :           :        +- Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 11)) && (t_minute#50 < 30)) && isnotnull(t_time_sk#46))
:  :           :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
:  :           +- Project [s_store_sk#56]
:  :              +- Filter ((isnotnull(s_store_name#61) && (s_store_name#61 = ese)) && isnotnull(s_store_sk#56))
:  :                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
:  +- Aggregate [count(1) AS h11_30_to_12#6L]
:     +- Project
:        +- Join Inner, (ss_store_sk#25 = s_store_sk#56)
:           :- Project [ss_store_sk#25]
:           :  +- Join Inner, (ss_sold_time_sk#19 = t_time_sk#46)
:           :     :- Project [ss_sold_time_sk#19, ss_store_sk#25]
:           :     :  +- Join Inner, (ss_hdemo_sk#23 = hd_demo_sk#41)
:           :     :     :- Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:           :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:           :     :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:           :     :     +- Project [hd_demo_sk#41]
:           :     :        +- Filter (((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= 6)) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= 4))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= 2))) && isnotnull(hd_demo_sk#41))
:           :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
:           :     +- Project [t_time_sk#46]
:           :        +- Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 11)) && (t_minute#50 >= 30)) && isnotnull(t_time_sk#46))
:           :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
:           +- Project [s_store_sk#56]
:              +- Filter ((isnotnull(s_store_name#61) && (s_store_name#61 = ese)) && isnotnull(s_store_sk#56))
:                 +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet
+- Aggregate [count(1) AS h12_to_12_30#7L]
   +- Project
      +- Join Inner, (ss_store_sk#25 = s_store_sk#56)
         :- Project [ss_store_sk#25]
         :  +- Join Inner, (ss_sold_time_sk#19 = t_time_sk#46)
         :     :- Project [ss_sold_time_sk#19, ss_store_sk#25]
         :     :  +- Join Inner, (ss_hdemo_sk#23 = hd_demo_sk#41)
         :     :     :- Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
         :     :     :  +- Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
         :     :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
         :     :     +- Project [hd_demo_sk#41]
         :     :        +- Filter (((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= 6)) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= 4))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= 2))) && isnotnull(hd_demo_sk#41))
         :     :           +- Relation[hd_demo_sk#41,hd_income_band_sk#42,hd_buy_potential#43,hd_dep_count#44,hd_vehicle_count#45] parquet
         :     +- Project [t_time_sk#46]
         :        +- Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 12)) && (t_minute#50 < 30)) && isnotnull(t_time_sk#46))
         :           +- Relation[t_time_sk#46,t_time_id#47,t_time#48,t_hour#49,t_minute#50,t_second#51,t_am_pm#52,t_shift#53,t_sub_shift#54,t_meal_time#55] parquet
         +- Project [s_store_sk#56]
            +- Filter ((isnotnull(s_store_name#61) && (s_store_name#61 = ese)) && isnotnull(s_store_sk#56))
               +- Relation[s_store_sk#56,s_store_id#57,s_rec_start_date#58,s_rec_end_date#59,s_closed_date_sk#60,s_store_name#61,s_number_employees#62,s_floor_space#63,s_hours#64,s_manager#65,s_market_id#66,s_geography_class#67,s_market_desc#68,s_market_manager#69,s_division_id#70,s_division_name#71,s_company_id#72,s_company_name#73,s_street_number#74,s_street_name#75,s_street_type#76,s_suite_number#77,s_city#78,s_county#79,... 5 more fields] parquet

== Physical Plan ==
BroadcastNestedLoopJoin BuildRight, Inner
:- BroadcastNestedLoopJoin BuildRight, Inner
:  :- BroadcastNestedLoopJoin BuildRight, Inner
:  :  :- BroadcastNestedLoopJoin BuildRight, Inner
:  :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
:  :  :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
:  :  :  :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
:  :  :  :  :  :  :- *(5) HashAggregate(keys=[], functions=[count(1)], output=[h8_30_to_9#0L])
:  :  :  :  :  :  :  +- Exchange SinglePartition
:  :  :  :  :  :  :     +- *(4) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#86L])
:  :  :  :  :  :  :        +- *(4) Project
:  :  :  :  :  :  :           +- *(4) BroadcastHashJoin [ss_store_sk#25], [s_store_sk#56], Inner, BuildRight
:  :  :  :  :  :  :              :- *(4) Project [ss_store_sk#25]
:  :  :  :  :  :  :              :  +- *(4) BroadcastHashJoin [ss_sold_time_sk#19], [t_time_sk#46], Inner, BuildRight
:  :  :  :  :  :  :              :     :- *(4) Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :  :  :  :  :              :     :  +- *(4) BroadcastHashJoin [ss_hdemo_sk#23], [hd_demo_sk#41], Inner, BuildRight
:  :  :  :  :  :  :              :     :     :- *(4) Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :  :  :  :  :              :     :     :  +- *(4) Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :  :  :  :  :              :     :     :     +- *(4) FileScan parquet tpcds.store_sales[ss_sold_time_sk#19,ss_hdemo_sk#23,ss_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
:  :  :  :  :  :  :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :  :  :  :              :     :        +- *(1) Project [hd_demo_sk#41]
:  :  :  :  :  :  :              :     :           +- *(1) Filter (((((hd_dep_count#44 = 4) && (hd_vehicle_count#45 <= 6)) || ((hd_dep_count#44 = 2) && (hd_vehicle_count#45 <= 4))) || ((hd_dep_count#44 = 0) && (hd_vehicle_count#45 <= 2))) && isnotnull(hd_demo_sk#41))
:  :  :  :  :  :  :              :     :              +- *(1) FileScan parquet tpcds.household_demographics[hd_demo_sk#41,hd_dep_count#44,hd_vehicle_count#45] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [Or(Or(And(EqualTo(hd_dep_count,4),LessThanOrEqual(hd_vehicle_count,6)),And(EqualTo(hd_dep_count,..., ReadSchema: struct<hd_demo_sk:int,hd_dep_count:int,hd_vehicle_count:int>
:  :  :  :  :  :  :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :  :  :  :              :        +- *(2) Project [t_time_sk#46]
:  :  :  :  :  :  :              :           +- *(2) Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 8)) && (t_minute#50 >= 30)) && isnotnull(t_time_sk#46))
:  :  :  :  :  :  :              :              +- *(2) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49,t_minute#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,8), GreaterThanOrEqual(t_minute,30), IsNo..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
:  :  :  :  :  :  :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :  :  :  :                 +- *(3) Project [s_store_sk#56]
:  :  :  :  :  :  :                    +- *(3) Filter ((isnotnull(s_store_name#61) && (s_store_name#61 = ese)) && isnotnull(s_store_sk#56))
:  :  :  :  :  :  :                       +- *(3) FileScan parquet tpcds.store[s_store_sk#56,s_store_name#61] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_name), EqualTo(s_store_name,ese), IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string>
:  :  :  :  :  :  +- BroadcastExchange IdentityBroadcastMode
:  :  :  :  :  :     +- *(10) HashAggregate(keys=[], functions=[count(1)], output=[h9_to_9_30#1L])
:  :  :  :  :  :        +- Exchange SinglePartition
:  :  :  :  :  :           +- *(9) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#88L])
:  :  :  :  :  :              +- *(9) Project
:  :  :  :  :  :                 +- *(9) BroadcastHashJoin [ss_store_sk#25], [s_store_sk#56], Inner, BuildRight
:  :  :  :  :  :                    :- *(9) Project [ss_store_sk#25]
:  :  :  :  :  :                    :  +- *(9) BroadcastHashJoin [ss_sold_time_sk#19], [t_time_sk#46], Inner, BuildRight
:  :  :  :  :  :                    :     :- *(9) Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :  :  :  :                    :     :  +- *(9) BroadcastHashJoin [ss_hdemo_sk#23], [hd_demo_sk#41], Inner, BuildRight
:  :  :  :  :  :                    :     :     :- *(9) Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :  :  :  :                    :     :     :  +- *(9) Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :  :  :  :                    :     :     :     +- *(9) FileScan parquet tpcds.store_sales[ss_sold_time_sk#19,ss_hdemo_sk#23,ss_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
:  :  :  :  :  :                    :     :     +- ReusedExchange [hd_demo_sk#41], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :  :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :  :  :                    :        +- *(7) Project [t_time_sk#46]
:  :  :  :  :  :                    :           +- *(7) Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 9)) && (t_minute#50 < 30)) && isnotnull(t_time_sk#46))
:  :  :  :  :  :                    :              +- *(7) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49,t_minute#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,9), LessThan(t_minute,30), IsNotNull(t_ti..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
:  :  :  :  :  :                    +- ReusedExchange [s_store_sk#56], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :  :  +- BroadcastExchange IdentityBroadcastMode
:  :  :  :  :     +- *(15) HashAggregate(keys=[], functions=[count(1)], output=[h9_30_to_10#2L])
:  :  :  :  :        +- Exchange SinglePartition
:  :  :  :  :           +- *(14) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#90L])
:  :  :  :  :              +- *(14) Project
:  :  :  :  :                 +- *(14) BroadcastHashJoin [ss_store_sk#25], [s_store_sk#56], Inner, BuildRight
:  :  :  :  :                    :- *(14) Project [ss_store_sk#25]
:  :  :  :  :                    :  +- *(14) BroadcastHashJoin [ss_sold_time_sk#19], [t_time_sk#46], Inner, BuildRight
:  :  :  :  :                    :     :- *(14) Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :  :  :                    :     :  +- *(14) BroadcastHashJoin [ss_hdemo_sk#23], [hd_demo_sk#41], Inner, BuildRight
:  :  :  :  :                    :     :     :- *(14) Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :  :  :                    :     :     :  +- *(14) Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :  :  :                    :     :     :     +- *(14) FileScan parquet tpcds.store_sales[ss_sold_time_sk#19,ss_hdemo_sk#23,ss_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
:  :  :  :  :                    :     :     +- ReusedExchange [hd_demo_sk#41], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :  :                    :        +- *(12) Project [t_time_sk#46]
:  :  :  :  :                    :           +- *(12) Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 9)) && (t_minute#50 >= 30)) && isnotnull(t_time_sk#46))
:  :  :  :  :                    :              +- *(12) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49,t_minute#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,9), GreaterThanOrEqual(t_minute,30), IsNo..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
:  :  :  :  :                    +- ReusedExchange [s_store_sk#56], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :  +- BroadcastExchange IdentityBroadcastMode
:  :  :  :     +- *(20) HashAggregate(keys=[], functions=[count(1)], output=[h10_to_10_30#3L])
:  :  :  :        +- Exchange SinglePartition
:  :  :  :           +- *(19) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#92L])
:  :  :  :              +- *(19) Project
:  :  :  :                 +- *(19) BroadcastHashJoin [ss_store_sk#25], [s_store_sk#56], Inner, BuildRight
:  :  :  :                    :- *(19) Project [ss_store_sk#25]
:  :  :  :                    :  +- *(19) BroadcastHashJoin [ss_sold_time_sk#19], [t_time_sk#46], Inner, BuildRight
:  :  :  :                    :     :- *(19) Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :  :                    :     :  +- *(19) BroadcastHashJoin [ss_hdemo_sk#23], [hd_demo_sk#41], Inner, BuildRight
:  :  :  :                    :     :     :- *(19) Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :  :                    :     :     :  +- *(19) Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :  :                    :     :     :     +- *(19) FileScan parquet tpcds.store_sales[ss_sold_time_sk#19,ss_hdemo_sk#23,ss_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
:  :  :  :                    :     :     +- ReusedExchange [hd_demo_sk#41], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  :                    :        +- *(17) Project [t_time_sk#46]
:  :  :  :                    :           +- *(17) Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 10)) && (t_minute#50 < 30)) && isnotnull(t_time_sk#46))
:  :  :  :                    :              +- *(17) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49,t_minute#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,10), LessThan(t_minute,30), IsNotNull(t_t..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
:  :  :  :                    +- ReusedExchange [s_store_sk#56], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :  +- BroadcastExchange IdentityBroadcastMode
:  :  :     +- *(25) HashAggregate(keys=[], functions=[count(1)], output=[h10_30_to_11#4L])
:  :  :        +- Exchange SinglePartition
:  :  :           +- *(24) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#94L])
:  :  :              +- *(24) Project
:  :  :                 +- *(24) BroadcastHashJoin [ss_store_sk#25], [s_store_sk#56], Inner, BuildRight
:  :  :                    :- *(24) Project [ss_store_sk#25]
:  :  :                    :  +- *(24) BroadcastHashJoin [ss_sold_time_sk#19], [t_time_sk#46], Inner, BuildRight
:  :  :                    :     :- *(24) Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :  :                    :     :  +- *(24) BroadcastHashJoin [ss_hdemo_sk#23], [hd_demo_sk#41], Inner, BuildRight
:  :  :                    :     :     :- *(24) Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :  :                    :     :     :  +- *(24) Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :  :                    :     :     :     +- *(24) FileScan parquet tpcds.store_sales[ss_sold_time_sk#19,ss_hdemo_sk#23,ss_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
:  :  :                    :     :     +- ReusedExchange [hd_demo_sk#41], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  :                    :        +- *(22) Project [t_time_sk#46]
:  :  :                    :           +- *(22) Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 10)) && (t_minute#50 >= 30)) && isnotnull(t_time_sk#46))
:  :  :                    :              +- *(22) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49,t_minute#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,10), GreaterThanOrEqual(t_minute,30), IsN..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
:  :  :                    +- ReusedExchange [s_store_sk#56], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :  +- BroadcastExchange IdentityBroadcastMode
:  :     +- *(30) HashAggregate(keys=[], functions=[count(1)], output=[h11_to_11_30#5L])
:  :        +- Exchange SinglePartition
:  :           +- *(29) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#96L])
:  :              +- *(29) Project
:  :                 +- *(29) BroadcastHashJoin [ss_store_sk#25], [s_store_sk#56], Inner, BuildRight
:  :                    :- *(29) Project [ss_store_sk#25]
:  :                    :  +- *(29) BroadcastHashJoin [ss_sold_time_sk#19], [t_time_sk#46], Inner, BuildRight
:  :                    :     :- *(29) Project [ss_sold_time_sk#19, ss_store_sk#25]
:  :                    :     :  +- *(29) BroadcastHashJoin [ss_hdemo_sk#23], [hd_demo_sk#41], Inner, BuildRight
:  :                    :     :     :- *(29) Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:  :                    :     :     :  +- *(29) Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:  :                    :     :     :     +- *(29) FileScan parquet tpcds.store_sales[ss_sold_time_sk#19,ss_hdemo_sk#23,ss_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
:  :                    :     :     +- ReusedExchange [hd_demo_sk#41], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  :                    :        +- *(27) Project [t_time_sk#46]
:  :                    :           +- *(27) Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 11)) && (t_minute#50 < 30)) && isnotnull(t_time_sk#46))
:  :                    :              +- *(27) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49,t_minute#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,11), LessThan(t_minute,30), IsNotNull(t_t..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
:  :                    +- ReusedExchange [s_store_sk#56], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:  +- BroadcastExchange IdentityBroadcastMode
:     +- *(35) HashAggregate(keys=[], functions=[count(1)], output=[h11_30_to_12#6L])
:        +- Exchange SinglePartition
:           +- *(34) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#98L])
:              +- *(34) Project
:                 +- *(34) BroadcastHashJoin [ss_store_sk#25], [s_store_sk#56], Inner, BuildRight
:                    :- *(34) Project [ss_store_sk#25]
:                    :  +- *(34) BroadcastHashJoin [ss_sold_time_sk#19], [t_time_sk#46], Inner, BuildRight
:                    :     :- *(34) Project [ss_sold_time_sk#19, ss_store_sk#25]
:                    :     :  +- *(34) BroadcastHashJoin [ss_hdemo_sk#23], [hd_demo_sk#41], Inner, BuildRight
:                    :     :     :- *(34) Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
:                    :     :     :  +- *(34) Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
:                    :     :     :     +- *(34) FileScan parquet tpcds.store_sales[ss_sold_time_sk#19,ss_hdemo_sk#23,ss_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
:                    :     :     +- ReusedExchange [hd_demo_sk#41], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
:                    :        +- *(32) Project [t_time_sk#46]
:                    :           +- *(32) Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 11)) && (t_minute#50 >= 30)) && isnotnull(t_time_sk#46))
:                    :              +- *(32) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49,t_minute#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,11), GreaterThanOrEqual(t_minute,30), IsN..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
:                    +- ReusedExchange [s_store_sk#56], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
+- BroadcastExchange IdentityBroadcastMode
   +- *(40) HashAggregate(keys=[], functions=[count(1)], output=[h12_to_12_30#7L])
      +- Exchange SinglePartition
         +- *(39) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#100L])
            +- *(39) Project
               +- *(39) BroadcastHashJoin [ss_store_sk#25], [s_store_sk#56], Inner, BuildRight
                  :- *(39) Project [ss_store_sk#25]
                  :  +- *(39) BroadcastHashJoin [ss_sold_time_sk#19], [t_time_sk#46], Inner, BuildRight
                  :     :- *(39) Project [ss_sold_time_sk#19, ss_store_sk#25]
                  :     :  +- *(39) BroadcastHashJoin [ss_hdemo_sk#23], [hd_demo_sk#41], Inner, BuildRight
                  :     :     :- *(39) Project [ss_sold_time_sk#19, ss_hdemo_sk#23, ss_store_sk#25]
                  :     :     :  +- *(39) Filter ((isnotnull(ss_hdemo_sk#23) && isnotnull(ss_sold_time_sk#19)) && isnotnull(ss_store_sk#25))
                  :     :     :     +- *(39) FileScan parquet tpcds.store_sales[ss_sold_time_sk#19,ss_hdemo_sk#23,ss_store_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_hdemo_sk), IsNotNull(ss_sold_time_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_time_sk:int,ss_hdemo_sk:int,ss_store_sk:int>
                  :     :     +- ReusedExchange [hd_demo_sk#41], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :        +- *(37) Project [t_time_sk#46]
                  :           +- *(37) Filter ((((isnotnull(t_hour#49) && isnotnull(t_minute#50)) && (t_hour#49 = 12)) && (t_minute#50 < 30)) && isnotnull(t_time_sk#46))
                  :              +- *(37) FileScan parquet tpcds.time_dim[t_time_sk#46,t_hour#49,t_minute#50] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [IsNotNull(t_hour), IsNotNull(t_minute), EqualTo(t_hour,12), LessThan(t_minute,30), IsNotNull(t_t..., ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int>
                  +- ReusedExchange [s_store_sk#56], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.826 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 88 in stream 0 using template query88.tpl
------------------------------------------------------^^^

