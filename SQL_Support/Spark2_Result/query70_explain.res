== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['lochierarchy DESC NULLS LAST, CASE WHEN ('lochierarchy = 0) THEN 's_state END ASC NULLS FIRST, 'rank_within_parent ASC NULLS FIRST], true
      +- 'Aggregate ['rollup('s_state, 's_county)], ['sum('ss_net_profit) AS total_sum#0, 's_state, 's_county, ('grouping('s_state) + 'grouping('s_county)) AS lochierarchy#1, 'rank() windowspecdefinition(('grouping('s_state) + 'grouping('s_county)), CASE WHEN ('grouping('s_county) = 0) THEN 's_state END, 'sum('ss_net_profit) DESC NULLS LAST, unspecifiedframe$()) AS rank_within_parent#2]
         +- 'Filter (((('d1.d_month_seq >= 1200) && ('d1.d_month_seq <= (1200 + 11))) && ('d1.d_date_sk = 'ss_sold_date_sk)) && (('s_store_sk = 'ss_store_sk) && 's_state IN (list#5 [])))
            :  +- 'Project ['s_state]
            :     +- 'Filter ('ranking <= 5)
            :        +- 'SubqueryAlias `tmp1`
            :           +- 'Aggregate ['s_state], ['s_state AS s_state#3, 'rank() windowspecdefinition('s_state, 'sum('ss_net_profit) DESC NULLS LAST, unspecifiedframe$()) AS ranking#4]
            :              +- 'Filter (((('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11))) && ('d_date_sk = 'ss_sold_date_sk)) && ('s_store_sk = 'ss_store_sk))
            :                 +- 'Join Inner
            :                    :- 'Join Inner
            :                    :  :- 'UnresolvedRelation `store_sales`
            :                    :  +- 'UnresolvedRelation `store`
            :                    +- 'UnresolvedRelation `date_dim`
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation `store_sales`
               :  +- 'SubqueryAlias `d1`
               :     +- 'UnresolvedRelation `date_dim`
               +- 'UnresolvedRelation `store`

== Analyzed Logical Plan ==
total_sum: double, s_state: string, s_county: string, lochierarchy: tinyint, rank_within_parent: int
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (cast(lochierarchy#1 as int) = 0) THEN s_state#112 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [total_sum#0, s_state#112, s_county#113, lochierarchy#1, rank_within_parent#2]
         +- Project [total_sum#0, s_state#112, s_county#113, lochierarchy#1, _w0#122, _w1#126, _w2#127, _w3#128, rank_within_parent#2, rank_within_parent#2]
            +- Window [rank(_w3#128) windowspecdefinition(_w1#126, _w2#127, _w3#128 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#126, _w2#127], [_w3#128 DESC NULLS LAST]
               +- Aggregate [s_state#112, s_county#113, spark_grouping_id#109], [sum(ss_net_profit#32) AS total_sum#0, s_state#112, s_county#113, (cast((shiftright(spark_grouping_id#109, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#109, 0) & 1) as tinyint)) AS lochierarchy#1, sum(ss_net_profit#32) AS _w0#122, (cast((shiftright(spark_grouping_id#109, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#109, 0) & 1) as tinyint)) AS _w1#126, CASE WHEN (cast(cast((shiftright(spark_grouping_id#109, 0) & 1) as tinyint) as int) = 0) THEN s_state#112 END AS _w2#127, sum(ss_net_profit#32) AS _w3#128]
                  +- Expand [List(ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, d_date_id#34, d_date#35, d_month_seq#36, d_week_seq#37, d_quarter_seq#38, d_year#39, d_dow#40, d_moy#41, d_dom#42, d_qoy#43, d_fy_year#44, d_fy_quarter_seq#45, d_fy_week_seq#46, d_day_name#47, d_quarter_name#48, d_holiday#49, d_weekend#50, d_following_holiday#51, d_first_dom#52, d_last_dom#53, d_same_day_ly#54, d_same_day_lq#55, d_current_day#56, d_current_week#57, d_current_month#58, d_current_quarter#59, d_current_year#60, s_store_sk#61, s_store_id#62, s_rec_start_date#63, s_rec_end_date#64, s_closed_date_sk#65, s_store_name#66, s_number_employees#67, s_floor_space#68, s_hours#69, s_manager#70, s_market_id#71, s_geography_class#72, s_market_desc#73, s_market_manager#74, s_division_id#75, s_division_name#76, s_company_id#77, s_company_name#78, s_street_number#79, s_street_name#80, s_street_type#81, s_suite_number#82, s_city#83, s_county#84, s_state#85, s_zip#86, s_country#87, s_gmt_offset#88, s_tax_precentage#89, s_state#110, s_county#111, 0), List(ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, d_date_id#34, d_date#35, d_month_seq#36, d_week_seq#37, d_quarter_seq#38, d_year#39, d_dow#40, d_moy#41, d_dom#42, d_qoy#43, d_fy_year#44, d_fy_quarter_seq#45, d_fy_week_seq#46, d_day_name#47, d_quarter_name#48, d_holiday#49, d_weekend#50, d_following_holiday#51, d_first_dom#52, d_last_dom#53, d_same_day_ly#54, d_same_day_lq#55, d_current_day#56, d_current_week#57, d_current_month#58, d_current_quarter#59, d_current_year#60, s_store_sk#61, s_store_id#62, s_rec_start_date#63, s_rec_end_date#64, s_closed_date_sk#65, s_store_name#66, s_number_employees#67, s_floor_space#68, s_hours#69, s_manager#70, s_market_id#71, s_geography_class#72, s_market_desc#73, s_market_manager#74, s_division_id#75, s_division_name#76, s_company_id#77, s_company_name#78, s_street_number#79, s_street_name#80, s_street_type#81, s_suite_number#82, s_city#83, s_county#84, s_state#85, s_zip#86, s_country#87, s_gmt_offset#88, s_tax_precentage#89, s_state#110, null, 1), List(ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, d_date_id#34, d_date#35, d_month_seq#36, d_week_seq#37, d_quarter_seq#38, d_year#39, d_dow#40, d_moy#41, d_dom#42, d_qoy#43, d_fy_year#44, d_fy_quarter_seq#45, d_fy_week_seq#46, d_day_name#47, d_quarter_name#48, d_holiday#49, d_weekend#50, d_following_holiday#51, d_first_dom#52, d_last_dom#53, d_same_day_ly#54, d_same_day_lq#55, d_current_day#56, d_current_week#57, d_current_month#58, d_current_quarter#59, d_current_year#60, s_store_sk#61, s_store_id#62, s_rec_start_date#63, s_rec_end_date#64, s_closed_date_sk#65, s_store_name#66, s_number_employees#67, s_floor_space#68, s_hours#69, s_manager#70, s_market_id#71, s_geography_class#72, s_market_desc#73, s_market_manager#74, s_division_id#75, s_division_name#76, s_company_id#77, s_company_name#78, s_street_number#79, s_street_name#80, s_street_type#81, s_suite_number#82, s_city#83, s_county#84, s_state#85, s_zip#86, s_country#87, s_gmt_offset#88, s_tax_precentage#89, null, null, 3)], [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, ... 59 more fields]
                     +- Project [ss_sold_date_sk#10, ss_sold_time_sk#11, ss_item_sk#12, ss_customer_sk#13, ss_cdemo_sk#14, ss_hdemo_sk#15, ss_addr_sk#16, ss_store_sk#17, ss_promo_sk#18, ss_ticket_number#19, ss_quantity#20, ss_wholesale_cost#21, ss_list_price#22, ss_sales_price#23, ss_ext_discount_amt#24, ss_ext_sales_price#25, ss_ext_wholesale_cost#26, ss_ext_list_price#27, ss_ext_tax#28, ss_coupon_amt#29, ss_net_paid#30, ss_net_paid_inc_tax#31, ss_net_profit#32, d_date_sk#33, ... 58 more fields]
                        +- Filter ((((d_month_seq#36 >= 1200) && (d_month_seq#36 <= (1200 + 11))) && (d_date_sk#33 = ss_sold_date_sk#10)) && ((s_store_sk#61 = ss_store_sk#17) && s_state#85 IN (list#5 [])))
                           :  +- Project [s_state#3]
                           :     +- Filter (ranking#4 <= 5)
                           :        +- SubqueryAlias `tmp1`
                           :           +- Project [s_state#3, ranking#4]
                           :              +- Project [s_state#3, _w0#96, s_state#85, _w2#100, ranking#4, ranking#4]
                           :                 +- Window [rank(_w2#100) windowspecdefinition(s_state#85, _w2#100 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS ranking#4], [s_state#85], [_w2#100 DESC NULLS LAST]
                           :                    +- Aggregate [s_state#85], [s_state#85 AS s_state#3, sum(ss_net_profit#32) AS _w0#96, s_state#85, sum(ss_net_profit#32) AS _w2#100]
                           :                       +- Filter ((((d_month_seq#36 >= 1200) && (d_month_seq#36 <= (1200 + 11))) && (d_date_sk#33 = ss_sold_date_sk#10)) && (s_store_sk#61 = ss_store_sk#17))
                           :                          +- Join Inner
                           :                             :- Join Inner
                           :                             :  :- SubqueryAlias `tpcds`.`store_sales`
                           :                             :  :  +- Relation[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
                           :                             :  +- SubqueryAlias `tpcds`.`store`
                           :                             :     +- Relation[s_store_sk#61,s_store_id#62,s_rec_start_date#63,s_rec_end_date#64,s_closed_date_sk#65,s_store_name#66,s_number_employees#67,s_floor_space#68,s_hours#69,s_manager#70,s_market_id#71,s_geography_class#72,s_market_desc#73,s_market_manager#74,s_division_id#75,s_division_name#76,s_company_id#77,s_company_name#78,s_street_number#79,s_street_name#80,s_street_type#81,s_suite_number#82,s_city#83,s_county#84,... 5 more fields] parquet
                           :                             +- SubqueryAlias `tpcds`.`date_dim`
                           :                                +- Relation[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
                           +- Join Inner
                              :- Join Inner
                              :  :- SubqueryAlias `tpcds`.`store_sales`
                              :  :  +- Relation[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
                              :  +- SubqueryAlias `d1`
                              :     +- SubqueryAlias `tpcds`.`date_dim`
                              :        +- Relation[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
                              +- SubqueryAlias `tpcds`.`store`
                                 +- Relation[s_store_sk#61,s_store_id#62,s_rec_start_date#63,s_rec_end_date#64,s_closed_date_sk#65,s_store_name#66,s_number_employees#67,s_floor_space#68,s_hours#69,s_manager#70,s_market_id#71,s_geography_class#72,s_market_desc#73,s_market_manager#74,s_division_id#75,s_division_name#76,s_company_id#77,s_company_name#78,s_street_number#79,s_street_name#80,s_street_type#81,s_suite_number#82,s_city#83,s_county#84,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [lochierarchy#1 DESC NULLS LAST, CASE WHEN (cast(lochierarchy#1 as int) = 0) THEN s_state#112 END ASC NULLS FIRST, rank_within_parent#2 ASC NULLS FIRST], true
      +- Project [total_sum#0, s_state#112, s_county#113, lochierarchy#1, rank_within_parent#2]
         +- Window [rank(_w3#128) windowspecdefinition(_w1#126, _w2#127, _w3#128 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#126, _w2#127], [_w3#128 DESC NULLS LAST]
            +- Aggregate [s_state#112, s_county#113, spark_grouping_id#109], [sum(ss_net_profit#32) AS total_sum#0, s_state#112, s_county#113, (cast((shiftright(spark_grouping_id#109, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#109, 0) & 1) as tinyint)) AS lochierarchy#1, (cast((shiftright(spark_grouping_id#109, 1) & 1) as tinyint) + cast((shiftright(spark_grouping_id#109, 0) & 1) as tinyint)) AS _w1#126, CASE WHEN (cast(cast((shiftright(spark_grouping_id#109, 0) & 1) as tinyint) as int) = 0) THEN s_state#112 END AS _w2#127, sum(ss_net_profit#32) AS _w3#128]
               +- Expand [List(ss_net_profit#32, s_state#85, s_county#84, 0), List(ss_net_profit#32, s_state#85, null, 1), List(ss_net_profit#32, null, null, 3)], [ss_net_profit#32, s_state#112, s_county#113, spark_grouping_id#109]
                  +- Project [ss_net_profit#32, s_state#85, s_county#84]
                     +- Join Inner, (s_store_sk#61 = ss_store_sk#17)
                        :- Project [ss_store_sk#17, ss_net_profit#32]
                        :  +- Join Inner, (d_date_sk#33 = ss_sold_date_sk#10)
                        :     :- Project [ss_sold_date_sk#10, ss_store_sk#17, ss_net_profit#32]
                        :     :  +- Filter (isnotnull(ss_sold_date_sk#10) && isnotnull(ss_store_sk#17))
                        :     :     +- Relation[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
                        :     +- Project [d_date_sk#33]
                        :        +- Filter (((isnotnull(d_month_seq#36) && (d_month_seq#36 >= 1200)) && (d_month_seq#36 <= 1211)) && isnotnull(d_date_sk#33))
                        :           +- Relation[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
                        +- Join LeftSemi, (s_state#85 = s_state#3)
                           :- Project [s_store_sk#61, s_county#84, s_state#85]
                           :  +- Filter isnotnull(s_store_sk#61)
                           :     +- Relation[s_store_sk#61,s_store_id#62,s_rec_start_date#63,s_rec_end_date#64,s_closed_date_sk#65,s_store_name#66,s_number_employees#67,s_floor_space#68,s_hours#69,s_manager#70,s_market_id#71,s_geography_class#72,s_market_desc#73,s_market_manager#74,s_division_id#75,s_division_name#76,s_company_id#77,s_company_name#78,s_street_number#79,s_street_name#80,s_street_type#81,s_suite_number#82,s_city#83,s_county#84,... 5 more fields] parquet
                           +- Project [s_state#3]
                              +- Filter (isnotnull(ranking#4) && (ranking#4 <= 5))
                                 +- Window [rank(_w2#100) windowspecdefinition(s_state#85, _w2#100 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS ranking#4], [s_state#85], [_w2#100 DESC NULLS LAST]
                                    +- Aggregate [s_state#85], [s_state#85 AS s_state#3, s_state#85, sum(ss_net_profit#32) AS _w2#100]
                                       +- Project [ss_net_profit#32, s_state#85]
                                          +- Join Inner, (d_date_sk#33 = ss_sold_date_sk#10)
                                             :- Project [ss_sold_date_sk#10, ss_net_profit#32, s_state#85]
                                             :  +- Join Inner, (s_store_sk#61 = ss_store_sk#17)
                                             :     :- Project [ss_sold_date_sk#10, ss_store_sk#17, ss_net_profit#32]
                                             :     :  +- Filter (isnotnull(ss_store_sk#17) && isnotnull(ss_sold_date_sk#10))
                                             :     :     +- Relation[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
                                             :     +- Project [s_store_sk#61, s_state#85]
                                             :        +- Filter isnotnull(s_store_sk#61)
                                             :           +- Relation[s_store_sk#61,s_store_id#62,s_rec_start_date#63,s_rec_end_date#64,s_closed_date_sk#65,s_store_name#66,s_number_employees#67,s_floor_space#68,s_hours#69,s_manager#70,s_market_id#71,s_geography_class#72,s_market_desc#73,s_market_manager#74,s_division_id#75,s_division_name#76,s_company_id#77,s_company_name#78,s_street_number#79,s_street_name#80,s_street_type#81,s_suite_number#82,s_city#83,s_county#84,... 5 more fields] parquet
                                             +- Project [d_date_sk#33]
                                                +- Filter (((isnotnull(d_month_seq#36) && (d_month_seq#36 >= 1200)) && (d_month_seq#36 <= 1211)) && isnotnull(d_date_sk#33))
                                                   +- Relation[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[lochierarchy#1 DESC NULLS LAST,CASE WHEN (cast(lochierarchy#1 as int) = 0) THEN s_state#112 END ASC NULLS FIRST,rank_within_parent#2 ASC NULLS FIRST], output=[total_sum#0,s_state#112,s_county#113,lochierarchy#1,rank_within_parent#2])
+- *(13) Project [total_sum#0, s_state#112, s_county#113, lochierarchy#1, rank_within_parent#2]
   +- Window [rank(_w3#128) windowspecdefinition(_w1#126, _w2#127, _w3#128 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank_within_parent#2], [_w1#126, _w2#127], [_w3#128 DESC NULLS LAST]
      +- *(12) Sort [_w1#126 ASC NULLS FIRST, _w2#127 ASC NULLS FIRST, _w3#128 DESC NULLS LAST], false, 0
         +- Exchange hashpartitioning(_w1#126, _w2#127, 200)
            +- *(11) HashAggregate(keys=[s_state#112, s_county#113, spark_grouping_id#109], functions=[sum(ss_net_profit#32)], output=[total_sum#0, s_state#112, s_county#113, lochierarchy#1, _w1#126, _w2#127, _w3#128])
               +- Exchange hashpartitioning(s_state#112, s_county#113, spark_grouping_id#109, 200)
                  +- *(10) HashAggregate(keys=[s_state#112, s_county#113, spark_grouping_id#109], functions=[partial_sum(ss_net_profit#32)], output=[s_state#112, s_county#113, spark_grouping_id#109, sum#142])
                     +- *(10) Expand [List(ss_net_profit#32, s_state#85, s_county#84, 0), List(ss_net_profit#32, s_state#85, null, 1), List(ss_net_profit#32, null, null, 3)], [ss_net_profit#32, s_state#112, s_county#113, spark_grouping_id#109]
                        +- *(10) Project [ss_net_profit#32, s_state#85, s_county#84]
                           +- *(10) BroadcastHashJoin [ss_store_sk#17], [s_store_sk#61], Inner, BuildRight
                              :- *(10) Project [ss_store_sk#17, ss_net_profit#32]
                              :  +- *(10) BroadcastHashJoin [ss_sold_date_sk#10], [d_date_sk#33], Inner, BuildRight
                              :     :- *(10) Project [ss_sold_date_sk#10, ss_store_sk#17, ss_net_profit#32]
                              :     :  +- *(10) Filter (isnotnull(ss_sold_date_sk#10) && isnotnull(ss_store_sk#17))
                              :     :     +- *(10) FileScan parquet tpcds.store_sales[ss_sold_date_sk#10,ss_store_sk#17,ss_net_profit#32] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_net_profit:double>
                              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              :        +- *(1) Project [d_date_sk#33]
                              :           +- *(1) Filter (((isnotnull(d_month_seq#36) && (d_month_seq#36 >= 1200)) && (d_month_seq#36 <= 1211)) && isnotnull(d_date_sk#33))
                              :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#33,d_month_seq#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_month_seq:int>
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 +- SortMergeJoin [s_state#85], [s_state#3], LeftSemi
                                    :- *(3) Sort [s_state#85 ASC NULLS FIRST], false, 0
                                    :  +- Exchange hashpartitioning(s_state#85, 200)
                                    :     +- *(2) Project [s_store_sk#61, s_county#84, s_state#85]
                                    :        +- *(2) Filter isnotnull(s_store_sk#61)
                                    :           +- *(2) FileScan parquet tpcds.store[s_store_sk#61,s_county#84,s_state#85] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_county:string,s_state:string>
                                    +- *(9) Sort [s_state#3 ASC NULLS FIRST], false, 0
                                       +- Exchange hashpartitioning(s_state#3, 200)
                                          +- *(8) Project [s_state#3]
                                             +- *(8) Filter (isnotnull(ranking#4) && (ranking#4 <= 5))
                                                +- Window [rank(_w2#100) windowspecdefinition(s_state#85, _w2#100 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS ranking#4], [s_state#85], [_w2#100 DESC NULLS LAST]
                                                   +- *(7) Sort [s_state#85 ASC NULLS FIRST, _w2#100 DESC NULLS LAST], false, 0
                                                      +- *(7) HashAggregate(keys=[s_state#85], functions=[sum(ss_net_profit#32)], output=[s_state#3, s_state#85, _w2#100])
                                                         +- Exchange hashpartitioning(s_state#85, 200)
                                                            +- *(6) HashAggregate(keys=[s_state#85], functions=[partial_sum(ss_net_profit#32)], output=[s_state#85, sum#144])
                                                               +- *(6) Project [ss_net_profit#32, s_state#85]
                                                                  +- *(6) BroadcastHashJoin [ss_sold_date_sk#10], [d_date_sk#33], Inner, BuildRight
                                                                     :- *(6) Project [ss_sold_date_sk#10, ss_net_profit#32, s_state#85]
                                                                     :  +- *(6) BroadcastHashJoin [ss_store_sk#17], [s_store_sk#61], Inner, BuildRight
                                                                     :     :- *(6) Project [ss_sold_date_sk#10, ss_store_sk#17, ss_net_profit#32]
                                                                     :     :  +- *(6) Filter (isnotnull(ss_store_sk#17) && isnotnull(ss_sold_date_sk#10))
                                                                     :     :     +- *(6) FileScan parquet tpcds.store_sales[ss_sold_date_sk#10,ss_store_sk#17,ss_net_profit#32] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_store_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_net_profit:double>
                                                                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                                                     :        +- *(4) Project [s_store_sk#61, s_state#85]
                                                                     :           +- *(4) Filter isnotnull(s_store_sk#61)
                                                                     :              +- *(4) FileScan parquet tpcds.store[s_store_sk#61,s_state#85] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_state:string>
                                                                     +- ReusedExchange [d_date_sk#33], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 4.692 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 70 in stream 0 using template query70.tpl
------------------------------------------------------^^^

