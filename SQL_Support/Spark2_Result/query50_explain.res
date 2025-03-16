== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['s_store_name ASC NULLS FIRST, 's_company_id ASC NULLS FIRST, 's_street_number ASC NULLS FIRST, 's_street_name ASC NULLS FIRST, 's_street_type ASC NULLS FIRST, 's_suite_number ASC NULLS FIRST, 's_city ASC NULLS FIRST, 's_county ASC NULLS FIRST, 's_state ASC NULLS FIRST, 's_zip ASC NULLS FIRST], true
      +- 'Aggregate ['s_store_name, 's_company_id, 's_street_number, 's_street_name, 's_street_type, 's_suite_number, 's_city, 's_county, 's_state, 's_zip], ['s_store_name, 's_company_id, 's_street_number, 's_street_name, 's_street_type, 's_suite_number, 's_city, 's_county, 's_state, 's_zip, 'sum(CASE WHEN (('sr_returned_date_sk - 'ss_sold_date_sk) <= 30) THEN 1 ELSE 0 END) AS 30_days#0, 'sum(CASE WHEN ((('sr_returned_date_sk - 'ss_sold_date_sk) > 30) && (('sr_returned_date_sk - 'ss_sold_date_sk) <= 60)) THEN 1 ELSE 0 END) AS 31_60_days#1, 'sum(CASE WHEN ((('sr_returned_date_sk - 'ss_sold_date_sk) > 60) && (('sr_returned_date_sk - 'ss_sold_date_sk) <= 90)) THEN 1 ELSE 0 END) AS 61_90_days#2, 'sum(CASE WHEN ((('sr_returned_date_sk - 'ss_sold_date_sk) > 90) && (('sr_returned_date_sk - 'ss_sold_date_sk) <= 120)) THEN 1 ELSE 0 END) AS 91_120_days#3, 'sum(CASE WHEN (('sr_returned_date_sk - 'ss_sold_date_sk) > 120) THEN 1 ELSE 0 END) AS above120_days#4]
         +- 'Filter (((('d2.d_year = 2001) && ('d2.d_moy = 8)) && (('ss_ticket_number = 'sr_ticket_number) && ('ss_item_sk = 'sr_item_sk))) && ((('ss_sold_date_sk = 'd1.d_date_sk) && ('sr_returned_date_sk = 'd2.d_date_sk)) && (('ss_customer_sk = 'sr_customer_sk) && ('ss_store_sk = 's_store_sk))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation `store_sales`
               :  :  :  +- 'UnresolvedRelation `store_returns`
               :  :  +- 'UnresolvedRelation `store`
               :  +- 'SubqueryAlias `d1`
               :     +- 'UnresolvedRelation `date_dim`
               +- 'SubqueryAlias `d2`
                  +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
s_store_name: string, s_company_id: int, s_street_number: string, s_street_name: string, s_street_type: string, s_suite_number: string, s_city: string, s_county: string, s_state: string, s_zip: string, 30_days: bigint, 31_60_days: bigint, 61_90_days: bigint, 91_120_days: bigint, above120_days: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Project [s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75, 30_days#0L, 31_60_days#1L, 61_90_days#2L, 91_120_days#3L, above120_days#4L]
      +- Sort [s_store_name#55 ASC NULLS FIRST, s_company_id#66 ASC NULLS FIRST, s_street_number#68 ASC NULLS FIRST, s_street_name#69 ASC NULLS FIRST, s_street_type#70 ASC NULLS FIRST, s_suite_number#71 ASC NULLS FIRST, s_city#72 ASC NULLS FIRST, s_county#73 ASC NULLS FIRST, s_state#74 ASC NULLS FIRST, s_zip#75 ASC NULLS FIRST], true
         +- Aggregate [s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75], [s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75, sum(cast(CASE WHEN ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)) AS 30_days#0L, sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 30) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)) AS 31_60_days#1L, sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 60) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)) AS 61_90_days#2L, sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 90) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)) AS 91_120_days#3L, sum(cast(CASE WHEN ((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint)) AS above120_days#4L]
            +- Filter ((((d_year#113 = 2001) && (d_moy#115 = 8)) && ((ss_ticket_number#16 = sr_ticket_number#39) && (ss_item_sk#9 = sr_item_sk#32))) && (((ss_sold_date_sk#7 = d_date_sk#79) && (sr_returned_date_sk#30 = d_date_sk#107)) && ((ss_customer_sk#10 = sr_customer_sk#33) && (ss_store_sk#14 = s_store_sk#50))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
                  :  :  :  :  +- Relation[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
                  :  :  :  +- SubqueryAlias `tpcds`.`store_returns`
                  :  :  :     +- Relation[sr_returned_date_sk#30,sr_return_time_sk#31,sr_item_sk#32,sr_customer_sk#33,sr_cdemo_sk#34,sr_hdemo_sk#35,sr_addr_sk#36,sr_store_sk#37,sr_reason_sk#38,sr_ticket_number#39,sr_return_quantity#40,sr_return_amt#41,sr_return_tax#42,sr_return_amt_inc_tax#43,sr_fee#44,sr_return_ship_cost#45,sr_refunded_cash#46,sr_reversed_charge#47,sr_store_credit#48,sr_net_loss#49] parquet
                  :  :  +- SubqueryAlias `tpcds`.`store`
                  :  :     +- Relation[s_store_sk#50,s_store_id#51,s_rec_start_date#52,s_rec_end_date#53,s_closed_date_sk#54,s_store_name#55,s_number_employees#56,s_floor_space#57,s_hours#58,s_manager#59,s_market_id#60,s_geography_class#61,s_market_desc#62,s_market_manager#63,s_division_id#64,s_division_name#65,s_company_id#66,s_company_name#67,s_street_number#68,s_street_name#69,s_street_type#70,s_suite_number#71,s_city#72,s_county#73,... 5 more fields] parquet
                  :  +- SubqueryAlias `d1`
                  :     +- SubqueryAlias `tpcds`.`date_dim`
                  :        +- Relation[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet
                  +- SubqueryAlias `d2`
                     +- SubqueryAlias `tpcds`.`date_dim`
                        +- Relation[d_date_sk#107,d_date_id#108,d_date#109,d_month_seq#110,d_week_seq#111,d_quarter_seq#112,d_year#113,d_dow#114,d_moy#115,d_dom#116,d_qoy#117,d_fy_year#118,d_fy_quarter_seq#119,d_fy_week_seq#120,d_day_name#121,d_quarter_name#122,d_holiday#123,d_weekend#124,d_following_holiday#125,d_first_dom#126,d_last_dom#127,d_same_day_ly#128,d_same_day_lq#129,d_current_day#130,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#55 ASC NULLS FIRST, s_company_id#66 ASC NULLS FIRST, s_street_number#68 ASC NULLS FIRST, s_street_name#69 ASC NULLS FIRST, s_street_type#70 ASC NULLS FIRST, s_suite_number#71 ASC NULLS FIRST, s_city#72 ASC NULLS FIRST, s_county#73 ASC NULLS FIRST, s_state#74 ASC NULLS FIRST, s_zip#75 ASC NULLS FIRST], true
      +- Aggregate [s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75], [s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75, sum(cast(CASE WHEN ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)) AS 30_days#0L, sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 30) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)) AS 31_60_days#1L, sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 60) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)) AS 61_90_days#2L, sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 90) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)) AS 91_120_days#3L, sum(cast(CASE WHEN ((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint)) AS above120_days#4L]
         +- Project [ss_sold_date_sk#7, sr_returned_date_sk#30, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
            +- Join Inner, (sr_returned_date_sk#30 = d_date_sk#107)
               :- Project [ss_sold_date_sk#7, sr_returned_date_sk#30, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :  +- Join Inner, (ss_sold_date_sk#7 = d_date_sk#79)
               :     :- Project [ss_sold_date_sk#7, sr_returned_date_sk#30, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :     :  +- Join Inner, (ss_store_sk#14 = s_store_sk#50)
               :     :     :- Project [ss_sold_date_sk#7, ss_store_sk#14, sr_returned_date_sk#30]
               :     :     :  +- Join Inner, (((ss_ticket_number#16 = sr_ticket_number#39) && (ss_item_sk#9 = sr_item_sk#32)) && (ss_customer_sk#10 = sr_customer_sk#33))
               :     :     :     :- Project [ss_sold_date_sk#7, ss_item_sk#9, ss_customer_sk#10, ss_store_sk#14, ss_ticket_number#16]
               :     :     :     :  +- Filter ((((isnotnull(ss_customer_sk#10) && isnotnull(ss_item_sk#9)) && isnotnull(ss_ticket_number#16)) && isnotnull(ss_store_sk#14)) && isnotnull(ss_sold_date_sk#7))
               :     :     :     :     +- Relation[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
               :     :     :     +- Project [sr_returned_date_sk#30, sr_item_sk#32, sr_customer_sk#33, sr_ticket_number#39]
               :     :     :        +- Filter (((isnotnull(sr_customer_sk#33) && isnotnull(sr_item_sk#32)) && isnotnull(sr_ticket_number#39)) && isnotnull(sr_returned_date_sk#30))
               :     :     :           +- Relation[sr_returned_date_sk#30,sr_return_time_sk#31,sr_item_sk#32,sr_customer_sk#33,sr_cdemo_sk#34,sr_hdemo_sk#35,sr_addr_sk#36,sr_store_sk#37,sr_reason_sk#38,sr_ticket_number#39,sr_return_quantity#40,sr_return_amt#41,sr_return_tax#42,sr_return_amt_inc_tax#43,sr_fee#44,sr_return_ship_cost#45,sr_refunded_cash#46,sr_reversed_charge#47,sr_store_credit#48,sr_net_loss#49] parquet
               :     :     +- Project [s_store_sk#50, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :     :        +- Filter isnotnull(s_store_sk#50)
               :     :           +- Relation[s_store_sk#50,s_store_id#51,s_rec_start_date#52,s_rec_end_date#53,s_closed_date_sk#54,s_store_name#55,s_number_employees#56,s_floor_space#57,s_hours#58,s_manager#59,s_market_id#60,s_geography_class#61,s_market_desc#62,s_market_manager#63,s_division_id#64,s_division_name#65,s_company_id#66,s_company_name#67,s_street_number#68,s_street_name#69,s_street_type#70,s_suite_number#71,s_city#72,s_county#73,... 5 more fields] parquet
               :     +- Project [d_date_sk#79]
               :        +- Filter isnotnull(d_date_sk#79)
               :           +- Relation[d_date_sk#79,d_date_id#80,d_date#81,d_month_seq#82,d_week_seq#83,d_quarter_seq#84,d_year#85,d_dow#86,d_moy#87,d_dom#88,d_qoy#89,d_fy_year#90,d_fy_quarter_seq#91,d_fy_week_seq#92,d_day_name#93,d_quarter_name#94,d_holiday#95,d_weekend#96,d_following_holiday#97,d_first_dom#98,d_last_dom#99,d_same_day_ly#100,d_same_day_lq#101,d_current_day#102,... 4 more fields] parquet
               +- Project [d_date_sk#107]
                  +- Filter ((((isnotnull(d_year#113) && isnotnull(d_moy#115)) && (d_year#113 = 2001)) && (d_moy#115 = 8)) && isnotnull(d_date_sk#107))
                     +- Relation[d_date_sk#107,d_date_id#108,d_date#109,d_month_seq#110,d_week_seq#111,d_quarter_seq#112,d_year#113,d_dow#114,d_moy#115,d_dom#116,d_qoy#117,d_fy_year#118,d_fy_quarter_seq#119,d_fy_week_seq#120,d_day_name#121,d_quarter_name#122,d_holiday#123,d_weekend#124,d_following_holiday#125,d_first_dom#126,d_last_dom#127,d_same_day_ly#128,d_same_day_lq#129,d_current_day#130,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[s_store_name#55 ASC NULLS FIRST,s_company_id#66 ASC NULLS FIRST,s_street_number#68 ASC NULLS FIRST,s_street_name#69 ASC NULLS FIRST,s_street_type#70 ASC NULLS FIRST,s_suite_number#71 ASC NULLS FIRST,s_city#72 ASC NULLS FIRST,s_county#73 ASC NULLS FIRST,s_state#74 ASC NULLS FIRST,s_zip#75 ASC NULLS FIRST], output=[s_store_name#55,s_company_id#66,s_street_number#68,s_street_name#69,s_street_type#70,s_suite_number#71,s_city#72,s_county#73,s_state#74,s_zip#75,30_days#0L,31_60_days#1L,61_90_days#2L,91_120_days#3L,above120_days#4L])
+- *(6) HashAggregate(keys=[s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75], functions=[sum(cast(CASE WHEN ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 30) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 60) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 90) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN ((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint))], output=[s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75, 30_days#0L, 31_60_days#1L, 61_90_days#2L, 91_120_days#3L, above120_days#4L])
   +- Exchange hashpartitioning(s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75, 200)
      +- *(5) HashAggregate(keys=[s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75], functions=[partial_sum(cast(CASE WHEN ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 30) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 30) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 60)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 60) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 90)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 90) && ((sr_returned_date_sk#30 - ss_sold_date_sk#7) <= 120)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN ((sr_returned_date_sk#30 - ss_sold_date_sk#7) > 120) THEN 1 ELSE 0 END as bigint))], output=[s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75, sum#155L, sum#156L, sum#157L, sum#158L, sum#159L])
         +- *(5) Project [ss_sold_date_sk#7, sr_returned_date_sk#30, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
            +- *(5) BroadcastHashJoin [sr_returned_date_sk#30], [d_date_sk#107], Inner, BuildRight
               :- *(5) Project [ss_sold_date_sk#7, sr_returned_date_sk#30, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :  +- *(5) BroadcastHashJoin [ss_sold_date_sk#7], [d_date_sk#79], Inner, BuildRight
               :     :- *(5) Project [ss_sold_date_sk#7, sr_returned_date_sk#30, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :     :  +- *(5) BroadcastHashJoin [ss_store_sk#14], [s_store_sk#50], Inner, BuildRight
               :     :     :- *(5) Project [ss_sold_date_sk#7, ss_store_sk#14, sr_returned_date_sk#30]
               :     :     :  +- *(5) BroadcastHashJoin [ss_ticket_number#16, ss_item_sk#9, ss_customer_sk#10], [sr_ticket_number#39, sr_item_sk#32, sr_customer_sk#33], Inner, BuildRight
               :     :     :     :- *(5) Project [ss_sold_date_sk#7, ss_item_sk#9, ss_customer_sk#10, ss_store_sk#14, ss_ticket_number#16]
               :     :     :     :  +- *(5) Filter ((((isnotnull(ss_customer_sk#10) && isnotnull(ss_item_sk#9)) && isnotnull(ss_ticket_number#16)) && isnotnull(ss_store_sk#14)) && isnotnull(ss_sold_date_sk#7))
               :     :     :     :     +- *(5) FileScan parquet tpcds.store_sales[ss_sold_date_sk#7,ss_item_sk#9,ss_customer_sk#10,ss_store_sk#14,ss_ticket_number#16] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_item_sk), IsNotNull(ss_ticket_number), IsNotNull(ss_stor..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ticket_number:int>
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[3, int, true], input[1, int, true], input[2, int, true]))
               :     :     :        +- *(1) Project [sr_returned_date_sk#30, sr_item_sk#32, sr_customer_sk#33, sr_ticket_number#39]
               :     :     :           +- *(1) Filter (((isnotnull(sr_customer_sk#33) && isnotnull(sr_item_sk#32)) && isnotnull(sr_ticket_number#39)) && isnotnull(sr_returned_date_sk#30))
               :     :     :              +- *(1) FileScan parquet tpcds.store_returns[sr_returned_date_sk#30,sr_item_sk#32,sr_customer_sk#33,sr_ticket_number#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_customer_sk), IsNotNull(sr_item_sk), IsNotNull(sr_ticket_number), IsNotNull(sr_retu..., ReadSchema: struct<sr_returned_date_sk:int,sr_item_sk:int,sr_customer_sk:int,sr_ticket_number:int>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(2) Project [s_store_sk#50, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :     :           +- *(2) Filter isnotnull(s_store_sk#50)
               :     :              +- *(2) FileScan parquet tpcds.store[s_store_sk#50,s_store_name#55,s_company_id#66,s_street_number#68,s_street_name#69,s_street_type#70,s_suite_number#71,s_city#72,s_county#73,s_state#74,s_zip#75] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_company_id:int,s_street_number:string,s_street_name:s...
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(3) Project [d_date_sk#79]
               :           +- *(3) Filter isnotnull(d_date_sk#79)
               :              +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#79] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(4) Project [d_date_sk#107]
                     +- *(4) Filter ((((isnotnull(d_year#113) && isnotnull(d_moy#115)) && (d_year#113 = 2001)) && (d_moy#115 = 8)) && isnotnull(d_date_sk#107))
                        +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#107,d_year#113,d_moy#115] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,8), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
Time taken: 4.111 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 50 in stream 0 using template query50.tpl
------------------------------------------------------^^^

