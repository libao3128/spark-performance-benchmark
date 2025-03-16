== Parsed Logical Plan ==
CTE [wss]
:  +- 'SubqueryAlias `wss`
:     +- 'Aggregate ['d_week_seq, 'ss_store_sk], ['d_week_seq, 'ss_store_sk, 'sum(CASE WHEN ('d_day_name = Sunday) THEN 'ss_sales_price ELSE null END) AS sun_sales#20, 'sum(CASE WHEN ('d_day_name = Monday) THEN 'ss_sales_price ELSE null END) AS mon_sales#21, 'sum(CASE WHEN ('d_day_name = Tuesday) THEN 'ss_sales_price ELSE null END) AS tue_sales#22, 'sum(CASE WHEN ('d_day_name = Wednesday) THEN 'ss_sales_price ELSE null END) AS wed_sales#23, 'sum(CASE WHEN ('d_day_name = Thursday) THEN 'ss_sales_price ELSE null END) AS thu_sales#24, 'sum(CASE WHEN ('d_day_name = Friday) THEN 'ss_sales_price ELSE null END) AS fri_sales#25, 'sum(CASE WHEN ('d_day_name = Saturday) THEN 'ss_sales_price ELSE null END) AS sat_sales#26]
:        +- 'Filter ('d_date_sk = 'ss_sold_date_sk)
:           +- 'Join Inner
:              :- 'UnresolvedRelation `store_sales`
:              +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['s_store_name1 ASC NULLS FIRST, 's_store_id1 ASC NULLS FIRST, 'd_week_seq1 ASC NULLS FIRST], true
         +- 'Project ['s_store_name1, 's_store_id1, 'd_week_seq1, unresolvedalias(('sun_sales1 / 'sun_sales2), None), unresolvedalias(('mon_sales1 / 'mon_sales2), None), unresolvedalias(('tue_sales1 / 'tue_sales2), None), unresolvedalias(('wed_sales1 / 'wed_sales2), None), unresolvedalias(('thu_sales1 / 'thu_sales2), None), unresolvedalias(('fri_sales1 / 'fri_sales2), None), unresolvedalias(('sat_sales1 / 'sat_sales2), None)]
            +- 'Filter (('s_store_id1 = 's_store_id2) && ('d_week_seq1 = ('d_week_seq2 - 52)))
               +- 'Join Inner
                  :- 'SubqueryAlias `y`
                  :  +- 'Project ['s_store_name AS s_store_name1#0, 'wss.d_week_seq AS d_week_seq1#1, 's_store_id AS s_store_id1#2, 'sun_sales AS sun_sales1#3, 'mon_sales AS mon_sales1#4, 'tue_sales AS tue_sales1#5, 'wed_sales AS wed_sales1#6, 'thu_sales AS thu_sales1#7, 'fri_sales AS fri_sales1#8, 'sat_sales AS sat_sales1#9]
                  :     +- 'Filter ((('d.d_week_seq = 'wss.d_week_seq) && ('ss_store_sk = 's_store_sk)) && (('d_month_seq >= 1212) && ('d_month_seq <= (1212 + 11))))
                  :        +- 'Join Inner
                  :           :- 'Join Inner
                  :           :  :- 'UnresolvedRelation `wss`
                  :           :  +- 'UnresolvedRelation `store`
                  :           +- 'SubqueryAlias `d`
                  :              +- 'UnresolvedRelation `date_dim`
                  +- 'SubqueryAlias `x`
                     +- 'Project ['s_store_name AS s_store_name2#10, 'wss.d_week_seq AS d_week_seq2#11, 's_store_id AS s_store_id2#12, 'sun_sales AS sun_sales2#13, 'mon_sales AS mon_sales2#14, 'tue_sales AS tue_sales2#15, 'wed_sales AS wed_sales2#16, 'thu_sales AS thu_sales2#17, 'fri_sales AS fri_sales2#18, 'sat_sales AS sat_sales2#19]
                        +- 'Filter ((('d.d_week_seq = 'wss.d_week_seq) && ('ss_store_sk = 's_store_sk)) && (('d_month_seq >= (1212 + 12)) && ('d_month_seq <= (1212 + 23))))
                           +- 'Join Inner
                              :- 'Join Inner
                              :  :- 'UnresolvedRelation `wss`
                              :  +- 'UnresolvedRelation `store`
                              +- 'SubqueryAlias `d`
                                 +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
s_store_name1: string, s_store_id1: string, d_week_seq1: int, (sun_sales1 / sun_sales2): double, (mon_sales1 / mon_sales2): double, (tue_sales1 / tue_sales2): double, (wed_sales1 / wed_sales2): double, (thu_sales1 / thu_sales2): double, (fri_sales1 / fri_sales2): double, (sat_sales1 / sat_sales2): double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name1#0 ASC NULLS FIRST, s_store_id1#2 ASC NULLS FIRST, d_week_seq1#1 ASC NULLS FIRST], true
      +- Project [s_store_name1#0, s_store_id1#2, d_week_seq1#1, (sun_sales1#3 / sun_sales2#13) AS (sun_sales1 / sun_sales2)#172, (mon_sales1#4 / mon_sales2#14) AS (mon_sales1 / mon_sales2)#173, (tue_sales1#5 / tue_sales2#15) AS (tue_sales1 / tue_sales2)#174, (wed_sales1#6 / wed_sales2#16) AS (wed_sales1 / wed_sales2)#175, (thu_sales1#7 / thu_sales2#17) AS (thu_sales1 / thu_sales2)#176, (fri_sales1#8 / fri_sales2#18) AS (fri_sales1 / fri_sales2)#177, (sat_sales1#9 / sat_sales2#19) AS (sat_sales1 / sat_sales2)#178]
         +- Filter ((s_store_id1#2 = s_store_id2#12) && (d_week_seq1#1 = (d_week_seq2#11 - 52)))
            +- Join Inner
               :- SubqueryAlias `y`
               :  +- Project [s_store_name#92 AS s_store_name1#0, d_week_seq#56 AS d_week_seq1#1, s_store_id#88 AS s_store_id1#2, sun_sales#20 AS sun_sales1#3, mon_sales#21 AS mon_sales1#4, tue_sales#22 AS tue_sales1#5, wed_sales#23 AS wed_sales1#6, thu_sales#24 AS thu_sales1#7, fri_sales#25 AS fri_sales1#8, sat_sales#26 AS sat_sales1#9]
               :     +- Filter (((d_week_seq#120 = d_week_seq#56) && (ss_store_sk#36 = s_store_sk#87)) && ((d_month_seq#119 >= 1212) && (d_month_seq#119 <= (1212 + 11))))
               :        +- Join Inner
               :           :- Join Inner
               :           :  :- SubqueryAlias `wss`
               :           :  :  +- Aggregate [d_week_seq#56, ss_store_sk#36], [d_week_seq#56, ss_store_sk#36, sum(CASE WHEN (d_day_name#66 = Sunday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS sun_sales#20, sum(CASE WHEN (d_day_name#66 = Monday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS mon_sales#21, sum(CASE WHEN (d_day_name#66 = Tuesday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS tue_sales#22, sum(CASE WHEN (d_day_name#66 = Wednesday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS wed_sales#23, sum(CASE WHEN (d_day_name#66 = Thursday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS thu_sales#24, sum(CASE WHEN (d_day_name#66 = Friday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS fri_sales#25, sum(CASE WHEN (d_day_name#66 = Saturday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS sat_sales#26]
               :           :  :     +- Filter (d_date_sk#52 = ss_sold_date_sk#29)
               :           :  :        +- Join Inner
               :           :  :           :- SubqueryAlias `tpcds`.`store_sales`
               :           :  :           :  +- Relation[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
               :           :  :           +- SubqueryAlias `tpcds`.`date_dim`
               :           :  :              +- Relation[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet
               :           :  +- SubqueryAlias `tpcds`.`store`
               :           :     +- Relation[s_store_sk#87,s_store_id#88,s_rec_start_date#89,s_rec_end_date#90,s_closed_date_sk#91,s_store_name#92,s_number_employees#93,s_floor_space#94,s_hours#95,s_manager#96,s_market_id#97,s_geography_class#98,s_market_desc#99,s_market_manager#100,s_division_id#101,s_division_name#102,s_company_id#103,s_company_name#104,s_street_number#105,s_street_name#106,s_street_type#107,s_suite_number#108,s_city#109,s_county#110,... 5 more fields] parquet
               :           +- SubqueryAlias `d`
               :              +- SubqueryAlias `tpcds`.`date_dim`
               :                 +- Relation[d_date_sk#116,d_date_id#117,d_date#118,d_month_seq#119,d_week_seq#120,d_quarter_seq#121,d_year#122,d_dow#123,d_moy#124,d_dom#125,d_qoy#126,d_fy_year#127,d_fy_quarter_seq#128,d_fy_week_seq#129,d_day_name#130,d_quarter_name#131,d_holiday#132,d_weekend#133,d_following_holiday#134,d_first_dom#135,d_last_dom#136,d_same_day_ly#137,d_same_day_lq#138,d_current_day#139,... 4 more fields] parquet
               +- SubqueryAlias `x`
                  +- Project [s_store_name#92 AS s_store_name2#10, d_week_seq#56 AS d_week_seq2#11, s_store_id#88 AS s_store_id2#12, sun_sales#20 AS sun_sales2#13, mon_sales#21 AS mon_sales2#14, tue_sales#22 AS tue_sales2#15, wed_sales#23 AS wed_sales2#16, thu_sales#24 AS thu_sales2#17, fri_sales#25 AS fri_sales2#18, sat_sales#26 AS sat_sales2#19]
                     +- Filter (((d_week_seq#148 = d_week_seq#56) && (ss_store_sk#36 = s_store_sk#87)) && ((d_month_seq#147 >= (1212 + 12)) && (d_month_seq#147 <= (1212 + 23))))
                        +- Join Inner
                           :- Join Inner
                           :  :- SubqueryAlias `wss`
                           :  :  +- Aggregate [d_week_seq#56, ss_store_sk#36], [d_week_seq#56, ss_store_sk#36, sum(CASE WHEN (d_day_name#66 = Sunday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS sun_sales#20, sum(CASE WHEN (d_day_name#66 = Monday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS mon_sales#21, sum(CASE WHEN (d_day_name#66 = Tuesday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS tue_sales#22, sum(CASE WHEN (d_day_name#66 = Wednesday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS wed_sales#23, sum(CASE WHEN (d_day_name#66 = Thursday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS thu_sales#24, sum(CASE WHEN (d_day_name#66 = Friday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS fri_sales#25, sum(CASE WHEN (d_day_name#66 = Saturday) THEN ss_sales_price#42 ELSE cast(null as double) END) AS sat_sales#26]
                           :  :     +- Filter (d_date_sk#52 = ss_sold_date_sk#29)
                           :  :        +- Join Inner
                           :  :           :- SubqueryAlias `tpcds`.`store_sales`
                           :  :           :  +- Relation[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
                           :  :           +- SubqueryAlias `tpcds`.`date_dim`
                           :  :              +- Relation[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet
                           :  +- SubqueryAlias `tpcds`.`store`
                           :     +- Relation[s_store_sk#87,s_store_id#88,s_rec_start_date#89,s_rec_end_date#90,s_closed_date_sk#91,s_store_name#92,s_number_employees#93,s_floor_space#94,s_hours#95,s_manager#96,s_market_id#97,s_geography_class#98,s_market_desc#99,s_market_manager#100,s_division_id#101,s_division_name#102,s_company_id#103,s_company_name#104,s_street_number#105,s_street_name#106,s_street_type#107,s_suite_number#108,s_city#109,s_county#110,... 5 more fields] parquet
                           +- SubqueryAlias `d`
                              +- SubqueryAlias `tpcds`.`date_dim`
                                 +- Relation[d_date_sk#144,d_date_id#145,d_date#146,d_month_seq#147,d_week_seq#148,d_quarter_seq#149,d_year#150,d_dow#151,d_moy#152,d_dom#153,d_qoy#154,d_fy_year#155,d_fy_quarter_seq#156,d_fy_week_seq#157,d_day_name#158,d_quarter_name#159,d_holiday#160,d_weekend#161,d_following_holiday#162,d_first_dom#163,d_last_dom#164,d_same_day_ly#165,d_same_day_lq#166,d_current_day#167,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name1#0 ASC NULLS FIRST, s_store_id1#2 ASC NULLS FIRST, d_week_seq1#1 ASC NULLS FIRST], true
      +- Project [s_store_name1#0, s_store_id1#2, d_week_seq1#1, (sun_sales1#3 / sun_sales2#13) AS (sun_sales1 / sun_sales2)#172, (mon_sales1#4 / mon_sales2#14) AS (mon_sales1 / mon_sales2)#173, (tue_sales1#5 / tue_sales2#15) AS (tue_sales1 / tue_sales2)#174, (wed_sales1#6 / wed_sales2#16) AS (wed_sales1 / wed_sales2)#175, (thu_sales1#7 / thu_sales2#17) AS (thu_sales1 / thu_sales2)#176, (fri_sales1#8 / fri_sales2#18) AS (fri_sales1 / fri_sales2)#177, (sat_sales1#9 / sat_sales2#19) AS (sat_sales1 / sat_sales2)#178]
         +- Join Inner, ((s_store_id1#2 = s_store_id2#12) && (d_week_seq1#1 = (d_week_seq2#11 - 52)))
            :- Project [s_store_name#92 AS s_store_name1#0, d_week_seq#56 AS d_week_seq1#1, s_store_id#88 AS s_store_id1#2, sun_sales#20 AS sun_sales1#3, mon_sales#21 AS mon_sales1#4, tue_sales#22 AS tue_sales1#5, wed_sales#23 AS wed_sales1#6, thu_sales#24 AS thu_sales1#7, fri_sales#25 AS fri_sales1#8, sat_sales#26 AS sat_sales1#9]
            :  +- Join Inner, (d_week_seq#120 = d_week_seq#56)
            :     :- Project [d_week_seq#56, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26, s_store_id#88, s_store_name#92]
            :     :  +- Join Inner, (ss_store_sk#36 = s_store_sk#87)
            :     :     :- Aggregate [d_week_seq#56, ss_store_sk#36], [d_week_seq#56, ss_store_sk#36, sum(CASE WHEN (d_day_name#66 = Sunday) THEN ss_sales_price#42 ELSE null END) AS sun_sales#20, sum(CASE WHEN (d_day_name#66 = Monday) THEN ss_sales_price#42 ELSE null END) AS mon_sales#21, sum(CASE WHEN (d_day_name#66 = Tuesday) THEN ss_sales_price#42 ELSE null END) AS tue_sales#22, sum(CASE WHEN (d_day_name#66 = Wednesday) THEN ss_sales_price#42 ELSE null END) AS wed_sales#23, sum(CASE WHEN (d_day_name#66 = Thursday) THEN ss_sales_price#42 ELSE null END) AS thu_sales#24, sum(CASE WHEN (d_day_name#66 = Friday) THEN ss_sales_price#42 ELSE null END) AS fri_sales#25, sum(CASE WHEN (d_day_name#66 = Saturday) THEN ss_sales_price#42 ELSE null END) AS sat_sales#26]
            :     :     :  +- Project [ss_store_sk#36, ss_sales_price#42, d_week_seq#56, d_day_name#66]
            :     :     :     +- Join Inner, (d_date_sk#52 = ss_sold_date_sk#29)
            :     :     :        :- Project [ss_sold_date_sk#29, ss_store_sk#36, ss_sales_price#42]
            :     :     :        :  +- Filter (isnotnull(ss_sold_date_sk#29) && isnotnull(ss_store_sk#36))
            :     :     :        :     +- Relation[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
            :     :     :        +- Project [d_date_sk#52, d_week_seq#56, d_day_name#66]
            :     :     :           +- Filter (isnotnull(d_date_sk#52) && isnotnull(d_week_seq#56))
            :     :     :              +- Relation[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet
            :     :     +- Project [s_store_sk#87, s_store_id#88, s_store_name#92]
            :     :        +- Filter (isnotnull(s_store_sk#87) && isnotnull(s_store_id#88))
            :     :           +- Relation[s_store_sk#87,s_store_id#88,s_rec_start_date#89,s_rec_end_date#90,s_closed_date_sk#91,s_store_name#92,s_number_employees#93,s_floor_space#94,s_hours#95,s_manager#96,s_market_id#97,s_geography_class#98,s_market_desc#99,s_market_manager#100,s_division_id#101,s_division_name#102,s_company_id#103,s_company_name#104,s_street_number#105,s_street_name#106,s_street_type#107,s_suite_number#108,s_city#109,s_county#110,... 5 more fields] parquet
            :     +- Project [d_week_seq#120]
            :        +- Filter (((isnotnull(d_month_seq#119) && (d_month_seq#119 >= 1212)) && (d_month_seq#119 <= 1223)) && isnotnull(d_week_seq#120))
            :           +- Relation[d_date_sk#116,d_date_id#117,d_date#118,d_month_seq#119,d_week_seq#120,d_quarter_seq#121,d_year#122,d_dow#123,d_moy#124,d_dom#125,d_qoy#126,d_fy_year#127,d_fy_quarter_seq#128,d_fy_week_seq#129,d_day_name#130,d_quarter_name#131,d_holiday#132,d_weekend#133,d_following_holiday#134,d_first_dom#135,d_last_dom#136,d_same_day_ly#137,d_same_day_lq#138,d_current_day#139,... 4 more fields] parquet
            +- Project [d_week_seq#56 AS d_week_seq2#11, s_store_id#88 AS s_store_id2#12, sun_sales#20 AS sun_sales2#13, mon_sales#21 AS mon_sales2#14, tue_sales#22 AS tue_sales2#15, wed_sales#23 AS wed_sales2#16, thu_sales#24 AS thu_sales2#17, fri_sales#25 AS fri_sales2#18, sat_sales#26 AS sat_sales2#19]
               +- Join Inner, (d_week_seq#148 = d_week_seq#56)
                  :- Project [d_week_seq#56, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26, s_store_id#88]
                  :  +- Join Inner, (ss_store_sk#36 = s_store_sk#87)
                  :     :- Aggregate [d_week_seq#56, ss_store_sk#36], [d_week_seq#56, ss_store_sk#36, sum(CASE WHEN (d_day_name#66 = Sunday) THEN ss_sales_price#42 ELSE null END) AS sun_sales#20, sum(CASE WHEN (d_day_name#66 = Monday) THEN ss_sales_price#42 ELSE null END) AS mon_sales#21, sum(CASE WHEN (d_day_name#66 = Tuesday) THEN ss_sales_price#42 ELSE null END) AS tue_sales#22, sum(CASE WHEN (d_day_name#66 = Wednesday) THEN ss_sales_price#42 ELSE null END) AS wed_sales#23, sum(CASE WHEN (d_day_name#66 = Thursday) THEN ss_sales_price#42 ELSE null END) AS thu_sales#24, sum(CASE WHEN (d_day_name#66 = Friday) THEN ss_sales_price#42 ELSE null END) AS fri_sales#25, sum(CASE WHEN (d_day_name#66 = Saturday) THEN ss_sales_price#42 ELSE null END) AS sat_sales#26]
                  :     :  +- Project [ss_store_sk#36, ss_sales_price#42, d_week_seq#56, d_day_name#66]
                  :     :     +- Join Inner, (d_date_sk#52 = ss_sold_date_sk#29)
                  :     :        :- Project [ss_sold_date_sk#29, ss_store_sk#36, ss_sales_price#42]
                  :     :        :  +- Filter (isnotnull(ss_sold_date_sk#29) && isnotnull(ss_store_sk#36))
                  :     :        :     +- Relation[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
                  :     :        +- Project [d_date_sk#52, d_week_seq#56, d_day_name#66]
                  :     :           +- Filter (isnotnull(d_date_sk#52) && isnotnull(d_week_seq#56))
                  :     :              +- Relation[d_date_sk#52,d_date_id#53,d_date#54,d_month_seq#55,d_week_seq#56,d_quarter_seq#57,d_year#58,d_dow#59,d_moy#60,d_dom#61,d_qoy#62,d_fy_year#63,d_fy_quarter_seq#64,d_fy_week_seq#65,d_day_name#66,d_quarter_name#67,d_holiday#68,d_weekend#69,d_following_holiday#70,d_first_dom#71,d_last_dom#72,d_same_day_ly#73,d_same_day_lq#74,d_current_day#75,... 4 more fields] parquet
                  :     +- Project [s_store_sk#87, s_store_id#88]
                  :        +- Filter (isnotnull(s_store_sk#87) && isnotnull(s_store_id#88))
                  :           +- Relation[s_store_sk#87,s_store_id#88,s_rec_start_date#89,s_rec_end_date#90,s_closed_date_sk#91,s_store_name#92,s_number_employees#93,s_floor_space#94,s_hours#95,s_manager#96,s_market_id#97,s_geography_class#98,s_market_desc#99,s_market_manager#100,s_division_id#101,s_division_name#102,s_company_id#103,s_company_name#104,s_street_number#105,s_street_name#106,s_street_type#107,s_suite_number#108,s_city#109,s_county#110,... 5 more fields] parquet
                  +- Project [d_week_seq#148]
                     +- Filter (((isnotnull(d_month_seq#147) && (d_month_seq#147 >= 1224)) && (d_month_seq#147 <= 1235)) && isnotnull(d_week_seq#148))
                        +- Relation[d_date_sk#144,d_date_id#145,d_date#146,d_month_seq#147,d_week_seq#148,d_quarter_seq#149,d_year#150,d_dow#151,d_moy#152,d_dom#153,d_qoy#154,d_fy_year#155,d_fy_quarter_seq#156,d_fy_week_seq#157,d_day_name#158,d_quarter_name#159,d_holiday#160,d_weekend#161,d_following_holiday#162,d_first_dom#163,d_last_dom#164,d_same_day_ly#165,d_same_day_lq#166,d_current_day#167,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[s_store_name1#0 ASC NULLS FIRST,s_store_id1#2 ASC NULLS FIRST,d_week_seq1#1 ASC NULLS FIRST], output=[s_store_name1#0,s_store_id1#2,d_week_seq1#1,(sun_sales1 / sun_sales2)#172,(mon_sales1 / mon_sales2)#173,(tue_sales1 / tue_sales2)#174,(wed_sales1 / wed_sales2)#175,(thu_sales1 / thu_sales2)#176,(fri_sales1 / fri_sales2)#177,(sat_sales1 / sat_sales2)#178])
+- *(13) Project [s_store_name1#0, s_store_id1#2, d_week_seq1#1, (sun_sales1#3 / sun_sales2#13) AS (sun_sales1 / sun_sales2)#172, (mon_sales1#4 / mon_sales2#14) AS (mon_sales1 / mon_sales2)#173, (tue_sales1#5 / tue_sales2#15) AS (tue_sales1 / tue_sales2)#174, (wed_sales1#6 / wed_sales2#16) AS (wed_sales1 / wed_sales2)#175, (thu_sales1#7 / thu_sales2#17) AS (thu_sales1 / thu_sales2)#176, (fri_sales1#8 / fri_sales2#18) AS (fri_sales1 / fri_sales2)#177, (sat_sales1#9 / sat_sales2#19) AS (sat_sales1 / sat_sales2)#178]
   +- *(13) SortMergeJoin [s_store_id1#2, d_week_seq1#1], [s_store_id2#12, (d_week_seq2#11 - 52)], Inner
      :- *(6) Sort [s_store_id1#2 ASC NULLS FIRST, d_week_seq1#1 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(s_store_id1#2, d_week_seq1#1, 200)
      :     +- *(5) Project [s_store_name#92 AS s_store_name1#0, d_week_seq#56 AS d_week_seq1#1, s_store_id#88 AS s_store_id1#2, sun_sales#20 AS sun_sales1#3, mon_sales#21 AS mon_sales1#4, tue_sales#22 AS tue_sales1#5, wed_sales#23 AS wed_sales1#6, thu_sales#24 AS thu_sales1#7, fri_sales#25 AS fri_sales1#8, sat_sales#26 AS sat_sales1#9]
      :        +- *(5) BroadcastHashJoin [d_week_seq#56], [d_week_seq#120], Inner, BuildRight
      :           :- *(5) Project [d_week_seq#56, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26, s_store_id#88, s_store_name#92]
      :           :  +- *(5) BroadcastHashJoin [ss_store_sk#36], [s_store_sk#87], Inner, BuildRight
      :           :     :- *(5) HashAggregate(keys=[d_week_seq#56, ss_store_sk#36], functions=[sum(CASE WHEN (d_day_name#66 = Sunday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Monday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Tuesday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Wednesday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Thursday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Friday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Saturday) THEN ss_sales_price#42 ELSE null END)], output=[d_week_seq#56, ss_store_sk#36, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26])
      :           :     :  +- Exchange hashpartitioning(d_week_seq#56, ss_store_sk#36, 200)
      :           :     :     +- *(2) HashAggregate(keys=[d_week_seq#56, ss_store_sk#36], functions=[partial_sum(CASE WHEN (d_day_name#66 = Sunday) THEN ss_sales_price#42 ELSE null END), partial_sum(CASE WHEN (d_day_name#66 = Monday) THEN ss_sales_price#42 ELSE null END), partial_sum(CASE WHEN (d_day_name#66 = Tuesday) THEN ss_sales_price#42 ELSE null END), partial_sum(CASE WHEN (d_day_name#66 = Wednesday) THEN ss_sales_price#42 ELSE null END), partial_sum(CASE WHEN (d_day_name#66 = Thursday) THEN ss_sales_price#42 ELSE null END), partial_sum(CASE WHEN (d_day_name#66 = Friday) THEN ss_sales_price#42 ELSE null END), partial_sum(CASE WHEN (d_day_name#66 = Saturday) THEN ss_sales_price#42 ELSE null END)], output=[d_week_seq#56, ss_store_sk#36, sum#186, sum#187, sum#188, sum#189, sum#190, sum#191, sum#192])
      :           :     :        +- *(2) Project [ss_store_sk#36, ss_sales_price#42, d_week_seq#56, d_day_name#66]
      :           :     :           +- *(2) BroadcastHashJoin [ss_sold_date_sk#29], [d_date_sk#52], Inner, BuildRight
      :           :     :              :- *(2) Project [ss_sold_date_sk#29, ss_store_sk#36, ss_sales_price#42]
      :           :     :              :  +- *(2) Filter (isnotnull(ss_sold_date_sk#29) && isnotnull(ss_store_sk#36))
      :           :     :              :     +- *(2) FileScan parquet tpcds.store_sales[ss_sold_date_sk#29,ss_store_sk#36,ss_sales_price#42] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_sales_price:double>
      :           :     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :           :     :                 +- *(1) Project [d_date_sk#52, d_week_seq#56, d_day_name#66]
      :           :     :                    +- *(1) Filter (isnotnull(d_date_sk#52) && isnotnull(d_week_seq#56))
      :           :     :                       +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#52,d_week_seq#56,d_day_name#66] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk), IsNotNull(d_week_seq)], ReadSchema: struct<d_date_sk:int,d_week_seq:int,d_day_name:string>
      :           :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :           :        +- *(3) Project [s_store_sk#87, s_store_id#88, s_store_name#92]
      :           :           +- *(3) Filter (isnotnull(s_store_sk#87) && isnotnull(s_store_id#88))
      :           :              +- *(3) FileScan parquet tpcds.store[s_store_sk#87,s_store_id#88,s_store_name#92] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_store_id)], ReadSchema: struct<s_store_sk:int,s_store_id:string,s_store_name:string>
      :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :              +- *(4) Project [d_week_seq#120]
      :                 +- *(4) Filter (((isnotnull(d_month_seq#119) && (d_month_seq#119 >= 1212)) && (d_month_seq#119 <= 1223)) && isnotnull(d_week_seq#120))
      :                    +- *(4) FileScan parquet tpcds.date_dim[d_month_seq#119,d_week_seq#120] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1212), LessThanOrEqual(d_month_seq,1223),..., ReadSchema: struct<d_month_seq:int,d_week_seq:int>
      +- *(12) Sort [s_store_id2#12 ASC NULLS FIRST, (d_week_seq2#11 - 52) ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(s_store_id2#12, (d_week_seq2#11 - 52), 200)
            +- *(11) Project [d_week_seq#56 AS d_week_seq2#11, s_store_id#88 AS s_store_id2#12, sun_sales#20 AS sun_sales2#13, mon_sales#21 AS mon_sales2#14, tue_sales#22 AS tue_sales2#15, wed_sales#23 AS wed_sales2#16, thu_sales#24 AS thu_sales2#17, fri_sales#25 AS fri_sales2#18, sat_sales#26 AS sat_sales2#19]
               +- *(11) BroadcastHashJoin [d_week_seq#56], [d_week_seq#148], Inner, BuildRight
                  :- *(11) Project [d_week_seq#56, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26, s_store_id#88]
                  :  +- *(11) BroadcastHashJoin [ss_store_sk#36], [s_store_sk#87], Inner, BuildRight
                  :     :- *(11) HashAggregate(keys=[d_week_seq#56, ss_store_sk#36], functions=[sum(CASE WHEN (d_day_name#66 = Sunday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Monday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Tuesday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Wednesday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Thursday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Friday) THEN ss_sales_price#42 ELSE null END), sum(CASE WHEN (d_day_name#66 = Saturday) THEN ss_sales_price#42 ELSE null END)], output=[d_week_seq#56, ss_store_sk#36, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26])
                  :     :  +- ReusedExchange [d_week_seq#56, ss_store_sk#36, sum#200, sum#201, sum#202, sum#203, sum#204, sum#205, sum#206], Exchange hashpartitioning(d_week_seq#56, ss_store_sk#36, 200)
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :        +- *(9) Project [s_store_sk#87, s_store_id#88]
                  :           +- *(9) Filter (isnotnull(s_store_sk#87) && isnotnull(s_store_id#88))
                  :              +- *(9) FileScan parquet tpcds.store[s_store_sk#87,s_store_id#88] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_store_id)], ReadSchema: struct<s_store_sk:int,s_store_id:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     +- *(10) Project [d_week_seq#148]
                        +- *(10) Filter (((isnotnull(d_month_seq#147) && (d_month_seq#147 >= 1224)) && (d_month_seq#147 <= 1235)) && isnotnull(d_week_seq#148))
                           +- *(10) FileScan parquet tpcds.date_dim[d_month_seq#147,d_week_seq#148] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1224), LessThanOrEqual(d_month_seq,1235),..., ReadSchema: struct<d_month_seq:int,d_week_seq:int>
Time taken: 4.816 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 59 in stream 0 using template query59.tpl
------------------------------------------------------^^^

