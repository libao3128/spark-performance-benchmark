Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741581823011
== Parsed Logical Plan ==
CTE [wss]
:  +- 'SubqueryAlias wss
:     +- 'Aggregate ['d_week_seq, 'ss_store_sk], ['d_week_seq, 'ss_store_sk, 'sum(CASE WHEN ('d_day_name = Sunday) THEN 'ss_sales_price ELSE null END) AS sun_sales#20, 'sum(CASE WHEN ('d_day_name = Monday) THEN 'ss_sales_price ELSE null END) AS mon_sales#21, 'sum(CASE WHEN ('d_day_name = Tuesday) THEN 'ss_sales_price ELSE null END) AS tue_sales#22, 'sum(CASE WHEN ('d_day_name = Wednesday) THEN 'ss_sales_price ELSE null END) AS wed_sales#23, 'sum(CASE WHEN ('d_day_name = Thursday) THEN 'ss_sales_price ELSE null END) AS thu_sales#24, 'sum(CASE WHEN ('d_day_name = Friday) THEN 'ss_sales_price ELSE null END) AS fri_sales#25, 'sum(CASE WHEN ('d_day_name = Saturday) THEN 'ss_sales_price ELSE null END) AS sat_sales#26]
:        +- 'Filter ('d_date_sk = 'ss_sold_date_sk)
:           +- 'Join Inner
:              :- 'UnresolvedRelation [store_sales], [], false
:              +- 'UnresolvedRelation [date_dim], [], false
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['s_store_name1 ASC NULLS FIRST, 's_store_id1 ASC NULLS FIRST, 'd_week_seq1 ASC NULLS FIRST], true
         +- 'Project ['s_store_name1, 's_store_id1, 'd_week_seq1, unresolvedalias(('sun_sales1 / 'sun_sales2), None), unresolvedalias(('mon_sales1 / 'mon_sales2), None), unresolvedalias(('tue_sales1 / 'tue_sales2), None), unresolvedalias(('wed_sales1 / 'wed_sales2), None), unresolvedalias(('thu_sales1 / 'thu_sales2), None), unresolvedalias(('fri_sales1 / 'fri_sales2), None), unresolvedalias(('sat_sales1 / 'sat_sales2), None)]
            +- 'Filter (('s_store_id1 = 's_store_id2) AND ('d_week_seq1 = ('d_week_seq2 - 52)))
               +- 'Join Inner
                  :- 'SubqueryAlias y
                  :  +- 'Project ['s_store_name AS s_store_name1#0, 'wss.d_week_seq AS d_week_seq1#1, 's_store_id AS s_store_id1#2, 'sun_sales AS sun_sales1#3, 'mon_sales AS mon_sales1#4, 'tue_sales AS tue_sales1#5, 'wed_sales AS wed_sales1#6, 'thu_sales AS thu_sales1#7, 'fri_sales AS fri_sales1#8, 'sat_sales AS sat_sales1#9]
                  :     +- 'Filter ((('d.d_week_seq = 'wss.d_week_seq) AND ('ss_store_sk = 's_store_sk)) AND (('d_month_seq >= 1212) AND ('d_month_seq <= (1212 + 11))))
                  :        +- 'Join Inner
                  :           :- 'Join Inner
                  :           :  :- 'UnresolvedRelation [wss], [], false
                  :           :  +- 'UnresolvedRelation [store], [], false
                  :           +- 'SubqueryAlias d
                  :              +- 'UnresolvedRelation [date_dim], [], false
                  +- 'SubqueryAlias x
                     +- 'Project ['s_store_name AS s_store_name2#10, 'wss.d_week_seq AS d_week_seq2#11, 's_store_id AS s_store_id2#12, 'sun_sales AS sun_sales2#13, 'mon_sales AS mon_sales2#14, 'tue_sales AS tue_sales2#15, 'wed_sales AS wed_sales2#16, 'thu_sales AS thu_sales2#17, 'fri_sales AS fri_sales2#18, 'sat_sales AS sat_sales2#19]
                        +- 'Filter ((('d.d_week_seq = 'wss.d_week_seq) AND ('ss_store_sk = 's_store_sk)) AND (('d_month_seq >= (1212 + 12)) AND ('d_month_seq <= (1212 + 23))))
                           +- 'Join Inner
                              :- 'Join Inner
                              :  :- 'UnresolvedRelation [wss], [], false
                              :  +- 'UnresolvedRelation [store], [], false
                              +- 'SubqueryAlias d
                                 +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
s_store_name1: string, s_store_id1: string, d_week_seq1: int, (sun_sales1 / sun_sales2): double, (mon_sales1 / mon_sales2): double, (tue_sales1 / tue_sales2): double, (wed_sales1 / wed_sales2): double, (thu_sales1 / thu_sales2): double, (fri_sales1 / fri_sales2): double, (sat_sales1 / sat_sales2): double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias wss
:     +- Aggregate [d_week_seq#59, ss_store_sk#39], [d_week_seq#59, ss_store_sk#39, sum(CASE WHEN (d_day_name#69 = Sunday) THEN ss_sales_price#45 ELSE cast(null as double) END) AS sun_sales#20, sum(CASE WHEN (d_day_name#69 = Monday) THEN ss_sales_price#45 ELSE cast(null as double) END) AS mon_sales#21, sum(CASE WHEN (d_day_name#69 = Tuesday) THEN ss_sales_price#45 ELSE cast(null as double) END) AS tue_sales#22, sum(CASE WHEN (d_day_name#69 = Wednesday) THEN ss_sales_price#45 ELSE cast(null as double) END) AS wed_sales#23, sum(CASE WHEN (d_day_name#69 = Thursday) THEN ss_sales_price#45 ELSE cast(null as double) END) AS thu_sales#24, sum(CASE WHEN (d_day_name#69 = Friday) THEN ss_sales_price#45 ELSE cast(null as double) END) AS fri_sales#25, sum(CASE WHEN (d_day_name#69 = Saturday) THEN ss_sales_price#45 ELSE cast(null as double) END) AS sat_sales#26]
:        +- Filter (d_date_sk#55 = ss_sold_date_sk#32)
:           +- Join Inner
:              :- SubqueryAlias spark_catalog.tpcds.store_sales
:              :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#32,ss_sold_time_sk#33,ss_item_sk#34,ss_customer_sk#35,ss_cdemo_sk#36,ss_hdemo_sk#37,ss_addr_sk#38,ss_store_sk#39,ss_promo_sk#40,ss_ticket_number#41,ss_quantity#42,ss_wholesale_cost#43,ss_list_price#44,ss_sales_price#45,ss_ext_discount_amt#46,ss_ext_sales_price#47,ss_ext_wholesale_cost#48,ss_ext_list_price#49,ss_ext_tax#50,ss_coupon_amt#51,ss_net_paid#52,ss_net_paid_inc_tax#53,ss_net_profit#54] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [s_store_name1#0 ASC NULLS FIRST, s_store_id1#2 ASC NULLS FIRST, d_week_seq1#1 ASC NULLS FIRST], true
         +- Project [s_store_name1#0, s_store_id1#2, d_week_seq1#1, (sun_sales1#3 / sun_sales2#13) AS (sun_sales1 / sun_sales2)#215, (mon_sales1#4 / mon_sales2#14) AS (mon_sales1 / mon_sales2)#216, (tue_sales1#5 / tue_sales2#15) AS (tue_sales1 / tue_sales2)#217, (wed_sales1#6 / wed_sales2#16) AS (wed_sales1 / wed_sales2)#218, (thu_sales1#7 / thu_sales2#17) AS (thu_sales1 / thu_sales2)#219, (fri_sales1#8 / fri_sales2#18) AS (fri_sales1 / fri_sales2)#220, (sat_sales1#9 / sat_sales2#19) AS (sat_sales1 / sat_sales2)#221]
            +- Filter ((s_store_id1#2 = s_store_id2#12) AND (d_week_seq1#1 = (d_week_seq2#11 - 52)))
               +- Join Inner
                  :- SubqueryAlias y
                  :  +- Project [s_store_name#88 AS s_store_name1#0, d_week_seq#59 AS d_week_seq1#1, s_store_id#84 AS s_store_id1#2, sun_sales#20 AS sun_sales1#3, mon_sales#21 AS mon_sales1#4, tue_sales#22 AS tue_sales1#5, wed_sales#23 AS wed_sales1#6, thu_sales#24 AS thu_sales1#7, fri_sales#25 AS fri_sales1#8, sat_sales#26 AS sat_sales1#9]
                  :     +- Filter (((d_week_seq#116 = d_week_seq#59) AND (ss_store_sk#39 = s_store_sk#83)) AND ((d_month_seq#115 >= 1212) AND (d_month_seq#115 <= (1212 + 11))))
                  :        +- Join Inner
                  :           :- Join Inner
                  :           :  :- SubqueryAlias wss
                  :           :  :  +- CTERelationRef 0, true, [d_week_seq#59, ss_store_sk#39, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26], false
                  :           :  +- SubqueryAlias spark_catalog.tpcds.store
                  :           :     +- Relation spark_catalog.tpcds.store[s_store_sk#83,s_store_id#84,s_rec_start_date#85,s_rec_end_date#86,s_closed_date_sk#87,s_store_name#88,s_number_employees#89,s_floor_space#90,s_hours#91,s_manager#92,s_market_id#93,s_geography_class#94,s_market_desc#95,s_market_manager#96,s_division_id#97,s_division_name#98,s_company_id#99,s_company_name#100,s_street_number#101,s_street_name#102,s_street_type#103,s_suite_number#104,s_city#105,s_county#106,... 5 more fields] parquet
                  :           +- SubqueryAlias d
                  :              +- SubqueryAlias spark_catalog.tpcds.date_dim
                  :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#112,d_date_id#113,d_date#114,d_month_seq#115,d_week_seq#116,d_quarter_seq#117,d_year#118,d_dow#119,d_moy#120,d_dom#121,d_qoy#122,d_fy_year#123,d_fy_quarter_seq#124,d_fy_week_seq#125,d_day_name#126,d_quarter_name#127,d_holiday#128,d_weekend#129,d_following_holiday#130,d_first_dom#131,d_last_dom#132,d_same_day_ly#133,d_same_day_lq#134,d_current_day#135,... 4 more fields] parquet
                  +- SubqueryAlias x
                     +- Project [s_store_name#145 AS s_store_name2#10, d_week_seq#206 AS d_week_seq2#11, s_store_id#141 AS s_store_id2#12, sun_sales#208 AS sun_sales2#13, mon_sales#209 AS mon_sales2#14, tue_sales#210 AS tue_sales2#15, wed_sales#211 AS wed_sales2#16, thu_sales#212 AS thu_sales2#17, fri_sales#213 AS fri_sales2#18, sat_sales#214 AS sat_sales2#19]
                        +- Filter (((d_week_seq#173 = d_week_seq#206) AND (ss_store_sk#207 = s_store_sk#140)) AND ((d_month_seq#172 >= (1212 + 12)) AND (d_month_seq#172 <= (1212 + 23))))
                           +- Join Inner
                              :- Join Inner
                              :  :- SubqueryAlias wss
                              :  :  +- CTERelationRef 0, true, [d_week_seq#206, ss_store_sk#207, sun_sales#208, mon_sales#209, tue_sales#210, wed_sales#211, thu_sales#212, fri_sales#213, sat_sales#214], false
                              :  +- SubqueryAlias spark_catalog.tpcds.store
                              :     +- Relation spark_catalog.tpcds.store[s_store_sk#140,s_store_id#141,s_rec_start_date#142,s_rec_end_date#143,s_closed_date_sk#144,s_store_name#145,s_number_employees#146,s_floor_space#147,s_hours#148,s_manager#149,s_market_id#150,s_geography_class#151,s_market_desc#152,s_market_manager#153,s_division_id#154,s_division_name#155,s_company_id#156,s_company_name#157,s_street_number#158,s_street_name#159,s_street_type#160,s_suite_number#161,s_city#162,s_county#163,... 5 more fields] parquet
                              +- SubqueryAlias d
                                 +- SubqueryAlias spark_catalog.tpcds.date_dim
                                    +- Relation spark_catalog.tpcds.date_dim[d_date_sk#169,d_date_id#170,d_date#171,d_month_seq#172,d_week_seq#173,d_quarter_seq#174,d_year#175,d_dow#176,d_moy#177,d_dom#178,d_qoy#179,d_fy_year#180,d_fy_quarter_seq#181,d_fy_week_seq#182,d_day_name#183,d_quarter_name#184,d_holiday#185,d_weekend#186,d_following_holiday#187,d_first_dom#188,d_last_dom#189,d_same_day_ly#190,d_same_day_lq#191,d_current_day#192,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name1#0 ASC NULLS FIRST, s_store_id1#2 ASC NULLS FIRST, d_week_seq1#1 ASC NULLS FIRST], true
      +- Project [s_store_name1#0, s_store_id1#2, d_week_seq1#1, (sun_sales1#3 / sun_sales2#13) AS (sun_sales1 / sun_sales2)#215, (mon_sales1#4 / mon_sales2#14) AS (mon_sales1 / mon_sales2)#216, (tue_sales1#5 / tue_sales2#15) AS (tue_sales1 / tue_sales2)#217, (wed_sales1#6 / wed_sales2#16) AS (wed_sales1 / wed_sales2)#218, (thu_sales1#7 / thu_sales2#17) AS (thu_sales1 / thu_sales2)#219, (fri_sales1#8 / fri_sales2#18) AS (fri_sales1 / fri_sales2)#220, (sat_sales1#9 / sat_sales2#19) AS (sat_sales1 / sat_sales2)#221]
         +- Join Inner, ((s_store_id1#2 = s_store_id2#12) AND (d_week_seq1#1 = (d_week_seq2#11 - 52)))
            :- Project [s_store_name#88 AS s_store_name1#0, d_week_seq#59 AS d_week_seq1#1, s_store_id#84 AS s_store_id1#2, sun_sales#20 AS sun_sales1#3, mon_sales#21 AS mon_sales1#4, tue_sales#22 AS tue_sales1#5, wed_sales#23 AS wed_sales1#6, thu_sales#24 AS thu_sales1#7, fri_sales#25 AS fri_sales1#8, sat_sales#26 AS sat_sales1#9]
            :  +- Join Inner, (d_week_seq#116 = d_week_seq#59)
            :     :- Project [d_week_seq#59, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26, s_store_id#84, s_store_name#88]
            :     :  +- Join Inner, (ss_store_sk#39 = s_store_sk#83)
            :     :     :- Aggregate [d_week_seq#59, ss_store_sk#39], [d_week_seq#59, ss_store_sk#39, sum(CASE WHEN (d_day_name#69 = Sunday) THEN ss_sales_price#45 END) AS sun_sales#20, sum(CASE WHEN (d_day_name#69 = Monday) THEN ss_sales_price#45 END) AS mon_sales#21, sum(CASE WHEN (d_day_name#69 = Tuesday) THEN ss_sales_price#45 END) AS tue_sales#22, sum(CASE WHEN (d_day_name#69 = Wednesday) THEN ss_sales_price#45 END) AS wed_sales#23, sum(CASE WHEN (d_day_name#69 = Thursday) THEN ss_sales_price#45 END) AS thu_sales#24, sum(CASE WHEN (d_day_name#69 = Friday) THEN ss_sales_price#45 END) AS fri_sales#25, sum(CASE WHEN (d_day_name#69 = Saturday) THEN ss_sales_price#45 END) AS sat_sales#26]
            :     :     :  +- Project [ss_store_sk#39, ss_sales_price#45, d_week_seq#59, d_day_name#69]
            :     :     :     +- Join Inner, (d_date_sk#55 = ss_sold_date_sk#32)
            :     :     :        :- Project [ss_sold_date_sk#32, ss_store_sk#39, ss_sales_price#45]
            :     :     :        :  +- Filter (isnotnull(ss_sold_date_sk#32) AND isnotnull(ss_store_sk#39))
            :     :     :        :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#32,ss_sold_time_sk#33,ss_item_sk#34,ss_customer_sk#35,ss_cdemo_sk#36,ss_hdemo_sk#37,ss_addr_sk#38,ss_store_sk#39,ss_promo_sk#40,ss_ticket_number#41,ss_quantity#42,ss_wholesale_cost#43,ss_list_price#44,ss_sales_price#45,ss_ext_discount_amt#46,ss_ext_sales_price#47,ss_ext_wholesale_cost#48,ss_ext_list_price#49,ss_ext_tax#50,ss_coupon_amt#51,ss_net_paid#52,ss_net_paid_inc_tax#53,ss_net_profit#54] parquet
            :     :     :        +- Project [d_date_sk#55, d_week_seq#59, d_day_name#69]
            :     :     :           +- Filter (isnotnull(d_date_sk#55) AND isnotnull(d_week_seq#59))
            :     :     :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#55,d_date_id#56,d_date#57,d_month_seq#58,d_week_seq#59,d_quarter_seq#60,d_year#61,d_dow#62,d_moy#63,d_dom#64,d_qoy#65,d_fy_year#66,d_fy_quarter_seq#67,d_fy_week_seq#68,d_day_name#69,d_quarter_name#70,d_holiday#71,d_weekend#72,d_following_holiday#73,d_first_dom#74,d_last_dom#75,d_same_day_ly#76,d_same_day_lq#77,d_current_day#78,... 4 more fields] parquet
            :     :     +- Project [s_store_sk#83, s_store_id#84, s_store_name#88]
            :     :        +- Filter (isnotnull(s_store_sk#83) AND isnotnull(s_store_id#84))
            :     :           +- Relation spark_catalog.tpcds.store[s_store_sk#83,s_store_id#84,s_rec_start_date#85,s_rec_end_date#86,s_closed_date_sk#87,s_store_name#88,s_number_employees#89,s_floor_space#90,s_hours#91,s_manager#92,s_market_id#93,s_geography_class#94,s_market_desc#95,s_market_manager#96,s_division_id#97,s_division_name#98,s_company_id#99,s_company_name#100,s_street_number#101,s_street_name#102,s_street_type#103,s_suite_number#104,s_city#105,s_county#106,... 5 more fields] parquet
            :     +- Project [d_week_seq#116]
            :        +- Filter ((isnotnull(d_month_seq#115) AND ((d_month_seq#115 >= 1212) AND (d_month_seq#115 <= 1223))) AND isnotnull(d_week_seq#116))
            :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#112,d_date_id#113,d_date#114,d_month_seq#115,d_week_seq#116,d_quarter_seq#117,d_year#118,d_dow#119,d_moy#120,d_dom#121,d_qoy#122,d_fy_year#123,d_fy_quarter_seq#124,d_fy_week_seq#125,d_day_name#126,d_quarter_name#127,d_holiday#128,d_weekend#129,d_following_holiday#130,d_first_dom#131,d_last_dom#132,d_same_day_ly#133,d_same_day_lq#134,d_current_day#135,... 4 more fields] parquet
            +- Project [d_week_seq#311 AS d_week_seq2#11, s_store_id#141 AS s_store_id2#12, sun_sales#208 AS sun_sales2#13, mon_sales#209 AS mon_sales2#14, tue_sales#210 AS tue_sales2#15, wed_sales#211 AS wed_sales2#16, thu_sales#212 AS thu_sales2#17, fri_sales#213 AS fri_sales2#18, sat_sales#214 AS sat_sales2#19]
               +- Join Inner, (d_week_seq#173 = d_week_seq#311)
                  :- Project [d_week_seq#311, sun_sales#208, mon_sales#209, tue_sales#210, wed_sales#211, thu_sales#212, fri_sales#213, sat_sales#214, s_store_id#141]
                  :  +- Join Inner, (ss_store_sk#291 = s_store_sk#140)
                  :     :- Aggregate [d_week_seq#311, ss_store_sk#291], [d_week_seq#311, ss_store_sk#291, sum(CASE WHEN (d_day_name#321 = Sunday) THEN ss_sales_price#297 END) AS sun_sales#208, sum(CASE WHEN (d_day_name#321 = Monday) THEN ss_sales_price#297 END) AS mon_sales#209, sum(CASE WHEN (d_day_name#321 = Tuesday) THEN ss_sales_price#297 END) AS tue_sales#210, sum(CASE WHEN (d_day_name#321 = Wednesday) THEN ss_sales_price#297 END) AS wed_sales#211, sum(CASE WHEN (d_day_name#321 = Thursday) THEN ss_sales_price#297 END) AS thu_sales#212, sum(CASE WHEN (d_day_name#321 = Friday) THEN ss_sales_price#297 END) AS fri_sales#213, sum(CASE WHEN (d_day_name#321 = Saturday) THEN ss_sales_price#297 END) AS sat_sales#214]
                  :     :  +- Project [ss_store_sk#291, ss_sales_price#297, d_week_seq#311, d_day_name#321]
                  :     :     +- Join Inner, (d_date_sk#307 = ss_sold_date_sk#284)
                  :     :        :- Project [ss_sold_date_sk#284, ss_store_sk#291, ss_sales_price#297]
                  :     :        :  +- Filter (isnotnull(ss_sold_date_sk#284) AND isnotnull(ss_store_sk#291))
                  :     :        :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#284,ss_sold_time_sk#285,ss_item_sk#286,ss_customer_sk#287,ss_cdemo_sk#288,ss_hdemo_sk#289,ss_addr_sk#290,ss_store_sk#291,ss_promo_sk#292,ss_ticket_number#293,ss_quantity#294,ss_wholesale_cost#295,ss_list_price#296,ss_sales_price#297,ss_ext_discount_amt#298,ss_ext_sales_price#299,ss_ext_wholesale_cost#300,ss_ext_list_price#301,ss_ext_tax#302,ss_coupon_amt#303,ss_net_paid#304,ss_net_paid_inc_tax#305,ss_net_profit#306] parquet
                  :     :        +- Project [d_date_sk#307, d_week_seq#311, d_day_name#321]
                  :     :           +- Filter (isnotnull(d_date_sk#307) AND isnotnull(d_week_seq#311))
                  :     :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#307,d_date_id#308,d_date#309,d_month_seq#310,d_week_seq#311,d_quarter_seq#312,d_year#313,d_dow#314,d_moy#315,d_dom#316,d_qoy#317,d_fy_year#318,d_fy_quarter_seq#319,d_fy_week_seq#320,d_day_name#321,d_quarter_name#322,d_holiday#323,d_weekend#324,d_following_holiday#325,d_first_dom#326,d_last_dom#327,d_same_day_ly#328,d_same_day_lq#329,d_current_day#330,... 4 more fields] parquet
                  :     +- Project [s_store_sk#140, s_store_id#141]
                  :        +- Filter (isnotnull(s_store_sk#140) AND isnotnull(s_store_id#141))
                  :           +- Relation spark_catalog.tpcds.store[s_store_sk#140,s_store_id#141,s_rec_start_date#142,s_rec_end_date#143,s_closed_date_sk#144,s_store_name#145,s_number_employees#146,s_floor_space#147,s_hours#148,s_manager#149,s_market_id#150,s_geography_class#151,s_market_desc#152,s_market_manager#153,s_division_id#154,s_division_name#155,s_company_id#156,s_company_name#157,s_street_number#158,s_street_name#159,s_street_type#160,s_suite_number#161,s_city#162,s_county#163,... 5 more fields] parquet
                  +- Project [d_week_seq#173]
                     +- Filter ((isnotnull(d_month_seq#172) AND ((d_month_seq#172 >= 1224) AND (d_month_seq#172 <= 1235))) AND isnotnull(d_week_seq#173))
                        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#169,d_date_id#170,d_date#171,d_month_seq#172,d_week_seq#173,d_quarter_seq#174,d_year#175,d_dow#176,d_moy#177,d_dom#178,d_qoy#179,d_fy_year#180,d_fy_quarter_seq#181,d_fy_week_seq#182,d_day_name#183,d_quarter_name#184,d_holiday#185,d_weekend#186,d_following_holiday#187,d_first_dom#188,d_last_dom#189,d_same_day_ly#190,d_same_day_lq#191,d_current_day#192,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[s_store_name1#0 ASC NULLS FIRST,s_store_id1#2 ASC NULLS FIRST,d_week_seq1#1 ASC NULLS FIRST], output=[s_store_name1#0,s_store_id1#2,d_week_seq1#1,(sun_sales1 / sun_sales2)#215,(mon_sales1 / mon_sales2)#216,(tue_sales1 / tue_sales2)#217,(wed_sales1 / wed_sales2)#218,(thu_sales1 / thu_sales2)#219,(fri_sales1 / fri_sales2)#220,(sat_sales1 / sat_sales2)#221])
   +- Project [s_store_name1#0, s_store_id1#2, d_week_seq1#1, (sun_sales1#3 / sun_sales2#13) AS (sun_sales1 / sun_sales2)#215, (mon_sales1#4 / mon_sales2#14) AS (mon_sales1 / mon_sales2)#216, (tue_sales1#5 / tue_sales2#15) AS (tue_sales1 / tue_sales2)#217, (wed_sales1#6 / wed_sales2#16) AS (wed_sales1 / wed_sales2)#218, (thu_sales1#7 / thu_sales2#17) AS (thu_sales1 / thu_sales2)#219, (fri_sales1#8 / fri_sales2#18) AS (fri_sales1 / fri_sales2)#220, (sat_sales1#9 / sat_sales2#19) AS (sat_sales1 / sat_sales2)#221]
      +- SortMergeJoin [s_store_id1#2, d_week_seq1#1], [s_store_id2#12, (d_week_seq2#11 - 52)], Inner
         :- Sort [s_store_id1#2 ASC NULLS FIRST, d_week_seq1#1 ASC NULLS FIRST], false, 0
         :  +- Exchange hashpartitioning(s_store_id1#2, d_week_seq1#1, 200), ENSURE_REQUIREMENTS, [plan_id=194]
         :     +- Project [s_store_name#88 AS s_store_name1#0, d_week_seq#59 AS d_week_seq1#1, s_store_id#84 AS s_store_id1#2, sun_sales#20 AS sun_sales1#3, mon_sales#21 AS mon_sales1#4, tue_sales#22 AS tue_sales1#5, wed_sales#23 AS wed_sales1#6, thu_sales#24 AS thu_sales1#7, fri_sales#25 AS fri_sales1#8, sat_sales#26 AS sat_sales1#9]
         :        +- BroadcastHashJoin [d_week_seq#59], [d_week_seq#116], Inner, BuildRight, false
         :           :- Project [d_week_seq#59, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26, s_store_id#84, s_store_name#88]
         :           :  +- BroadcastHashJoin [ss_store_sk#39], [s_store_sk#83], Inner, BuildRight, false
         :           :     :- HashAggregate(keys=[d_week_seq#59, ss_store_sk#39], functions=[sum(CASE WHEN (d_day_name#69 = Sunday) THEN ss_sales_price#45 END), sum(CASE WHEN (d_day_name#69 = Monday) THEN ss_sales_price#45 END), sum(CASE WHEN (d_day_name#69 = Tuesday) THEN ss_sales_price#45 END), sum(CASE WHEN (d_day_name#69 = Wednesday) THEN ss_sales_price#45 END), sum(CASE WHEN (d_day_name#69 = Thursday) THEN ss_sales_price#45 END), sum(CASE WHEN (d_day_name#69 = Friday) THEN ss_sales_price#45 END), sum(CASE WHEN (d_day_name#69 = Saturday) THEN ss_sales_price#45 END)], output=[d_week_seq#59, ss_store_sk#39, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26])
         :           :     :  +- Exchange hashpartitioning(d_week_seq#59, ss_store_sk#39, 200), ENSURE_REQUIREMENTS, [plan_id=167]
         :           :     :     +- HashAggregate(keys=[d_week_seq#59, ss_store_sk#39], functions=[partial_sum(CASE WHEN (d_day_name#69 = Sunday) THEN ss_sales_price#45 END), partial_sum(CASE WHEN (d_day_name#69 = Monday) THEN ss_sales_price#45 END), partial_sum(CASE WHEN (d_day_name#69 = Tuesday) THEN ss_sales_price#45 END), partial_sum(CASE WHEN (d_day_name#69 = Wednesday) THEN ss_sales_price#45 END), partial_sum(CASE WHEN (d_day_name#69 = Thursday) THEN ss_sales_price#45 END), partial_sum(CASE WHEN (d_day_name#69 = Friday) THEN ss_sales_price#45 END), partial_sum(CASE WHEN (d_day_name#69 = Saturday) THEN ss_sales_price#45 END)], output=[d_week_seq#59, ss_store_sk#39, sum#349, sum#350, sum#351, sum#352, sum#353, sum#354, sum#355])
         :           :     :        +- Project [ss_store_sk#39, ss_sales_price#45, d_week_seq#59, d_day_name#69]
         :           :     :           +- BroadcastHashJoin [ss_sold_date_sk#32], [d_date_sk#55], Inner, BuildRight, false
         :           :     :              :- Filter (isnotnull(ss_sold_date_sk#32) AND isnotnull(ss_store_sk#39))
         :           :     :              :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#32,ss_store_sk#39,ss_sales_price#45] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#32), isnotnull(ss_store_sk#39)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_sales_price:double>
         :           :     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=162]
         :           :     :                 +- Filter (isnotnull(d_date_sk#55) AND isnotnull(d_week_seq#59))
         :           :     :                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#55,d_week_seq#59,d_day_name#69] Batched: true, DataFilters: [isnotnull(d_date_sk#55), isnotnull(d_week_seq#59)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk), IsNotNull(d_week_seq)], ReadSchema: struct<d_date_sk:int,d_week_seq:int,d_day_name:string>
         :           :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=170]
         :           :        +- Filter (isnotnull(s_store_sk#83) AND isnotnull(s_store_id#84))
         :           :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#83,s_store_id#84,s_store_name#88] Batched: true, DataFilters: [isnotnull(s_store_sk#83), isnotnull(s_store_id#84)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_store_id)], ReadSchema: struct<s_store_sk:int,s_store_id:string,s_store_name:string>
         :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=174]
         :              +- Project [d_week_seq#116]
         :                 +- Filter (((isnotnull(d_month_seq#115) AND (d_month_seq#115 >= 1212)) AND (d_month_seq#115 <= 1223)) AND isnotnull(d_week_seq#116))
         :                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_month_seq#115,d_week_seq#116] Batched: true, DataFilters: [isnotnull(d_month_seq#115), (d_month_seq#115 >= 1212), (d_month_seq#115 <= 1223), isnotnull(d_we..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1212), LessThanOrEqual(d_month_seq,1223),..., ReadSchema: struct<d_month_seq:int,d_week_seq:int>
         +- Sort [s_store_id2#12 ASC NULLS FIRST, (d_week_seq2#11 - 52) ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(s_store_id2#12, (d_week_seq2#11 - 52), 200), ENSURE_REQUIREMENTS, [plan_id=195]
               +- Project [d_week_seq#311 AS d_week_seq2#11, s_store_id#141 AS s_store_id2#12, sun_sales#208 AS sun_sales2#13, mon_sales#209 AS mon_sales2#14, tue_sales#210 AS tue_sales2#15, wed_sales#211 AS wed_sales2#16, thu_sales#212 AS thu_sales2#17, fri_sales#213 AS fri_sales2#18, sat_sales#214 AS sat_sales2#19]
                  +- BroadcastHashJoin [d_week_seq#311], [d_week_seq#173], Inner, BuildRight, false
                     :- Project [d_week_seq#311, sun_sales#208, mon_sales#209, tue_sales#210, wed_sales#211, thu_sales#212, fri_sales#213, sat_sales#214, s_store_id#141]
                     :  +- BroadcastHashJoin [ss_store_sk#291], [s_store_sk#140], Inner, BuildRight, false
                     :     :- HashAggregate(keys=[d_week_seq#311, ss_store_sk#291], functions=[sum(CASE WHEN (d_day_name#321 = Sunday) THEN ss_sales_price#297 END), sum(CASE WHEN (d_day_name#321 = Monday) THEN ss_sales_price#297 END), sum(CASE WHEN (d_day_name#321 = Tuesday) THEN ss_sales_price#297 END), sum(CASE WHEN (d_day_name#321 = Wednesday) THEN ss_sales_price#297 END), sum(CASE WHEN (d_day_name#321 = Thursday) THEN ss_sales_price#297 END), sum(CASE WHEN (d_day_name#321 = Friday) THEN ss_sales_price#297 END), sum(CASE WHEN (d_day_name#321 = Saturday) THEN ss_sales_price#297 END)], output=[d_week_seq#311, ss_store_sk#291, sun_sales#208, mon_sales#209, tue_sales#210, wed_sales#211, thu_sales#212, fri_sales#213, sat_sales#214])
                     :     :  +- Exchange hashpartitioning(d_week_seq#311, ss_store_sk#291, 200), ENSURE_REQUIREMENTS, [plan_id=182]
                     :     :     +- HashAggregate(keys=[d_week_seq#311, ss_store_sk#291], functions=[partial_sum(CASE WHEN (d_day_name#321 = Sunday) THEN ss_sales_price#297 END), partial_sum(CASE WHEN (d_day_name#321 = Monday) THEN ss_sales_price#297 END), partial_sum(CASE WHEN (d_day_name#321 = Tuesday) THEN ss_sales_price#297 END), partial_sum(CASE WHEN (d_day_name#321 = Wednesday) THEN ss_sales_price#297 END), partial_sum(CASE WHEN (d_day_name#321 = Thursday) THEN ss_sales_price#297 END), partial_sum(CASE WHEN (d_day_name#321 = Friday) THEN ss_sales_price#297 END), partial_sum(CASE WHEN (d_day_name#321 = Saturday) THEN ss_sales_price#297 END)], output=[d_week_seq#311, ss_store_sk#291, sum#363, sum#364, sum#365, sum#366, sum#367, sum#368, sum#369])
                     :     :        +- Project [ss_store_sk#291, ss_sales_price#297, d_week_seq#311, d_day_name#321]
                     :     :           +- BroadcastHashJoin [ss_sold_date_sk#284], [d_date_sk#307], Inner, BuildRight, false
                     :     :              :- Filter (isnotnull(ss_sold_date_sk#284) AND isnotnull(ss_store_sk#291))
                     :     :              :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#284,ss_store_sk#291,ss_sales_price#297] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#284), isnotnull(ss_store_sk#291)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_sales_price:double>
                     :     :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=177]
                     :     :                 +- Filter (isnotnull(d_date_sk#307) AND isnotnull(d_week_seq#311))
                     :     :                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#307,d_week_seq#311,d_day_name#321] Batched: true, DataFilters: [isnotnull(d_date_sk#307), isnotnull(d_week_seq#311)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk), IsNotNull(d_week_seq)], ReadSchema: struct<d_date_sk:int,d_week_seq:int,d_day_name:string>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=185]
                     :        +- Filter (isnotnull(s_store_sk#140) AND isnotnull(s_store_id#141))
                     :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#140,s_store_id#141] Batched: true, DataFilters: [isnotnull(s_store_sk#140), isnotnull(s_store_id#141)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_store_id)], ReadSchema: struct<s_store_sk:int,s_store_id:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=189]
                        +- Project [d_week_seq#173]
                           +- Filter (((isnotnull(d_month_seq#172) AND (d_month_seq#172 >= 1224)) AND (d_month_seq#172 <= 1235)) AND isnotnull(d_week_seq#173))
                              +- FileScan parquet spark_catalog.tpcds.date_dim[d_month_seq#172,d_week_seq#173] Batched: true, DataFilters: [isnotnull(d_month_seq#172), (d_month_seq#172 >= 1224), (d_month_seq#172 <= 1235), isnotnull(d_we..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1224), LessThanOrEqual(d_month_seq,1235),..., ReadSchema: struct<d_month_seq:int,d_week_seq:int>

Time taken: 2.912 seconds, Fetched 1 row(s)
