== Parsed Logical Plan ==
CTE [wscs, wswscs]
:  :- 'SubqueryAlias `wscs`
:  :  +- 'Project ['sold_date_sk, 'sales_price]
:  :     +- 'SubqueryAlias `x`
:  :        +- 'Union
:  :           :- 'Project ['ws_sold_date_sk AS sold_date_sk#16, 'ws_ext_sales_price AS sales_price#17]
:  :           :  +- 'UnresolvedRelation `web_sales`
:  :           +- 'Project ['cs_sold_date_sk AS sold_date_sk#18, 'cs_ext_sales_price AS sales_price#19]
:  :              +- 'UnresolvedRelation `catalog_sales`
:  +- 'SubqueryAlias `wswscs`
:     +- 'Aggregate ['d_week_seq], ['d_week_seq, 'sum(CASE WHEN ('d_day_name = Sunday) THEN 'sales_price ELSE null END) AS sun_sales#20, 'sum(CASE WHEN ('d_day_name = Monday) THEN 'sales_price ELSE null END) AS mon_sales#21, 'sum(CASE WHEN ('d_day_name = Tuesday) THEN 'sales_price ELSE null END) AS tue_sales#22, 'sum(CASE WHEN ('d_day_name = Wednesday) THEN 'sales_price ELSE null END) AS wed_sales#23, 'sum(CASE WHEN ('d_day_name = Thursday) THEN 'sales_price ELSE null END) AS thu_sales#24, 'sum(CASE WHEN ('d_day_name = Friday) THEN 'sales_price ELSE null END) AS fri_sales#25, 'sum(CASE WHEN ('d_day_name = Saturday) THEN 'sales_price ELSE null END) AS sat_sales#26]
:        +- 'Filter ('d_date_sk = 'sold_date_sk)
:           +- 'Join Inner
:              :- 'UnresolvedRelation `wscs`
:              +- 'UnresolvedRelation `date_dim`
+- 'Sort ['d_week_seq1 ASC NULLS FIRST], true
   +- 'Project ['d_week_seq1, unresolvedalias('round(('sun_sales1 / 'sun_sales2), 2), None), unresolvedalias('round(('mon_sales1 / 'mon_sales2), 2), None), unresolvedalias('round(('tue_sales1 / 'tue_sales2), 2), None), unresolvedalias('round(('wed_sales1 / 'wed_sales2), 2), None), unresolvedalias('round(('thu_sales1 / 'thu_sales2), 2), None), unresolvedalias('round(('fri_sales1 / 'fri_sales2), 2), None), unresolvedalias('round(('sat_sales1 / 'sat_sales2), 2), None)]
      +- 'Filter ('d_week_seq1 = ('d_week_seq2 - 53))
         +- 'Join Inner
            :- 'SubqueryAlias `y`
            :  +- 'Project ['wswscs.d_week_seq AS d_week_seq1#0, 'sun_sales AS sun_sales1#1, 'mon_sales AS mon_sales1#2, 'tue_sales AS tue_sales1#3, 'wed_sales AS wed_sales1#4, 'thu_sales AS thu_sales1#5, 'fri_sales AS fri_sales1#6, 'sat_sales AS sat_sales1#7]
            :     +- 'Filter (('date_dim.d_week_seq = 'wswscs.d_week_seq) && ('d_year = 2001))
            :        +- 'Join Inner
            :           :- 'UnresolvedRelation `wswscs`
            :           +- 'UnresolvedRelation `date_dim`
            +- 'SubqueryAlias `z`
               +- 'Project ['wswscs.d_week_seq AS d_week_seq2#8, 'sun_sales AS sun_sales2#9, 'mon_sales AS mon_sales2#10, 'tue_sales AS tue_sales2#11, 'wed_sales AS wed_sales2#12, 'thu_sales AS thu_sales2#13, 'fri_sales AS fri_sales2#14, 'sat_sales AS sat_sales2#15]
                  +- 'Filter (('date_dim.d_week_seq = 'wswscs.d_week_seq) && ('d_year = (2001 + 1)))
                     +- 'Join Inner
                        :- 'UnresolvedRelation `wswscs`
                        +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
d_week_seq1: int, round((sun_sales1 / sun_sales2), 2): double, round((mon_sales1 / mon_sales2), 2): double, round((tue_sales1 / tue_sales2), 2): double, round((wed_sales1 / wed_sales2), 2): double, round((thu_sales1 / thu_sales2), 2): double, round((fri_sales1 / fri_sales2), 2): double, round((sat_sales1 / sat_sales2), 2): double
Sort [d_week_seq1#0 ASC NULLS FIRST], true
+- Project [d_week_seq1#0, round((sun_sales1#1 / sun_sales2#9), 2) AS round((sun_sales1 / sun_sales2), 2)#188, round((mon_sales1#2 / mon_sales2#10), 2) AS round((mon_sales1 / mon_sales2), 2)#189, round((tue_sales1#3 / tue_sales2#11), 2) AS round((tue_sales1 / tue_sales2), 2)#190, round((wed_sales1#4 / wed_sales2#12), 2) AS round((wed_sales1 / wed_sales2), 2)#191, round((thu_sales1#5 / thu_sales2#13), 2) AS round((thu_sales1 / thu_sales2), 2)#192, round((fri_sales1#6 / fri_sales2#14), 2) AS round((fri_sales1 / fri_sales2), 2)#193, round((sat_sales1#7 / sat_sales2#15), 2) AS round((sat_sales1 / sat_sales2), 2)#194]
   +- Filter (d_week_seq1#0 = (d_week_seq2#8 - 53))
      +- Join Inner
         :- SubqueryAlias `y`
         :  +- Project [d_week_seq#101 AS d_week_seq1#0, sun_sales#20 AS sun_sales1#1, mon_sales#21 AS mon_sales1#2, tue_sales#22 AS tue_sales1#3, wed_sales#23 AS wed_sales1#4, thu_sales#24 AS thu_sales1#5, fri_sales#25 AS fri_sales1#6, sat_sales#26 AS sat_sales1#7]
         :     +- Filter ((d_week_seq#136 = d_week_seq#101) && (d_year#138 = 2001))
         :        +- Join Inner
         :           :- SubqueryAlias `wswscs`
         :           :  +- Aggregate [d_week_seq#101], [d_week_seq#101, sum(CASE WHEN (d_day_name#111 = Sunday) THEN sales_price#17 ELSE cast(null as double) END) AS sun_sales#20, sum(CASE WHEN (d_day_name#111 = Monday) THEN sales_price#17 ELSE cast(null as double) END) AS mon_sales#21, sum(CASE WHEN (d_day_name#111 = Tuesday) THEN sales_price#17 ELSE cast(null as double) END) AS tue_sales#22, sum(CASE WHEN (d_day_name#111 = Wednesday) THEN sales_price#17 ELSE cast(null as double) END) AS wed_sales#23, sum(CASE WHEN (d_day_name#111 = Thursday) THEN sales_price#17 ELSE cast(null as double) END) AS thu_sales#24, sum(CASE WHEN (d_day_name#111 = Friday) THEN sales_price#17 ELSE cast(null as double) END) AS fri_sales#25, sum(CASE WHEN (d_day_name#111 = Saturday) THEN sales_price#17 ELSE cast(null as double) END) AS sat_sales#26]
         :           :     +- Filter (d_date_sk#97 = sold_date_sk#16)
         :           :        +- Join Inner
         :           :           :- SubqueryAlias `wscs`
         :           :           :  +- Project [sold_date_sk#16, sales_price#17]
         :           :           :     +- SubqueryAlias `x`
         :           :           :        +- Union
         :           :           :           :- Project [ws_sold_date_sk#29 AS sold_date_sk#16, ws_ext_sales_price#52 AS sales_price#17]
         :           :           :           :  +- SubqueryAlias `tpcds`.`web_sales`
         :           :           :           :     +- Relation[ws_sold_date_sk#29,ws_sold_time_sk#30,ws_ship_date_sk#31,ws_item_sk#32,ws_bill_customer_sk#33,ws_bill_cdemo_sk#34,ws_bill_hdemo_sk#35,ws_bill_addr_sk#36,ws_ship_customer_sk#37,ws_ship_cdemo_sk#38,ws_ship_hdemo_sk#39,ws_ship_addr_sk#40,ws_web_page_sk#41,ws_web_site_sk#42,ws_ship_mode_sk#43,ws_warehouse_sk#44,ws_promo_sk#45,ws_order_number#46,ws_quantity#47,ws_wholesale_cost#48,ws_list_price#49,ws_sales_price#50,ws_ext_discount_amt#51,ws_ext_sales_price#52,... 10 more fields] parquet
         :           :           :           +- Project [cs_sold_date_sk#63 AS sold_date_sk#18, cs_ext_sales_price#86 AS sales_price#19]
         :           :           :              +- SubqueryAlias `tpcds`.`catalog_sales`
         :           :           :                 +- Relation[cs_sold_date_sk#63,cs_sold_time_sk#64,cs_ship_date_sk#65,cs_bill_customer_sk#66,cs_bill_cdemo_sk#67,cs_bill_hdemo_sk#68,cs_bill_addr_sk#69,cs_ship_customer_sk#70,cs_ship_cdemo_sk#71,cs_ship_hdemo_sk#72,cs_ship_addr_sk#73,cs_call_center_sk#74,cs_catalog_page_sk#75,cs_ship_mode_sk#76,cs_warehouse_sk#77,cs_item_sk#78,cs_promo_sk#79,cs_order_number#80,cs_quantity#81,cs_wholesale_cost#82,cs_list_price#83,cs_sales_price#84,cs_ext_discount_amt#85,cs_ext_sales_price#86,... 10 more fields] parquet
         :           :           +- SubqueryAlias `tpcds`.`date_dim`
         :           :              +- Relation[d_date_sk#97,d_date_id#98,d_date#99,d_month_seq#100,d_week_seq#101,d_quarter_seq#102,d_year#103,d_dow#104,d_moy#105,d_dom#106,d_qoy#107,d_fy_year#108,d_fy_quarter_seq#109,d_fy_week_seq#110,d_day_name#111,d_quarter_name#112,d_holiday#113,d_weekend#114,d_following_holiday#115,d_first_dom#116,d_last_dom#117,d_same_day_ly#118,d_same_day_lq#119,d_current_day#120,... 4 more fields] parquet
         :           +- SubqueryAlias `tpcds`.`date_dim`
         :              +- Relation[d_date_sk#132,d_date_id#133,d_date#134,d_month_seq#135,d_week_seq#136,d_quarter_seq#137,d_year#138,d_dow#139,d_moy#140,d_dom#141,d_qoy#142,d_fy_year#143,d_fy_quarter_seq#144,d_fy_week_seq#145,d_day_name#146,d_quarter_name#147,d_holiday#148,d_weekend#149,d_following_holiday#150,d_first_dom#151,d_last_dom#152,d_same_day_ly#153,d_same_day_lq#154,d_current_day#155,... 4 more fields] parquet
         +- SubqueryAlias `z`
            +- Project [d_week_seq#101 AS d_week_seq2#8, sun_sales#20 AS sun_sales2#9, mon_sales#21 AS mon_sales2#10, tue_sales#22 AS tue_sales2#11, wed_sales#23 AS wed_sales2#12, thu_sales#24 AS thu_sales2#13, fri_sales#25 AS fri_sales2#14, sat_sales#26 AS sat_sales2#15]
               +- Filter ((d_week_seq#164 = d_week_seq#101) && (d_year#166 = (2001 + 1)))
                  +- Join Inner
                     :- SubqueryAlias `wswscs`
                     :  +- Aggregate [d_week_seq#101], [d_week_seq#101, sum(CASE WHEN (d_day_name#111 = Sunday) THEN sales_price#17 ELSE cast(null as double) END) AS sun_sales#20, sum(CASE WHEN (d_day_name#111 = Monday) THEN sales_price#17 ELSE cast(null as double) END) AS mon_sales#21, sum(CASE WHEN (d_day_name#111 = Tuesday) THEN sales_price#17 ELSE cast(null as double) END) AS tue_sales#22, sum(CASE WHEN (d_day_name#111 = Wednesday) THEN sales_price#17 ELSE cast(null as double) END) AS wed_sales#23, sum(CASE WHEN (d_day_name#111 = Thursday) THEN sales_price#17 ELSE cast(null as double) END) AS thu_sales#24, sum(CASE WHEN (d_day_name#111 = Friday) THEN sales_price#17 ELSE cast(null as double) END) AS fri_sales#25, sum(CASE WHEN (d_day_name#111 = Saturday) THEN sales_price#17 ELSE cast(null as double) END) AS sat_sales#26]
                     :     +- Filter (d_date_sk#97 = sold_date_sk#16)
                     :        +- Join Inner
                     :           :- SubqueryAlias `wscs`
                     :           :  +- Project [sold_date_sk#16, sales_price#17]
                     :           :     +- SubqueryAlias `x`
                     :           :        +- Union
                     :           :           :- Project [ws_sold_date_sk#29 AS sold_date_sk#16, ws_ext_sales_price#52 AS sales_price#17]
                     :           :           :  +- SubqueryAlias `tpcds`.`web_sales`
                     :           :           :     +- Relation[ws_sold_date_sk#29,ws_sold_time_sk#30,ws_ship_date_sk#31,ws_item_sk#32,ws_bill_customer_sk#33,ws_bill_cdemo_sk#34,ws_bill_hdemo_sk#35,ws_bill_addr_sk#36,ws_ship_customer_sk#37,ws_ship_cdemo_sk#38,ws_ship_hdemo_sk#39,ws_ship_addr_sk#40,ws_web_page_sk#41,ws_web_site_sk#42,ws_ship_mode_sk#43,ws_warehouse_sk#44,ws_promo_sk#45,ws_order_number#46,ws_quantity#47,ws_wholesale_cost#48,ws_list_price#49,ws_sales_price#50,ws_ext_discount_amt#51,ws_ext_sales_price#52,... 10 more fields] parquet
                     :           :           +- Project [cs_sold_date_sk#63 AS sold_date_sk#18, cs_ext_sales_price#86 AS sales_price#19]
                     :           :              +- SubqueryAlias `tpcds`.`catalog_sales`
                     :           :                 +- Relation[cs_sold_date_sk#63,cs_sold_time_sk#64,cs_ship_date_sk#65,cs_bill_customer_sk#66,cs_bill_cdemo_sk#67,cs_bill_hdemo_sk#68,cs_bill_addr_sk#69,cs_ship_customer_sk#70,cs_ship_cdemo_sk#71,cs_ship_hdemo_sk#72,cs_ship_addr_sk#73,cs_call_center_sk#74,cs_catalog_page_sk#75,cs_ship_mode_sk#76,cs_warehouse_sk#77,cs_item_sk#78,cs_promo_sk#79,cs_order_number#80,cs_quantity#81,cs_wholesale_cost#82,cs_list_price#83,cs_sales_price#84,cs_ext_discount_amt#85,cs_ext_sales_price#86,... 10 more fields] parquet
                     :           +- SubqueryAlias `tpcds`.`date_dim`
                     :              +- Relation[d_date_sk#97,d_date_id#98,d_date#99,d_month_seq#100,d_week_seq#101,d_quarter_seq#102,d_year#103,d_dow#104,d_moy#105,d_dom#106,d_qoy#107,d_fy_year#108,d_fy_quarter_seq#109,d_fy_week_seq#110,d_day_name#111,d_quarter_name#112,d_holiday#113,d_weekend#114,d_following_holiday#115,d_first_dom#116,d_last_dom#117,d_same_day_ly#118,d_same_day_lq#119,d_current_day#120,... 4 more fields] parquet
                     +- SubqueryAlias `tpcds`.`date_dim`
                        +- Relation[d_date_sk#160,d_date_id#161,d_date#162,d_month_seq#163,d_week_seq#164,d_quarter_seq#165,d_year#166,d_dow#167,d_moy#168,d_dom#169,d_qoy#170,d_fy_year#171,d_fy_quarter_seq#172,d_fy_week_seq#173,d_day_name#174,d_quarter_name#175,d_holiday#176,d_weekend#177,d_following_holiday#178,d_first_dom#179,d_last_dom#180,d_same_day_ly#181,d_same_day_lq#182,d_current_day#183,... 4 more fields] parquet

== Optimized Logical Plan ==
Sort [d_week_seq1#0 ASC NULLS FIRST], true
+- Project [d_week_seq1#0, round((sun_sales1#1 / sun_sales2#9), 2) AS round((sun_sales1 / sun_sales2), 2)#188, round((mon_sales1#2 / mon_sales2#10), 2) AS round((mon_sales1 / mon_sales2), 2)#189, round((tue_sales1#3 / tue_sales2#11), 2) AS round((tue_sales1 / tue_sales2), 2)#190, round((wed_sales1#4 / wed_sales2#12), 2) AS round((wed_sales1 / wed_sales2), 2)#191, round((thu_sales1#5 / thu_sales2#13), 2) AS round((thu_sales1 / thu_sales2), 2)#192, round((fri_sales1#6 / fri_sales2#14), 2) AS round((fri_sales1 / fri_sales2), 2)#193, round((sat_sales1#7 / sat_sales2#15), 2) AS round((sat_sales1 / sat_sales2), 2)#194]
   +- Join Inner, (d_week_seq1#0 = (d_week_seq2#8 - 53))
      :- Project [d_week_seq#101 AS d_week_seq1#0, sun_sales#20 AS sun_sales1#1, mon_sales#21 AS mon_sales1#2, tue_sales#22 AS tue_sales1#3, wed_sales#23 AS wed_sales1#4, thu_sales#24 AS thu_sales1#5, fri_sales#25 AS fri_sales1#6, sat_sales#26 AS sat_sales1#7]
      :  +- Join Inner, (d_week_seq#136 = d_week_seq#101)
      :     :- Aggregate [d_week_seq#101], [d_week_seq#101, sum(CASE WHEN (d_day_name#111 = Sunday) THEN sales_price#17 ELSE null END) AS sun_sales#20, sum(CASE WHEN (d_day_name#111 = Monday) THEN sales_price#17 ELSE null END) AS mon_sales#21, sum(CASE WHEN (d_day_name#111 = Tuesday) THEN sales_price#17 ELSE null END) AS tue_sales#22, sum(CASE WHEN (d_day_name#111 = Wednesday) THEN sales_price#17 ELSE null END) AS wed_sales#23, sum(CASE WHEN (d_day_name#111 = Thursday) THEN sales_price#17 ELSE null END) AS thu_sales#24, sum(CASE WHEN (d_day_name#111 = Friday) THEN sales_price#17 ELSE null END) AS fri_sales#25, sum(CASE WHEN (d_day_name#111 = Saturday) THEN sales_price#17 ELSE null END) AS sat_sales#26]
      :     :  +- Project [sales_price#17, d_week_seq#101, d_day_name#111]
      :     :     +- Join Inner, (d_date_sk#97 = sold_date_sk#16)
      :     :        :- Union
      :     :        :  :- Project [ws_sold_date_sk#29 AS sold_date_sk#16, ws_ext_sales_price#52 AS sales_price#17]
      :     :        :  :  +- Filter isnotnull(ws_sold_date_sk#29)
      :     :        :  :     +- Relation[ws_sold_date_sk#29,ws_sold_time_sk#30,ws_ship_date_sk#31,ws_item_sk#32,ws_bill_customer_sk#33,ws_bill_cdemo_sk#34,ws_bill_hdemo_sk#35,ws_bill_addr_sk#36,ws_ship_customer_sk#37,ws_ship_cdemo_sk#38,ws_ship_hdemo_sk#39,ws_ship_addr_sk#40,ws_web_page_sk#41,ws_web_site_sk#42,ws_ship_mode_sk#43,ws_warehouse_sk#44,ws_promo_sk#45,ws_order_number#46,ws_quantity#47,ws_wholesale_cost#48,ws_list_price#49,ws_sales_price#50,ws_ext_discount_amt#51,ws_ext_sales_price#52,... 10 more fields] parquet
      :     :        :  +- Project [cs_sold_date_sk#63 AS sold_date_sk#18, cs_ext_sales_price#86 AS sales_price#19]
      :     :        :     +- Filter isnotnull(cs_sold_date_sk#63)
      :     :        :        +- Relation[cs_sold_date_sk#63,cs_sold_time_sk#64,cs_ship_date_sk#65,cs_bill_customer_sk#66,cs_bill_cdemo_sk#67,cs_bill_hdemo_sk#68,cs_bill_addr_sk#69,cs_ship_customer_sk#70,cs_ship_cdemo_sk#71,cs_ship_hdemo_sk#72,cs_ship_addr_sk#73,cs_call_center_sk#74,cs_catalog_page_sk#75,cs_ship_mode_sk#76,cs_warehouse_sk#77,cs_item_sk#78,cs_promo_sk#79,cs_order_number#80,cs_quantity#81,cs_wholesale_cost#82,cs_list_price#83,cs_sales_price#84,cs_ext_discount_amt#85,cs_ext_sales_price#86,... 10 more fields] parquet
      :     :        +- Project [d_date_sk#97, d_week_seq#101, d_day_name#111]
      :     :           +- Filter (isnotnull(d_date_sk#97) && isnotnull(d_week_seq#101))
      :     :              +- Relation[d_date_sk#97,d_date_id#98,d_date#99,d_month_seq#100,d_week_seq#101,d_quarter_seq#102,d_year#103,d_dow#104,d_moy#105,d_dom#106,d_qoy#107,d_fy_year#108,d_fy_quarter_seq#109,d_fy_week_seq#110,d_day_name#111,d_quarter_name#112,d_holiday#113,d_weekend#114,d_following_holiday#115,d_first_dom#116,d_last_dom#117,d_same_day_ly#118,d_same_day_lq#119,d_current_day#120,... 4 more fields] parquet
      :     +- Project [d_week_seq#136]
      :        +- Filter ((isnotnull(d_year#138) && (d_year#138 = 2001)) && isnotnull(d_week_seq#136))
      :           +- Relation[d_date_sk#132,d_date_id#133,d_date#134,d_month_seq#135,d_week_seq#136,d_quarter_seq#137,d_year#138,d_dow#139,d_moy#140,d_dom#141,d_qoy#142,d_fy_year#143,d_fy_quarter_seq#144,d_fy_week_seq#145,d_day_name#146,d_quarter_name#147,d_holiday#148,d_weekend#149,d_following_holiday#150,d_first_dom#151,d_last_dom#152,d_same_day_ly#153,d_same_day_lq#154,d_current_day#155,... 4 more fields] parquet
      +- Project [d_week_seq#101 AS d_week_seq2#8, sun_sales#20 AS sun_sales2#9, mon_sales#21 AS mon_sales2#10, tue_sales#22 AS tue_sales2#11, wed_sales#23 AS wed_sales2#12, thu_sales#24 AS thu_sales2#13, fri_sales#25 AS fri_sales2#14, sat_sales#26 AS sat_sales2#15]
         +- Join Inner, (d_week_seq#164 = d_week_seq#101)
            :- Aggregate [d_week_seq#101], [d_week_seq#101, sum(CASE WHEN (d_day_name#111 = Sunday) THEN sales_price#17 ELSE null END) AS sun_sales#20, sum(CASE WHEN (d_day_name#111 = Monday) THEN sales_price#17 ELSE null END) AS mon_sales#21, sum(CASE WHEN (d_day_name#111 = Tuesday) THEN sales_price#17 ELSE null END) AS tue_sales#22, sum(CASE WHEN (d_day_name#111 = Wednesday) THEN sales_price#17 ELSE null END) AS wed_sales#23, sum(CASE WHEN (d_day_name#111 = Thursday) THEN sales_price#17 ELSE null END) AS thu_sales#24, sum(CASE WHEN (d_day_name#111 = Friday) THEN sales_price#17 ELSE null END) AS fri_sales#25, sum(CASE WHEN (d_day_name#111 = Saturday) THEN sales_price#17 ELSE null END) AS sat_sales#26]
            :  +- Project [sales_price#17, d_week_seq#101, d_day_name#111]
            :     +- Join Inner, (d_date_sk#97 = sold_date_sk#16)
            :        :- Union
            :        :  :- Project [ws_sold_date_sk#29 AS sold_date_sk#16, ws_ext_sales_price#52 AS sales_price#17]
            :        :  :  +- Filter isnotnull(ws_sold_date_sk#29)
            :        :  :     +- Relation[ws_sold_date_sk#29,ws_sold_time_sk#30,ws_ship_date_sk#31,ws_item_sk#32,ws_bill_customer_sk#33,ws_bill_cdemo_sk#34,ws_bill_hdemo_sk#35,ws_bill_addr_sk#36,ws_ship_customer_sk#37,ws_ship_cdemo_sk#38,ws_ship_hdemo_sk#39,ws_ship_addr_sk#40,ws_web_page_sk#41,ws_web_site_sk#42,ws_ship_mode_sk#43,ws_warehouse_sk#44,ws_promo_sk#45,ws_order_number#46,ws_quantity#47,ws_wholesale_cost#48,ws_list_price#49,ws_sales_price#50,ws_ext_discount_amt#51,ws_ext_sales_price#52,... 10 more fields] parquet
            :        :  +- Project [cs_sold_date_sk#63 AS sold_date_sk#18, cs_ext_sales_price#86 AS sales_price#19]
            :        :     +- Filter isnotnull(cs_sold_date_sk#63)
            :        :        +- Relation[cs_sold_date_sk#63,cs_sold_time_sk#64,cs_ship_date_sk#65,cs_bill_customer_sk#66,cs_bill_cdemo_sk#67,cs_bill_hdemo_sk#68,cs_bill_addr_sk#69,cs_ship_customer_sk#70,cs_ship_cdemo_sk#71,cs_ship_hdemo_sk#72,cs_ship_addr_sk#73,cs_call_center_sk#74,cs_catalog_page_sk#75,cs_ship_mode_sk#76,cs_warehouse_sk#77,cs_item_sk#78,cs_promo_sk#79,cs_order_number#80,cs_quantity#81,cs_wholesale_cost#82,cs_list_price#83,cs_sales_price#84,cs_ext_discount_amt#85,cs_ext_sales_price#86,... 10 more fields] parquet
            :        +- Project [d_date_sk#97, d_week_seq#101, d_day_name#111]
            :           +- Filter (isnotnull(d_date_sk#97) && isnotnull(d_week_seq#101))
            :              +- Relation[d_date_sk#97,d_date_id#98,d_date#99,d_month_seq#100,d_week_seq#101,d_quarter_seq#102,d_year#103,d_dow#104,d_moy#105,d_dom#106,d_qoy#107,d_fy_year#108,d_fy_quarter_seq#109,d_fy_week_seq#110,d_day_name#111,d_quarter_name#112,d_holiday#113,d_weekend#114,d_following_holiday#115,d_first_dom#116,d_last_dom#117,d_same_day_ly#118,d_same_day_lq#119,d_current_day#120,... 4 more fields] parquet
            +- Project [d_week_seq#164]
               +- Filter ((isnotnull(d_year#166) && (d_year#166 = 2002)) && isnotnull(d_week_seq#164))
                  +- Relation[d_date_sk#160,d_date_id#161,d_date#162,d_month_seq#163,d_week_seq#164,d_quarter_seq#165,d_year#166,d_dow#167,d_moy#168,d_dom#169,d_qoy#170,d_fy_year#171,d_fy_quarter_seq#172,d_fy_week_seq#173,d_day_name#174,d_quarter_name#175,d_holiday#176,d_weekend#177,d_following_holiday#178,d_first_dom#179,d_last_dom#180,d_same_day_ly#181,d_same_day_lq#182,d_current_day#183,... 4 more fields] parquet

== Physical Plan ==
*(16) Sort [d_week_seq1#0 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(d_week_seq1#0 ASC NULLS FIRST, 200)
   +- *(15) Project [d_week_seq1#0, round((sun_sales1#1 / sun_sales2#9), 2) AS round((sun_sales1 / sun_sales2), 2)#188, round((mon_sales1#2 / mon_sales2#10), 2) AS round((mon_sales1 / mon_sales2), 2)#189, round((tue_sales1#3 / tue_sales2#11), 2) AS round((tue_sales1 / tue_sales2), 2)#190, round((wed_sales1#4 / wed_sales2#12), 2) AS round((wed_sales1 / wed_sales2), 2)#191, round((thu_sales1#5 / thu_sales2#13), 2) AS round((thu_sales1 / thu_sales2), 2)#192, round((fri_sales1#6 / fri_sales2#14), 2) AS round((fri_sales1 / fri_sales2), 2)#193, round((sat_sales1#7 / sat_sales2#15), 2) AS round((sat_sales1 / sat_sales2), 2)#194]
      +- *(15) SortMergeJoin [d_week_seq1#0], [(d_week_seq2#8 - 53)], Inner
         :- *(7) Sort [d_week_seq1#0 ASC NULLS FIRST], false, 0
         :  +- Exchange hashpartitioning(d_week_seq1#0, 200)
         :     +- *(6) Project [d_week_seq#101 AS d_week_seq1#0, sun_sales#20 AS sun_sales1#1, mon_sales#21 AS mon_sales1#2, tue_sales#22 AS tue_sales1#3, wed_sales#23 AS wed_sales1#4, thu_sales#24 AS thu_sales1#5, fri_sales#25 AS fri_sales1#6, sat_sales#26 AS sat_sales1#7]
         :        +- *(6) BroadcastHashJoin [d_week_seq#101], [d_week_seq#136], Inner, BuildRight
         :           :- *(6) HashAggregate(keys=[d_week_seq#101], functions=[sum(CASE WHEN (d_day_name#111 = Sunday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Monday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Tuesday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Wednesday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Thursday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Friday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Saturday) THEN sales_price#17 ELSE null END)], output=[d_week_seq#101, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26])
         :           :  +- Exchange hashpartitioning(d_week_seq#101, 200)
         :           :     +- *(4) HashAggregate(keys=[d_week_seq#101], functions=[partial_sum(CASE WHEN (d_day_name#111 = Sunday) THEN sales_price#17 ELSE null END), partial_sum(CASE WHEN (d_day_name#111 = Monday) THEN sales_price#17 ELSE null END), partial_sum(CASE WHEN (d_day_name#111 = Tuesday) THEN sales_price#17 ELSE null END), partial_sum(CASE WHEN (d_day_name#111 = Wednesday) THEN sales_price#17 ELSE null END), partial_sum(CASE WHEN (d_day_name#111 = Thursday) THEN sales_price#17 ELSE null END), partial_sum(CASE WHEN (d_day_name#111 = Friday) THEN sales_price#17 ELSE null END), partial_sum(CASE WHEN (d_day_name#111 = Saturday) THEN sales_price#17 ELSE null END)], output=[d_week_seq#101, sum#202, sum#203, sum#204, sum#205, sum#206, sum#207, sum#208])
         :           :        +- *(4) Project [sales_price#17, d_week_seq#101, d_day_name#111]
         :           :           +- *(4) BroadcastHashJoin [sold_date_sk#16], [d_date_sk#97], Inner, BuildRight
         :           :              :- Union
         :           :              :  :- *(1) Project [ws_sold_date_sk#29 AS sold_date_sk#16, ws_ext_sales_price#52 AS sales_price#17]
         :           :              :  :  +- *(1) Filter isnotnull(ws_sold_date_sk#29)
         :           :              :  :     +- *(1) FileScan parquet tpcds.web_sales[ws_sold_date_sk#29,ws_ext_sales_price#52] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_ext_sales_price:double>
         :           :              :  +- *(2) Project [cs_sold_date_sk#63 AS sold_date_sk#18, cs_ext_sales_price#86 AS sales_price#19]
         :           :              :     +- *(2) Filter isnotnull(cs_sold_date_sk#63)
         :           :              :        +- *(2) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#63,cs_ext_sales_price#86] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_ext_sales_price:double>
         :           :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :           :                 +- *(3) Project [d_date_sk#97, d_week_seq#101, d_day_name#111]
         :           :                    +- *(3) Filter (isnotnull(d_date_sk#97) && isnotnull(d_week_seq#101))
         :           :                       +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#97,d_week_seq#101,d_day_name#111] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk), IsNotNull(d_week_seq)], ReadSchema: struct<d_date_sk:int,d_week_seq:int,d_day_name:string>
         :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :              +- *(5) Project [d_week_seq#136]
         :                 +- *(5) Filter ((isnotnull(d_year#138) && (d_year#138 = 2001)) && isnotnull(d_week_seq#136))
         :                    +- *(5) FileScan parquet tpcds.date_dim[d_week_seq#136,d_year#138] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_week_seq)], ReadSchema: struct<d_week_seq:int,d_year:int>
         +- *(14) Sort [(d_week_seq2#8 - 53) ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning((d_week_seq2#8 - 53), 200)
               +- *(13) Project [d_week_seq#101 AS d_week_seq2#8, sun_sales#20 AS sun_sales2#9, mon_sales#21 AS mon_sales2#10, tue_sales#22 AS tue_sales2#11, wed_sales#23 AS wed_sales2#12, thu_sales#24 AS thu_sales2#13, fri_sales#25 AS fri_sales2#14, sat_sales#26 AS sat_sales2#15]
                  +- *(13) BroadcastHashJoin [d_week_seq#101], [d_week_seq#164], Inner, BuildRight
                     :- *(13) HashAggregate(keys=[d_week_seq#101], functions=[sum(CASE WHEN (d_day_name#111 = Sunday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Monday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Tuesday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Wednesday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Thursday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Friday) THEN sales_price#17 ELSE null END), sum(CASE WHEN (d_day_name#111 = Saturday) THEN sales_price#17 ELSE null END)], output=[d_week_seq#101, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26])
                     :  +- ReusedExchange [d_week_seq#101, sum#216, sum#217, sum#218, sum#219, sum#220, sum#221, sum#222], Exchange hashpartitioning(d_week_seq#101, 200)
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        +- *(12) Project [d_week_seq#164]
                           +- *(12) Filter ((isnotnull(d_year#166) && (d_year#166 = 2002)) && isnotnull(d_week_seq#164))
                              +- *(12) FileScan parquet tpcds.date_dim[d_week_seq#164,d_year#166] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_week_seq)], ReadSchema: struct<d_week_seq:int,d_year:int>
Time taken: 4.601 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 52)

== SQL ==

-- end query 2 in stream 0 using template query2.tpl
----------------------------------------------------^^^

