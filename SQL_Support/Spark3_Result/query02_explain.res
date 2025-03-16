Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579505182
== Parsed Logical Plan ==
CTE [wscs, wswscs]
:  :- 'SubqueryAlias wscs
:  :  +- 'Project ['sold_date_sk, 'sales_price]
:  :     +- 'SubqueryAlias x
:  :        +- 'Union false, false
:  :           :- 'Project ['ws_sold_date_sk AS sold_date_sk#16, 'ws_ext_sales_price AS sales_price#17]
:  :           :  +- 'UnresolvedRelation [web_sales], [], false
:  :           +- 'Project ['cs_sold_date_sk AS sold_date_sk#18, 'cs_ext_sales_price AS sales_price#19]
:  :              +- 'UnresolvedRelation [catalog_sales], [], false
:  +- 'SubqueryAlias wswscs
:     +- 'Aggregate ['d_week_seq], ['d_week_seq, 'sum(CASE WHEN ('d_day_name = Sunday) THEN 'sales_price ELSE null END) AS sun_sales#20, 'sum(CASE WHEN ('d_day_name = Monday) THEN 'sales_price ELSE null END) AS mon_sales#21, 'sum(CASE WHEN ('d_day_name = Tuesday) THEN 'sales_price ELSE null END) AS tue_sales#22, 'sum(CASE WHEN ('d_day_name = Wednesday) THEN 'sales_price ELSE null END) AS wed_sales#23, 'sum(CASE WHEN ('d_day_name = Thursday) THEN 'sales_price ELSE null END) AS thu_sales#24, 'sum(CASE WHEN ('d_day_name = Friday) THEN 'sales_price ELSE null END) AS fri_sales#25, 'sum(CASE WHEN ('d_day_name = Saturday) THEN 'sales_price ELSE null END) AS sat_sales#26]
:        +- 'Filter ('d_date_sk = 'sold_date_sk)
:           +- 'Join Inner
:              :- 'UnresolvedRelation [wscs], [], false
:              +- 'UnresolvedRelation [date_dim], [], false
+- 'Sort ['d_week_seq1 ASC NULLS FIRST], true
   +- 'Project ['d_week_seq1, unresolvedalias('round(('sun_sales1 / 'sun_sales2), 2), None), unresolvedalias('round(('mon_sales1 / 'mon_sales2), 2), None), unresolvedalias('round(('tue_sales1 / 'tue_sales2), 2), None), unresolvedalias('round(('wed_sales1 / 'wed_sales2), 2), None), unresolvedalias('round(('thu_sales1 / 'thu_sales2), 2), None), unresolvedalias('round(('fri_sales1 / 'fri_sales2), 2), None), unresolvedalias('round(('sat_sales1 / 'sat_sales2), 2), None)]
      +- 'Filter ('d_week_seq1 = ('d_week_seq2 - 53))
         +- 'Join Inner
            :- 'SubqueryAlias y
            :  +- 'Project ['wswscs.d_week_seq AS d_week_seq1#0, 'sun_sales AS sun_sales1#1, 'mon_sales AS mon_sales1#2, 'tue_sales AS tue_sales1#3, 'wed_sales AS wed_sales1#4, 'thu_sales AS thu_sales1#5, 'fri_sales AS fri_sales1#6, 'sat_sales AS sat_sales1#7]
            :     +- 'Filter (('date_dim.d_week_seq = 'wswscs.d_week_seq) AND ('d_year = 2001))
            :        +- 'Join Inner
            :           :- 'UnresolvedRelation [wswscs], [], false
            :           +- 'UnresolvedRelation [date_dim], [], false
            +- 'SubqueryAlias z
               +- 'Project ['wswscs.d_week_seq AS d_week_seq2#8, 'sun_sales AS sun_sales2#9, 'mon_sales AS mon_sales2#10, 'tue_sales AS tue_sales2#11, 'wed_sales AS wed_sales2#12, 'thu_sales AS thu_sales2#13, 'fri_sales AS fri_sales2#14, 'sat_sales AS sat_sales2#15]
                  +- 'Filter (('date_dim.d_week_seq = 'wswscs.d_week_seq) AND ('d_year = (2001 + 1)))
                     +- 'Join Inner
                        :- 'UnresolvedRelation [wswscs], [], false
                        +- 'UnresolvedRelation [date_dim], [], false

== Analyzed Logical Plan ==
d_week_seq1: int, round((sun_sales1 / sun_sales2), 2): double, round((mon_sales1 / mon_sales2), 2): double, round((tue_sales1 / tue_sales2), 2): double, round((wed_sales1 / wed_sales2), 2): double, round((thu_sales1 / thu_sales2), 2): double, round((fri_sales1 / fri_sales2), 2): double, round((sat_sales1 / sat_sales2), 2): double
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias wscs
:     +- Project [sold_date_sk#16, sales_price#17]
:        +- SubqueryAlias x
:           +- Union false, false
:              :- Project [ws_sold_date_sk#32 AS sold_date_sk#16, ws_ext_sales_price#55 AS sales_price#17]
:              :  +- SubqueryAlias spark_catalog.tpcds.web_sales
:              :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#32,ws_sold_time_sk#33,ws_ship_date_sk#34,ws_item_sk#35,ws_bill_customer_sk#36,ws_bill_cdemo_sk#37,ws_bill_hdemo_sk#38,ws_bill_addr_sk#39,ws_ship_customer_sk#40,ws_ship_cdemo_sk#41,ws_ship_hdemo_sk#42,ws_ship_addr_sk#43,ws_web_page_sk#44,ws_web_site_sk#45,ws_ship_mode_sk#46,ws_warehouse_sk#47,ws_promo_sk#48,ws_order_number#49,ws_quantity#50,ws_wholesale_cost#51,ws_list_price#52,ws_sales_price#53,ws_ext_discount_amt#54,ws_ext_sales_price#55,... 10 more fields] parquet
:              +- Project [cs_sold_date_sk#66 AS sold_date_sk#18, cs_ext_sales_price#89 AS sales_price#19]
:                 +- SubqueryAlias spark_catalog.tpcds.catalog_sales
:                    +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#66,cs_sold_time_sk#67,cs_ship_date_sk#68,cs_bill_customer_sk#69,cs_bill_cdemo_sk#70,cs_bill_hdemo_sk#71,cs_bill_addr_sk#72,cs_ship_customer_sk#73,cs_ship_cdemo_sk#74,cs_ship_hdemo_sk#75,cs_ship_addr_sk#76,cs_call_center_sk#77,cs_catalog_page_sk#78,cs_ship_mode_sk#79,cs_warehouse_sk#80,cs_item_sk#81,cs_promo_sk#82,cs_order_number#83,cs_quantity#84,cs_wholesale_cost#85,cs_list_price#86,cs_sales_price#87,cs_ext_discount_amt#88,cs_ext_sales_price#89,... 10 more fields] parquet
:- CTERelationDef 1, false
:  +- SubqueryAlias wswscs
:     +- Aggregate [d_week_seq#104], [d_week_seq#104, sum(CASE WHEN (d_day_name#114 = Sunday) THEN sales_price#17 ELSE cast(null as double) END) AS sun_sales#20, sum(CASE WHEN (d_day_name#114 = Monday) THEN sales_price#17 ELSE cast(null as double) END) AS mon_sales#21, sum(CASE WHEN (d_day_name#114 = Tuesday) THEN sales_price#17 ELSE cast(null as double) END) AS tue_sales#22, sum(CASE WHEN (d_day_name#114 = Wednesday) THEN sales_price#17 ELSE cast(null as double) END) AS wed_sales#23, sum(CASE WHEN (d_day_name#114 = Thursday) THEN sales_price#17 ELSE cast(null as double) END) AS thu_sales#24, sum(CASE WHEN (d_day_name#114 = Friday) THEN sales_price#17 ELSE cast(null as double) END) AS fri_sales#25, sum(CASE WHEN (d_day_name#114 = Saturday) THEN sales_price#17 ELSE cast(null as double) END) AS sat_sales#26]
:        +- Filter (d_date_sk#100 = sold_date_sk#16)
:           +- Join Inner
:              :- SubqueryAlias wscs
:              :  +- CTERelationRef 0, true, [sold_date_sk#16, sales_price#17], false
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#100,d_date_id#101,d_date#102,d_month_seq#103,d_week_seq#104,d_quarter_seq#105,d_year#106,d_dow#107,d_moy#108,d_dom#109,d_qoy#110,d_fy_year#111,d_fy_quarter_seq#112,d_fy_week_seq#113,d_day_name#114,d_quarter_name#115,d_holiday#116,d_weekend#117,d_following_holiday#118,d_first_dom#119,d_last_dom#120,d_same_day_ly#121,d_same_day_lq#122,d_current_day#123,... 4 more fields] parquet
+- Sort [d_week_seq1#0 ASC NULLS FIRST], true
   +- Project [d_week_seq1#0, round((sun_sales1#1 / sun_sales2#9), 2) AS round((sun_sales1 / sun_sales2), 2)#202, round((mon_sales1#2 / mon_sales2#10), 2) AS round((mon_sales1 / mon_sales2), 2)#203, round((tue_sales1#3 / tue_sales2#11), 2) AS round((tue_sales1 / tue_sales2), 2)#204, round((wed_sales1#4 / wed_sales2#12), 2) AS round((wed_sales1 / wed_sales2), 2)#205, round((thu_sales1#5 / thu_sales2#13), 2) AS round((thu_sales1 / thu_sales2), 2)#206, round((fri_sales1#6 / fri_sales2#14), 2) AS round((fri_sales1 / fri_sales2), 2)#207, round((sat_sales1#7 / sat_sales2#15), 2) AS round((sat_sales1 / sat_sales2), 2)#208]
      +- Filter (d_week_seq1#0 = (d_week_seq2#8 - 53))
         +- Join Inner
            :- SubqueryAlias y
            :  +- Project [d_week_seq#104 AS d_week_seq1#0, sun_sales#20 AS sun_sales1#1, mon_sales#21 AS mon_sales1#2, tue_sales#22 AS tue_sales1#3, wed_sales#23 AS wed_sales1#4, thu_sales#24 AS thu_sales1#5, fri_sales#25 AS fri_sales1#6, sat_sales#26 AS sat_sales1#7]
            :     +- Filter ((d_week_seq#132 = d_week_seq#104) AND (d_year#134 = 2001))
            :        +- Join Inner
            :           :- SubqueryAlias wswscs
            :           :  +- CTERelationRef 1, true, [d_week_seq#104, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26], false
            :           +- SubqueryAlias spark_catalog.tpcds.date_dim
            :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#128,d_date_id#129,d_date#130,d_month_seq#131,d_week_seq#132,d_quarter_seq#133,d_year#134,d_dow#135,d_moy#136,d_dom#137,d_qoy#138,d_fy_year#139,d_fy_quarter_seq#140,d_fy_week_seq#141,d_day_name#142,d_quarter_name#143,d_holiday#144,d_weekend#145,d_following_holiday#146,d_first_dom#147,d_last_dom#148,d_same_day_ly#149,d_same_day_lq#150,d_current_day#151,... 4 more fields] parquet
            +- SubqueryAlias z
               +- Project [d_week_seq#194 AS d_week_seq2#8, sun_sales#195 AS sun_sales2#9, mon_sales#196 AS mon_sales2#10, tue_sales#197 AS tue_sales2#11, wed_sales#198 AS wed_sales2#12, thu_sales#199 AS thu_sales2#13, fri_sales#200 AS fri_sales2#14, sat_sales#201 AS sat_sales2#15]
                  +- Filter ((d_week_seq#160 = d_week_seq#194) AND (d_year#162 = (2001 + 1)))
                     +- Join Inner
                        :- SubqueryAlias wswscs
                        :  +- CTERelationRef 1, true, [d_week_seq#194, sun_sales#195, mon_sales#196, tue_sales#197, wed_sales#198, thu_sales#199, fri_sales#200, sat_sales#201], false
                        +- SubqueryAlias spark_catalog.tpcds.date_dim
                           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#156,d_date_id#157,d_date#158,d_month_seq#159,d_week_seq#160,d_quarter_seq#161,d_year#162,d_dow#163,d_moy#164,d_dom#165,d_qoy#166,d_fy_year#167,d_fy_quarter_seq#168,d_fy_week_seq#169,d_day_name#170,d_quarter_name#171,d_holiday#172,d_weekend#173,d_following_holiday#174,d_first_dom#175,d_last_dom#176,d_same_day_ly#177,d_same_day_lq#178,d_current_day#179,... 4 more fields] parquet

== Optimized Logical Plan ==
Sort [d_week_seq1#0 ASC NULLS FIRST], true
+- Project [d_week_seq1#0, round((sun_sales1#1 / sun_sales2#9), 2) AS round((sun_sales1 / sun_sales2), 2)#202, round((mon_sales1#2 / mon_sales2#10), 2) AS round((mon_sales1 / mon_sales2), 2)#203, round((tue_sales1#3 / tue_sales2#11), 2) AS round((tue_sales1 / tue_sales2), 2)#204, round((wed_sales1#4 / wed_sales2#12), 2) AS round((wed_sales1 / wed_sales2), 2)#205, round((thu_sales1#5 / thu_sales2#13), 2) AS round((thu_sales1 / thu_sales2), 2)#206, round((fri_sales1#6 / fri_sales2#14), 2) AS round((fri_sales1 / fri_sales2), 2)#207, round((sat_sales1#7 / sat_sales2#15), 2) AS round((sat_sales1 / sat_sales2), 2)#208]
   +- Join Inner, (d_week_seq1#0 = (d_week_seq2#8 - 53))
      :- Project [d_week_seq#104 AS d_week_seq1#0, sun_sales#20 AS sun_sales1#1, mon_sales#21 AS mon_sales1#2, tue_sales#22 AS tue_sales1#3, wed_sales#23 AS wed_sales1#4, thu_sales#24 AS thu_sales1#5, fri_sales#25 AS fri_sales1#6, sat_sales#26 AS sat_sales1#7]
      :  +- Join Inner, (d_week_seq#132 = d_week_seq#104)
      :     :- Aggregate [d_week_seq#104], [d_week_seq#104, sum(CASE WHEN (d_day_name#114 = Sunday) THEN sales_price#17 END) AS sun_sales#20, sum(CASE WHEN (d_day_name#114 = Monday) THEN sales_price#17 END) AS mon_sales#21, sum(CASE WHEN (d_day_name#114 = Tuesday) THEN sales_price#17 END) AS tue_sales#22, sum(CASE WHEN (d_day_name#114 = Wednesday) THEN sales_price#17 END) AS wed_sales#23, sum(CASE WHEN (d_day_name#114 = Thursday) THEN sales_price#17 END) AS thu_sales#24, sum(CASE WHEN (d_day_name#114 = Friday) THEN sales_price#17 END) AS fri_sales#25, sum(CASE WHEN (d_day_name#114 = Saturday) THEN sales_price#17 END) AS sat_sales#26]
      :     :  +- Project [sales_price#17, d_week_seq#104, d_day_name#114]
      :     :     +- Join Inner, (d_date_sk#100 = sold_date_sk#16)
      :     :        :- Union false, false
      :     :        :  :- Project [ws_sold_date_sk#32 AS sold_date_sk#16, ws_ext_sales_price#55 AS sales_price#17]
      :     :        :  :  +- Filter isnotnull(ws_sold_date_sk#32)
      :     :        :  :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#32,ws_sold_time_sk#33,ws_ship_date_sk#34,ws_item_sk#35,ws_bill_customer_sk#36,ws_bill_cdemo_sk#37,ws_bill_hdemo_sk#38,ws_bill_addr_sk#39,ws_ship_customer_sk#40,ws_ship_cdemo_sk#41,ws_ship_hdemo_sk#42,ws_ship_addr_sk#43,ws_web_page_sk#44,ws_web_site_sk#45,ws_ship_mode_sk#46,ws_warehouse_sk#47,ws_promo_sk#48,ws_order_number#49,ws_quantity#50,ws_wholesale_cost#51,ws_list_price#52,ws_sales_price#53,ws_ext_discount_amt#54,ws_ext_sales_price#55,... 10 more fields] parquet
      :     :        :  +- Project [cs_sold_date_sk#66 AS sold_date_sk#18, cs_ext_sales_price#89 AS sales_price#19]
      :     :        :     +- Filter isnotnull(cs_sold_date_sk#66)
      :     :        :        +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#66,cs_sold_time_sk#67,cs_ship_date_sk#68,cs_bill_customer_sk#69,cs_bill_cdemo_sk#70,cs_bill_hdemo_sk#71,cs_bill_addr_sk#72,cs_ship_customer_sk#73,cs_ship_cdemo_sk#74,cs_ship_hdemo_sk#75,cs_ship_addr_sk#76,cs_call_center_sk#77,cs_catalog_page_sk#78,cs_ship_mode_sk#79,cs_warehouse_sk#80,cs_item_sk#81,cs_promo_sk#82,cs_order_number#83,cs_quantity#84,cs_wholesale_cost#85,cs_list_price#86,cs_sales_price#87,cs_ext_discount_amt#88,cs_ext_sales_price#89,... 10 more fields] parquet
      :     :        +- Project [d_date_sk#100, d_week_seq#104, d_day_name#114]
      :     :           +- Filter (isnotnull(d_date_sk#100) AND isnotnull(d_week_seq#104))
      :     :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#100,d_date_id#101,d_date#102,d_month_seq#103,d_week_seq#104,d_quarter_seq#105,d_year#106,d_dow#107,d_moy#108,d_dom#109,d_qoy#110,d_fy_year#111,d_fy_quarter_seq#112,d_fy_week_seq#113,d_day_name#114,d_quarter_name#115,d_holiday#116,d_weekend#117,d_following_holiday#118,d_first_dom#119,d_last_dom#120,d_same_day_ly#121,d_same_day_lq#122,d_current_day#123,... 4 more fields] parquet
      :     +- Project [d_week_seq#132]
      :        +- Filter ((isnotnull(d_year#134) AND (d_year#134 = 2001)) AND isnotnull(d_week_seq#132))
      :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#128,d_date_id#129,d_date#130,d_month_seq#131,d_week_seq#132,d_quarter_seq#133,d_year#134,d_dow#135,d_moy#136,d_dom#137,d_qoy#138,d_fy_year#139,d_fy_quarter_seq#140,d_fy_week_seq#141,d_day_name#142,d_quarter_name#143,d_holiday#144,d_weekend#145,d_following_holiday#146,d_first_dom#147,d_last_dom#148,d_same_day_ly#149,d_same_day_lq#150,d_current_day#151,... 4 more fields] parquet
      +- Project [d_week_seq#386 AS d_week_seq2#8, sun_sales#195 AS sun_sales2#9, mon_sales#196 AS mon_sales2#10, tue_sales#197 AS tue_sales2#11, wed_sales#198 AS wed_sales2#12, thu_sales#199 AS thu_sales2#13, fri_sales#200 AS fri_sales2#14, sat_sales#201 AS sat_sales2#15]
         +- Join Inner, (d_week_seq#160 = d_week_seq#386)
            :- Aggregate [d_week_seq#386], [d_week_seq#386, sum(CASE WHEN (d_day_name#396 = Sunday) THEN sales_price#17 END) AS sun_sales#195, sum(CASE WHEN (d_day_name#396 = Monday) THEN sales_price#17 END) AS mon_sales#196, sum(CASE WHEN (d_day_name#396 = Tuesday) THEN sales_price#17 END) AS tue_sales#197, sum(CASE WHEN (d_day_name#396 = Wednesday) THEN sales_price#17 END) AS wed_sales#198, sum(CASE WHEN (d_day_name#396 = Thursday) THEN sales_price#17 END) AS thu_sales#199, sum(CASE WHEN (d_day_name#396 = Friday) THEN sales_price#17 END) AS fri_sales#200, sum(CASE WHEN (d_day_name#396 = Saturday) THEN sales_price#17 END) AS sat_sales#201]
            :  +- Project [sales_price#17, d_week_seq#386, d_day_name#396]
            :     +- Join Inner, (d_date_sk#382 = sold_date_sk#16)
            :        :- Union false, false
            :        :  :- Project [ws_sold_date_sk#314 AS sold_date_sk#16, ws_ext_sales_price#337 AS sales_price#17]
            :        :  :  +- Filter isnotnull(ws_sold_date_sk#314)
            :        :  :     +- Relation spark_catalog.tpcds.web_sales[ws_sold_date_sk#314,ws_sold_time_sk#315,ws_ship_date_sk#316,ws_item_sk#317,ws_bill_customer_sk#318,ws_bill_cdemo_sk#319,ws_bill_hdemo_sk#320,ws_bill_addr_sk#321,ws_ship_customer_sk#322,ws_ship_cdemo_sk#323,ws_ship_hdemo_sk#324,ws_ship_addr_sk#325,ws_web_page_sk#326,ws_web_site_sk#327,ws_ship_mode_sk#328,ws_warehouse_sk#329,ws_promo_sk#330,ws_order_number#331,ws_quantity#332,ws_wholesale_cost#333,ws_list_price#334,ws_sales_price#335,ws_ext_discount_amt#336,ws_ext_sales_price#337,... 10 more fields] parquet
            :        :  +- Project [cs_sold_date_sk#348 AS sold_date_sk#18, cs_ext_sales_price#371 AS sales_price#19]
            :        :     +- Filter isnotnull(cs_sold_date_sk#348)
            :        :        +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#348,cs_sold_time_sk#349,cs_ship_date_sk#350,cs_bill_customer_sk#351,cs_bill_cdemo_sk#352,cs_bill_hdemo_sk#353,cs_bill_addr_sk#354,cs_ship_customer_sk#355,cs_ship_cdemo_sk#356,cs_ship_hdemo_sk#357,cs_ship_addr_sk#358,cs_call_center_sk#359,cs_catalog_page_sk#360,cs_ship_mode_sk#361,cs_warehouse_sk#362,cs_item_sk#363,cs_promo_sk#364,cs_order_number#365,cs_quantity#366,cs_wholesale_cost#367,cs_list_price#368,cs_sales_price#369,cs_ext_discount_amt#370,cs_ext_sales_price#371,... 10 more fields] parquet
            :        +- Project [d_date_sk#382, d_week_seq#386, d_day_name#396]
            :           +- Filter (isnotnull(d_date_sk#382) AND isnotnull(d_week_seq#386))
            :              +- Relation spark_catalog.tpcds.date_dim[d_date_sk#382,d_date_id#383,d_date#384,d_month_seq#385,d_week_seq#386,d_quarter_seq#387,d_year#388,d_dow#389,d_moy#390,d_dom#391,d_qoy#392,d_fy_year#393,d_fy_quarter_seq#394,d_fy_week_seq#395,d_day_name#396,d_quarter_name#397,d_holiday#398,d_weekend#399,d_following_holiday#400,d_first_dom#401,d_last_dom#402,d_same_day_ly#403,d_same_day_lq#404,d_current_day#405,... 4 more fields] parquet
            +- Project [d_week_seq#160]
               +- Filter ((isnotnull(d_year#162) AND (d_year#162 = 2002)) AND isnotnull(d_week_seq#160))
                  +- Relation spark_catalog.tpcds.date_dim[d_date_sk#156,d_date_id#157,d_date#158,d_month_seq#159,d_week_seq#160,d_quarter_seq#161,d_year#162,d_dow#163,d_moy#164,d_dom#165,d_qoy#166,d_fy_year#167,d_fy_quarter_seq#168,d_fy_week_seq#169,d_day_name#170,d_quarter_name#171,d_holiday#172,d_weekend#173,d_following_holiday#174,d_first_dom#175,d_last_dom#176,d_same_day_ly#177,d_same_day_lq#178,d_current_day#179,... 4 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [d_week_seq1#0 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(d_week_seq1#0 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=181]
      +- Project [d_week_seq1#0, round((sun_sales1#1 / sun_sales2#9), 2) AS round((sun_sales1 / sun_sales2), 2)#202, round((mon_sales1#2 / mon_sales2#10), 2) AS round((mon_sales1 / mon_sales2), 2)#203, round((tue_sales1#3 / tue_sales2#11), 2) AS round((tue_sales1 / tue_sales2), 2)#204, round((wed_sales1#4 / wed_sales2#12), 2) AS round((wed_sales1 / wed_sales2), 2)#205, round((thu_sales1#5 / thu_sales2#13), 2) AS round((thu_sales1 / thu_sales2), 2)#206, round((fri_sales1#6 / fri_sales2#14), 2) AS round((fri_sales1 / fri_sales2), 2)#207, round((sat_sales1#7 / sat_sales2#15), 2) AS round((sat_sales1 / sat_sales2), 2)#208]
         +- SortMergeJoin [d_week_seq1#0], [(d_week_seq2#8 - 53)], Inner
            :- Sort [d_week_seq1#0 ASC NULLS FIRST], false, 0
            :  +- Project [d_week_seq#104 AS d_week_seq1#0, sun_sales#20 AS sun_sales1#1, mon_sales#21 AS mon_sales1#2, tue_sales#22 AS tue_sales1#3, wed_sales#23 AS wed_sales1#4, thu_sales#24 AS thu_sales1#5, fri_sales#25 AS fri_sales1#6, sat_sales#26 AS sat_sales1#7]
            :     +- BroadcastHashJoin [d_week_seq#104], [d_week_seq#132], Inner, BuildRight, false
            :        :- HashAggregate(keys=[d_week_seq#104], functions=[sum(CASE WHEN (d_day_name#114 = Sunday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#114 = Monday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#114 = Tuesday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#114 = Wednesday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#114 = Thursday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#114 = Friday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#114 = Saturday) THEN sales_price#17 END)], output=[d_week_seq#104, sun_sales#20, mon_sales#21, tue_sales#22, wed_sales#23, thu_sales#24, fri_sales#25, sat_sales#26])
            :        :  +- Exchange hashpartitioning(d_week_seq#104, 200), ENSURE_REQUIREMENTS, [plan_id=156]
            :        :     +- HashAggregate(keys=[d_week_seq#104], functions=[partial_sum(CASE WHEN (d_day_name#114 = Sunday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#114 = Monday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#114 = Tuesday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#114 = Wednesday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#114 = Thursday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#114 = Friday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#114 = Saturday) THEN sales_price#17 END)], output=[d_week_seq#104, sum#424, sum#425, sum#426, sum#427, sum#428, sum#429, sum#430])
            :        :        +- Project [sales_price#17, d_week_seq#104, d_day_name#114]
            :        :           +- BroadcastHashJoin [sold_date_sk#16], [d_date_sk#100], Inner, BuildRight, false
            :        :              :- Union
            :        :              :  :- Project [ws_sold_date_sk#32 AS sold_date_sk#16, ws_ext_sales_price#55 AS sales_price#17]
            :        :              :  :  +- Filter isnotnull(ws_sold_date_sk#32)
            :        :              :  :     +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#32,ws_ext_sales_price#55] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#32)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_ext_sales_price:double>
            :        :              :  +- Project [cs_sold_date_sk#66 AS sold_date_sk#18, cs_ext_sales_price#89 AS sales_price#19]
            :        :              :     +- Filter isnotnull(cs_sold_date_sk#66)
            :        :              :        +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#66,cs_ext_sales_price#89] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#66)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_ext_sales_price:double>
            :        :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=151]
            :        :                 +- Filter (isnotnull(d_date_sk#100) AND isnotnull(d_week_seq#104))
            :        :                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#100,d_week_seq#104,d_day_name#114] Batched: true, DataFilters: [isnotnull(d_date_sk#100), isnotnull(d_week_seq#104)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk), IsNotNull(d_week_seq)], ReadSchema: struct<d_date_sk:int,d_week_seq:int,d_day_name:string>
            :        +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=159]
            :           +- Project [d_week_seq#132]
            :              +- Filter ((isnotnull(d_year#134) AND (d_year#134 = 2001)) AND isnotnull(d_week_seq#132))
            :                 +- FileScan parquet spark_catalog.tpcds.date_dim[d_week_seq#132,d_year#134] Batched: true, DataFilters: [isnotnull(d_year#134), (d_year#134 = 2001), isnotnull(d_week_seq#132)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_week_seq)], ReadSchema: struct<d_week_seq:int,d_year:int>
            +- Sort [(d_week_seq2#8 - 53) ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning((d_week_seq2#8 - 53), 200), ENSURE_REQUIREMENTS, [plan_id=175]
                  +- Project [d_week_seq#386 AS d_week_seq2#8, sun_sales#195 AS sun_sales2#9, mon_sales#196 AS mon_sales2#10, tue_sales#197 AS tue_sales2#11, wed_sales#198 AS wed_sales2#12, thu_sales#199 AS thu_sales2#13, fri_sales#200 AS fri_sales2#14, sat_sales#201 AS sat_sales2#15]
                     +- BroadcastHashJoin [d_week_seq#386], [d_week_seq#160], Inner, BuildRight, false
                        :- HashAggregate(keys=[d_week_seq#386], functions=[sum(CASE WHEN (d_day_name#396 = Sunday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#396 = Monday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#396 = Tuesday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#396 = Wednesday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#396 = Thursday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#396 = Friday) THEN sales_price#17 END), sum(CASE WHEN (d_day_name#396 = Saturday) THEN sales_price#17 END)], output=[d_week_seq#386, sun_sales#195, mon_sales#196, tue_sales#197, wed_sales#198, thu_sales#199, fri_sales#200, sat_sales#201])
                        :  +- Exchange hashpartitioning(d_week_seq#386, 200), ENSURE_REQUIREMENTS, [plan_id=167]
                        :     +- HashAggregate(keys=[d_week_seq#386], functions=[partial_sum(CASE WHEN (d_day_name#396 = Sunday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#396 = Monday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#396 = Tuesday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#396 = Wednesday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#396 = Thursday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#396 = Friday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#396 = Saturday) THEN sales_price#17 END)], output=[d_week_seq#386, sum#438, sum#439, sum#440, sum#441, sum#442, sum#443, sum#444])
                        :        +- Project [sales_price#17, d_week_seq#386, d_day_name#396]
                        :           +- BroadcastHashJoin [sold_date_sk#16], [d_date_sk#382], Inner, BuildRight, false
                        :              :- Union
                        :              :  :- Project [ws_sold_date_sk#314 AS sold_date_sk#16, ws_ext_sales_price#337 AS sales_price#17]
                        :              :  :  +- Filter isnotnull(ws_sold_date_sk#314)
                        :              :  :     +- FileScan parquet spark_catalog.tpcds.web_sales[ws_sold_date_sk#314,ws_ext_sales_price#337] Batched: true, DataFilters: [isnotnull(ws_sold_date_sk#314)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_ext_sales_price:double>
                        :              :  +- Project [cs_sold_date_sk#348 AS sold_date_sk#18, cs_ext_sales_price#371 AS sales_price#19]
                        :              :     +- Filter isnotnull(cs_sold_date_sk#348)
                        :              :        +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#348,cs_ext_sales_price#371] Batched: true, DataFilters: [isnotnull(cs_sold_date_sk#348)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_ext_sales_price:double>
                        :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=162]
                        :                 +- Filter (isnotnull(d_date_sk#382) AND isnotnull(d_week_seq#386))
                        :                    +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#382,d_week_seq#386,d_day_name#396] Batched: true, DataFilters: [isnotnull(d_date_sk#382), isnotnull(d_week_seq#386)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk), IsNotNull(d_week_seq)], ReadSchema: struct<d_date_sk:int,d_week_seq:int,d_day_name:string>
                        +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=170]
                           +- Project [d_week_seq#160]
                              +- Filter ((isnotnull(d_year#162) AND (d_year#162 = 2002)) AND isnotnull(d_week_seq#160))
                                 +- FileScan parquet spark_catalog.tpcds.date_dim[d_week_seq#160,d_year#162] Batched: true, DataFilters: [isnotnull(d_year#162), (d_year#162 = 2002), isnotnull(d_week_seq#160)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_week_seq)], ReadSchema: struct<d_week_seq:int,d_year:int>

Time taken: 3.135 seconds, Fetched 1 row(s)
