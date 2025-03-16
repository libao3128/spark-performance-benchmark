Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579786534
== Parsed Logical Plan ==
'Project [CASE WHEN (scalar-subquery#0 [] > 74129) THEN scalar-subquery#1 [] ELSE scalar-subquery#2 [] END AS bucket1#3, CASE WHEN (scalar-subquery#4 [] > 122840) THEN scalar-subquery#5 [] ELSE scalar-subquery#6 [] END AS bucket2#7, CASE WHEN (scalar-subquery#8 [] > 56580) THEN scalar-subquery#9 [] ELSE scalar-subquery#10 [] END AS bucket3#11, CASE WHEN (scalar-subquery#12 [] > 10097) THEN scalar-subquery#13 [] ELSE scalar-subquery#14 [] END AS bucket4#15, CASE WHEN (scalar-subquery#16 [] > 165306) THEN scalar-subquery#17 [] ELSE scalar-subquery#18 [] END AS bucket5#19]
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 1) AND ('ss_quantity <= 20))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 1) AND ('ss_quantity <= 20))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:  :  +- 'Filter (('ss_quantity >= 1) AND ('ss_quantity <= 20))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 21) AND ('ss_quantity <= 40))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 21) AND ('ss_quantity <= 40))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:  :  +- 'Filter (('ss_quantity >= 21) AND ('ss_quantity <= 40))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 41) AND ('ss_quantity <= 60))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 41) AND ('ss_quantity <= 60))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:  :  +- 'Filter (('ss_quantity >= 41) AND ('ss_quantity <= 60))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 61) AND ('ss_quantity <= 80))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 61) AND ('ss_quantity <= 80))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:  :  +- 'Filter (('ss_quantity >= 61) AND ('ss_quantity <= 80))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 81) AND ('ss_quantity <= 100))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 81) AND ('ss_quantity <= 100))
:  :     +- 'UnresolvedRelation [store_sales], [], false
:  +- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:     +- 'Filter (('ss_quantity >= 81) AND ('ss_quantity <= 100))
:        +- 'UnresolvedRelation [store_sales], [], false
+- 'Filter ('r_reason_sk = 1)
   +- 'UnresolvedRelation [reason], [], false

== Analyzed Logical Plan ==
bucket1: double, bucket2: double, bucket3: double, bucket4: double, bucket5: double
Project [CASE WHEN (scalar-subquery#0 [] > cast(74129 as bigint)) THEN scalar-subquery#1 [] ELSE scalar-subquery#2 [] END AS bucket1#3, CASE WHEN (scalar-subquery#4 [] > cast(122840 as bigint)) THEN scalar-subquery#5 [] ELSE scalar-subquery#6 [] END AS bucket2#7, CASE WHEN (scalar-subquery#8 [] > cast(56580 as bigint)) THEN scalar-subquery#9 [] ELSE scalar-subquery#10 [] END AS bucket3#11, CASE WHEN (scalar-subquery#12 [] > cast(10097 as bigint)) THEN scalar-subquery#13 [] ELSE scalar-subquery#14 [] END AS bucket4#15, CASE WHEN (scalar-subquery#16 [] > cast(165306 as bigint)) THEN scalar-subquery#17 [] ELSE scalar-subquery#18 [] END AS bucket5#19]
:  :- Aggregate [count(1) AS count(1)#52L]
:  :  +- Filter ((ss_quantity#39 >= 1) AND (ss_quantity#39 <= 20))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#97) AS avg(ss_ext_discount_amt)#55]
:  :  +- Filter ((ss_quantity#93 >= 1) AND (ss_quantity#93 <= 20))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#83,ss_sold_time_sk#84,ss_item_sk#85,ss_customer_sk#86,ss_cdemo_sk#87,ss_hdemo_sk#88,ss_addr_sk#89,ss_store_sk#90,ss_promo_sk#91,ss_ticket_number#92,ss_quantity#93,ss_wholesale_cost#94,ss_list_price#95,ss_sales_price#96,ss_ext_discount_amt#97,ss_ext_sales_price#98,ss_ext_wholesale_cost#99,ss_ext_list_price#100,ss_ext_tax#101,ss_coupon_amt#102,ss_net_paid#103,ss_net_paid_inc_tax#104,ss_net_profit#105] parquet
:  :- Aggregate [avg(ss_net_paid#126) AS avg(ss_net_paid)#57]
:  :  +- Filter ((ss_quantity#116 >= 1) AND (ss_quantity#116 <= 20))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#106,ss_sold_time_sk#107,ss_item_sk#108,ss_customer_sk#109,ss_cdemo_sk#110,ss_hdemo_sk#111,ss_addr_sk#112,ss_store_sk#113,ss_promo_sk#114,ss_ticket_number#115,ss_quantity#116,ss_wholesale_cost#117,ss_list_price#118,ss_sales_price#119,ss_ext_discount_amt#120,ss_ext_sales_price#121,ss_ext_wholesale_cost#122,ss_ext_list_price#123,ss_ext_tax#124,ss_coupon_amt#125,ss_net_paid#126,ss_net_paid_inc_tax#127,ss_net_profit#128] parquet
:  :- Aggregate [count(1) AS count(1)#59L]
:  :  +- Filter ((ss_quantity#139 >= 21) AND (ss_quantity#139 <= 40))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#129,ss_sold_time_sk#130,ss_item_sk#131,ss_customer_sk#132,ss_cdemo_sk#133,ss_hdemo_sk#134,ss_addr_sk#135,ss_store_sk#136,ss_promo_sk#137,ss_ticket_number#138,ss_quantity#139,ss_wholesale_cost#140,ss_list_price#141,ss_sales_price#142,ss_ext_discount_amt#143,ss_ext_sales_price#144,ss_ext_wholesale_cost#145,ss_ext_list_price#146,ss_ext_tax#147,ss_coupon_amt#148,ss_net_paid#149,ss_net_paid_inc_tax#150,ss_net_profit#151] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#166) AS avg(ss_ext_discount_amt)#61]
:  :  +- Filter ((ss_quantity#162 >= 21) AND (ss_quantity#162 <= 40))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#152,ss_sold_time_sk#153,ss_item_sk#154,ss_customer_sk#155,ss_cdemo_sk#156,ss_hdemo_sk#157,ss_addr_sk#158,ss_store_sk#159,ss_promo_sk#160,ss_ticket_number#161,ss_quantity#162,ss_wholesale_cost#163,ss_list_price#164,ss_sales_price#165,ss_ext_discount_amt#166,ss_ext_sales_price#167,ss_ext_wholesale_cost#168,ss_ext_list_price#169,ss_ext_tax#170,ss_coupon_amt#171,ss_net_paid#172,ss_net_paid_inc_tax#173,ss_net_profit#174] parquet
:  :- Aggregate [avg(ss_net_paid#195) AS avg(ss_net_paid)#63]
:  :  +- Filter ((ss_quantity#185 >= 21) AND (ss_quantity#185 <= 40))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#175,ss_sold_time_sk#176,ss_item_sk#177,ss_customer_sk#178,ss_cdemo_sk#179,ss_hdemo_sk#180,ss_addr_sk#181,ss_store_sk#182,ss_promo_sk#183,ss_ticket_number#184,ss_quantity#185,ss_wholesale_cost#186,ss_list_price#187,ss_sales_price#188,ss_ext_discount_amt#189,ss_ext_sales_price#190,ss_ext_wholesale_cost#191,ss_ext_list_price#192,ss_ext_tax#193,ss_coupon_amt#194,ss_net_paid#195,ss_net_paid_inc_tax#196,ss_net_profit#197] parquet
:  :- Aggregate [count(1) AS count(1)#65L]
:  :  +- Filter ((ss_quantity#208 >= 41) AND (ss_quantity#208 <= 60))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#198,ss_sold_time_sk#199,ss_item_sk#200,ss_customer_sk#201,ss_cdemo_sk#202,ss_hdemo_sk#203,ss_addr_sk#204,ss_store_sk#205,ss_promo_sk#206,ss_ticket_number#207,ss_quantity#208,ss_wholesale_cost#209,ss_list_price#210,ss_sales_price#211,ss_ext_discount_amt#212,ss_ext_sales_price#213,ss_ext_wholesale_cost#214,ss_ext_list_price#215,ss_ext_tax#216,ss_coupon_amt#217,ss_net_paid#218,ss_net_paid_inc_tax#219,ss_net_profit#220] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#235) AS avg(ss_ext_discount_amt)#67]
:  :  +- Filter ((ss_quantity#231 >= 41) AND (ss_quantity#231 <= 60))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#221,ss_sold_time_sk#222,ss_item_sk#223,ss_customer_sk#224,ss_cdemo_sk#225,ss_hdemo_sk#226,ss_addr_sk#227,ss_store_sk#228,ss_promo_sk#229,ss_ticket_number#230,ss_quantity#231,ss_wholesale_cost#232,ss_list_price#233,ss_sales_price#234,ss_ext_discount_amt#235,ss_ext_sales_price#236,ss_ext_wholesale_cost#237,ss_ext_list_price#238,ss_ext_tax#239,ss_coupon_amt#240,ss_net_paid#241,ss_net_paid_inc_tax#242,ss_net_profit#243] parquet
:  :- Aggregate [avg(ss_net_paid#264) AS avg(ss_net_paid)#69]
:  :  +- Filter ((ss_quantity#254 >= 41) AND (ss_quantity#254 <= 60))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#244,ss_sold_time_sk#245,ss_item_sk#246,ss_customer_sk#247,ss_cdemo_sk#248,ss_hdemo_sk#249,ss_addr_sk#250,ss_store_sk#251,ss_promo_sk#252,ss_ticket_number#253,ss_quantity#254,ss_wholesale_cost#255,ss_list_price#256,ss_sales_price#257,ss_ext_discount_amt#258,ss_ext_sales_price#259,ss_ext_wholesale_cost#260,ss_ext_list_price#261,ss_ext_tax#262,ss_coupon_amt#263,ss_net_paid#264,ss_net_paid_inc_tax#265,ss_net_profit#266] parquet
:  :- Aggregate [count(1) AS count(1)#71L]
:  :  +- Filter ((ss_quantity#277 >= 61) AND (ss_quantity#277 <= 80))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#267,ss_sold_time_sk#268,ss_item_sk#269,ss_customer_sk#270,ss_cdemo_sk#271,ss_hdemo_sk#272,ss_addr_sk#273,ss_store_sk#274,ss_promo_sk#275,ss_ticket_number#276,ss_quantity#277,ss_wholesale_cost#278,ss_list_price#279,ss_sales_price#280,ss_ext_discount_amt#281,ss_ext_sales_price#282,ss_ext_wholesale_cost#283,ss_ext_list_price#284,ss_ext_tax#285,ss_coupon_amt#286,ss_net_paid#287,ss_net_paid_inc_tax#288,ss_net_profit#289] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#304) AS avg(ss_ext_discount_amt)#73]
:  :  +- Filter ((ss_quantity#300 >= 61) AND (ss_quantity#300 <= 80))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#290,ss_sold_time_sk#291,ss_item_sk#292,ss_customer_sk#293,ss_cdemo_sk#294,ss_hdemo_sk#295,ss_addr_sk#296,ss_store_sk#297,ss_promo_sk#298,ss_ticket_number#299,ss_quantity#300,ss_wholesale_cost#301,ss_list_price#302,ss_sales_price#303,ss_ext_discount_amt#304,ss_ext_sales_price#305,ss_ext_wholesale_cost#306,ss_ext_list_price#307,ss_ext_tax#308,ss_coupon_amt#309,ss_net_paid#310,ss_net_paid_inc_tax#311,ss_net_profit#312] parquet
:  :- Aggregate [avg(ss_net_paid#333) AS avg(ss_net_paid)#75]
:  :  +- Filter ((ss_quantity#323 >= 61) AND (ss_quantity#323 <= 80))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#313,ss_sold_time_sk#314,ss_item_sk#315,ss_customer_sk#316,ss_cdemo_sk#317,ss_hdemo_sk#318,ss_addr_sk#319,ss_store_sk#320,ss_promo_sk#321,ss_ticket_number#322,ss_quantity#323,ss_wholesale_cost#324,ss_list_price#325,ss_sales_price#326,ss_ext_discount_amt#327,ss_ext_sales_price#328,ss_ext_wholesale_cost#329,ss_ext_list_price#330,ss_ext_tax#331,ss_coupon_amt#332,ss_net_paid#333,ss_net_paid_inc_tax#334,ss_net_profit#335] parquet
:  :- Aggregate [count(1) AS count(1)#77L]
:  :  +- Filter ((ss_quantity#346 >= 81) AND (ss_quantity#346 <= 100))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#336,ss_sold_time_sk#337,ss_item_sk#338,ss_customer_sk#339,ss_cdemo_sk#340,ss_hdemo_sk#341,ss_addr_sk#342,ss_store_sk#343,ss_promo_sk#344,ss_ticket_number#345,ss_quantity#346,ss_wholesale_cost#347,ss_list_price#348,ss_sales_price#349,ss_ext_discount_amt#350,ss_ext_sales_price#351,ss_ext_wholesale_cost#352,ss_ext_list_price#353,ss_ext_tax#354,ss_coupon_amt#355,ss_net_paid#356,ss_net_paid_inc_tax#357,ss_net_profit#358] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#373) AS avg(ss_ext_discount_amt)#79]
:  :  +- Filter ((ss_quantity#369 >= 81) AND (ss_quantity#369 <= 100))
:  :     +- SubqueryAlias spark_catalog.tpcds.store_sales
:  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#359,ss_sold_time_sk#360,ss_item_sk#361,ss_customer_sk#362,ss_cdemo_sk#363,ss_hdemo_sk#364,ss_addr_sk#365,ss_store_sk#366,ss_promo_sk#367,ss_ticket_number#368,ss_quantity#369,ss_wholesale_cost#370,ss_list_price#371,ss_sales_price#372,ss_ext_discount_amt#373,ss_ext_sales_price#374,ss_ext_wholesale_cost#375,ss_ext_list_price#376,ss_ext_tax#377,ss_coupon_amt#378,ss_net_paid#379,ss_net_paid_inc_tax#380,ss_net_profit#381] parquet
:  +- Aggregate [avg(ss_net_paid#402) AS avg(ss_net_paid)#81]
:     +- Filter ((ss_quantity#392 >= 81) AND (ss_quantity#392 <= 100))
:        +- SubqueryAlias spark_catalog.tpcds.store_sales
:           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#382,ss_sold_time_sk#383,ss_item_sk#384,ss_customer_sk#385,ss_cdemo_sk#386,ss_hdemo_sk#387,ss_addr_sk#388,ss_store_sk#389,ss_promo_sk#390,ss_ticket_number#391,ss_quantity#392,ss_wholesale_cost#393,ss_list_price#394,ss_sales_price#395,ss_ext_discount_amt#396,ss_ext_sales_price#397,ss_ext_wholesale_cost#398,ss_ext_list_price#399,ss_ext_tax#400,ss_coupon_amt#401,ss_net_paid#402,ss_net_paid_inc_tax#403,ss_net_profit#404] parquet
+- Filter (r_reason_sk#25 = 1)
   +- SubqueryAlias spark_catalog.tpcds.reason
      +- Relation spark_catalog.tpcds.reason[r_reason_sk#25,r_reason_id#26,r_reason_desc#27] parquet

== Optimized Logical Plan ==
Project [CASE WHEN (scalar-subquery#0 [].count(1) > 74129) THEN scalar-subquery#1 [].avg(ss_ext_discount_amt) ELSE scalar-subquery#2 [].avg(ss_net_paid) END AS bucket1#3, CASE WHEN (scalar-subquery#4 [].count(1) > 122840) THEN scalar-subquery#5 [].avg(ss_ext_discount_amt) ELSE scalar-subquery#6 [].avg(ss_net_paid) END AS bucket2#7, CASE WHEN (scalar-subquery#8 [].count(1) > 56580) THEN scalar-subquery#9 [].avg(ss_ext_discount_amt) ELSE scalar-subquery#10 [].avg(ss_net_paid) END AS bucket3#11, CASE WHEN (scalar-subquery#12 [].count(1) > 10097) THEN scalar-subquery#13 [].avg(ss_ext_discount_amt) ELSE scalar-subquery#14 [].avg(ss_net_paid) END AS bucket4#15, CASE WHEN (scalar-subquery#16 [].count(1) > 165306) THEN scalar-subquery#17 [].avg(ss_ext_discount_amt) ELSE scalar-subquery#18 [].avg(ss_net_paid) END AS bucket5#19]
:  :- Project [named_struct(count(1), count(1)#52L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#55, avg(ss_net_paid), avg(ss_net_paid)#57) AS mergedValue#448]
:  :  +- Aggregate [count(1) AS count(1)#52L, avg(ss_ext_discount_amt#43) AS avg(ss_ext_discount_amt)#55, avg(ss_net_paid#49) AS avg(ss_net_paid)#57]
:  :     +- Project [ss_ext_discount_amt#43, ss_net_paid#49]
:  :        +- Filter (isnotnull(ss_quantity#39) AND ((ss_quantity#39 >= 1) AND (ss_quantity#39 <= 20)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
:  :- Project [named_struct(count(1), count(1)#52L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#55, avg(ss_net_paid), avg(ss_net_paid)#57) AS mergedValue#448]
:  :  +- Aggregate [count(1) AS count(1)#52L, avg(ss_ext_discount_amt#43) AS avg(ss_ext_discount_amt)#55, avg(ss_net_paid#49) AS avg(ss_net_paid)#57]
:  :     +- Project [ss_ext_discount_amt#43, ss_net_paid#49]
:  :        +- Filter (isnotnull(ss_quantity#39) AND ((ss_quantity#39 >= 1) AND (ss_quantity#39 <= 20)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
:  :- Project [named_struct(count(1), count(1)#52L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#55, avg(ss_net_paid), avg(ss_net_paid)#57) AS mergedValue#448]
:  :  +- Aggregate [count(1) AS count(1)#52L, avg(ss_ext_discount_amt#43) AS avg(ss_ext_discount_amt)#55, avg(ss_net_paid#49) AS avg(ss_net_paid)#57]
:  :     +- Project [ss_ext_discount_amt#43, ss_net_paid#49]
:  :        +- Filter (isnotnull(ss_quantity#39) AND ((ss_quantity#39 >= 1) AND (ss_quantity#39 <= 20)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#29,ss_sold_time_sk#30,ss_item_sk#31,ss_customer_sk#32,ss_cdemo_sk#33,ss_hdemo_sk#34,ss_addr_sk#35,ss_store_sk#36,ss_promo_sk#37,ss_ticket_number#38,ss_quantity#39,ss_wholesale_cost#40,ss_list_price#41,ss_sales_price#42,ss_ext_discount_amt#43,ss_ext_sales_price#44,ss_ext_wholesale_cost#45,ss_ext_list_price#46,ss_ext_tax#47,ss_coupon_amt#48,ss_net_paid#49,ss_net_paid_inc_tax#50,ss_net_profit#51] parquet
:  :- Project [named_struct(count(1), count(1)#59L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#61, avg(ss_net_paid), avg(ss_net_paid)#63) AS mergedValue#449]
:  :  +- Aggregate [count(1) AS count(1)#59L, avg(ss_ext_discount_amt#143) AS avg(ss_ext_discount_amt)#61, avg(ss_net_paid#149) AS avg(ss_net_paid)#63]
:  :     +- Project [ss_ext_discount_amt#143, ss_net_paid#149]
:  :        +- Filter (isnotnull(ss_quantity#139) AND ((ss_quantity#139 >= 21) AND (ss_quantity#139 <= 40)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#129,ss_sold_time_sk#130,ss_item_sk#131,ss_customer_sk#132,ss_cdemo_sk#133,ss_hdemo_sk#134,ss_addr_sk#135,ss_store_sk#136,ss_promo_sk#137,ss_ticket_number#138,ss_quantity#139,ss_wholesale_cost#140,ss_list_price#141,ss_sales_price#142,ss_ext_discount_amt#143,ss_ext_sales_price#144,ss_ext_wholesale_cost#145,ss_ext_list_price#146,ss_ext_tax#147,ss_coupon_amt#148,ss_net_paid#149,ss_net_paid_inc_tax#150,ss_net_profit#151] parquet
:  :- Project [named_struct(count(1), count(1)#59L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#61, avg(ss_net_paid), avg(ss_net_paid)#63) AS mergedValue#449]
:  :  +- Aggregate [count(1) AS count(1)#59L, avg(ss_ext_discount_amt#143) AS avg(ss_ext_discount_amt)#61, avg(ss_net_paid#149) AS avg(ss_net_paid)#63]
:  :     +- Project [ss_ext_discount_amt#143, ss_net_paid#149]
:  :        +- Filter (isnotnull(ss_quantity#139) AND ((ss_quantity#139 >= 21) AND (ss_quantity#139 <= 40)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#129,ss_sold_time_sk#130,ss_item_sk#131,ss_customer_sk#132,ss_cdemo_sk#133,ss_hdemo_sk#134,ss_addr_sk#135,ss_store_sk#136,ss_promo_sk#137,ss_ticket_number#138,ss_quantity#139,ss_wholesale_cost#140,ss_list_price#141,ss_sales_price#142,ss_ext_discount_amt#143,ss_ext_sales_price#144,ss_ext_wholesale_cost#145,ss_ext_list_price#146,ss_ext_tax#147,ss_coupon_amt#148,ss_net_paid#149,ss_net_paid_inc_tax#150,ss_net_profit#151] parquet
:  :- Project [named_struct(count(1), count(1)#59L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#61, avg(ss_net_paid), avg(ss_net_paid)#63) AS mergedValue#449]
:  :  +- Aggregate [count(1) AS count(1)#59L, avg(ss_ext_discount_amt#143) AS avg(ss_ext_discount_amt)#61, avg(ss_net_paid#149) AS avg(ss_net_paid)#63]
:  :     +- Project [ss_ext_discount_amt#143, ss_net_paid#149]
:  :        +- Filter (isnotnull(ss_quantity#139) AND ((ss_quantity#139 >= 21) AND (ss_quantity#139 <= 40)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#129,ss_sold_time_sk#130,ss_item_sk#131,ss_customer_sk#132,ss_cdemo_sk#133,ss_hdemo_sk#134,ss_addr_sk#135,ss_store_sk#136,ss_promo_sk#137,ss_ticket_number#138,ss_quantity#139,ss_wholesale_cost#140,ss_list_price#141,ss_sales_price#142,ss_ext_discount_amt#143,ss_ext_sales_price#144,ss_ext_wholesale_cost#145,ss_ext_list_price#146,ss_ext_tax#147,ss_coupon_amt#148,ss_net_paid#149,ss_net_paid_inc_tax#150,ss_net_profit#151] parquet
:  :- Project [named_struct(count(1), count(1)#65L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#67, avg(ss_net_paid), avg(ss_net_paid)#69) AS mergedValue#450]
:  :  +- Aggregate [count(1) AS count(1)#65L, avg(ss_ext_discount_amt#212) AS avg(ss_ext_discount_amt)#67, avg(ss_net_paid#218) AS avg(ss_net_paid)#69]
:  :     +- Project [ss_ext_discount_amt#212, ss_net_paid#218]
:  :        +- Filter (isnotnull(ss_quantity#208) AND ((ss_quantity#208 >= 41) AND (ss_quantity#208 <= 60)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#198,ss_sold_time_sk#199,ss_item_sk#200,ss_customer_sk#201,ss_cdemo_sk#202,ss_hdemo_sk#203,ss_addr_sk#204,ss_store_sk#205,ss_promo_sk#206,ss_ticket_number#207,ss_quantity#208,ss_wholesale_cost#209,ss_list_price#210,ss_sales_price#211,ss_ext_discount_amt#212,ss_ext_sales_price#213,ss_ext_wholesale_cost#214,ss_ext_list_price#215,ss_ext_tax#216,ss_coupon_amt#217,ss_net_paid#218,ss_net_paid_inc_tax#219,ss_net_profit#220] parquet
:  :- Project [named_struct(count(1), count(1)#65L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#67, avg(ss_net_paid), avg(ss_net_paid)#69) AS mergedValue#450]
:  :  +- Aggregate [count(1) AS count(1)#65L, avg(ss_ext_discount_amt#212) AS avg(ss_ext_discount_amt)#67, avg(ss_net_paid#218) AS avg(ss_net_paid)#69]
:  :     +- Project [ss_ext_discount_amt#212, ss_net_paid#218]
:  :        +- Filter (isnotnull(ss_quantity#208) AND ((ss_quantity#208 >= 41) AND (ss_quantity#208 <= 60)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#198,ss_sold_time_sk#199,ss_item_sk#200,ss_customer_sk#201,ss_cdemo_sk#202,ss_hdemo_sk#203,ss_addr_sk#204,ss_store_sk#205,ss_promo_sk#206,ss_ticket_number#207,ss_quantity#208,ss_wholesale_cost#209,ss_list_price#210,ss_sales_price#211,ss_ext_discount_amt#212,ss_ext_sales_price#213,ss_ext_wholesale_cost#214,ss_ext_list_price#215,ss_ext_tax#216,ss_coupon_amt#217,ss_net_paid#218,ss_net_paid_inc_tax#219,ss_net_profit#220] parquet
:  :- Project [named_struct(count(1), count(1)#65L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#67, avg(ss_net_paid), avg(ss_net_paid)#69) AS mergedValue#450]
:  :  +- Aggregate [count(1) AS count(1)#65L, avg(ss_ext_discount_amt#212) AS avg(ss_ext_discount_amt)#67, avg(ss_net_paid#218) AS avg(ss_net_paid)#69]
:  :     +- Project [ss_ext_discount_amt#212, ss_net_paid#218]
:  :        +- Filter (isnotnull(ss_quantity#208) AND ((ss_quantity#208 >= 41) AND (ss_quantity#208 <= 60)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#198,ss_sold_time_sk#199,ss_item_sk#200,ss_customer_sk#201,ss_cdemo_sk#202,ss_hdemo_sk#203,ss_addr_sk#204,ss_store_sk#205,ss_promo_sk#206,ss_ticket_number#207,ss_quantity#208,ss_wholesale_cost#209,ss_list_price#210,ss_sales_price#211,ss_ext_discount_amt#212,ss_ext_sales_price#213,ss_ext_wholesale_cost#214,ss_ext_list_price#215,ss_ext_tax#216,ss_coupon_amt#217,ss_net_paid#218,ss_net_paid_inc_tax#219,ss_net_profit#220] parquet
:  :- Project [named_struct(count(1), count(1)#71L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#73, avg(ss_net_paid), avg(ss_net_paid)#75) AS mergedValue#451]
:  :  +- Aggregate [count(1) AS count(1)#71L, avg(ss_ext_discount_amt#281) AS avg(ss_ext_discount_amt)#73, avg(ss_net_paid#287) AS avg(ss_net_paid)#75]
:  :     +- Project [ss_ext_discount_amt#281, ss_net_paid#287]
:  :        +- Filter (isnotnull(ss_quantity#277) AND ((ss_quantity#277 >= 61) AND (ss_quantity#277 <= 80)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#267,ss_sold_time_sk#268,ss_item_sk#269,ss_customer_sk#270,ss_cdemo_sk#271,ss_hdemo_sk#272,ss_addr_sk#273,ss_store_sk#274,ss_promo_sk#275,ss_ticket_number#276,ss_quantity#277,ss_wholesale_cost#278,ss_list_price#279,ss_sales_price#280,ss_ext_discount_amt#281,ss_ext_sales_price#282,ss_ext_wholesale_cost#283,ss_ext_list_price#284,ss_ext_tax#285,ss_coupon_amt#286,ss_net_paid#287,ss_net_paid_inc_tax#288,ss_net_profit#289] parquet
:  :- Project [named_struct(count(1), count(1)#71L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#73, avg(ss_net_paid), avg(ss_net_paid)#75) AS mergedValue#451]
:  :  +- Aggregate [count(1) AS count(1)#71L, avg(ss_ext_discount_amt#281) AS avg(ss_ext_discount_amt)#73, avg(ss_net_paid#287) AS avg(ss_net_paid)#75]
:  :     +- Project [ss_ext_discount_amt#281, ss_net_paid#287]
:  :        +- Filter (isnotnull(ss_quantity#277) AND ((ss_quantity#277 >= 61) AND (ss_quantity#277 <= 80)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#267,ss_sold_time_sk#268,ss_item_sk#269,ss_customer_sk#270,ss_cdemo_sk#271,ss_hdemo_sk#272,ss_addr_sk#273,ss_store_sk#274,ss_promo_sk#275,ss_ticket_number#276,ss_quantity#277,ss_wholesale_cost#278,ss_list_price#279,ss_sales_price#280,ss_ext_discount_amt#281,ss_ext_sales_price#282,ss_ext_wholesale_cost#283,ss_ext_list_price#284,ss_ext_tax#285,ss_coupon_amt#286,ss_net_paid#287,ss_net_paid_inc_tax#288,ss_net_profit#289] parquet
:  :- Project [named_struct(count(1), count(1)#71L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#73, avg(ss_net_paid), avg(ss_net_paid)#75) AS mergedValue#451]
:  :  +- Aggregate [count(1) AS count(1)#71L, avg(ss_ext_discount_amt#281) AS avg(ss_ext_discount_amt)#73, avg(ss_net_paid#287) AS avg(ss_net_paid)#75]
:  :     +- Project [ss_ext_discount_amt#281, ss_net_paid#287]
:  :        +- Filter (isnotnull(ss_quantity#277) AND ((ss_quantity#277 >= 61) AND (ss_quantity#277 <= 80)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#267,ss_sold_time_sk#268,ss_item_sk#269,ss_customer_sk#270,ss_cdemo_sk#271,ss_hdemo_sk#272,ss_addr_sk#273,ss_store_sk#274,ss_promo_sk#275,ss_ticket_number#276,ss_quantity#277,ss_wholesale_cost#278,ss_list_price#279,ss_sales_price#280,ss_ext_discount_amt#281,ss_ext_sales_price#282,ss_ext_wholesale_cost#283,ss_ext_list_price#284,ss_ext_tax#285,ss_coupon_amt#286,ss_net_paid#287,ss_net_paid_inc_tax#288,ss_net_profit#289] parquet
:  :- Project [named_struct(count(1), count(1)#77L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#79, avg(ss_net_paid), avg(ss_net_paid)#81) AS mergedValue#452]
:  :  +- Aggregate [count(1) AS count(1)#77L, avg(ss_ext_discount_amt#350) AS avg(ss_ext_discount_amt)#79, avg(ss_net_paid#356) AS avg(ss_net_paid)#81]
:  :     +- Project [ss_ext_discount_amt#350, ss_net_paid#356]
:  :        +- Filter (isnotnull(ss_quantity#346) AND ((ss_quantity#346 >= 81) AND (ss_quantity#346 <= 100)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#336,ss_sold_time_sk#337,ss_item_sk#338,ss_customer_sk#339,ss_cdemo_sk#340,ss_hdemo_sk#341,ss_addr_sk#342,ss_store_sk#343,ss_promo_sk#344,ss_ticket_number#345,ss_quantity#346,ss_wholesale_cost#347,ss_list_price#348,ss_sales_price#349,ss_ext_discount_amt#350,ss_ext_sales_price#351,ss_ext_wholesale_cost#352,ss_ext_list_price#353,ss_ext_tax#354,ss_coupon_amt#355,ss_net_paid#356,ss_net_paid_inc_tax#357,ss_net_profit#358] parquet
:  :- Project [named_struct(count(1), count(1)#77L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#79, avg(ss_net_paid), avg(ss_net_paid)#81) AS mergedValue#452]
:  :  +- Aggregate [count(1) AS count(1)#77L, avg(ss_ext_discount_amt#350) AS avg(ss_ext_discount_amt)#79, avg(ss_net_paid#356) AS avg(ss_net_paid)#81]
:  :     +- Project [ss_ext_discount_amt#350, ss_net_paid#356]
:  :        +- Filter (isnotnull(ss_quantity#346) AND ((ss_quantity#346 >= 81) AND (ss_quantity#346 <= 100)))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#336,ss_sold_time_sk#337,ss_item_sk#338,ss_customer_sk#339,ss_cdemo_sk#340,ss_hdemo_sk#341,ss_addr_sk#342,ss_store_sk#343,ss_promo_sk#344,ss_ticket_number#345,ss_quantity#346,ss_wholesale_cost#347,ss_list_price#348,ss_sales_price#349,ss_ext_discount_amt#350,ss_ext_sales_price#351,ss_ext_wholesale_cost#352,ss_ext_list_price#353,ss_ext_tax#354,ss_coupon_amt#355,ss_net_paid#356,ss_net_paid_inc_tax#357,ss_net_profit#358] parquet
:  +- Project [named_struct(count(1), count(1)#77L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#79, avg(ss_net_paid), avg(ss_net_paid)#81) AS mergedValue#452]
:     +- Aggregate [count(1) AS count(1)#77L, avg(ss_ext_discount_amt#350) AS avg(ss_ext_discount_amt)#79, avg(ss_net_paid#356) AS avg(ss_net_paid)#81]
:        +- Project [ss_ext_discount_amt#350, ss_net_paid#356]
:           +- Filter (isnotnull(ss_quantity#346) AND ((ss_quantity#346 >= 81) AND (ss_quantity#346 <= 100)))
:              +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#336,ss_sold_time_sk#337,ss_item_sk#338,ss_customer_sk#339,ss_cdemo_sk#340,ss_hdemo_sk#341,ss_addr_sk#342,ss_store_sk#343,ss_promo_sk#344,ss_ticket_number#345,ss_quantity#346,ss_wholesale_cost#347,ss_list_price#348,ss_sales_price#349,ss_ext_discount_amt#350,ss_ext_sales_price#351,ss_ext_wholesale_cost#352,ss_ext_list_price#353,ss_ext_tax#354,ss_coupon_amt#355,ss_net_paid#356,ss_net_paid_inc_tax#357,ss_net_profit#358] parquet
+- Filter (isnotnull(r_reason_sk#25) AND (r_reason_sk#25 = 1))
   +- Relation spark_catalog.tpcds.reason[r_reason_sk#25,r_reason_id#26,r_reason_desc#27] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [CASE WHEN (Subquery subquery#0, [id=#253].count(1) > 74129) THEN Subquery subquery#1, [id=#254].avg(ss_ext_discount_amt) ELSE Subquery subquery#2, [id=#255].avg(ss_net_paid) END AS bucket1#3, CASE WHEN (Subquery subquery#4, [id=#256].count(1) > 122840) THEN Subquery subquery#5, [id=#257].avg(ss_ext_discount_amt) ELSE Subquery subquery#6, [id=#258].avg(ss_net_paid) END AS bucket2#7, CASE WHEN (Subquery subquery#8, [id=#259].count(1) > 56580) THEN Subquery subquery#9, [id=#260].avg(ss_ext_discount_amt) ELSE Subquery subquery#10, [id=#261].avg(ss_net_paid) END AS bucket3#11, CASE WHEN (Subquery subquery#12, [id=#262].count(1) > 10097) THEN Subquery subquery#13, [id=#263].avg(ss_ext_discount_amt) ELSE Subquery subquery#14, [id=#264].avg(ss_net_paid) END AS bucket4#15, CASE WHEN (Subquery subquery#16, [id=#265].count(1) > 165306) THEN Subquery subquery#17, [id=#266].avg(ss_ext_discount_amt) ELSE Subquery subquery#18, [id=#267].avg(ss_net_paid) END AS bucket5#19]
   :  :- Subquery subquery#0, [id=#253]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#52L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#55, avg(ss_net_paid), avg(ss_net_paid)#57) AS mergedValue#448]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#43), avg(ss_net_paid#49)], output=[count(1)#52L, avg(ss_ext_discount_amt)#55, avg(ss_net_paid)#57])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=26]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#43), partial_avg(ss_net_paid#49)], output=[count#453L, sum#454, count#455L, sum#456, count#457L])
   :  :                 +- Project [ss_ext_discount_amt#43, ss_net_paid#49]
   :  :                    +- Filter ((isnotnull(ss_quantity#39) AND (ss_quantity#39 >= 1)) AND (ss_quantity#39 <= 20))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#39,ss_ext_discount_amt#43,ss_net_paid#49] Batched: true, DataFilters: [isnotnull(ss_quantity#39), (ss_quantity#39 >= 1), (ss_quantity#39 <= 20)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,1), LessThanOrEqual(ss_quantity,20)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#1, [id=#254]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#52L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#55, avg(ss_net_paid), avg(ss_net_paid)#57) AS mergedValue#448]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#43), avg(ss_net_paid#49)], output=[count(1)#52L, avg(ss_ext_discount_amt)#55, avg(ss_net_paid)#57])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=42]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#43), partial_avg(ss_net_paid#49)], output=[count#453L, sum#454, count#455L, sum#456, count#457L])
   :  :                 +- Project [ss_ext_discount_amt#43, ss_net_paid#49]
   :  :                    +- Filter ((isnotnull(ss_quantity#39) AND (ss_quantity#39 >= 1)) AND (ss_quantity#39 <= 20))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#39,ss_ext_discount_amt#43,ss_net_paid#49] Batched: true, DataFilters: [isnotnull(ss_quantity#39), (ss_quantity#39 >= 1), (ss_quantity#39 <= 20)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,1), LessThanOrEqual(ss_quantity,20)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#2, [id=#255]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#52L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#55, avg(ss_net_paid), avg(ss_net_paid)#57) AS mergedValue#448]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#43), avg(ss_net_paid#49)], output=[count(1)#52L, avg(ss_ext_discount_amt)#55, avg(ss_net_paid)#57])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=58]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#43), partial_avg(ss_net_paid#49)], output=[count#453L, sum#454, count#455L, sum#456, count#457L])
   :  :                 +- Project [ss_ext_discount_amt#43, ss_net_paid#49]
   :  :                    +- Filter ((isnotnull(ss_quantity#39) AND (ss_quantity#39 >= 1)) AND (ss_quantity#39 <= 20))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#39,ss_ext_discount_amt#43,ss_net_paid#49] Batched: true, DataFilters: [isnotnull(ss_quantity#39), (ss_quantity#39 >= 1), (ss_quantity#39 <= 20)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,1), LessThanOrEqual(ss_quantity,20)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#4, [id=#256]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#59L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#61, avg(ss_net_paid), avg(ss_net_paid)#63) AS mergedValue#449]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#143), avg(ss_net_paid#149)], output=[count(1)#59L, avg(ss_ext_discount_amt)#61, avg(ss_net_paid)#63])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=74]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#143), partial_avg(ss_net_paid#149)], output=[count#458L, sum#459, count#460L, sum#461, count#462L])
   :  :                 +- Project [ss_ext_discount_amt#143, ss_net_paid#149]
   :  :                    +- Filter ((isnotnull(ss_quantity#139) AND (ss_quantity#139 >= 21)) AND (ss_quantity#139 <= 40))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#139,ss_ext_discount_amt#143,ss_net_paid#149] Batched: true, DataFilters: [isnotnull(ss_quantity#139), (ss_quantity#139 >= 21), (ss_quantity#139 <= 40)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,21), LessThanOrEqual(ss_quantity,40)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#5, [id=#257]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#59L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#61, avg(ss_net_paid), avg(ss_net_paid)#63) AS mergedValue#449]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#143), avg(ss_net_paid#149)], output=[count(1)#59L, avg(ss_ext_discount_amt)#61, avg(ss_net_paid)#63])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=90]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#143), partial_avg(ss_net_paid#149)], output=[count#458L, sum#459, count#460L, sum#461, count#462L])
   :  :                 +- Project [ss_ext_discount_amt#143, ss_net_paid#149]
   :  :                    +- Filter ((isnotnull(ss_quantity#139) AND (ss_quantity#139 >= 21)) AND (ss_quantity#139 <= 40))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#139,ss_ext_discount_amt#143,ss_net_paid#149] Batched: true, DataFilters: [isnotnull(ss_quantity#139), (ss_quantity#139 >= 21), (ss_quantity#139 <= 40)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,21), LessThanOrEqual(ss_quantity,40)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#6, [id=#258]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#59L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#61, avg(ss_net_paid), avg(ss_net_paid)#63) AS mergedValue#449]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#143), avg(ss_net_paid#149)], output=[count(1)#59L, avg(ss_ext_discount_amt)#61, avg(ss_net_paid)#63])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=106]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#143), partial_avg(ss_net_paid#149)], output=[count#458L, sum#459, count#460L, sum#461, count#462L])
   :  :                 +- Project [ss_ext_discount_amt#143, ss_net_paid#149]
   :  :                    +- Filter ((isnotnull(ss_quantity#139) AND (ss_quantity#139 >= 21)) AND (ss_quantity#139 <= 40))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#139,ss_ext_discount_amt#143,ss_net_paid#149] Batched: true, DataFilters: [isnotnull(ss_quantity#139), (ss_quantity#139 >= 21), (ss_quantity#139 <= 40)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,21), LessThanOrEqual(ss_quantity,40)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#8, [id=#259]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#65L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#67, avg(ss_net_paid), avg(ss_net_paid)#69) AS mergedValue#450]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#212), avg(ss_net_paid#218)], output=[count(1)#65L, avg(ss_ext_discount_amt)#67, avg(ss_net_paid)#69])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=122]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#212), partial_avg(ss_net_paid#218)], output=[count#463L, sum#464, count#465L, sum#466, count#467L])
   :  :                 +- Project [ss_ext_discount_amt#212, ss_net_paid#218]
   :  :                    +- Filter ((isnotnull(ss_quantity#208) AND (ss_quantity#208 >= 41)) AND (ss_quantity#208 <= 60))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#208,ss_ext_discount_amt#212,ss_net_paid#218] Batched: true, DataFilters: [isnotnull(ss_quantity#208), (ss_quantity#208 >= 41), (ss_quantity#208 <= 60)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,41), LessThanOrEqual(ss_quantity,60)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#9, [id=#260]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#65L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#67, avg(ss_net_paid), avg(ss_net_paid)#69) AS mergedValue#450]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#212), avg(ss_net_paid#218)], output=[count(1)#65L, avg(ss_ext_discount_amt)#67, avg(ss_net_paid)#69])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=138]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#212), partial_avg(ss_net_paid#218)], output=[count#463L, sum#464, count#465L, sum#466, count#467L])
   :  :                 +- Project [ss_ext_discount_amt#212, ss_net_paid#218]
   :  :                    +- Filter ((isnotnull(ss_quantity#208) AND (ss_quantity#208 >= 41)) AND (ss_quantity#208 <= 60))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#208,ss_ext_discount_amt#212,ss_net_paid#218] Batched: true, DataFilters: [isnotnull(ss_quantity#208), (ss_quantity#208 >= 41), (ss_quantity#208 <= 60)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,41), LessThanOrEqual(ss_quantity,60)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#10, [id=#261]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#65L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#67, avg(ss_net_paid), avg(ss_net_paid)#69) AS mergedValue#450]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#212), avg(ss_net_paid#218)], output=[count(1)#65L, avg(ss_ext_discount_amt)#67, avg(ss_net_paid)#69])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=154]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#212), partial_avg(ss_net_paid#218)], output=[count#463L, sum#464, count#465L, sum#466, count#467L])
   :  :                 +- Project [ss_ext_discount_amt#212, ss_net_paid#218]
   :  :                    +- Filter ((isnotnull(ss_quantity#208) AND (ss_quantity#208 >= 41)) AND (ss_quantity#208 <= 60))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#208,ss_ext_discount_amt#212,ss_net_paid#218] Batched: true, DataFilters: [isnotnull(ss_quantity#208), (ss_quantity#208 >= 41), (ss_quantity#208 <= 60)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,41), LessThanOrEqual(ss_quantity,60)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#12, [id=#262]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#71L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#73, avg(ss_net_paid), avg(ss_net_paid)#75) AS mergedValue#451]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#281), avg(ss_net_paid#287)], output=[count(1)#71L, avg(ss_ext_discount_amt)#73, avg(ss_net_paid)#75])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=170]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#281), partial_avg(ss_net_paid#287)], output=[count#468L, sum#469, count#470L, sum#471, count#472L])
   :  :                 +- Project [ss_ext_discount_amt#281, ss_net_paid#287]
   :  :                    +- Filter ((isnotnull(ss_quantity#277) AND (ss_quantity#277 >= 61)) AND (ss_quantity#277 <= 80))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#277,ss_ext_discount_amt#281,ss_net_paid#287] Batched: true, DataFilters: [isnotnull(ss_quantity#277), (ss_quantity#277 >= 61), (ss_quantity#277 <= 80)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,61), LessThanOrEqual(ss_quantity,80)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#13, [id=#263]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#71L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#73, avg(ss_net_paid), avg(ss_net_paid)#75) AS mergedValue#451]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#281), avg(ss_net_paid#287)], output=[count(1)#71L, avg(ss_ext_discount_amt)#73, avg(ss_net_paid)#75])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=186]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#281), partial_avg(ss_net_paid#287)], output=[count#468L, sum#469, count#470L, sum#471, count#472L])
   :  :                 +- Project [ss_ext_discount_amt#281, ss_net_paid#287]
   :  :                    +- Filter ((isnotnull(ss_quantity#277) AND (ss_quantity#277 >= 61)) AND (ss_quantity#277 <= 80))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#277,ss_ext_discount_amt#281,ss_net_paid#287] Batched: true, DataFilters: [isnotnull(ss_quantity#277), (ss_quantity#277 >= 61), (ss_quantity#277 <= 80)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,61), LessThanOrEqual(ss_quantity,80)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#14, [id=#264]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#71L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#73, avg(ss_net_paid), avg(ss_net_paid)#75) AS mergedValue#451]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#281), avg(ss_net_paid#287)], output=[count(1)#71L, avg(ss_ext_discount_amt)#73, avg(ss_net_paid)#75])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=202]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#281), partial_avg(ss_net_paid#287)], output=[count#468L, sum#469, count#470L, sum#471, count#472L])
   :  :                 +- Project [ss_ext_discount_amt#281, ss_net_paid#287]
   :  :                    +- Filter ((isnotnull(ss_quantity#277) AND (ss_quantity#277 >= 61)) AND (ss_quantity#277 <= 80))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#277,ss_ext_discount_amt#281,ss_net_paid#287] Batched: true, DataFilters: [isnotnull(ss_quantity#277), (ss_quantity#277 >= 61), (ss_quantity#277 <= 80)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,61), LessThanOrEqual(ss_quantity,80)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#16, [id=#265]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#77L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#79, avg(ss_net_paid), avg(ss_net_paid)#81) AS mergedValue#452]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#350), avg(ss_net_paid#356)], output=[count(1)#77L, avg(ss_ext_discount_amt)#79, avg(ss_net_paid)#81])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=218]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#350), partial_avg(ss_net_paid#356)], output=[count#475L, sum#476, count#477L, sum#478, count#479L])
   :  :                 +- Project [ss_ext_discount_amt#350, ss_net_paid#356]
   :  :                    +- Filter ((isnotnull(ss_quantity#346) AND (ss_quantity#346 >= 81)) AND (ss_quantity#346 <= 100))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#346,ss_ext_discount_amt#350,ss_net_paid#356] Batched: true, DataFilters: [isnotnull(ss_quantity#346), (ss_quantity#346 >= 81), (ss_quantity#346 <= 100)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,81), LessThanOrEqual(ss_quantity,100)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  :- Subquery subquery#17, [id=#266]
   :  :  +- AdaptiveSparkPlan isFinalPlan=false
   :  :     +- Project [named_struct(count(1), count(1)#77L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#79, avg(ss_net_paid), avg(ss_net_paid)#81) AS mergedValue#452]
   :  :        +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#350), avg(ss_net_paid#356)], output=[count(1)#77L, avg(ss_ext_discount_amt)#79, avg(ss_net_paid)#81])
   :  :           +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=234]
   :  :              +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#350), partial_avg(ss_net_paid#356)], output=[count#475L, sum#476, count#477L, sum#478, count#479L])
   :  :                 +- Project [ss_ext_discount_amt#350, ss_net_paid#356]
   :  :                    +- Filter ((isnotnull(ss_quantity#346) AND (ss_quantity#346 >= 81)) AND (ss_quantity#346 <= 100))
   :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#346,ss_ext_discount_amt#350,ss_net_paid#356] Batched: true, DataFilters: [isnotnull(ss_quantity#346), (ss_quantity#346 >= 81), (ss_quantity#346 <= 100)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,81), LessThanOrEqual(ss_quantity,100)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   :  +- Subquery subquery#18, [id=#267]
   :     +- AdaptiveSparkPlan isFinalPlan=false
   :        +- Project [named_struct(count(1), count(1)#77L, avg(ss_ext_discount_amt), avg(ss_ext_discount_amt)#79, avg(ss_net_paid), avg(ss_net_paid)#81) AS mergedValue#452]
   :           +- HashAggregate(keys=[], functions=[count(1), avg(ss_ext_discount_amt#350), avg(ss_net_paid#356)], output=[count(1)#77L, avg(ss_ext_discount_amt)#79, avg(ss_net_paid)#81])
   :              +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=250]
   :                 +- HashAggregate(keys=[], functions=[partial_count(1), partial_avg(ss_ext_discount_amt#350), partial_avg(ss_net_paid#356)], output=[count#475L, sum#476, count#477L, sum#478, count#479L])
   :                    +- Project [ss_ext_discount_amt#350, ss_net_paid#356]
   :                       +- Filter ((isnotnull(ss_quantity#346) AND (ss_quantity#346 >= 81)) AND (ss_quantity#346 <= 100))
   :                          +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#346,ss_ext_discount_amt#350,ss_net_paid#356] Batched: true, DataFilters: [isnotnull(ss_quantity#346), (ss_quantity#346 >= 81), (ss_quantity#346 <= 100)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,81), LessThanOrEqual(ss_quantity,100)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double,ss_net_paid:double>
   +- Filter (isnotnull(r_reason_sk#25) AND (r_reason_sk#25 = 1))
      +- FileScan parquet spark_catalog.tpcds.reason[r_reason_sk#25] Batched: true, DataFilters: [isnotnull(r_reason_sk#25), (r_reason_sk#25 = 1)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(r_reason_sk), EqualTo(r_reason_sk,1)], ReadSchema: struct<r_reason_sk:int>

Time taken: 3.222 seconds, Fetched 1 row(s)
