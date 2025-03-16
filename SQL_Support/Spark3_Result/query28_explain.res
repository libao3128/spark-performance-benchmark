Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580617683
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Project [*]
      +- 'Join Inner
         :- 'Join Inner
         :  :- 'Join Inner
         :  :  :- 'Join Inner
         :  :  :  :- 'Join Inner
         :  :  :  :  :- 'SubqueryAlias B1
         :  :  :  :  :  +- 'Project ['avg('ss_list_price) AS B1_LP#0, 'count('ss_list_price) AS B1_CNT#1, 'count(distinct 'ss_list_price) AS B1_CNTD#2]
         :  :  :  :  :     +- 'Filter ((('ss_quantity >= 0) AND ('ss_quantity <= 5)) AND (((('ss_list_price >= 8) AND ('ss_list_price <= (8 + 10))) OR (('ss_coupon_amt >= 459) AND ('ss_coupon_amt <= (459 + 1000)))) OR (('ss_wholesale_cost >= 57) AND ('ss_wholesale_cost <= (57 + 20)))))
         :  :  :  :  :        +- 'UnresolvedRelation [store_sales], [], false
         :  :  :  :  +- 'SubqueryAlias B2
         :  :  :  :     +- 'Project ['avg('ss_list_price) AS B2_LP#3, 'count('ss_list_price) AS B2_CNT#4, 'count(distinct 'ss_list_price) AS B2_CNTD#5]
         :  :  :  :        +- 'Filter ((('ss_quantity >= 6) AND ('ss_quantity <= 10)) AND (((('ss_list_price >= 90) AND ('ss_list_price <= (90 + 10))) OR (('ss_coupon_amt >= 2323) AND ('ss_coupon_amt <= (2323 + 1000)))) OR (('ss_wholesale_cost >= 31) AND ('ss_wholesale_cost <= (31 + 20)))))
         :  :  :  :           +- 'UnresolvedRelation [store_sales], [], false
         :  :  :  +- 'SubqueryAlias B3
         :  :  :     +- 'Project ['avg('ss_list_price) AS B3_LP#6, 'count('ss_list_price) AS B3_CNT#7, 'count(distinct 'ss_list_price) AS B3_CNTD#8]
         :  :  :        +- 'Filter ((('ss_quantity >= 11) AND ('ss_quantity <= 15)) AND (((('ss_list_price >= 142) AND ('ss_list_price <= (142 + 10))) OR (('ss_coupon_amt >= 12214) AND ('ss_coupon_amt <= (12214 + 1000)))) OR (('ss_wholesale_cost >= 79) AND ('ss_wholesale_cost <= (79 + 20)))))
         :  :  :           +- 'UnresolvedRelation [store_sales], [], false
         :  :  +- 'SubqueryAlias B4
         :  :     +- 'Project ['avg('ss_list_price) AS B4_LP#9, 'count('ss_list_price) AS B4_CNT#10, 'count(distinct 'ss_list_price) AS B4_CNTD#11]
         :  :        +- 'Filter ((('ss_quantity >= 16) AND ('ss_quantity <= 20)) AND (((('ss_list_price >= 135) AND ('ss_list_price <= (135 + 10))) OR (('ss_coupon_amt >= 6071) AND ('ss_coupon_amt <= (6071 + 1000)))) OR (('ss_wholesale_cost >= 38) AND ('ss_wholesale_cost <= (38 + 20)))))
         :  :           +- 'UnresolvedRelation [store_sales], [], false
         :  +- 'SubqueryAlias B5
         :     +- 'Project ['avg('ss_list_price) AS B5_LP#12, 'count('ss_list_price) AS B5_CNT#13, 'count(distinct 'ss_list_price) AS B5_CNTD#14]
         :        +- 'Filter ((('ss_quantity >= 21) AND ('ss_quantity <= 25)) AND (((('ss_list_price >= 122) AND ('ss_list_price <= (122 + 10))) OR (('ss_coupon_amt >= 836) AND ('ss_coupon_amt <= (836 + 1000)))) OR (('ss_wholesale_cost >= 17) AND ('ss_wholesale_cost <= (17 + 20)))))
         :           +- 'UnresolvedRelation [store_sales], [], false
         +- 'SubqueryAlias B6
            +- 'Project ['avg('ss_list_price) AS B6_LP#15, 'count('ss_list_price) AS B6_CNT#16, 'count(distinct 'ss_list_price) AS B6_CNTD#17]
               +- 'Filter ((('ss_quantity >= 26) AND ('ss_quantity <= 30)) AND (((('ss_list_price >= 154) AND ('ss_list_price <= (154 + 10))) OR (('ss_coupon_amt >= 7326) AND ('ss_coupon_amt <= (7326 + 1000)))) OR (('ss_wholesale_cost >= 7) AND ('ss_wholesale_cost <= (7 + 20)))))
                  +- 'UnresolvedRelation [store_sales], [], false

== Analyzed Logical Plan ==
B1_LP: double, B1_CNT: bigint, B1_CNTD: bigint, B2_LP: double, B2_CNT: bigint, B2_CNTD: bigint, B3_LP: double, B3_CNT: bigint, B3_CNTD: bigint, B4_LP: double, B4_CNT: bigint, B4_CNTD: bigint, B5_LP: double, B5_CNT: bigint, B5_CNTD: bigint, B6_LP: double, B6_CNT: bigint, B6_CNTD: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Project [B1_LP#0, B1_CNT#1L, B1_CNTD#2L, B2_LP#3, B2_CNT#4L, B2_CNTD#5L, B3_LP#6, B3_CNT#7L, B3_CNTD#8L, B4_LP#9, B4_CNT#10L, B4_CNTD#11L, B5_LP#12, B5_CNT#13L, B5_CNTD#14L, B6_LP#15, B6_CNT#16L, B6_CNTD#17L]
      +- Join Inner
         :- Join Inner
         :  :- Join Inner
         :  :  :- Join Inner
         :  :  :  :- Join Inner
         :  :  :  :  :- SubqueryAlias B1
         :  :  :  :  :  +- Aggregate [avg(ss_list_price#35) AS B1_LP#0, count(ss_list_price#35) AS B1_CNT#1L, count(distinct ss_list_price#35) AS B1_CNTD#2L]
         :  :  :  :  :     +- Filter (((ss_quantity#33 >= 0) AND (ss_quantity#33 <= 5)) AND ((((ss_list_price#35 >= cast(8 as double)) AND (ss_list_price#35 <= cast((8 + 10) as double))) OR ((ss_coupon_amt#42 >= cast(459 as double)) AND (ss_coupon_amt#42 <= cast((459 + 1000) as double)))) OR ((ss_wholesale_cost#34 >= cast(57 as double)) AND (ss_wholesale_cost#34 <= cast((57 + 20) as double)))))
         :  :  :  :  :        +- SubqueryAlias spark_catalog.tpcds.store_sales
         :  :  :  :  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#23,ss_sold_time_sk#24,ss_item_sk#25,ss_customer_sk#26,ss_cdemo_sk#27,ss_hdemo_sk#28,ss_addr_sk#29,ss_store_sk#30,ss_promo_sk#31,ss_ticket_number#32,ss_quantity#33,ss_wholesale_cost#34,ss_list_price#35,ss_sales_price#36,ss_ext_discount_amt#37,ss_ext_sales_price#38,ss_ext_wholesale_cost#39,ss_ext_list_price#40,ss_ext_tax#41,ss_coupon_amt#42,ss_net_paid#43,ss_net_paid_inc_tax#44,ss_net_profit#45] parquet
         :  :  :  :  +- SubqueryAlias B2
         :  :  :  :     +- Aggregate [avg(ss_list_price#58) AS B2_LP#3, count(ss_list_price#58) AS B2_CNT#4L, count(distinct ss_list_price#58) AS B2_CNTD#5L]
         :  :  :  :        +- Filter (((ss_quantity#56 >= 6) AND (ss_quantity#56 <= 10)) AND ((((ss_list_price#58 >= cast(90 as double)) AND (ss_list_price#58 <= cast((90 + 10) as double))) OR ((ss_coupon_amt#65 >= cast(2323 as double)) AND (ss_coupon_amt#65 <= cast((2323 + 1000) as double)))) OR ((ss_wholesale_cost#57 >= cast(31 as double)) AND (ss_wholesale_cost#57 <= cast((31 + 20) as double)))))
         :  :  :  :           +- SubqueryAlias spark_catalog.tpcds.store_sales
         :  :  :  :              +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#46,ss_sold_time_sk#47,ss_item_sk#48,ss_customer_sk#49,ss_cdemo_sk#50,ss_hdemo_sk#51,ss_addr_sk#52,ss_store_sk#53,ss_promo_sk#54,ss_ticket_number#55,ss_quantity#56,ss_wholesale_cost#57,ss_list_price#58,ss_sales_price#59,ss_ext_discount_amt#60,ss_ext_sales_price#61,ss_ext_wholesale_cost#62,ss_ext_list_price#63,ss_ext_tax#64,ss_coupon_amt#65,ss_net_paid#66,ss_net_paid_inc_tax#67,ss_net_profit#68] parquet
         :  :  :  +- SubqueryAlias B3
         :  :  :     +- Aggregate [avg(ss_list_price#81) AS B3_LP#6, count(ss_list_price#81) AS B3_CNT#7L, count(distinct ss_list_price#81) AS B3_CNTD#8L]
         :  :  :        +- Filter (((ss_quantity#79 >= 11) AND (ss_quantity#79 <= 15)) AND ((((ss_list_price#81 >= cast(142 as double)) AND (ss_list_price#81 <= cast((142 + 10) as double))) OR ((ss_coupon_amt#88 >= cast(12214 as double)) AND (ss_coupon_amt#88 <= cast((12214 + 1000) as double)))) OR ((ss_wholesale_cost#80 >= cast(79 as double)) AND (ss_wholesale_cost#80 <= cast((79 + 20) as double)))))
         :  :  :           +- SubqueryAlias spark_catalog.tpcds.store_sales
         :  :  :              +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#69,ss_sold_time_sk#70,ss_item_sk#71,ss_customer_sk#72,ss_cdemo_sk#73,ss_hdemo_sk#74,ss_addr_sk#75,ss_store_sk#76,ss_promo_sk#77,ss_ticket_number#78,ss_quantity#79,ss_wholesale_cost#80,ss_list_price#81,ss_sales_price#82,ss_ext_discount_amt#83,ss_ext_sales_price#84,ss_ext_wholesale_cost#85,ss_ext_list_price#86,ss_ext_tax#87,ss_coupon_amt#88,ss_net_paid#89,ss_net_paid_inc_tax#90,ss_net_profit#91] parquet
         :  :  +- SubqueryAlias B4
         :  :     +- Aggregate [avg(ss_list_price#104) AS B4_LP#9, count(ss_list_price#104) AS B4_CNT#10L, count(distinct ss_list_price#104) AS B4_CNTD#11L]
         :  :        +- Filter (((ss_quantity#102 >= 16) AND (ss_quantity#102 <= 20)) AND ((((ss_list_price#104 >= cast(135 as double)) AND (ss_list_price#104 <= cast((135 + 10) as double))) OR ((ss_coupon_amt#111 >= cast(6071 as double)) AND (ss_coupon_amt#111 <= cast((6071 + 1000) as double)))) OR ((ss_wholesale_cost#103 >= cast(38 as double)) AND (ss_wholesale_cost#103 <= cast((38 + 20) as double)))))
         :  :           +- SubqueryAlias spark_catalog.tpcds.store_sales
         :  :              +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#92,ss_sold_time_sk#93,ss_item_sk#94,ss_customer_sk#95,ss_cdemo_sk#96,ss_hdemo_sk#97,ss_addr_sk#98,ss_store_sk#99,ss_promo_sk#100,ss_ticket_number#101,ss_quantity#102,ss_wholesale_cost#103,ss_list_price#104,ss_sales_price#105,ss_ext_discount_amt#106,ss_ext_sales_price#107,ss_ext_wholesale_cost#108,ss_ext_list_price#109,ss_ext_tax#110,ss_coupon_amt#111,ss_net_paid#112,ss_net_paid_inc_tax#113,ss_net_profit#114] parquet
         :  +- SubqueryAlias B5
         :     +- Aggregate [avg(ss_list_price#127) AS B5_LP#12, count(ss_list_price#127) AS B5_CNT#13L, count(distinct ss_list_price#127) AS B5_CNTD#14L]
         :        +- Filter (((ss_quantity#125 >= 21) AND (ss_quantity#125 <= 25)) AND ((((ss_list_price#127 >= cast(122 as double)) AND (ss_list_price#127 <= cast((122 + 10) as double))) OR ((ss_coupon_amt#134 >= cast(836 as double)) AND (ss_coupon_amt#134 <= cast((836 + 1000) as double)))) OR ((ss_wholesale_cost#126 >= cast(17 as double)) AND (ss_wholesale_cost#126 <= cast((17 + 20) as double)))))
         :           +- SubqueryAlias spark_catalog.tpcds.store_sales
         :              +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#115,ss_sold_time_sk#116,ss_item_sk#117,ss_customer_sk#118,ss_cdemo_sk#119,ss_hdemo_sk#120,ss_addr_sk#121,ss_store_sk#122,ss_promo_sk#123,ss_ticket_number#124,ss_quantity#125,ss_wholesale_cost#126,ss_list_price#127,ss_sales_price#128,ss_ext_discount_amt#129,ss_ext_sales_price#130,ss_ext_wholesale_cost#131,ss_ext_list_price#132,ss_ext_tax#133,ss_coupon_amt#134,ss_net_paid#135,ss_net_paid_inc_tax#136,ss_net_profit#137] parquet
         +- SubqueryAlias B6
            +- Aggregate [avg(ss_list_price#150) AS B6_LP#15, count(ss_list_price#150) AS B6_CNT#16L, count(distinct ss_list_price#150) AS B6_CNTD#17L]
               +- Filter (((ss_quantity#148 >= 26) AND (ss_quantity#148 <= 30)) AND ((((ss_list_price#150 >= cast(154 as double)) AND (ss_list_price#150 <= cast((154 + 10) as double))) OR ((ss_coupon_amt#157 >= cast(7326 as double)) AND (ss_coupon_amt#157 <= cast((7326 + 1000) as double)))) OR ((ss_wholesale_cost#149 >= cast(7 as double)) AND (ss_wholesale_cost#149 <= cast((7 + 20) as double)))))
                  +- SubqueryAlias spark_catalog.tpcds.store_sales
                     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#138,ss_sold_time_sk#139,ss_item_sk#140,ss_customer_sk#141,ss_cdemo_sk#142,ss_hdemo_sk#143,ss_addr_sk#144,ss_store_sk#145,ss_promo_sk#146,ss_ticket_number#147,ss_quantity#148,ss_wholesale_cost#149,ss_list_price#150,ss_sales_price#151,ss_ext_discount_amt#152,ss_ext_sales_price#153,ss_ext_wholesale_cost#154,ss_ext_list_price#155,ss_ext_tax#156,ss_coupon_amt#157,ss_net_paid#158,ss_net_paid_inc_tax#159,ss_net_profit#160] parquet

== Optimized Logical Plan ==
Join Inner
:- Join Inner
:  :- Join Inner
:  :  :- Join Inner
:  :  :  :- Join Inner
:  :  :  :  :- Aggregate [avg(ss_list_price#35) AS B1_LP#0, count(ss_list_price#35) AS B1_CNT#1L, count(distinct ss_list_price#35) AS B1_CNTD#2L]
:  :  :  :  :  +- Project [ss_list_price#35]
:  :  :  :  :     +- Filter (isnotnull(ss_quantity#33) AND (((ss_quantity#33 >= 0) AND (ss_quantity#33 <= 5)) AND ((((ss_list_price#35 >= 8.0) AND (ss_list_price#35 <= 18.0)) OR ((ss_coupon_amt#42 >= 459.0) AND (ss_coupon_amt#42 <= 1459.0))) OR ((ss_wholesale_cost#34 >= 57.0) AND (ss_wholesale_cost#34 <= 77.0)))))
:  :  :  :  :        +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#23,ss_sold_time_sk#24,ss_item_sk#25,ss_customer_sk#26,ss_cdemo_sk#27,ss_hdemo_sk#28,ss_addr_sk#29,ss_store_sk#30,ss_promo_sk#31,ss_ticket_number#32,ss_quantity#33,ss_wholesale_cost#34,ss_list_price#35,ss_sales_price#36,ss_ext_discount_amt#37,ss_ext_sales_price#38,ss_ext_wholesale_cost#39,ss_ext_list_price#40,ss_ext_tax#41,ss_coupon_amt#42,ss_net_paid#43,ss_net_paid_inc_tax#44,ss_net_profit#45] parquet
:  :  :  :  +- Aggregate [avg(ss_list_price#58) AS B2_LP#3, count(ss_list_price#58) AS B2_CNT#4L, count(distinct ss_list_price#58) AS B2_CNTD#5L]
:  :  :  :     +- Project [ss_list_price#58]
:  :  :  :        +- Filter (isnotnull(ss_quantity#56) AND (((ss_quantity#56 >= 6) AND (ss_quantity#56 <= 10)) AND ((((ss_list_price#58 >= 90.0) AND (ss_list_price#58 <= 100.0)) OR ((ss_coupon_amt#65 >= 2323.0) AND (ss_coupon_amt#65 <= 3323.0))) OR ((ss_wholesale_cost#57 >= 31.0) AND (ss_wholesale_cost#57 <= 51.0)))))
:  :  :  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#46,ss_sold_time_sk#47,ss_item_sk#48,ss_customer_sk#49,ss_cdemo_sk#50,ss_hdemo_sk#51,ss_addr_sk#52,ss_store_sk#53,ss_promo_sk#54,ss_ticket_number#55,ss_quantity#56,ss_wholesale_cost#57,ss_list_price#58,ss_sales_price#59,ss_ext_discount_amt#60,ss_ext_sales_price#61,ss_ext_wholesale_cost#62,ss_ext_list_price#63,ss_ext_tax#64,ss_coupon_amt#65,ss_net_paid#66,ss_net_paid_inc_tax#67,ss_net_profit#68] parquet
:  :  :  +- Aggregate [avg(ss_list_price#81) AS B3_LP#6, count(ss_list_price#81) AS B3_CNT#7L, count(distinct ss_list_price#81) AS B3_CNTD#8L]
:  :  :     +- Project [ss_list_price#81]
:  :  :        +- Filter (isnotnull(ss_quantity#79) AND (((ss_quantity#79 >= 11) AND (ss_quantity#79 <= 15)) AND ((((ss_list_price#81 >= 142.0) AND (ss_list_price#81 <= 152.0)) OR ((ss_coupon_amt#88 >= 12214.0) AND (ss_coupon_amt#88 <= 13214.0))) OR ((ss_wholesale_cost#80 >= 79.0) AND (ss_wholesale_cost#80 <= 99.0)))))
:  :  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#69,ss_sold_time_sk#70,ss_item_sk#71,ss_customer_sk#72,ss_cdemo_sk#73,ss_hdemo_sk#74,ss_addr_sk#75,ss_store_sk#76,ss_promo_sk#77,ss_ticket_number#78,ss_quantity#79,ss_wholesale_cost#80,ss_list_price#81,ss_sales_price#82,ss_ext_discount_amt#83,ss_ext_sales_price#84,ss_ext_wholesale_cost#85,ss_ext_list_price#86,ss_ext_tax#87,ss_coupon_amt#88,ss_net_paid#89,ss_net_paid_inc_tax#90,ss_net_profit#91] parquet
:  :  +- Aggregate [avg(ss_list_price#104) AS B4_LP#9, count(ss_list_price#104) AS B4_CNT#10L, count(distinct ss_list_price#104) AS B4_CNTD#11L]
:  :     +- Project [ss_list_price#104]
:  :        +- Filter (isnotnull(ss_quantity#102) AND (((ss_quantity#102 >= 16) AND (ss_quantity#102 <= 20)) AND ((((ss_list_price#104 >= 135.0) AND (ss_list_price#104 <= 145.0)) OR ((ss_coupon_amt#111 >= 6071.0) AND (ss_coupon_amt#111 <= 7071.0))) OR ((ss_wholesale_cost#103 >= 38.0) AND (ss_wholesale_cost#103 <= 58.0)))))
:  :           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#92,ss_sold_time_sk#93,ss_item_sk#94,ss_customer_sk#95,ss_cdemo_sk#96,ss_hdemo_sk#97,ss_addr_sk#98,ss_store_sk#99,ss_promo_sk#100,ss_ticket_number#101,ss_quantity#102,ss_wholesale_cost#103,ss_list_price#104,ss_sales_price#105,ss_ext_discount_amt#106,ss_ext_sales_price#107,ss_ext_wholesale_cost#108,ss_ext_list_price#109,ss_ext_tax#110,ss_coupon_amt#111,ss_net_paid#112,ss_net_paid_inc_tax#113,ss_net_profit#114] parquet
:  +- Aggregate [avg(ss_list_price#127) AS B5_LP#12, count(ss_list_price#127) AS B5_CNT#13L, count(distinct ss_list_price#127) AS B5_CNTD#14L]
:     +- Project [ss_list_price#127]
:        +- Filter (isnotnull(ss_quantity#125) AND (((ss_quantity#125 >= 21) AND (ss_quantity#125 <= 25)) AND ((((ss_list_price#127 >= 122.0) AND (ss_list_price#127 <= 132.0)) OR ((ss_coupon_amt#134 >= 836.0) AND (ss_coupon_amt#134 <= 1836.0))) OR ((ss_wholesale_cost#126 >= 17.0) AND (ss_wholesale_cost#126 <= 37.0)))))
:           +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#115,ss_sold_time_sk#116,ss_item_sk#117,ss_customer_sk#118,ss_cdemo_sk#119,ss_hdemo_sk#120,ss_addr_sk#121,ss_store_sk#122,ss_promo_sk#123,ss_ticket_number#124,ss_quantity#125,ss_wholesale_cost#126,ss_list_price#127,ss_sales_price#128,ss_ext_discount_amt#129,ss_ext_sales_price#130,ss_ext_wholesale_cost#131,ss_ext_list_price#132,ss_ext_tax#133,ss_coupon_amt#134,ss_net_paid#135,ss_net_paid_inc_tax#136,ss_net_profit#137] parquet
+- Aggregate [avg(ss_list_price#150) AS B6_LP#15, count(ss_list_price#150) AS B6_CNT#16L, count(distinct ss_list_price#150) AS B6_CNTD#17L]
   +- Project [ss_list_price#150]
      +- Filter (isnotnull(ss_quantity#148) AND (((ss_quantity#148 >= 26) AND (ss_quantity#148 <= 30)) AND ((((ss_list_price#150 >= 154.0) AND (ss_list_price#150 <= 164.0)) OR ((ss_coupon_amt#157 >= 7326.0) AND (ss_coupon_amt#157 <= 8326.0))) OR ((ss_wholesale_cost#149 >= 7.0) AND (ss_wholesale_cost#149 <= 27.0)))))
         +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#138,ss_sold_time_sk#139,ss_item_sk#140,ss_customer_sk#141,ss_cdemo_sk#142,ss_hdemo_sk#143,ss_addr_sk#144,ss_store_sk#145,ss_promo_sk#146,ss_ticket_number#147,ss_quantity#148,ss_wholesale_cost#149,ss_list_price#150,ss_sales_price#151,ss_ext_discount_amt#152,ss_ext_sales_price#153,ss_ext_wholesale_cost#154,ss_ext_list_price#155,ss_ext_tax#156,ss_coupon_amt#157,ss_net_paid#158,ss_net_paid_inc_tax#159,ss_net_profit#160] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- BroadcastNestedLoopJoin BuildRight, Inner
   :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :  :  :- HashAggregate(keys=[], functions=[avg(ss_list_price#35), count(ss_list_price#35), count(distinct ss_list_price#185)], output=[B1_LP#0, B1_CNT#1L, B1_CNTD#2L])
   :  :  :  :  :  +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=156]
   :  :  :  :  :     +- HashAggregate(keys=[], functions=[merge_avg(ss_list_price#35), merge_count(ss_list_price#35), partial_count(distinct ss_list_price#185)], output=[sum#188, count#189L, count#191L, count#194L])
   :  :  :  :  :        +- HashAggregate(keys=[ss_list_price#185], functions=[merge_avg(ss_list_price#35), merge_count(ss_list_price#35)], output=[ss_list_price#185, sum#188, count#189L, count#191L])
   :  :  :  :  :           +- Exchange hashpartitioning(ss_list_price#185, 200), ENSURE_REQUIREMENTS, [plan_id=152]
   :  :  :  :  :              +- HashAggregate(keys=[knownfloatingpointnormalized(normalizenanandzero(ss_list_price#35)) AS ss_list_price#185], functions=[partial_avg(ss_list_price#35), partial_count(ss_list_price#35)], output=[ss_list_price#185, sum#188, count#189L, count#191L])
   :  :  :  :  :                 +- Project [ss_list_price#35]
   :  :  :  :  :                    +- Filter (((isnotnull(ss_quantity#33) AND (ss_quantity#33 >= 0)) AND (ss_quantity#33 <= 5)) AND ((((ss_list_price#35 >= 8.0) AND (ss_list_price#35 <= 18.0)) OR ((ss_coupon_amt#42 >= 459.0) AND (ss_coupon_amt#42 <= 1459.0))) OR ((ss_wholesale_cost#34 >= 57.0) AND (ss_wholesale_cost#34 <= 77.0))))
   :  :  :  :  :                       +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#33,ss_wholesale_cost#34,ss_list_price#35,ss_coupon_amt#42] Batched: true, DataFilters: [isnotnull(ss_quantity#33), (ss_quantity#33 >= 0), (ss_quantity#33 <= 5), ((((ss_list_price#35 >=..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,0), LessThanOrEqual(ss_quantity,5), Or(Or..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   :  :  :  :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=165]
   :  :  :  :     +- HashAggregate(keys=[], functions=[avg(ss_list_price#58), count(ss_list_price#58), count(distinct ss_list_price#196)], output=[B2_LP#3, B2_CNT#4L, B2_CNTD#5L])
   :  :  :  :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=162]
   :  :  :  :           +- HashAggregate(keys=[], functions=[merge_avg(ss_list_price#58), merge_count(ss_list_price#58), partial_count(distinct ss_list_price#196)], output=[sum#199, count#200L, count#202L, count#205L])
   :  :  :  :              +- HashAggregate(keys=[ss_list_price#196], functions=[merge_avg(ss_list_price#58), merge_count(ss_list_price#58)], output=[ss_list_price#196, sum#199, count#200L, count#202L])
   :  :  :  :                 +- Exchange hashpartitioning(ss_list_price#196, 200), ENSURE_REQUIREMENTS, [plan_id=158]
   :  :  :  :                    +- HashAggregate(keys=[knownfloatingpointnormalized(normalizenanandzero(ss_list_price#58)) AS ss_list_price#196], functions=[partial_avg(ss_list_price#58), partial_count(ss_list_price#58)], output=[ss_list_price#196, sum#199, count#200L, count#202L])
   :  :  :  :                       +- Project [ss_list_price#58]
   :  :  :  :                          +- Filter (((isnotnull(ss_quantity#56) AND (ss_quantity#56 >= 6)) AND (ss_quantity#56 <= 10)) AND ((((ss_list_price#58 >= 90.0) AND (ss_list_price#58 <= 100.0)) OR ((ss_coupon_amt#65 >= 2323.0) AND (ss_coupon_amt#65 <= 3323.0))) OR ((ss_wholesale_cost#57 >= 31.0) AND (ss_wholesale_cost#57 <= 51.0))))
   :  :  :  :                             +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#56,ss_wholesale_cost#57,ss_list_price#58,ss_coupon_amt#65] Batched: true, DataFilters: [isnotnull(ss_quantity#56), (ss_quantity#56 >= 6), (ss_quantity#56 <= 10), ((((ss_list_price#58 >..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,6), LessThanOrEqual(ss_quantity,10), Or(O..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   :  :  :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=174]
   :  :  :     +- HashAggregate(keys=[], functions=[avg(ss_list_price#81), count(ss_list_price#81), count(distinct ss_list_price#207)], output=[B3_LP#6, B3_CNT#7L, B3_CNTD#8L])
   :  :  :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=171]
   :  :  :           +- HashAggregate(keys=[], functions=[merge_avg(ss_list_price#81), merge_count(ss_list_price#81), partial_count(distinct ss_list_price#207)], output=[sum#210, count#211L, count#213L, count#216L])
   :  :  :              +- HashAggregate(keys=[ss_list_price#207], functions=[merge_avg(ss_list_price#81), merge_count(ss_list_price#81)], output=[ss_list_price#207, sum#210, count#211L, count#213L])
   :  :  :                 +- Exchange hashpartitioning(ss_list_price#207, 200), ENSURE_REQUIREMENTS, [plan_id=167]
   :  :  :                    +- HashAggregate(keys=[knownfloatingpointnormalized(normalizenanandzero(ss_list_price#81)) AS ss_list_price#207], functions=[partial_avg(ss_list_price#81), partial_count(ss_list_price#81)], output=[ss_list_price#207, sum#210, count#211L, count#213L])
   :  :  :                       +- Project [ss_list_price#81]
   :  :  :                          +- Filter (((isnotnull(ss_quantity#79) AND (ss_quantity#79 >= 11)) AND (ss_quantity#79 <= 15)) AND ((((ss_list_price#81 >= 142.0) AND (ss_list_price#81 <= 152.0)) OR ((ss_coupon_amt#88 >= 12214.0) AND (ss_coupon_amt#88 <= 13214.0))) OR ((ss_wholesale_cost#80 >= 79.0) AND (ss_wholesale_cost#80 <= 99.0))))
   :  :  :                             +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#79,ss_wholesale_cost#80,ss_list_price#81,ss_coupon_amt#88] Batched: true, DataFilters: [isnotnull(ss_quantity#79), (ss_quantity#79 >= 11), (ss_quantity#79 <= 15), ((((ss_list_price#81 ..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,11), LessThanOrEqual(ss_quantity,15), Or(..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   :  :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=183]
   :  :     +- HashAggregate(keys=[], functions=[avg(ss_list_price#104), count(ss_list_price#104), count(distinct ss_list_price#218)], output=[B4_LP#9, B4_CNT#10L, B4_CNTD#11L])
   :  :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=180]
   :  :           +- HashAggregate(keys=[], functions=[merge_avg(ss_list_price#104), merge_count(ss_list_price#104), partial_count(distinct ss_list_price#218)], output=[sum#221, count#222L, count#224L, count#227L])
   :  :              +- HashAggregate(keys=[ss_list_price#218], functions=[merge_avg(ss_list_price#104), merge_count(ss_list_price#104)], output=[ss_list_price#218, sum#221, count#222L, count#224L])
   :  :                 +- Exchange hashpartitioning(ss_list_price#218, 200), ENSURE_REQUIREMENTS, [plan_id=176]
   :  :                    +- HashAggregate(keys=[knownfloatingpointnormalized(normalizenanandzero(ss_list_price#104)) AS ss_list_price#218], functions=[partial_avg(ss_list_price#104), partial_count(ss_list_price#104)], output=[ss_list_price#218, sum#221, count#222L, count#224L])
   :  :                       +- Project [ss_list_price#104]
   :  :                          +- Filter (((isnotnull(ss_quantity#102) AND (ss_quantity#102 >= 16)) AND (ss_quantity#102 <= 20)) AND ((((ss_list_price#104 >= 135.0) AND (ss_list_price#104 <= 145.0)) OR ((ss_coupon_amt#111 >= 6071.0) AND (ss_coupon_amt#111 <= 7071.0))) OR ((ss_wholesale_cost#103 >= 38.0) AND (ss_wholesale_cost#103 <= 58.0))))
   :  :                             +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#102,ss_wholesale_cost#103,ss_list_price#104,ss_coupon_amt#111] Batched: true, DataFilters: [isnotnull(ss_quantity#102), (ss_quantity#102 >= 16), (ss_quantity#102 <= 20), ((((ss_list_price#..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,16), LessThanOrEqual(ss_quantity,20), Or(..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   :  +- BroadcastExchange IdentityBroadcastMode, [plan_id=192]
   :     +- HashAggregate(keys=[], functions=[avg(ss_list_price#127), count(ss_list_price#127), count(distinct ss_list_price#229)], output=[B5_LP#12, B5_CNT#13L, B5_CNTD#14L])
   :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=189]
   :           +- HashAggregate(keys=[], functions=[merge_avg(ss_list_price#127), merge_count(ss_list_price#127), partial_count(distinct ss_list_price#229)], output=[sum#232, count#233L, count#235L, count#238L])
   :              +- HashAggregate(keys=[ss_list_price#229], functions=[merge_avg(ss_list_price#127), merge_count(ss_list_price#127)], output=[ss_list_price#229, sum#232, count#233L, count#235L])
   :                 +- Exchange hashpartitioning(ss_list_price#229, 200), ENSURE_REQUIREMENTS, [plan_id=185]
   :                    +- HashAggregate(keys=[knownfloatingpointnormalized(normalizenanandzero(ss_list_price#127)) AS ss_list_price#229], functions=[partial_avg(ss_list_price#127), partial_count(ss_list_price#127)], output=[ss_list_price#229, sum#232, count#233L, count#235L])
   :                       +- Project [ss_list_price#127]
   :                          +- Filter (((isnotnull(ss_quantity#125) AND (ss_quantity#125 >= 21)) AND (ss_quantity#125 <= 25)) AND ((((ss_list_price#127 >= 122.0) AND (ss_list_price#127 <= 132.0)) OR ((ss_coupon_amt#134 >= 836.0) AND (ss_coupon_amt#134 <= 1836.0))) OR ((ss_wholesale_cost#126 >= 17.0) AND (ss_wholesale_cost#126 <= 37.0))))
   :                             +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#125,ss_wholesale_cost#126,ss_list_price#127,ss_coupon_amt#134] Batched: true, DataFilters: [isnotnull(ss_quantity#125), (ss_quantity#125 >= 21), (ss_quantity#125 <= 25), ((((ss_list_price#..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,21), LessThanOrEqual(ss_quantity,25), Or(..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   +- BroadcastExchange IdentityBroadcastMode, [plan_id=201]
      +- HashAggregate(keys=[], functions=[avg(ss_list_price#150), count(ss_list_price#150), count(distinct ss_list_price#240)], output=[B6_LP#15, B6_CNT#16L, B6_CNTD#17L])
         +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=198]
            +- HashAggregate(keys=[], functions=[merge_avg(ss_list_price#150), merge_count(ss_list_price#150), partial_count(distinct ss_list_price#240)], output=[sum#243, count#244L, count#246L, count#249L])
               +- HashAggregate(keys=[ss_list_price#240], functions=[merge_avg(ss_list_price#150), merge_count(ss_list_price#150)], output=[ss_list_price#240, sum#243, count#244L, count#246L])
                  +- Exchange hashpartitioning(ss_list_price#240, 200), ENSURE_REQUIREMENTS, [plan_id=194]
                     +- HashAggregate(keys=[knownfloatingpointnormalized(normalizenanandzero(ss_list_price#150)) AS ss_list_price#240], functions=[partial_avg(ss_list_price#150), partial_count(ss_list_price#150)], output=[ss_list_price#240, sum#243, count#244L, count#246L])
                        +- Project [ss_list_price#150]
                           +- Filter (((isnotnull(ss_quantity#148) AND (ss_quantity#148 >= 26)) AND (ss_quantity#148 <= 30)) AND ((((ss_list_price#150 >= 154.0) AND (ss_list_price#150 <= 164.0)) OR ((ss_coupon_amt#157 >= 7326.0) AND (ss_coupon_amt#157 <= 8326.0))) OR ((ss_wholesale_cost#149 >= 7.0) AND (ss_wholesale_cost#149 <= 27.0))))
                              +- FileScan parquet spark_catalog.tpcds.store_sales[ss_quantity#148,ss_wholesale_cost#149,ss_list_price#150,ss_coupon_amt#157] Batched: true, DataFilters: [isnotnull(ss_quantity#148), (ss_quantity#148 >= 26), (ss_quantity#148 <= 30), ((((ss_list_price#..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,26), LessThanOrEqual(ss_quantity,30), Or(..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>

Time taken: 2.345 seconds, Fetched 1 row(s)
