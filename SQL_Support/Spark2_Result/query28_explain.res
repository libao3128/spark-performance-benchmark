== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Project [*]
      +- 'Join Inner
         :- 'Join Inner
         :  :- 'Join Inner
         :  :  :- 'Join Inner
         :  :  :  :- 'Join Inner
         :  :  :  :  :- 'SubqueryAlias `B1`
         :  :  :  :  :  +- 'Project ['avg('ss_list_price) AS B1_LP#0, 'count('ss_list_price) AS B1_CNT#1, 'count('ss_list_price) AS B1_CNTD#2]
         :  :  :  :  :     +- 'Filter ((('ss_quantity >= 0) && ('ss_quantity <= 5)) && (((('ss_list_price >= 8) && ('ss_list_price <= (8 + 10))) || (('ss_coupon_amt >= 459) && ('ss_coupon_amt <= (459 + 1000)))) || (('ss_wholesale_cost >= 57) && ('ss_wholesale_cost <= (57 + 20)))))
         :  :  :  :  :        +- 'UnresolvedRelation `store_sales`
         :  :  :  :  +- 'SubqueryAlias `B2`
         :  :  :  :     +- 'Project ['avg('ss_list_price) AS B2_LP#3, 'count('ss_list_price) AS B2_CNT#4, 'count('ss_list_price) AS B2_CNTD#5]
         :  :  :  :        +- 'Filter ((('ss_quantity >= 6) && ('ss_quantity <= 10)) && (((('ss_list_price >= 90) && ('ss_list_price <= (90 + 10))) || (('ss_coupon_amt >= 2323) && ('ss_coupon_amt <= (2323 + 1000)))) || (('ss_wholesale_cost >= 31) && ('ss_wholesale_cost <= (31 + 20)))))
         :  :  :  :           +- 'UnresolvedRelation `store_sales`
         :  :  :  +- 'SubqueryAlias `B3`
         :  :  :     +- 'Project ['avg('ss_list_price) AS B3_LP#6, 'count('ss_list_price) AS B3_CNT#7, 'count('ss_list_price) AS B3_CNTD#8]
         :  :  :        +- 'Filter ((('ss_quantity >= 11) && ('ss_quantity <= 15)) && (((('ss_list_price >= 142) && ('ss_list_price <= (142 + 10))) || (('ss_coupon_amt >= 12214) && ('ss_coupon_amt <= (12214 + 1000)))) || (('ss_wholesale_cost >= 79) && ('ss_wholesale_cost <= (79 + 20)))))
         :  :  :           +- 'UnresolvedRelation `store_sales`
         :  :  +- 'SubqueryAlias `B4`
         :  :     +- 'Project ['avg('ss_list_price) AS B4_LP#9, 'count('ss_list_price) AS B4_CNT#10, 'count('ss_list_price) AS B4_CNTD#11]
         :  :        +- 'Filter ((('ss_quantity >= 16) && ('ss_quantity <= 20)) && (((('ss_list_price >= 135) && ('ss_list_price <= (135 + 10))) || (('ss_coupon_amt >= 6071) && ('ss_coupon_amt <= (6071 + 1000)))) || (('ss_wholesale_cost >= 38) && ('ss_wholesale_cost <= (38 + 20)))))
         :  :           +- 'UnresolvedRelation `store_sales`
         :  +- 'SubqueryAlias `B5`
         :     +- 'Project ['avg('ss_list_price) AS B5_LP#12, 'count('ss_list_price) AS B5_CNT#13, 'count('ss_list_price) AS B5_CNTD#14]
         :        +- 'Filter ((('ss_quantity >= 21) && ('ss_quantity <= 25)) && (((('ss_list_price >= 122) && ('ss_list_price <= (122 + 10))) || (('ss_coupon_amt >= 836) && ('ss_coupon_amt <= (836 + 1000)))) || (('ss_wholesale_cost >= 17) && ('ss_wholesale_cost <= (17 + 20)))))
         :           +- 'UnresolvedRelation `store_sales`
         +- 'SubqueryAlias `B6`
            +- 'Project ['avg('ss_list_price) AS B6_LP#15, 'count('ss_list_price) AS B6_CNT#16, 'count('ss_list_price) AS B6_CNTD#17]
               +- 'Filter ((('ss_quantity >= 26) && ('ss_quantity <= 30)) && (((('ss_list_price >= 154) && ('ss_list_price <= (154 + 10))) || (('ss_coupon_amt >= 7326) && ('ss_coupon_amt <= (7326 + 1000)))) || (('ss_wholesale_cost >= 7) && ('ss_wholesale_cost <= (7 + 20)))))
                  +- 'UnresolvedRelation `store_sales`

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
         :  :  :  :  :- SubqueryAlias `B1`
         :  :  :  :  :  +- Aggregate [avg(ss_list_price#32) AS B1_LP#0, count(ss_list_price#32) AS B1_CNT#1L, count(distinct ss_list_price#32) AS B1_CNTD#2L]
         :  :  :  :  :     +- Filter (((ss_quantity#30 >= 0) && (ss_quantity#30 <= 5)) && ((((ss_list_price#32 >= cast(8 as double)) && (ss_list_price#32 <= cast((8 + 10) as double))) || ((ss_coupon_amt#39 >= cast(459 as double)) && (ss_coupon_amt#39 <= cast((459 + 1000) as double)))) || ((ss_wholesale_cost#31 >= cast(57 as double)) && (ss_wholesale_cost#31 <= cast((57 + 20) as double)))))
         :  :  :  :  :        +- SubqueryAlias `tpcds`.`store_sales`
         :  :  :  :  :           +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
         :  :  :  :  +- SubqueryAlias `B2`
         :  :  :  :     +- Aggregate [avg(ss_list_price#32) AS B2_LP#3, count(ss_list_price#32) AS B2_CNT#4L, count(distinct ss_list_price#32) AS B2_CNTD#5L]
         :  :  :  :        +- Filter (((ss_quantity#30 >= 6) && (ss_quantity#30 <= 10)) && ((((ss_list_price#32 >= cast(90 as double)) && (ss_list_price#32 <= cast((90 + 10) as double))) || ((ss_coupon_amt#39 >= cast(2323 as double)) && (ss_coupon_amt#39 <= cast((2323 + 1000) as double)))) || ((ss_wholesale_cost#31 >= cast(31 as double)) && (ss_wholesale_cost#31 <= cast((31 + 20) as double)))))
         :  :  :  :           +- SubqueryAlias `tpcds`.`store_sales`
         :  :  :  :              +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
         :  :  :  +- SubqueryAlias `B3`
         :  :  :     +- Aggregate [avg(ss_list_price#32) AS B3_LP#6, count(ss_list_price#32) AS B3_CNT#7L, count(distinct ss_list_price#32) AS B3_CNTD#8L]
         :  :  :        +- Filter (((ss_quantity#30 >= 11) && (ss_quantity#30 <= 15)) && ((((ss_list_price#32 >= cast(142 as double)) && (ss_list_price#32 <= cast((142 + 10) as double))) || ((ss_coupon_amt#39 >= cast(12214 as double)) && (ss_coupon_amt#39 <= cast((12214 + 1000) as double)))) || ((ss_wholesale_cost#31 >= cast(79 as double)) && (ss_wholesale_cost#31 <= cast((79 + 20) as double)))))
         :  :  :           +- SubqueryAlias `tpcds`.`store_sales`
         :  :  :              +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
         :  :  +- SubqueryAlias `B4`
         :  :     +- Aggregate [avg(ss_list_price#32) AS B4_LP#9, count(ss_list_price#32) AS B4_CNT#10L, count(distinct ss_list_price#32) AS B4_CNTD#11L]
         :  :        +- Filter (((ss_quantity#30 >= 16) && (ss_quantity#30 <= 20)) && ((((ss_list_price#32 >= cast(135 as double)) && (ss_list_price#32 <= cast((135 + 10) as double))) || ((ss_coupon_amt#39 >= cast(6071 as double)) && (ss_coupon_amt#39 <= cast((6071 + 1000) as double)))) || ((ss_wholesale_cost#31 >= cast(38 as double)) && (ss_wholesale_cost#31 <= cast((38 + 20) as double)))))
         :  :           +- SubqueryAlias `tpcds`.`store_sales`
         :  :              +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
         :  +- SubqueryAlias `B5`
         :     +- Aggregate [avg(ss_list_price#32) AS B5_LP#12, count(ss_list_price#32) AS B5_CNT#13L, count(distinct ss_list_price#32) AS B5_CNTD#14L]
         :        +- Filter (((ss_quantity#30 >= 21) && (ss_quantity#30 <= 25)) && ((((ss_list_price#32 >= cast(122 as double)) && (ss_list_price#32 <= cast((122 + 10) as double))) || ((ss_coupon_amt#39 >= cast(836 as double)) && (ss_coupon_amt#39 <= cast((836 + 1000) as double)))) || ((ss_wholesale_cost#31 >= cast(17 as double)) && (ss_wholesale_cost#31 <= cast((17 + 20) as double)))))
         :           +- SubqueryAlias `tpcds`.`store_sales`
         :              +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
         +- SubqueryAlias `B6`
            +- Aggregate [avg(ss_list_price#32) AS B6_LP#15, count(ss_list_price#32) AS B6_CNT#16L, count(distinct ss_list_price#32) AS B6_CNTD#17L]
               +- Filter (((ss_quantity#30 >= 26) && (ss_quantity#30 <= 30)) && ((((ss_list_price#32 >= cast(154 as double)) && (ss_list_price#32 <= cast((154 + 10) as double))) || ((ss_coupon_amt#39 >= cast(7326 as double)) && (ss_coupon_amt#39 <= cast((7326 + 1000) as double)))) || ((ss_wholesale_cost#31 >= cast(7 as double)) && (ss_wholesale_cost#31 <= cast((7 + 20) as double)))))
                  +- SubqueryAlias `tpcds`.`store_sales`
                     +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Join Inner
      :- Join Inner
      :  :- Join Inner
      :  :  :- Join Inner
      :  :  :  :- Join Inner
      :  :  :  :  :- Aggregate [avg(ss_list_price#32) AS B1_LP#0, count(ss_list_price#32) AS B1_CNT#1L, count(distinct ss_list_price#32) AS B1_CNTD#2L]
      :  :  :  :  :  +- Project [ss_list_price#32]
      :  :  :  :  :     +- Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 0)) && (ss_quantity#30 <= 5)) && ((((ss_list_price#32 >= 8.0) && (ss_list_price#32 <= 18.0)) || ((ss_coupon_amt#39 >= 459.0) && (ss_coupon_amt#39 <= 1459.0))) || ((ss_wholesale_cost#31 >= 57.0) && (ss_wholesale_cost#31 <= 77.0))))
      :  :  :  :  :        +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
      :  :  :  :  +- Aggregate [avg(ss_list_price#32) AS B2_LP#3, count(ss_list_price#32) AS B2_CNT#4L, count(distinct ss_list_price#32) AS B2_CNTD#5L]
      :  :  :  :     +- Project [ss_list_price#32]
      :  :  :  :        +- Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 6)) && (ss_quantity#30 <= 10)) && ((((ss_list_price#32 >= 90.0) && (ss_list_price#32 <= 100.0)) || ((ss_coupon_amt#39 >= 2323.0) && (ss_coupon_amt#39 <= 3323.0))) || ((ss_wholesale_cost#31 >= 31.0) && (ss_wholesale_cost#31 <= 51.0))))
      :  :  :  :           +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
      :  :  :  +- Aggregate [avg(ss_list_price#32) AS B3_LP#6, count(ss_list_price#32) AS B3_CNT#7L, count(distinct ss_list_price#32) AS B3_CNTD#8L]
      :  :  :     +- Project [ss_list_price#32]
      :  :  :        +- Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 11)) && (ss_quantity#30 <= 15)) && ((((ss_list_price#32 >= 142.0) && (ss_list_price#32 <= 152.0)) || ((ss_coupon_amt#39 >= 12214.0) && (ss_coupon_amt#39 <= 13214.0))) || ((ss_wholesale_cost#31 >= 79.0) && (ss_wholesale_cost#31 <= 99.0))))
      :  :  :           +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
      :  :  +- Aggregate [avg(ss_list_price#32) AS B4_LP#9, count(ss_list_price#32) AS B4_CNT#10L, count(distinct ss_list_price#32) AS B4_CNTD#11L]
      :  :     +- Project [ss_list_price#32]
      :  :        +- Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 16)) && (ss_quantity#30 <= 20)) && ((((ss_list_price#32 >= 135.0) && (ss_list_price#32 <= 145.0)) || ((ss_coupon_amt#39 >= 6071.0) && (ss_coupon_amt#39 <= 7071.0))) || ((ss_wholesale_cost#31 >= 38.0) && (ss_wholesale_cost#31 <= 58.0))))
      :  :           +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
      :  +- Aggregate [avg(ss_list_price#32) AS B5_LP#12, count(ss_list_price#32) AS B5_CNT#13L, count(distinct ss_list_price#32) AS B5_CNTD#14L]
      :     +- Project [ss_list_price#32]
      :        +- Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 21)) && (ss_quantity#30 <= 25)) && ((((ss_list_price#32 >= 122.0) && (ss_list_price#32 <= 132.0)) || ((ss_coupon_amt#39 >= 836.0) && (ss_coupon_amt#39 <= 1836.0))) || ((ss_wholesale_cost#31 >= 17.0) && (ss_wholesale_cost#31 <= 37.0))))
      :           +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet
      +- Aggregate [avg(ss_list_price#32) AS B6_LP#15, count(ss_list_price#32) AS B6_CNT#16L, count(distinct ss_list_price#32) AS B6_CNTD#17L]
         +- Project [ss_list_price#32]
            +- Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 26)) && (ss_quantity#30 <= 30)) && ((((ss_list_price#32 >= 154.0) && (ss_list_price#32 <= 164.0)) || ((ss_coupon_amt#39 >= 7326.0) && (ss_coupon_amt#39 <= 8326.0))) || ((ss_wholesale_cost#31 >= 7.0) && (ss_wholesale_cost#31 <= 27.0))))
               +- Relation[ss_sold_date_sk#20,ss_sold_time_sk#21,ss_item_sk#22,ss_customer_sk#23,ss_cdemo_sk#24,ss_hdemo_sk#25,ss_addr_sk#26,ss_store_sk#27,ss_promo_sk#28,ss_ticket_number#29,ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42] parquet

== Physical Plan ==
CollectLimit 100
+- BroadcastNestedLoopJoin BuildRight, Inner
   :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :  :- BroadcastNestedLoopJoin BuildRight, Inner
   :  :  :  :  :- *(3) HashAggregate(keys=[], functions=[avg(ss_list_price#32), count(ss_list_price#32), count(distinct ss_list_price#32)], output=[B1_LP#0, B1_CNT#1L, B1_CNTD#2L])
   :  :  :  :  :  +- Exchange SinglePartition
   :  :  :  :  :     +- *(2) HashAggregate(keys=[], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32), partial_count(distinct ss_list_price#32)], output=[sum#63, count#64L, count#66L, count#69L])
   :  :  :  :  :        +- *(2) HashAggregate(keys=[ss_list_price#32], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32)], output=[ss_list_price#32, sum#63, count#64L, count#66L])
   :  :  :  :  :           +- Exchange hashpartitioning(ss_list_price#32, 200)
   :  :  :  :  :              +- *(1) HashAggregate(keys=[ss_list_price#32], functions=[partial_avg(ss_list_price#32), partial_count(ss_list_price#32)], output=[ss_list_price#32, sum#63, count#64L, count#66L])
   :  :  :  :  :                 +- *(1) Project [ss_list_price#32]
   :  :  :  :  :                    +- *(1) Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 0)) && (ss_quantity#30 <= 5)) && ((((ss_list_price#32 >= 8.0) && (ss_list_price#32 <= 18.0)) || ((ss_coupon_amt#39 >= 459.0) && (ss_coupon_amt#39 <= 1459.0))) || ((ss_wholesale_cost#31 >= 57.0) && (ss_wholesale_cost#31 <= 77.0))))
   :  :  :  :  :                       +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_coupon_amt#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,0), LessThanOrEqual(ss_quantity,5), Or(Or..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   :  :  :  :  +- BroadcastExchange IdentityBroadcastMode
   :  :  :  :     +- *(6) HashAggregate(keys=[], functions=[avg(ss_list_price#32), count(ss_list_price#32), count(distinct ss_list_price#32)], output=[B2_LP#3, B2_CNT#4L, B2_CNTD#5L])
   :  :  :  :        +- Exchange SinglePartition
   :  :  :  :           +- *(5) HashAggregate(keys=[], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32), partial_count(distinct ss_list_price#32)], output=[sum#73, count#74L, count#76L, count#79L])
   :  :  :  :              +- *(5) HashAggregate(keys=[ss_list_price#32], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32)], output=[ss_list_price#32, sum#73, count#74L, count#76L])
   :  :  :  :                 +- Exchange hashpartitioning(ss_list_price#32, 200)
   :  :  :  :                    +- *(4) HashAggregate(keys=[ss_list_price#32], functions=[partial_avg(ss_list_price#32), partial_count(ss_list_price#32)], output=[ss_list_price#32, sum#73, count#74L, count#76L])
   :  :  :  :                       +- *(4) Project [ss_list_price#32]
   :  :  :  :                          +- *(4) Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 6)) && (ss_quantity#30 <= 10)) && ((((ss_list_price#32 >= 90.0) && (ss_list_price#32 <= 100.0)) || ((ss_coupon_amt#39 >= 2323.0) && (ss_coupon_amt#39 <= 3323.0))) || ((ss_wholesale_cost#31 >= 31.0) && (ss_wholesale_cost#31 <= 51.0))))
   :  :  :  :                             +- *(4) FileScan parquet tpcds.store_sales[ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_coupon_amt#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,6), LessThanOrEqual(ss_quantity,10), Or(O..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   :  :  :  +- BroadcastExchange IdentityBroadcastMode
   :  :  :     +- *(9) HashAggregate(keys=[], functions=[avg(ss_list_price#32), count(ss_list_price#32), count(distinct ss_list_price#32)], output=[B3_LP#6, B3_CNT#7L, B3_CNTD#8L])
   :  :  :        +- Exchange SinglePartition
   :  :  :           +- *(8) HashAggregate(keys=[], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32), partial_count(distinct ss_list_price#32)], output=[sum#83, count#84L, count#86L, count#89L])
   :  :  :              +- *(8) HashAggregate(keys=[ss_list_price#32], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32)], output=[ss_list_price#32, sum#83, count#84L, count#86L])
   :  :  :                 +- Exchange hashpartitioning(ss_list_price#32, 200)
   :  :  :                    +- *(7) HashAggregate(keys=[ss_list_price#32], functions=[partial_avg(ss_list_price#32), partial_count(ss_list_price#32)], output=[ss_list_price#32, sum#83, count#84L, count#86L])
   :  :  :                       +- *(7) Project [ss_list_price#32]
   :  :  :                          +- *(7) Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 11)) && (ss_quantity#30 <= 15)) && ((((ss_list_price#32 >= 142.0) && (ss_list_price#32 <= 152.0)) || ((ss_coupon_amt#39 >= 12214.0) && (ss_coupon_amt#39 <= 13214.0))) || ((ss_wholesale_cost#31 >= 79.0) && (ss_wholesale_cost#31 <= 99.0))))
   :  :  :                             +- *(7) FileScan parquet tpcds.store_sales[ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_coupon_amt#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,11), LessThanOrEqual(ss_quantity,15), Or(..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   :  :  +- BroadcastExchange IdentityBroadcastMode
   :  :     +- *(12) HashAggregate(keys=[], functions=[avg(ss_list_price#32), count(ss_list_price#32), count(distinct ss_list_price#32)], output=[B4_LP#9, B4_CNT#10L, B4_CNTD#11L])
   :  :        +- Exchange SinglePartition
   :  :           +- *(11) HashAggregate(keys=[], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32), partial_count(distinct ss_list_price#32)], output=[sum#93, count#94L, count#96L, count#99L])
   :  :              +- *(11) HashAggregate(keys=[ss_list_price#32], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32)], output=[ss_list_price#32, sum#93, count#94L, count#96L])
   :  :                 +- Exchange hashpartitioning(ss_list_price#32, 200)
   :  :                    +- *(10) HashAggregate(keys=[ss_list_price#32], functions=[partial_avg(ss_list_price#32), partial_count(ss_list_price#32)], output=[ss_list_price#32, sum#93, count#94L, count#96L])
   :  :                       +- *(10) Project [ss_list_price#32]
   :  :                          +- *(10) Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 16)) && (ss_quantity#30 <= 20)) && ((((ss_list_price#32 >= 135.0) && (ss_list_price#32 <= 145.0)) || ((ss_coupon_amt#39 >= 6071.0) && (ss_coupon_amt#39 <= 7071.0))) || ((ss_wholesale_cost#31 >= 38.0) && (ss_wholesale_cost#31 <= 58.0))))
   :  :                             +- *(10) FileScan parquet tpcds.store_sales[ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_coupon_amt#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,16), LessThanOrEqual(ss_quantity,20), Or(..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   :  +- BroadcastExchange IdentityBroadcastMode
   :     +- *(15) HashAggregate(keys=[], functions=[avg(ss_list_price#32), count(ss_list_price#32), count(distinct ss_list_price#32)], output=[B5_LP#12, B5_CNT#13L, B5_CNTD#14L])
   :        +- Exchange SinglePartition
   :           +- *(14) HashAggregate(keys=[], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32), partial_count(distinct ss_list_price#32)], output=[sum#103, count#104L, count#106L, count#109L])
   :              +- *(14) HashAggregate(keys=[ss_list_price#32], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32)], output=[ss_list_price#32, sum#103, count#104L, count#106L])
   :                 +- Exchange hashpartitioning(ss_list_price#32, 200)
   :                    +- *(13) HashAggregate(keys=[ss_list_price#32], functions=[partial_avg(ss_list_price#32), partial_count(ss_list_price#32)], output=[ss_list_price#32, sum#103, count#104L, count#106L])
   :                       +- *(13) Project [ss_list_price#32]
   :                          +- *(13) Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 21)) && (ss_quantity#30 <= 25)) && ((((ss_list_price#32 >= 122.0) && (ss_list_price#32 <= 132.0)) || ((ss_coupon_amt#39 >= 836.0) && (ss_coupon_amt#39 <= 1836.0))) || ((ss_wholesale_cost#31 >= 17.0) && (ss_wholesale_cost#31 <= 37.0))))
   :                             +- *(13) FileScan parquet tpcds.store_sales[ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_coupon_amt#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,21), LessThanOrEqual(ss_quantity,25), Or(..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
   +- BroadcastExchange IdentityBroadcastMode
      +- *(18) HashAggregate(keys=[], functions=[avg(ss_list_price#32), count(ss_list_price#32), count(distinct ss_list_price#32)], output=[B6_LP#15, B6_CNT#16L, B6_CNTD#17L])
         +- Exchange SinglePartition
            +- *(17) HashAggregate(keys=[], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32), partial_count(distinct ss_list_price#32)], output=[sum#113, count#114L, count#116L, count#119L])
               +- *(17) HashAggregate(keys=[ss_list_price#32], functions=[merge_avg(ss_list_price#32), merge_count(ss_list_price#32)], output=[ss_list_price#32, sum#113, count#114L, count#116L])
                  +- Exchange hashpartitioning(ss_list_price#32, 200)
                     +- *(16) HashAggregate(keys=[ss_list_price#32], functions=[partial_avg(ss_list_price#32), partial_count(ss_list_price#32)], output=[ss_list_price#32, sum#113, count#114L, count#116L])
                        +- *(16) Project [ss_list_price#32]
                           +- *(16) Filter (((isnotnull(ss_quantity#30) && (ss_quantity#30 >= 26)) && (ss_quantity#30 <= 30)) && ((((ss_list_price#32 >= 154.0) && (ss_list_price#32 <= 164.0)) || ((ss_coupon_amt#39 >= 7326.0) && (ss_coupon_amt#39 <= 8326.0))) || ((ss_wholesale_cost#31 >= 7.0) && (ss_wholesale_cost#31 <= 27.0))))
                              +- *(16) FileScan parquet tpcds.store_sales[ss_quantity#30,ss_wholesale_cost#31,ss_list_price#32,ss_coupon_amt#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,26), LessThanOrEqual(ss_quantity,30), Or(..., ReadSchema: struct<ss_quantity:int,ss_wholesale_cost:double,ss_list_price:double,ss_coupon_amt:double>
Time taken: 4.545 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 28 in stream 0 using template query28.tpl
------------------------------------------------------^^^

