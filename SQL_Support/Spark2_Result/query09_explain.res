== Parsed Logical Plan ==
'Project [CASE WHEN (scalar-subquery#0 [] > 74129) THEN scalar-subquery#1 [] ELSE scalar-subquery#2 [] END AS bucket1#3, CASE WHEN (scalar-subquery#4 [] > 122840) THEN scalar-subquery#5 [] ELSE scalar-subquery#6 [] END AS bucket2#7, CASE WHEN (scalar-subquery#8 [] > 56580) THEN scalar-subquery#9 [] ELSE scalar-subquery#10 [] END AS bucket3#11, CASE WHEN (scalar-subquery#12 [] > 10097) THEN scalar-subquery#13 [] ELSE scalar-subquery#14 [] END AS bucket4#15, CASE WHEN (scalar-subquery#16 [] > 165306) THEN scalar-subquery#17 [] ELSE scalar-subquery#18 [] END AS bucket5#19]
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 1) && ('ss_quantity <= 20))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 1) && ('ss_quantity <= 20))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:  :  +- 'Filter (('ss_quantity >= 1) && ('ss_quantity <= 20))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 21) && ('ss_quantity <= 40))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 21) && ('ss_quantity <= 40))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:  :  +- 'Filter (('ss_quantity >= 21) && ('ss_quantity <= 40))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 41) && ('ss_quantity <= 60))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 41) && ('ss_quantity <= 60))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:  :  +- 'Filter (('ss_quantity >= 41) && ('ss_quantity <= 60))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 61) && ('ss_quantity <= 80))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 61) && ('ss_quantity <= 80))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:  :  +- 'Filter (('ss_quantity >= 61) && ('ss_quantity <= 80))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('count(1), None)]
:  :  +- 'Filter (('ss_quantity >= 81) && ('ss_quantity <= 100))
:  :     +- 'UnresolvedRelation `store_sales`
:  :- 'Project [unresolvedalias('avg('ss_ext_discount_amt), None)]
:  :  +- 'Filter (('ss_quantity >= 81) && ('ss_quantity <= 100))
:  :     +- 'UnresolvedRelation `store_sales`
:  +- 'Project [unresolvedalias('avg('ss_net_paid), None)]
:     +- 'Filter (('ss_quantity >= 81) && ('ss_quantity <= 100))
:        +- 'UnresolvedRelation `store_sales`
+- 'Filter ('r_reason_sk = 1)
   +- 'UnresolvedRelation `reason`

== Analyzed Logical Plan ==
bucket1: double, bucket2: double, bucket3: double, bucket4: double, bucket5: double
Project [CASE WHEN (scalar-subquery#0 [] > cast(74129 as bigint)) THEN scalar-subquery#1 [] ELSE scalar-subquery#2 [] END AS bucket1#3, CASE WHEN (scalar-subquery#4 [] > cast(122840 as bigint)) THEN scalar-subquery#5 [] ELSE scalar-subquery#6 [] END AS bucket2#7, CASE WHEN (scalar-subquery#8 [] > cast(56580 as bigint)) THEN scalar-subquery#9 [] ELSE scalar-subquery#10 [] END AS bucket3#11, CASE WHEN (scalar-subquery#12 [] > cast(10097 as bigint)) THEN scalar-subquery#13 [] ELSE scalar-subquery#14 [] END AS bucket4#15, CASE WHEN (scalar-subquery#16 [] > cast(165306 as bigint)) THEN scalar-subquery#17 [] ELSE scalar-subquery#18 [] END AS bucket5#19]
:  :- Aggregate [count(1) AS count(1)#49L]
:  :  +- Filter ((ss_quantity#36 >= 1) && (ss_quantity#36 <= 20))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#51]
:  :  +- Filter ((ss_quantity#36 >= 1) && (ss_quantity#36 <= 20))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#53]
:  :  +- Filter ((ss_quantity#36 >= 1) && (ss_quantity#36 <= 20))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [count(1) AS count(1)#55L]
:  :  +- Filter ((ss_quantity#36 >= 21) && (ss_quantity#36 <= 40))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#57]
:  :  +- Filter ((ss_quantity#36 >= 21) && (ss_quantity#36 <= 40))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#59]
:  :  +- Filter ((ss_quantity#36 >= 21) && (ss_quantity#36 <= 40))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [count(1) AS count(1)#61L]
:  :  +- Filter ((ss_quantity#36 >= 41) && (ss_quantity#36 <= 60))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#63]
:  :  +- Filter ((ss_quantity#36 >= 41) && (ss_quantity#36 <= 60))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#65]
:  :  +- Filter ((ss_quantity#36 >= 41) && (ss_quantity#36 <= 60))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [count(1) AS count(1)#67L]
:  :  +- Filter ((ss_quantity#36 >= 61) && (ss_quantity#36 <= 80))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#69]
:  :  +- Filter ((ss_quantity#36 >= 61) && (ss_quantity#36 <= 80))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#71]
:  :  +- Filter ((ss_quantity#36 >= 61) && (ss_quantity#36 <= 80))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [count(1) AS count(1)#73L]
:  :  +- Filter ((ss_quantity#36 >= 81) && (ss_quantity#36 <= 100))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#75]
:  :  +- Filter ((ss_quantity#36 >= 81) && (ss_quantity#36 <= 100))
:  :     +- SubqueryAlias `tpcds`.`store_sales`
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  +- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#77]
:     +- Filter ((ss_quantity#36 >= 81) && (ss_quantity#36 <= 100))
:        +- SubqueryAlias `tpcds`.`store_sales`
:           +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
+- Filter (r_reason_sk#22 = 1)
   +- SubqueryAlias `tpcds`.`reason`
      +- Relation[r_reason_sk#22,r_reason_id#23,r_reason_desc#24] parquet

== Optimized Logical Plan ==
Project [CASE WHEN (scalar-subquery#0 [] > 74129) THEN scalar-subquery#1 [] ELSE scalar-subquery#2 [] END AS bucket1#3, CASE WHEN (scalar-subquery#4 [] > 122840) THEN scalar-subquery#5 [] ELSE scalar-subquery#6 [] END AS bucket2#7, CASE WHEN (scalar-subquery#8 [] > 56580) THEN scalar-subquery#9 [] ELSE scalar-subquery#10 [] END AS bucket3#11, CASE WHEN (scalar-subquery#12 [] > 10097) THEN scalar-subquery#13 [] ELSE scalar-subquery#14 [] END AS bucket4#15, CASE WHEN (scalar-subquery#16 [] > 165306) THEN scalar-subquery#17 [] ELSE scalar-subquery#18 [] END AS bucket5#19]
:  :- Aggregate [count(1) AS count(1)#49L]
:  :  +- Project
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 1)) && (ss_quantity#36 <= 20))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#51]
:  :  +- Project [ss_ext_discount_amt#40]
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 1)) && (ss_quantity#36 <= 20))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#53]
:  :  +- Project [ss_net_paid#46]
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 1)) && (ss_quantity#36 <= 20))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [count(1) AS count(1)#55L]
:  :  +- Project
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 21)) && (ss_quantity#36 <= 40))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#57]
:  :  +- Project [ss_ext_discount_amt#40]
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 21)) && (ss_quantity#36 <= 40))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#59]
:  :  +- Project [ss_net_paid#46]
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 21)) && (ss_quantity#36 <= 40))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [count(1) AS count(1)#61L]
:  :  +- Project
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 41)) && (ss_quantity#36 <= 60))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#63]
:  :  +- Project [ss_ext_discount_amt#40]
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 41)) && (ss_quantity#36 <= 60))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#65]
:  :  +- Project [ss_net_paid#46]
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 41)) && (ss_quantity#36 <= 60))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [count(1) AS count(1)#67L]
:  :  +- Project
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 61)) && (ss_quantity#36 <= 80))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#69]
:  :  +- Project [ss_ext_discount_amt#40]
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 61)) && (ss_quantity#36 <= 80))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#71]
:  :  +- Project [ss_net_paid#46]
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 61)) && (ss_quantity#36 <= 80))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [count(1) AS count(1)#73L]
:  :  +- Project
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 81)) && (ss_quantity#36 <= 100))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  :- Aggregate [avg(ss_ext_discount_amt#40) AS avg(ss_ext_discount_amt)#75]
:  :  +- Project [ss_ext_discount_amt#40]
:  :     +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 81)) && (ss_quantity#36 <= 100))
:  :        +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
:  +- Aggregate [avg(ss_net_paid#46) AS avg(ss_net_paid)#77]
:     +- Project [ss_net_paid#46]
:        +- Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 81)) && (ss_quantity#36 <= 100))
:           +- Relation[ss_sold_date_sk#26,ss_sold_time_sk#27,ss_item_sk#28,ss_customer_sk#29,ss_cdemo_sk#30,ss_hdemo_sk#31,ss_addr_sk#32,ss_store_sk#33,ss_promo_sk#34,ss_ticket_number#35,ss_quantity#36,ss_wholesale_cost#37,ss_list_price#38,ss_sales_price#39,ss_ext_discount_amt#40,ss_ext_sales_price#41,ss_ext_wholesale_cost#42,ss_ext_list_price#43,ss_ext_tax#44,ss_coupon_amt#45,ss_net_paid#46,ss_net_paid_inc_tax#47,ss_net_profit#48] parquet
+- Filter (isnotnull(r_reason_sk#22) && (r_reason_sk#22 = 1))
   +- Relation[r_reason_sk#22,r_reason_id#23,r_reason_desc#24] parquet

== Physical Plan ==
*(1) Project [CASE WHEN (Subquery subquery0 > 74129) THEN Subquery subquery1 ELSE Subquery subquery2 END AS bucket1#3, CASE WHEN (Subquery subquery4 > 122840) THEN Subquery subquery5 ELSE Subquery subquery6 END AS bucket2#7, CASE WHEN (Subquery subquery8 > 56580) THEN Subquery subquery9 ELSE Subquery subquery10 END AS bucket3#11, CASE WHEN (Subquery subquery12 > 10097) THEN Subquery subquery13 ELSE Subquery subquery14 END AS bucket4#15, CASE WHEN (Subquery subquery16 > 165306) THEN Subquery subquery17 ELSE Subquery subquery18 END AS bucket5#19]
:  :- Subquery subquery0
:  :  +- *(2) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#49L])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#79L])
:  :           +- *(1) Project
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 1)) && (ss_quantity#36 <= 20))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,1), LessThanOrEqual(ss_quantity,20)], ReadSchema: struct<ss_quantity:int>
:  :- Subquery subquery1
:  :  +- *(2) HashAggregate(keys=[], functions=[avg(ss_ext_discount_amt#40)], output=[avg(ss_ext_discount_amt)#51])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_ext_discount_amt#40)], output=[sum#82, count#83L])
:  :           +- *(1) Project [ss_ext_discount_amt#40]
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 1)) && (ss_quantity#36 <= 20))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_ext_discount_amt#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,1), LessThanOrEqual(ss_quantity,20)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double>
:  :- Subquery subquery2
:  :  +- *(2) HashAggregate(keys=[], functions=[avg(ss_net_paid#46)], output=[avg(ss_net_paid)#53])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_net_paid#46)], output=[sum#86, count#87L])
:  :           +- *(1) Project [ss_net_paid#46]
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 1)) && (ss_quantity#36 <= 20))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_net_paid#46] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,1), LessThanOrEqual(ss_quantity,20)], ReadSchema: struct<ss_quantity:int,ss_net_paid:double>
:  :- Subquery subquery4
:  :  +- *(2) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#55L])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#89L])
:  :           +- *(1) Project
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 21)) && (ss_quantity#36 <= 40))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,21), LessThanOrEqual(ss_quantity,40)], ReadSchema: struct<ss_quantity:int>
:  :- Subquery subquery5
:  :  +- *(2) HashAggregate(keys=[], functions=[avg(ss_ext_discount_amt#40)], output=[avg(ss_ext_discount_amt)#57])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_ext_discount_amt#40)], output=[sum#92, count#93L])
:  :           +- *(1) Project [ss_ext_discount_amt#40]
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 21)) && (ss_quantity#36 <= 40))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_ext_discount_amt#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,21), LessThanOrEqual(ss_quantity,40)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double>
:  :- Subquery subquery6
:  :  +- *(2) HashAggregate(keys=[], functions=[avg(ss_net_paid#46)], output=[avg(ss_net_paid)#59])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_net_paid#46)], output=[sum#96, count#97L])
:  :           +- *(1) Project [ss_net_paid#46]
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 21)) && (ss_quantity#36 <= 40))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_net_paid#46] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,21), LessThanOrEqual(ss_quantity,40)], ReadSchema: struct<ss_quantity:int,ss_net_paid:double>
:  :- Subquery subquery8
:  :  +- *(2) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#61L])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#99L])
:  :           +- *(1) Project
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 41)) && (ss_quantity#36 <= 60))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,41), LessThanOrEqual(ss_quantity,60)], ReadSchema: struct<ss_quantity:int>
:  :- Subquery subquery9
:  :  +- *(2) HashAggregate(keys=[], functions=[avg(ss_ext_discount_amt#40)], output=[avg(ss_ext_discount_amt)#63])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_ext_discount_amt#40)], output=[sum#102, count#103L])
:  :           +- *(1) Project [ss_ext_discount_amt#40]
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 41)) && (ss_quantity#36 <= 60))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_ext_discount_amt#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,41), LessThanOrEqual(ss_quantity,60)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double>
:  :- Subquery subquery10
:  :  +- *(2) HashAggregate(keys=[], functions=[avg(ss_net_paid#46)], output=[avg(ss_net_paid)#65])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_net_paid#46)], output=[sum#106, count#107L])
:  :           +- *(1) Project [ss_net_paid#46]
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 41)) && (ss_quantity#36 <= 60))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_net_paid#46] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,41), LessThanOrEqual(ss_quantity,60)], ReadSchema: struct<ss_quantity:int,ss_net_paid:double>
:  :- Subquery subquery12
:  :  +- *(2) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#67L])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#109L])
:  :           +- *(1) Project
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 61)) && (ss_quantity#36 <= 80))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,61), LessThanOrEqual(ss_quantity,80)], ReadSchema: struct<ss_quantity:int>
:  :- Subquery subquery13
:  :  +- *(2) HashAggregate(keys=[], functions=[avg(ss_ext_discount_amt#40)], output=[avg(ss_ext_discount_amt)#69])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_ext_discount_amt#40)], output=[sum#112, count#113L])
:  :           +- *(1) Project [ss_ext_discount_amt#40]
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 61)) && (ss_quantity#36 <= 80))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_ext_discount_amt#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,61), LessThanOrEqual(ss_quantity,80)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double>
:  :- Subquery subquery14
:  :  +- *(2) HashAggregate(keys=[], functions=[avg(ss_net_paid#46)], output=[avg(ss_net_paid)#71])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_net_paid#46)], output=[sum#116, count#117L])
:  :           +- *(1) Project [ss_net_paid#46]
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 61)) && (ss_quantity#36 <= 80))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_net_paid#46] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,61), LessThanOrEqual(ss_quantity,80)], ReadSchema: struct<ss_quantity:int,ss_net_paid:double>
:  :- Subquery subquery16
:  :  +- *(2) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#73L])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#119L])
:  :           +- *(1) Project
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 81)) && (ss_quantity#36 <= 100))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,81), LessThanOrEqual(ss_quantity,100)], ReadSchema: struct<ss_quantity:int>
:  :- Subquery subquery17
:  :  +- *(2) HashAggregate(keys=[], functions=[avg(ss_ext_discount_amt#40)], output=[avg(ss_ext_discount_amt)#75])
:  :     +- Exchange SinglePartition
:  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_ext_discount_amt#40)], output=[sum#122, count#123L])
:  :           +- *(1) Project [ss_ext_discount_amt#40]
:  :              +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 81)) && (ss_quantity#36 <= 100))
:  :                 +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_ext_discount_amt#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,81), LessThanOrEqual(ss_quantity,100)], ReadSchema: struct<ss_quantity:int,ss_ext_discount_amt:double>
:  +- Subquery subquery18
:     +- *(2) HashAggregate(keys=[], functions=[avg(ss_net_paid#46)], output=[avg(ss_net_paid)#77])
:        +- Exchange SinglePartition
:           +- *(1) HashAggregate(keys=[], functions=[partial_avg(ss_net_paid#46)], output=[sum#126, count#127L])
:              +- *(1) Project [ss_net_paid#46]
:                 +- *(1) Filter ((isnotnull(ss_quantity#36) && (ss_quantity#36 >= 81)) && (ss_quantity#36 <= 100))
:                    +- *(1) FileScan parquet tpcds.store_sales[ss_quantity#36,ss_net_paid#46] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_quantity), GreaterThanOrEqual(ss_quantity,81), LessThanOrEqual(ss_quantity,100)], ReadSchema: struct<ss_quantity:int,ss_net_paid:double>
+- *(1) Filter (isnotnull(r_reason_sk#22) && (r_reason_sk#22 = 1))
   +- *(1) FileScan parquet tpcds.reason[r_reason_sk#22] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/r..., PartitionFilters: [], PushedFilters: [IsNotNull(r_reason_sk), EqualTo(r_reason_sk,1)], ReadSchema: struct<r_reason_sk:int>
Time taken: 4.899 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 52)

== SQL ==

-- end query 9 in stream 0 using template query9.tpl
----------------------------------------------------^^^

