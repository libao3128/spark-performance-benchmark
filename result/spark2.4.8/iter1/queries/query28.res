Error in query: Detected implicit cartesian product for INNER join between logical plans
Join Inner
:- Join Inner
:  :- Join Inner
:  :  :- Join Inner
:  :  :  :- Aggregate [avg(ss_list_price#30) AS B1_LP#0, count(ss_list_price#30) AS B1_CNT#1L, count(distinct ss_list_price#30) AS B1_CNTD#2L]
:  :  :  :  +- Project [ss_list_price#30]
:  :  :  :     +- Filter (isnotnull(ss_quantity#28) && (((ss_quantity#28 >= 0) && (ss_quantity#28 <= 5)) && ((((ss_list_price#30 >= 8.0) && (ss_list_price#30 <= 18.0)) || ((ss_coupon_amt#37 >= 459.0) && (ss_coupon_amt#37 <= 1459.0))) || ((ss_wholesale_cost#29 >= 57.0) && (ss_wholesale_cost#29 <= 77.0)))))
:  :  :  :        +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:  :  :  +- Aggregate [avg(ss_list_price#30) AS B2_LP#3, count(ss_list_price#30) AS B2_CNT#4L, count(distinct ss_list_price#30) AS B2_CNTD#5L]
:  :  :     +- Project [ss_list_price#30]
:  :  :        +- Filter (isnotnull(ss_quantity#28) && (((ss_quantity#28 >= 6) && (ss_quantity#28 <= 10)) && ((((ss_list_price#30 >= 90.0) && (ss_list_price#30 <= 100.0)) || ((ss_coupon_amt#37 >= 2323.0) && (ss_coupon_amt#37 <= 3323.0))) || ((ss_wholesale_cost#29 >= 31.0) && (ss_wholesale_cost#29 <= 51.0)))))
:  :  :           +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:  :  +- Aggregate [avg(ss_list_price#30) AS B3_LP#6, count(ss_list_price#30) AS B3_CNT#7L, count(distinct ss_list_price#30) AS B3_CNTD#8L]
:  :     +- Project [ss_list_price#30]
:  :        +- Filter (isnotnull(ss_quantity#28) && (((ss_quantity#28 >= 11) && (ss_quantity#28 <= 15)) && ((((ss_list_price#30 >= 142.0) && (ss_list_price#30 <= 152.0)) || ((ss_coupon_amt#37 >= 12214.0) && (ss_coupon_amt#37 <= 13214.0))) || ((ss_wholesale_cost#29 >= 79.0) && (ss_wholesale_cost#29 <= 99.0)))))
:  :           +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
:  +- Aggregate [avg(ss_list_price#30) AS B4_LP#9, count(ss_list_price#30) AS B4_CNT#10L, count(distinct ss_list_price#30) AS B4_CNTD#11L]
:     +- Project [ss_list_price#30]
:        +- Filter (isnotnull(ss_quantity#28) && (((ss_quantity#28 >= 16) && (ss_quantity#28 <= 20)) && ((((ss_list_price#30 >= 135.0) && (ss_list_price#30 <= 145.0)) || ((ss_coupon_amt#37 >= 6071.0) && (ss_coupon_amt#37 <= 7071.0))) || ((ss_wholesale_cost#29 >= 38.0) && (ss_wholesale_cost#29 <= 58.0)))))
:           +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
+- Aggregate [avg(ss_list_price#30) AS B5_LP#12, count(ss_list_price#30) AS B5_CNT#13L, count(distinct ss_list_price#30) AS B5_CNTD#14L]
   +- Project [ss_list_price#30]
      +- Filter (isnotnull(ss_quantity#28) && (((ss_quantity#28 >= 21) && (ss_quantity#28 <= 25)) && ((((ss_list_price#30 >= 122.0) && (ss_list_price#30 <= 132.0)) || ((ss_coupon_amt#37 >= 836.0) && (ss_coupon_amt#37 <= 1836.0))) || ((ss_wholesale_cost#29 >= 17.0) && (ss_wholesale_cost#29 <= 37.0)))))
         +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
and
Aggregate [avg(ss_list_price#30) AS B6_LP#15, count(ss_list_price#30) AS B6_CNT#16L, count(distinct ss_list_price#30) AS B6_CNTD#17L]
+- Project [ss_list_price#30]
   +- Filter (isnotnull(ss_quantity#28) && (((ss_quantity#28 >= 26) && (ss_quantity#28 <= 30)) && ((((ss_list_price#30 >= 154.0) && (ss_list_price#30 <= 164.0)) || ((ss_coupon_amt#37 >= 7326.0) && (ss_coupon_amt#37 <= 8326.0))) || ((ss_wholesale_cost#29 >= 7.0) && (ss_wholesale_cost#29 <= 27.0)))))
      +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
Join condition is missing or trivial.
Either: use the CROSS JOIN syntax to allow cartesian products between these
relations, or: enable implicit cartesian products by setting the configuration
variable spark.sql.crossJoin.enabled=true;
