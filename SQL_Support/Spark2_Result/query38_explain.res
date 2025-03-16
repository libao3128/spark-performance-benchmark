== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Project [unresolvedalias('count(1), None)]
      +- 'SubqueryAlias `hot_cust`
         +- 'Intersect false
            :- 'Intersect false
            :  :- 'Distinct
            :  :  +- 'Project ['c_last_name, 'c_first_name, 'd_date]
            :  :     +- 'Filter ((('store_sales.ss_sold_date_sk = 'date_dim.d_date_sk) && ('store_sales.ss_customer_sk = 'customer.c_customer_sk)) && (('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11))))
            :  :        +- 'Join Inner
            :  :           :- 'Join Inner
            :  :           :  :- 'UnresolvedRelation `store_sales`
            :  :           :  +- 'UnresolvedRelation `date_dim`
            :  :           +- 'UnresolvedRelation `customer`
            :  +- 'Distinct
            :     +- 'Project ['c_last_name, 'c_first_name, 'd_date]
            :        +- 'Filter ((('catalog_sales.cs_sold_date_sk = 'date_dim.d_date_sk) && ('catalog_sales.cs_bill_customer_sk = 'customer.c_customer_sk)) && (('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11))))
            :           +- 'Join Inner
            :              :- 'Join Inner
            :              :  :- 'UnresolvedRelation `catalog_sales`
            :              :  +- 'UnresolvedRelation `date_dim`
            :              +- 'UnresolvedRelation `customer`
            +- 'Distinct
               +- 'Project ['c_last_name, 'c_first_name, 'd_date]
                  +- 'Filter ((('web_sales.ws_sold_date_sk = 'date_dim.d_date_sk) && ('web_sales.ws_bill_customer_sk = 'customer.c_customer_sk)) && (('d_month_seq >= 1200) && ('d_month_seq <= (1200 + 11))))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'UnresolvedRelation `web_sales`
                        :  +- 'UnresolvedRelation `date_dim`
                        +- 'UnresolvedRelation `customer`

== Analyzed Logical Plan ==
count(1): bigint
GlobalLimit 100
+- LocalLimit 100
   +- Aggregate [count(1) AS count(1)#268L]
      +- SubqueryAlias `hot_cust`
         +- Intersect false
            :- Project [c_last_name#63, c_first_name#62, d_date#28]
            :  +- Intersect false
            :     :- Project [c_last_name#63, c_first_name#62, d_date#28]
            :     :  +- Distinct
            :     :     +- Project [c_last_name#63, c_first_name#62, d_date#28]
            :     :        +- Filter (((ss_sold_date_sk#3 = d_date_sk#26) && (ss_customer_sk#6 = c_customer_sk#54)) && ((d_month_seq#29 >= 1200) && (d_month_seq#29 <= (1200 + 11))))
            :     :           +- Join Inner
            :     :              :- Join Inner
            :     :              :  :- SubqueryAlias `tpcds`.`store_sales`
            :     :              :  :  +- Relation[ss_sold_date_sk#3,ss_sold_time_sk#4,ss_item_sk#5,ss_customer_sk#6,ss_cdemo_sk#7,ss_hdemo_sk#8,ss_addr_sk#9,ss_store_sk#10,ss_promo_sk#11,ss_ticket_number#12,ss_quantity#13,ss_wholesale_cost#14,ss_list_price#15,ss_sales_price#16,ss_ext_discount_amt#17,ss_ext_sales_price#18,ss_ext_wholesale_cost#19,ss_ext_list_price#20,ss_ext_tax#21,ss_coupon_amt#22,ss_net_paid#23,ss_net_paid_inc_tax#24,ss_net_profit#25] parquet
            :     :              :  +- SubqueryAlias `tpcds`.`date_dim`
            :     :              :     +- Relation[d_date_sk#26,d_date_id#27,d_date#28,d_month_seq#29,d_week_seq#30,d_quarter_seq#31,d_year#32,d_dow#33,d_moy#34,d_dom#35,d_qoy#36,d_fy_year#37,d_fy_quarter_seq#38,d_fy_week_seq#39,d_day_name#40,d_quarter_name#41,d_holiday#42,d_weekend#43,d_following_holiday#44,d_first_dom#45,d_last_dom#46,d_same_day_ly#47,d_same_day_lq#48,d_current_day#49,... 4 more fields] parquet
            :     :              +- SubqueryAlias `tpcds`.`customer`
            :     :                 +- Relation[c_customer_sk#54,c_customer_id#55,c_current_cdemo_sk#56,c_current_hdemo_sk#57,c_current_addr_sk#58,c_first_shipto_date_sk#59,c_first_sales_date_sk#60,c_salutation#61,c_first_name#62,c_last_name#63,c_preferred_cust_flag#64,c_birth_day#65,c_birth_month#66,c_birth_year#67,c_birth_country#68,c_login#69,c_email_address#70,c_last_review_date#71] parquet
            :     +- Project [c_last_name#195, c_first_name#194, d_date#142]
            :        +- Distinct
            :           +- Project [c_last_name#195, c_first_name#194, d_date#142]
            :              +- Filter (((cs_sold_date_sk#72 = d_date_sk#140) && (cs_bill_customer_sk#75 = c_customer_sk#186)) && ((d_month_seq#143 >= 1200) && (d_month_seq#143 <= (1200 + 11))))
            :                 +- Join Inner
            :                    :- Join Inner
            :                    :  :- SubqueryAlias `tpcds`.`catalog_sales`
            :                    :  :  +- Relation[cs_sold_date_sk#72,cs_sold_time_sk#73,cs_ship_date_sk#74,cs_bill_customer_sk#75,cs_bill_cdemo_sk#76,cs_bill_hdemo_sk#77,cs_bill_addr_sk#78,cs_ship_customer_sk#79,cs_ship_cdemo_sk#80,cs_ship_hdemo_sk#81,cs_ship_addr_sk#82,cs_call_center_sk#83,cs_catalog_page_sk#84,cs_ship_mode_sk#85,cs_warehouse_sk#86,cs_item_sk#87,cs_promo_sk#88,cs_order_number#89,cs_quantity#90,cs_wholesale_cost#91,cs_list_price#92,cs_sales_price#93,cs_ext_discount_amt#94,cs_ext_sales_price#95,... 10 more fields] parquet
            :                    :  +- SubqueryAlias `tpcds`.`date_dim`
            :                    :     +- Relation[d_date_sk#140,d_date_id#141,d_date#142,d_month_seq#143,d_week_seq#144,d_quarter_seq#145,d_year#146,d_dow#147,d_moy#148,d_dom#149,d_qoy#150,d_fy_year#151,d_fy_quarter_seq#152,d_fy_week_seq#153,d_day_name#154,d_quarter_name#155,d_holiday#156,d_weekend#157,d_following_holiday#158,d_first_dom#159,d_last_dom#160,d_same_day_ly#161,d_same_day_lq#162,d_current_day#163,... 4 more fields] parquet
            :                    +- SubqueryAlias `tpcds`.`customer`
            :                       +- Relation[c_customer_sk#186,c_customer_id#187,c_current_cdemo_sk#188,c_current_hdemo_sk#189,c_current_addr_sk#190,c_first_shipto_date_sk#191,c_first_sales_date_sk#192,c_salutation#193,c_first_name#194,c_last_name#195,c_preferred_cust_flag#196,c_birth_day#197,c_birth_month#198,c_birth_year#199,c_birth_country#200,c_login#201,c_email_address#202,c_last_review_date#203] parquet
            +- Project [c_last_name#259, c_first_name#258, d_date#206]
               +- Distinct
                  +- Project [c_last_name#259, c_first_name#258, d_date#206]
                     +- Filter (((ws_sold_date_sk#106 = d_date_sk#204) && (ws_bill_customer_sk#110 = c_customer_sk#250)) && ((d_month_seq#207 >= 1200) && (d_month_seq#207 <= (1200 + 11))))
                        +- Join Inner
                           :- Join Inner
                           :  :- SubqueryAlias `tpcds`.`web_sales`
                           :  :  +- Relation[ws_sold_date_sk#106,ws_sold_time_sk#107,ws_ship_date_sk#108,ws_item_sk#109,ws_bill_customer_sk#110,ws_bill_cdemo_sk#111,ws_bill_hdemo_sk#112,ws_bill_addr_sk#113,ws_ship_customer_sk#114,ws_ship_cdemo_sk#115,ws_ship_hdemo_sk#116,ws_ship_addr_sk#117,ws_web_page_sk#118,ws_web_site_sk#119,ws_ship_mode_sk#120,ws_warehouse_sk#121,ws_promo_sk#122,ws_order_number#123,ws_quantity#124,ws_wholesale_cost#125,ws_list_price#126,ws_sales_price#127,ws_ext_discount_amt#128,ws_ext_sales_price#129,... 10 more fields] parquet
                           :  +- SubqueryAlias `tpcds`.`date_dim`
                           :     +- Relation[d_date_sk#204,d_date_id#205,d_date#206,d_month_seq#207,d_week_seq#208,d_quarter_seq#209,d_year#210,d_dow#211,d_moy#212,d_dom#213,d_qoy#214,d_fy_year#215,d_fy_quarter_seq#216,d_fy_week_seq#217,d_day_name#218,d_quarter_name#219,d_holiday#220,d_weekend#221,d_following_holiday#222,d_first_dom#223,d_last_dom#224,d_same_day_ly#225,d_same_day_lq#226,d_current_day#227,... 4 more fields] parquet
                           +- SubqueryAlias `tpcds`.`customer`
                              +- Relation[c_customer_sk#250,c_customer_id#251,c_current_cdemo_sk#252,c_current_hdemo_sk#253,c_current_addr_sk#254,c_first_shipto_date_sk#255,c_first_sales_date_sk#256,c_salutation#257,c_first_name#258,c_last_name#259,c_preferred_cust_flag#260,c_birth_day#261,c_birth_month#262,c_birth_year#263,c_birth_country#264,c_login#265,c_email_address#266,c_last_review_date#267] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Aggregate [count(1) AS count(1)#268L]
      +- Aggregate [c_last_name#63, c_first_name#62, d_date#28]
         +- Join LeftSemi, (((c_last_name#63 <=> c_last_name#259) && (c_first_name#62 <=> c_first_name#258)) && (d_date#28 <=> d_date#206))
            :- Aggregate [c_last_name#63, c_first_name#62, d_date#28], [c_last_name#63, c_first_name#62, d_date#28]
            :  +- Join LeftSemi, (((c_last_name#63 <=> c_last_name#195) && (c_first_name#62 <=> c_first_name#194)) && (d_date#28 <=> d_date#142))
            :     :- Aggregate [c_last_name#63, c_first_name#62, d_date#28], [c_last_name#63, c_first_name#62, d_date#28]
            :     :  +- Project [c_last_name#63, c_first_name#62, d_date#28]
            :     :     +- Join Inner, (ss_customer_sk#6 = c_customer_sk#54)
            :     :        :- Project [ss_customer_sk#6, d_date#28]
            :     :        :  +- Join Inner, (ss_sold_date_sk#3 = d_date_sk#26)
            :     :        :     :- Project [ss_sold_date_sk#3, ss_customer_sk#6]
            :     :        :     :  +- Filter (isnotnull(ss_sold_date_sk#3) && isnotnull(ss_customer_sk#6))
            :     :        :     :     +- Relation[ss_sold_date_sk#3,ss_sold_time_sk#4,ss_item_sk#5,ss_customer_sk#6,ss_cdemo_sk#7,ss_hdemo_sk#8,ss_addr_sk#9,ss_store_sk#10,ss_promo_sk#11,ss_ticket_number#12,ss_quantity#13,ss_wholesale_cost#14,ss_list_price#15,ss_sales_price#16,ss_ext_discount_amt#17,ss_ext_sales_price#18,ss_ext_wholesale_cost#19,ss_ext_list_price#20,ss_ext_tax#21,ss_coupon_amt#22,ss_net_paid#23,ss_net_paid_inc_tax#24,ss_net_profit#25] parquet
            :     :        :     +- Project [d_date_sk#26, d_date#28]
            :     :        :        +- Filter (((isnotnull(d_month_seq#29) && (d_month_seq#29 >= 1200)) && (d_month_seq#29 <= 1211)) && isnotnull(d_date_sk#26))
            :     :        :           +- Relation[d_date_sk#26,d_date_id#27,d_date#28,d_month_seq#29,d_week_seq#30,d_quarter_seq#31,d_year#32,d_dow#33,d_moy#34,d_dom#35,d_qoy#36,d_fy_year#37,d_fy_quarter_seq#38,d_fy_week_seq#39,d_day_name#40,d_quarter_name#41,d_holiday#42,d_weekend#43,d_following_holiday#44,d_first_dom#45,d_last_dom#46,d_same_day_ly#47,d_same_day_lq#48,d_current_day#49,... 4 more fields] parquet
            :     :        +- Project [c_customer_sk#54, c_first_name#62, c_last_name#63]
            :     :           +- Filter isnotnull(c_customer_sk#54)
            :     :              +- Relation[c_customer_sk#54,c_customer_id#55,c_current_cdemo_sk#56,c_current_hdemo_sk#57,c_current_addr_sk#58,c_first_shipto_date_sk#59,c_first_sales_date_sk#60,c_salutation#61,c_first_name#62,c_last_name#63,c_preferred_cust_flag#64,c_birth_day#65,c_birth_month#66,c_birth_year#67,c_birth_country#68,c_login#69,c_email_address#70,c_last_review_date#71] parquet
            :     +- Aggregate [c_last_name#195, c_first_name#194, d_date#142], [c_last_name#195, c_first_name#194, d_date#142]
            :        +- Project [c_last_name#195, c_first_name#194, d_date#142]
            :           +- Join Inner, (cs_bill_customer_sk#75 = c_customer_sk#186)
            :              :- Project [cs_bill_customer_sk#75, d_date#142]
            :              :  +- Join Inner, (cs_sold_date_sk#72 = d_date_sk#140)
            :              :     :- Project [cs_sold_date_sk#72, cs_bill_customer_sk#75]
            :              :     :  +- Filter (isnotnull(cs_sold_date_sk#72) && isnotnull(cs_bill_customer_sk#75))
            :              :     :     +- Relation[cs_sold_date_sk#72,cs_sold_time_sk#73,cs_ship_date_sk#74,cs_bill_customer_sk#75,cs_bill_cdemo_sk#76,cs_bill_hdemo_sk#77,cs_bill_addr_sk#78,cs_ship_customer_sk#79,cs_ship_cdemo_sk#80,cs_ship_hdemo_sk#81,cs_ship_addr_sk#82,cs_call_center_sk#83,cs_catalog_page_sk#84,cs_ship_mode_sk#85,cs_warehouse_sk#86,cs_item_sk#87,cs_promo_sk#88,cs_order_number#89,cs_quantity#90,cs_wholesale_cost#91,cs_list_price#92,cs_sales_price#93,cs_ext_discount_amt#94,cs_ext_sales_price#95,... 10 more fields] parquet
            :              :     +- Project [d_date_sk#140, d_date#142]
            :              :        +- Filter (((isnotnull(d_month_seq#143) && (d_month_seq#143 >= 1200)) && (d_month_seq#143 <= 1211)) && isnotnull(d_date_sk#140))
            :              :           +- Relation[d_date_sk#140,d_date_id#141,d_date#142,d_month_seq#143,d_week_seq#144,d_quarter_seq#145,d_year#146,d_dow#147,d_moy#148,d_dom#149,d_qoy#150,d_fy_year#151,d_fy_quarter_seq#152,d_fy_week_seq#153,d_day_name#154,d_quarter_name#155,d_holiday#156,d_weekend#157,d_following_holiday#158,d_first_dom#159,d_last_dom#160,d_same_day_ly#161,d_same_day_lq#162,d_current_day#163,... 4 more fields] parquet
            :              +- Project [c_customer_sk#186, c_first_name#194, c_last_name#195]
            :                 +- Filter isnotnull(c_customer_sk#186)
            :                    +- Relation[c_customer_sk#186,c_customer_id#187,c_current_cdemo_sk#188,c_current_hdemo_sk#189,c_current_addr_sk#190,c_first_shipto_date_sk#191,c_first_sales_date_sk#192,c_salutation#193,c_first_name#194,c_last_name#195,c_preferred_cust_flag#196,c_birth_day#197,c_birth_month#198,c_birth_year#199,c_birth_country#200,c_login#201,c_email_address#202,c_last_review_date#203] parquet
            +- Aggregate [c_last_name#259, c_first_name#258, d_date#206], [c_last_name#259, c_first_name#258, d_date#206]
               +- Project [c_last_name#259, c_first_name#258, d_date#206]
                  +- Join Inner, (ws_bill_customer_sk#110 = c_customer_sk#250)
                     :- Project [ws_bill_customer_sk#110, d_date#206]
                     :  +- Join Inner, (ws_sold_date_sk#106 = d_date_sk#204)
                     :     :- Project [ws_sold_date_sk#106, ws_bill_customer_sk#110]
                     :     :  +- Filter (isnotnull(ws_sold_date_sk#106) && isnotnull(ws_bill_customer_sk#110))
                     :     :     +- Relation[ws_sold_date_sk#106,ws_sold_time_sk#107,ws_ship_date_sk#108,ws_item_sk#109,ws_bill_customer_sk#110,ws_bill_cdemo_sk#111,ws_bill_hdemo_sk#112,ws_bill_addr_sk#113,ws_ship_customer_sk#114,ws_ship_cdemo_sk#115,ws_ship_hdemo_sk#116,ws_ship_addr_sk#117,ws_web_page_sk#118,ws_web_site_sk#119,ws_ship_mode_sk#120,ws_warehouse_sk#121,ws_promo_sk#122,ws_order_number#123,ws_quantity#124,ws_wholesale_cost#125,ws_list_price#126,ws_sales_price#127,ws_ext_discount_amt#128,ws_ext_sales_price#129,... 10 more fields] parquet
                     :     +- Project [d_date_sk#204, d_date#206]
                     :        +- Filter (((isnotnull(d_month_seq#207) && (d_month_seq#207 >= 1200)) && (d_month_seq#207 <= 1211)) && isnotnull(d_date_sk#204))
                     :           +- Relation[d_date_sk#204,d_date_id#205,d_date#206,d_month_seq#207,d_week_seq#208,d_quarter_seq#209,d_year#210,d_dow#211,d_moy#212,d_dom#213,d_qoy#214,d_fy_year#215,d_fy_quarter_seq#216,d_fy_week_seq#217,d_day_name#218,d_quarter_name#219,d_holiday#220,d_weekend#221,d_following_holiday#222,d_first_dom#223,d_last_dom#224,d_same_day_ly#225,d_same_day_lq#226,d_current_day#227,... 4 more fields] parquet
                     +- Project [c_customer_sk#250, c_first_name#258, c_last_name#259]
                        +- Filter isnotnull(c_customer_sk#250)
                           +- Relation[c_customer_sk#250,c_customer_id#251,c_current_cdemo_sk#252,c_current_hdemo_sk#253,c_current_addr_sk#254,c_first_shipto_date_sk#255,c_first_sales_date_sk#256,c_salutation#257,c_first_name#258,c_last_name#259,c_preferred_cust_flag#260,c_birth_day#261,c_birth_month#262,c_birth_year#263,c_birth_country#264,c_login#265,c_email_address#266,c_last_review_date#267] parquet

== Physical Plan ==
CollectLimit 100
+- *(21) HashAggregate(keys=[], functions=[count(1)], output=[count(1)#268L])
   +- Exchange SinglePartition
      +- *(20) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#270L])
         +- *(20) HashAggregate(keys=[c_last_name#63, c_first_name#62, d_date#28], functions=[], output=[])
            +- Exchange hashpartitioning(c_last_name#63, c_first_name#62, d_date#28, 200)
               +- *(19) HashAggregate(keys=[c_last_name#63, c_first_name#62, d_date#28], functions=[], output=[c_last_name#63, c_first_name#62, d_date#28])
                  +- SortMergeJoin [coalesce(c_last_name#63, ), coalesce(c_first_name#62, ), coalesce(d_date#28, )], [coalesce(c_last_name#259, ), coalesce(c_first_name#258, ), coalesce(d_date#206, )], LeftSemi, (((c_last_name#63 <=> c_last_name#259) && (c_first_name#62 <=> c_first_name#258)) && (d_date#28 <=> d_date#206))
                     :- *(13) Sort [coalesce(c_last_name#63, ) ASC NULLS FIRST, coalesce(c_first_name#62, ) ASC NULLS FIRST, coalesce(d_date#28, ) ASC NULLS FIRST], false, 0
                     :  +- Exchange hashpartitioning(coalesce(c_last_name#63, ), coalesce(c_first_name#62, ), coalesce(d_date#28, ), 200)
                     :     +- *(12) HashAggregate(keys=[c_last_name#63, c_first_name#62, d_date#28], functions=[], output=[c_last_name#63, c_first_name#62, d_date#28])
                     :        +- Exchange hashpartitioning(c_last_name#63, c_first_name#62, d_date#28, 200)
                     :           +- *(11) HashAggregate(keys=[c_last_name#63, c_first_name#62, d_date#28], functions=[], output=[c_last_name#63, c_first_name#62, d_date#28])
                     :              +- SortMergeJoin [coalesce(c_last_name#63, ), coalesce(c_first_name#62, ), coalesce(d_date#28, )], [coalesce(c_last_name#195, ), coalesce(c_first_name#194, ), coalesce(d_date#142, )], LeftSemi, (((c_last_name#63 <=> c_last_name#195) && (c_first_name#62 <=> c_first_name#194)) && (d_date#28 <=> d_date#142))
                     :                 :- *(5) Sort [coalesce(c_last_name#63, ) ASC NULLS FIRST, coalesce(c_first_name#62, ) ASC NULLS FIRST, coalesce(d_date#28, ) ASC NULLS FIRST], false, 0
                     :                 :  +- Exchange hashpartitioning(coalesce(c_last_name#63, ), coalesce(c_first_name#62, ), coalesce(d_date#28, ), 200)
                     :                 :     +- *(4) HashAggregate(keys=[c_last_name#63, c_first_name#62, d_date#28], functions=[], output=[c_last_name#63, c_first_name#62, d_date#28])
                     :                 :        +- Exchange hashpartitioning(c_last_name#63, c_first_name#62, d_date#28, 200)
                     :                 :           +- *(3) HashAggregate(keys=[c_last_name#63, c_first_name#62, d_date#28], functions=[], output=[c_last_name#63, c_first_name#62, d_date#28])
                     :                 :              +- *(3) Project [c_last_name#63, c_first_name#62, d_date#28]
                     :                 :                 +- *(3) BroadcastHashJoin [ss_customer_sk#6], [c_customer_sk#54], Inner, BuildRight
                     :                 :                    :- *(3) Project [ss_customer_sk#6, d_date#28]
                     :                 :                    :  +- *(3) BroadcastHashJoin [ss_sold_date_sk#3], [d_date_sk#26], Inner, BuildRight
                     :                 :                    :     :- *(3) Project [ss_sold_date_sk#3, ss_customer_sk#6]
                     :                 :                    :     :  +- *(3) Filter (isnotnull(ss_sold_date_sk#3) && isnotnull(ss_customer_sk#6))
                     :                 :                    :     :     +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#3,ss_customer_sk#6] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_customer_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int>
                     :                 :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :                 :                    :        +- *(1) Project [d_date_sk#26, d_date#28]
                     :                 :                    :           +- *(1) Filter (((isnotnull(d_month_seq#29) && (d_month_seq#29 >= 1200)) && (d_month_seq#29 <= 1211)) && isnotnull(d_date_sk#26))
                     :                 :                    :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#26,d_date#28,d_month_seq#29] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), GreaterThanOrEqual(d_month_seq,1200), LessThanOrEqual(d_month_seq,1211),..., ReadSchema: struct<d_date_sk:int,d_date:string,d_month_seq:int>
                     :                 :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :                 :                       +- *(2) Project [c_customer_sk#54, c_first_name#62, c_last_name#63]
                     :                 :                          +- *(2) Filter isnotnull(c_customer_sk#54)
                     :                 :                             +- *(2) FileScan parquet tpcds.customer[c_customer_sk#54,c_first_name#62,c_last_name#63] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_first_name:string,c_last_name:string>
                     :                 +- *(10) Sort [coalesce(c_last_name#195, ) ASC NULLS FIRST, coalesce(c_first_name#194, ) ASC NULLS FIRST, coalesce(d_date#142, ) ASC NULLS FIRST], false, 0
                     :                    +- Exchange hashpartitioning(coalesce(c_last_name#195, ), coalesce(c_first_name#194, ), coalesce(d_date#142, ), 200)
                     :                       +- *(9) HashAggregate(keys=[c_last_name#195, c_first_name#194, d_date#142], functions=[], output=[c_last_name#195, c_first_name#194, d_date#142])
                     :                          +- Exchange hashpartitioning(c_last_name#195, c_first_name#194, d_date#142, 200)
                     :                             +- *(8) HashAggregate(keys=[c_last_name#195, c_first_name#194, d_date#142], functions=[], output=[c_last_name#195, c_first_name#194, d_date#142])
                     :                                +- *(8) Project [c_last_name#195, c_first_name#194, d_date#142]
                     :                                   +- *(8) BroadcastHashJoin [cs_bill_customer_sk#75], [c_customer_sk#186], Inner, BuildRight
                     :                                      :- *(8) Project [cs_bill_customer_sk#75, d_date#142]
                     :                                      :  +- *(8) BroadcastHashJoin [cs_sold_date_sk#72], [d_date_sk#140], Inner, BuildRight
                     :                                      :     :- *(8) Project [cs_sold_date_sk#72, cs_bill_customer_sk#75]
                     :                                      :     :  +- *(8) Filter (isnotnull(cs_sold_date_sk#72) && isnotnull(cs_bill_customer_sk#75))
                     :                                      :     :     +- *(8) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#72,cs_bill_customer_sk#75] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_bill_customer_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int>
                     :                                      :     +- ReusedExchange [d_date_sk#140, d_date#142], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :                                      +- ReusedExchange [c_customer_sk#186, c_first_name#194, c_last_name#195], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     +- *(18) Sort [coalesce(c_last_name#259, ) ASC NULLS FIRST, coalesce(c_first_name#258, ) ASC NULLS FIRST, coalesce(d_date#206, ) ASC NULLS FIRST], false, 0
                        +- Exchange hashpartitioning(coalesce(c_last_name#259, ), coalesce(c_first_name#258, ), coalesce(d_date#206, ), 200)
                           +- *(17) HashAggregate(keys=[c_last_name#259, c_first_name#258, d_date#206], functions=[], output=[c_last_name#259, c_first_name#258, d_date#206])
                              +- Exchange hashpartitioning(c_last_name#259, c_first_name#258, d_date#206, 200)
                                 +- *(16) HashAggregate(keys=[c_last_name#259, c_first_name#258, d_date#206], functions=[], output=[c_last_name#259, c_first_name#258, d_date#206])
                                    +- *(16) Project [c_last_name#259, c_first_name#258, d_date#206]
                                       +- *(16) BroadcastHashJoin [ws_bill_customer_sk#110], [c_customer_sk#250], Inner, BuildRight
                                          :- *(16) Project [ws_bill_customer_sk#110, d_date#206]
                                          :  +- *(16) BroadcastHashJoin [ws_sold_date_sk#106], [d_date_sk#204], Inner, BuildRight
                                          :     :- *(16) Project [ws_sold_date_sk#106, ws_bill_customer_sk#110]
                                          :     :  +- *(16) Filter (isnotnull(ws_sold_date_sk#106) && isnotnull(ws_bill_customer_sk#110))
                                          :     :     +- *(16) FileScan parquet tpcds.web_sales[ws_sold_date_sk#106,ws_bill_customer_sk#110] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_customer_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_bill_customer_sk:int>
                                          :     +- ReusedExchange [d_date_sk#204, d_date#206], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                          +- ReusedExchange [c_customer_sk#250, c_first_name#258, c_last_name#259], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 4.411 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 38 in stream 0 using template query38.tpl
------------------------------------------------------^^^

