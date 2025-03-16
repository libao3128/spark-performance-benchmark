Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580659724
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST, 's_store_id ASC NULLS FIRST, 's_store_name ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id, 'i_item_desc, 's_store_id, 's_store_name], ['i_item_id, 'i_item_desc, 's_store_id, 's_store_name, 'sum('ss_quantity) AS store_sales_quantity#0, 'sum('sr_return_quantity) AS store_returns_quantity#1, 'sum('cs_quantity) AS catalog_sales_quantity#2]
         +- 'Filter ((((('d1.d_moy = 9) AND ('d1.d_year = 1999)) AND (('d1.d_date_sk = 'ss_sold_date_sk) AND ('i_item_sk = 'ss_item_sk))) AND ((('s_store_sk = 'ss_store_sk) AND ('ss_customer_sk = 'sr_customer_sk)) AND (('ss_item_sk = 'sr_item_sk) AND ('ss_ticket_number = 'sr_ticket_number)))) AND (((('sr_returned_date_sk = 'd2.d_date_sk) AND (('d2.d_moy >= 9) AND ('d2.d_moy <= (9 + 3)))) AND (('d2.d_year = 1999) AND ('sr_customer_sk = 'cs_bill_customer_sk))) AND ((('sr_item_sk = 'cs_item_sk) AND ('cs_sold_date_sk = 'd3.d_date_sk)) AND 'd3.d_year IN (1999,(1999 + 1),(1999 + 2)))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :  :- 'UnresolvedRelation [store_sales], [], false
               :  :  :  :  :  :  +- 'UnresolvedRelation [store_returns], [], false
               :  :  :  :  :  +- 'UnresolvedRelation [catalog_sales], [], false
               :  :  :  :  +- 'SubqueryAlias d1
               :  :  :  :     +- 'UnresolvedRelation [date_dim], [], false
               :  :  :  +- 'SubqueryAlias d2
               :  :  :     +- 'UnresolvedRelation [date_dim], [], false
               :  :  +- 'SubqueryAlias d3
               :  :     +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [store], [], false
               +- 'UnresolvedRelation [item], [], false

== Analyzed Logical Plan ==
i_item_id: string, i_item_desc: string, s_store_id: string, s_store_name: string, store_sales_quantity: bigint, store_returns_quantity: bigint, catalog_sales_quantity: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#143 ASC NULLS FIRST, i_item_desc#146 ASC NULLS FIRST, s_store_id#114 ASC NULLS FIRST, s_store_name#118 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#143, i_item_desc#146, s_store_id#114, s_store_name#118], [i_item_id#143, i_item_desc#146, s_store_id#114, s_store_name#118, sum(ss_quantity#18) AS store_sales_quantity#0L, sum(sr_return_quantity#41) AS store_returns_quantity#1L, sum(cs_quantity#69) AS catalog_sales_quantity#2L]
         +- Filter (((((d_moy#93 = 9) AND (d_year#91 = 1999)) AND ((d_date_sk#85 = ss_sold_date_sk#8) AND (i_item_sk#142 = ss_item_sk#10))) AND (((s_store_sk#113 = ss_store_sk#15) AND (ss_customer_sk#11 = sr_customer_sk#34)) AND ((ss_item_sk#10 = sr_item_sk#33) AND (ss_ticket_number#17 = sr_ticket_number#40)))) AND ((((sr_returned_date_sk#31 = d_date_sk#164) AND ((d_moy#172 >= 9) AND (d_moy#172 <= (9 + 3)))) AND ((d_year#170 = 1999) AND (sr_customer_sk#34 = cs_bill_customer_sk#54))) AND (((sr_item_sk#33 = cs_item_sk#66) AND (cs_sold_date_sk#51 = d_date_sk#192)) AND d_year#198 IN (1999,(1999 + 1),(1999 + 2)))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- Join Inner
               :  :  :  :  :- Join Inner
               :  :  :  :  :  :- Join Inner
               :  :  :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :  :  :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
               :  :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.store_returns
               :  :  :  :  :  :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#31,sr_return_time_sk#32,sr_item_sk#33,sr_customer_sk#34,sr_cdemo_sk#35,sr_hdemo_sk#36,sr_addr_sk#37,sr_store_sk#38,sr_reason_sk#39,sr_ticket_number#40,sr_return_quantity#41,sr_return_amt#42,sr_return_tax#43,sr_return_amt_inc_tax#44,sr_fee#45,sr_return_ship_cost#46,sr_refunded_cash#47,sr_reversed_charge#48,sr_store_credit#49,sr_net_loss#50] parquet
               :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_sales
               :  :  :  :  :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#51,cs_sold_time_sk#52,cs_ship_date_sk#53,cs_bill_customer_sk#54,cs_bill_cdemo_sk#55,cs_bill_hdemo_sk#56,cs_bill_addr_sk#57,cs_ship_customer_sk#58,cs_ship_cdemo_sk#59,cs_ship_hdemo_sk#60,cs_ship_addr_sk#61,cs_call_center_sk#62,cs_catalog_page_sk#63,cs_ship_mode_sk#64,cs_warehouse_sk#65,cs_item_sk#66,cs_promo_sk#67,cs_order_number#68,cs_quantity#69,cs_wholesale_cost#70,cs_list_price#71,cs_sales_price#72,cs_ext_discount_amt#73,cs_ext_sales_price#74,... 10 more fields] parquet
               :  :  :  :  +- SubqueryAlias d1
               :  :  :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :  :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#85,d_date_id#86,d_date#87,d_month_seq#88,d_week_seq#89,d_quarter_seq#90,d_year#91,d_dow#92,d_moy#93,d_dom#94,d_qoy#95,d_fy_year#96,d_fy_quarter_seq#97,d_fy_week_seq#98,d_day_name#99,d_quarter_name#100,d_holiday#101,d_weekend#102,d_following_holiday#103,d_first_dom#104,d_last_dom#105,d_same_day_ly#106,d_same_day_lq#107,d_current_day#108,... 4 more fields] parquet
               :  :  :  +- SubqueryAlias d2
               :  :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#164,d_date_id#165,d_date#166,d_month_seq#167,d_week_seq#168,d_quarter_seq#169,d_year#170,d_dow#171,d_moy#172,d_dom#173,d_qoy#174,d_fy_year#175,d_fy_quarter_seq#176,d_fy_week_seq#177,d_day_name#178,d_quarter_name#179,d_holiday#180,d_weekend#181,d_following_holiday#182,d_first_dom#183,d_last_dom#184,d_same_day_ly#185,d_same_day_lq#186,d_current_day#187,... 4 more fields] parquet
               :  :  +- SubqueryAlias d3
               :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#192,d_date_id#193,d_date#194,d_month_seq#195,d_week_seq#196,d_quarter_seq#197,d_year#198,d_dow#199,d_moy#200,d_dom#201,d_qoy#202,d_fy_year#203,d_fy_quarter_seq#204,d_fy_week_seq#205,d_day_name#206,d_quarter_name#207,d_holiday#208,d_weekend#209,d_following_holiday#210,d_first_dom#211,d_last_dom#212,d_same_day_ly#213,d_same_day_lq#214,d_current_day#215,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.store
               :     +- Relation spark_catalog.tpcds.store[s_store_sk#113,s_store_id#114,s_rec_start_date#115,s_rec_end_date#116,s_closed_date_sk#117,s_store_name#118,s_number_employees#119,s_floor_space#120,s_hours#121,s_manager#122,s_market_id#123,s_geography_class#124,s_market_desc#125,s_market_manager#126,s_division_id#127,s_division_name#128,s_company_id#129,s_company_name#130,s_street_number#131,s_street_name#132,s_street_type#133,s_suite_number#134,s_city#135,s_county#136,... 5 more fields] parquet
               +- SubqueryAlias spark_catalog.tpcds.item
                  +- Relation spark_catalog.tpcds.item[i_item_sk#142,i_item_id#143,i_rec_start_date#144,i_rec_end_date#145,i_item_desc#146,i_current_price#147,i_wholesale_cost#148,i_brand_id#149,i_brand#150,i_class_id#151,i_class#152,i_category_id#153,i_category#154,i_manufact_id#155,i_manufact#156,i_size#157,i_formulation#158,i_color#159,i_units#160,i_container#161,i_manager_id#162,i_product_name#163] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#143 ASC NULLS FIRST, i_item_desc#146 ASC NULLS FIRST, s_store_id#114 ASC NULLS FIRST, s_store_name#118 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#143, i_item_desc#146, s_store_id#114, s_store_name#118], [i_item_id#143, i_item_desc#146, s_store_id#114, s_store_name#118, sum(ss_quantity#18) AS store_sales_quantity#0L, sum(sr_return_quantity#41) AS store_returns_quantity#1L, sum(cs_quantity#69) AS catalog_sales_quantity#2L]
         +- Project [ss_quantity#18, sr_return_quantity#41, cs_quantity#69, s_store_id#114, s_store_name#118, i_item_id#143, i_item_desc#146]
            +- Join Inner, (i_item_sk#142 = ss_item_sk#10)
               :- Project [ss_item_sk#10, ss_quantity#18, sr_return_quantity#41, cs_quantity#69, s_store_id#114, s_store_name#118]
               :  +- Join Inner, (s_store_sk#113 = ss_store_sk#15)
               :     :- Project [ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_return_quantity#41, cs_quantity#69]
               :     :  +- Join Inner, (cs_sold_date_sk#51 = d_date_sk#192)
               :     :     :- Project [ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_return_quantity#41, cs_sold_date_sk#51, cs_quantity#69]
               :     :     :  +- Join Inner, (sr_returned_date_sk#31 = d_date_sk#164)
               :     :     :     :- Project [ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_returned_date_sk#31, sr_return_quantity#41, cs_sold_date_sk#51, cs_quantity#69]
               :     :     :     :  +- Join Inner, (d_date_sk#85 = ss_sold_date_sk#8)
               :     :     :     :     :- Project [ss_sold_date_sk#8, ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_returned_date_sk#31, sr_return_quantity#41, cs_sold_date_sk#51, cs_quantity#69]
               :     :     :     :     :  +- Join Inner, ((sr_customer_sk#34 = cs_bill_customer_sk#54) AND (sr_item_sk#33 = cs_item_sk#66))
               :     :     :     :     :     :- Project [ss_sold_date_sk#8, ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_returned_date_sk#31, sr_item_sk#33, sr_customer_sk#34, sr_return_quantity#41]
               :     :     :     :     :     :  +- Join Inner, (((ss_customer_sk#11 = sr_customer_sk#34) AND (ss_item_sk#10 = sr_item_sk#33)) AND (ss_ticket_number#17 = sr_ticket_number#40))
               :     :     :     :     :     :     :- Project [ss_sold_date_sk#8, ss_item_sk#10, ss_customer_sk#11, ss_store_sk#15, ss_ticket_number#17, ss_quantity#18]
               :     :     :     :     :     :     :  +- Filter (((isnotnull(ss_customer_sk#11) AND isnotnull(ss_item_sk#10)) AND isnotnull(ss_ticket_number#17)) AND (isnotnull(ss_sold_date_sk#8) AND isnotnull(ss_store_sk#15)))
               :     :     :     :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#8,ss_sold_time_sk#9,ss_item_sk#10,ss_customer_sk#11,ss_cdemo_sk#12,ss_hdemo_sk#13,ss_addr_sk#14,ss_store_sk#15,ss_promo_sk#16,ss_ticket_number#17,ss_quantity#18,ss_wholesale_cost#19,ss_list_price#20,ss_sales_price#21,ss_ext_discount_amt#22,ss_ext_sales_price#23,ss_ext_wholesale_cost#24,ss_ext_list_price#25,ss_ext_tax#26,ss_coupon_amt#27,ss_net_paid#28,ss_net_paid_inc_tax#29,ss_net_profit#30] parquet
               :     :     :     :     :     :     +- Project [sr_returned_date_sk#31, sr_item_sk#33, sr_customer_sk#34, sr_ticket_number#40, sr_return_quantity#41]
               :     :     :     :     :     :        +- Filter (((isnotnull(sr_customer_sk#34) AND isnotnull(sr_item_sk#33)) AND isnotnull(sr_ticket_number#40)) AND isnotnull(sr_returned_date_sk#31))
               :     :     :     :     :     :           +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#31,sr_return_time_sk#32,sr_item_sk#33,sr_customer_sk#34,sr_cdemo_sk#35,sr_hdemo_sk#36,sr_addr_sk#37,sr_store_sk#38,sr_reason_sk#39,sr_ticket_number#40,sr_return_quantity#41,sr_return_amt#42,sr_return_tax#43,sr_return_amt_inc_tax#44,sr_fee#45,sr_return_ship_cost#46,sr_refunded_cash#47,sr_reversed_charge#48,sr_store_credit#49,sr_net_loss#50] parquet
               :     :     :     :     :     +- Project [cs_sold_date_sk#51, cs_bill_customer_sk#54, cs_item_sk#66, cs_quantity#69]
               :     :     :     :     :        +- Filter ((isnotnull(cs_bill_customer_sk#54) AND isnotnull(cs_item_sk#66)) AND isnotnull(cs_sold_date_sk#51))
               :     :     :     :     :           +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#51,cs_sold_time_sk#52,cs_ship_date_sk#53,cs_bill_customer_sk#54,cs_bill_cdemo_sk#55,cs_bill_hdemo_sk#56,cs_bill_addr_sk#57,cs_ship_customer_sk#58,cs_ship_cdemo_sk#59,cs_ship_hdemo_sk#60,cs_ship_addr_sk#61,cs_call_center_sk#62,cs_catalog_page_sk#63,cs_ship_mode_sk#64,cs_warehouse_sk#65,cs_item_sk#66,cs_promo_sk#67,cs_order_number#68,cs_quantity#69,cs_wholesale_cost#70,cs_list_price#71,cs_sales_price#72,cs_ext_discount_amt#73,cs_ext_sales_price#74,... 10 more fields] parquet
               :     :     :     :     +- Project [d_date_sk#85]
               :     :     :     :        +- Filter (((isnotnull(d_moy#93) AND isnotnull(d_year#91)) AND ((d_moy#93 = 9) AND (d_year#91 = 1999))) AND isnotnull(d_date_sk#85))
               :     :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#85,d_date_id#86,d_date#87,d_month_seq#88,d_week_seq#89,d_quarter_seq#90,d_year#91,d_dow#92,d_moy#93,d_dom#94,d_qoy#95,d_fy_year#96,d_fy_quarter_seq#97,d_fy_week_seq#98,d_day_name#99,d_quarter_name#100,d_holiday#101,d_weekend#102,d_following_holiday#103,d_first_dom#104,d_last_dom#105,d_same_day_ly#106,d_same_day_lq#107,d_current_day#108,... 4 more fields] parquet
               :     :     :     +- Project [d_date_sk#164]
               :     :     :        +- Filter (((isnotnull(d_moy#172) AND isnotnull(d_year#170)) AND (((d_moy#172 >= 9) AND (d_moy#172 <= 12)) AND (d_year#170 = 1999))) AND isnotnull(d_date_sk#164))
               :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#164,d_date_id#165,d_date#166,d_month_seq#167,d_week_seq#168,d_quarter_seq#169,d_year#170,d_dow#171,d_moy#172,d_dom#173,d_qoy#174,d_fy_year#175,d_fy_quarter_seq#176,d_fy_week_seq#177,d_day_name#178,d_quarter_name#179,d_holiday#180,d_weekend#181,d_following_holiday#182,d_first_dom#183,d_last_dom#184,d_same_day_ly#185,d_same_day_lq#186,d_current_day#187,... 4 more fields] parquet
               :     :     +- Project [d_date_sk#192]
               :     :        +- Filter (d_year#198 IN (1999,2000,2001) AND isnotnull(d_date_sk#192))
               :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#192,d_date_id#193,d_date#194,d_month_seq#195,d_week_seq#196,d_quarter_seq#197,d_year#198,d_dow#199,d_moy#200,d_dom#201,d_qoy#202,d_fy_year#203,d_fy_quarter_seq#204,d_fy_week_seq#205,d_day_name#206,d_quarter_name#207,d_holiday#208,d_weekend#209,d_following_holiday#210,d_first_dom#211,d_last_dom#212,d_same_day_ly#213,d_same_day_lq#214,d_current_day#215,... 4 more fields] parquet
               :     +- Project [s_store_sk#113, s_store_id#114, s_store_name#118]
               :        +- Filter isnotnull(s_store_sk#113)
               :           +- Relation spark_catalog.tpcds.store[s_store_sk#113,s_store_id#114,s_rec_start_date#115,s_rec_end_date#116,s_closed_date_sk#117,s_store_name#118,s_number_employees#119,s_floor_space#120,s_hours#121,s_manager#122,s_market_id#123,s_geography_class#124,s_market_desc#125,s_market_manager#126,s_division_id#127,s_division_name#128,s_company_id#129,s_company_name#130,s_street_number#131,s_street_name#132,s_street_type#133,s_suite_number#134,s_city#135,s_county#136,... 5 more fields] parquet
               +- Project [i_item_sk#142, i_item_id#143, i_item_desc#146]
                  +- Filter isnotnull(i_item_sk#142)
                     +- Relation spark_catalog.tpcds.item[i_item_sk#142,i_item_id#143,i_rec_start_date#144,i_rec_end_date#145,i_item_desc#146,i_current_price#147,i_wholesale_cost#148,i_brand_id#149,i_brand#150,i_class_id#151,i_class#152,i_category_id#153,i_category#154,i_manufact_id#155,i_manufact#156,i_size#157,i_formulation#158,i_color#159,i_units#160,i_container#161,i_manager_id#162,i_product_name#163] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[i_item_id#143 ASC NULLS FIRST,i_item_desc#146 ASC NULLS FIRST,s_store_id#114 ASC NULLS FIRST,s_store_name#118 ASC NULLS FIRST], output=[i_item_id#143,i_item_desc#146,s_store_id#114,s_store_name#118,store_sales_quantity#0L,store_returns_quantity#1L,catalog_sales_quantity#2L])
   +- HashAggregate(keys=[i_item_id#143, i_item_desc#146, s_store_id#114, s_store_name#118], functions=[sum(ss_quantity#18), sum(sr_return_quantity#41), sum(cs_quantity#69)], output=[i_item_id#143, i_item_desc#146, s_store_id#114, s_store_name#118, store_sales_quantity#0L, store_returns_quantity#1L, catalog_sales_quantity#2L])
      +- Exchange hashpartitioning(i_item_id#143, i_item_desc#146, s_store_id#114, s_store_name#118, 200), ENSURE_REQUIREMENTS, [plan_id=186]
         +- HashAggregate(keys=[i_item_id#143, i_item_desc#146, s_store_id#114, s_store_name#118], functions=[partial_sum(ss_quantity#18), partial_sum(sr_return_quantity#41), partial_sum(cs_quantity#69)], output=[i_item_id#143, i_item_desc#146, s_store_id#114, s_store_name#118, sum#234L, sum#235L, sum#236L])
            +- Project [ss_quantity#18, sr_return_quantity#41, cs_quantity#69, s_store_id#114, s_store_name#118, i_item_id#143, i_item_desc#146]
               +- BroadcastHashJoin [ss_item_sk#10], [i_item_sk#142], Inner, BuildRight, false
                  :- Project [ss_item_sk#10, ss_quantity#18, sr_return_quantity#41, cs_quantity#69, s_store_id#114, s_store_name#118]
                  :  +- BroadcastHashJoin [ss_store_sk#15], [s_store_sk#113], Inner, BuildRight, false
                  :     :- Project [ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_return_quantity#41, cs_quantity#69]
                  :     :  +- BroadcastHashJoin [cs_sold_date_sk#51], [d_date_sk#192], Inner, BuildRight, false
                  :     :     :- Project [ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_return_quantity#41, cs_sold_date_sk#51, cs_quantity#69]
                  :     :     :  +- BroadcastHashJoin [sr_returned_date_sk#31], [d_date_sk#164], Inner, BuildRight, false
                  :     :     :     :- Project [ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_returned_date_sk#31, sr_return_quantity#41, cs_sold_date_sk#51, cs_quantity#69]
                  :     :     :     :  +- BroadcastHashJoin [ss_sold_date_sk#8], [d_date_sk#85], Inner, BuildRight, false
                  :     :     :     :     :- Project [ss_sold_date_sk#8, ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_returned_date_sk#31, sr_return_quantity#41, cs_sold_date_sk#51, cs_quantity#69]
                  :     :     :     :     :  +- SortMergeJoin [sr_customer_sk#34, sr_item_sk#33], [cs_bill_customer_sk#54, cs_item_sk#66], Inner
                  :     :     :     :     :     :- Sort [sr_customer_sk#34 ASC NULLS FIRST, sr_item_sk#33 ASC NULLS FIRST], false, 0
                  :     :     :     :     :     :  +- Exchange hashpartitioning(sr_customer_sk#34, sr_item_sk#33, 200), ENSURE_REQUIREMENTS, [plan_id=158]
                  :     :     :     :     :     :     +- Project [ss_sold_date_sk#8, ss_item_sk#10, ss_store_sk#15, ss_quantity#18, sr_returned_date_sk#31, sr_item_sk#33, sr_customer_sk#34, sr_return_quantity#41]
                  :     :     :     :     :     :        +- BroadcastHashJoin [ss_customer_sk#11, ss_item_sk#10, ss_ticket_number#17], [sr_customer_sk#34, sr_item_sk#33, sr_ticket_number#40], Inner, BuildRight, false
                  :     :     :     :     :     :           :- Filter ((((isnotnull(ss_customer_sk#11) AND isnotnull(ss_item_sk#10)) AND isnotnull(ss_ticket_number#17)) AND isnotnull(ss_sold_date_sk#8)) AND isnotnull(ss_store_sk#15))
                  :     :     :     :     :     :           :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#8,ss_item_sk#10,ss_customer_sk#11,ss_store_sk#15,ss_ticket_number#17,ss_quantity#18] Batched: true, DataFilters: [isnotnull(ss_customer_sk#11), isnotnull(ss_item_sk#10), isnotnull(ss_ticket_number#17), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_item_sk), IsNotNull(ss_ticket_number), IsNotNull(ss_sold..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ticket_number:int...
                  :     :     :     :     :     :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[2, int, false], input[1, int, false], input[3, int, false]),false), [plan_id=153]
                  :     :     :     :     :     :              +- Filter (((isnotnull(sr_customer_sk#34) AND isnotnull(sr_item_sk#33)) AND isnotnull(sr_ticket_number#40)) AND isnotnull(sr_returned_date_sk#31))
                  :     :     :     :     :     :                 +- FileScan parquet spark_catalog.tpcds.store_returns[sr_returned_date_sk#31,sr_item_sk#33,sr_customer_sk#34,sr_ticket_number#40,sr_return_quantity#41] Batched: true, DataFilters: [isnotnull(sr_customer_sk#34), isnotnull(sr_item_sk#33), isnotnull(sr_ticket_number#40), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_customer_sk), IsNotNull(sr_item_sk), IsNotNull(sr_ticket_number), IsNotNull(sr_retu..., ReadSchema: struct<sr_returned_date_sk:int,sr_item_sk:int,sr_customer_sk:int,sr_ticket_number:int,sr_return_q...
                  :     :     :     :     :     +- Sort [cs_bill_customer_sk#54 ASC NULLS FIRST, cs_item_sk#66 ASC NULLS FIRST], false, 0
                  :     :     :     :     :        +- Exchange hashpartitioning(cs_bill_customer_sk#54, cs_item_sk#66, 200), ENSURE_REQUIREMENTS, [plan_id=159]
                  :     :     :     :     :           +- Filter ((isnotnull(cs_bill_customer_sk#54) AND isnotnull(cs_item_sk#66)) AND isnotnull(cs_sold_date_sk#51))
                  :     :     :     :     :              +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#51,cs_bill_customer_sk#54,cs_item_sk#66,cs_quantity#69] Batched: true, DataFilters: [isnotnull(cs_bill_customer_sk#54), isnotnull(cs_item_sk#66), isnotnull(cs_sold_date_sk#51)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_bill_customer_sk), IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int,cs_quantity:int>
                  :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=165]
                  :     :     :     :        +- Project [d_date_sk#85]
                  :     :     :     :           +- Filter ((((isnotnull(d_moy#93) AND isnotnull(d_year#91)) AND (d_moy#93 = 9)) AND (d_year#91 = 1999)) AND isnotnull(d_date_sk#85))
                  :     :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#85,d_year#91,d_moy#93] Batched: true, DataFilters: [isnotnull(d_moy#93), isnotnull(d_year#91), (d_moy#93 = 9), (d_year#91 = 1999), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,9), EqualTo(d_year,1999), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=169]
                  :     :     :        +- Project [d_date_sk#164]
                  :     :     :           +- Filter (((((isnotnull(d_moy#172) AND isnotnull(d_year#170)) AND (d_moy#172 >= 9)) AND (d_moy#172 <= 12)) AND (d_year#170 = 1999)) AND isnotnull(d_date_sk#164))
                  :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#164,d_year#170,d_moy#172] Batched: true, DataFilters: [isnotnull(d_moy#172), isnotnull(d_year#170), (d_moy#172 >= 9), (d_moy#172 <= 12), (d_year#170 = ..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), GreaterThanOrEqual(d_moy,9), LessThanOrEqual(d_moy,12), Equ..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=173]
                  :     :        +- Project [d_date_sk#192]
                  :     :           +- Filter (d_year#198 IN (1999,2000,2001) AND isnotnull(d_date_sk#192))
                  :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#192,d_year#198] Batched: true, DataFilters: [d_year#198 IN (1999,2000,2001), isnotnull(d_date_sk#192)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_year, [1999,2000,2001]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=177]
                  :        +- Filter isnotnull(s_store_sk#113)
                  :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#113,s_store_id#114,s_store_name#118] Batched: true, DataFilters: [isnotnull(s_store_sk#113)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_id:string,s_store_name:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=181]
                     +- Filter isnotnull(i_item_sk#142)
                        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#142,i_item_id#143,i_item_desc#146] Batched: true, DataFilters: [isnotnull(i_item_sk#142)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string>

Time taken: 3.126 seconds, Fetched 1 row(s)
