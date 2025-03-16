Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741580145897
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST, 's_state ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id, 'i_item_desc, 's_state], ['i_item_id, 'i_item_desc, 's_state, 'count('ss_quantity) AS store_sales_quantitycount#0, 'avg('ss_quantity) AS store_sales_quantityave#1, 'stddev_samp('ss_quantity) AS store_sales_quantitystdev#2, ('stddev_samp('ss_quantity) / 'avg('ss_quantity)) AS store_sales_quantitycov#3, 'count('sr_return_quantity) AS store_returns_quantitycount#4, 'avg('sr_return_quantity) AS store_returns_quantityave#5, 'stddev_samp('sr_return_quantity) AS store_returns_quantitystdev#6, ('stddev_samp('sr_return_quantity) / 'avg('sr_return_quantity)) AS store_returns_quantitycov#7, 'count('cs_quantity) AS catalog_sales_quantitycount#8, 'avg('cs_quantity) AS catalog_sales_quantityave#9, 'stddev_samp('cs_quantity) AS catalog_sales_quantitystdev#10, ('stddev_samp('cs_quantity) / 'avg('cs_quantity)) AS catalog_sales_quantitycov#11]
         +- 'Filter ((((('d1.d_quarter_name = 2001Q1) AND ('d1.d_date_sk = 'ss_sold_date_sk)) AND (('i_item_sk = 'ss_item_sk) AND ('s_store_sk = 'ss_store_sk))) AND ((('ss_customer_sk = 'sr_customer_sk) AND ('ss_item_sk = 'sr_item_sk)) AND ('ss_ticket_number = 'sr_ticket_number))) AND (((('sr_returned_date_sk = 'd2.d_date_sk) AND 'd2.d_quarter_name IN (2001Q1,2001Q2,2001Q3)) AND ('sr_customer_sk = 'cs_bill_customer_sk)) AND ((('sr_item_sk = 'cs_item_sk) AND ('cs_sold_date_sk = 'd3.d_date_sk)) AND 'd3.d_quarter_name IN (2001Q1,2001Q2,2001Q3))))
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
i_item_id: string, i_item_desc: string, s_state: string, store_sales_quantitycount: bigint, store_sales_quantityave: double, store_sales_quantitystdev: double, store_sales_quantitycov: double, store_returns_quantitycount: bigint, store_returns_quantityave: double, store_returns_quantitystdev: double, store_returns_quantitycov: double, catalog_sales_quantitycount: bigint, catalog_sales_quantityave: double, catalog_sales_quantitystdev: double, catalog_sales_quantitycov: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#152 ASC NULLS FIRST, i_item_desc#155 ASC NULLS FIRST, s_state#146 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#152, i_item_desc#155, s_state#146], [i_item_id#152, i_item_desc#155, s_state#146, count(ss_quantity#27) AS store_sales_quantitycount#0L, avg(ss_quantity#27) AS store_sales_quantityave#1, stddev_samp(cast(ss_quantity#27 as double)) AS store_sales_quantitystdev#2, (stddev_samp(cast(ss_quantity#27 as double)) / avg(ss_quantity#27)) AS store_sales_quantitycov#3, count(sr_return_quantity#50) AS store_returns_quantitycount#4L, avg(sr_return_quantity#50) AS store_returns_quantityave#5, stddev_samp(cast(sr_return_quantity#50 as double)) AS store_returns_quantitystdev#6, (stddev_samp(cast(sr_return_quantity#50 as double)) / avg(sr_return_quantity#50)) AS store_returns_quantitycov#7, count(cs_quantity#78) AS catalog_sales_quantitycount#8L, avg(cs_quantity#78) AS catalog_sales_quantityave#9, stddev_samp(cast(cs_quantity#78 as double)) AS catalog_sales_quantitystdev#10, (stddev_samp(cast(cs_quantity#78 as double)) / avg(cs_quantity#78)) AS catalog_sales_quantitycov#11]
         +- Filter (((((d_quarter_name#109 = 2001Q1) AND (d_date_sk#94 = ss_sold_date_sk#17)) AND ((i_item_sk#151 = ss_item_sk#19) AND (s_store_sk#122 = ss_store_sk#24))) AND (((ss_customer_sk#20 = sr_customer_sk#43) AND (ss_item_sk#19 = sr_item_sk#42)) AND (ss_ticket_number#26 = sr_ticket_number#49))) AND ((((sr_returned_date_sk#40 = d_date_sk#173) AND d_quarter_name#188 IN (2001Q1,2001Q2,2001Q3)) AND (sr_customer_sk#43 = cs_bill_customer_sk#63)) AND (((sr_item_sk#42 = cs_item_sk#75) AND (cs_sold_date_sk#60 = d_date_sk#201)) AND d_quarter_name#216 IN (2001Q1,2001Q2,2001Q3))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- Join Inner
               :  :  :  :  :- Join Inner
               :  :  :  :  :  :- Join Inner
               :  :  :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :  :  :  :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#17,ss_sold_time_sk#18,ss_item_sk#19,ss_customer_sk#20,ss_cdemo_sk#21,ss_hdemo_sk#22,ss_addr_sk#23,ss_store_sk#24,ss_promo_sk#25,ss_ticket_number#26,ss_quantity#27,ss_wholesale_cost#28,ss_list_price#29,ss_sales_price#30,ss_ext_discount_amt#31,ss_ext_sales_price#32,ss_ext_wholesale_cost#33,ss_ext_list_price#34,ss_ext_tax#35,ss_coupon_amt#36,ss_net_paid#37,ss_net_paid_inc_tax#38,ss_net_profit#39] parquet
               :  :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.store_returns
               :  :  :  :  :  :     +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#40,sr_return_time_sk#41,sr_item_sk#42,sr_customer_sk#43,sr_cdemo_sk#44,sr_hdemo_sk#45,sr_addr_sk#46,sr_store_sk#47,sr_reason_sk#48,sr_ticket_number#49,sr_return_quantity#50,sr_return_amt#51,sr_return_tax#52,sr_return_amt_inc_tax#53,sr_fee#54,sr_return_ship_cost#55,sr_refunded_cash#56,sr_reversed_charge#57,sr_store_credit#58,sr_net_loss#59] parquet
               :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_sales
               :  :  :  :  :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#60,cs_sold_time_sk#61,cs_ship_date_sk#62,cs_bill_customer_sk#63,cs_bill_cdemo_sk#64,cs_bill_hdemo_sk#65,cs_bill_addr_sk#66,cs_ship_customer_sk#67,cs_ship_cdemo_sk#68,cs_ship_hdemo_sk#69,cs_ship_addr_sk#70,cs_call_center_sk#71,cs_catalog_page_sk#72,cs_ship_mode_sk#73,cs_warehouse_sk#74,cs_item_sk#75,cs_promo_sk#76,cs_order_number#77,cs_quantity#78,cs_wholesale_cost#79,cs_list_price#80,cs_sales_price#81,cs_ext_discount_amt#82,cs_ext_sales_price#83,... 10 more fields] parquet
               :  :  :  :  +- SubqueryAlias d1
               :  :  :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :  :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#94,d_date_id#95,d_date#96,d_month_seq#97,d_week_seq#98,d_quarter_seq#99,d_year#100,d_dow#101,d_moy#102,d_dom#103,d_qoy#104,d_fy_year#105,d_fy_quarter_seq#106,d_fy_week_seq#107,d_day_name#108,d_quarter_name#109,d_holiday#110,d_weekend#111,d_following_holiday#112,d_first_dom#113,d_last_dom#114,d_same_day_ly#115,d_same_day_lq#116,d_current_day#117,... 4 more fields] parquet
               :  :  :  +- SubqueryAlias d2
               :  :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#173,d_date_id#174,d_date#175,d_month_seq#176,d_week_seq#177,d_quarter_seq#178,d_year#179,d_dow#180,d_moy#181,d_dom#182,d_qoy#183,d_fy_year#184,d_fy_quarter_seq#185,d_fy_week_seq#186,d_day_name#187,d_quarter_name#188,d_holiday#189,d_weekend#190,d_following_holiday#191,d_first_dom#192,d_last_dom#193,d_same_day_ly#194,d_same_day_lq#195,d_current_day#196,... 4 more fields] parquet
               :  :  +- SubqueryAlias d3
               :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#201,d_date_id#202,d_date#203,d_month_seq#204,d_week_seq#205,d_quarter_seq#206,d_year#207,d_dow#208,d_moy#209,d_dom#210,d_qoy#211,d_fy_year#212,d_fy_quarter_seq#213,d_fy_week_seq#214,d_day_name#215,d_quarter_name#216,d_holiday#217,d_weekend#218,d_following_holiday#219,d_first_dom#220,d_last_dom#221,d_same_day_ly#222,d_same_day_lq#223,d_current_day#224,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.store
               :     +- Relation spark_catalog.tpcds.store[s_store_sk#122,s_store_id#123,s_rec_start_date#124,s_rec_end_date#125,s_closed_date_sk#126,s_store_name#127,s_number_employees#128,s_floor_space#129,s_hours#130,s_manager#131,s_market_id#132,s_geography_class#133,s_market_desc#134,s_market_manager#135,s_division_id#136,s_division_name#137,s_company_id#138,s_company_name#139,s_street_number#140,s_street_name#141,s_street_type#142,s_suite_number#143,s_city#144,s_county#145,... 5 more fields] parquet
               +- SubqueryAlias spark_catalog.tpcds.item
                  +- Relation spark_catalog.tpcds.item[i_item_sk#151,i_item_id#152,i_rec_start_date#153,i_rec_end_date#154,i_item_desc#155,i_current_price#156,i_wholesale_cost#157,i_brand_id#158,i_brand#159,i_class_id#160,i_class#161,i_category_id#162,i_category#163,i_manufact_id#164,i_manufact#165,i_size#166,i_formulation#167,i_color#168,i_units#169,i_container#170,i_manager_id#171,i_product_name#172] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#152 ASC NULLS FIRST, i_item_desc#155 ASC NULLS FIRST, s_state#146 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#152, i_item_desc#155, s_state#146], [i_item_id#152, i_item_desc#155, s_state#146, count(ss_quantity#27) AS store_sales_quantitycount#0L, avg(ss_quantity#27) AS store_sales_quantityave#1, stddev_samp(cast(ss_quantity#27 as double)) AS store_sales_quantitystdev#2, (stddev_samp(cast(ss_quantity#27 as double)) / avg(ss_quantity#27)) AS store_sales_quantitycov#3, count(sr_return_quantity#50) AS store_returns_quantitycount#4L, avg(sr_return_quantity#50) AS store_returns_quantityave#5, stddev_samp(cast(sr_return_quantity#50 as double)) AS store_returns_quantitystdev#6, (stddev_samp(cast(sr_return_quantity#50 as double)) / avg(sr_return_quantity#50)) AS store_returns_quantitycov#7, count(cs_quantity#78) AS catalog_sales_quantitycount#8L, avg(cs_quantity#78) AS catalog_sales_quantityave#9, stddev_samp(cast(cs_quantity#78 as double)) AS catalog_sales_quantitystdev#10, (stddev_samp(cast(cs_quantity#78 as double)) / avg(cs_quantity#78)) AS catalog_sales_quantitycov#11]
         +- Project [ss_quantity#27, sr_return_quantity#50, cs_quantity#78, s_state#146, i_item_id#152, i_item_desc#155]
            +- Join Inner, (i_item_sk#151 = ss_item_sk#19)
               :- Project [ss_item_sk#19, ss_quantity#27, sr_return_quantity#50, cs_quantity#78, s_state#146]
               :  +- Join Inner, (s_store_sk#122 = ss_store_sk#24)
               :     :- Project [ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_return_quantity#50, cs_quantity#78]
               :     :  +- Join Inner, (cs_sold_date_sk#60 = d_date_sk#201)
               :     :     :- Project [ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_return_quantity#50, cs_sold_date_sk#60, cs_quantity#78]
               :     :     :  +- Join Inner, (sr_returned_date_sk#40 = d_date_sk#173)
               :     :     :     :- Project [ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_returned_date_sk#40, sr_return_quantity#50, cs_sold_date_sk#60, cs_quantity#78]
               :     :     :     :  +- Join Inner, (d_date_sk#94 = ss_sold_date_sk#17)
               :     :     :     :     :- Project [ss_sold_date_sk#17, ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_returned_date_sk#40, sr_return_quantity#50, cs_sold_date_sk#60, cs_quantity#78]
               :     :     :     :     :  +- Join Inner, ((sr_customer_sk#43 = cs_bill_customer_sk#63) AND (sr_item_sk#42 = cs_item_sk#75))
               :     :     :     :     :     :- Project [ss_sold_date_sk#17, ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_returned_date_sk#40, sr_item_sk#42, sr_customer_sk#43, sr_return_quantity#50]
               :     :     :     :     :     :  +- Join Inner, (((ss_customer_sk#20 = sr_customer_sk#43) AND (ss_item_sk#19 = sr_item_sk#42)) AND (ss_ticket_number#26 = sr_ticket_number#49))
               :     :     :     :     :     :     :- Project [ss_sold_date_sk#17, ss_item_sk#19, ss_customer_sk#20, ss_store_sk#24, ss_ticket_number#26, ss_quantity#27]
               :     :     :     :     :     :     :  +- Filter (((isnotnull(ss_customer_sk#20) AND isnotnull(ss_item_sk#19)) AND isnotnull(ss_ticket_number#26)) AND (isnotnull(ss_sold_date_sk#17) AND isnotnull(ss_store_sk#24)))
               :     :     :     :     :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#17,ss_sold_time_sk#18,ss_item_sk#19,ss_customer_sk#20,ss_cdemo_sk#21,ss_hdemo_sk#22,ss_addr_sk#23,ss_store_sk#24,ss_promo_sk#25,ss_ticket_number#26,ss_quantity#27,ss_wholesale_cost#28,ss_list_price#29,ss_sales_price#30,ss_ext_discount_amt#31,ss_ext_sales_price#32,ss_ext_wholesale_cost#33,ss_ext_list_price#34,ss_ext_tax#35,ss_coupon_amt#36,ss_net_paid#37,ss_net_paid_inc_tax#38,ss_net_profit#39] parquet
               :     :     :     :     :     :     +- Project [sr_returned_date_sk#40, sr_item_sk#42, sr_customer_sk#43, sr_ticket_number#49, sr_return_quantity#50]
               :     :     :     :     :     :        +- Filter (((isnotnull(sr_customer_sk#43) AND isnotnull(sr_item_sk#42)) AND isnotnull(sr_ticket_number#49)) AND isnotnull(sr_returned_date_sk#40))
               :     :     :     :     :     :           +- Relation spark_catalog.tpcds.store_returns[sr_returned_date_sk#40,sr_return_time_sk#41,sr_item_sk#42,sr_customer_sk#43,sr_cdemo_sk#44,sr_hdemo_sk#45,sr_addr_sk#46,sr_store_sk#47,sr_reason_sk#48,sr_ticket_number#49,sr_return_quantity#50,sr_return_amt#51,sr_return_tax#52,sr_return_amt_inc_tax#53,sr_fee#54,sr_return_ship_cost#55,sr_refunded_cash#56,sr_reversed_charge#57,sr_store_credit#58,sr_net_loss#59] parquet
               :     :     :     :     :     +- Project [cs_sold_date_sk#60, cs_bill_customer_sk#63, cs_item_sk#75, cs_quantity#78]
               :     :     :     :     :        +- Filter ((isnotnull(cs_bill_customer_sk#63) AND isnotnull(cs_item_sk#75)) AND isnotnull(cs_sold_date_sk#60))
               :     :     :     :     :           +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#60,cs_sold_time_sk#61,cs_ship_date_sk#62,cs_bill_customer_sk#63,cs_bill_cdemo_sk#64,cs_bill_hdemo_sk#65,cs_bill_addr_sk#66,cs_ship_customer_sk#67,cs_ship_cdemo_sk#68,cs_ship_hdemo_sk#69,cs_ship_addr_sk#70,cs_call_center_sk#71,cs_catalog_page_sk#72,cs_ship_mode_sk#73,cs_warehouse_sk#74,cs_item_sk#75,cs_promo_sk#76,cs_order_number#77,cs_quantity#78,cs_wholesale_cost#79,cs_list_price#80,cs_sales_price#81,cs_ext_discount_amt#82,cs_ext_sales_price#83,... 10 more fields] parquet
               :     :     :     :     +- Project [d_date_sk#94]
               :     :     :     :        +- Filter ((isnotnull(d_quarter_name#109) AND (d_quarter_name#109 = 2001Q1)) AND isnotnull(d_date_sk#94))
               :     :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#94,d_date_id#95,d_date#96,d_month_seq#97,d_week_seq#98,d_quarter_seq#99,d_year#100,d_dow#101,d_moy#102,d_dom#103,d_qoy#104,d_fy_year#105,d_fy_quarter_seq#106,d_fy_week_seq#107,d_day_name#108,d_quarter_name#109,d_holiday#110,d_weekend#111,d_following_holiday#112,d_first_dom#113,d_last_dom#114,d_same_day_ly#115,d_same_day_lq#116,d_current_day#117,... 4 more fields] parquet
               :     :     :     +- Project [d_date_sk#173]
               :     :     :        +- Filter (d_quarter_name#188 IN (2001Q1,2001Q2,2001Q3) AND isnotnull(d_date_sk#173))
               :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#173,d_date_id#174,d_date#175,d_month_seq#176,d_week_seq#177,d_quarter_seq#178,d_year#179,d_dow#180,d_moy#181,d_dom#182,d_qoy#183,d_fy_year#184,d_fy_quarter_seq#185,d_fy_week_seq#186,d_day_name#187,d_quarter_name#188,d_holiday#189,d_weekend#190,d_following_holiday#191,d_first_dom#192,d_last_dom#193,d_same_day_ly#194,d_same_day_lq#195,d_current_day#196,... 4 more fields] parquet
               :     :     +- Project [d_date_sk#201]
               :     :        +- Filter (d_quarter_name#216 IN (2001Q1,2001Q2,2001Q3) AND isnotnull(d_date_sk#201))
               :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#201,d_date_id#202,d_date#203,d_month_seq#204,d_week_seq#205,d_quarter_seq#206,d_year#207,d_dow#208,d_moy#209,d_dom#210,d_qoy#211,d_fy_year#212,d_fy_quarter_seq#213,d_fy_week_seq#214,d_day_name#215,d_quarter_name#216,d_holiday#217,d_weekend#218,d_following_holiday#219,d_first_dom#220,d_last_dom#221,d_same_day_ly#222,d_same_day_lq#223,d_current_day#224,... 4 more fields] parquet
               :     +- Project [s_store_sk#122, s_state#146]
               :        +- Filter isnotnull(s_store_sk#122)
               :           +- Relation spark_catalog.tpcds.store[s_store_sk#122,s_store_id#123,s_rec_start_date#124,s_rec_end_date#125,s_closed_date_sk#126,s_store_name#127,s_number_employees#128,s_floor_space#129,s_hours#130,s_manager#131,s_market_id#132,s_geography_class#133,s_market_desc#134,s_market_manager#135,s_division_id#136,s_division_name#137,s_company_id#138,s_company_name#139,s_street_number#140,s_street_name#141,s_street_type#142,s_suite_number#143,s_city#144,s_county#145,... 5 more fields] parquet
               +- Project [i_item_sk#151, i_item_id#152, i_item_desc#155]
                  +- Filter isnotnull(i_item_sk#151)
                     +- Relation spark_catalog.tpcds.item[i_item_sk#151,i_item_id#152,i_rec_start_date#153,i_rec_end_date#154,i_item_desc#155,i_current_price#156,i_wholesale_cost#157,i_brand_id#158,i_brand#159,i_class_id#160,i_class#161,i_category_id#162,i_category#163,i_manufact_id#164,i_manufact#165,i_size#166,i_formulation#167,i_color#168,i_units#169,i_container#170,i_manager_id#171,i_product_name#172] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[i_item_id#152 ASC NULLS FIRST,i_item_desc#155 ASC NULLS FIRST,s_state#146 ASC NULLS FIRST], output=[i_item_id#152,i_item_desc#155,s_state#146,store_sales_quantitycount#0L,store_sales_quantityave#1,store_sales_quantitystdev#2,store_sales_quantitycov#3,store_returns_quantitycount#4L,store_returns_quantityave#5,store_returns_quantitystdev#6,store_returns_quantitycov#7,catalog_sales_quantitycount#8L,catalog_sales_quantityave#9,catalog_sales_quantitystdev#10,catalog_sales_quantitycov#11])
   +- HashAggregate(keys=[i_item_id#152, i_item_desc#155, s_state#146], functions=[count(ss_quantity#27), avg(ss_quantity#27), stddev_samp(cast(ss_quantity#27 as double)), count(sr_return_quantity#50), avg(sr_return_quantity#50), stddev_samp(cast(sr_return_quantity#50 as double)), count(cs_quantity#78), avg(cs_quantity#78), stddev_samp(cast(cs_quantity#78 as double))], output=[i_item_id#152, i_item_desc#155, s_state#146, store_sales_quantitycount#0L, store_sales_quantityave#1, store_sales_quantitystdev#2, store_sales_quantitycov#3, store_returns_quantitycount#4L, store_returns_quantityave#5, store_returns_quantitystdev#6, store_returns_quantitycov#7, catalog_sales_quantitycount#8L, catalog_sales_quantityave#9, catalog_sales_quantitystdev#10, catalog_sales_quantitycov#11])
      +- Exchange hashpartitioning(i_item_id#152, i_item_desc#155, s_state#146, 200), ENSURE_REQUIREMENTS, [plan_id=186]
         +- HashAggregate(keys=[i_item_id#152, i_item_desc#155, s_state#146], functions=[partial_count(ss_quantity#27), partial_avg(ss_quantity#27), partial_stddev_samp(cast(ss_quantity#27 as double)), partial_count(sr_return_quantity#50), partial_avg(sr_return_quantity#50), partial_stddev_samp(cast(sr_return_quantity#50 as double)), partial_count(cs_quantity#78), partial_avg(cs_quantity#78), partial_stddev_samp(cast(cs_quantity#78 as double))], output=[i_item_id#152, i_item_desc#155, s_state#146, count#453L, sum#454, count#455L, n#353, avg#354, m2#355, count#456L, sum#457, count#458L, n#369, avg#370, m2#371, count#459L, sum#460, count#461L, n#385, avg#386, m2#387])
            +- Project [ss_quantity#27, sr_return_quantity#50, cs_quantity#78, s_state#146, i_item_id#152, i_item_desc#155]
               +- BroadcastHashJoin [ss_item_sk#19], [i_item_sk#151], Inner, BuildRight, false
                  :- Project [ss_item_sk#19, ss_quantity#27, sr_return_quantity#50, cs_quantity#78, s_state#146]
                  :  +- BroadcastHashJoin [ss_store_sk#24], [s_store_sk#122], Inner, BuildRight, false
                  :     :- Project [ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_return_quantity#50, cs_quantity#78]
                  :     :  +- BroadcastHashJoin [cs_sold_date_sk#60], [d_date_sk#201], Inner, BuildRight, false
                  :     :     :- Project [ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_return_quantity#50, cs_sold_date_sk#60, cs_quantity#78]
                  :     :     :  +- BroadcastHashJoin [sr_returned_date_sk#40], [d_date_sk#173], Inner, BuildRight, false
                  :     :     :     :- Project [ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_returned_date_sk#40, sr_return_quantity#50, cs_sold_date_sk#60, cs_quantity#78]
                  :     :     :     :  +- BroadcastHashJoin [ss_sold_date_sk#17], [d_date_sk#94], Inner, BuildRight, false
                  :     :     :     :     :- Project [ss_sold_date_sk#17, ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_returned_date_sk#40, sr_return_quantity#50, cs_sold_date_sk#60, cs_quantity#78]
                  :     :     :     :     :  +- SortMergeJoin [sr_customer_sk#43, sr_item_sk#42], [cs_bill_customer_sk#63, cs_item_sk#75], Inner
                  :     :     :     :     :     :- Sort [sr_customer_sk#43 ASC NULLS FIRST, sr_item_sk#42 ASC NULLS FIRST], false, 0
                  :     :     :     :     :     :  +- Exchange hashpartitioning(sr_customer_sk#43, sr_item_sk#42, 200), ENSURE_REQUIREMENTS, [plan_id=158]
                  :     :     :     :     :     :     +- Project [ss_sold_date_sk#17, ss_item_sk#19, ss_store_sk#24, ss_quantity#27, sr_returned_date_sk#40, sr_item_sk#42, sr_customer_sk#43, sr_return_quantity#50]
                  :     :     :     :     :     :        +- BroadcastHashJoin [ss_customer_sk#20, ss_item_sk#19, ss_ticket_number#26], [sr_customer_sk#43, sr_item_sk#42, sr_ticket_number#49], Inner, BuildRight, false
                  :     :     :     :     :     :           :- Filter ((((isnotnull(ss_customer_sk#20) AND isnotnull(ss_item_sk#19)) AND isnotnull(ss_ticket_number#26)) AND isnotnull(ss_sold_date_sk#17)) AND isnotnull(ss_store_sk#24))
                  :     :     :     :     :     :           :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#17,ss_item_sk#19,ss_customer_sk#20,ss_store_sk#24,ss_ticket_number#26,ss_quantity#27] Batched: true, DataFilters: [isnotnull(ss_customer_sk#20), isnotnull(ss_item_sk#19), isnotnull(ss_ticket_number#26), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_item_sk), IsNotNull(ss_ticket_number), IsNotNull(ss_sold..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ticket_number:int...
                  :     :     :     :     :     :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[2, int, false], input[1, int, false], input[3, int, false]),false), [plan_id=153]
                  :     :     :     :     :     :              +- Filter (((isnotnull(sr_customer_sk#43) AND isnotnull(sr_item_sk#42)) AND isnotnull(sr_ticket_number#49)) AND isnotnull(sr_returned_date_sk#40))
                  :     :     :     :     :     :                 +- FileScan parquet spark_catalog.tpcds.store_returns[sr_returned_date_sk#40,sr_item_sk#42,sr_customer_sk#43,sr_ticket_number#49,sr_return_quantity#50] Batched: true, DataFilters: [isnotnull(sr_customer_sk#43), isnotnull(sr_item_sk#42), isnotnull(sr_ticket_number#49), isnotnul..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_customer_sk), IsNotNull(sr_item_sk), IsNotNull(sr_ticket_number), IsNotNull(sr_retu..., ReadSchema: struct<sr_returned_date_sk:int,sr_item_sk:int,sr_customer_sk:int,sr_ticket_number:int,sr_return_q...
                  :     :     :     :     :     +- Sort [cs_bill_customer_sk#63 ASC NULLS FIRST, cs_item_sk#75 ASC NULLS FIRST], false, 0
                  :     :     :     :     :        +- Exchange hashpartitioning(cs_bill_customer_sk#63, cs_item_sk#75, 200), ENSURE_REQUIREMENTS, [plan_id=159]
                  :     :     :     :     :           +- Filter ((isnotnull(cs_bill_customer_sk#63) AND isnotnull(cs_item_sk#75)) AND isnotnull(cs_sold_date_sk#60))
                  :     :     :     :     :              +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#60,cs_bill_customer_sk#63,cs_item_sk#75,cs_quantity#78] Batched: true, DataFilters: [isnotnull(cs_bill_customer_sk#63), isnotnull(cs_item_sk#75), isnotnull(cs_sold_date_sk#60)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_bill_customer_sk), IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int,cs_quantity:int>
                  :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=165]
                  :     :     :     :        +- Project [d_date_sk#94]
                  :     :     :     :           +- Filter ((isnotnull(d_quarter_name#109) AND (d_quarter_name#109 = 2001Q1)) AND isnotnull(d_date_sk#94))
                  :     :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#94,d_quarter_name#109] Batched: true, DataFilters: [isnotnull(d_quarter_name#109), (d_quarter_name#109 = 2001Q1), isnotnull(d_date_sk#94)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_quarter_name), EqualTo(d_quarter_name,2001Q1), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_quarter_name:string>
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=169]
                  :     :     :        +- Project [d_date_sk#173]
                  :     :     :           +- Filter (d_quarter_name#188 IN (2001Q1,2001Q2,2001Q3) AND isnotnull(d_date_sk#173))
                  :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#173,d_quarter_name#188] Batched: true, DataFilters: [d_quarter_name#188 IN (2001Q1,2001Q2,2001Q3), isnotnull(d_date_sk#173)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_quarter_name, [2001Q1,2001Q2,2001Q3]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_quarter_name:string>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=173]
                  :     :        +- Project [d_date_sk#201]
                  :     :           +- Filter (d_quarter_name#216 IN (2001Q1,2001Q2,2001Q3) AND isnotnull(d_date_sk#201))
                  :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#201,d_quarter_name#216] Batched: true, DataFilters: [d_quarter_name#216 IN (2001Q1,2001Q2,2001Q3), isnotnull(d_date_sk#201)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [In(d_quarter_name, [2001Q1,2001Q2,2001Q3]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_quarter_name:string>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=177]
                  :        +- Filter isnotnull(s_store_sk#122)
                  :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#122,s_state#146] Batched: true, DataFilters: [isnotnull(s_store_sk#122)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_state:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=181]
                     +- Filter isnotnull(i_item_sk#151)
                        +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#151,i_item_id#152,i_item_desc#155] Batched: true, DataFilters: [isnotnull(i_item_sk#151)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string>

Time taken: 3.001 seconds, Fetched 1 row(s)
