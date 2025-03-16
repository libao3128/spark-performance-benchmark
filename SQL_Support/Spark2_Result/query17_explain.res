== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST, 's_state ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id, 'i_item_desc, 's_state], ['i_item_id, 'i_item_desc, 's_state, 'count('ss_quantity) AS store_sales_quantitycount#0, 'avg('ss_quantity) AS store_sales_quantityave#1, 'stddev_samp('ss_quantity) AS store_sales_quantitystdev#2, ('stddev_samp('ss_quantity) / 'avg('ss_quantity)) AS store_sales_quantitycov#3, 'count('sr_return_quantity) AS store_returns_quantitycount#4, 'avg('sr_return_quantity) AS store_returns_quantityave#5, 'stddev_samp('sr_return_quantity) AS store_returns_quantitystdev#6, ('stddev_samp('sr_return_quantity) / 'avg('sr_return_quantity)) AS store_returns_quantitycov#7, 'count('cs_quantity) AS catalog_sales_quantitycount#8, 'avg('cs_quantity) AS catalog_sales_quantityave#9, 'stddev_samp('cs_quantity) AS catalog_sales_quantitystdev#10, ('stddev_samp('cs_quantity) / 'avg('cs_quantity)) AS catalog_sales_quantitycov#11]
         +- 'Filter ((((('d1.d_quarter_name = 2001Q1) && ('d1.d_date_sk = 'ss_sold_date_sk)) && (('i_item_sk = 'ss_item_sk) && ('s_store_sk = 'ss_store_sk))) && ((('ss_customer_sk = 'sr_customer_sk) && ('ss_item_sk = 'sr_item_sk)) && ('ss_ticket_number = 'sr_ticket_number))) && (((('sr_returned_date_sk = 'd2.d_date_sk) && 'd2.d_quarter_name IN (2001Q1,2001Q2,2001Q3)) && ('sr_customer_sk = 'cs_bill_customer_sk)) && ((('sr_item_sk = 'cs_item_sk) && ('cs_sold_date_sk = 'd3.d_date_sk)) && 'd3.d_quarter_name IN (2001Q1,2001Q2,2001Q3))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :  :- 'UnresolvedRelation `store_sales`
               :  :  :  :  :  :  +- 'UnresolvedRelation `store_returns`
               :  :  :  :  :  +- 'UnresolvedRelation `catalog_sales`
               :  :  :  :  +- 'SubqueryAlias `d1`
               :  :  :  :     +- 'UnresolvedRelation `date_dim`
               :  :  :  +- 'SubqueryAlias `d2`
               :  :  :     +- 'UnresolvedRelation `date_dim`
               :  :  +- 'SubqueryAlias `d3`
               :  :     +- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `store`
               +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
i_item_id: string, i_item_desc: string, s_state: string, store_sales_quantitycount: bigint, store_sales_quantityave: double, store_sales_quantitystdev: double, store_sales_quantitycov: double, store_returns_quantitycount: bigint, store_returns_quantityave: double, store_returns_quantitystdev: double, store_returns_quantitycov: double, catalog_sales_quantitycount: bigint, catalog_sales_quantityave: double, catalog_sales_quantitystdev: double, catalog_sales_quantitycov: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#149 ASC NULLS FIRST, i_item_desc#152 ASC NULLS FIRST, s_state#143 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#149, i_item_desc#152, s_state#143], [i_item_id#149, i_item_desc#152, s_state#143, count(ss_quantity#24) AS store_sales_quantitycount#0L, avg(cast(ss_quantity#24 as bigint)) AS store_sales_quantityave#1, stddev_samp(cast(ss_quantity#24 as double)) AS store_sales_quantitystdev#2, (stddev_samp(cast(ss_quantity#24 as double)) / avg(cast(ss_quantity#24 as bigint))) AS store_sales_quantitycov#3, count(sr_return_quantity#47) AS store_returns_quantitycount#4L, avg(cast(sr_return_quantity#47 as bigint)) AS store_returns_quantityave#5, stddev_samp(cast(sr_return_quantity#47 as double)) AS store_returns_quantitystdev#6, (stddev_samp(cast(sr_return_quantity#47 as double)) / avg(cast(sr_return_quantity#47 as bigint))) AS store_returns_quantitycov#7, count(cs_quantity#75) AS catalog_sales_quantitycount#8L, avg(cast(cs_quantity#75 as bigint)) AS catalog_sales_quantityave#9, stddev_samp(cast(cs_quantity#75 as double)) AS catalog_sales_quantitystdev#10, (stddev_samp(cast(cs_quantity#75 as double)) / avg(cast(cs_quantity#75 as bigint))) AS catalog_sales_quantitycov#11]
         +- Filter (((((d_quarter_name#106 = 2001Q1) && (d_date_sk#91 = ss_sold_date_sk#14)) && ((i_item_sk#148 = ss_item_sk#16) && (s_store_sk#119 = ss_store_sk#21))) && (((ss_customer_sk#17 = sr_customer_sk#40) && (ss_item_sk#16 = sr_item_sk#39)) && (ss_ticket_number#23 = sr_ticket_number#46))) && ((((sr_returned_date_sk#37 = d_date_sk#170) && d_quarter_name#185 IN (2001Q1,2001Q2,2001Q3)) && (sr_customer_sk#40 = cs_bill_customer_sk#60)) && (((sr_item_sk#39 = cs_item_sk#72) && (cs_sold_date_sk#57 = d_date_sk#198)) && d_quarter_name#213 IN (2001Q1,2001Q2,2001Q3))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- Join Inner
               :  :  :  :  :- Join Inner
               :  :  :  :  :  :- Join Inner
               :  :  :  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
               :  :  :  :  :  :  :  +- Relation[ss_sold_date_sk#14,ss_sold_time_sk#15,ss_item_sk#16,ss_customer_sk#17,ss_cdemo_sk#18,ss_hdemo_sk#19,ss_addr_sk#20,ss_store_sk#21,ss_promo_sk#22,ss_ticket_number#23,ss_quantity#24,ss_wholesale_cost#25,ss_list_price#26,ss_sales_price#27,ss_ext_discount_amt#28,ss_ext_sales_price#29,ss_ext_wholesale_cost#30,ss_ext_list_price#31,ss_ext_tax#32,ss_coupon_amt#33,ss_net_paid#34,ss_net_paid_inc_tax#35,ss_net_profit#36] parquet
               :  :  :  :  :  :  +- SubqueryAlias `tpcds`.`store_returns`
               :  :  :  :  :  :     +- Relation[sr_returned_date_sk#37,sr_return_time_sk#38,sr_item_sk#39,sr_customer_sk#40,sr_cdemo_sk#41,sr_hdemo_sk#42,sr_addr_sk#43,sr_store_sk#44,sr_reason_sk#45,sr_ticket_number#46,sr_return_quantity#47,sr_return_amt#48,sr_return_tax#49,sr_return_amt_inc_tax#50,sr_fee#51,sr_return_ship_cost#52,sr_refunded_cash#53,sr_reversed_charge#54,sr_store_credit#55,sr_net_loss#56] parquet
               :  :  :  :  :  +- SubqueryAlias `tpcds`.`catalog_sales`
               :  :  :  :  :     +- Relation[cs_sold_date_sk#57,cs_sold_time_sk#58,cs_ship_date_sk#59,cs_bill_customer_sk#60,cs_bill_cdemo_sk#61,cs_bill_hdemo_sk#62,cs_bill_addr_sk#63,cs_ship_customer_sk#64,cs_ship_cdemo_sk#65,cs_ship_hdemo_sk#66,cs_ship_addr_sk#67,cs_call_center_sk#68,cs_catalog_page_sk#69,cs_ship_mode_sk#70,cs_warehouse_sk#71,cs_item_sk#72,cs_promo_sk#73,cs_order_number#74,cs_quantity#75,cs_wholesale_cost#76,cs_list_price#77,cs_sales_price#78,cs_ext_discount_amt#79,cs_ext_sales_price#80,... 10 more fields] parquet
               :  :  :  :  +- SubqueryAlias `d1`
               :  :  :  :     +- SubqueryAlias `tpcds`.`date_dim`
               :  :  :  :        +- Relation[d_date_sk#91,d_date_id#92,d_date#93,d_month_seq#94,d_week_seq#95,d_quarter_seq#96,d_year#97,d_dow#98,d_moy#99,d_dom#100,d_qoy#101,d_fy_year#102,d_fy_quarter_seq#103,d_fy_week_seq#104,d_day_name#105,d_quarter_name#106,d_holiday#107,d_weekend#108,d_following_holiday#109,d_first_dom#110,d_last_dom#111,d_same_day_ly#112,d_same_day_lq#113,d_current_day#114,... 4 more fields] parquet
               :  :  :  +- SubqueryAlias `d2`
               :  :  :     +- SubqueryAlias `tpcds`.`date_dim`
               :  :  :        +- Relation[d_date_sk#170,d_date_id#171,d_date#172,d_month_seq#173,d_week_seq#174,d_quarter_seq#175,d_year#176,d_dow#177,d_moy#178,d_dom#179,d_qoy#180,d_fy_year#181,d_fy_quarter_seq#182,d_fy_week_seq#183,d_day_name#184,d_quarter_name#185,d_holiday#186,d_weekend#187,d_following_holiday#188,d_first_dom#189,d_last_dom#190,d_same_day_ly#191,d_same_day_lq#192,d_current_day#193,... 4 more fields] parquet
               :  :  +- SubqueryAlias `d3`
               :  :     +- SubqueryAlias `tpcds`.`date_dim`
               :  :        +- Relation[d_date_sk#198,d_date_id#199,d_date#200,d_month_seq#201,d_week_seq#202,d_quarter_seq#203,d_year#204,d_dow#205,d_moy#206,d_dom#207,d_qoy#208,d_fy_year#209,d_fy_quarter_seq#210,d_fy_week_seq#211,d_day_name#212,d_quarter_name#213,d_holiday#214,d_weekend#215,d_following_holiday#216,d_first_dom#217,d_last_dom#218,d_same_day_ly#219,d_same_day_lq#220,d_current_day#221,... 4 more fields] parquet
               :  +- SubqueryAlias `tpcds`.`store`
               :     +- Relation[s_store_sk#119,s_store_id#120,s_rec_start_date#121,s_rec_end_date#122,s_closed_date_sk#123,s_store_name#124,s_number_employees#125,s_floor_space#126,s_hours#127,s_manager#128,s_market_id#129,s_geography_class#130,s_market_desc#131,s_market_manager#132,s_division_id#133,s_division_name#134,s_company_id#135,s_company_name#136,s_street_number#137,s_street_name#138,s_street_type#139,s_suite_number#140,s_city#141,s_county#142,... 5 more fields] parquet
               +- SubqueryAlias `tpcds`.`item`
                  +- Relation[i_item_sk#148,i_item_id#149,i_rec_start_date#150,i_rec_end_date#151,i_item_desc#152,i_current_price#153,i_wholesale_cost#154,i_brand_id#155,i_brand#156,i_class_id#157,i_class#158,i_category_id#159,i_category#160,i_manufact_id#161,i_manufact#162,i_size#163,i_formulation#164,i_color#165,i_units#166,i_container#167,i_manager_id#168,i_product_name#169] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#149 ASC NULLS FIRST, i_item_desc#152 ASC NULLS FIRST, s_state#143 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#149, i_item_desc#152, s_state#143], [i_item_id#149, i_item_desc#152, s_state#143, count(ss_quantity#24) AS store_sales_quantitycount#0L, avg(cast(ss_quantity#24 as bigint)) AS store_sales_quantityave#1, stddev_samp(cast(ss_quantity#24 as double)) AS store_sales_quantitystdev#2, (stddev_samp(cast(ss_quantity#24 as double)) / avg(cast(ss_quantity#24 as bigint))) AS store_sales_quantitycov#3, count(sr_return_quantity#47) AS store_returns_quantitycount#4L, avg(cast(sr_return_quantity#47 as bigint)) AS store_returns_quantityave#5, stddev_samp(cast(sr_return_quantity#47 as double)) AS store_returns_quantitystdev#6, (stddev_samp(cast(sr_return_quantity#47 as double)) / avg(cast(sr_return_quantity#47 as bigint))) AS store_returns_quantitycov#7, count(cs_quantity#75) AS catalog_sales_quantitycount#8L, avg(cast(cs_quantity#75 as bigint)) AS catalog_sales_quantityave#9, stddev_samp(cast(cs_quantity#75 as double)) AS catalog_sales_quantitystdev#10, (stddev_samp(cast(cs_quantity#75 as double)) / avg(cast(cs_quantity#75 as bigint))) AS catalog_sales_quantitycov#11]
         +- Project [ss_quantity#24, sr_return_quantity#47, cs_quantity#75, s_state#143, i_item_id#149, i_item_desc#152]
            +- Join Inner, (i_item_sk#148 = ss_item_sk#16)
               :- Project [ss_item_sk#16, ss_quantity#24, sr_return_quantity#47, cs_quantity#75, s_state#143]
               :  +- Join Inner, (s_store_sk#119 = ss_store_sk#21)
               :     :- Project [ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_return_quantity#47, cs_quantity#75]
               :     :  +- Join Inner, (cs_sold_date_sk#57 = d_date_sk#198)
               :     :     :- Project [ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_return_quantity#47, cs_sold_date_sk#57, cs_quantity#75]
               :     :     :  +- Join Inner, (sr_returned_date_sk#37 = d_date_sk#170)
               :     :     :     :- Project [ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_returned_date_sk#37, sr_return_quantity#47, cs_sold_date_sk#57, cs_quantity#75]
               :     :     :     :  +- Join Inner, (d_date_sk#91 = ss_sold_date_sk#14)
               :     :     :     :     :- Project [ss_sold_date_sk#14, ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_returned_date_sk#37, sr_return_quantity#47, cs_sold_date_sk#57, cs_quantity#75]
               :     :     :     :     :  +- Join Inner, ((sr_customer_sk#40 = cs_bill_customer_sk#60) && (sr_item_sk#39 = cs_item_sk#72))
               :     :     :     :     :     :- Project [ss_sold_date_sk#14, ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_returned_date_sk#37, sr_item_sk#39, sr_customer_sk#40, sr_return_quantity#47]
               :     :     :     :     :     :  +- Join Inner, (((ss_customer_sk#17 = sr_customer_sk#40) && (ss_item_sk#16 = sr_item_sk#39)) && (ss_ticket_number#23 = sr_ticket_number#46))
               :     :     :     :     :     :     :- Project [ss_sold_date_sk#14, ss_item_sk#16, ss_customer_sk#17, ss_store_sk#21, ss_ticket_number#23, ss_quantity#24]
               :     :     :     :     :     :     :  +- Filter ((((isnotnull(ss_ticket_number#23) && isnotnull(ss_item_sk#16)) && isnotnull(ss_customer_sk#17)) && isnotnull(ss_sold_date_sk#14)) && isnotnull(ss_store_sk#21))
               :     :     :     :     :     :     :     +- Relation[ss_sold_date_sk#14,ss_sold_time_sk#15,ss_item_sk#16,ss_customer_sk#17,ss_cdemo_sk#18,ss_hdemo_sk#19,ss_addr_sk#20,ss_store_sk#21,ss_promo_sk#22,ss_ticket_number#23,ss_quantity#24,ss_wholesale_cost#25,ss_list_price#26,ss_sales_price#27,ss_ext_discount_amt#28,ss_ext_sales_price#29,ss_ext_wholesale_cost#30,ss_ext_list_price#31,ss_ext_tax#32,ss_coupon_amt#33,ss_net_paid#34,ss_net_paid_inc_tax#35,ss_net_profit#36] parquet
               :     :     :     :     :     :     +- Project [sr_returned_date_sk#37, sr_item_sk#39, sr_customer_sk#40, sr_ticket_number#46, sr_return_quantity#47]
               :     :     :     :     :     :        +- Filter (((isnotnull(sr_item_sk#39) && isnotnull(sr_ticket_number#46)) && isnotnull(sr_customer_sk#40)) && isnotnull(sr_returned_date_sk#37))
               :     :     :     :     :     :           +- Relation[sr_returned_date_sk#37,sr_return_time_sk#38,sr_item_sk#39,sr_customer_sk#40,sr_cdemo_sk#41,sr_hdemo_sk#42,sr_addr_sk#43,sr_store_sk#44,sr_reason_sk#45,sr_ticket_number#46,sr_return_quantity#47,sr_return_amt#48,sr_return_tax#49,sr_return_amt_inc_tax#50,sr_fee#51,sr_return_ship_cost#52,sr_refunded_cash#53,sr_reversed_charge#54,sr_store_credit#55,sr_net_loss#56] parquet
               :     :     :     :     :     +- Project [cs_sold_date_sk#57, cs_bill_customer_sk#60, cs_item_sk#72, cs_quantity#75]
               :     :     :     :     :        +- Filter ((isnotnull(cs_item_sk#72) && isnotnull(cs_bill_customer_sk#60)) && isnotnull(cs_sold_date_sk#57))
               :     :     :     :     :           +- Relation[cs_sold_date_sk#57,cs_sold_time_sk#58,cs_ship_date_sk#59,cs_bill_customer_sk#60,cs_bill_cdemo_sk#61,cs_bill_hdemo_sk#62,cs_bill_addr_sk#63,cs_ship_customer_sk#64,cs_ship_cdemo_sk#65,cs_ship_hdemo_sk#66,cs_ship_addr_sk#67,cs_call_center_sk#68,cs_catalog_page_sk#69,cs_ship_mode_sk#70,cs_warehouse_sk#71,cs_item_sk#72,cs_promo_sk#73,cs_order_number#74,cs_quantity#75,cs_wholesale_cost#76,cs_list_price#77,cs_sales_price#78,cs_ext_discount_amt#79,cs_ext_sales_price#80,... 10 more fields] parquet
               :     :     :     :     +- Project [d_date_sk#91]
               :     :     :     :        +- Filter ((isnotnull(d_quarter_name#106) && (d_quarter_name#106 = 2001Q1)) && isnotnull(d_date_sk#91))
               :     :     :     :           +- Relation[d_date_sk#91,d_date_id#92,d_date#93,d_month_seq#94,d_week_seq#95,d_quarter_seq#96,d_year#97,d_dow#98,d_moy#99,d_dom#100,d_qoy#101,d_fy_year#102,d_fy_quarter_seq#103,d_fy_week_seq#104,d_day_name#105,d_quarter_name#106,d_holiday#107,d_weekend#108,d_following_holiday#109,d_first_dom#110,d_last_dom#111,d_same_day_ly#112,d_same_day_lq#113,d_current_day#114,... 4 more fields] parquet
               :     :     :     +- Project [d_date_sk#170]
               :     :     :        +- Filter (d_quarter_name#185 IN (2001Q1,2001Q2,2001Q3) && isnotnull(d_date_sk#170))
               :     :     :           +- Relation[d_date_sk#170,d_date_id#171,d_date#172,d_month_seq#173,d_week_seq#174,d_quarter_seq#175,d_year#176,d_dow#177,d_moy#178,d_dom#179,d_qoy#180,d_fy_year#181,d_fy_quarter_seq#182,d_fy_week_seq#183,d_day_name#184,d_quarter_name#185,d_holiday#186,d_weekend#187,d_following_holiday#188,d_first_dom#189,d_last_dom#190,d_same_day_ly#191,d_same_day_lq#192,d_current_day#193,... 4 more fields] parquet
               :     :     +- Project [d_date_sk#198]
               :     :        +- Filter (d_quarter_name#213 IN (2001Q1,2001Q2,2001Q3) && isnotnull(d_date_sk#198))
               :     :           +- Relation[d_date_sk#198,d_date_id#199,d_date#200,d_month_seq#201,d_week_seq#202,d_quarter_seq#203,d_year#204,d_dow#205,d_moy#206,d_dom#207,d_qoy#208,d_fy_year#209,d_fy_quarter_seq#210,d_fy_week_seq#211,d_day_name#212,d_quarter_name#213,d_holiday#214,d_weekend#215,d_following_holiday#216,d_first_dom#217,d_last_dom#218,d_same_day_ly#219,d_same_day_lq#220,d_current_day#221,... 4 more fields] parquet
               :     +- Project [s_store_sk#119, s_state#143]
               :        +- Filter isnotnull(s_store_sk#119)
               :           +- Relation[s_store_sk#119,s_store_id#120,s_rec_start_date#121,s_rec_end_date#122,s_closed_date_sk#123,s_store_name#124,s_number_employees#125,s_floor_space#126,s_hours#127,s_manager#128,s_market_id#129,s_geography_class#130,s_market_desc#131,s_market_manager#132,s_division_id#133,s_division_name#134,s_company_id#135,s_company_name#136,s_street_number#137,s_street_name#138,s_street_type#139,s_suite_number#140,s_city#141,s_county#142,... 5 more fields] parquet
               +- Project [i_item_sk#148, i_item_id#149, i_item_desc#152]
                  +- Filter isnotnull(i_item_sk#148)
                     +- Relation[i_item_sk#148,i_item_id#149,i_rec_start_date#150,i_rec_end_date#151,i_item_desc#152,i_current_price#153,i_wholesale_cost#154,i_brand_id#155,i_brand#156,i_class_id#157,i_class#158,i_category_id#159,i_category#160,i_manufact_id#161,i_manufact#162,i_size#163,i_formulation#164,i_color#165,i_units#166,i_container#167,i_manager_id#168,i_product_name#169] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[i_item_id#149 ASC NULLS FIRST,i_item_desc#152 ASC NULLS FIRST,s_state#143 ASC NULLS FIRST], output=[i_item_id#149,i_item_desc#152,s_state#143,store_sales_quantitycount#0L,store_sales_quantityave#1,store_sales_quantitystdev#2,store_sales_quantitycov#3,store_returns_quantitycount#4L,store_returns_quantityave#5,store_returns_quantitystdev#6,store_returns_quantitycov#7,catalog_sales_quantitycount#8L,catalog_sales_quantityave#9,catalog_sales_quantitystdev#10,catalog_sales_quantitycov#11])
+- *(12) HashAggregate(keys=[i_item_id#149, i_item_desc#152, s_state#143], functions=[count(ss_quantity#24), avg(cast(ss_quantity#24 as bigint)), stddev_samp(cast(ss_quantity#24 as double)), count(sr_return_quantity#47), avg(cast(sr_return_quantity#47 as bigint)), stddev_samp(cast(sr_return_quantity#47 as double)), count(cs_quantity#75), avg(cast(cs_quantity#75 as bigint)), stddev_samp(cast(cs_quantity#75 as double))], output=[i_item_id#149, i_item_desc#152, s_state#143, store_sales_quantitycount#0L, store_sales_quantityave#1, store_sales_quantitystdev#2, store_sales_quantitycov#3, store_returns_quantitycount#4L, store_returns_quantityave#5, store_returns_quantitystdev#6, store_returns_quantitycov#7, catalog_sales_quantitycount#8L, catalog_sales_quantityave#9, catalog_sales_quantitystdev#10, catalog_sales_quantitycov#11])
   +- Exchange hashpartitioning(i_item_id#149, i_item_desc#152, s_state#143, 200)
      +- *(11) HashAggregate(keys=[i_item_id#149, i_item_desc#152, s_state#143], functions=[partial_count(ss_quantity#24), partial_avg(cast(ss_quantity#24 as bigint)), partial_stddev_samp(cast(ss_quantity#24 as double)), partial_count(sr_return_quantity#47), partial_avg(cast(sr_return_quantity#47 as bigint)), partial_stddev_samp(cast(sr_return_quantity#47 as double)), partial_count(cs_quantity#75), partial_avg(cast(cs_quantity#75 as bigint)), partial_stddev_samp(cast(cs_quantity#75 as double))], output=[i_item_id#149, i_item_desc#152, s_state#143, count#442L, sum#443, count#444L, n#342, avg#343, m2#344, count#445L, sum#446, count#447L, n#358, avg#359, m2#360, count#448L, sum#449, count#450L, n#374, avg#375, m2#376])
         +- *(11) Project [ss_quantity#24, sr_return_quantity#47, cs_quantity#75, s_state#143, i_item_id#149, i_item_desc#152]
            +- *(11) BroadcastHashJoin [ss_item_sk#16], [i_item_sk#148], Inner, BuildRight
               :- *(11) Project [ss_item_sk#16, ss_quantity#24, sr_return_quantity#47, cs_quantity#75, s_state#143]
               :  +- *(11) BroadcastHashJoin [ss_store_sk#21], [s_store_sk#119], Inner, BuildRight
               :     :- *(11) Project [ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_return_quantity#47, cs_quantity#75]
               :     :  +- *(11) BroadcastHashJoin [cs_sold_date_sk#57], [d_date_sk#198], Inner, BuildRight
               :     :     :- *(11) Project [ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_return_quantity#47, cs_sold_date_sk#57, cs_quantity#75]
               :     :     :  +- *(11) BroadcastHashJoin [sr_returned_date_sk#37], [d_date_sk#170], Inner, BuildRight
               :     :     :     :- *(11) Project [ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_returned_date_sk#37, sr_return_quantity#47, cs_sold_date_sk#57, cs_quantity#75]
               :     :     :     :  +- *(11) BroadcastHashJoin [ss_sold_date_sk#14], [d_date_sk#91], Inner, BuildRight
               :     :     :     :     :- *(11) Project [ss_sold_date_sk#14, ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_returned_date_sk#37, sr_return_quantity#47, cs_sold_date_sk#57, cs_quantity#75]
               :     :     :     :     :  +- *(11) SortMergeJoin [sr_customer_sk#40, sr_item_sk#39], [cs_bill_customer_sk#60, cs_item_sk#72], Inner
               :     :     :     :     :     :- *(3) Sort [sr_customer_sk#40 ASC NULLS FIRST, sr_item_sk#39 ASC NULLS FIRST], false, 0
               :     :     :     :     :     :  +- Exchange hashpartitioning(sr_customer_sk#40, sr_item_sk#39, 200)
               :     :     :     :     :     :     +- *(2) Project [ss_sold_date_sk#14, ss_item_sk#16, ss_store_sk#21, ss_quantity#24, sr_returned_date_sk#37, sr_item_sk#39, sr_customer_sk#40, sr_return_quantity#47]
               :     :     :     :     :     :        +- *(2) BroadcastHashJoin [ss_customer_sk#17, ss_item_sk#16, ss_ticket_number#23], [sr_customer_sk#40, sr_item_sk#39, sr_ticket_number#46], Inner, BuildRight
               :     :     :     :     :     :           :- *(2) Project [ss_sold_date_sk#14, ss_item_sk#16, ss_customer_sk#17, ss_store_sk#21, ss_ticket_number#23, ss_quantity#24]
               :     :     :     :     :     :           :  +- *(2) Filter ((((isnotnull(ss_ticket_number#23) && isnotnull(ss_item_sk#16)) && isnotnull(ss_customer_sk#17)) && isnotnull(ss_sold_date_sk#14)) && isnotnull(ss_store_sk#21))
               :     :     :     :     :     :           :     +- *(2) FileScan parquet tpcds.store_sales[ss_sold_date_sk#14,ss_item_sk#16,ss_customer_sk#17,ss_store_sk#21,ss_ticket_number#23,ss_quantity#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_ticket_number), IsNotNull(ss_item_sk), IsNotNull(ss_customer_sk), IsNotNull(ss_sold..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ticket_number:int...
               :     :     :     :     :     :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[2, int, true], input[1, int, true], input[3, int, true]))
               :     :     :     :     :     :              +- *(1) Project [sr_returned_date_sk#37, sr_item_sk#39, sr_customer_sk#40, sr_ticket_number#46, sr_return_quantity#47]
               :     :     :     :     :     :                 +- *(1) Filter (((isnotnull(sr_item_sk#39) && isnotnull(sr_ticket_number#46)) && isnotnull(sr_customer_sk#40)) && isnotnull(sr_returned_date_sk#37))
               :     :     :     :     :     :                    +- *(1) FileScan parquet tpcds.store_returns[sr_returned_date_sk#37,sr_item_sk#39,sr_customer_sk#40,sr_ticket_number#46,sr_return_quantity#47] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_ticket_number), IsNotNull(sr_customer_sk), IsNotNull(sr_retu..., ReadSchema: struct<sr_returned_date_sk:int,sr_item_sk:int,sr_customer_sk:int,sr_ticket_number:int,sr_return_q...
               :     :     :     :     :     +- *(5) Sort [cs_bill_customer_sk#60 ASC NULLS FIRST, cs_item_sk#72 ASC NULLS FIRST], false, 0
               :     :     :     :     :        +- Exchange hashpartitioning(cs_bill_customer_sk#60, cs_item_sk#72, 200)
               :     :     :     :     :           +- *(4) Project [cs_sold_date_sk#57, cs_bill_customer_sk#60, cs_item_sk#72, cs_quantity#75]
               :     :     :     :     :              +- *(4) Filter ((isnotnull(cs_item_sk#72) && isnotnull(cs_bill_customer_sk#60)) && isnotnull(cs_sold_date_sk#57))
               :     :     :     :     :                 +- *(4) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#57,cs_bill_customer_sk#60,cs_item_sk#72,cs_quantity#75] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_bill_customer_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int,cs_quantity:int>
               :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :        +- *(6) Project [d_date_sk#91]
               :     :     :     :           +- *(6) Filter ((isnotnull(d_quarter_name#106) && (d_quarter_name#106 = 2001Q1)) && isnotnull(d_date_sk#91))
               :     :     :     :              +- *(6) FileScan parquet tpcds.date_dim[d_date_sk#91,d_quarter_name#106] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_quarter_name), EqualTo(d_quarter_name,2001Q1), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_quarter_name:string>
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :        +- *(7) Project [d_date_sk#170]
               :     :     :           +- *(7) Filter (d_quarter_name#185 IN (2001Q1,2001Q2,2001Q3) && isnotnull(d_date_sk#170))
               :     :     :              +- *(7) FileScan parquet tpcds.date_dim[d_date_sk#170,d_quarter_name#185] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [In(d_quarter_name, [2001Q1,2001Q2,2001Q3]), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_quarter_name:string>
               :     :     +- ReusedExchange [d_date_sk#198], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(9) Project [s_store_sk#119, s_state#143]
               :           +- *(9) Filter isnotnull(s_store_sk#119)
               :              +- *(9) FileScan parquet tpcds.store[s_store_sk#119,s_state#143] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_state:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(10) Project [i_item_sk#148, i_item_id#149, i_item_desc#152]
                     +- *(10) Filter isnotnull(i_item_sk#148)
                        +- *(10) FileScan parquet tpcds.item[i_item_sk#148,i_item_id#149,i_item_desc#152] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string>
Time taken: 4.571 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 17 in stream 0 using template query17.tpl
------------------------------------------------------^^^

