Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741582318611
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['total_cnt DESC NULLS LAST, 'i_item_desc ASC NULLS FIRST, 'w_warehouse_name ASC NULLS FIRST, 'd_week_seq ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_desc, 'w_warehouse_name, 'd1.d_week_seq], ['i_item_desc, 'w_warehouse_name, 'd1.d_week_seq, 'sum(CASE WHEN isnull('p_promo_sk) THEN 1 ELSE 0 END) AS no_promo#0, 'sum(CASE WHEN isnotnull('p_promo_sk) THEN 1 ELSE 0 END) AS promo#1, 'count(1) AS total_cnt#2]
         +- 'Filter (((('d1.d_week_seq = 'd2.d_week_seq) AND ('inv_quantity_on_hand < 'cs_quantity)) AND ('d3.d_date > 'date_add(cast('d1.d_date as date), 5))) AND ((('hd_buy_potential = >10000) AND ('d1.d_year = 1999)) AND ('cd_marital_status = D)))
            +- 'Join LeftOuter, (('cr_item_sk = 'cs_item_sk) AND ('cr_order_number = 'cs_order_number))
               :- 'Join LeftOuter, ('cs_promo_sk = 'p_promo_sk)
               :  :- 'Join Inner, ('cs_ship_date_sk = 'd3.d_date_sk)
               :  :  :- 'Join Inner, ('inv_date_sk = 'd2.d_date_sk)
               :  :  :  :- 'Join Inner, ('cs_sold_date_sk = 'd1.d_date_sk)
               :  :  :  :  :- 'Join Inner, ('cs_bill_hdemo_sk = 'hd_demo_sk)
               :  :  :  :  :  :- 'Join Inner, ('cs_bill_cdemo_sk = 'cd_demo_sk)
               :  :  :  :  :  :  :- 'Join Inner, ('i_item_sk = 'cs_item_sk)
               :  :  :  :  :  :  :  :- 'Join Inner, ('w_warehouse_sk = 'inv_warehouse_sk)
               :  :  :  :  :  :  :  :  :- 'Join Inner, ('cs_item_sk = 'inv_item_sk)
               :  :  :  :  :  :  :  :  :  :- 'UnresolvedRelation [catalog_sales], [], false
               :  :  :  :  :  :  :  :  :  +- 'UnresolvedRelation [inventory], [], false
               :  :  :  :  :  :  :  :  +- 'UnresolvedRelation [warehouse], [], false
               :  :  :  :  :  :  :  +- 'UnresolvedRelation [item], [], false
               :  :  :  :  :  :  +- 'UnresolvedRelation [customer_demographics], [], false
               :  :  :  :  :  +- 'UnresolvedRelation [household_demographics], [], false
               :  :  :  :  +- 'SubqueryAlias d1
               :  :  :  :     +- 'UnresolvedRelation [date_dim], [], false
               :  :  :  +- 'SubqueryAlias d2
               :  :  :     +- 'UnresolvedRelation [date_dim], [], false
               :  :  +- 'SubqueryAlias d3
               :  :     +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [promotion], [], false
               +- 'UnresolvedRelation [catalog_returns], [], false

== Analyzed Logical Plan ==
i_item_desc: string, w_warehouse_name: string, d_week_seq: int, no_promo: bigint, promo: bigint, total_cnt: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [total_cnt#2L DESC NULLS LAST, i_item_desc#65 ASC NULLS FIRST, w_warehouse_name#49 ASC NULLS FIRST, d_week_seq#101 ASC NULLS FIRST], true
      +- Aggregate [i_item_desc#65, w_warehouse_name#49, d_week_seq#101], [i_item_desc#65, w_warehouse_name#49, d_week_seq#101, sum(CASE WHEN isnull(p_promo_sk#125) THEN 1 ELSE 0 END) AS no_promo#0L, sum(CASE WHEN isnotnull(p_promo_sk#125) THEN 1 ELSE 0 END) AS promo#1L, count(1) AS total_cnt#2L]
         +- Filter ((((d_week_seq#101 = d_week_seq#175) AND (inv_quantity_on_hand#46L < cast(cs_quantity#27 as bigint))) AND (cast(d_date#201 as date) > date_add(cast(d_date#99 as date), 5))) AND (((hd_buy_potential#94 = >10000) AND (d_year#103 = 1999)) AND (cd_marital_status#85 = D)))
            +- Join LeftOuter, ((cr_item_sk#146 = cs_item_sk#24) AND (cr_order_number#160 = cs_order_number#26))
               :- Join LeftOuter, (cs_promo_sk#25 = p_promo_sk#125)
               :  :- Join Inner, (cs_ship_date_sk#11 = d_date_sk#199)
               :  :  :- Join Inner, (inv_date_sk#43 = d_date_sk#171)
               :  :  :  :- Join Inner, (cs_sold_date_sk#9 = d_date_sk#97)
               :  :  :  :  :- Join Inner, (cs_bill_hdemo_sk#14 = hd_demo_sk#92)
               :  :  :  :  :  :- Join Inner, (cs_bill_cdemo_sk#13 = cd_demo_sk#83)
               :  :  :  :  :  :  :- Join Inner, (i_item_sk#61 = cs_item_sk#24)
               :  :  :  :  :  :  :  :- Join Inner, (w_warehouse_sk#47 = inv_warehouse_sk#45)
               :  :  :  :  :  :  :  :  :- Join Inner, (cs_item_sk#24 = inv_item_sk#44)
               :  :  :  :  :  :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.catalog_sales
               :  :  :  :  :  :  :  :  :  :  +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#9,cs_sold_time_sk#10,cs_ship_date_sk#11,cs_bill_customer_sk#12,cs_bill_cdemo_sk#13,cs_bill_hdemo_sk#14,cs_bill_addr_sk#15,cs_ship_customer_sk#16,cs_ship_cdemo_sk#17,cs_ship_hdemo_sk#18,cs_ship_addr_sk#19,cs_call_center_sk#20,cs_catalog_page_sk#21,cs_ship_mode_sk#22,cs_warehouse_sk#23,cs_item_sk#24,cs_promo_sk#25,cs_order_number#26,cs_quantity#27,cs_wholesale_cost#28,cs_list_price#29,cs_sales_price#30,cs_ext_discount_amt#31,cs_ext_sales_price#32,... 10 more fields] parquet
               :  :  :  :  :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.inventory
               :  :  :  :  :  :  :  :  :     +- Relation spark_catalog.tpcds.inventory[inv_date_sk#43,inv_item_sk#44,inv_warehouse_sk#45,inv_quantity_on_hand#46L] parquet
               :  :  :  :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.warehouse
               :  :  :  :  :  :  :  :     +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#47,w_warehouse_id#48,w_warehouse_name#49,w_warehouse_sq_ft#50,w_street_number#51,w_street_name#52,w_street_type#53,w_suite_number#54,w_city#55,w_county#56,w_state#57,w_zip#58,w_country#59,w_gmt_offset#60] parquet
               :  :  :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.item
               :  :  :  :  :  :  :     +- Relation spark_catalog.tpcds.item[i_item_sk#61,i_item_id#62,i_rec_start_date#63,i_rec_end_date#64,i_item_desc#65,i_current_price#66,i_wholesale_cost#67,i_brand_id#68,i_brand#69,i_class_id#70,i_class#71,i_category_id#72,i_category#73,i_manufact_id#74,i_manufact#75,i_size#76,i_formulation#77,i_color#78,i_units#79,i_container#80,i_manager_id#81,i_product_name#82] parquet
               :  :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer_demographics
               :  :  :  :  :  :     +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#83,cd_gender#84,cd_marital_status#85,cd_education_status#86,cd_purchase_estimate#87,cd_credit_rating#88,cd_dep_count#89,cd_dep_employed_count#90,cd_dep_college_count#91] parquet
               :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.household_demographics
               :  :  :  :  :     +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#92,hd_income_band_sk#93,hd_buy_potential#94,hd_dep_count#95,hd_vehicle_count#96] parquet
               :  :  :  :  +- SubqueryAlias d1
               :  :  :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :  :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#97,d_date_id#98,d_date#99,d_month_seq#100,d_week_seq#101,d_quarter_seq#102,d_year#103,d_dow#104,d_moy#105,d_dom#106,d_qoy#107,d_fy_year#108,d_fy_quarter_seq#109,d_fy_week_seq#110,d_day_name#111,d_quarter_name#112,d_holiday#113,d_weekend#114,d_following_holiday#115,d_first_dom#116,d_last_dom#117,d_same_day_ly#118,d_same_day_lq#119,d_current_day#120,... 4 more fields] parquet
               :  :  :  +- SubqueryAlias d2
               :  :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#171,d_date_id#172,d_date#173,d_month_seq#174,d_week_seq#175,d_quarter_seq#176,d_year#177,d_dow#178,d_moy#179,d_dom#180,d_qoy#181,d_fy_year#182,d_fy_quarter_seq#183,d_fy_week_seq#184,d_day_name#185,d_quarter_name#186,d_holiday#187,d_weekend#188,d_following_holiday#189,d_first_dom#190,d_last_dom#191,d_same_day_ly#192,d_same_day_lq#193,d_current_day#194,... 4 more fields] parquet
               :  :  +- SubqueryAlias d3
               :  :     +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#199,d_date_id#200,d_date#201,d_month_seq#202,d_week_seq#203,d_quarter_seq#204,d_year#205,d_dow#206,d_moy#207,d_dom#208,d_qoy#209,d_fy_year#210,d_fy_quarter_seq#211,d_fy_week_seq#212,d_day_name#213,d_quarter_name#214,d_holiday#215,d_weekend#216,d_following_holiday#217,d_first_dom#218,d_last_dom#219,d_same_day_ly#220,d_same_day_lq#221,d_current_day#222,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.promotion
               :     +- Relation spark_catalog.tpcds.promotion[p_promo_sk#125,p_promo_id#126,p_start_date_sk#127,p_end_date_sk#128,p_item_sk#129,p_cost#130,p_response_target#131,p_promo_name#132,p_channel_dmail#133,p_channel_email#134,p_channel_catalog#135,p_channel_tv#136,p_channel_radio#137,p_channel_press#138,p_channel_event#139,p_channel_demo#140,p_channel_details#141,p_purpose#142,p_discount_active#143] parquet
               +- SubqueryAlias spark_catalog.tpcds.catalog_returns
                  +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#144,cr_returned_time_sk#145,cr_item_sk#146,cr_refunded_customer_sk#147,cr_refunded_cdemo_sk#148,cr_refunded_hdemo_sk#149,cr_refunded_addr_sk#150,cr_returning_customer_sk#151,cr_returning_cdemo_sk#152,cr_returning_hdemo_sk#153,cr_returning_addr_sk#154,cr_call_center_sk#155,cr_catalog_page_sk#156,cr_ship_mode_sk#157,cr_warehouse_sk#158,cr_reason_sk#159,cr_order_number#160,cr_return_quantity#161,cr_return_amount#162,cr_return_tax#163,cr_return_amt_inc_tax#164,cr_fee#165,cr_return_ship_cost#166,cr_refunded_cash#167,... 3 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [total_cnt#2L DESC NULLS LAST, i_item_desc#65 ASC NULLS FIRST, w_warehouse_name#49 ASC NULLS FIRST, d_week_seq#101 ASC NULLS FIRST], true
      +- Aggregate [i_item_desc#65, w_warehouse_name#49, d_week_seq#101], [i_item_desc#65, w_warehouse_name#49, d_week_seq#101, sum(CASE WHEN isnull(p_promo_sk#125) THEN 1 ELSE 0 END) AS no_promo#0L, sum(CASE WHEN isnotnull(p_promo_sk#125) THEN 1 ELSE 0 END) AS promo#1L, count(1) AS total_cnt#2L]
         +- Project [w_warehouse_name#49, i_item_desc#65, d_week_seq#101, p_promo_sk#125]
            +- Join LeftOuter, ((cr_item_sk#146 = cs_item_sk#24) AND (cr_order_number#160 = cs_order_number#26))
               :- Project [cs_item_sk#24, cs_order_number#26, w_warehouse_name#49, i_item_desc#65, d_week_seq#101, p_promo_sk#125]
               :  +- Join LeftOuter, (cs_promo_sk#25 = p_promo_sk#125)
               :     :- Project [cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, w_warehouse_name#49, i_item_desc#65, d_week_seq#101]
               :     :  +- Join Inner, ((cast(d_date#201 as date) > date_add(cast(d_date#99 as date), 5)) AND (cs_ship_date_sk#11 = d_date_sk#199))
               :     :     :- Project [cs_ship_date_sk#11, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, w_warehouse_name#49, i_item_desc#65, d_date#99, d_week_seq#101]
               :     :     :  +- Join Inner, ((d_week_seq#101 = d_week_seq#175) AND (inv_date_sk#43 = d_date_sk#171))
               :     :     :     :- Project [cs_ship_date_sk#11, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49, i_item_desc#65, d_date#99, d_week_seq#101]
               :     :     :     :  +- Join Inner, (cs_sold_date_sk#9 = d_date_sk#97)
               :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49, i_item_desc#65]
               :     :     :     :     :  +- Join Inner, (cs_bill_hdemo_sk#14 = hd_demo_sk#92)
               :     :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_bill_hdemo_sk#14, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49, i_item_desc#65]
               :     :     :     :     :     :  +- Join Inner, (cs_bill_cdemo_sk#13 = cd_demo_sk#83)
               :     :     :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_bill_cdemo_sk#13, cs_bill_hdemo_sk#14, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49, i_item_desc#65]
               :     :     :     :     :     :     :  +- Join Inner, (i_item_sk#61 = cs_item_sk#24)
               :     :     :     :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_bill_cdemo_sk#13, cs_bill_hdemo_sk#14, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49]
               :     :     :     :     :     :     :     :  +- Join Inner, (w_warehouse_sk#47 = inv_warehouse_sk#45)
               :     :     :     :     :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_bill_cdemo_sk#13, cs_bill_hdemo_sk#14, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, inv_warehouse_sk#45]
               :     :     :     :     :     :     :     :     :  +- Join Inner, ((inv_quantity_on_hand#46L < cast(cs_quantity#27 as bigint)) AND (cs_item_sk#24 = inv_item_sk#44))
               :     :     :     :     :     :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_bill_cdemo_sk#13, cs_bill_hdemo_sk#14, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, cs_quantity#27]
               :     :     :     :     :     :     :     :     :     :  +- Filter (((isnotnull(cs_quantity#27) AND isnotnull(cs_item_sk#24)) AND isnotnull(cs_bill_cdemo_sk#13)) AND ((isnotnull(cs_bill_hdemo_sk#14) AND isnotnull(cs_sold_date_sk#9)) AND isnotnull(cs_ship_date_sk#11)))
               :     :     :     :     :     :     :     :     :     :     +- Relation spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#9,cs_sold_time_sk#10,cs_ship_date_sk#11,cs_bill_customer_sk#12,cs_bill_cdemo_sk#13,cs_bill_hdemo_sk#14,cs_bill_addr_sk#15,cs_ship_customer_sk#16,cs_ship_cdemo_sk#17,cs_ship_hdemo_sk#18,cs_ship_addr_sk#19,cs_call_center_sk#20,cs_catalog_page_sk#21,cs_ship_mode_sk#22,cs_warehouse_sk#23,cs_item_sk#24,cs_promo_sk#25,cs_order_number#26,cs_quantity#27,cs_wholesale_cost#28,cs_list_price#29,cs_sales_price#30,cs_ext_discount_amt#31,cs_ext_sales_price#32,... 10 more fields] parquet
               :     :     :     :     :     :     :     :     :     +- Filter (((isnotnull(inv_quantity_on_hand#46L) AND isnotnull(inv_item_sk#44)) AND isnotnull(inv_warehouse_sk#45)) AND isnotnull(inv_date_sk#43))
               :     :     :     :     :     :     :     :     :        +- Relation spark_catalog.tpcds.inventory[inv_date_sk#43,inv_item_sk#44,inv_warehouse_sk#45,inv_quantity_on_hand#46L] parquet
               :     :     :     :     :     :     :     :     +- Project [w_warehouse_sk#47, w_warehouse_name#49]
               :     :     :     :     :     :     :     :        +- Filter isnotnull(w_warehouse_sk#47)
               :     :     :     :     :     :     :     :           +- Relation spark_catalog.tpcds.warehouse[w_warehouse_sk#47,w_warehouse_id#48,w_warehouse_name#49,w_warehouse_sq_ft#50,w_street_number#51,w_street_name#52,w_street_type#53,w_suite_number#54,w_city#55,w_county#56,w_state#57,w_zip#58,w_country#59,w_gmt_offset#60] parquet
               :     :     :     :     :     :     :     +- Project [i_item_sk#61, i_item_desc#65]
               :     :     :     :     :     :     :        +- Filter isnotnull(i_item_sk#61)
               :     :     :     :     :     :     :           +- Relation spark_catalog.tpcds.item[i_item_sk#61,i_item_id#62,i_rec_start_date#63,i_rec_end_date#64,i_item_desc#65,i_current_price#66,i_wholesale_cost#67,i_brand_id#68,i_brand#69,i_class_id#70,i_class#71,i_category_id#72,i_category#73,i_manufact_id#74,i_manufact#75,i_size#76,i_formulation#77,i_color#78,i_units#79,i_container#80,i_manager_id#81,i_product_name#82] parquet
               :     :     :     :     :     :     +- Project [cd_demo_sk#83]
               :     :     :     :     :     :        +- Filter ((isnotnull(cd_marital_status#85) AND (cd_marital_status#85 = D)) AND isnotnull(cd_demo_sk#83))
               :     :     :     :     :     :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#83,cd_gender#84,cd_marital_status#85,cd_education_status#86,cd_purchase_estimate#87,cd_credit_rating#88,cd_dep_count#89,cd_dep_employed_count#90,cd_dep_college_count#91] parquet
               :     :     :     :     :     +- Project [hd_demo_sk#92]
               :     :     :     :     :        +- Filter ((isnotnull(hd_buy_potential#94) AND (hd_buy_potential#94 = >10000)) AND isnotnull(hd_demo_sk#92))
               :     :     :     :     :           +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#92,hd_income_band_sk#93,hd_buy_potential#94,hd_dep_count#95,hd_vehicle_count#96] parquet
               :     :     :     :     +- Project [d_date_sk#97, d_date#99, d_week_seq#101]
               :     :     :     :        +- Filter (((isnotnull(d_year#103) AND (d_year#103 = 1999)) AND isnotnull(d_date_sk#97)) AND (isnotnull(d_week_seq#101) AND isnotnull(d_date#99)))
               :     :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#97,d_date_id#98,d_date#99,d_month_seq#100,d_week_seq#101,d_quarter_seq#102,d_year#103,d_dow#104,d_moy#105,d_dom#106,d_qoy#107,d_fy_year#108,d_fy_quarter_seq#109,d_fy_week_seq#110,d_day_name#111,d_quarter_name#112,d_holiday#113,d_weekend#114,d_following_holiday#115,d_first_dom#116,d_last_dom#117,d_same_day_ly#118,d_same_day_lq#119,d_current_day#120,... 4 more fields] parquet
               :     :     :     +- Project [d_date_sk#171, d_week_seq#175]
               :     :     :        +- Filter (isnotnull(d_week_seq#175) AND isnotnull(d_date_sk#171))
               :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#171,d_date_id#172,d_date#173,d_month_seq#174,d_week_seq#175,d_quarter_seq#176,d_year#177,d_dow#178,d_moy#179,d_dom#180,d_qoy#181,d_fy_year#182,d_fy_quarter_seq#183,d_fy_week_seq#184,d_day_name#185,d_quarter_name#186,d_holiday#187,d_weekend#188,d_following_holiday#189,d_first_dom#190,d_last_dom#191,d_same_day_ly#192,d_same_day_lq#193,d_current_day#194,... 4 more fields] parquet
               :     :     +- Project [d_date_sk#199, d_date#201]
               :     :        +- Filter (isnotnull(d_date#201) AND isnotnull(d_date_sk#199))
               :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#199,d_date_id#200,d_date#201,d_month_seq#202,d_week_seq#203,d_quarter_seq#204,d_year#205,d_dow#206,d_moy#207,d_dom#208,d_qoy#209,d_fy_year#210,d_fy_quarter_seq#211,d_fy_week_seq#212,d_day_name#213,d_quarter_name#214,d_holiday#215,d_weekend#216,d_following_holiday#217,d_first_dom#218,d_last_dom#219,d_same_day_ly#220,d_same_day_lq#221,d_current_day#222,... 4 more fields] parquet
               :     +- Project [p_promo_sk#125]
               :        +- Filter isnotnull(p_promo_sk#125)
               :           +- Relation spark_catalog.tpcds.promotion[p_promo_sk#125,p_promo_id#126,p_start_date_sk#127,p_end_date_sk#128,p_item_sk#129,p_cost#130,p_response_target#131,p_promo_name#132,p_channel_dmail#133,p_channel_email#134,p_channel_catalog#135,p_channel_tv#136,p_channel_radio#137,p_channel_press#138,p_channel_event#139,p_channel_demo#140,p_channel_details#141,p_purpose#142,p_discount_active#143] parquet
               +- Project [cr_item_sk#146, cr_order_number#160]
                  +- Filter (isnotnull(cr_item_sk#146) AND isnotnull(cr_order_number#160))
                     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#144,cr_returned_time_sk#145,cr_item_sk#146,cr_refunded_customer_sk#147,cr_refunded_cdemo_sk#148,cr_refunded_hdemo_sk#149,cr_refunded_addr_sk#150,cr_returning_customer_sk#151,cr_returning_cdemo_sk#152,cr_returning_hdemo_sk#153,cr_returning_addr_sk#154,cr_call_center_sk#155,cr_catalog_page_sk#156,cr_ship_mode_sk#157,cr_warehouse_sk#158,cr_reason_sk#159,cr_order_number#160,cr_return_quantity#161,cr_return_amount#162,cr_return_tax#163,cr_return_amt_inc_tax#164,cr_fee#165,cr_return_ship_cost#166,cr_refunded_cash#167,... 3 more fields] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[total_cnt#2L DESC NULLS LAST,i_item_desc#65 ASC NULLS FIRST,w_warehouse_name#49 ASC NULLS FIRST,d_week_seq#101 ASC NULLS FIRST], output=[i_item_desc#65,w_warehouse_name#49,d_week_seq#101,no_promo#0L,promo#1L,total_cnt#2L])
   +- HashAggregate(keys=[i_item_desc#65, w_warehouse_name#49, d_week_seq#101], functions=[sum(CASE WHEN isnull(p_promo_sk#125) THEN 1 ELSE 0 END), sum(CASE WHEN isnotnull(p_promo_sk#125) THEN 1 ELSE 0 END), count(1)], output=[i_item_desc#65, w_warehouse_name#49, d_week_seq#101, no_promo#0L, promo#1L, total_cnt#2L])
      +- Exchange hashpartitioning(i_item_desc#65, w_warehouse_name#49, d_week_seq#101, 200), ENSURE_REQUIREMENTS, [plan_id=252]
         +- HashAggregate(keys=[i_item_desc#65, w_warehouse_name#49, d_week_seq#101], functions=[partial_sum(CASE WHEN isnull(p_promo_sk#125) THEN 1 ELSE 0 END), partial_sum(CASE WHEN isnotnull(p_promo_sk#125) THEN 1 ELSE 0 END), partial_count(1)], output=[i_item_desc#65, w_warehouse_name#49, d_week_seq#101, sum#243L, sum#244L, count#245L])
            +- Project [w_warehouse_name#49, i_item_desc#65, d_week_seq#101, p_promo_sk#125]
               +- BroadcastHashJoin [cs_item_sk#24, cs_order_number#26], [cr_item_sk#146, cr_order_number#160], LeftOuter, BuildRight, false
                  :- Project [cs_item_sk#24, cs_order_number#26, w_warehouse_name#49, i_item_desc#65, d_week_seq#101, p_promo_sk#125]
                  :  +- BroadcastHashJoin [cs_promo_sk#25], [p_promo_sk#125], LeftOuter, BuildRight, false
                  :     :- Project [cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, w_warehouse_name#49, i_item_desc#65, d_week_seq#101]
                  :     :  +- BroadcastHashJoin [cs_ship_date_sk#11], [d_date_sk#199], Inner, BuildRight, (cast(d_date#201 as date) > date_add(cast(d_date#99 as date), 5)), false
                  :     :     :- Project [cs_ship_date_sk#11, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, w_warehouse_name#49, i_item_desc#65, d_date#99, d_week_seq#101]
                  :     :     :  +- BroadcastHashJoin [d_week_seq#101, inv_date_sk#43], [d_week_seq#175, d_date_sk#171], Inner, BuildRight, false
                  :     :     :     :- Project [cs_ship_date_sk#11, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49, i_item_desc#65, d_date#99, d_week_seq#101]
                  :     :     :     :  +- BroadcastHashJoin [cs_sold_date_sk#9], [d_date_sk#97], Inner, BuildRight, false
                  :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49, i_item_desc#65]
                  :     :     :     :     :  +- BroadcastHashJoin [cs_bill_hdemo_sk#14], [hd_demo_sk#92], Inner, BuildRight, false
                  :     :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_bill_hdemo_sk#14, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49, i_item_desc#65]
                  :     :     :     :     :     :  +- BroadcastHashJoin [cs_bill_cdemo_sk#13], [cd_demo_sk#83], Inner, BuildRight, false
                  :     :     :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_bill_cdemo_sk#13, cs_bill_hdemo_sk#14, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49, i_item_desc#65]
                  :     :     :     :     :     :     :  +- BroadcastHashJoin [cs_item_sk#24], [i_item_sk#61], Inner, BuildRight, false
                  :     :     :     :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_bill_cdemo_sk#13, cs_bill_hdemo_sk#14, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, w_warehouse_name#49]
                  :     :     :     :     :     :     :     :  +- BroadcastHashJoin [inv_warehouse_sk#45], [w_warehouse_sk#47], Inner, BuildRight, false
                  :     :     :     :     :     :     :     :     :- Project [cs_sold_date_sk#9, cs_ship_date_sk#11, cs_bill_cdemo_sk#13, cs_bill_hdemo_sk#14, cs_item_sk#24, cs_promo_sk#25, cs_order_number#26, inv_date_sk#43, inv_warehouse_sk#45]
                  :     :     :     :     :     :     :     :     :  +- SortMergeJoin [cs_item_sk#24], [inv_item_sk#44], Inner, (inv_quantity_on_hand#46L < cast(cs_quantity#27 as bigint))
                  :     :     :     :     :     :     :     :     :     :- Sort [cs_item_sk#24 ASC NULLS FIRST], false, 0
                  :     :     :     :     :     :     :     :     :     :  +- Exchange hashpartitioning(cs_item_sk#24, 200), ENSURE_REQUIREMENTS, [plan_id=208]
                  :     :     :     :     :     :     :     :     :     :     +- Filter (((((isnotnull(cs_quantity#27) AND isnotnull(cs_item_sk#24)) AND isnotnull(cs_bill_cdemo_sk#13)) AND isnotnull(cs_bill_hdemo_sk#14)) AND isnotnull(cs_sold_date_sk#9)) AND isnotnull(cs_ship_date_sk#11))
                  :     :     :     :     :     :     :     :     :     :        +- FileScan parquet spark_catalog.tpcds.catalog_sales[cs_sold_date_sk#9,cs_ship_date_sk#11,cs_bill_cdemo_sk#13,cs_bill_hdemo_sk#14,cs_item_sk#24,cs_promo_sk#25,cs_order_number#26,cs_quantity#27] Batched: true, DataFilters: [isnotnull(cs_quantity#27), isnotnull(cs_item_sk#24), isnotnull(cs_bill_cdemo_sk#13), isnotnull(c..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_quantity), IsNotNull(cs_item_sk), IsNotNull(cs_bill_cdemo_sk), IsNotNull(cs_bill_hd..., ReadSchema: struct<cs_sold_date_sk:int,cs_ship_date_sk:int,cs_bill_cdemo_sk:int,cs_bill_hdemo_sk:int,cs_item_...
                  :     :     :     :     :     :     :     :     :     +- Sort [inv_item_sk#44 ASC NULLS FIRST], false, 0
                  :     :     :     :     :     :     :     :     :        +- Exchange hashpartitioning(inv_item_sk#44, 200), ENSURE_REQUIREMENTS, [plan_id=209]
                  :     :     :     :     :     :     :     :     :           +- Filter (((isnotnull(inv_quantity_on_hand#46L) AND isnotnull(inv_item_sk#44)) AND isnotnull(inv_warehouse_sk#45)) AND isnotnull(inv_date_sk#43))
                  :     :     :     :     :     :     :     :     :              +- FileScan parquet spark_catalog.tpcds.inventory[inv_date_sk#43,inv_item_sk#44,inv_warehouse_sk#45,inv_quantity_on_hand#46L] Batched: true, DataFilters: [isnotnull(inv_quantity_on_hand#46L), isnotnull(inv_item_sk#44), isnotnull(inv_warehouse_sk#45), ..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_quantity_on_hand), IsNotNull(inv_item_sk), IsNotNull(inv_warehouse_sk), IsNotNull(..., ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_warehouse_sk:int,inv_quantity_on_hand:bigint>
                  :     :     :     :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=215]
                  :     :     :     :     :     :     :     :        +- Filter isnotnull(w_warehouse_sk#47)
                  :     :     :     :     :     :     :     :           +- FileScan parquet spark_catalog.tpcds.warehouse[w_warehouse_sk#47,w_warehouse_name#49] Batched: true, DataFilters: [isnotnull(w_warehouse_sk#47)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
                  :     :     :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=219]
                  :     :     :     :     :     :     :        +- Filter isnotnull(i_item_sk#61)
                  :     :     :     :     :     :     :           +- FileScan parquet spark_catalog.tpcds.item[i_item_sk#61,i_item_desc#65] Batched: true, DataFilters: [isnotnull(i_item_sk#61)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_desc:string>
                  :     :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=223]
                  :     :     :     :     :     :        +- Project [cd_demo_sk#83]
                  :     :     :     :     :     :           +- Filter ((isnotnull(cd_marital_status#85) AND (cd_marital_status#85 = D)) AND isnotnull(cd_demo_sk#83))
                  :     :     :     :     :     :              +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#83,cd_marital_status#85] Batched: true, DataFilters: [isnotnull(cd_marital_status#85), (cd_marital_status#85 = D), isnotnull(cd_demo_sk#83)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_marital_status), EqualTo(cd_marital_status,D), IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string>
                  :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=227]
                  :     :     :     :     :        +- Project [hd_demo_sk#92]
                  :     :     :     :     :           +- Filter ((isnotnull(hd_buy_potential#94) AND (hd_buy_potential#94 = >10000)) AND isnotnull(hd_demo_sk#92))
                  :     :     :     :     :              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#92,hd_buy_potential#94] Batched: true, DataFilters: [isnotnull(hd_buy_potential#94), (hd_buy_potential#94 = >10000), isnotnull(hd_demo_sk#92)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_buy_potential), EqualTo(hd_buy_potential,>10000), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_buy_potential:string>
                  :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=231]
                  :     :     :     :        +- Project [d_date_sk#97, d_date#99, d_week_seq#101]
                  :     :     :     :           +- Filter ((((isnotnull(d_year#103) AND (d_year#103 = 1999)) AND isnotnull(d_date_sk#97)) AND isnotnull(d_week_seq#101)) AND isnotnull(d_date#99))
                  :     :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#97,d_date#99,d_week_seq#101,d_year#103] Batched: true, DataFilters: [isnotnull(d_year#103), (d_year#103 = 1999), isnotnull(d_date_sk#97), isnotnull(d_week_seq#101), ..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,1999), IsNotNull(d_date_sk), IsNotNull(d_week_seq), IsNotNull(..., ReadSchema: struct<d_date_sk:int,d_date:string,d_week_seq:int,d_year:int>
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, false] as bigint), 32) | (cast(input[0, int, false] as bigint) & 4294967295))),false), [plan_id=235]
                  :     :     :        +- Filter (isnotnull(d_week_seq#175) AND isnotnull(d_date_sk#171))
                  :     :     :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#171,d_week_seq#175] Batched: true, DataFilters: [isnotnull(d_week_seq#175), isnotnull(d_date_sk#171)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_week_seq), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_week_seq:int>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=239]
                  :     :        +- Filter (isnotnull(d_date#201) AND isnotnull(d_date_sk#199))
                  :     :           +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#199,d_date#201] Batched: true, DataFilters: [isnotnull(d_date#201), isnotnull(d_date_sk#199)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=243]
                  :        +- Filter isnotnull(p_promo_sk#125)
                  :           +- FileScan parquet spark_catalog.tpcds.promotion[p_promo_sk#125] Batched: true, DataFilters: [isnotnull(p_promo_sk#125)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int>
                  +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, false] as bigint), 32) | (cast(input[1, int, false] as bigint) & 4294967295))),false), [plan_id=247]
                     +- Filter (isnotnull(cr_item_sk#146) AND isnotnull(cr_order_number#160))
                        +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_item_sk#146,cr_order_number#160] Batched: true, DataFilters: [isnotnull(cr_item_sk#146), isnotnull(cr_order_number#160)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_item_sk), IsNotNull(cr_order_number)], ReadSchema: struct<cr_item_sk:int,cr_order_number:int>

Time taken: 3.228 seconds, Fetched 1 row(s)
