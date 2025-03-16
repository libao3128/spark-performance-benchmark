== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['total_cnt DESC NULLS LAST, 'i_item_desc ASC NULLS FIRST, 'w_warehouse_name ASC NULLS FIRST, 'd_week_seq ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_desc, 'w_warehouse_name, 'd1.d_week_seq], ['i_item_desc, 'w_warehouse_name, 'd1.d_week_seq, 'sum(CASE WHEN isnull('p_promo_sk) THEN 1 ELSE 0 END) AS no_promo#0, 'sum(CASE WHEN isnotnull('p_promo_sk) THEN 1 ELSE 0 END) AS promo#1, 'count(1) AS total_cnt#2]
         +- 'Filter (((('d1.d_week_seq = 'd2.d_week_seq) && ('inv_quantity_on_hand < 'cs_quantity)) && ('d3.d_date > 'date_add(cast('d1.d_date as date), 5))) && ((('hd_buy_potential = >10000) && ('d1.d_year = 1999)) && ('cd_marital_status = D)))
            +- 'Join LeftOuter, (('cr_item_sk = 'cs_item_sk) && ('cr_order_number = 'cs_order_number))
               :- 'Join LeftOuter, ('cs_promo_sk = 'p_promo_sk)
               :  :- 'Join Inner, ('cs_ship_date_sk = 'd3.d_date_sk)
               :  :  :- 'Join Inner, ('inv_date_sk = 'd2.d_date_sk)
               :  :  :  :- 'Join Inner, ('cs_sold_date_sk = 'd1.d_date_sk)
               :  :  :  :  :- 'Join Inner, ('cs_bill_hdemo_sk = 'hd_demo_sk)
               :  :  :  :  :  :- 'Join Inner, ('cs_bill_cdemo_sk = 'cd_demo_sk)
               :  :  :  :  :  :  :- 'Join Inner, ('i_item_sk = 'cs_item_sk)
               :  :  :  :  :  :  :  :- 'Join Inner, ('w_warehouse_sk = 'inv_warehouse_sk)
               :  :  :  :  :  :  :  :  :- 'Join Inner, ('cs_item_sk = 'inv_item_sk)
               :  :  :  :  :  :  :  :  :  :- 'UnresolvedRelation `catalog_sales`
               :  :  :  :  :  :  :  :  :  +- 'UnresolvedRelation `inventory`
               :  :  :  :  :  :  :  :  +- 'UnresolvedRelation `warehouse`
               :  :  :  :  :  :  :  +- 'UnresolvedRelation `item`
               :  :  :  :  :  :  +- 'UnresolvedRelation `customer_demographics`
               :  :  :  :  :  +- 'UnresolvedRelation `household_demographics`
               :  :  :  :  +- 'SubqueryAlias `d1`
               :  :  :  :     +- 'UnresolvedRelation `date_dim`
               :  :  :  +- 'SubqueryAlias `d2`
               :  :  :     +- 'UnresolvedRelation `date_dim`
               :  :  +- 'SubqueryAlias `d3`
               :  :     +- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `promotion`
               +- 'UnresolvedRelation `catalog_returns`

== Analyzed Logical Plan ==
i_item_desc: string, w_warehouse_name: string, d_week_seq: int, no_promo: bigint, promo: bigint, total_cnt: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [total_cnt#2L DESC NULLS LAST, i_item_desc#62 ASC NULLS FIRST, w_warehouse_name#46 ASC NULLS FIRST, d_week_seq#98 ASC NULLS FIRST], true
      +- Aggregate [i_item_desc#62, w_warehouse_name#46, d_week_seq#98], [i_item_desc#62, w_warehouse_name#46, d_week_seq#98, sum(cast(CASE WHEN isnull(p_promo_sk#122) THEN 1 ELSE 0 END as bigint)) AS no_promo#0L, sum(cast(CASE WHEN isnotnull(p_promo_sk#122) THEN 1 ELSE 0 END as bigint)) AS promo#1L, count(1) AS total_cnt#2L]
         +- Filter ((((d_week_seq#98 = d_week_seq#172) && (inv_quantity_on_hand#43L < cast(cs_quantity#24 as bigint))) && (d_date#198 > cast(date_add(cast(d_date#96 as date), 5) as string))) && (((hd_buy_potential#91 = >10000) && (d_year#100 = 1999)) && (cd_marital_status#82 = D)))
            +- Join LeftOuter, ((cr_item_sk#143 = cs_item_sk#21) && (cr_order_number#157 = cs_order_number#23))
               :- Join LeftOuter, (cs_promo_sk#22 = p_promo_sk#122)
               :  :- Join Inner, (cs_ship_date_sk#8 = d_date_sk#196)
               :  :  :- Join Inner, (inv_date_sk#40 = d_date_sk#168)
               :  :  :  :- Join Inner, (cs_sold_date_sk#6 = d_date_sk#94)
               :  :  :  :  :- Join Inner, (cs_bill_hdemo_sk#11 = hd_demo_sk#89)
               :  :  :  :  :  :- Join Inner, (cs_bill_cdemo_sk#10 = cd_demo_sk#80)
               :  :  :  :  :  :  :- Join Inner, (i_item_sk#58 = cs_item_sk#21)
               :  :  :  :  :  :  :  :- Join Inner, (w_warehouse_sk#44 = inv_warehouse_sk#42)
               :  :  :  :  :  :  :  :  :- Join Inner, (cs_item_sk#21 = inv_item_sk#41)
               :  :  :  :  :  :  :  :  :  :- SubqueryAlias `tpcds`.`catalog_sales`
               :  :  :  :  :  :  :  :  :  :  +- Relation[cs_sold_date_sk#6,cs_sold_time_sk#7,cs_ship_date_sk#8,cs_bill_customer_sk#9,cs_bill_cdemo_sk#10,cs_bill_hdemo_sk#11,cs_bill_addr_sk#12,cs_ship_customer_sk#13,cs_ship_cdemo_sk#14,cs_ship_hdemo_sk#15,cs_ship_addr_sk#16,cs_call_center_sk#17,cs_catalog_page_sk#18,cs_ship_mode_sk#19,cs_warehouse_sk#20,cs_item_sk#21,cs_promo_sk#22,cs_order_number#23,cs_quantity#24,cs_wholesale_cost#25,cs_list_price#26,cs_sales_price#27,cs_ext_discount_amt#28,cs_ext_sales_price#29,... 10 more fields] parquet
               :  :  :  :  :  :  :  :  :  +- SubqueryAlias `tpcds`.`inventory`
               :  :  :  :  :  :  :  :  :     +- Relation[inv_date_sk#40,inv_item_sk#41,inv_warehouse_sk#42,inv_quantity_on_hand#43L] parquet
               :  :  :  :  :  :  :  :  +- SubqueryAlias `tpcds`.`warehouse`
               :  :  :  :  :  :  :  :     +- Relation[w_warehouse_sk#44,w_warehouse_id#45,w_warehouse_name#46,w_warehouse_sq_ft#47,w_street_number#48,w_street_name#49,w_street_type#50,w_suite_number#51,w_city#52,w_county#53,w_state#54,w_zip#55,w_country#56,w_gmt_offset#57] parquet
               :  :  :  :  :  :  :  +- SubqueryAlias `tpcds`.`item`
               :  :  :  :  :  :  :     +- Relation[i_item_sk#58,i_item_id#59,i_rec_start_date#60,i_rec_end_date#61,i_item_desc#62,i_current_price#63,i_wholesale_cost#64,i_brand_id#65,i_brand#66,i_class_id#67,i_class#68,i_category_id#69,i_category#70,i_manufact_id#71,i_manufact#72,i_size#73,i_formulation#74,i_color#75,i_units#76,i_container#77,i_manager_id#78,i_product_name#79] parquet
               :  :  :  :  :  :  +- SubqueryAlias `tpcds`.`customer_demographics`
               :  :  :  :  :  :     +- Relation[cd_demo_sk#80,cd_gender#81,cd_marital_status#82,cd_education_status#83,cd_purchase_estimate#84,cd_credit_rating#85,cd_dep_count#86,cd_dep_employed_count#87,cd_dep_college_count#88] parquet
               :  :  :  :  :  +- SubqueryAlias `tpcds`.`household_demographics`
               :  :  :  :  :     +- Relation[hd_demo_sk#89,hd_income_band_sk#90,hd_buy_potential#91,hd_dep_count#92,hd_vehicle_count#93] parquet
               :  :  :  :  +- SubqueryAlias `d1`
               :  :  :  :     +- SubqueryAlias `tpcds`.`date_dim`
               :  :  :  :        +- Relation[d_date_sk#94,d_date_id#95,d_date#96,d_month_seq#97,d_week_seq#98,d_quarter_seq#99,d_year#100,d_dow#101,d_moy#102,d_dom#103,d_qoy#104,d_fy_year#105,d_fy_quarter_seq#106,d_fy_week_seq#107,d_day_name#108,d_quarter_name#109,d_holiday#110,d_weekend#111,d_following_holiday#112,d_first_dom#113,d_last_dom#114,d_same_day_ly#115,d_same_day_lq#116,d_current_day#117,... 4 more fields] parquet
               :  :  :  +- SubqueryAlias `d2`
               :  :  :     +- SubqueryAlias `tpcds`.`date_dim`
               :  :  :        +- Relation[d_date_sk#168,d_date_id#169,d_date#170,d_month_seq#171,d_week_seq#172,d_quarter_seq#173,d_year#174,d_dow#175,d_moy#176,d_dom#177,d_qoy#178,d_fy_year#179,d_fy_quarter_seq#180,d_fy_week_seq#181,d_day_name#182,d_quarter_name#183,d_holiday#184,d_weekend#185,d_following_holiday#186,d_first_dom#187,d_last_dom#188,d_same_day_ly#189,d_same_day_lq#190,d_current_day#191,... 4 more fields] parquet
               :  :  +- SubqueryAlias `d3`
               :  :     +- SubqueryAlias `tpcds`.`date_dim`
               :  :        +- Relation[d_date_sk#196,d_date_id#197,d_date#198,d_month_seq#199,d_week_seq#200,d_quarter_seq#201,d_year#202,d_dow#203,d_moy#204,d_dom#205,d_qoy#206,d_fy_year#207,d_fy_quarter_seq#208,d_fy_week_seq#209,d_day_name#210,d_quarter_name#211,d_holiday#212,d_weekend#213,d_following_holiday#214,d_first_dom#215,d_last_dom#216,d_same_day_ly#217,d_same_day_lq#218,d_current_day#219,... 4 more fields] parquet
               :  +- SubqueryAlias `tpcds`.`promotion`
               :     +- Relation[p_promo_sk#122,p_promo_id#123,p_start_date_sk#124,p_end_date_sk#125,p_item_sk#126,p_cost#127,p_response_target#128,p_promo_name#129,p_channel_dmail#130,p_channel_email#131,p_channel_catalog#132,p_channel_tv#133,p_channel_radio#134,p_channel_press#135,p_channel_event#136,p_channel_demo#137,p_channel_details#138,p_purpose#139,p_discount_active#140] parquet
               +- SubqueryAlias `tpcds`.`catalog_returns`
                  +- Relation[cr_returned_date_sk#141,cr_returned_time_sk#142,cr_item_sk#143,cr_refunded_customer_sk#144,cr_refunded_cdemo_sk#145,cr_refunded_hdemo_sk#146,cr_refunded_addr_sk#147,cr_returning_customer_sk#148,cr_returning_cdemo_sk#149,cr_returning_hdemo_sk#150,cr_returning_addr_sk#151,cr_call_center_sk#152,cr_catalog_page_sk#153,cr_ship_mode_sk#154,cr_warehouse_sk#155,cr_reason_sk#156,cr_order_number#157,cr_return_quantity#158,cr_return_amount#159,cr_return_tax#160,cr_return_amt_inc_tax#161,cr_fee#162,cr_return_ship_cost#163,cr_refunded_cash#164,... 3 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [total_cnt#2L DESC NULLS LAST, i_item_desc#62 ASC NULLS FIRST, w_warehouse_name#46 ASC NULLS FIRST, d_week_seq#98 ASC NULLS FIRST], true
      +- Aggregate [i_item_desc#62, w_warehouse_name#46, d_week_seq#98], [i_item_desc#62, w_warehouse_name#46, d_week_seq#98, sum(cast(CASE WHEN isnull(p_promo_sk#122) THEN 1 ELSE 0 END as bigint)) AS no_promo#0L, sum(cast(CASE WHEN isnotnull(p_promo_sk#122) THEN 1 ELSE 0 END as bigint)) AS promo#1L, count(1) AS total_cnt#2L]
         +- Project [w_warehouse_name#46, i_item_desc#62, d_week_seq#98, p_promo_sk#122]
            +- Join LeftOuter, ((cr_item_sk#143 = cs_item_sk#21) && (cr_order_number#157 = cs_order_number#23))
               :- Project [cs_item_sk#21, cs_order_number#23, w_warehouse_name#46, i_item_desc#62, d_week_seq#98, p_promo_sk#122]
               :  +- Join LeftOuter, (cs_promo_sk#22 = p_promo_sk#122)
               :     :- Project [cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, w_warehouse_name#46, i_item_desc#62, d_week_seq#98]
               :     :  +- Join Inner, ((d_date#198 > cast(date_add(cast(d_date#96 as date), 5) as string)) && (cs_ship_date_sk#8 = d_date_sk#196))
               :     :     :- Project [cs_ship_date_sk#8, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, w_warehouse_name#46, i_item_desc#62, d_date#96, d_week_seq#98]
               :     :     :  +- Join Inner, ((d_week_seq#98 = d_week_seq#172) && (inv_date_sk#40 = d_date_sk#168))
               :     :     :     :- Project [cs_ship_date_sk#8, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46, i_item_desc#62, d_date#96, d_week_seq#98]
               :     :     :     :  +- Join Inner, (cs_sold_date_sk#6 = d_date_sk#94)
               :     :     :     :     :- Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46, i_item_desc#62]
               :     :     :     :     :  +- Join Inner, (cs_bill_hdemo_sk#11 = hd_demo_sk#89)
               :     :     :     :     :     :- Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46, i_item_desc#62]
               :     :     :     :     :     :  +- Join Inner, (cs_bill_cdemo_sk#10 = cd_demo_sk#80)
               :     :     :     :     :     :     :- Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_cdemo_sk#10, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46, i_item_desc#62]
               :     :     :     :     :     :     :  +- Join Inner, (i_item_sk#58 = cs_item_sk#21)
               :     :     :     :     :     :     :     :- Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_cdemo_sk#10, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46]
               :     :     :     :     :     :     :     :  +- Join Inner, (w_warehouse_sk#44 = inv_warehouse_sk#42)
               :     :     :     :     :     :     :     :     :- Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_cdemo_sk#10, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, inv_warehouse_sk#42]
               :     :     :     :     :     :     :     :     :  +- Join Inner, ((inv_quantity_on_hand#43L < cast(cs_quantity#24 as bigint)) && (cs_item_sk#21 = inv_item_sk#41))
               :     :     :     :     :     :     :     :     :     :- Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_cdemo_sk#10, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, cs_quantity#24]
               :     :     :     :     :     :     :     :     :     :  +- Filter (((((isnotnull(cs_quantity#24) && isnotnull(cs_item_sk#21)) && isnotnull(cs_bill_cdemo_sk#10)) && isnotnull(cs_bill_hdemo_sk#11)) && isnotnull(cs_sold_date_sk#6)) && isnotnull(cs_ship_date_sk#8))
               :     :     :     :     :     :     :     :     :     :     +- Relation[cs_sold_date_sk#6,cs_sold_time_sk#7,cs_ship_date_sk#8,cs_bill_customer_sk#9,cs_bill_cdemo_sk#10,cs_bill_hdemo_sk#11,cs_bill_addr_sk#12,cs_ship_customer_sk#13,cs_ship_cdemo_sk#14,cs_ship_hdemo_sk#15,cs_ship_addr_sk#16,cs_call_center_sk#17,cs_catalog_page_sk#18,cs_ship_mode_sk#19,cs_warehouse_sk#20,cs_item_sk#21,cs_promo_sk#22,cs_order_number#23,cs_quantity#24,cs_wholesale_cost#25,cs_list_price#26,cs_sales_price#27,cs_ext_discount_amt#28,cs_ext_sales_price#29,... 10 more fields] parquet
               :     :     :     :     :     :     :     :     :     +- Filter (((isnotnull(inv_quantity_on_hand#43L) && isnotnull(inv_item_sk#41)) && isnotnull(inv_warehouse_sk#42)) && isnotnull(inv_date_sk#40))
               :     :     :     :     :     :     :     :     :        +- Relation[inv_date_sk#40,inv_item_sk#41,inv_warehouse_sk#42,inv_quantity_on_hand#43L] parquet
               :     :     :     :     :     :     :     :     +- Project [w_warehouse_sk#44, w_warehouse_name#46]
               :     :     :     :     :     :     :     :        +- Filter isnotnull(w_warehouse_sk#44)
               :     :     :     :     :     :     :     :           +- Relation[w_warehouse_sk#44,w_warehouse_id#45,w_warehouse_name#46,w_warehouse_sq_ft#47,w_street_number#48,w_street_name#49,w_street_type#50,w_suite_number#51,w_city#52,w_county#53,w_state#54,w_zip#55,w_country#56,w_gmt_offset#57] parquet
               :     :     :     :     :     :     :     +- Project [i_item_sk#58, i_item_desc#62]
               :     :     :     :     :     :     :        +- Filter isnotnull(i_item_sk#58)
               :     :     :     :     :     :     :           +- Relation[i_item_sk#58,i_item_id#59,i_rec_start_date#60,i_rec_end_date#61,i_item_desc#62,i_current_price#63,i_wholesale_cost#64,i_brand_id#65,i_brand#66,i_class_id#67,i_class#68,i_category_id#69,i_category#70,i_manufact_id#71,i_manufact#72,i_size#73,i_formulation#74,i_color#75,i_units#76,i_container#77,i_manager_id#78,i_product_name#79] parquet
               :     :     :     :     :     :     +- Project [cd_demo_sk#80]
               :     :     :     :     :     :        +- Filter ((isnotnull(cd_marital_status#82) && (cd_marital_status#82 = D)) && isnotnull(cd_demo_sk#80))
               :     :     :     :     :     :           +- Relation[cd_demo_sk#80,cd_gender#81,cd_marital_status#82,cd_education_status#83,cd_purchase_estimate#84,cd_credit_rating#85,cd_dep_count#86,cd_dep_employed_count#87,cd_dep_college_count#88] parquet
               :     :     :     :     :     +- Project [hd_demo_sk#89]
               :     :     :     :     :        +- Filter ((isnotnull(hd_buy_potential#91) && (hd_buy_potential#91 = >10000)) && isnotnull(hd_demo_sk#89))
               :     :     :     :     :           +- Relation[hd_demo_sk#89,hd_income_band_sk#90,hd_buy_potential#91,hd_dep_count#92,hd_vehicle_count#93] parquet
               :     :     :     :     +- Project [d_date_sk#94, d_date#96, d_week_seq#98]
               :     :     :     :        +- Filter (((isnotnull(d_year#100) && (d_year#100 = 1999)) && isnotnull(d_date_sk#94)) && isnotnull(d_week_seq#98))
               :     :     :     :           +- Relation[d_date_sk#94,d_date_id#95,d_date#96,d_month_seq#97,d_week_seq#98,d_quarter_seq#99,d_year#100,d_dow#101,d_moy#102,d_dom#103,d_qoy#104,d_fy_year#105,d_fy_quarter_seq#106,d_fy_week_seq#107,d_day_name#108,d_quarter_name#109,d_holiday#110,d_weekend#111,d_following_holiday#112,d_first_dom#113,d_last_dom#114,d_same_day_ly#115,d_same_day_lq#116,d_current_day#117,... 4 more fields] parquet
               :     :     :     +- Project [d_date_sk#168, d_week_seq#172]
               :     :     :        +- Filter (isnotnull(d_date_sk#168) && isnotnull(d_week_seq#172))
               :     :     :           +- Relation[d_date_sk#168,d_date_id#169,d_date#170,d_month_seq#171,d_week_seq#172,d_quarter_seq#173,d_year#174,d_dow#175,d_moy#176,d_dom#177,d_qoy#178,d_fy_year#179,d_fy_quarter_seq#180,d_fy_week_seq#181,d_day_name#182,d_quarter_name#183,d_holiday#184,d_weekend#185,d_following_holiday#186,d_first_dom#187,d_last_dom#188,d_same_day_ly#189,d_same_day_lq#190,d_current_day#191,... 4 more fields] parquet
               :     :     +- Project [d_date_sk#196, d_date#198]
               :     :        +- Filter (isnotnull(d_date#198) && isnotnull(d_date_sk#196))
               :     :           +- Relation[d_date_sk#196,d_date_id#197,d_date#198,d_month_seq#199,d_week_seq#200,d_quarter_seq#201,d_year#202,d_dow#203,d_moy#204,d_dom#205,d_qoy#206,d_fy_year#207,d_fy_quarter_seq#208,d_fy_week_seq#209,d_day_name#210,d_quarter_name#211,d_holiday#212,d_weekend#213,d_following_holiday#214,d_first_dom#215,d_last_dom#216,d_same_day_ly#217,d_same_day_lq#218,d_current_day#219,... 4 more fields] parquet
               :     +- Project [p_promo_sk#122]
               :        +- Filter isnotnull(p_promo_sk#122)
               :           +- Relation[p_promo_sk#122,p_promo_id#123,p_start_date_sk#124,p_end_date_sk#125,p_item_sk#126,p_cost#127,p_response_target#128,p_promo_name#129,p_channel_dmail#130,p_channel_email#131,p_channel_catalog#132,p_channel_tv#133,p_channel_radio#134,p_channel_press#135,p_channel_event#136,p_channel_demo#137,p_channel_details#138,p_purpose#139,p_discount_active#140] parquet
               +- Project [cr_item_sk#143, cr_order_number#157]
                  +- Filter (isnotnull(cr_item_sk#143) && isnotnull(cr_order_number#157))
                     +- Relation[cr_returned_date_sk#141,cr_returned_time_sk#142,cr_item_sk#143,cr_refunded_customer_sk#144,cr_refunded_cdemo_sk#145,cr_refunded_hdemo_sk#146,cr_refunded_addr_sk#147,cr_returning_customer_sk#148,cr_returning_cdemo_sk#149,cr_returning_hdemo_sk#150,cr_returning_addr_sk#151,cr_call_center_sk#152,cr_catalog_page_sk#153,cr_ship_mode_sk#154,cr_warehouse_sk#155,cr_reason_sk#156,cr_order_number#157,cr_return_quantity#158,cr_return_amount#159,cr_return_tax#160,cr_return_amt_inc_tax#161,cr_fee#162,cr_return_ship_cost#163,cr_refunded_cash#164,... 3 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[total_cnt#2L DESC NULLS LAST,i_item_desc#62 ASC NULLS FIRST,w_warehouse_name#46 ASC NULLS FIRST,d_week_seq#98 ASC NULLS FIRST], output=[i_item_desc#62,w_warehouse_name#46,d_week_seq#98,no_promo#0L,promo#1L,total_cnt#2L])
+- *(15) HashAggregate(keys=[i_item_desc#62, w_warehouse_name#46, d_week_seq#98], functions=[sum(cast(CASE WHEN isnull(p_promo_sk#122) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN isnotnull(p_promo_sk#122) THEN 1 ELSE 0 END as bigint)), count(1)], output=[i_item_desc#62, w_warehouse_name#46, d_week_seq#98, no_promo#0L, promo#1L, total_cnt#2L])
   +- Exchange hashpartitioning(i_item_desc#62, w_warehouse_name#46, d_week_seq#98, 200)
      +- *(14) HashAggregate(keys=[i_item_desc#62, w_warehouse_name#46, d_week_seq#98], functions=[partial_sum(cast(CASE WHEN isnull(p_promo_sk#122) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN isnotnull(p_promo_sk#122) THEN 1 ELSE 0 END as bigint)), partial_count(1)], output=[i_item_desc#62, w_warehouse_name#46, d_week_seq#98, sum#233L, sum#234L, count#235L])
         +- *(14) Project [w_warehouse_name#46, i_item_desc#62, d_week_seq#98, p_promo_sk#122]
            +- *(14) BroadcastHashJoin [cs_item_sk#21, cs_order_number#23], [cr_item_sk#143, cr_order_number#157], LeftOuter, BuildRight
               :- *(14) Project [cs_item_sk#21, cs_order_number#23, w_warehouse_name#46, i_item_desc#62, d_week_seq#98, p_promo_sk#122]
               :  +- *(14) BroadcastHashJoin [cs_promo_sk#22], [p_promo_sk#122], LeftOuter, BuildRight
               :     :- *(14) Project [cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, w_warehouse_name#46, i_item_desc#62, d_week_seq#98]
               :     :  +- *(14) BroadcastHashJoin [cs_ship_date_sk#8], [d_date_sk#196], Inner, BuildRight, (d_date#198 > cast(date_add(cast(d_date#96 as date), 5) as string))
               :     :     :- *(14) Project [cs_ship_date_sk#8, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, w_warehouse_name#46, i_item_desc#62, d_date#96, d_week_seq#98]
               :     :     :  +- *(14) BroadcastHashJoin [d_week_seq#98, inv_date_sk#40], [d_week_seq#172, d_date_sk#168], Inner, BuildRight
               :     :     :     :- *(14) Project [cs_ship_date_sk#8, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46, i_item_desc#62, d_date#96, d_week_seq#98]
               :     :     :     :  +- *(14) BroadcastHashJoin [cs_sold_date_sk#6], [d_date_sk#94], Inner, BuildRight
               :     :     :     :     :- *(14) Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46, i_item_desc#62]
               :     :     :     :     :  +- *(14) BroadcastHashJoin [cs_bill_hdemo_sk#11], [hd_demo_sk#89], Inner, BuildRight
               :     :     :     :     :     :- *(14) Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46, i_item_desc#62]
               :     :     :     :     :     :  +- *(14) BroadcastHashJoin [cs_bill_cdemo_sk#10], [cd_demo_sk#80], Inner, BuildRight
               :     :     :     :     :     :     :- *(14) Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_cdemo_sk#10, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46, i_item_desc#62]
               :     :     :     :     :     :     :  +- *(14) BroadcastHashJoin [cs_item_sk#21], [i_item_sk#58], Inner, BuildRight
               :     :     :     :     :     :     :     :- *(14) Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_cdemo_sk#10, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, w_warehouse_name#46]
               :     :     :     :     :     :     :     :  +- *(14) BroadcastHashJoin [inv_warehouse_sk#42], [w_warehouse_sk#44], Inner, BuildRight
               :     :     :     :     :     :     :     :     :- *(14) Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_cdemo_sk#10, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, inv_date_sk#40, inv_warehouse_sk#42]
               :     :     :     :     :     :     :     :     :  +- *(14) SortMergeJoin [cs_item_sk#21], [inv_item_sk#41], Inner, (inv_quantity_on_hand#43L < cast(cs_quantity#24 as bigint))
               :     :     :     :     :     :     :     :     :     :- *(2) Sort [cs_item_sk#21 ASC NULLS FIRST], false, 0
               :     :     :     :     :     :     :     :     :     :  +- Exchange hashpartitioning(cs_item_sk#21, 200)
               :     :     :     :     :     :     :     :     :     :     +- *(1) Project [cs_sold_date_sk#6, cs_ship_date_sk#8, cs_bill_cdemo_sk#10, cs_bill_hdemo_sk#11, cs_item_sk#21, cs_promo_sk#22, cs_order_number#23, cs_quantity#24]
               :     :     :     :     :     :     :     :     :     :        +- *(1) Filter (((((isnotnull(cs_quantity#24) && isnotnull(cs_item_sk#21)) && isnotnull(cs_bill_cdemo_sk#10)) && isnotnull(cs_bill_hdemo_sk#11)) && isnotnull(cs_sold_date_sk#6)) && isnotnull(cs_ship_date_sk#8))
               :     :     :     :     :     :     :     :     :     :           +- *(1) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#6,cs_ship_date_sk#8,cs_bill_cdemo_sk#10,cs_bill_hdemo_sk#11,cs_item_sk#21,cs_promo_sk#22,cs_order_number#23,cs_quantity#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_quantity), IsNotNull(cs_item_sk), IsNotNull(cs_bill_cdemo_sk), IsNotNull(cs_bill_hd..., ReadSchema: struct<cs_sold_date_sk:int,cs_ship_date_sk:int,cs_bill_cdemo_sk:int,cs_bill_hdemo_sk:int,cs_item_...
               :     :     :     :     :     :     :     :     :     +- *(4) Sort [inv_item_sk#41 ASC NULLS FIRST], false, 0
               :     :     :     :     :     :     :     :     :        +- Exchange hashpartitioning(inv_item_sk#41, 200)
               :     :     :     :     :     :     :     :     :           +- *(3) Project [inv_date_sk#40, inv_item_sk#41, inv_warehouse_sk#42, inv_quantity_on_hand#43L]
               :     :     :     :     :     :     :     :     :              +- *(3) Filter (((isnotnull(inv_quantity_on_hand#43L) && isnotnull(inv_item_sk#41)) && isnotnull(inv_warehouse_sk#42)) && isnotnull(inv_date_sk#40))
               :     :     :     :     :     :     :     :     :                 +- *(3) FileScan parquet tpcds.inventory[inv_date_sk#40,inv_item_sk#41,inv_warehouse_sk#42,inv_quantity_on_hand#43L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(inv_quantity_on_hand), IsNotNull(inv_item_sk), IsNotNull(inv_warehouse_sk), IsNotNull(..., ReadSchema: struct<inv_date_sk:int,inv_item_sk:int,inv_warehouse_sk:int,inv_quantity_on_hand:bigint>
               :     :     :     :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :     :     :     :     :        +- *(5) Project [w_warehouse_sk#44, w_warehouse_name#46]
               :     :     :     :     :     :     :     :           +- *(5) Filter isnotnull(w_warehouse_sk#44)
               :     :     :     :     :     :     :     :              +- *(5) FileScan parquet tpcds.warehouse[w_warehouse_sk#44,w_warehouse_name#46] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(w_warehouse_sk)], ReadSchema: struct<w_warehouse_sk:int,w_warehouse_name:string>
               :     :     :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :     :     :     :        +- *(6) Project [i_item_sk#58, i_item_desc#62]
               :     :     :     :     :     :     :           +- *(6) Filter isnotnull(i_item_sk#58)
               :     :     :     :     :     :     :              +- *(6) FileScan parquet tpcds.item[i_item_sk#58,i_item_desc#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_desc:string>
               :     :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :     :     :        +- *(7) Project [cd_demo_sk#80]
               :     :     :     :     :     :           +- *(7) Filter ((isnotnull(cd_marital_status#82) && (cd_marital_status#82 = D)) && isnotnull(cd_demo_sk#80))
               :     :     :     :     :     :              +- *(7) FileScan parquet tpcds.customer_demographics[cd_demo_sk#80,cd_marital_status#82] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_marital_status), EqualTo(cd_marital_status,D), IsNotNull(cd_demo_sk)], ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string>
               :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :     :        +- *(8) Project [hd_demo_sk#89]
               :     :     :     :     :           +- *(8) Filter ((isnotnull(hd_buy_potential#91) && (hd_buy_potential#91 = >10000)) && isnotnull(hd_demo_sk#89))
               :     :     :     :     :              +- *(8) FileScan parquet tpcds.household_demographics[hd_demo_sk#89,hd_buy_potential#91] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_buy_potential), EqualTo(hd_buy_potential,>10000), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_buy_potential:string>
               :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :        +- *(9) Project [d_date_sk#94, d_date#96, d_week_seq#98]
               :     :     :     :           +- *(9) Filter (((isnotnull(d_year#100) && (d_year#100 = 1999)) && isnotnull(d_date_sk#94)) && isnotnull(d_week_seq#98))
               :     :     :     :              +- *(9) FileScan parquet tpcds.date_dim[d_date_sk#94,d_date#96,d_week_seq#98,d_year#100] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,1999), IsNotNull(d_date_sk), IsNotNull(d_week_seq)], ReadSchema: struct<d_date_sk:int,d_date:string,d_week_seq:int,d_year:int>
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
               :     :     :        +- *(10) Project [d_date_sk#168, d_week_seq#172]
               :     :     :           +- *(10) Filter (isnotnull(d_date_sk#168) && isnotnull(d_week_seq#172))
               :     :     :              +- *(10) FileScan parquet tpcds.date_dim[d_date_sk#168,d_week_seq#172] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk), IsNotNull(d_week_seq)], ReadSchema: struct<d_date_sk:int,d_week_seq:int>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(11) Project [d_date_sk#196, d_date#198]
               :     :           +- *(11) Filter (isnotnull(d_date#198) && isnotnull(d_date_sk#196))
               :     :              +- *(11) FileScan parquet tpcds.date_dim[d_date_sk#196,d_date#198] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(12) Project [p_promo_sk#122]
               :           +- *(12) Filter isnotnull(p_promo_sk#122)
               :              +- *(12) FileScan parquet tpcds.promotion[p_promo_sk#122] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/p..., PartitionFilters: [], PushedFilters: [IsNotNull(p_promo_sk)], ReadSchema: struct<p_promo_sk:int>
               +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, true] as bigint), 32) | (cast(input[1, int, true] as bigint) & 4294967295))))
                  +- *(13) Project [cr_item_sk#143, cr_order_number#157]
                     +- *(13) Filter (isnotnull(cr_item_sk#143) && isnotnull(cr_order_number#157))
                        +- *(13) FileScan parquet tpcds.catalog_returns[cr_item_sk#143,cr_order_number#157] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_item_sk), IsNotNull(cr_order_number)], ReadSchema: struct<cr_item_sk:int,cr_order_number:int>
Time taken: 5.078 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 72 in stream 0 using template query72.tpl
------------------------------------------------------^^^

