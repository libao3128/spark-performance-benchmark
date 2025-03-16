== Parsed Logical Plan ==
CTE [v1, v2]
:  :- 'SubqueryAlias `v1`
:  :  +- 'Aggregate ['i_category, 'i_brand, 'cc_name, 'd_year, 'd_moy], ['i_category, 'i_brand, 'cc_name, 'd_year, 'd_moy, 'sum('cs_sales_price) AS sum_sales#0, 'avg('sum('cs_sales_price)) windowspecdefinition('i_category, 'i_brand, 'cc_name, 'd_year, unspecifiedframe$()) AS avg_monthly_sales#1, 'rank() windowspecdefinition('i_category, 'i_brand, 'cc_name, 'd_year ASC NULLS FIRST, 'd_moy ASC NULLS FIRST, unspecifiedframe$()) AS rn#2]
:  :     +- 'Filter ((('cs_item_sk = 'i_item_sk) && ('cs_sold_date_sk = 'd_date_sk)) && (('cc_call_center_sk = 'cs_call_center_sk) && ((('d_year = 1999) || (('d_year = (1999 - 1)) && ('d_moy = 12))) || (('d_year = (1999 + 1)) && ('d_moy = 1)))))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'UnresolvedRelation `item`
:  :           :  :  +- 'UnresolvedRelation `catalog_sales`
:  :           :  +- 'UnresolvedRelation `date_dim`
:  :           +- 'UnresolvedRelation `call_center`
:  +- 'SubqueryAlias `v2`
:     +- 'Project ['v1.i_category, 'v1.i_brand, 'v1.cc_name, 'v1.d_year, 'v1.d_moy, 'v1.avg_monthly_sales, 'v1.sum_sales, 'v1_lag.sum_sales AS psum#3, 'v1_lead.sum_sales AS nsum#4]
:        +- 'Filter (((('v1.i_category = 'v1_lag.i_category) && ('v1.i_category = 'v1_lead.i_category)) && (('v1.i_brand = 'v1_lag.i_brand) && ('v1.i_brand = 'v1_lead.i_brand))) && ((('v1.cc_name = 'v1_lag.cc_name) && ('v1.cc_name = 'v1_lead.cc_name)) && (('v1.rn = ('v1_lag.rn + 1)) && ('v1.rn = ('v1_lead.rn - 1)))))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation `v1`
:              :  +- 'SubqueryAlias `v1_lag`
:              :     +- 'UnresolvedRelation `v1`
:              +- 'SubqueryAlias `v1_lead`
:                 +- 'UnresolvedRelation `v1`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort [('sum_sales - 'avg_monthly_sales) ASC NULLS FIRST, 3 ASC NULLS FIRST], true
         +- 'Project [*]
            +- 'Filter ((('d_year = 1999) && ('avg_monthly_sales > 0)) && (CASE WHEN ('avg_monthly_sales > 0) THEN ('abs(('sum_sales - 'avg_monthly_sales)) / 'avg_monthly_sales) ELSE null END > 0.1))
               +- 'UnresolvedRelation `v2`

== Analyzed Logical Plan ==
i_category: string, i_brand: string, cc_name: string, d_year: int, d_moy: int, avg_monthly_sales: double, sum_sales: double, psum: double, nsum: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, cc_name#99 ASC NULLS FIRST], true
      +- Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, avg_monthly_sales#1, sum_sales#0, psum#3, nsum#4]
         +- Filter (((d_year#71 = 1999) && (avg_monthly_sales#1 > cast(0 as double))) && (CASE WHEN (avg_monthly_sales#1 > cast(0 as double)) THEN (abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) ELSE cast(null as double) END > cast(0.1 as double)))
            +- SubqueryAlias `v2`
               +- Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, avg_monthly_sales#1, sum_sales#0, sum_sales#333 AS psum#3, sum_sales#820 AS nsum#4]
                  +- Filter ((((i_category#21 = i_category#440) && (i_category#21 = i_category#927)) && ((i_brand#17 = i_brand#436) && (i_brand#17 = i_brand#923))) && (((cc_name#99 = cc_name#602) && (cc_name#99 = cc_name#1089)) && ((rn#2 = (rn#237 + 1)) && (rn#2 = (rn#724 - 1)))))
                     +- Join Inner
                        :- Join Inner
                        :  :- SubqueryAlias `v1`
                        :  :  +- Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum_sales#0, avg_monthly_sales#1, rn#2]
                        :  :     +- Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum_sales#0, _w0#135, rn#2, avg_monthly_sales#1, avg_monthly_sales#1, rn#2]
                        :  :        +- Window [avg(_w0#135) windowspecdefinition(i_category#21, i_brand#17, cc_name#99, d_year#71, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#21, i_brand#17, cc_name#99, d_year#71]
                        :  :           +- Window [rank(d_year#71, d_moy#73) windowspecdefinition(i_category#21, i_brand#17, cc_name#99, d_year#71 ASC NULLS FIRST, d_moy#73 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#21, i_brand#17, cc_name#99], [d_year#71 ASC NULLS FIRST, d_moy#73 ASC NULLS FIRST]
                        :  :              +- Aggregate [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73], [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum(cs_sales_price#52) AS sum_sales#0, sum(cs_sales_price#52) AS _w0#135]
                        :  :                 +- Filter (((cs_item_sk#46 = i_item_sk#9) && (cs_sold_date_sk#31 = d_date_sk#65)) && ((cc_call_center_sk#93 = cs_call_center_sk#42) && (((d_year#71 = 1999) || ((d_year#71 = (1999 - 1)) && (d_moy#73 = 12))) || ((d_year#71 = (1999 + 1)) && (d_moy#73 = 1)))))
                        :  :                    +- Join Inner
                        :  :                       :- Join Inner
                        :  :                       :  :- Join Inner
                        :  :                       :  :  :- SubqueryAlias `tpcds`.`item`
                        :  :                       :  :  :  +- Relation[i_item_sk#9,i_item_id#10,i_rec_start_date#11,i_rec_end_date#12,i_item_desc#13,i_current_price#14,i_wholesale_cost#15,i_brand_id#16,i_brand#17,i_class_id#18,i_class#19,i_category_id#20,i_category#21,i_manufact_id#22,i_manufact#23,i_size#24,i_formulation#25,i_color#26,i_units#27,i_container#28,i_manager_id#29,i_product_name#30] parquet
                        :  :                       :  :  +- SubqueryAlias `tpcds`.`catalog_sales`
                        :  :                       :  :     +- Relation[cs_sold_date_sk#31,cs_sold_time_sk#32,cs_ship_date_sk#33,cs_bill_customer_sk#34,cs_bill_cdemo_sk#35,cs_bill_hdemo_sk#36,cs_bill_addr_sk#37,cs_ship_customer_sk#38,cs_ship_cdemo_sk#39,cs_ship_hdemo_sk#40,cs_ship_addr_sk#41,cs_call_center_sk#42,cs_catalog_page_sk#43,cs_ship_mode_sk#44,cs_warehouse_sk#45,cs_item_sk#46,cs_promo_sk#47,cs_order_number#48,cs_quantity#49,cs_wholesale_cost#50,cs_list_price#51,cs_sales_price#52,cs_ext_discount_amt#53,cs_ext_sales_price#54,... 10 more fields] parquet
                        :  :                       :  +- SubqueryAlias `tpcds`.`date_dim`
                        :  :                       :     +- Relation[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
                        :  :                       +- SubqueryAlias `tpcds`.`call_center`
                        :  :                          +- Relation[cc_call_center_sk#93,cc_call_center_id#94,cc_rec_start_date#95,cc_rec_end_date#96,cc_closed_date_sk#97,cc_open_date_sk#98,cc_name#99,cc_class#100,cc_employees#101,cc_sq_ft#102,cc_hours#103,cc_manager#104,cc_mkt_id#105,cc_mkt_class#106,cc_mkt_desc#107,cc_market_manager#108,cc_division#109,cc_division_name#110,cc_company#111,cc_company_name#112,cc_street_number#113,cc_street_name#114,cc_street_type#115,cc_suite_number#116,... 7 more fields] parquet
                        :  +- SubqueryAlias `v1_lag`
                        :     +- SubqueryAlias `v1`
                        :        +- Project [i_category#440, i_brand#436, cc_name#602, d_year#527, d_moy#529, sum_sales#333, avg_monthly_sales#140, rn#237]
                        :           +- Project [i_category#440, i_brand#436, cc_name#602, d_year#527, d_moy#529, sum_sales#333, _w0#334, rn#237, avg_monthly_sales#140, avg_monthly_sales#140, rn#237]
                        :              +- Window [avg(_w0#334) windowspecdefinition(i_category#440, i_brand#436, cc_name#602, d_year#527, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#140], [i_category#440, i_brand#436, cc_name#602, d_year#527]
                        :                 +- Window [rank(d_year#527, d_moy#529) windowspecdefinition(i_category#440, i_brand#436, cc_name#602, d_year#527 ASC NULLS FIRST, d_moy#529 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#237], [i_category#440, i_brand#436, cc_name#602], [d_year#527 ASC NULLS FIRST, d_moy#529 ASC NULLS FIRST]
                        :                    +- Aggregate [i_category#440, i_brand#436, cc_name#602, d_year#527, d_moy#529], [i_category#440, i_brand#436, cc_name#602, d_year#527, d_moy#529, sum(cs_sales_price#52) AS sum_sales#333, sum(cs_sales_price#52) AS _w0#334]
                        :                       +- Filter (((cs_item_sk#46 = i_item_sk#428) && (cs_sold_date_sk#31 = d_date_sk#521)) && ((cc_call_center_sk#596 = cs_call_center_sk#42) && (((d_year#527 = 1999) || ((d_year#527 = (1999 - 1)) && (d_moy#529 = 12))) || ((d_year#527 = (1999 + 1)) && (d_moy#529 = 1)))))
                        :                          +- Join Inner
                        :                             :- Join Inner
                        :                             :  :- Join Inner
                        :                             :  :  :- SubqueryAlias `tpcds`.`item`
                        :                             :  :  :  +- Relation[i_item_sk#428,i_item_id#429,i_rec_start_date#430,i_rec_end_date#431,i_item_desc#432,i_current_price#433,i_wholesale_cost#434,i_brand_id#435,i_brand#436,i_class_id#437,i_class#438,i_category_id#439,i_category#440,i_manufact_id#441,i_manufact#442,i_size#443,i_formulation#444,i_color#445,i_units#446,i_container#447,i_manager_id#448,i_product_name#449] parquet
                        :                             :  :  +- SubqueryAlias `tpcds`.`catalog_sales`
                        :                             :  :     +- Relation[cs_sold_date_sk#31,cs_sold_time_sk#32,cs_ship_date_sk#33,cs_bill_customer_sk#34,cs_bill_cdemo_sk#35,cs_bill_hdemo_sk#36,cs_bill_addr_sk#37,cs_ship_customer_sk#38,cs_ship_cdemo_sk#39,cs_ship_hdemo_sk#40,cs_ship_addr_sk#41,cs_call_center_sk#42,cs_catalog_page_sk#43,cs_ship_mode_sk#44,cs_warehouse_sk#45,cs_item_sk#46,cs_promo_sk#47,cs_order_number#48,cs_quantity#49,cs_wholesale_cost#50,cs_list_price#51,cs_sales_price#52,cs_ext_discount_amt#53,cs_ext_sales_price#54,... 10 more fields] parquet
                        :                             :  +- SubqueryAlias `tpcds`.`date_dim`
                        :                             :     +- Relation[d_date_sk#521,d_date_id#522,d_date#523,d_month_seq#524,d_week_seq#525,d_quarter_seq#526,d_year#527,d_dow#528,d_moy#529,d_dom#530,d_qoy#531,d_fy_year#532,d_fy_quarter_seq#533,d_fy_week_seq#534,d_day_name#535,d_quarter_name#536,d_holiday#537,d_weekend#538,d_following_holiday#539,d_first_dom#540,d_last_dom#541,d_same_day_ly#542,d_same_day_lq#543,d_current_day#544,... 4 more fields] parquet
                        :                             +- SubqueryAlias `tpcds`.`call_center`
                        :                                +- Relation[cc_call_center_sk#596,cc_call_center_id#597,cc_rec_start_date#598,cc_rec_end_date#599,cc_closed_date_sk#600,cc_open_date_sk#601,cc_name#602,cc_class#603,cc_employees#604,cc_sq_ft#605,cc_hours#606,cc_manager#607,cc_mkt_id#608,cc_mkt_class#609,cc_mkt_desc#610,cc_market_manager#611,cc_division#612,cc_division_name#613,cc_company#614,cc_company_name#615,cc_street_number#616,cc_street_name#617,cc_street_type#618,cc_suite_number#619,... 7 more fields] parquet
                        +- SubqueryAlias `v1_lead`
                           +- SubqueryAlias `v1`
                              +- Project [i_category#927, i_brand#923, cc_name#1089, d_year#1014, d_moy#1016, sum_sales#820, avg_monthly_sales#627, rn#724]
                                 +- Project [i_category#927, i_brand#923, cc_name#1089, d_year#1014, d_moy#1016, sum_sales#820, _w0#821, rn#724, avg_monthly_sales#627, avg_monthly_sales#627, rn#724]
                                    +- Window [avg(_w0#821) windowspecdefinition(i_category#927, i_brand#923, cc_name#1089, d_year#1014, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#627], [i_category#927, i_brand#923, cc_name#1089, d_year#1014]
                                       +- Window [rank(d_year#1014, d_moy#1016) windowspecdefinition(i_category#927, i_brand#923, cc_name#1089, d_year#1014 ASC NULLS FIRST, d_moy#1016 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#724], [i_category#927, i_brand#923, cc_name#1089], [d_year#1014 ASC NULLS FIRST, d_moy#1016 ASC NULLS FIRST]
                                          +- Aggregate [i_category#927, i_brand#923, cc_name#1089, d_year#1014, d_moy#1016], [i_category#927, i_brand#923, cc_name#1089, d_year#1014, d_moy#1016, sum(cs_sales_price#52) AS sum_sales#820, sum(cs_sales_price#52) AS _w0#821]
                                             +- Filter (((cs_item_sk#46 = i_item_sk#915) && (cs_sold_date_sk#31 = d_date_sk#1008)) && ((cc_call_center_sk#1083 = cs_call_center_sk#42) && (((d_year#1014 = 1999) || ((d_year#1014 = (1999 - 1)) && (d_moy#1016 = 12))) || ((d_year#1014 = (1999 + 1)) && (d_moy#1016 = 1)))))
                                                +- Join Inner
                                                   :- Join Inner
                                                   :  :- Join Inner
                                                   :  :  :- SubqueryAlias `tpcds`.`item`
                                                   :  :  :  +- Relation[i_item_sk#915,i_item_id#916,i_rec_start_date#917,i_rec_end_date#918,i_item_desc#919,i_current_price#920,i_wholesale_cost#921,i_brand_id#922,i_brand#923,i_class_id#924,i_class#925,i_category_id#926,i_category#927,i_manufact_id#928,i_manufact#929,i_size#930,i_formulation#931,i_color#932,i_units#933,i_container#934,i_manager_id#935,i_product_name#936] parquet
                                                   :  :  +- SubqueryAlias `tpcds`.`catalog_sales`
                                                   :  :     +- Relation[cs_sold_date_sk#31,cs_sold_time_sk#32,cs_ship_date_sk#33,cs_bill_customer_sk#34,cs_bill_cdemo_sk#35,cs_bill_hdemo_sk#36,cs_bill_addr_sk#37,cs_ship_customer_sk#38,cs_ship_cdemo_sk#39,cs_ship_hdemo_sk#40,cs_ship_addr_sk#41,cs_call_center_sk#42,cs_catalog_page_sk#43,cs_ship_mode_sk#44,cs_warehouse_sk#45,cs_item_sk#46,cs_promo_sk#47,cs_order_number#48,cs_quantity#49,cs_wholesale_cost#50,cs_list_price#51,cs_sales_price#52,cs_ext_discount_amt#53,cs_ext_sales_price#54,... 10 more fields] parquet
                                                   :  +- SubqueryAlias `tpcds`.`date_dim`
                                                   :     +- Relation[d_date_sk#1008,d_date_id#1009,d_date#1010,d_month_seq#1011,d_week_seq#1012,d_quarter_seq#1013,d_year#1014,d_dow#1015,d_moy#1016,d_dom#1017,d_qoy#1018,d_fy_year#1019,d_fy_quarter_seq#1020,d_fy_week_seq#1021,d_day_name#1022,d_quarter_name#1023,d_holiday#1024,d_weekend#1025,d_following_holiday#1026,d_first_dom#1027,d_last_dom#1028,d_same_day_ly#1029,d_same_day_lq#1030,d_current_day#1031,... 4 more fields] parquet
                                                   +- SubqueryAlias `tpcds`.`call_center`
                                                      +- Relation[cc_call_center_sk#1083,cc_call_center_id#1084,cc_rec_start_date#1085,cc_rec_end_date#1086,cc_closed_date_sk#1087,cc_open_date_sk#1088,cc_name#1089,cc_class#1090,cc_employees#1091,cc_sq_ft#1092,cc_hours#1093,cc_manager#1094,cc_mkt_id#1095,cc_mkt_class#1096,cc_mkt_desc#1097,cc_market_manager#1098,cc_division#1099,cc_division_name#1100,cc_company#1101,cc_company_name#1102,cc_street_number#1103,cc_street_name#1104,cc_street_type#1105,cc_suite_number#1106,... 7 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, cc_name#99 ASC NULLS FIRST], true
      +- Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, avg_monthly_sales#1, sum_sales#0, sum_sales#333 AS psum#3, sum_sales#820 AS nsum#4]
         +- Join Inner, ((((i_category#21 = i_category#927) && (i_brand#17 = i_brand#923)) && (cc_name#99 = cc_name#1089)) && (rn#2 = (rn#724 - 1)))
            :- Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum_sales#0, avg_monthly_sales#1, rn#2, sum_sales#333]
            :  +- Join Inner, ((((i_category#21 = i_category#440) && (i_brand#17 = i_brand#436)) && (cc_name#99 = cc_name#602)) && (rn#2 = (rn#237 + 1)))
            :     :- Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum_sales#0, avg_monthly_sales#1, rn#2]
            :     :  +- Filter (((isnotnull(avg_monthly_sales#1) && (avg_monthly_sales#1 > 0.0)) && (CASE WHEN (avg_monthly_sales#1 > 0.0) THEN (abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) ELSE null END > 0.1)) && isnotnull(rn#2))
            :     :     +- Window [avg(_w0#135) windowspecdefinition(i_category#21, i_brand#17, cc_name#99, d_year#71, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#21, i_brand#17, cc_name#99, d_year#71]
            :     :        +- Filter (isnotnull(d_year#71) && (d_year#71 = 1999))
            :     :           +- Window [rank(d_year#71, d_moy#73) windowspecdefinition(i_category#21, i_brand#17, cc_name#99, d_year#71 ASC NULLS FIRST, d_moy#73 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#21, i_brand#17, cc_name#99], [d_year#71 ASC NULLS FIRST, d_moy#73 ASC NULLS FIRST]
            :     :              +- Aggregate [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73], [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum(cs_sales_price#52) AS sum_sales#0, sum(cs_sales_price#52) AS _w0#135]
            :     :                 +- Project [i_brand#17, i_category#21, cs_sales_price#52, d_year#71, d_moy#73, cc_name#99]
            :     :                    +- Join Inner, (cc_call_center_sk#93 = cs_call_center_sk#42)
            :     :                       :- Project [i_brand#17, i_category#21, cs_call_center_sk#42, cs_sales_price#52, d_year#71, d_moy#73]
            :     :                       :  +- Join Inner, (cs_sold_date_sk#31 = d_date_sk#65)
            :     :                       :     :- Project [i_brand#17, i_category#21, cs_sold_date_sk#31, cs_call_center_sk#42, cs_sales_price#52]
            :     :                       :     :  +- Join Inner, (cs_item_sk#46 = i_item_sk#9)
            :     :                       :     :     :- Project [i_item_sk#9, i_brand#17, i_category#21]
            :     :                       :     :     :  +- Filter ((isnotnull(i_item_sk#9) && isnotnull(i_brand#17)) && isnotnull(i_category#21))
            :     :                       :     :     :     +- Relation[i_item_sk#9,i_item_id#10,i_rec_start_date#11,i_rec_end_date#12,i_item_desc#13,i_current_price#14,i_wholesale_cost#15,i_brand_id#16,i_brand#17,i_class_id#18,i_class#19,i_category_id#20,i_category#21,i_manufact_id#22,i_manufact#23,i_size#24,i_formulation#25,i_color#26,i_units#27,i_container#28,i_manager_id#29,i_product_name#30] parquet
            :     :                       :     :     +- Project [cs_sold_date_sk#31, cs_call_center_sk#42, cs_item_sk#46, cs_sales_price#52]
            :     :                       :     :        +- Filter ((isnotnull(cs_item_sk#46) && isnotnull(cs_sold_date_sk#31)) && isnotnull(cs_call_center_sk#42))
            :     :                       :     :           +- Relation[cs_sold_date_sk#31,cs_sold_time_sk#32,cs_ship_date_sk#33,cs_bill_customer_sk#34,cs_bill_cdemo_sk#35,cs_bill_hdemo_sk#36,cs_bill_addr_sk#37,cs_ship_customer_sk#38,cs_ship_cdemo_sk#39,cs_ship_hdemo_sk#40,cs_ship_addr_sk#41,cs_call_center_sk#42,cs_catalog_page_sk#43,cs_ship_mode_sk#44,cs_warehouse_sk#45,cs_item_sk#46,cs_promo_sk#47,cs_order_number#48,cs_quantity#49,cs_wholesale_cost#50,cs_list_price#51,cs_sales_price#52,cs_ext_discount_amt#53,cs_ext_sales_price#54,... 10 more fields] parquet
            :     :                       :     +- Project [d_date_sk#65, d_year#71, d_moy#73]
            :     :                       :        +- Filter ((((d_year#71 = 1999) || ((d_year#71 = 1998) && (d_moy#73 = 12))) || ((d_year#71 = 2000) && (d_moy#73 = 1))) && isnotnull(d_date_sk#65))
            :     :                       :           +- Relation[d_date_sk#65,d_date_id#66,d_date#67,d_month_seq#68,d_week_seq#69,d_quarter_seq#70,d_year#71,d_dow#72,d_moy#73,d_dom#74,d_qoy#75,d_fy_year#76,d_fy_quarter_seq#77,d_fy_week_seq#78,d_day_name#79,d_quarter_name#80,d_holiday#81,d_weekend#82,d_following_holiday#83,d_first_dom#84,d_last_dom#85,d_same_day_ly#86,d_same_day_lq#87,d_current_day#88,... 4 more fields] parquet
            :     :                       +- Project [cc_call_center_sk#93, cc_name#99]
            :     :                          +- Filter (isnotnull(cc_call_center_sk#93) && isnotnull(cc_name#99))
            :     :                             +- Relation[cc_call_center_sk#93,cc_call_center_id#94,cc_rec_start_date#95,cc_rec_end_date#96,cc_closed_date_sk#97,cc_open_date_sk#98,cc_name#99,cc_class#100,cc_employees#101,cc_sq_ft#102,cc_hours#103,cc_manager#104,cc_mkt_id#105,cc_mkt_class#106,cc_mkt_desc#107,cc_market_manager#108,cc_division#109,cc_division_name#110,cc_company#111,cc_company_name#112,cc_street_number#113,cc_street_name#114,cc_street_type#115,cc_suite_number#116,... 7 more fields] parquet
            :     +- Project [i_category#440, i_brand#436, cc_name#602, sum_sales#333, rn#237]
            :        +- Filter isnotnull(rn#237)
            :           +- Window [rank(d_year#527, d_moy#529) windowspecdefinition(i_category#440, i_brand#436, cc_name#602, d_year#527 ASC NULLS FIRST, d_moy#529 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#237], [i_category#440, i_brand#436, cc_name#602], [d_year#527 ASC NULLS FIRST, d_moy#529 ASC NULLS FIRST]
            :              +- Aggregate [i_category#440, i_brand#436, cc_name#602, d_year#527, d_moy#529], [i_category#440, i_brand#436, cc_name#602, d_year#527, d_moy#529, sum(cs_sales_price#52) AS sum_sales#333]
            :                 +- Project [i_brand#436, i_category#440, cs_sales_price#52, d_year#527, d_moy#529, cc_name#602]
            :                    +- Join Inner, (cc_call_center_sk#596 = cs_call_center_sk#42)
            :                       :- Project [i_brand#436, i_category#440, cs_call_center_sk#42, cs_sales_price#52, d_year#527, d_moy#529]
            :                       :  +- Join Inner, (cs_sold_date_sk#31 = d_date_sk#521)
            :                       :     :- Project [i_brand#436, i_category#440, cs_sold_date_sk#31, cs_call_center_sk#42, cs_sales_price#52]
            :                       :     :  +- Join Inner, (cs_item_sk#46 = i_item_sk#428)
            :                       :     :     :- Project [i_item_sk#428, i_brand#436, i_category#440]
            :                       :     :     :  +- Filter ((isnotnull(i_item_sk#428) && isnotnull(i_brand#436)) && isnotnull(i_category#440))
            :                       :     :     :     +- Relation[i_item_sk#428,i_item_id#429,i_rec_start_date#430,i_rec_end_date#431,i_item_desc#432,i_current_price#433,i_wholesale_cost#434,i_brand_id#435,i_brand#436,i_class_id#437,i_class#438,i_category_id#439,i_category#440,i_manufact_id#441,i_manufact#442,i_size#443,i_formulation#444,i_color#445,i_units#446,i_container#447,i_manager_id#448,i_product_name#449] parquet
            :                       :     :     +- Project [cs_sold_date_sk#31, cs_call_center_sk#42, cs_item_sk#46, cs_sales_price#52]
            :                       :     :        +- Filter ((isnotnull(cs_item_sk#46) && isnotnull(cs_sold_date_sk#31)) && isnotnull(cs_call_center_sk#42))
            :                       :     :           +- Relation[cs_sold_date_sk#31,cs_sold_time_sk#32,cs_ship_date_sk#33,cs_bill_customer_sk#34,cs_bill_cdemo_sk#35,cs_bill_hdemo_sk#36,cs_bill_addr_sk#37,cs_ship_customer_sk#38,cs_ship_cdemo_sk#39,cs_ship_hdemo_sk#40,cs_ship_addr_sk#41,cs_call_center_sk#42,cs_catalog_page_sk#43,cs_ship_mode_sk#44,cs_warehouse_sk#45,cs_item_sk#46,cs_promo_sk#47,cs_order_number#48,cs_quantity#49,cs_wholesale_cost#50,cs_list_price#51,cs_sales_price#52,cs_ext_discount_amt#53,cs_ext_sales_price#54,... 10 more fields] parquet
            :                       :     +- Project [d_date_sk#521, d_year#527, d_moy#529]
            :                       :        +- Filter ((((d_year#527 = 1999) || ((d_year#527 = 1998) && (d_moy#529 = 12))) || ((d_year#527 = 2000) && (d_moy#529 = 1))) && isnotnull(d_date_sk#521))
            :                       :           +- Relation[d_date_sk#521,d_date_id#522,d_date#523,d_month_seq#524,d_week_seq#525,d_quarter_seq#526,d_year#527,d_dow#528,d_moy#529,d_dom#530,d_qoy#531,d_fy_year#532,d_fy_quarter_seq#533,d_fy_week_seq#534,d_day_name#535,d_quarter_name#536,d_holiday#537,d_weekend#538,d_following_holiday#539,d_first_dom#540,d_last_dom#541,d_same_day_ly#542,d_same_day_lq#543,d_current_day#544,... 4 more fields] parquet
            :                       +- Project [cc_call_center_sk#596, cc_name#602]
            :                          +- Filter (isnotnull(cc_call_center_sk#596) && isnotnull(cc_name#602))
            :                             +- Relation[cc_call_center_sk#596,cc_call_center_id#597,cc_rec_start_date#598,cc_rec_end_date#599,cc_closed_date_sk#600,cc_open_date_sk#601,cc_name#602,cc_class#603,cc_employees#604,cc_sq_ft#605,cc_hours#606,cc_manager#607,cc_mkt_id#608,cc_mkt_class#609,cc_mkt_desc#610,cc_market_manager#611,cc_division#612,cc_division_name#613,cc_company#614,cc_company_name#615,cc_street_number#616,cc_street_name#617,cc_street_type#618,cc_suite_number#619,... 7 more fields] parquet
            +- Project [i_category#927, i_brand#923, cc_name#1089, sum_sales#820, rn#724]
               +- Filter isnotnull(rn#724)
                  +- Window [rank(d_year#1014, d_moy#1016) windowspecdefinition(i_category#927, i_brand#923, cc_name#1089, d_year#1014 ASC NULLS FIRST, d_moy#1016 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#724], [i_category#927, i_brand#923, cc_name#1089], [d_year#1014 ASC NULLS FIRST, d_moy#1016 ASC NULLS FIRST]
                     +- Aggregate [i_category#927, i_brand#923, cc_name#1089, d_year#1014, d_moy#1016], [i_category#927, i_brand#923, cc_name#1089, d_year#1014, d_moy#1016, sum(cs_sales_price#52) AS sum_sales#820]
                        +- Project [i_brand#923, i_category#927, cs_sales_price#52, d_year#1014, d_moy#1016, cc_name#1089]
                           +- Join Inner, (cc_call_center_sk#1083 = cs_call_center_sk#42)
                              :- Project [i_brand#923, i_category#927, cs_call_center_sk#42, cs_sales_price#52, d_year#1014, d_moy#1016]
                              :  +- Join Inner, (cs_sold_date_sk#31 = d_date_sk#1008)
                              :     :- Project [i_brand#923, i_category#927, cs_sold_date_sk#31, cs_call_center_sk#42, cs_sales_price#52]
                              :     :  +- Join Inner, (cs_item_sk#46 = i_item_sk#915)
                              :     :     :- Project [i_item_sk#915, i_brand#923, i_category#927]
                              :     :     :  +- Filter ((isnotnull(i_item_sk#915) && isnotnull(i_category#927)) && isnotnull(i_brand#923))
                              :     :     :     +- Relation[i_item_sk#915,i_item_id#916,i_rec_start_date#917,i_rec_end_date#918,i_item_desc#919,i_current_price#920,i_wholesale_cost#921,i_brand_id#922,i_brand#923,i_class_id#924,i_class#925,i_category_id#926,i_category#927,i_manufact_id#928,i_manufact#929,i_size#930,i_formulation#931,i_color#932,i_units#933,i_container#934,i_manager_id#935,i_product_name#936] parquet
                              :     :     +- Project [cs_sold_date_sk#31, cs_call_center_sk#42, cs_item_sk#46, cs_sales_price#52]
                              :     :        +- Filter ((isnotnull(cs_item_sk#46) && isnotnull(cs_sold_date_sk#31)) && isnotnull(cs_call_center_sk#42))
                              :     :           +- Relation[cs_sold_date_sk#31,cs_sold_time_sk#32,cs_ship_date_sk#33,cs_bill_customer_sk#34,cs_bill_cdemo_sk#35,cs_bill_hdemo_sk#36,cs_bill_addr_sk#37,cs_ship_customer_sk#38,cs_ship_cdemo_sk#39,cs_ship_hdemo_sk#40,cs_ship_addr_sk#41,cs_call_center_sk#42,cs_catalog_page_sk#43,cs_ship_mode_sk#44,cs_warehouse_sk#45,cs_item_sk#46,cs_promo_sk#47,cs_order_number#48,cs_quantity#49,cs_wholesale_cost#50,cs_list_price#51,cs_sales_price#52,cs_ext_discount_amt#53,cs_ext_sales_price#54,... 10 more fields] parquet
                              :     +- Project [d_date_sk#1008, d_year#1014, d_moy#1016]
                              :        +- Filter ((((d_year#1014 = 1999) || ((d_year#1014 = 1998) && (d_moy#1016 = 12))) || ((d_year#1014 = 2000) && (d_moy#1016 = 1))) && isnotnull(d_date_sk#1008))
                              :           +- Relation[d_date_sk#1008,d_date_id#1009,d_date#1010,d_month_seq#1011,d_week_seq#1012,d_quarter_seq#1013,d_year#1014,d_dow#1015,d_moy#1016,d_dom#1017,d_qoy#1018,d_fy_year#1019,d_fy_quarter_seq#1020,d_fy_week_seq#1021,d_day_name#1022,d_quarter_name#1023,d_holiday#1024,d_weekend#1025,d_following_holiday#1026,d_first_dom#1027,d_last_dom#1028,d_same_day_ly#1029,d_same_day_lq#1030,d_current_day#1031,... 4 more fields] parquet
                              +- Project [cc_call_center_sk#1083, cc_name#1089]
                                 +- Filter (isnotnull(cc_call_center_sk#1083) && isnotnull(cc_name#1089))
                                    +- Relation[cc_call_center_sk#1083,cc_call_center_id#1084,cc_rec_start_date#1085,cc_rec_end_date#1086,cc_closed_date_sk#1087,cc_open_date_sk#1088,cc_name#1089,cc_class#1090,cc_employees#1091,cc_sq_ft#1092,cc_hours#1093,cc_manager#1094,cc_mkt_id#1095,cc_mkt_class#1096,cc_mkt_desc#1097,cc_market_manager#1098,cc_division#1099,cc_division_name#1100,cc_company#1101,cc_company_name#1102,cc_street_number#1103,cc_street_name#1104,cc_street_type#1105,cc_suite_number#1106,... 7 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST,cc_name#99 ASC NULLS FIRST], output=[i_category#21,i_brand#17,cc_name#99,d_year#71,d_moy#73,avg_monthly_sales#1,sum_sales#0,psum#3,nsum#4])
+- *(27) Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, avg_monthly_sales#1, sum_sales#0, sum_sales#333 AS psum#3, sum_sales#820 AS nsum#4]
   +- *(27) SortMergeJoin [i_category#21, i_brand#17, cc_name#99, rn#2], [i_category#927, i_brand#923, cc_name#1089, (rn#724 - 1)], Inner
      :- *(18) Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum_sales#0, avg_monthly_sales#1, rn#2, sum_sales#333]
      :  +- *(18) SortMergeJoin [i_category#21, i_brand#17, cc_name#99, rn#2], [i_category#440, i_brand#436, cc_name#602, (rn#237 + 1)], Inner
      :     :- *(9) Sort [i_category#21 ASC NULLS FIRST, i_brand#17 ASC NULLS FIRST, cc_name#99 ASC NULLS FIRST, rn#2 ASC NULLS FIRST], false, 0
      :     :  +- Exchange hashpartitioning(i_category#21, i_brand#17, cc_name#99, rn#2, 200)
      :     :     +- *(8) Project [i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum_sales#0, avg_monthly_sales#1, rn#2]
      :     :        +- *(8) Filter (((isnotnull(avg_monthly_sales#1) && (avg_monthly_sales#1 > 0.0)) && (CASE WHEN (avg_monthly_sales#1 > 0.0) THEN (abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) ELSE null END > 0.1)) && isnotnull(rn#2))
      :     :           +- Window [avg(_w0#135) windowspecdefinition(i_category#21, i_brand#17, cc_name#99, d_year#71, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#21, i_brand#17, cc_name#99, d_year#71]
      :     :              +- *(7) Filter (isnotnull(d_year#71) && (d_year#71 = 1999))
      :     :                 +- Window [rank(d_year#71, d_moy#73) windowspecdefinition(i_category#21, i_brand#17, cc_name#99, d_year#71 ASC NULLS FIRST, d_moy#73 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#21, i_brand#17, cc_name#99], [d_year#71 ASC NULLS FIRST, d_moy#73 ASC NULLS FIRST]
      :     :                    +- *(6) Sort [i_category#21 ASC NULLS FIRST, i_brand#17 ASC NULLS FIRST, cc_name#99 ASC NULLS FIRST, d_year#71 ASC NULLS FIRST, d_moy#73 ASC NULLS FIRST], false, 0
      :     :                       +- Exchange hashpartitioning(i_category#21, i_brand#17, cc_name#99, 200)
      :     :                          +- *(5) HashAggregate(keys=[i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73], functions=[sum(cs_sales_price#52)], output=[i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum_sales#0, _w0#135])
      :     :                             +- Exchange hashpartitioning(i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, 200)
      :     :                                +- *(4) HashAggregate(keys=[i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73], functions=[partial_sum(cs_sales_price#52)], output=[i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, sum#1199])
      :     :                                   +- *(4) Project [i_brand#17, i_category#21, cs_sales_price#52, d_year#71, d_moy#73, cc_name#99]
      :     :                                      +- *(4) BroadcastHashJoin [cs_call_center_sk#42], [cc_call_center_sk#93], Inner, BuildRight
      :     :                                         :- *(4) Project [i_brand#17, i_category#21, cs_call_center_sk#42, cs_sales_price#52, d_year#71, d_moy#73]
      :     :                                         :  +- *(4) BroadcastHashJoin [cs_sold_date_sk#31], [d_date_sk#65], Inner, BuildRight
      :     :                                         :     :- *(4) Project [i_brand#17, i_category#21, cs_sold_date_sk#31, cs_call_center_sk#42, cs_sales_price#52]
      :     :                                         :     :  +- *(4) BroadcastHashJoin [i_item_sk#9], [cs_item_sk#46], Inner, BuildLeft
      :     :                                         :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                                         :     :     :  +- *(1) Project [i_item_sk#9, i_brand#17, i_category#21]
      :     :                                         :     :     :     +- *(1) Filter ((isnotnull(i_item_sk#9) && isnotnull(i_brand#17)) && isnotnull(i_category#21))
      :     :                                         :     :     :        +- *(1) FileScan parquet tpcds.item[i_item_sk#9,i_brand#17,i_category#21] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_brand), IsNotNull(i_category)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_category:string>
      :     :                                         :     :     +- *(4) Project [cs_sold_date_sk#31, cs_call_center_sk#42, cs_item_sk#46, cs_sales_price#52]
      :     :                                         :     :        +- *(4) Filter ((isnotnull(cs_item_sk#46) && isnotnull(cs_sold_date_sk#31)) && isnotnull(cs_call_center_sk#42))
      :     :                                         :     :           +- *(4) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#31,cs_call_center_sk#42,cs_item_sk#46,cs_sales_price#52] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk), IsNotNull(cs_call_center_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_call_center_sk:int,cs_item_sk:int,cs_sales_price:double>
      :     :                                         :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                                         :        +- *(2) Project [d_date_sk#65, d_year#71, d_moy#73]
      :     :                                         :           +- *(2) Filter ((((d_year#71 = 1999) || ((d_year#71 = 1998) && (d_moy#73 = 12))) || ((d_year#71 = 2000) && (d_moy#73 = 1))) && isnotnull(d_date_sk#65))
      :     :                                         :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#65,d_year#71,d_moy#73] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(d_year,1999),And(EqualTo(d_year,1998),EqualTo(d_moy,12))),And(EqualTo(d_year,2000)..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
      :     :                                         +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                                            +- *(3) Project [cc_call_center_sk#93, cc_name#99]
      :     :                                               +- *(3) Filter (isnotnull(cc_call_center_sk#93) && isnotnull(cc_name#99))
      :     :                                                  +- *(3) FileScan parquet tpcds.call_center[cc_call_center_sk#93,cc_name#99] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk), IsNotNull(cc_name)], ReadSchema: struct<cc_call_center_sk:int,cc_name:string>
      :     +- *(17) Sort [i_category#440 ASC NULLS FIRST, i_brand#436 ASC NULLS FIRST, cc_name#602 ASC NULLS FIRST, (rn#237 + 1) ASC NULLS FIRST], false, 0
      :        +- Exchange hashpartitioning(i_category#440, i_brand#436, cc_name#602, (rn#237 + 1), 200)
      :           +- *(16) Project [i_category#440, i_brand#436, cc_name#602, sum_sales#333, rn#237]
      :              +- *(16) Filter isnotnull(rn#237)
      :                 +- Window [rank(d_year#527, d_moy#529) windowspecdefinition(i_category#440, i_brand#436, cc_name#602, d_year#527 ASC NULLS FIRST, d_moy#529 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#237], [i_category#440, i_brand#436, cc_name#602], [d_year#527 ASC NULLS FIRST, d_moy#529 ASC NULLS FIRST]
      :                    +- *(15) Sort [i_category#440 ASC NULLS FIRST, i_brand#436 ASC NULLS FIRST, cc_name#602 ASC NULLS FIRST, d_year#527 ASC NULLS FIRST, d_moy#529 ASC NULLS FIRST], false, 0
      :                       +- Exchange hashpartitioning(i_category#440, i_brand#436, cc_name#602, 200)
      :                          +- *(14) HashAggregate(keys=[i_category#440, i_brand#436, cc_name#602, d_year#527, d_moy#529], functions=[sum(cs_sales_price#52)], output=[i_category#440, i_brand#436, cc_name#602, d_year#527, d_moy#529, sum_sales#333])
      :                             +- ReusedExchange [i_category#440, i_brand#436, cc_name#602, d_year#527, d_moy#529, sum#1199], Exchange hashpartitioning(i_category#21, i_brand#17, cc_name#99, d_year#71, d_moy#73, 200)
      +- *(26) Sort [i_category#927 ASC NULLS FIRST, i_brand#923 ASC NULLS FIRST, cc_name#1089 ASC NULLS FIRST, (rn#724 - 1) ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(i_category#927, i_brand#923, cc_name#1089, (rn#724 - 1), 200)
            +- *(25) Project [i_category#927, i_brand#923, cc_name#1089, sum_sales#820, rn#724]
               +- *(25) Filter isnotnull(rn#724)
                  +- Window [rank(d_year#1014, d_moy#1016) windowspecdefinition(i_category#927, i_brand#923, cc_name#1089, d_year#1014 ASC NULLS FIRST, d_moy#1016 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#724], [i_category#927, i_brand#923, cc_name#1089], [d_year#1014 ASC NULLS FIRST, d_moy#1016 ASC NULLS FIRST]
                     +- *(24) Sort [i_category#927 ASC NULLS FIRST, i_brand#923 ASC NULLS FIRST, cc_name#1089 ASC NULLS FIRST, d_year#1014 ASC NULLS FIRST, d_moy#1016 ASC NULLS FIRST], false, 0
                        +- ReusedExchange [i_category#927, i_brand#923, cc_name#1089, d_year#1014, d_moy#1016, sum_sales#820], Exchange hashpartitioning(i_category#440, i_brand#436, cc_name#602, 200)
Time taken: 5.967 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 57 in stream 0 using template query57.tpl
------------------------------------------------------^^^

