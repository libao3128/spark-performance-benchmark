== Parsed Logical Plan ==
CTE [v1, v2]
:  :- 'SubqueryAlias `v1`
:  :  +- 'Aggregate ['i_category, 'i_brand, 's_store_name, 's_company_name, 'd_year, 'd_moy], ['i_category, 'i_brand, 's_store_name, 's_company_name, 'd_year, 'd_moy, 'sum('ss_sales_price) AS sum_sales#0, 'avg('sum('ss_sales_price)) windowspecdefinition('i_category, 'i_brand, 's_store_name, 's_company_name, 'd_year, unspecifiedframe$()) AS avg_monthly_sales#1, 'rank() windowspecdefinition('i_category, 'i_brand, 's_store_name, 's_company_name, 'd_year ASC NULLS FIRST, 'd_moy ASC NULLS FIRST, unspecifiedframe$()) AS rn#2]
:  :     +- 'Filter ((('ss_item_sk = 'i_item_sk) && ('ss_sold_date_sk = 'd_date_sk)) && (('ss_store_sk = 's_store_sk) && ((('d_year = 1999) || (('d_year = (1999 - 1)) && ('d_moy = 12))) || (('d_year = (1999 + 1)) && ('d_moy = 1)))))
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'UnresolvedRelation `item`
:  :           :  :  +- 'UnresolvedRelation `store_sales`
:  :           :  +- 'UnresolvedRelation `date_dim`
:  :           +- 'UnresolvedRelation `store`
:  +- 'SubqueryAlias `v2`
:     +- 'Project ['v1.i_category, 'v1.i_brand, 'v1.s_store_name, 'v1.s_company_name, 'v1.d_year, 'v1.d_moy, 'v1.avg_monthly_sales, 'v1.sum_sales, 'v1_lag.sum_sales AS psum#3, 'v1_lead.sum_sales AS nsum#4]
:        +- 'Filter ((((('v1.i_category = 'v1_lag.i_category) && ('v1.i_category = 'v1_lead.i_category)) && ('v1.i_brand = 'v1_lag.i_brand)) && (('v1.i_brand = 'v1_lead.i_brand) && ('v1.s_store_name = 'v1_lag.s_store_name))) && (((('v1.s_store_name = 'v1_lead.s_store_name) && ('v1.s_company_name = 'v1_lag.s_company_name)) && ('v1.s_company_name = 'v1_lead.s_company_name)) && (('v1.rn = ('v1_lag.rn + 1)) && ('v1.rn = ('v1_lead.rn - 1)))))
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
i_category: string, i_brand: string, s_store_name: string, s_company_name: string, d_year: int, d_moy: int, avg_monthly_sales: double, sum_sales: double, psum: double, nsum: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, s_store_name#87 ASC NULLS FIRST], true
      +- Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, avg_monthly_sales#1, sum_sales#0, psum#3, nsum#4]
         +- Filter (((d_year#60 = 1999) && (avg_monthly_sales#1 > cast(0 as double))) && (CASE WHEN (avg_monthly_sales#1 > cast(0 as double)) THEN (abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) ELSE cast(null as double) END > cast(0.1 as double)))
            +- SubqueryAlias `v2`
               +- Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, avg_monthly_sales#1, sum_sales#0, sum_sales#316 AS psum#3, sum_sales#791 AS nsum#4]
                  +- Filter (((((i_category#21 = i_category#421) && (i_category#21 = i_category#896)) && (i_brand#17 = i_brand#417)) && ((i_brand#17 = i_brand#892) && (s_store_name#87 = s_store_name#578))) && ((((s_store_name#87 = s_store_name#1053) && (s_company_name#99 = s_company_name#590)) && (s_company_name#99 = s_company_name#1065)) && ((rn#2 = (rn#127 + 1)) && (rn#2 = (rn#602 - 1)))))
                     +- Join Inner
                        :- Join Inner
                        :  :- SubqueryAlias `v1`
                        :  :  +- Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum_sales#0, avg_monthly_sales#1, rn#2]
                        :  :     +- Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum_sales#0, _w0#122, avg_monthly_sales#1, rn#2, avg_monthly_sales#1, rn#2]
                        :  :        +- Window [rank(d_year#60, d_moy#62) windowspecdefinition(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60 ASC NULLS FIRST, d_moy#62 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#21, i_brand#17, s_store_name#87, s_company_name#99], [d_year#60 ASC NULLS FIRST, d_moy#62 ASC NULLS FIRST]
                        :  :           +- Window [avg(_w0#122) windowspecdefinition(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60]
                        :  :              +- Aggregate [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62], [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum(ss_sales_price#44) AS sum_sales#0, sum(ss_sales_price#44) AS _w0#122]
                        :  :                 +- Filter (((ss_item_sk#33 = i_item_sk#9) && (ss_sold_date_sk#31 = d_date_sk#54)) && ((ss_store_sk#38 = s_store_sk#82) && (((d_year#60 = 1999) || ((d_year#60 = (1999 - 1)) && (d_moy#62 = 12))) || ((d_year#60 = (1999 + 1)) && (d_moy#62 = 1)))))
                        :  :                    +- Join Inner
                        :  :                       :- Join Inner
                        :  :                       :  :- Join Inner
                        :  :                       :  :  :- SubqueryAlias `tpcds`.`item`
                        :  :                       :  :  :  +- Relation[i_item_sk#9,i_item_id#10,i_rec_start_date#11,i_rec_end_date#12,i_item_desc#13,i_current_price#14,i_wholesale_cost#15,i_brand_id#16,i_brand#17,i_class_id#18,i_class#19,i_category_id#20,i_category#21,i_manufact_id#22,i_manufact#23,i_size#24,i_formulation#25,i_color#26,i_units#27,i_container#28,i_manager_id#29,i_product_name#30] parquet
                        :  :                       :  :  +- SubqueryAlias `tpcds`.`store_sales`
                        :  :                       :  :     +- Relation[ss_sold_date_sk#31,ss_sold_time_sk#32,ss_item_sk#33,ss_customer_sk#34,ss_cdemo_sk#35,ss_hdemo_sk#36,ss_addr_sk#37,ss_store_sk#38,ss_promo_sk#39,ss_ticket_number#40,ss_quantity#41,ss_wholesale_cost#42,ss_list_price#43,ss_sales_price#44,ss_ext_discount_amt#45,ss_ext_sales_price#46,ss_ext_wholesale_cost#47,ss_ext_list_price#48,ss_ext_tax#49,ss_coupon_amt#50,ss_net_paid#51,ss_net_paid_inc_tax#52,ss_net_profit#53] parquet
                        :  :                       :  +- SubqueryAlias `tpcds`.`date_dim`
                        :  :                       :     +- Relation[d_date_sk#54,d_date_id#55,d_date#56,d_month_seq#57,d_week_seq#58,d_quarter_seq#59,d_year#60,d_dow#61,d_moy#62,d_dom#63,d_qoy#64,d_fy_year#65,d_fy_quarter_seq#66,d_fy_week_seq#67,d_day_name#68,d_quarter_name#69,d_holiday#70,d_weekend#71,d_following_holiday#72,d_first_dom#73,d_last_dom#74,d_same_day_ly#75,d_same_day_lq#76,d_current_day#77,... 4 more fields] parquet
                        :  :                       +- SubqueryAlias `tpcds`.`store`
                        :  :                          +- Relation[s_store_sk#82,s_store_id#83,s_rec_start_date#84,s_rec_end_date#85,s_closed_date_sk#86,s_store_name#87,s_number_employees#88,s_floor_space#89,s_hours#90,s_manager#91,s_market_id#92,s_geography_class#93,s_market_desc#94,s_market_manager#95,s_division_id#96,s_division_name#97,s_company_id#98,s_company_name#99,s_street_number#100,s_street_name#101,s_street_type#102,s_suite_number#103,s_city#104,s_county#105,... 5 more fields] parquet
                        :  +- SubqueryAlias `v1_lag`
                        :     +- SubqueryAlias `v1`
                        :        +- Project [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, d_moy#508, sum_sales#316, avg_monthly_sales#222, rn#127]
                        :           +- Project [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, d_moy#508, sum_sales#316, _w0#317, avg_monthly_sales#222, rn#127, avg_monthly_sales#222, rn#127]
                        :              +- Window [rank(d_year#506, d_moy#508) windowspecdefinition(i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506 ASC NULLS FIRST, d_moy#508 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#127], [i_category#421, i_brand#417, s_store_name#578, s_company_name#590], [d_year#506 ASC NULLS FIRST, d_moy#508 ASC NULLS FIRST]
                        :                 +- Window [avg(_w0#317) windowspecdefinition(i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#222], [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506]
                        :                    +- Aggregate [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, d_moy#508], [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, d_moy#508, sum(ss_sales_price#44) AS sum_sales#316, sum(ss_sales_price#44) AS _w0#317]
                        :                       +- Filter (((ss_item_sk#33 = i_item_sk#409) && (ss_sold_date_sk#31 = d_date_sk#500)) && ((ss_store_sk#38 = s_store_sk#573) && (((d_year#506 = 1999) || ((d_year#506 = (1999 - 1)) && (d_moy#508 = 12))) || ((d_year#506 = (1999 + 1)) && (d_moy#508 = 1)))))
                        :                          +- Join Inner
                        :                             :- Join Inner
                        :                             :  :- Join Inner
                        :                             :  :  :- SubqueryAlias `tpcds`.`item`
                        :                             :  :  :  +- Relation[i_item_sk#409,i_item_id#410,i_rec_start_date#411,i_rec_end_date#412,i_item_desc#413,i_current_price#414,i_wholesale_cost#415,i_brand_id#416,i_brand#417,i_class_id#418,i_class#419,i_category_id#420,i_category#421,i_manufact_id#422,i_manufact#423,i_size#424,i_formulation#425,i_color#426,i_units#427,i_container#428,i_manager_id#429,i_product_name#430] parquet
                        :                             :  :  +- SubqueryAlias `tpcds`.`store_sales`
                        :                             :  :     +- Relation[ss_sold_date_sk#31,ss_sold_time_sk#32,ss_item_sk#33,ss_customer_sk#34,ss_cdemo_sk#35,ss_hdemo_sk#36,ss_addr_sk#37,ss_store_sk#38,ss_promo_sk#39,ss_ticket_number#40,ss_quantity#41,ss_wholesale_cost#42,ss_list_price#43,ss_sales_price#44,ss_ext_discount_amt#45,ss_ext_sales_price#46,ss_ext_wholesale_cost#47,ss_ext_list_price#48,ss_ext_tax#49,ss_coupon_amt#50,ss_net_paid#51,ss_net_paid_inc_tax#52,ss_net_profit#53] parquet
                        :                             :  +- SubqueryAlias `tpcds`.`date_dim`
                        :                             :     +- Relation[d_date_sk#500,d_date_id#501,d_date#502,d_month_seq#503,d_week_seq#504,d_quarter_seq#505,d_year#506,d_dow#507,d_moy#508,d_dom#509,d_qoy#510,d_fy_year#511,d_fy_quarter_seq#512,d_fy_week_seq#513,d_day_name#514,d_quarter_name#515,d_holiday#516,d_weekend#517,d_following_holiday#518,d_first_dom#519,d_last_dom#520,d_same_day_ly#521,d_same_day_lq#522,d_current_day#523,... 4 more fields] parquet
                        :                             +- SubqueryAlias `tpcds`.`store`
                        :                                +- Relation[s_store_sk#573,s_store_id#574,s_rec_start_date#575,s_rec_end_date#576,s_closed_date_sk#577,s_store_name#578,s_number_employees#579,s_floor_space#580,s_hours#581,s_manager#582,s_market_id#583,s_geography_class#584,s_market_desc#585,s_market_manager#586,s_division_id#587,s_division_name#588,s_company_id#589,s_company_name#590,s_street_number#591,s_street_name#592,s_street_type#593,s_suite_number#594,s_city#595,s_county#596,... 5 more fields] parquet
                        +- SubqueryAlias `v1_lead`
                           +- SubqueryAlias `v1`
                              +- Project [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981, d_moy#983, sum_sales#791, avg_monthly_sales#697, rn#602]
                                 +- Project [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981, d_moy#983, sum_sales#791, _w0#792, avg_monthly_sales#697, rn#602, avg_monthly_sales#697, rn#602]
                                    +- Window [rank(d_year#981, d_moy#983) windowspecdefinition(i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981 ASC NULLS FIRST, d_moy#983 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#602], [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065], [d_year#981 ASC NULLS FIRST, d_moy#983 ASC NULLS FIRST]
                                       +- Window [avg(_w0#792) windowspecdefinition(i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#697], [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981]
                                          +- Aggregate [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981, d_moy#983], [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981, d_moy#983, sum(ss_sales_price#44) AS sum_sales#791, sum(ss_sales_price#44) AS _w0#792]
                                             +- Filter (((ss_item_sk#33 = i_item_sk#884) && (ss_sold_date_sk#31 = d_date_sk#975)) && ((ss_store_sk#38 = s_store_sk#1048) && (((d_year#981 = 1999) || ((d_year#981 = (1999 - 1)) && (d_moy#983 = 12))) || ((d_year#981 = (1999 + 1)) && (d_moy#983 = 1)))))
                                                +- Join Inner
                                                   :- Join Inner
                                                   :  :- Join Inner
                                                   :  :  :- SubqueryAlias `tpcds`.`item`
                                                   :  :  :  +- Relation[i_item_sk#884,i_item_id#885,i_rec_start_date#886,i_rec_end_date#887,i_item_desc#888,i_current_price#889,i_wholesale_cost#890,i_brand_id#891,i_brand#892,i_class_id#893,i_class#894,i_category_id#895,i_category#896,i_manufact_id#897,i_manufact#898,i_size#899,i_formulation#900,i_color#901,i_units#902,i_container#903,i_manager_id#904,i_product_name#905] parquet
                                                   :  :  +- SubqueryAlias `tpcds`.`store_sales`
                                                   :  :     +- Relation[ss_sold_date_sk#31,ss_sold_time_sk#32,ss_item_sk#33,ss_customer_sk#34,ss_cdemo_sk#35,ss_hdemo_sk#36,ss_addr_sk#37,ss_store_sk#38,ss_promo_sk#39,ss_ticket_number#40,ss_quantity#41,ss_wholesale_cost#42,ss_list_price#43,ss_sales_price#44,ss_ext_discount_amt#45,ss_ext_sales_price#46,ss_ext_wholesale_cost#47,ss_ext_list_price#48,ss_ext_tax#49,ss_coupon_amt#50,ss_net_paid#51,ss_net_paid_inc_tax#52,ss_net_profit#53] parquet
                                                   :  +- SubqueryAlias `tpcds`.`date_dim`
                                                   :     +- Relation[d_date_sk#975,d_date_id#976,d_date#977,d_month_seq#978,d_week_seq#979,d_quarter_seq#980,d_year#981,d_dow#982,d_moy#983,d_dom#984,d_qoy#985,d_fy_year#986,d_fy_quarter_seq#987,d_fy_week_seq#988,d_day_name#989,d_quarter_name#990,d_holiday#991,d_weekend#992,d_following_holiday#993,d_first_dom#994,d_last_dom#995,d_same_day_ly#996,d_same_day_lq#997,d_current_day#998,... 4 more fields] parquet
                                                   +- SubqueryAlias `tpcds`.`store`
                                                      +- Relation[s_store_sk#1048,s_store_id#1049,s_rec_start_date#1050,s_rec_end_date#1051,s_closed_date_sk#1052,s_store_name#1053,s_number_employees#1054,s_floor_space#1055,s_hours#1056,s_manager#1057,s_market_id#1058,s_geography_class#1059,s_market_desc#1060,s_market_manager#1061,s_division_id#1062,s_division_name#1063,s_company_id#1064,s_company_name#1065,s_street_number#1066,s_street_name#1067,s_street_type#1068,s_suite_number#1069,s_city#1070,s_county#1071,... 5 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST, s_store_name#87 ASC NULLS FIRST], true
      +- Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, avg_monthly_sales#1, sum_sales#0, sum_sales#316 AS psum#3, sum_sales#791 AS nsum#4]
         +- Join Inner, (((((i_category#21 = i_category#896) && (i_brand#17 = i_brand#892)) && (s_store_name#87 = s_store_name#1053)) && (s_company_name#99 = s_company_name#1065)) && (rn#2 = (rn#602 - 1)))
            :- Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum_sales#0, avg_monthly_sales#1, rn#2, sum_sales#316]
            :  +- Join Inner, (((((i_category#21 = i_category#421) && (i_brand#17 = i_brand#417)) && (s_store_name#87 = s_store_name#578)) && (s_company_name#99 = s_company_name#590)) && (rn#2 = (rn#127 + 1)))
            :     :- Filter (((((isnotnull(d_year#60) && isnotnull(avg_monthly_sales#1)) && (d_year#60 = 1999)) && (avg_monthly_sales#1 > 0.0)) && (CASE WHEN (avg_monthly_sales#1 > 0.0) THEN (abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) ELSE null END > 0.1)) && isnotnull(rn#2))
            :     :  +- Window [rank(d_year#60, d_moy#62) windowspecdefinition(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60 ASC NULLS FIRST, d_moy#62 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#21, i_brand#17, s_store_name#87, s_company_name#99], [d_year#60 ASC NULLS FIRST, d_moy#62 ASC NULLS FIRST]
            :     :     +- Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum_sales#0, avg_monthly_sales#1]
            :     :        +- Window [avg(_w0#122) windowspecdefinition(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60]
            :     :           +- Aggregate [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62], [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum(ss_sales_price#44) AS sum_sales#0, sum(ss_sales_price#44) AS _w0#122]
            :     :              +- Project [i_brand#17, i_category#21, ss_sales_price#44, d_year#60, d_moy#62, s_store_name#87, s_company_name#99]
            :     :                 +- Join Inner, (ss_store_sk#38 = s_store_sk#82)
            :     :                    :- Project [i_brand#17, i_category#21, ss_store_sk#38, ss_sales_price#44, d_year#60, d_moy#62]
            :     :                    :  +- Join Inner, (ss_sold_date_sk#31 = d_date_sk#54)
            :     :                    :     :- Project [i_brand#17, i_category#21, ss_sold_date_sk#31, ss_store_sk#38, ss_sales_price#44]
            :     :                    :     :  +- Join Inner, (ss_item_sk#33 = i_item_sk#9)
            :     :                    :     :     :- Project [i_item_sk#9, i_brand#17, i_category#21]
            :     :                    :     :     :  +- Filter ((isnotnull(i_item_sk#9) && isnotnull(i_brand#17)) && isnotnull(i_category#21))
            :     :                    :     :     :     +- Relation[i_item_sk#9,i_item_id#10,i_rec_start_date#11,i_rec_end_date#12,i_item_desc#13,i_current_price#14,i_wholesale_cost#15,i_brand_id#16,i_brand#17,i_class_id#18,i_class#19,i_category_id#20,i_category#21,i_manufact_id#22,i_manufact#23,i_size#24,i_formulation#25,i_color#26,i_units#27,i_container#28,i_manager_id#29,i_product_name#30] parquet
            :     :                    :     :     +- Project [ss_sold_date_sk#31, ss_item_sk#33, ss_store_sk#38, ss_sales_price#44]
            :     :                    :     :        +- Filter ((isnotnull(ss_item_sk#33) && isnotnull(ss_sold_date_sk#31)) && isnotnull(ss_store_sk#38))
            :     :                    :     :           +- Relation[ss_sold_date_sk#31,ss_sold_time_sk#32,ss_item_sk#33,ss_customer_sk#34,ss_cdemo_sk#35,ss_hdemo_sk#36,ss_addr_sk#37,ss_store_sk#38,ss_promo_sk#39,ss_ticket_number#40,ss_quantity#41,ss_wholesale_cost#42,ss_list_price#43,ss_sales_price#44,ss_ext_discount_amt#45,ss_ext_sales_price#46,ss_ext_wholesale_cost#47,ss_ext_list_price#48,ss_ext_tax#49,ss_coupon_amt#50,ss_net_paid#51,ss_net_paid_inc_tax#52,ss_net_profit#53] parquet
            :     :                    :     +- Project [d_date_sk#54, d_year#60, d_moy#62]
            :     :                    :        +- Filter ((((d_year#60 = 1999) || ((d_year#60 = 1998) && (d_moy#62 = 12))) || ((d_year#60 = 2000) && (d_moy#62 = 1))) && isnotnull(d_date_sk#54))
            :     :                    :           +- Relation[d_date_sk#54,d_date_id#55,d_date#56,d_month_seq#57,d_week_seq#58,d_quarter_seq#59,d_year#60,d_dow#61,d_moy#62,d_dom#63,d_qoy#64,d_fy_year#65,d_fy_quarter_seq#66,d_fy_week_seq#67,d_day_name#68,d_quarter_name#69,d_holiday#70,d_weekend#71,d_following_holiday#72,d_first_dom#73,d_last_dom#74,d_same_day_ly#75,d_same_day_lq#76,d_current_day#77,... 4 more fields] parquet
            :     :                    +- Project [s_store_sk#82, s_store_name#87, s_company_name#99]
            :     :                       +- Filter ((isnotnull(s_store_sk#82) && isnotnull(s_company_name#99)) && isnotnull(s_store_name#87))
            :     :                          +- Relation[s_store_sk#82,s_store_id#83,s_rec_start_date#84,s_rec_end_date#85,s_closed_date_sk#86,s_store_name#87,s_number_employees#88,s_floor_space#89,s_hours#90,s_manager#91,s_market_id#92,s_geography_class#93,s_market_desc#94,s_market_manager#95,s_division_id#96,s_division_name#97,s_company_id#98,s_company_name#99,s_street_number#100,s_street_name#101,s_street_type#102,s_suite_number#103,s_city#104,s_county#105,... 5 more fields] parquet
            :     +- Project [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, sum_sales#316, rn#127]
            :        +- Filter isnotnull(rn#127)
            :           +- Window [rank(d_year#506, d_moy#508) windowspecdefinition(i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506 ASC NULLS FIRST, d_moy#508 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#127], [i_category#421, i_brand#417, s_store_name#578, s_company_name#590], [d_year#506 ASC NULLS FIRST, d_moy#508 ASC NULLS FIRST]
            :              +- Aggregate [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, d_moy#508], [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, d_moy#508, sum(ss_sales_price#44) AS sum_sales#316]
            :                 +- Project [i_brand#417, i_category#421, ss_sales_price#44, d_year#506, d_moy#508, s_store_name#578, s_company_name#590]
            :                    +- Join Inner, (ss_store_sk#38 = s_store_sk#573)
            :                       :- Project [i_brand#417, i_category#421, ss_store_sk#38, ss_sales_price#44, d_year#506, d_moy#508]
            :                       :  +- Join Inner, (ss_sold_date_sk#31 = d_date_sk#500)
            :                       :     :- Project [i_brand#417, i_category#421, ss_sold_date_sk#31, ss_store_sk#38, ss_sales_price#44]
            :                       :     :  +- Join Inner, (ss_item_sk#33 = i_item_sk#409)
            :                       :     :     :- Project [i_item_sk#409, i_brand#417, i_category#421]
            :                       :     :     :  +- Filter ((isnotnull(i_item_sk#409) && isnotnull(i_category#421)) && isnotnull(i_brand#417))
            :                       :     :     :     +- Relation[i_item_sk#409,i_item_id#410,i_rec_start_date#411,i_rec_end_date#412,i_item_desc#413,i_current_price#414,i_wholesale_cost#415,i_brand_id#416,i_brand#417,i_class_id#418,i_class#419,i_category_id#420,i_category#421,i_manufact_id#422,i_manufact#423,i_size#424,i_formulation#425,i_color#426,i_units#427,i_container#428,i_manager_id#429,i_product_name#430] parquet
            :                       :     :     +- Project [ss_sold_date_sk#31, ss_item_sk#33, ss_store_sk#38, ss_sales_price#44]
            :                       :     :        +- Filter ((isnotnull(ss_item_sk#33) && isnotnull(ss_sold_date_sk#31)) && isnotnull(ss_store_sk#38))
            :                       :     :           +- Relation[ss_sold_date_sk#31,ss_sold_time_sk#32,ss_item_sk#33,ss_customer_sk#34,ss_cdemo_sk#35,ss_hdemo_sk#36,ss_addr_sk#37,ss_store_sk#38,ss_promo_sk#39,ss_ticket_number#40,ss_quantity#41,ss_wholesale_cost#42,ss_list_price#43,ss_sales_price#44,ss_ext_discount_amt#45,ss_ext_sales_price#46,ss_ext_wholesale_cost#47,ss_ext_list_price#48,ss_ext_tax#49,ss_coupon_amt#50,ss_net_paid#51,ss_net_paid_inc_tax#52,ss_net_profit#53] parquet
            :                       :     +- Project [d_date_sk#500, d_year#506, d_moy#508]
            :                       :        +- Filter ((((d_year#506 = 1999) || ((d_year#506 = 1998) && (d_moy#508 = 12))) || ((d_year#506 = 2000) && (d_moy#508 = 1))) && isnotnull(d_date_sk#500))
            :                       :           +- Relation[d_date_sk#500,d_date_id#501,d_date#502,d_month_seq#503,d_week_seq#504,d_quarter_seq#505,d_year#506,d_dow#507,d_moy#508,d_dom#509,d_qoy#510,d_fy_year#511,d_fy_quarter_seq#512,d_fy_week_seq#513,d_day_name#514,d_quarter_name#515,d_holiday#516,d_weekend#517,d_following_holiday#518,d_first_dom#519,d_last_dom#520,d_same_day_ly#521,d_same_day_lq#522,d_current_day#523,... 4 more fields] parquet
            :                       +- Project [s_store_sk#573, s_store_name#578, s_company_name#590]
            :                          +- Filter ((isnotnull(s_store_sk#573) && isnotnull(s_store_name#578)) && isnotnull(s_company_name#590))
            :                             +- Relation[s_store_sk#573,s_store_id#574,s_rec_start_date#575,s_rec_end_date#576,s_closed_date_sk#577,s_store_name#578,s_number_employees#579,s_floor_space#580,s_hours#581,s_manager#582,s_market_id#583,s_geography_class#584,s_market_desc#585,s_market_manager#586,s_division_id#587,s_division_name#588,s_company_id#589,s_company_name#590,s_street_number#591,s_street_name#592,s_street_type#593,s_suite_number#594,s_city#595,s_county#596,... 5 more fields] parquet
            +- Project [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, sum_sales#791, rn#602]
               +- Filter isnotnull(rn#602)
                  +- Window [rank(d_year#981, d_moy#983) windowspecdefinition(i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981 ASC NULLS FIRST, d_moy#983 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#602], [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065], [d_year#981 ASC NULLS FIRST, d_moy#983 ASC NULLS FIRST]
                     +- Aggregate [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981, d_moy#983], [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981, d_moy#983, sum(ss_sales_price#44) AS sum_sales#791]
                        +- Project [i_brand#892, i_category#896, ss_sales_price#44, d_year#981, d_moy#983, s_store_name#1053, s_company_name#1065]
                           +- Join Inner, (ss_store_sk#38 = s_store_sk#1048)
                              :- Project [i_brand#892, i_category#896, ss_store_sk#38, ss_sales_price#44, d_year#981, d_moy#983]
                              :  +- Join Inner, (ss_sold_date_sk#31 = d_date_sk#975)
                              :     :- Project [i_brand#892, i_category#896, ss_sold_date_sk#31, ss_store_sk#38, ss_sales_price#44]
                              :     :  +- Join Inner, (ss_item_sk#33 = i_item_sk#884)
                              :     :     :- Project [i_item_sk#884, i_brand#892, i_category#896]
                              :     :     :  +- Filter ((isnotnull(i_item_sk#884) && isnotnull(i_category#896)) && isnotnull(i_brand#892))
                              :     :     :     +- Relation[i_item_sk#884,i_item_id#885,i_rec_start_date#886,i_rec_end_date#887,i_item_desc#888,i_current_price#889,i_wholesale_cost#890,i_brand_id#891,i_brand#892,i_class_id#893,i_class#894,i_category_id#895,i_category#896,i_manufact_id#897,i_manufact#898,i_size#899,i_formulation#900,i_color#901,i_units#902,i_container#903,i_manager_id#904,i_product_name#905] parquet
                              :     :     +- Project [ss_sold_date_sk#31, ss_item_sk#33, ss_store_sk#38, ss_sales_price#44]
                              :     :        +- Filter ((isnotnull(ss_item_sk#33) && isnotnull(ss_sold_date_sk#31)) && isnotnull(ss_store_sk#38))
                              :     :           +- Relation[ss_sold_date_sk#31,ss_sold_time_sk#32,ss_item_sk#33,ss_customer_sk#34,ss_cdemo_sk#35,ss_hdemo_sk#36,ss_addr_sk#37,ss_store_sk#38,ss_promo_sk#39,ss_ticket_number#40,ss_quantity#41,ss_wholesale_cost#42,ss_list_price#43,ss_sales_price#44,ss_ext_discount_amt#45,ss_ext_sales_price#46,ss_ext_wholesale_cost#47,ss_ext_list_price#48,ss_ext_tax#49,ss_coupon_amt#50,ss_net_paid#51,ss_net_paid_inc_tax#52,ss_net_profit#53] parquet
                              :     +- Project [d_date_sk#975, d_year#981, d_moy#983]
                              :        +- Filter ((((d_year#981 = 1999) || ((d_year#981 = 1998) && (d_moy#983 = 12))) || ((d_year#981 = 2000) && (d_moy#983 = 1))) && isnotnull(d_date_sk#975))
                              :           +- Relation[d_date_sk#975,d_date_id#976,d_date#977,d_month_seq#978,d_week_seq#979,d_quarter_seq#980,d_year#981,d_dow#982,d_moy#983,d_dom#984,d_qoy#985,d_fy_year#986,d_fy_quarter_seq#987,d_fy_week_seq#988,d_day_name#989,d_quarter_name#990,d_holiday#991,d_weekend#992,d_following_holiday#993,d_first_dom#994,d_last_dom#995,d_same_day_ly#996,d_same_day_lq#997,d_current_day#998,... 4 more fields] parquet
                              +- Project [s_store_sk#1048, s_store_name#1053, s_company_name#1065]
                                 +- Filter ((isnotnull(s_store_sk#1048) && isnotnull(s_store_name#1053)) && isnotnull(s_company_name#1065))
                                    +- Relation[s_store_sk#1048,s_store_id#1049,s_rec_start_date#1050,s_rec_end_date#1051,s_closed_date_sk#1052,s_store_name#1053,s_number_employees#1054,s_floor_space#1055,s_hours#1056,s_manager#1057,s_market_id#1058,s_geography_class#1059,s_market_desc#1060,s_market_manager#1061,s_division_id#1062,s_division_name#1063,s_company_id#1064,s_company_name#1065,s_street_number#1066,s_street_name#1067,s_street_type#1068,s_suite_number#1069,s_city#1070,s_county#1071,... 5 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[(sum_sales#0 - avg_monthly_sales#1) ASC NULLS FIRST,s_store_name#87 ASC NULLS FIRST], output=[i_category#21,i_brand#17,s_store_name#87,s_company_name#99,d_year#60,d_moy#62,avg_monthly_sales#1,sum_sales#0,psum#3,nsum#4])
+- *(28) Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, avg_monthly_sales#1, sum_sales#0, sum_sales#316 AS psum#3, sum_sales#791 AS nsum#4]
   +- *(28) SortMergeJoin [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, rn#2], [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, (rn#602 - 1)], Inner
      :- *(19) Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum_sales#0, avg_monthly_sales#1, rn#2, sum_sales#316]
      :  +- *(19) SortMergeJoin [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, rn#2], [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, (rn#127 + 1)], Inner
      :     :- *(10) Sort [i_category#21 ASC NULLS FIRST, i_brand#17 ASC NULLS FIRST, s_store_name#87 ASC NULLS FIRST, s_company_name#99 ASC NULLS FIRST, rn#2 ASC NULLS FIRST], false, 0
      :     :  +- Exchange hashpartitioning(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, rn#2, 200)
      :     :     +- *(9) Filter (((((isnotnull(d_year#60) && isnotnull(avg_monthly_sales#1)) && (d_year#60 = 1999)) && (avg_monthly_sales#1 > 0.0)) && (CASE WHEN (avg_monthly_sales#1 > 0.0) THEN (abs((sum_sales#0 - avg_monthly_sales#1)) / avg_monthly_sales#1) ELSE null END > 0.1)) && isnotnull(rn#2))
      :     :        +- Window [rank(d_year#60, d_moy#62) windowspecdefinition(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60 ASC NULLS FIRST, d_moy#62 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#2], [i_category#21, i_brand#17, s_store_name#87, s_company_name#99], [d_year#60 ASC NULLS FIRST, d_moy#62 ASC NULLS FIRST]
      :     :           +- *(8) Sort [i_category#21 ASC NULLS FIRST, i_brand#17 ASC NULLS FIRST, s_store_name#87 ASC NULLS FIRST, s_company_name#99 ASC NULLS FIRST, d_year#60 ASC NULLS FIRST, d_moy#62 ASC NULLS FIRST], false, 0
      :     :              +- Exchange hashpartitioning(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, 200)
      :     :                 +- *(7) Project [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum_sales#0, avg_monthly_sales#1]
      :     :                    +- Window [avg(_w0#122) windowspecdefinition(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_monthly_sales#1], [i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60]
      :     :                       +- *(6) Sort [i_category#21 ASC NULLS FIRST, i_brand#17 ASC NULLS FIRST, s_store_name#87 ASC NULLS FIRST, s_company_name#99 ASC NULLS FIRST, d_year#60 ASC NULLS FIRST], false, 0
      :     :                          +- Exchange hashpartitioning(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, 200)
      :     :                             +- *(5) HashAggregate(keys=[i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62], functions=[sum(ss_sales_price#44)], output=[i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum_sales#0, _w0#122])
      :     :                                +- Exchange hashpartitioning(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, 200)
      :     :                                   +- *(4) HashAggregate(keys=[i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62], functions=[partial_sum(ss_sales_price#44)], output=[i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, sum#1162])
      :     :                                      +- *(4) Project [i_brand#17, i_category#21, ss_sales_price#44, d_year#60, d_moy#62, s_store_name#87, s_company_name#99]
      :     :                                         +- *(4) BroadcastHashJoin [ss_store_sk#38], [s_store_sk#82], Inner, BuildRight
      :     :                                            :- *(4) Project [i_brand#17, i_category#21, ss_store_sk#38, ss_sales_price#44, d_year#60, d_moy#62]
      :     :                                            :  +- *(4) BroadcastHashJoin [ss_sold_date_sk#31], [d_date_sk#54], Inner, BuildRight
      :     :                                            :     :- *(4) Project [i_brand#17, i_category#21, ss_sold_date_sk#31, ss_store_sk#38, ss_sales_price#44]
      :     :                                            :     :  +- *(4) BroadcastHashJoin [i_item_sk#9], [ss_item_sk#33], Inner, BuildLeft
      :     :                                            :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                                            :     :     :  +- *(1) Project [i_item_sk#9, i_brand#17, i_category#21]
      :     :                                            :     :     :     +- *(1) Filter ((isnotnull(i_item_sk#9) && isnotnull(i_brand#17)) && isnotnull(i_category#21))
      :     :                                            :     :     :        +- *(1) FileScan parquet tpcds.item[i_item_sk#9,i_brand#17,i_category#21] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_brand), IsNotNull(i_category)], ReadSchema: struct<i_item_sk:int,i_brand:string,i_category:string>
      :     :                                            :     :     +- *(4) Project [ss_sold_date_sk#31, ss_item_sk#33, ss_store_sk#38, ss_sales_price#44]
      :     :                                            :     :        +- *(4) Filter ((isnotnull(ss_item_sk#33) && isnotnull(ss_sold_date_sk#31)) && isnotnull(ss_store_sk#38))
      :     :                                            :     :           +- *(4) FileScan parquet tpcds.store_sales[ss_sold_date_sk#31,ss_item_sk#33,ss_store_sk#38,ss_sales_price#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_sales_price:double>
      :     :                                            :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                                            :        +- *(2) Project [d_date_sk#54, d_year#60, d_moy#62]
      :     :                                            :           +- *(2) Filter ((((d_year#60 = 1999) || ((d_year#60 = 1998) && (d_moy#62 = 12))) || ((d_year#60 = 2000) && (d_moy#62 = 1))) && isnotnull(d_date_sk#54))
      :     :                                            :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#54,d_year#60,d_moy#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [Or(Or(EqualTo(d_year,1999),And(EqualTo(d_year,1998),EqualTo(d_moy,12))),And(EqualTo(d_year,2000)..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
      :     :                                            +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                                               +- *(3) Project [s_store_sk#82, s_store_name#87, s_company_name#99]
      :     :                                                  +- *(3) Filter ((isnotnull(s_store_sk#82) && isnotnull(s_company_name#99)) && isnotnull(s_store_name#87))
      :     :                                                     +- *(3) FileScan parquet tpcds.store[s_store_sk#82,s_store_name#87,s_company_name#99] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_company_name), IsNotNull(s_store_name)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_company_name:string>
      :     +- *(18) Sort [i_category#421 ASC NULLS FIRST, i_brand#417 ASC NULLS FIRST, s_store_name#578 ASC NULLS FIRST, s_company_name#590 ASC NULLS FIRST, (rn#127 + 1) ASC NULLS FIRST], false, 0
      :        +- Exchange hashpartitioning(i_category#421, i_brand#417, s_store_name#578, s_company_name#590, (rn#127 + 1), 200)
      :           +- *(17) Project [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, sum_sales#316, rn#127]
      :              +- *(17) Filter isnotnull(rn#127)
      :                 +- Window [rank(d_year#506, d_moy#508) windowspecdefinition(i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506 ASC NULLS FIRST, d_moy#508 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#127], [i_category#421, i_brand#417, s_store_name#578, s_company_name#590], [d_year#506 ASC NULLS FIRST, d_moy#508 ASC NULLS FIRST]
      :                    +- *(16) Sort [i_category#421 ASC NULLS FIRST, i_brand#417 ASC NULLS FIRST, s_store_name#578 ASC NULLS FIRST, s_company_name#590 ASC NULLS FIRST, d_year#506 ASC NULLS FIRST, d_moy#508 ASC NULLS FIRST], false, 0
      :                       +- Exchange hashpartitioning(i_category#421, i_brand#417, s_store_name#578, s_company_name#590, 200)
      :                          +- *(15) HashAggregate(keys=[i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, d_moy#508], functions=[sum(ss_sales_price#44)], output=[i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, d_moy#508, sum_sales#316])
      :                             +- ReusedExchange [i_category#421, i_brand#417, s_store_name#578, s_company_name#590, d_year#506, d_moy#508, sum#1162], Exchange hashpartitioning(i_category#21, i_brand#17, s_store_name#87, s_company_name#99, d_year#60, d_moy#62, 200)
      +- *(27) Sort [i_category#896 ASC NULLS FIRST, i_brand#892 ASC NULLS FIRST, s_store_name#1053 ASC NULLS FIRST, s_company_name#1065 ASC NULLS FIRST, (rn#602 - 1) ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, (rn#602 - 1), 200)
            +- *(26) Project [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, sum_sales#791, rn#602]
               +- *(26) Filter isnotnull(rn#602)
                  +- Window [rank(d_year#981, d_moy#983) windowspecdefinition(i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981 ASC NULLS FIRST, d_moy#983 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#602], [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065], [d_year#981 ASC NULLS FIRST, d_moy#983 ASC NULLS FIRST]
                     +- *(25) Sort [i_category#896 ASC NULLS FIRST, i_brand#892 ASC NULLS FIRST, s_store_name#1053 ASC NULLS FIRST, s_company_name#1065 ASC NULLS FIRST, d_year#981 ASC NULLS FIRST, d_moy#983 ASC NULLS FIRST], false, 0
                        +- ReusedExchange [i_category#896, i_brand#892, s_store_name#1053, s_company_name#1065, d_year#981, d_moy#983, sum_sales#791], Exchange hashpartitioning(i_category#421, i_brand#417, s_store_name#578, s_company_name#590, 200)
Time taken: 5.929 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 47 in stream 0 using template query47.tpl
------------------------------------------------------^^^

