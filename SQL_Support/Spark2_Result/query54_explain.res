== Parsed Logical Plan ==
CTE [my_customers, my_revenue, segments]
:  :- 'SubqueryAlias `my_customers`
:  :  +- 'Distinct
:  :     +- 'Project ['c_customer_sk, 'c_current_addr_sk]
:  :        +- 'Filter (((('sold_date_sk = 'd_date_sk) && ('item_sk = 'i_item_sk)) && (('i_category = Women) && ('i_class = maternity))) && ((('c_customer_sk = 'cs_or_ws_sales.customer_sk) && ('d_moy = 12)) && ('d_year = 1998)))
:  :           +- 'Join Inner
:  :              :- 'Join Inner
:  :              :  :- 'Join Inner
:  :              :  :  :- 'SubqueryAlias `cs_or_ws_sales`
:  :              :  :  :  +- 'Union
:  :              :  :  :     :- 'Project ['cs_sold_date_sk AS sold_date_sk#2, 'cs_bill_customer_sk AS customer_sk#3, 'cs_item_sk AS item_sk#4]
:  :              :  :  :     :  +- 'UnresolvedRelation `catalog_sales`
:  :              :  :  :     +- 'Project ['ws_sold_date_sk AS sold_date_sk#5, 'ws_bill_customer_sk AS customer_sk#6, 'ws_item_sk AS item_sk#7]
:  :              :  :  :        +- 'UnresolvedRelation `web_sales`
:  :              :  :  +- 'UnresolvedRelation `item`
:  :              :  +- 'UnresolvedRelation `date_dim`
:  :              +- 'UnresolvedRelation `customer`
:  :- 'SubqueryAlias `my_revenue`
:  :  +- 'Aggregate ['c_customer_sk], ['c_customer_sk, 'sum('ss_ext_sales_price) AS revenue#8]
:  :     +- 'Filter (((('c_current_addr_sk = 'ca_address_sk) && ('ca_county = 's_county)) && ('ca_state = 's_state)) && ((('ss_sold_date_sk = 'd_date_sk) && ('c_customer_sk = 'ss_customer_sk)) && (('d_month_seq >= scalar-subquery#9 []) && ('d_month_seq <= scalar-subquery#10 []))))
:  :        :  :- 'Distinct
:  :        :  :  +- 'Project [unresolvedalias(('d_month_seq + 1), None)]
:  :        :  :     +- 'Filter (('d_year = 1998) && ('d_moy = 12))
:  :        :  :        +- 'UnresolvedRelation `date_dim`
:  :        :  +- 'Distinct
:  :        :     +- 'Project [unresolvedalias(('d_month_seq + 3), None)]
:  :        :        +- 'Filter (('d_year = 1998) && ('d_moy = 12))
:  :        :           +- 'UnresolvedRelation `date_dim`
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'Join Inner
:  :           :  :  :  :- 'UnresolvedRelation `my_customers`
:  :           :  :  :  +- 'UnresolvedRelation `store_sales`
:  :           :  :  +- 'UnresolvedRelation `customer_address`
:  :           :  +- 'UnresolvedRelation `store`
:  :           +- 'UnresolvedRelation `date_dim`
:  +- 'SubqueryAlias `segments`
:     +- 'Project [cast(('revenue / 50) as int) AS segment#11]
:        +- 'UnresolvedRelation `my_revenue`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['segment ASC NULLS FIRST, 'num_customers ASC NULLS FIRST], true
         +- 'Aggregate ['segment], ['segment, 'count(1) AS num_customers#0, ('segment * 50) AS segment_base#1]
            +- 'UnresolvedRelation `segments`

== Analyzed Logical Plan ==
segment: int, num_customers: bigint, segment_base: int
GlobalLimit 100
+- LocalLimit 100
   +- Sort [segment#11 ASC NULLS FIRST, num_customers#0L ASC NULLS FIRST], true
      +- Aggregate [segment#11], [segment#11, count(1) AS num_customers#0L, (segment#11 * 50) AS segment_base#1]
         +- SubqueryAlias `segments`
            +- Project [cast((revenue#8 / cast(50 as double)) as int) AS segment#11]
               +- SubqueryAlias `my_revenue`
                  +- Aggregate [c_customer_sk#132], [c_customer_sk#132, sum(ss_ext_sales_price#165) AS revenue#8]
                     +- Filter ((((c_current_addr_sk#136 = ca_address_sk#173) && (ca_county#180 = s_county#209)) && (ca_state#181 = s_state#210)) && (((ss_sold_date_sk#150 = d_date_sk#104) && (c_customer_sk#132 = ss_customer_sk#153)) && ((d_month_seq#107 >= scalar-subquery#9 []) && (d_month_seq#107 <= scalar-subquery#10 []))))
                        :  :- Distinct
                        :  :  +- Project [(d_month_seq#107 + 1) AS (d_month_seq + 1)#215]
                        :  :     +- Filter ((d_year#110 = 1998) && (d_moy#112 = 12))
                        :  :        +- SubqueryAlias `tpcds`.`date_dim`
                        :  :           +- Relation[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet
                        :  +- Distinct
                        :     +- Project [(d_month_seq#107 + 3) AS (d_month_seq + 3)#216]
                        :        +- Filter ((d_year#110 = 1998) && (d_moy#112 = 12))
                        :           +- SubqueryAlias `tpcds`.`date_dim`
                        :              +- Relation[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet
                        +- Join Inner
                           :- Join Inner
                           :  :- Join Inner
                           :  :  :- Join Inner
                           :  :  :  :- SubqueryAlias `my_customers`
                           :  :  :  :  +- Distinct
                           :  :  :  :     +- Project [c_customer_sk#132, c_current_addr_sk#136]
                           :  :  :  :        +- Filter ((((sold_date_sk#2 = d_date_sk#104) && (item_sk#4 = i_item_sk#82)) && ((i_category#94 = Women) && (i_class#92 = maternity))) && (((c_customer_sk#132 = customer_sk#3) && (d_moy#112 = 12)) && (d_year#110 = 1998)))
                           :  :  :  :           +- Join Inner
                           :  :  :  :              :- Join Inner
                           :  :  :  :              :  :- Join Inner
                           :  :  :  :              :  :  :- SubqueryAlias `cs_or_ws_sales`
                           :  :  :  :              :  :  :  +- Union
                           :  :  :  :              :  :  :     :- Project [cs_sold_date_sk#14 AS sold_date_sk#2, cs_bill_customer_sk#17 AS customer_sk#3, cs_item_sk#29 AS item_sk#4]
                           :  :  :  :              :  :  :     :  +- SubqueryAlias `tpcds`.`catalog_sales`
                           :  :  :  :              :  :  :     :     +- Relation[cs_sold_date_sk#14,cs_sold_time_sk#15,cs_ship_date_sk#16,cs_bill_customer_sk#17,cs_bill_cdemo_sk#18,cs_bill_hdemo_sk#19,cs_bill_addr_sk#20,cs_ship_customer_sk#21,cs_ship_cdemo_sk#22,cs_ship_hdemo_sk#23,cs_ship_addr_sk#24,cs_call_center_sk#25,cs_catalog_page_sk#26,cs_ship_mode_sk#27,cs_warehouse_sk#28,cs_item_sk#29,cs_promo_sk#30,cs_order_number#31,cs_quantity#32,cs_wholesale_cost#33,cs_list_price#34,cs_sales_price#35,cs_ext_discount_amt#36,cs_ext_sales_price#37,... 10 more fields] parquet
                           :  :  :  :              :  :  :     +- Project [ws_sold_date_sk#48 AS sold_date_sk#5, ws_bill_customer_sk#52 AS customer_sk#6, ws_item_sk#51 AS item_sk#7]
                           :  :  :  :              :  :  :        +- SubqueryAlias `tpcds`.`web_sales`
                           :  :  :  :              :  :  :           +- Relation[ws_sold_date_sk#48,ws_sold_time_sk#49,ws_ship_date_sk#50,ws_item_sk#51,ws_bill_customer_sk#52,ws_bill_cdemo_sk#53,ws_bill_hdemo_sk#54,ws_bill_addr_sk#55,ws_ship_customer_sk#56,ws_ship_cdemo_sk#57,ws_ship_hdemo_sk#58,ws_ship_addr_sk#59,ws_web_page_sk#60,ws_web_site_sk#61,ws_ship_mode_sk#62,ws_warehouse_sk#63,ws_promo_sk#64,ws_order_number#65,ws_quantity#66,ws_wholesale_cost#67,ws_list_price#68,ws_sales_price#69,ws_ext_discount_amt#70,ws_ext_sales_price#71,... 10 more fields] parquet
                           :  :  :  :              :  :  +- SubqueryAlias `tpcds`.`item`
                           :  :  :  :              :  :     +- Relation[i_item_sk#82,i_item_id#83,i_rec_start_date#84,i_rec_end_date#85,i_item_desc#86,i_current_price#87,i_wholesale_cost#88,i_brand_id#89,i_brand#90,i_class_id#91,i_class#92,i_category_id#93,i_category#94,i_manufact_id#95,i_manufact#96,i_size#97,i_formulation#98,i_color#99,i_units#100,i_container#101,i_manager_id#102,i_product_name#103] parquet
                           :  :  :  :              :  +- SubqueryAlias `tpcds`.`date_dim`
                           :  :  :  :              :     +- Relation[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet
                           :  :  :  :              +- SubqueryAlias `tpcds`.`customer`
                           :  :  :  :                 +- Relation[c_customer_sk#132,c_customer_id#133,c_current_cdemo_sk#134,c_current_hdemo_sk#135,c_current_addr_sk#136,c_first_shipto_date_sk#137,c_first_sales_date_sk#138,c_salutation#139,c_first_name#140,c_last_name#141,c_preferred_cust_flag#142,c_birth_day#143,c_birth_month#144,c_birth_year#145,c_birth_country#146,c_login#147,c_email_address#148,c_last_review_date#149] parquet
                           :  :  :  +- SubqueryAlias `tpcds`.`store_sales`
                           :  :  :     +- Relation[ss_sold_date_sk#150,ss_sold_time_sk#151,ss_item_sk#152,ss_customer_sk#153,ss_cdemo_sk#154,ss_hdemo_sk#155,ss_addr_sk#156,ss_store_sk#157,ss_promo_sk#158,ss_ticket_number#159,ss_quantity#160,ss_wholesale_cost#161,ss_list_price#162,ss_sales_price#163,ss_ext_discount_amt#164,ss_ext_sales_price#165,ss_ext_wholesale_cost#166,ss_ext_list_price#167,ss_ext_tax#168,ss_coupon_amt#169,ss_net_paid#170,ss_net_paid_inc_tax#171,ss_net_profit#172] parquet
                           :  :  +- SubqueryAlias `tpcds`.`customer_address`
                           :  :     +- Relation[ca_address_sk#173,ca_address_id#174,ca_street_number#175,ca_street_name#176,ca_street_type#177,ca_suite_number#178,ca_city#179,ca_county#180,ca_state#181,ca_zip#182,ca_country#183,ca_gmt_offset#184,ca_location_type#185] parquet
                           :  +- SubqueryAlias `tpcds`.`store`
                           :     +- Relation[s_store_sk#186,s_store_id#187,s_rec_start_date#188,s_rec_end_date#189,s_closed_date_sk#190,s_store_name#191,s_number_employees#192,s_floor_space#193,s_hours#194,s_manager#195,s_market_id#196,s_geography_class#197,s_market_desc#198,s_market_manager#199,s_division_id#200,s_division_name#201,s_company_id#202,s_company_name#203,s_street_number#204,s_street_name#205,s_street_type#206,s_suite_number#207,s_city#208,s_county#209,... 5 more fields] parquet
                           +- SubqueryAlias `tpcds`.`date_dim`
                              +- Relation[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [segment#11 ASC NULLS FIRST, num_customers#0L ASC NULLS FIRST], true
      +- Aggregate [segment#11], [segment#11, count(1) AS num_customers#0L, (segment#11 * 50) AS segment_base#1]
         +- Aggregate [c_customer_sk#132], [cast((sum(ss_ext_sales_price#165) / 50.0) as int) AS segment#11]
            +- Project [c_customer_sk#132, ss_ext_sales_price#165]
               +- Join Inner, (ss_sold_date_sk#150 = d_date_sk#104)
                  :- Project [c_customer_sk#132, ss_sold_date_sk#150, ss_ext_sales_price#165]
                  :  +- Join Inner, ((ca_county#180 = s_county#209) && (ca_state#181 = s_state#210))
                  :     :- Project [c_customer_sk#132, ss_sold_date_sk#150, ss_ext_sales_price#165, ca_county#180, ca_state#181]
                  :     :  +- Join Inner, (c_current_addr_sk#136 = ca_address_sk#173)
                  :     :     :- Project [c_customer_sk#132, c_current_addr_sk#136, ss_sold_date_sk#150, ss_ext_sales_price#165]
                  :     :     :  +- Join Inner, (c_customer_sk#132 = ss_customer_sk#153)
                  :     :     :     :- Aggregate [c_customer_sk#132, c_current_addr_sk#136], [c_customer_sk#132, c_current_addr_sk#136]
                  :     :     :     :  +- Project [c_customer_sk#132, c_current_addr_sk#136]
                  :     :     :     :     +- Join Inner, (c_customer_sk#132 = customer_sk#3)
                  :     :     :     :        :- Project [customer_sk#3]
                  :     :     :     :        :  +- Join Inner, (sold_date_sk#2 = d_date_sk#104)
                  :     :     :     :        :     :- Project [sold_date_sk#2, customer_sk#3]
                  :     :     :     :        :     :  +- Join Inner, (item_sk#4 = i_item_sk#82)
                  :     :     :     :        :     :     :- Union
                  :     :     :     :        :     :     :  :- Project [cs_sold_date_sk#14 AS sold_date_sk#2, cs_bill_customer_sk#17 AS customer_sk#3, cs_item_sk#29 AS item_sk#4]
                  :     :     :     :        :     :     :  :  +- Filter ((isnotnull(cs_item_sk#29) && isnotnull(cs_sold_date_sk#14)) && isnotnull(cs_bill_customer_sk#17))
                  :     :     :     :        :     :     :  :     +- Relation[cs_sold_date_sk#14,cs_sold_time_sk#15,cs_ship_date_sk#16,cs_bill_customer_sk#17,cs_bill_cdemo_sk#18,cs_bill_hdemo_sk#19,cs_bill_addr_sk#20,cs_ship_customer_sk#21,cs_ship_cdemo_sk#22,cs_ship_hdemo_sk#23,cs_ship_addr_sk#24,cs_call_center_sk#25,cs_catalog_page_sk#26,cs_ship_mode_sk#27,cs_warehouse_sk#28,cs_item_sk#29,cs_promo_sk#30,cs_order_number#31,cs_quantity#32,cs_wholesale_cost#33,cs_list_price#34,cs_sales_price#35,cs_ext_discount_amt#36,cs_ext_sales_price#37,... 10 more fields] parquet
                  :     :     :     :        :     :     :  +- Project [ws_sold_date_sk#48 AS sold_date_sk#5, ws_bill_customer_sk#52 AS customer_sk#6, ws_item_sk#51 AS item_sk#7]
                  :     :     :     :        :     :     :     +- Filter ((isnotnull(ws_item_sk#51) && isnotnull(ws_sold_date_sk#48)) && isnotnull(ws_bill_customer_sk#52))
                  :     :     :     :        :     :     :        +- Relation[ws_sold_date_sk#48,ws_sold_time_sk#49,ws_ship_date_sk#50,ws_item_sk#51,ws_bill_customer_sk#52,ws_bill_cdemo_sk#53,ws_bill_hdemo_sk#54,ws_bill_addr_sk#55,ws_ship_customer_sk#56,ws_ship_cdemo_sk#57,ws_ship_hdemo_sk#58,ws_ship_addr_sk#59,ws_web_page_sk#60,ws_web_site_sk#61,ws_ship_mode_sk#62,ws_warehouse_sk#63,ws_promo_sk#64,ws_order_number#65,ws_quantity#66,ws_wholesale_cost#67,ws_list_price#68,ws_sales_price#69,ws_ext_discount_amt#70,ws_ext_sales_price#71,... 10 more fields] parquet
                  :     :     :     :        :     :     +- Project [i_item_sk#82]
                  :     :     :     :        :     :        +- Filter ((((isnotnull(i_category#94) && isnotnull(i_class#92)) && (i_category#94 = Women)) && (i_class#92 = maternity)) && isnotnull(i_item_sk#82))
                  :     :     :     :        :     :           +- Relation[i_item_sk#82,i_item_id#83,i_rec_start_date#84,i_rec_end_date#85,i_item_desc#86,i_current_price#87,i_wholesale_cost#88,i_brand_id#89,i_brand#90,i_class_id#91,i_class#92,i_category_id#93,i_category#94,i_manufact_id#95,i_manufact#96,i_size#97,i_formulation#98,i_color#99,i_units#100,i_container#101,i_manager_id#102,i_product_name#103] parquet
                  :     :     :     :        :     +- Project [d_date_sk#104]
                  :     :     :     :        :        +- Filter ((((isnotnull(d_moy#112) && isnotnull(d_year#110)) && (d_moy#112 = 12)) && (d_year#110 = 1998)) && isnotnull(d_date_sk#104))
                  :     :     :     :        :           +- Relation[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet
                  :     :     :     :        +- Project [c_customer_sk#132, c_current_addr_sk#136]
                  :     :     :     :           +- Filter (isnotnull(c_customer_sk#132) && isnotnull(c_current_addr_sk#136))
                  :     :     :     :              +- Relation[c_customer_sk#132,c_customer_id#133,c_current_cdemo_sk#134,c_current_hdemo_sk#135,c_current_addr_sk#136,c_first_shipto_date_sk#137,c_first_sales_date_sk#138,c_salutation#139,c_first_name#140,c_last_name#141,c_preferred_cust_flag#142,c_birth_day#143,c_birth_month#144,c_birth_year#145,c_birth_country#146,c_login#147,c_email_address#148,c_last_review_date#149] parquet
                  :     :     :     +- Project [ss_sold_date_sk#150, ss_customer_sk#153, ss_ext_sales_price#165]
                  :     :     :        +- Filter (isnotnull(ss_customer_sk#153) && isnotnull(ss_sold_date_sk#150))
                  :     :     :           +- Relation[ss_sold_date_sk#150,ss_sold_time_sk#151,ss_item_sk#152,ss_customer_sk#153,ss_cdemo_sk#154,ss_hdemo_sk#155,ss_addr_sk#156,ss_store_sk#157,ss_promo_sk#158,ss_ticket_number#159,ss_quantity#160,ss_wholesale_cost#161,ss_list_price#162,ss_sales_price#163,ss_ext_discount_amt#164,ss_ext_sales_price#165,ss_ext_wholesale_cost#166,ss_ext_list_price#167,ss_ext_tax#168,ss_coupon_amt#169,ss_net_paid#170,ss_net_paid_inc_tax#171,ss_net_profit#172] parquet
                  :     :     +- Project [ca_address_sk#173, ca_county#180, ca_state#181]
                  :     :        +- Filter ((isnotnull(ca_address_sk#173) && isnotnull(ca_state#181)) && isnotnull(ca_county#180))
                  :     :           +- Relation[ca_address_sk#173,ca_address_id#174,ca_street_number#175,ca_street_name#176,ca_street_type#177,ca_suite_number#178,ca_city#179,ca_county#180,ca_state#181,ca_zip#182,ca_country#183,ca_gmt_offset#184,ca_location_type#185] parquet
                  :     +- Project [s_county#209, s_state#210]
                  :        +- Filter (isnotnull(s_state#210) && isnotnull(s_county#209))
                  :           +- Relation[s_store_sk#186,s_store_id#187,s_rec_start_date#188,s_rec_end_date#189,s_closed_date_sk#190,s_store_name#191,s_number_employees#192,s_floor_space#193,s_hours#194,s_manager#195,s_market_id#196,s_geography_class#197,s_market_desc#198,s_market_manager#199,s_division_id#200,s_division_name#201,s_company_id#202,s_company_name#203,s_street_number#204,s_street_name#205,s_street_type#206,s_suite_number#207,s_city#208,s_county#209,... 5 more fields] parquet
                  +- Project [d_date_sk#104]
                     +- Filter (((isnotnull(d_month_seq#107) && (d_month_seq#107 >= scalar-subquery#9 [])) && (d_month_seq#107 <= scalar-subquery#10 [])) && isnotnull(d_date_sk#104))
                        :  :- Aggregate [(d_month_seq + 1)#215], [(d_month_seq + 1)#215]
                        :  :  +- Project [(d_month_seq#107 + 1) AS (d_month_seq + 1)#215]
                        :  :     +- Filter (((isnotnull(d_year#110) && isnotnull(d_moy#112)) && (d_year#110 = 1998)) && (d_moy#112 = 12))
                        :  :        +- Relation[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet
                        :  +- Aggregate [(d_month_seq + 3)#216], [(d_month_seq + 3)#216]
                        :     +- Project [(d_month_seq#107 + 3) AS (d_month_seq + 3)#216]
                        :        +- Filter (((isnotnull(d_year#110) && isnotnull(d_moy#112)) && (d_year#110 = 1998)) && (d_moy#112 = 12))
                        :           +- Relation[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet
                        +- Relation[d_date_sk#104,d_date_id#105,d_date#106,d_month_seq#107,d_week_seq#108,d_quarter_seq#109,d_year#110,d_dow#111,d_moy#112,d_dom#113,d_qoy#114,d_fy_year#115,d_fy_quarter_seq#116,d_fy_week_seq#117,d_day_name#118,d_quarter_name#119,d_holiday#120,d_weekend#121,d_following_holiday#122,d_first_dom#123,d_last_dom#124,d_same_day_ly#125,d_same_day_lq#126,d_current_day#127,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[segment#11 ASC NULLS FIRST,num_customers#0L ASC NULLS FIRST], output=[segment#11,num_customers#0L,segment_base#1])
+- *(15) HashAggregate(keys=[segment#11], functions=[count(1)], output=[segment#11, num_customers#0L, segment_base#1])
   +- Exchange hashpartitioning(segment#11, 200)
      +- *(14) HashAggregate(keys=[segment#11], functions=[partial_count(1)], output=[segment#11, count#222L])
         +- *(14) HashAggregate(keys=[c_customer_sk#132], functions=[sum(ss_ext_sales_price#165)], output=[segment#11])
            +- *(14) HashAggregate(keys=[c_customer_sk#132], functions=[partial_sum(ss_ext_sales_price#165)], output=[c_customer_sk#132, sum#224])
               +- *(14) Project [c_customer_sk#132, ss_ext_sales_price#165]
                  +- *(14) BroadcastHashJoin [ss_sold_date_sk#150], [d_date_sk#104], Inner, BuildRight
                     :- *(14) Project [c_customer_sk#132, ss_sold_date_sk#150, ss_ext_sales_price#165]
                     :  +- *(14) BroadcastHashJoin [ca_county#180, ca_state#181], [s_county#209, s_state#210], Inner, BuildRight
                     :     :- *(14) Project [c_customer_sk#132, ss_sold_date_sk#150, ss_ext_sales_price#165, ca_county#180, ca_state#181]
                     :     :  +- *(14) BroadcastHashJoin [c_current_addr_sk#136], [ca_address_sk#173], Inner, BuildRight
                     :     :     :- *(14) Project [c_customer_sk#132, c_current_addr_sk#136, ss_sold_date_sk#150, ss_ext_sales_price#165]
                     :     :     :  +- *(14) SortMergeJoin [c_customer_sk#132], [ss_customer_sk#153], Inner
                     :     :     :     :- *(8) Sort [c_customer_sk#132 ASC NULLS FIRST], false, 0
                     :     :     :     :  +- Exchange hashpartitioning(c_customer_sk#132, 200)
                     :     :     :     :     +- *(7) HashAggregate(keys=[c_customer_sk#132, c_current_addr_sk#136], functions=[], output=[c_customer_sk#132, c_current_addr_sk#136])
                     :     :     :     :        +- Exchange hashpartitioning(c_customer_sk#132, c_current_addr_sk#136, 200)
                     :     :     :     :           +- *(6) HashAggregate(keys=[c_customer_sk#132, c_current_addr_sk#136], functions=[], output=[c_customer_sk#132, c_current_addr_sk#136])
                     :     :     :     :              +- *(6) Project [c_customer_sk#132, c_current_addr_sk#136]
                     :     :     :     :                 +- *(6) BroadcastHashJoin [customer_sk#3], [c_customer_sk#132], Inner, BuildRight
                     :     :     :     :                    :- *(6) Project [customer_sk#3]
                     :     :     :     :                    :  +- *(6) BroadcastHashJoin [sold_date_sk#2], [d_date_sk#104], Inner, BuildRight
                     :     :     :     :                    :     :- *(6) Project [sold_date_sk#2, customer_sk#3]
                     :     :     :     :                    :     :  +- *(6) BroadcastHashJoin [item_sk#4], [i_item_sk#82], Inner, BuildRight
                     :     :     :     :                    :     :     :- Union
                     :     :     :     :                    :     :     :  :- *(1) Project [cs_sold_date_sk#14 AS sold_date_sk#2, cs_bill_customer_sk#17 AS customer_sk#3, cs_item_sk#29 AS item_sk#4]
                     :     :     :     :                    :     :     :  :  +- *(1) Filter ((isnotnull(cs_item_sk#29) && isnotnull(cs_sold_date_sk#14)) && isnotnull(cs_bill_customer_sk#17))
                     :     :     :     :                    :     :     :  :     +- *(1) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#14,cs_bill_customer_sk#17,cs_item_sk#29] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk), IsNotNull(cs_bill_customer_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int>
                     :     :     :     :                    :     :     :  +- *(2) Project [ws_sold_date_sk#48 AS sold_date_sk#5, ws_bill_customer_sk#52 AS customer_sk#6, ws_item_sk#51 AS item_sk#7]
                     :     :     :     :                    :     :     :     +- *(2) Filter ((isnotnull(ws_item_sk#51) && isnotnull(ws_sold_date_sk#48)) && isnotnull(ws_bill_customer_sk#52))
                     :     :     :     :                    :     :     :        +- *(2) FileScan parquet tpcds.web_sales[ws_sold_date_sk#48,ws_item_sk#51,ws_bill_customer_sk#52] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_customer_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_customer_sk:int>
                     :     :     :     :                    :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :     :     :     :                    :     :        +- *(3) Project [i_item_sk#82]
                     :     :     :     :                    :     :           +- *(3) Filter ((((isnotnull(i_category#94) && isnotnull(i_class#92)) && (i_category#94 = Women)) && (i_class#92 = maternity)) && isnotnull(i_item_sk#82))
                     :     :     :     :                    :     :              +- *(3) FileScan parquet tpcds.item[i_item_sk#82,i_class#92,i_category#94] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), IsNotNull(i_class), EqualTo(i_category,Women), EqualTo(i_class,maternity)..., ReadSchema: struct<i_item_sk:int,i_class:string,i_category:string>
                     :     :     :     :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :     :     :     :                    :        +- *(4) Project [d_date_sk#104]
                     :     :     :     :                    :           +- *(4) Filter ((((isnotnull(d_moy#112) && isnotnull(d_year#110)) && (d_moy#112 = 12)) && (d_year#110 = 1998)) && isnotnull(d_date_sk#104))
                     :     :     :     :                    :              +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#104,d_year#110,d_moy#112] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,12), EqualTo(d_year,1998), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                     :     :     :     :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :     :     :     :                       +- *(5) Project [c_customer_sk#132, c_current_addr_sk#136]
                     :     :     :     :                          +- *(5) Filter (isnotnull(c_customer_sk#132) && isnotnull(c_current_addr_sk#136))
                     :     :     :     :                             +- *(5) FileScan parquet tpcds.customer[c_customer_sk#132,c_current_addr_sk#136] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
                     :     :     :     +- *(10) Sort [ss_customer_sk#153 ASC NULLS FIRST], false, 0
                     :     :     :        +- Exchange hashpartitioning(ss_customer_sk#153, 200)
                     :     :     :           +- *(9) Project [ss_sold_date_sk#150, ss_customer_sk#153, ss_ext_sales_price#165]
                     :     :     :              +- *(9) Filter (isnotnull(ss_customer_sk#153) && isnotnull(ss_sold_date_sk#150))
                     :     :     :                 +- *(9) FileScan parquet tpcds.store_sales[ss_sold_date_sk#150,ss_customer_sk#153,ss_ext_sales_price#165] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_customer_sk:int,ss_ext_sales_price:double>
                     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :     :        +- *(11) Project [ca_address_sk#173, ca_county#180, ca_state#181]
                     :     :           +- *(11) Filter ((isnotnull(ca_address_sk#173) && isnotnull(ca_state#181)) && isnotnull(ca_county#180))
                     :     :              +- *(11) FileScan parquet tpcds.customer_address[ca_address_sk#173,ca_county#180,ca_state#181] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_state), IsNotNull(ca_county)], ReadSchema: struct<ca_address_sk:int,ca_county:string,ca_state:string>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true], input[1, string, true]))
                     :        +- *(12) Project [s_county#209, s_state#210]
                     :           +- *(12) Filter (isnotnull(s_state#210) && isnotnull(s_county#209))
                     :              +- *(12) FileScan parquet tpcds.store[s_county#209,s_state#210] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_state), IsNotNull(s_county)], ReadSchema: struct<s_county:string,s_state:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                        +- *(13) Project [d_date_sk#104]
                           +- *(13) Filter (((isnotnull(d_month_seq#107) && (d_month_seq#107 >= Subquery subquery9)) && (d_month_seq#107 <= Subquery subquery10)) && isnotnull(d_date_sk#104))
                              :  :- Subquery subquery9
                              :  :  +- *(2) HashAggregate(keys=[(d_month_seq + 1)#215], functions=[], output=[(d_month_seq + 1)#215])
                              :  :     +- Exchange hashpartitioning((d_month_seq + 1)#215, 200)
                              :  :        +- *(1) HashAggregate(keys=[(d_month_seq + 1)#215], functions=[], output=[(d_month_seq + 1)#215])
                              :  :           +- *(1) Project [(d_month_seq#107 + 1) AS (d_month_seq + 1)#215]
                              :  :              +- *(1) Filter (((isnotnull(d_year#110) && isnotnull(d_moy#112)) && (d_year#110 = 1998)) && (d_moy#112 = 12))
                              :  :                 +- *(1) FileScan parquet tpcds.date_dim[d_month_seq#107,d_year#110,d_moy#112] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,12)], ReadSchema: struct<d_month_seq:int,d_year:int,d_moy:int>
                              :  +- Subquery subquery10
                              :     +- *(2) HashAggregate(keys=[(d_month_seq + 3)#216], functions=[], output=[(d_month_seq + 3)#216])
                              :        +- Exchange hashpartitioning((d_month_seq + 3)#216, 200)
                              :           +- *(1) HashAggregate(keys=[(d_month_seq + 3)#216], functions=[], output=[(d_month_seq + 3)#216])
                              :              +- *(1) Project [(d_month_seq#107 + 3) AS (d_month_seq + 3)#216]
                              :                 +- *(1) Filter (((isnotnull(d_year#110) && isnotnull(d_moy#112)) && (d_year#110 = 1998)) && (d_moy#112 = 12))
                              :                    +- *(1) FileScan parquet tpcds.date_dim[d_month_seq#107,d_year#110,d_moy#112] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,12)], ReadSchema: struct<d_month_seq:int,d_year:int,d_moy:int>
                              +- *(13) FileScan parquet tpcds.date_dim[d_date_sk#104,d_month_seq#107] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_month_seq:int>
                                    :- Subquery subquery9
                                    :  +- *(2) HashAggregate(keys=[(d_month_seq + 1)#215], functions=[], output=[(d_month_seq + 1)#215])
                                    :     +- Exchange hashpartitioning((d_month_seq + 1)#215, 200)
                                    :        +- *(1) HashAggregate(keys=[(d_month_seq + 1)#215], functions=[], output=[(d_month_seq + 1)#215])
                                    :           +- *(1) Project [(d_month_seq#107 + 1) AS (d_month_seq + 1)#215]
                                    :              +- *(1) Filter (((isnotnull(d_year#110) && isnotnull(d_moy#112)) && (d_year#110 = 1998)) && (d_moy#112 = 12))
                                    :                 +- *(1) FileScan parquet tpcds.date_dim[d_month_seq#107,d_year#110,d_moy#112] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,12)], ReadSchema: struct<d_month_seq:int,d_year:int,d_moy:int>
                                    +- Subquery subquery10
                                       +- *(2) HashAggregate(keys=[(d_month_seq + 3)#216], functions=[], output=[(d_month_seq + 3)#216])
                                          +- Exchange hashpartitioning((d_month_seq + 3)#216, 200)
                                             +- *(1) HashAggregate(keys=[(d_month_seq + 3)#216], functions=[], output=[(d_month_seq + 3)#216])
                                                +- *(1) Project [(d_month_seq#107 + 3) AS (d_month_seq + 3)#216]
                                                   +- *(1) Filter (((isnotnull(d_year#110) && isnotnull(d_moy#112)) && (d_year#110 = 1998)) && (d_moy#112 = 12))
                                                      +- *(1) FileScan parquet tpcds.date_dim[d_month_seq#107,d_year#110,d_moy#112] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,12)], ReadSchema: struct<d_month_seq:int,d_year:int,d_moy:int>
Time taken: 4.983 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 54 in stream 0 using template query54.tpl
------------------------------------------------------^^^

