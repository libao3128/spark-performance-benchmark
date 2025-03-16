== Parsed Logical Plan ==
CTE [ss, cs, ws]
:  :- 'SubqueryAlias `ss`
:  :  +- 'Aggregate ['i_manufact_id], ['i_manufact_id, 'sum('ss_ext_sales_price) AS total_sales#1]
:  :     +- 'Filter ((('i_manufact_id IN (list#2 []) && ('ss_item_sk = 'i_item_sk)) && (('ss_sold_date_sk = 'd_date_sk) && ('d_year = 1998))) && ((('d_moy = 5) && ('ss_addr_sk = 'ca_address_sk)) && ('ca_gmt_offset = -5)))
:  :        :  +- 'Project ['i_manufact_id]
:  :        :     +- 'Filter 'i_category IN (Electronics)
:  :        :        +- 'UnresolvedRelation `item`
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'UnresolvedRelation `store_sales`
:  :           :  :  +- 'UnresolvedRelation `date_dim`
:  :           :  +- 'UnresolvedRelation `customer_address`
:  :           +- 'UnresolvedRelation `item`
:  :- 'SubqueryAlias `cs`
:  :  +- 'Aggregate ['i_manufact_id], ['i_manufact_id, 'sum('cs_ext_sales_price) AS total_sales#3]
:  :     +- 'Filter ((('i_manufact_id IN (list#4 []) && ('cs_item_sk = 'i_item_sk)) && (('cs_sold_date_sk = 'd_date_sk) && ('d_year = 1998))) && ((('d_moy = 5) && ('cs_bill_addr_sk = 'ca_address_sk)) && ('ca_gmt_offset = -5)))
:  :        :  +- 'Project ['i_manufact_id]
:  :        :     +- 'Filter 'i_category IN (Electronics)
:  :        :        +- 'UnresolvedRelation `item`
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'Join Inner
:  :           :  :  :- 'UnresolvedRelation `catalog_sales`
:  :           :  :  +- 'UnresolvedRelation `date_dim`
:  :           :  +- 'UnresolvedRelation `customer_address`
:  :           +- 'UnresolvedRelation `item`
:  +- 'SubqueryAlias `ws`
:     +- 'Aggregate ['i_manufact_id], ['i_manufact_id, 'sum('ws_ext_sales_price) AS total_sales#5]
:        +- 'Filter ((('i_manufact_id IN (list#6 []) && ('ws_item_sk = 'i_item_sk)) && (('ws_sold_date_sk = 'd_date_sk) && ('d_year = 1998))) && ((('d_moy = 5) && ('ws_bill_addr_sk = 'ca_address_sk)) && ('ca_gmt_offset = -5)))
:           :  +- 'Project ['i_manufact_id]
:           :     +- 'Filter 'i_category IN (Electronics)
:           :        +- 'UnresolvedRelation `item`
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'Join Inner
:              :  :  :- 'UnresolvedRelation `web_sales`
:              :  :  +- 'UnresolvedRelation `date_dim`
:              :  +- 'UnresolvedRelation `customer_address`
:              +- 'UnresolvedRelation `item`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['total_sales ASC NULLS FIRST], true
         +- 'Aggregate ['i_manufact_id], ['i_manufact_id, 'sum('total_sales) AS total_sales#0]
            +- 'SubqueryAlias `tmp1`
               +- 'Union
                  :- 'Union
                  :  :- 'Project [*]
                  :  :  +- 'UnresolvedRelation `ss`
                  :  +- 'Project [*]
                  :     +- 'UnresolvedRelation `cs`
                  +- 'Project [*]
                     +- 'UnresolvedRelation `ws`

== Analyzed Logical Plan ==
i_manufact_id: int, total_sales: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [total_sales#0 ASC NULLS FIRST], true
      +- Aggregate [i_manufact_id#86], [i_manufact_id#86, sum(total_sales#1) AS total_sales#0]
         +- SubqueryAlias `tmp1`
            +- Union
               :- Union
               :  :- Project [i_manufact_id#86, total_sales#1]
               :  :  +- SubqueryAlias `ss`
               :  :     +- Aggregate [i_manufact_id#86], [i_manufact_id#86, sum(ss_ext_sales_price#24) AS total_sales#1]
               :  :        +- Filter (((i_manufact_id#86 IN (list#2 []) && (ss_item_sk#11 = i_item_sk#73)) && ((ss_sold_date_sk#9 = d_date_sk#32) && (d_year#38 = 1998))) && (((d_moy#40 = 5) && (ss_addr_sk#15 = ca_address_sk#60)) && (ca_gmt_offset#71 = cast(-5 as double))))
               :  :           :  +- Project [i_manufact_id#86]
               :  :           :     +- Filter i_category#85 IN (Electronics)
               :  :           :        +- SubqueryAlias `tpcds`.`item`
               :  :           :           +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
               :  :           +- Join Inner
               :  :              :- Join Inner
               :  :              :  :- Join Inner
               :  :              :  :  :- SubqueryAlias `tpcds`.`store_sales`
               :  :              :  :  :  +- Relation[ss_sold_date_sk#9,ss_sold_time_sk#10,ss_item_sk#11,ss_customer_sk#12,ss_cdemo_sk#13,ss_hdemo_sk#14,ss_addr_sk#15,ss_store_sk#16,ss_promo_sk#17,ss_ticket_number#18,ss_quantity#19,ss_wholesale_cost#20,ss_list_price#21,ss_sales_price#22,ss_ext_discount_amt#23,ss_ext_sales_price#24,ss_ext_wholesale_cost#25,ss_ext_list_price#26,ss_ext_tax#27,ss_coupon_amt#28,ss_net_paid#29,ss_net_paid_inc_tax#30,ss_net_profit#31] parquet
               :  :              :  :  +- SubqueryAlias `tpcds`.`date_dim`
               :  :              :  :     +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
               :  :              :  +- SubqueryAlias `tpcds`.`customer_address`
               :  :              :     +- Relation[ca_address_sk#60,ca_address_id#61,ca_street_number#62,ca_street_name#63,ca_street_type#64,ca_suite_number#65,ca_city#66,ca_county#67,ca_state#68,ca_zip#69,ca_country#70,ca_gmt_offset#71,ca_location_type#72] parquet
               :  :              +- SubqueryAlias `tpcds`.`item`
               :  :                 +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
               :  +- Project [i_manufact_id#86, total_sales#3]
               :     +- SubqueryAlias `cs`
               :        +- Aggregate [i_manufact_id#86], [i_manufact_id#86, sum(cs_ext_sales_price#119) AS total_sales#3]
               :           +- Filter (((i_manufact_id#86 IN (list#4 []) && (cs_item_sk#111 = i_item_sk#73)) && ((cs_sold_date_sk#96 = d_date_sk#32) && (d_year#38 = 1998))) && (((d_moy#40 = 5) && (cs_bill_addr_sk#102 = ca_address_sk#60)) && (ca_gmt_offset#71 = cast(-5 as double))))
               :              :  +- Project [i_manufact_id#86]
               :              :     +- Filter i_category#85 IN (Electronics)
               :              :        +- SubqueryAlias `tpcds`.`item`
               :              :           +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
               :              +- Join Inner
               :                 :- Join Inner
               :                 :  :- Join Inner
               :                 :  :  :- SubqueryAlias `tpcds`.`catalog_sales`
               :                 :  :  :  +- Relation[cs_sold_date_sk#96,cs_sold_time_sk#97,cs_ship_date_sk#98,cs_bill_customer_sk#99,cs_bill_cdemo_sk#100,cs_bill_hdemo_sk#101,cs_bill_addr_sk#102,cs_ship_customer_sk#103,cs_ship_cdemo_sk#104,cs_ship_hdemo_sk#105,cs_ship_addr_sk#106,cs_call_center_sk#107,cs_catalog_page_sk#108,cs_ship_mode_sk#109,cs_warehouse_sk#110,cs_item_sk#111,cs_promo_sk#112,cs_order_number#113,cs_quantity#114,cs_wholesale_cost#115,cs_list_price#116,cs_sales_price#117,cs_ext_discount_amt#118,cs_ext_sales_price#119,... 10 more fields] parquet
               :                 :  :  +- SubqueryAlias `tpcds`.`date_dim`
               :                 :  :     +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
               :                 :  +- SubqueryAlias `tpcds`.`customer_address`
               :                 :     +- Relation[ca_address_sk#60,ca_address_id#61,ca_street_number#62,ca_street_name#63,ca_street_type#64,ca_suite_number#65,ca_city#66,ca_county#67,ca_state#68,ca_zip#69,ca_country#70,ca_gmt_offset#71,ca_location_type#72] parquet
               :                 +- SubqueryAlias `tpcds`.`item`
               :                    +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
               +- Project [i_manufact_id#86, total_sales#5]
                  +- SubqueryAlias `ws`
                     +- Aggregate [i_manufact_id#86], [i_manufact_id#86, sum(ws_ext_sales_price#154) AS total_sales#5]
                        +- Filter (((i_manufact_id#86 IN (list#6 []) && (ws_item_sk#134 = i_item_sk#73)) && ((ws_sold_date_sk#131 = d_date_sk#32) && (d_year#38 = 1998))) && (((d_moy#40 = 5) && (ws_bill_addr_sk#138 = ca_address_sk#60)) && (ca_gmt_offset#71 = cast(-5 as double))))
                           :  +- Project [i_manufact_id#86]
                           :     +- Filter i_category#85 IN (Electronics)
                           :        +- SubqueryAlias `tpcds`.`item`
                           :           +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
                           +- Join Inner
                              :- Join Inner
                              :  :- Join Inner
                              :  :  :- SubqueryAlias `tpcds`.`web_sales`
                              :  :  :  +- Relation[ws_sold_date_sk#131,ws_sold_time_sk#132,ws_ship_date_sk#133,ws_item_sk#134,ws_bill_customer_sk#135,ws_bill_cdemo_sk#136,ws_bill_hdemo_sk#137,ws_bill_addr_sk#138,ws_ship_customer_sk#139,ws_ship_cdemo_sk#140,ws_ship_hdemo_sk#141,ws_ship_addr_sk#142,ws_web_page_sk#143,ws_web_site_sk#144,ws_ship_mode_sk#145,ws_warehouse_sk#146,ws_promo_sk#147,ws_order_number#148,ws_quantity#149,ws_wholesale_cost#150,ws_list_price#151,ws_sales_price#152,ws_ext_discount_amt#153,ws_ext_sales_price#154,... 10 more fields] parquet
                              :  :  +- SubqueryAlias `tpcds`.`date_dim`
                              :  :     +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
                              :  +- SubqueryAlias `tpcds`.`customer_address`
                              :     +- Relation[ca_address_sk#60,ca_address_id#61,ca_street_number#62,ca_street_name#63,ca_street_type#64,ca_suite_number#65,ca_city#66,ca_county#67,ca_state#68,ca_zip#69,ca_country#70,ca_gmt_offset#71,ca_location_type#72] parquet
                              +- SubqueryAlias `tpcds`.`item`
                                 +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [total_sales#0 ASC NULLS FIRST], true
      +- Aggregate [i_manufact_id#86], [i_manufact_id#86, sum(total_sales#1) AS total_sales#0]
         +- Union
            :- Aggregate [i_manufact_id#86], [i_manufact_id#86, sum(ss_ext_sales_price#24) AS total_sales#1]
            :  +- Project [ss_ext_sales_price#24, i_manufact_id#86]
            :     +- Join Inner, (ss_item_sk#11 = i_item_sk#73)
            :        :- Project [ss_item_sk#11, ss_ext_sales_price#24]
            :        :  +- Join Inner, (ss_addr_sk#15 = ca_address_sk#60)
            :        :     :- Project [ss_item_sk#11, ss_addr_sk#15, ss_ext_sales_price#24]
            :        :     :  +- Join Inner, (ss_sold_date_sk#9 = d_date_sk#32)
            :        :     :     :- Project [ss_sold_date_sk#9, ss_item_sk#11, ss_addr_sk#15, ss_ext_sales_price#24]
            :        :     :     :  +- Filter ((isnotnull(ss_sold_date_sk#9) && isnotnull(ss_addr_sk#15)) && isnotnull(ss_item_sk#11))
            :        :     :     :     +- Relation[ss_sold_date_sk#9,ss_sold_time_sk#10,ss_item_sk#11,ss_customer_sk#12,ss_cdemo_sk#13,ss_hdemo_sk#14,ss_addr_sk#15,ss_store_sk#16,ss_promo_sk#17,ss_ticket_number#18,ss_quantity#19,ss_wholesale_cost#20,ss_list_price#21,ss_sales_price#22,ss_ext_discount_amt#23,ss_ext_sales_price#24,ss_ext_wholesale_cost#25,ss_ext_list_price#26,ss_ext_tax#27,ss_coupon_amt#28,ss_net_paid#29,ss_net_paid_inc_tax#30,ss_net_profit#31] parquet
            :        :     :     +- Project [d_date_sk#32]
            :        :     :        +- Filter ((((isnotnull(d_year#38) && isnotnull(d_moy#40)) && (d_year#38 = 1998)) && (d_moy#40 = 5)) && isnotnull(d_date_sk#32))
            :        :     :           +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
            :        :     +- Project [ca_address_sk#60]
            :        :        +- Filter ((isnotnull(ca_gmt_offset#71) && (ca_gmt_offset#71 = -5.0)) && isnotnull(ca_address_sk#60))
            :        :           +- Relation[ca_address_sk#60,ca_address_id#61,ca_street_number#62,ca_street_name#63,ca_street_type#64,ca_suite_number#65,ca_city#66,ca_county#67,ca_state#68,ca_zip#69,ca_country#70,ca_gmt_offset#71,ca_location_type#72] parquet
            :        +- Join LeftSemi, (i_manufact_id#86 = i_manufact_id#86#168)
            :           :- Project [i_item_sk#73, i_manufact_id#86]
            :           :  +- Filter isnotnull(i_item_sk#73)
            :           :     +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
            :           +- Project [i_manufact_id#86 AS i_manufact_id#86#168]
            :              +- Filter (isnotnull(i_category#85) && (i_category#85 = Electronics))
            :                 +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
            :- Aggregate [i_manufact_id#86], [i_manufact_id#86, sum(cs_ext_sales_price#119) AS total_sales#3]
            :  +- Project [cs_ext_sales_price#119, i_manufact_id#86]
            :     +- Join Inner, (cs_item_sk#111 = i_item_sk#73)
            :        :- Project [cs_item_sk#111, cs_ext_sales_price#119]
            :        :  +- Join Inner, (cs_bill_addr_sk#102 = ca_address_sk#60)
            :        :     :- Project [cs_bill_addr_sk#102, cs_item_sk#111, cs_ext_sales_price#119]
            :        :     :  +- Join Inner, (cs_sold_date_sk#96 = d_date_sk#32)
            :        :     :     :- Project [cs_sold_date_sk#96, cs_bill_addr_sk#102, cs_item_sk#111, cs_ext_sales_price#119]
            :        :     :     :  +- Filter ((isnotnull(cs_sold_date_sk#96) && isnotnull(cs_bill_addr_sk#102)) && isnotnull(cs_item_sk#111))
            :        :     :     :     +- Relation[cs_sold_date_sk#96,cs_sold_time_sk#97,cs_ship_date_sk#98,cs_bill_customer_sk#99,cs_bill_cdemo_sk#100,cs_bill_hdemo_sk#101,cs_bill_addr_sk#102,cs_ship_customer_sk#103,cs_ship_cdemo_sk#104,cs_ship_hdemo_sk#105,cs_ship_addr_sk#106,cs_call_center_sk#107,cs_catalog_page_sk#108,cs_ship_mode_sk#109,cs_warehouse_sk#110,cs_item_sk#111,cs_promo_sk#112,cs_order_number#113,cs_quantity#114,cs_wholesale_cost#115,cs_list_price#116,cs_sales_price#117,cs_ext_discount_amt#118,cs_ext_sales_price#119,... 10 more fields] parquet
            :        :     :     +- Project [d_date_sk#32]
            :        :     :        +- Filter ((((isnotnull(d_year#38) && isnotnull(d_moy#40)) && (d_year#38 = 1998)) && (d_moy#40 = 5)) && isnotnull(d_date_sk#32))
            :        :     :           +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
            :        :     +- Project [ca_address_sk#60]
            :        :        +- Filter ((isnotnull(ca_gmt_offset#71) && (ca_gmt_offset#71 = -5.0)) && isnotnull(ca_address_sk#60))
            :        :           +- Relation[ca_address_sk#60,ca_address_id#61,ca_street_number#62,ca_street_name#63,ca_street_type#64,ca_suite_number#65,ca_city#66,ca_county#67,ca_state#68,ca_zip#69,ca_country#70,ca_gmt_offset#71,ca_location_type#72] parquet
            :        +- Join LeftSemi, (i_manufact_id#86 = i_manufact_id#86#169)
            :           :- Project [i_item_sk#73, i_manufact_id#86]
            :           :  +- Filter isnotnull(i_item_sk#73)
            :           :     +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
            :           +- Project [i_manufact_id#86 AS i_manufact_id#86#169]
            :              +- Filter (isnotnull(i_category#85) && (i_category#85 = Electronics))
            :                 +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
            +- Aggregate [i_manufact_id#86], [i_manufact_id#86, sum(ws_ext_sales_price#154) AS total_sales#5]
               +- Project [ws_ext_sales_price#154, i_manufact_id#86]
                  +- Join Inner, (ws_item_sk#134 = i_item_sk#73)
                     :- Project [ws_item_sk#134, ws_ext_sales_price#154]
                     :  +- Join Inner, (ws_bill_addr_sk#138 = ca_address_sk#60)
                     :     :- Project [ws_item_sk#134, ws_bill_addr_sk#138, ws_ext_sales_price#154]
                     :     :  +- Join Inner, (ws_sold_date_sk#131 = d_date_sk#32)
                     :     :     :- Project [ws_sold_date_sk#131, ws_item_sk#134, ws_bill_addr_sk#138, ws_ext_sales_price#154]
                     :     :     :  +- Filter ((isnotnull(ws_sold_date_sk#131) && isnotnull(ws_bill_addr_sk#138)) && isnotnull(ws_item_sk#134))
                     :     :     :     +- Relation[ws_sold_date_sk#131,ws_sold_time_sk#132,ws_ship_date_sk#133,ws_item_sk#134,ws_bill_customer_sk#135,ws_bill_cdemo_sk#136,ws_bill_hdemo_sk#137,ws_bill_addr_sk#138,ws_ship_customer_sk#139,ws_ship_cdemo_sk#140,ws_ship_hdemo_sk#141,ws_ship_addr_sk#142,ws_web_page_sk#143,ws_web_site_sk#144,ws_ship_mode_sk#145,ws_warehouse_sk#146,ws_promo_sk#147,ws_order_number#148,ws_quantity#149,ws_wholesale_cost#150,ws_list_price#151,ws_sales_price#152,ws_ext_discount_amt#153,ws_ext_sales_price#154,... 10 more fields] parquet
                     :     :     +- Project [d_date_sk#32]
                     :     :        +- Filter ((((isnotnull(d_year#38) && isnotnull(d_moy#40)) && (d_year#38 = 1998)) && (d_moy#40 = 5)) && isnotnull(d_date_sk#32))
                     :     :           +- Relation[d_date_sk#32,d_date_id#33,d_date#34,d_month_seq#35,d_week_seq#36,d_quarter_seq#37,d_year#38,d_dow#39,d_moy#40,d_dom#41,d_qoy#42,d_fy_year#43,d_fy_quarter_seq#44,d_fy_week_seq#45,d_day_name#46,d_quarter_name#47,d_holiday#48,d_weekend#49,d_following_holiday#50,d_first_dom#51,d_last_dom#52,d_same_day_ly#53,d_same_day_lq#54,d_current_day#55,... 4 more fields] parquet
                     :     +- Project [ca_address_sk#60]
                     :        +- Filter ((isnotnull(ca_gmt_offset#71) && (ca_gmt_offset#71 = -5.0)) && isnotnull(ca_address_sk#60))
                     :           +- Relation[ca_address_sk#60,ca_address_id#61,ca_street_number#62,ca_street_name#63,ca_street_type#64,ca_suite_number#65,ca_city#66,ca_county#67,ca_state#68,ca_zip#69,ca_country#70,ca_gmt_offset#71,ca_location_type#72] parquet
                     +- Join LeftSemi, (i_manufact_id#86 = i_manufact_id#86#170)
                        :- Project [i_item_sk#73, i_manufact_id#86]
                        :  +- Filter isnotnull(i_item_sk#73)
                        :     +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet
                        +- Project [i_manufact_id#86 AS i_manufact_id#86#170]
                           +- Filter (isnotnull(i_category#85) && (i_category#85 = Electronics))
                              +- Relation[i_item_sk#73,i_item_id#74,i_rec_start_date#75,i_rec_end_date#76,i_item_desc#77,i_current_price#78,i_wholesale_cost#79,i_brand_id#80,i_brand#81,i_class_id#82,i_class#83,i_category_id#84,i_category#85,i_manufact_id#86,i_manufact#87,i_size#88,i_formulation#89,i_color#90,i_units#91,i_container#92,i_manager_id#93,i_product_name#94] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[total_sales#0 ASC NULLS FIRST], output=[i_manufact_id#86,total_sales#0])
+- *(20) HashAggregate(keys=[i_manufact_id#86], functions=[sum(total_sales#1)], output=[i_manufact_id#86, total_sales#0])
   +- Exchange hashpartitioning(i_manufact_id#86, 200)
      +- *(19) HashAggregate(keys=[i_manufact_id#86], functions=[partial_sum(total_sales#1)], output=[i_manufact_id#86, sum#172])
         +- Union
            :- *(6) HashAggregate(keys=[i_manufact_id#86], functions=[sum(ss_ext_sales_price#24)], output=[i_manufact_id#86, total_sales#1])
            :  +- Exchange hashpartitioning(i_manufact_id#86, 200)
            :     +- *(5) HashAggregate(keys=[i_manufact_id#86], functions=[partial_sum(ss_ext_sales_price#24)], output=[i_manufact_id#86, sum#174])
            :        +- *(5) Project [ss_ext_sales_price#24, i_manufact_id#86]
            :           +- *(5) BroadcastHashJoin [ss_item_sk#11], [i_item_sk#73], Inner, BuildRight
            :              :- *(5) Project [ss_item_sk#11, ss_ext_sales_price#24]
            :              :  +- *(5) BroadcastHashJoin [ss_addr_sk#15], [ca_address_sk#60], Inner, BuildRight
            :              :     :- *(5) Project [ss_item_sk#11, ss_addr_sk#15, ss_ext_sales_price#24]
            :              :     :  +- *(5) BroadcastHashJoin [ss_sold_date_sk#9], [d_date_sk#32], Inner, BuildRight
            :              :     :     :- *(5) Project [ss_sold_date_sk#9, ss_item_sk#11, ss_addr_sk#15, ss_ext_sales_price#24]
            :              :     :     :  +- *(5) Filter ((isnotnull(ss_sold_date_sk#9) && isnotnull(ss_addr_sk#15)) && isnotnull(ss_item_sk#11))
            :              :     :     :     +- *(5) FileScan parquet tpcds.store_sales[ss_sold_date_sk#9,ss_item_sk#11,ss_addr_sk#15,ss_ext_sales_price#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_addr_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_addr_sk:int,ss_ext_sales_price:double>
            :              :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :              :     :        +- *(1) Project [d_date_sk#32]
            :              :     :           +- *(1) Filter ((((isnotnull(d_year#38) && isnotnull(d_moy#40)) && (d_year#38 = 1998)) && (d_moy#40 = 5)) && isnotnull(d_date_sk#32))
            :              :     :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#32,d_year#38,d_moy#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,5), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
            :              :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :              :        +- *(2) Project [ca_address_sk#60]
            :              :           +- *(2) Filter ((isnotnull(ca_gmt_offset#71) && (ca_gmt_offset#71 = -5.0)) && isnotnull(ca_address_sk#60))
            :              :              +- *(2) FileScan parquet tpcds.customer_address[ca_address_sk#60,ca_gmt_offset#71] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_gmt_offset), EqualTo(ca_gmt_offset,-5.0), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_gmt_offset:double>
            :              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :                 +- *(4) BroadcastHashJoin [i_manufact_id#86], [i_manufact_id#86#168], LeftSemi, BuildRight
            :                    :- *(4) Project [i_item_sk#73, i_manufact_id#86]
            :                    :  +- *(4) Filter isnotnull(i_item_sk#73)
            :                    :     +- *(4) FileScan parquet tpcds.item[i_item_sk#73,i_manufact_id#86] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_manufact_id:int>
            :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :                       +- *(3) Project [i_manufact_id#86 AS i_manufact_id#86#168]
            :                          +- *(3) Filter (isnotnull(i_category#85) && (i_category#85 = Electronics))
            :                             +- *(3) FileScan parquet tpcds.item[i_category#85,i_manufact_id#86] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), EqualTo(i_category,Electronics)], ReadSchema: struct<i_category:string,i_manufact_id:int>
            :- *(12) HashAggregate(keys=[i_manufact_id#86], functions=[sum(cs_ext_sales_price#119)], output=[i_manufact_id#86, total_sales#3])
            :  +- Exchange hashpartitioning(i_manufact_id#86, 200)
            :     +- *(11) HashAggregate(keys=[i_manufact_id#86], functions=[partial_sum(cs_ext_sales_price#119)], output=[i_manufact_id#86, sum#176])
            :        +- *(11) Project [cs_ext_sales_price#119, i_manufact_id#86]
            :           +- *(11) BroadcastHashJoin [cs_item_sk#111], [i_item_sk#73], Inner, BuildRight
            :              :- *(11) Project [cs_item_sk#111, cs_ext_sales_price#119]
            :              :  +- *(11) BroadcastHashJoin [cs_bill_addr_sk#102], [ca_address_sk#60], Inner, BuildRight
            :              :     :- *(11) Project [cs_bill_addr_sk#102, cs_item_sk#111, cs_ext_sales_price#119]
            :              :     :  +- *(11) BroadcastHashJoin [cs_sold_date_sk#96], [d_date_sk#32], Inner, BuildRight
            :              :     :     :- *(11) Project [cs_sold_date_sk#96, cs_bill_addr_sk#102, cs_item_sk#111, cs_ext_sales_price#119]
            :              :     :     :  +- *(11) Filter ((isnotnull(cs_sold_date_sk#96) && isnotnull(cs_bill_addr_sk#102)) && isnotnull(cs_item_sk#111))
            :              :     :     :     +- *(11) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#96,cs_bill_addr_sk#102,cs_item_sk#111,cs_ext_sales_price#119] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_bill_addr_sk), IsNotNull(cs_item_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_addr_sk:int,cs_item_sk:int,cs_ext_sales_price:double>
            :              :     :     +- ReusedExchange [d_date_sk#32], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :              :     +- ReusedExchange [ca_address_sk#60], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :              +- ReusedExchange [i_item_sk#73, i_manufact_id#86], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            +- *(18) HashAggregate(keys=[i_manufact_id#86], functions=[sum(ws_ext_sales_price#154)], output=[i_manufact_id#86, total_sales#5])
               +- Exchange hashpartitioning(i_manufact_id#86, 200)
                  +- *(17) HashAggregate(keys=[i_manufact_id#86], functions=[partial_sum(ws_ext_sales_price#154)], output=[i_manufact_id#86, sum#178])
                     +- *(17) Project [ws_ext_sales_price#154, i_manufact_id#86]
                        +- *(17) BroadcastHashJoin [ws_item_sk#134], [i_item_sk#73], Inner, BuildRight
                           :- *(17) Project [ws_item_sk#134, ws_ext_sales_price#154]
                           :  +- *(17) BroadcastHashJoin [ws_bill_addr_sk#138], [ca_address_sk#60], Inner, BuildRight
                           :     :- *(17) Project [ws_item_sk#134, ws_bill_addr_sk#138, ws_ext_sales_price#154]
                           :     :  +- *(17) BroadcastHashJoin [ws_sold_date_sk#131], [d_date_sk#32], Inner, BuildRight
                           :     :     :- *(17) Project [ws_sold_date_sk#131, ws_item_sk#134, ws_bill_addr_sk#138, ws_ext_sales_price#154]
                           :     :     :  +- *(17) Filter ((isnotnull(ws_sold_date_sk#131) && isnotnull(ws_bill_addr_sk#138)) && isnotnull(ws_item_sk#134))
                           :     :     :     +- *(17) FileScan parquet tpcds.web_sales[ws_sold_date_sk#131,ws_item_sk#134,ws_bill_addr_sk#138,ws_ext_sales_price#154] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_bill_addr_sk), IsNotNull(ws_item_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_bill_addr_sk:int,ws_ext_sales_price:double>
                           :     :     +- ReusedExchange [d_date_sk#32], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                           :     +- ReusedExchange [ca_address_sk#60], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                           +- ReusedExchange [i_item_sk#73, i_manufact_id#86], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.265 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 33 in stream 0 using template query33.tpl
------------------------------------------------------^^^

