== Parsed Logical Plan ==
CTE [ss_items, cs_items, ws_items]
:  :- 'SubqueryAlias `ss_items`
:  :  +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#4, 'sum('ss_ext_sales_price) AS ss_item_rev#5]
:  :     +- 'Filter ((('ss_item_sk = 'i_item_sk) && 'd_date IN (list#7 [])) && ('ss_sold_date_sk = 'd_date_sk))
:  :        :  +- 'Project ['d_date]
:  :        :     +- 'Filter ('d_week_seq = scalar-subquery#6 [])
:  :        :        :  +- 'Project ['d_week_seq]
:  :        :        :     +- 'Filter ('d_date = 2000-01-03)
:  :        :        :        +- 'UnresolvedRelation `date_dim`
:  :        :        +- 'UnresolvedRelation `date_dim`
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation `store_sales`
:  :           :  +- 'UnresolvedRelation `item`
:  :           +- 'UnresolvedRelation `date_dim`
:  :- 'SubqueryAlias `cs_items`
:  :  +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#8, 'sum('cs_ext_sales_price) AS cs_item_rev#9]
:  :     +- 'Filter ((('cs_item_sk = 'i_item_sk) && 'd_date IN (list#11 [])) && ('cs_sold_date_sk = 'd_date_sk))
:  :        :  +- 'Project ['d_date]
:  :        :     +- 'Filter ('d_week_seq = scalar-subquery#10 [])
:  :        :        :  +- 'Project ['d_week_seq]
:  :        :        :     +- 'Filter ('d_date = 2000-01-03)
:  :        :        :        +- 'UnresolvedRelation `date_dim`
:  :        :        +- 'UnresolvedRelation `date_dim`
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation `catalog_sales`
:  :           :  +- 'UnresolvedRelation `item`
:  :           +- 'UnresolvedRelation `date_dim`
:  +- 'SubqueryAlias `ws_items`
:     +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#12, 'sum('ws_ext_sales_price) AS ws_item_rev#13]
:        +- 'Filter ((('ws_item_sk = 'i_item_sk) && 'd_date IN (list#15 [])) && ('ws_sold_date_sk = 'd_date_sk))
:           :  +- 'Project ['d_date]
:           :     +- 'Filter ('d_week_seq = scalar-subquery#14 [])
:           :        :  +- 'Project ['d_week_seq]
:           :        :     +- 'Filter ('d_date = 2000-01-03)
:           :        :        +- 'UnresolvedRelation `date_dim`
:           :        +- 'UnresolvedRelation `date_dim`
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation `web_sales`
:              :  +- 'UnresolvedRelation `item`
:              +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['item_id ASC NULLS FIRST, 'ss_item_rev ASC NULLS FIRST], true
         +- 'Project ['ss_items.item_id, 'ss_item_rev, (('ss_item_rev / ((('ss_item_rev + 'cs_item_rev) + 'ws_item_rev) / 3)) * 100) AS ss_dev#0, 'cs_item_rev, (('cs_item_rev / ((('ss_item_rev + 'cs_item_rev) + 'ws_item_rev) / 3)) * 100) AS cs_dev#1, 'ws_item_rev, (('ws_item_rev / ((('ss_item_rev + 'cs_item_rev) + 'ws_item_rev) / 3)) * 100) AS ws_dev#2, ((('ss_item_rev + 'cs_item_rev) + 'ws_item_rev) / 3) AS average#3]
            +- 'Filter (((('ss_items.item_id = 'cs_items.item_id) && ('ss_items.item_id = 'ws_items.item_id)) && ((('ss_item_rev >= (0.9 * 'cs_item_rev)) && ('ss_item_rev <= (1.1 * 'cs_item_rev))) && (('ss_item_rev >= (0.9 * 'ws_item_rev)) && ('ss_item_rev <= (1.1 * 'ws_item_rev))))) && (((('cs_item_rev >= (0.9 * 'ss_item_rev)) && ('cs_item_rev <= (1.1 * 'ss_item_rev))) && (('cs_item_rev >= (0.9 * 'ws_item_rev)) && ('cs_item_rev <= (1.1 * 'ws_item_rev)))) && ((('ws_item_rev >= (0.9 * 'ss_item_rev)) && ('ws_item_rev <= (1.1 * 'ss_item_rev))) && (('ws_item_rev >= (0.9 * 'cs_item_rev)) && ('ws_item_rev <= (1.1 * 'cs_item_rev))))))
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'UnresolvedRelation `ss_items`
                  :  +- 'UnresolvedRelation `cs_items`
                  +- 'UnresolvedRelation `ws_items`

== Analyzed Logical Plan ==
item_id: string, ss_item_rev: double, ss_dev: double, cs_item_rev: double, cs_dev: double, ws_item_rev: double, ws_dev: double, average: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [item_id#4 ASC NULLS FIRST, ss_item_rev#5 ASC NULLS FIRST], true
      +- Project [item_id#4, ss_item_rev#5, ((ss_item_rev#5 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / cast(3 as double))) * cast(100 as double)) AS ss_dev#0, cs_item_rev#9, ((cs_item_rev#9 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / cast(3 as double))) * cast(100 as double)) AS cs_dev#1, ws_item_rev#13, ((ws_item_rev#13 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / cast(3 as double))) * cast(100 as double)) AS ws_dev#2, (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / cast(3 as double)) AS average#3]
         +- Filter ((((item_id#4 = item_id#8) && (item_id#4 = item_id#12)) && (((ss_item_rev#5 >= (cast(0.9 as double) * cs_item_rev#9)) && (ss_item_rev#5 <= (cast(1.1 as double) * cs_item_rev#9))) && ((ss_item_rev#5 >= (cast(0.9 as double) * ws_item_rev#13)) && (ss_item_rev#5 <= (cast(1.1 as double) * ws_item_rev#13))))) && ((((cs_item_rev#9 >= (cast(0.9 as double) * ss_item_rev#5)) && (cs_item_rev#9 <= (cast(1.1 as double) * ss_item_rev#5))) && ((cs_item_rev#9 >= (cast(0.9 as double) * ws_item_rev#13)) && (cs_item_rev#9 <= (cast(1.1 as double) * ws_item_rev#13)))) && (((ws_item_rev#13 >= (cast(0.9 as double) * ss_item_rev#5)) && (ws_item_rev#13 <= (cast(1.1 as double) * ss_item_rev#5))) && ((ws_item_rev#13 >= (cast(0.9 as double) * cs_item_rev#9)) && (ws_item_rev#13 <= (cast(1.1 as double) * cs_item_rev#9))))))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `ss_items`
               :  :  +- Aggregate [i_item_id#42], [i_item_id#42 AS item_id#4, sum(ss_ext_sales_price#33) AS ss_item_rev#5]
               :  :     +- Filter (((ss_item_sk#20 = i_item_sk#41) && d_date#65 IN (list#7 [])) && (ss_sold_date_sk#18 = d_date_sk#63))
               :  :        :  +- Project [d_date#65]
               :  :        :     +- Filter (d_week_seq#67 = scalar-subquery#6 [])
               :  :        :        :  +- Project [d_week_seq#67]
               :  :        :        :     +- Filter (d_date#65 = 2000-01-03)
               :  :        :        :        +- SubqueryAlias `tpcds`.`date_dim`
               :  :        :        :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :  :        :        +- SubqueryAlias `tpcds`.`date_dim`
               :  :        :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :  :        +- Join Inner
               :  :           :- Join Inner
               :  :           :  :- SubqueryAlias `tpcds`.`store_sales`
               :  :           :  :  +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
               :  :           :  +- SubqueryAlias `tpcds`.`item`
               :  :           :     +- Relation[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
               :  :           +- SubqueryAlias `tpcds`.`date_dim`
               :  :              +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :  +- SubqueryAlias `cs_items`
               :     +- Aggregate [i_item_id#42], [i_item_id#42 AS item_id#8, sum(cs_ext_sales_price#115) AS cs_item_rev#9]
               :        +- Filter (((cs_item_sk#107 = i_item_sk#41) && d_date#65 IN (list#11 [])) && (cs_sold_date_sk#92 = d_date_sk#63))
               :           :  +- Project [d_date#65]
               :           :     +- Filter (d_week_seq#67 = scalar-subquery#10 [])
               :           :        :  +- Project [d_week_seq#67]
               :           :        :     +- Filter (d_date#65 = 2000-01-03)
               :           :        :        +- SubqueryAlias `tpcds`.`date_dim`
               :           :        :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :           :        +- SubqueryAlias `tpcds`.`date_dim`
               :           :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               :           +- Join Inner
               :              :- Join Inner
               :              :  :- SubqueryAlias `tpcds`.`catalog_sales`
               :              :  :  +- Relation[cs_sold_date_sk#92,cs_sold_time_sk#93,cs_ship_date_sk#94,cs_bill_customer_sk#95,cs_bill_cdemo_sk#96,cs_bill_hdemo_sk#97,cs_bill_addr_sk#98,cs_ship_customer_sk#99,cs_ship_cdemo_sk#100,cs_ship_hdemo_sk#101,cs_ship_addr_sk#102,cs_call_center_sk#103,cs_catalog_page_sk#104,cs_ship_mode_sk#105,cs_warehouse_sk#106,cs_item_sk#107,cs_promo_sk#108,cs_order_number#109,cs_quantity#110,cs_wholesale_cost#111,cs_list_price#112,cs_sales_price#113,cs_ext_discount_amt#114,cs_ext_sales_price#115,... 10 more fields] parquet
               :              :  +- SubqueryAlias `tpcds`.`item`
               :              :     +- Relation[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
               :              +- SubqueryAlias `tpcds`.`date_dim`
               :                 +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
               +- SubqueryAlias `ws_items`
                  +- Aggregate [i_item_id#42], [i_item_id#42 AS item_id#12, sum(ws_ext_sales_price#150) AS ws_item_rev#13]
                     +- Filter (((ws_item_sk#130 = i_item_sk#41) && d_date#65 IN (list#15 [])) && (ws_sold_date_sk#127 = d_date_sk#63))
                        :  +- Project [d_date#65]
                        :     +- Filter (d_week_seq#67 = scalar-subquery#14 [])
                        :        :  +- Project [d_week_seq#67]
                        :        :     +- Filter (d_date#65 = 2000-01-03)
                        :        :        +- SubqueryAlias `tpcds`.`date_dim`
                        :        :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
                        :        +- SubqueryAlias `tpcds`.`date_dim`
                        :           +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
                        +- Join Inner
                           :- Join Inner
                           :  :- SubqueryAlias `tpcds`.`web_sales`
                           :  :  +- Relation[ws_sold_date_sk#127,ws_sold_time_sk#128,ws_ship_date_sk#129,ws_item_sk#130,ws_bill_customer_sk#131,ws_bill_cdemo_sk#132,ws_bill_hdemo_sk#133,ws_bill_addr_sk#134,ws_ship_customer_sk#135,ws_ship_cdemo_sk#136,ws_ship_hdemo_sk#137,ws_ship_addr_sk#138,ws_web_page_sk#139,ws_web_site_sk#140,ws_ship_mode_sk#141,ws_warehouse_sk#142,ws_promo_sk#143,ws_order_number#144,ws_quantity#145,ws_wholesale_cost#146,ws_list_price#147,ws_sales_price#148,ws_ext_discount_amt#149,ws_ext_sales_price#150,... 10 more fields] parquet
                           :  +- SubqueryAlias `tpcds`.`item`
                           :     +- Relation[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
                           +- SubqueryAlias `tpcds`.`date_dim`
                              +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [item_id#4 ASC NULLS FIRST, ss_item_rev#5 ASC NULLS FIRST], true
      +- Project [item_id#4, ss_item_rev#5, ((ss_item_rev#5 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS ss_dev#0, cs_item_rev#9, ((cs_item_rev#9 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS cs_dev#1, ws_item_rev#13, ((ws_item_rev#13 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS ws_dev#2, (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0) AS average#3]
         +- Join Inner, (((((((((item_id#4 = item_id#12) && (ss_item_rev#5 >= (0.9 * ws_item_rev#13))) && (ss_item_rev#5 <= (1.1 * ws_item_rev#13))) && (cs_item_rev#9 >= (0.9 * ws_item_rev#13))) && (cs_item_rev#9 <= (1.1 * ws_item_rev#13))) && (ws_item_rev#13 >= (0.9 * ss_item_rev#5))) && (ws_item_rev#13 <= (1.1 * ss_item_rev#5))) && (ws_item_rev#13 >= (0.9 * cs_item_rev#9))) && (ws_item_rev#13 <= (1.1 * cs_item_rev#9)))
            :- Project [item_id#4, ss_item_rev#5, cs_item_rev#9]
            :  +- Join Inner, (((((item_id#4 = item_id#8) && (ss_item_rev#5 >= (0.9 * cs_item_rev#9))) && (ss_item_rev#5 <= (1.1 * cs_item_rev#9))) && (cs_item_rev#9 >= (0.9 * ss_item_rev#5))) && (cs_item_rev#9 <= (1.1 * ss_item_rev#5)))
            :     :- Filter isnotnull(ss_item_rev#5)
            :     :  +- Aggregate [i_item_id#42], [i_item_id#42 AS item_id#4, sum(ss_ext_sales_price#33) AS ss_item_rev#5]
            :     :     +- Project [ss_ext_sales_price#33, i_item_id#42]
            :     :        +- Join Inner, (ss_sold_date_sk#18 = d_date_sk#63)
            :     :           :- Project [ss_sold_date_sk#18, ss_ext_sales_price#33, i_item_id#42]
            :     :           :  +- Join Inner, (ss_item_sk#20 = i_item_sk#41)
            :     :           :     :- Project [ss_sold_date_sk#18, ss_item_sk#20, ss_ext_sales_price#33]
            :     :           :     :  +- Filter (isnotnull(ss_item_sk#20) && isnotnull(ss_sold_date_sk#18))
            :     :           :     :     +- Relation[ss_sold_date_sk#18,ss_sold_time_sk#19,ss_item_sk#20,ss_customer_sk#21,ss_cdemo_sk#22,ss_hdemo_sk#23,ss_addr_sk#24,ss_store_sk#25,ss_promo_sk#26,ss_ticket_number#27,ss_quantity#28,ss_wholesale_cost#29,ss_list_price#30,ss_sales_price#31,ss_ext_discount_amt#32,ss_ext_sales_price#33,ss_ext_wholesale_cost#34,ss_ext_list_price#35,ss_ext_tax#36,ss_coupon_amt#37,ss_net_paid#38,ss_net_paid_inc_tax#39,ss_net_profit#40] parquet
            :     :           :     +- Project [i_item_sk#41, i_item_id#42]
            :     :           :        +- Filter (isnotnull(i_item_sk#41) && isnotnull(i_item_id#42))
            :     :           :           +- Relation[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
            :     :           +- Project [d_date_sk#63]
            :     :              +- Join LeftSemi, (d_date#65 = d_date#65#162)
            :     :                 :- Project [d_date_sk#63, d_date#65]
            :     :                 :  +- Filter isnotnull(d_date_sk#63)
            :     :                 :     +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            :     :                 +- Project [d_date#65 AS d_date#65#162]
            :     :                    +- Filter (isnotnull(d_week_seq#67) && (d_week_seq#67 = scalar-subquery#6 []))
            :     :                       :  +- Project [d_week_seq#67]
            :     :                       :     +- Filter (isnotnull(d_date#65) && (d_date#65 = 2000-01-03))
            :     :                       :        +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            :     :                       +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            :     +- Filter isnotnull(cs_item_rev#9)
            :        +- Aggregate [i_item_id#42], [i_item_id#42 AS item_id#8, sum(cs_ext_sales_price#115) AS cs_item_rev#9]
            :           +- Project [cs_ext_sales_price#115, i_item_id#42]
            :              +- Join Inner, (cs_sold_date_sk#92 = d_date_sk#63)
            :                 :- Project [cs_sold_date_sk#92, cs_ext_sales_price#115, i_item_id#42]
            :                 :  +- Join Inner, (cs_item_sk#107 = i_item_sk#41)
            :                 :     :- Project [cs_sold_date_sk#92, cs_item_sk#107, cs_ext_sales_price#115]
            :                 :     :  +- Filter (isnotnull(cs_item_sk#107) && isnotnull(cs_sold_date_sk#92))
            :                 :     :     +- Relation[cs_sold_date_sk#92,cs_sold_time_sk#93,cs_ship_date_sk#94,cs_bill_customer_sk#95,cs_bill_cdemo_sk#96,cs_bill_hdemo_sk#97,cs_bill_addr_sk#98,cs_ship_customer_sk#99,cs_ship_cdemo_sk#100,cs_ship_hdemo_sk#101,cs_ship_addr_sk#102,cs_call_center_sk#103,cs_catalog_page_sk#104,cs_ship_mode_sk#105,cs_warehouse_sk#106,cs_item_sk#107,cs_promo_sk#108,cs_order_number#109,cs_quantity#110,cs_wholesale_cost#111,cs_list_price#112,cs_sales_price#113,cs_ext_discount_amt#114,cs_ext_sales_price#115,... 10 more fields] parquet
            :                 :     +- Project [i_item_sk#41, i_item_id#42]
            :                 :        +- Filter (isnotnull(i_item_sk#41) && isnotnull(i_item_id#42))
            :                 :           +- Relation[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
            :                 +- Project [d_date_sk#63]
            :                    +- Join LeftSemi, (d_date#65 = d_date#65#163)
            :                       :- Project [d_date_sk#63, d_date#65]
            :                       :  +- Filter isnotnull(d_date_sk#63)
            :                       :     +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            :                       +- Project [d_date#65 AS d_date#65#163]
            :                          +- Filter (isnotnull(d_week_seq#67) && (d_week_seq#67 = scalar-subquery#10 []))
            :                             :  +- Project [d_week_seq#67]
            :                             :     +- Filter (isnotnull(d_date#65) && (d_date#65 = 2000-01-03))
            :                             :        +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            :                             +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
            +- Filter isnotnull(ws_item_rev#13)
               +- Aggregate [i_item_id#42], [i_item_id#42 AS item_id#12, sum(ws_ext_sales_price#150) AS ws_item_rev#13]
                  +- Project [ws_ext_sales_price#150, i_item_id#42]
                     +- Join Inner, (ws_sold_date_sk#127 = d_date_sk#63)
                        :- Project [ws_sold_date_sk#127, ws_ext_sales_price#150, i_item_id#42]
                        :  +- Join Inner, (ws_item_sk#130 = i_item_sk#41)
                        :     :- Project [ws_sold_date_sk#127, ws_item_sk#130, ws_ext_sales_price#150]
                        :     :  +- Filter (isnotnull(ws_item_sk#130) && isnotnull(ws_sold_date_sk#127))
                        :     :     +- Relation[ws_sold_date_sk#127,ws_sold_time_sk#128,ws_ship_date_sk#129,ws_item_sk#130,ws_bill_customer_sk#131,ws_bill_cdemo_sk#132,ws_bill_hdemo_sk#133,ws_bill_addr_sk#134,ws_ship_customer_sk#135,ws_ship_cdemo_sk#136,ws_ship_hdemo_sk#137,ws_ship_addr_sk#138,ws_web_page_sk#139,ws_web_site_sk#140,ws_ship_mode_sk#141,ws_warehouse_sk#142,ws_promo_sk#143,ws_order_number#144,ws_quantity#145,ws_wholesale_cost#146,ws_list_price#147,ws_sales_price#148,ws_ext_discount_amt#149,ws_ext_sales_price#150,... 10 more fields] parquet
                        :     +- Project [i_item_sk#41, i_item_id#42]
                        :        +- Filter (isnotnull(i_item_sk#41) && isnotnull(i_item_id#42))
                        :           +- Relation[i_item_sk#41,i_item_id#42,i_rec_start_date#43,i_rec_end_date#44,i_item_desc#45,i_current_price#46,i_wholesale_cost#47,i_brand_id#48,i_brand#49,i_class_id#50,i_class#51,i_category_id#52,i_category#53,i_manufact_id#54,i_manufact#55,i_size#56,i_formulation#57,i_color#58,i_units#59,i_container#60,i_manager_id#61,i_product_name#62] parquet
                        +- Project [d_date_sk#63]
                           +- Join LeftSemi, (d_date#65 = d_date#65#164)
                              :- Project [d_date_sk#63, d_date#65]
                              :  +- Filter isnotnull(d_date_sk#63)
                              :     +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
                              +- Project [d_date#65 AS d_date#65#164]
                                 +- Filter (isnotnull(d_week_seq#67) && (d_week_seq#67 = scalar-subquery#14 []))
                                    :  +- Project [d_week_seq#67]
                                    :     +- Filter (isnotnull(d_date#65) && (d_date#65 = 2000-01-03))
                                    :        +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet
                                    +- Relation[d_date_sk#63,d_date_id#64,d_date#65,d_month_seq#66,d_week_seq#67,d_quarter_seq#68,d_year#69,d_dow#70,d_moy#71,d_dom#72,d_qoy#73,d_fy_year#74,d_fy_quarter_seq#75,d_fy_week_seq#76,d_day_name#77,d_quarter_name#78,d_holiday#79,d_weekend#80,d_following_holiday#81,d_first_dom#82,d_last_dom#83,d_same_day_ly#84,d_same_day_lq#85,d_current_day#86,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[item_id#4 ASC NULLS FIRST,ss_item_rev#5 ASC NULLS FIRST], output=[item_id#4,ss_item_rev#5,ss_dev#0,cs_item_rev#9,cs_dev#1,ws_item_rev#13,ws_dev#2,average#3])
+- *(20) Project [item_id#4, ss_item_rev#5, ((ss_item_rev#5 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS ss_dev#0, cs_item_rev#9, ((cs_item_rev#9 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS cs_dev#1, ws_item_rev#13, ((ws_item_rev#13 / (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0)) * 100.0) AS ws_dev#2, (((ss_item_rev#5 + cs_item_rev#9) + ws_item_rev#13) / 3.0) AS average#3]
   +- *(20) SortMergeJoin [item_id#4], [item_id#12], Inner, ((((((((ss_item_rev#5 >= (0.9 * ws_item_rev#13)) && (ss_item_rev#5 <= (1.1 * ws_item_rev#13))) && (cs_item_rev#9 >= (0.9 * ws_item_rev#13))) && (cs_item_rev#9 <= (1.1 * ws_item_rev#13))) && (ws_item_rev#13 >= (0.9 * ss_item_rev#5))) && (ws_item_rev#13 <= (1.1 * ss_item_rev#5))) && (ws_item_rev#13 >= (0.9 * cs_item_rev#9))) && (ws_item_rev#13 <= (1.1 * cs_item_rev#9)))
      :- *(13) Project [item_id#4, ss_item_rev#5, cs_item_rev#9]
      :  +- *(13) SortMergeJoin [item_id#4], [item_id#8], Inner, ((((ss_item_rev#5 >= (0.9 * cs_item_rev#9)) && (ss_item_rev#5 <= (1.1 * cs_item_rev#9))) && (cs_item_rev#9 >= (0.9 * ss_item_rev#5))) && (cs_item_rev#9 <= (1.1 * ss_item_rev#5)))
      :     :- *(6) Sort [item_id#4 ASC NULLS FIRST], false, 0
      :     :  +- Exchange hashpartitioning(item_id#4, 200)
      :     :     +- *(5) Filter isnotnull(ss_item_rev#5)
      :     :        +- *(5) HashAggregate(keys=[i_item_id#42], functions=[sum(ss_ext_sales_price#33)], output=[item_id#4, ss_item_rev#5])
      :     :           +- Exchange hashpartitioning(i_item_id#42, 200)
      :     :              +- *(4) HashAggregate(keys=[i_item_id#42], functions=[partial_sum(ss_ext_sales_price#33)], output=[i_item_id#42, sum#166])
      :     :                 +- *(4) Project [ss_ext_sales_price#33, i_item_id#42]
      :     :                    +- *(4) BroadcastHashJoin [ss_sold_date_sk#18], [d_date_sk#63], Inner, BuildRight
      :     :                       :- *(4) Project [ss_sold_date_sk#18, ss_ext_sales_price#33, i_item_id#42]
      :     :                       :  +- *(4) BroadcastHashJoin [ss_item_sk#20], [i_item_sk#41], Inner, BuildRight
      :     :                       :     :- *(4) Project [ss_sold_date_sk#18, ss_item_sk#20, ss_ext_sales_price#33]
      :     :                       :     :  +- *(4) Filter (isnotnull(ss_item_sk#20) && isnotnull(ss_sold_date_sk#18))
      :     :                       :     :     +- *(4) FileScan parquet tpcds.store_sales[ss_sold_date_sk#18,ss_item_sk#20,ss_ext_sales_price#33] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
      :     :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                       :        +- *(1) Project [i_item_sk#41, i_item_id#42]
      :     :                       :           +- *(1) Filter (isnotnull(i_item_sk#41) && isnotnull(i_item_id#42))
      :     :                       :              +- *(1) FileScan parquet tpcds.item[i_item_sk#41,i_item_id#42] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_item_id)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
      :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                          +- *(3) Project [d_date_sk#63]
      :     :                             +- *(3) BroadcastHashJoin [d_date#65], [d_date#65#162], LeftSemi, BuildRight
      :     :                                :- *(3) Project [d_date_sk#63, d_date#65]
      :     :                                :  +- *(3) Filter isnotnull(d_date_sk#63)
      :     :                                :     +- *(3) FileScan parquet tpcds.date_dim[d_date_sk#63,d_date#65] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
      :     :                                +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
      :     :                                   +- *(2) Project [d_date#65 AS d_date#65#162]
      :     :                                      +- *(2) Filter (isnotnull(d_week_seq#67) && (d_week_seq#67 = Subquery subquery6))
      :     :                                         :  +- Subquery subquery6
      :     :                                         :     +- *(1) Project [d_week_seq#67]
      :     :                                         :        +- *(1) Filter (isnotnull(d_date#65) && (d_date#65 = 2000-01-03))
      :     :                                         :           +- *(1) FileScan parquet tpcds.date_dim[d_date#65,d_week_seq#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), EqualTo(d_date,2000-01-03)], ReadSchema: struct<d_date:string,d_week_seq:int>
      :     :                                         +- *(2) FileScan parquet tpcds.date_dim[d_date#65,d_week_seq#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_week_seq)], ReadSchema: struct<d_date:string,d_week_seq:int>
      :     :                                               +- Subquery subquery6
      :     :                                                  +- *(1) Project [d_week_seq#67]
      :     :                                                     +- *(1) Filter (isnotnull(d_date#65) && (d_date#65 = 2000-01-03))
      :     :                                                        +- *(1) FileScan parquet tpcds.date_dim[d_date#65,d_week_seq#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), EqualTo(d_date,2000-01-03)], ReadSchema: struct<d_date:string,d_week_seq:int>
      :     +- *(12) Sort [item_id#8 ASC NULLS FIRST], false, 0
      :        +- Exchange hashpartitioning(item_id#8, 200)
      :           +- *(11) Filter isnotnull(cs_item_rev#9)
      :              +- *(11) HashAggregate(keys=[i_item_id#42], functions=[sum(cs_ext_sales_price#115)], output=[item_id#8, cs_item_rev#9])
      :                 +- Exchange hashpartitioning(i_item_id#42, 200)
      :                    +- *(10) HashAggregate(keys=[i_item_id#42], functions=[partial_sum(cs_ext_sales_price#115)], output=[i_item_id#42, sum#168])
      :                       +- *(10) Project [cs_ext_sales_price#115, i_item_id#42]
      :                          +- *(10) BroadcastHashJoin [cs_sold_date_sk#92], [d_date_sk#63], Inner, BuildRight
      :                             :- *(10) Project [cs_sold_date_sk#92, cs_ext_sales_price#115, i_item_id#42]
      :                             :  +- *(10) BroadcastHashJoin [cs_item_sk#107], [i_item_sk#41], Inner, BuildRight
      :                             :     :- *(10) Project [cs_sold_date_sk#92, cs_item_sk#107, cs_ext_sales_price#115]
      :                             :     :  +- *(10) Filter (isnotnull(cs_item_sk#107) && isnotnull(cs_sold_date_sk#92))
      :                             :     :     +- *(10) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#92,cs_item_sk#107,cs_ext_sales_price#115] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_ext_sales_price:double>
      :                             :     +- ReusedExchange [i_item_sk#41, i_item_id#42], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                                +- *(9) Project [d_date_sk#63]
      :                                   +- *(9) BroadcastHashJoin [d_date#65], [d_date#65#163], LeftSemi, BuildRight
      :                                      :- *(9) Project [d_date_sk#63, d_date#65]
      :                                      :  +- *(9) Filter isnotnull(d_date_sk#63)
      :                                      :     +- *(9) FileScan parquet tpcds.date_dim[d_date_sk#63,d_date#65] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
      :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
      :                                         +- *(8) Project [d_date#65 AS d_date#65#163]
      :                                            +- *(8) Filter (isnotnull(d_week_seq#67) && (d_week_seq#67 = Subquery subquery10))
      :                                               :  +- Subquery subquery10
      :                                               :     +- *(1) Project [d_week_seq#67]
      :                                               :        +- *(1) Filter (isnotnull(d_date#65) && (d_date#65 = 2000-01-03))
      :                                               :           +- *(1) FileScan parquet tpcds.date_dim[d_date#65,d_week_seq#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), EqualTo(d_date,2000-01-03)], ReadSchema: struct<d_date:string,d_week_seq:int>
      :                                               +- *(8) FileScan parquet tpcds.date_dim[d_date#65,d_week_seq#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_week_seq)], ReadSchema: struct<d_date:string,d_week_seq:int>
      :                                                     +- Subquery subquery10
      :                                                        +- *(1) Project [d_week_seq#67]
      :                                                           +- *(1) Filter (isnotnull(d_date#65) && (d_date#65 = 2000-01-03))
      :                                                              +- *(1) FileScan parquet tpcds.date_dim[d_date#65,d_week_seq#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), EqualTo(d_date,2000-01-03)], ReadSchema: struct<d_date:string,d_week_seq:int>
      +- *(19) Sort [item_id#12 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(item_id#12, 200)
            +- *(18) Filter isnotnull(ws_item_rev#13)
               +- *(18) HashAggregate(keys=[i_item_id#42], functions=[sum(ws_ext_sales_price#150)], output=[item_id#12, ws_item_rev#13])
                  +- Exchange hashpartitioning(i_item_id#42, 200)
                     +- *(17) HashAggregate(keys=[i_item_id#42], functions=[partial_sum(ws_ext_sales_price#150)], output=[i_item_id#42, sum#170])
                        +- *(17) Project [ws_ext_sales_price#150, i_item_id#42]
                           +- *(17) BroadcastHashJoin [ws_sold_date_sk#127], [d_date_sk#63], Inner, BuildRight
                              :- *(17) Project [ws_sold_date_sk#127, ws_ext_sales_price#150, i_item_id#42]
                              :  +- *(17) BroadcastHashJoin [ws_item_sk#130], [i_item_sk#41], Inner, BuildRight
                              :     :- *(17) Project [ws_sold_date_sk#127, ws_item_sk#130, ws_ext_sales_price#150]
                              :     :  +- *(17) Filter (isnotnull(ws_item_sk#130) && isnotnull(ws_sold_date_sk#127))
                              :     :     +- *(17) FileScan parquet tpcds.web_sales[ws_sold_date_sk#127,ws_item_sk#130,ws_ext_sales_price#150] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_ext_sales_price:double>
                              :     +- ReusedExchange [i_item_sk#41, i_item_id#42], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                              +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 +- *(16) Project [d_date_sk#63]
                                    +- *(16) BroadcastHashJoin [d_date#65], [d_date#65#164], LeftSemi, BuildRight
                                       :- *(16) Project [d_date_sk#63, d_date#65]
                                       :  +- *(16) Filter isnotnull(d_date_sk#63)
                                       :     +- *(16) FileScan parquet tpcds.date_dim[d_date_sk#63,d_date#65] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
                                       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
                                          +- *(15) Project [d_date#65 AS d_date#65#164]
                                             +- *(15) Filter (isnotnull(d_week_seq#67) && (d_week_seq#67 = Subquery subquery14))
                                                :  +- Subquery subquery14
                                                :     +- *(1) Project [d_week_seq#67]
                                                :        +- *(1) Filter (isnotnull(d_date#65) && (d_date#65 = 2000-01-03))
                                                :           +- *(1) FileScan parquet tpcds.date_dim[d_date#65,d_week_seq#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), EqualTo(d_date,2000-01-03)], ReadSchema: struct<d_date:string,d_week_seq:int>
                                                +- *(15) FileScan parquet tpcds.date_dim[d_date#65,d_week_seq#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_week_seq)], ReadSchema: struct<d_date:string,d_week_seq:int>
                                                      +- Subquery subquery14
                                                         +- *(1) Project [d_week_seq#67]
                                                            +- *(1) Filter (isnotnull(d_date#65) && (d_date#65 = 2000-01-03))
                                                               +- *(1) FileScan parquet tpcds.date_dim[d_date#65,d_week_seq#67] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date), EqualTo(d_date,2000-01-03)], ReadSchema: struct<d_date:string,d_week_seq:int>
Time taken: 5.418 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 58 in stream 0 using template query58.tpl
------------------------------------------------------^^^

