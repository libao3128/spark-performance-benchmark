== Parsed Logical Plan ==
CTE [sr_items, cr_items, wr_items]
:  :- 'SubqueryAlias `sr_items`
:  :  +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#4, 'sum('sr_return_quantity) AS sr_item_qty#5]
:  :     +- 'Filter ((('sr_item_sk = 'i_item_sk) && 'd_date IN (list#7 [])) && ('sr_returned_date_sk = 'd_date_sk))
:  :        :  +- 'Project ['d_date]
:  :        :     +- 'Filter 'd_week_seq IN (list#6 [])
:  :        :        :  +- 'Project ['d_week_seq]
:  :        :        :     +- 'Filter 'd_date IN (2000-06-30,2000-09-27,2000-11-17)
:  :        :        :        +- 'UnresolvedRelation `date_dim`
:  :        :        +- 'UnresolvedRelation `date_dim`
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation `store_returns`
:  :           :  +- 'UnresolvedRelation `item`
:  :           +- 'UnresolvedRelation `date_dim`
:  :- 'SubqueryAlias `cr_items`
:  :  +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#8, 'sum('cr_return_quantity) AS cr_item_qty#9]
:  :     +- 'Filter ((('cr_item_sk = 'i_item_sk) && 'd_date IN (list#11 [])) && ('cr_returned_date_sk = 'd_date_sk))
:  :        :  +- 'Project ['d_date]
:  :        :     +- 'Filter 'd_week_seq IN (list#10 [])
:  :        :        :  +- 'Project ['d_week_seq]
:  :        :        :     +- 'Filter 'd_date IN (2000-06-30,2000-09-27,2000-11-17)
:  :        :        :        +- 'UnresolvedRelation `date_dim`
:  :        :        +- 'UnresolvedRelation `date_dim`
:  :        +- 'Join Inner
:  :           :- 'Join Inner
:  :           :  :- 'UnresolvedRelation `catalog_returns`
:  :           :  +- 'UnresolvedRelation `item`
:  :           +- 'UnresolvedRelation `date_dim`
:  +- 'SubqueryAlias `wr_items`
:     +- 'Aggregate ['i_item_id], ['i_item_id AS item_id#12, 'sum('wr_return_quantity) AS wr_item_qty#13]
:        +- 'Filter ((('wr_item_sk = 'i_item_sk) && 'd_date IN (list#15 [])) && ('wr_returned_date_sk = 'd_date_sk))
:           :  +- 'Project ['d_date]
:           :     +- 'Filter 'd_week_seq IN (list#14 [])
:           :        :  +- 'Project ['d_week_seq]
:           :        :     +- 'Filter 'd_date IN (2000-06-30,2000-09-27,2000-11-17)
:           :        :        +- 'UnresolvedRelation `date_dim`
:           :        +- 'UnresolvedRelation `date_dim`
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation `web_returns`
:              :  +- 'UnresolvedRelation `item`
:              +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['sr_items.item_id ASC NULLS FIRST, 'sr_item_qty ASC NULLS FIRST], true
         +- 'Project ['sr_items.item_id, 'sr_item_qty, ((('sr_item_qty / (('sr_item_qty + 'cr_item_qty) + 'wr_item_qty)) / 3.0) * 100) AS sr_dev#0, 'cr_item_qty, ((('cr_item_qty / (('sr_item_qty + 'cr_item_qty) + 'wr_item_qty)) / 3.0) * 100) AS cr_dev#1, 'wr_item_qty, ((('wr_item_qty / (('sr_item_qty + 'cr_item_qty) + 'wr_item_qty)) / 3.0) * 100) AS wr_dev#2, ((('sr_item_qty + 'cr_item_qty) + 'wr_item_qty) / 3.0) AS average#3]
            +- 'Filter (('sr_items.item_id = 'cr_items.item_id) && ('sr_items.item_id = 'wr_items.item_id))
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'UnresolvedRelation `sr_items`
                  :  +- 'UnresolvedRelation `cr_items`
                  +- 'UnresolvedRelation `wr_items`

== Analyzed Logical Plan ==
item_id: string, sr_item_qty: bigint, sr_dev: double, cr_item_qty: bigint, cr_dev: double, wr_item_qty: bigint, wr_dev: double, average: decimal(27,6)
GlobalLimit 100
+- LocalLimit 100
   +- Sort [item_id#4 ASC NULLS FIRST, sr_item_qty#5L ASC NULLS FIRST], true
      +- Project [item_id#4, sr_item_qty#5L, (((cast(sr_item_qty#5L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / cast(3.0 as double)) * cast(100 as double)) AS sr_dev#0, cr_item_qty#9L, (((cast(cr_item_qty#9L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / cast(3.0 as double)) * cast(100 as double)) AS cr_dev#1, wr_item_qty#13L, (((cast(wr_item_qty#13L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / cast(3.0 as double)) * cast(100 as double)) AS wr_dev#2, CheckOverflow((promote_precision(cast(cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as decimal(20,0)) as decimal(21,1))) / promote_precision(cast(3.0 as decimal(21,1)))), DecimalType(27,6)) AS average#3]
         +- Filter ((item_id#4 = item_id#8) && (item_id#4 = item_id#12))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `sr_items`
               :  :  +- Aggregate [i_item_id#39], [i_item_id#39 AS item_id#4, sum(cast(sr_return_quantity#28 as bigint)) AS sr_item_qty#5L]
               :  :     +- Filter (((sr_item_sk#20 = i_item_sk#38) && d_date#62 IN (list#7 [])) && (sr_returned_date_sk#18 = d_date_sk#60))
               :  :        :  +- Project [d_date#62]
               :  :        :     +- Filter d_week_seq#64 IN (list#6 [])
               :  :        :        :  +- Project [d_week_seq#64]
               :  :        :        :     +- Filter d_date#62 IN (2000-06-30,2000-09-27,2000-11-17)
               :  :        :        :        +- SubqueryAlias `tpcds`.`date_dim`
               :  :        :        :           +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
               :  :        :        +- SubqueryAlias `tpcds`.`date_dim`
               :  :        :           +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
               :  :        +- Join Inner
               :  :           :- Join Inner
               :  :           :  :- SubqueryAlias `tpcds`.`store_returns`
               :  :           :  :  +- Relation[sr_returned_date_sk#18,sr_return_time_sk#19,sr_item_sk#20,sr_customer_sk#21,sr_cdemo_sk#22,sr_hdemo_sk#23,sr_addr_sk#24,sr_store_sk#25,sr_reason_sk#26,sr_ticket_number#27,sr_return_quantity#28,sr_return_amt#29,sr_return_tax#30,sr_return_amt_inc_tax#31,sr_fee#32,sr_return_ship_cost#33,sr_refunded_cash#34,sr_reversed_charge#35,sr_store_credit#36,sr_net_loss#37] parquet
               :  :           :  +- SubqueryAlias `tpcds`.`item`
               :  :           :     +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
               :  :           +- SubqueryAlias `tpcds`.`date_dim`
               :  :              +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
               :  +- SubqueryAlias `cr_items`
               :     +- Aggregate [i_item_id#39], [i_item_id#39 AS item_id#8, sum(cast(cr_return_quantity#106 as bigint)) AS cr_item_qty#9L]
               :        +- Filter (((cr_item_sk#91 = i_item_sk#38) && d_date#62 IN (list#11 [])) && (cr_returned_date_sk#89 = d_date_sk#60))
               :           :  +- Project [d_date#62]
               :           :     +- Filter d_week_seq#64 IN (list#10 [])
               :           :        :  +- Project [d_week_seq#64]
               :           :        :     +- Filter d_date#62 IN (2000-06-30,2000-09-27,2000-11-17)
               :           :        :        +- SubqueryAlias `tpcds`.`date_dim`
               :           :        :           +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
               :           :        +- SubqueryAlias `tpcds`.`date_dim`
               :           :           +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
               :           +- Join Inner
               :              :- Join Inner
               :              :  :- SubqueryAlias `tpcds`.`catalog_returns`
               :              :  :  +- Relation[cr_returned_date_sk#89,cr_returned_time_sk#90,cr_item_sk#91,cr_refunded_customer_sk#92,cr_refunded_cdemo_sk#93,cr_refunded_hdemo_sk#94,cr_refunded_addr_sk#95,cr_returning_customer_sk#96,cr_returning_cdemo_sk#97,cr_returning_hdemo_sk#98,cr_returning_addr_sk#99,cr_call_center_sk#100,cr_catalog_page_sk#101,cr_ship_mode_sk#102,cr_warehouse_sk#103,cr_reason_sk#104,cr_order_number#105,cr_return_quantity#106,cr_return_amount#107,cr_return_tax#108,cr_return_amt_inc_tax#109,cr_fee#110,cr_return_ship_cost#111,cr_refunded_cash#112,... 3 more fields] parquet
               :              :  +- SubqueryAlias `tpcds`.`item`
               :              :     +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
               :              +- SubqueryAlias `tpcds`.`date_dim`
               :                 +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
               +- SubqueryAlias `wr_items`
                  +- Aggregate [i_item_id#39], [i_item_id#39 AS item_id#12, sum(cast(wr_return_quantity#131 as bigint)) AS wr_item_qty#13L]
                     +- Filter (((wr_item_sk#119 = i_item_sk#38) && d_date#62 IN (list#15 [])) && (wr_returned_date_sk#117 = d_date_sk#60))
                        :  +- Project [d_date#62]
                        :     +- Filter d_week_seq#64 IN (list#14 [])
                        :        :  +- Project [d_week_seq#64]
                        :        :     +- Filter d_date#62 IN (2000-06-30,2000-09-27,2000-11-17)
                        :        :        +- SubqueryAlias `tpcds`.`date_dim`
                        :        :           +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
                        :        +- SubqueryAlias `tpcds`.`date_dim`
                        :           +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
                        +- Join Inner
                           :- Join Inner
                           :  :- SubqueryAlias `tpcds`.`web_returns`
                           :  :  +- Relation[wr_returned_date_sk#117,wr_returned_time_sk#118,wr_item_sk#119,wr_refunded_customer_sk#120,wr_refunded_cdemo_sk#121,wr_refunded_hdemo_sk#122,wr_refunded_addr_sk#123,wr_returning_customer_sk#124,wr_returning_cdemo_sk#125,wr_returning_hdemo_sk#126,wr_returning_addr_sk#127,wr_web_page_sk#128,wr_reason_sk#129,wr_order_number#130,wr_return_quantity#131,wr_return_amt#132,wr_return_tax#133,wr_return_amt_inc_tax#134,wr_fee#135,wr_return_ship_cost#136,wr_refunded_cash#137,wr_reversed_charge#138,wr_account_credit#139,wr_net_loss#140] parquet
                           :  +- SubqueryAlias `tpcds`.`item`
                           :     +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
                           +- SubqueryAlias `tpcds`.`date_dim`
                              +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [item_id#4 ASC NULLS FIRST, sr_item_qty#5L ASC NULLS FIRST], true
      +- Project [item_id#4, sr_item_qty#5L, (((cast(sr_item_qty#5L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS sr_dev#0, cr_item_qty#9L, (((cast(cr_item_qty#9L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS cr_dev#1, wr_item_qty#13L, (((cast(wr_item_qty#13L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS wr_dev#2, CheckOverflow((promote_precision(cast(cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as decimal(20,0)) as decimal(21,1))) / 3.0), DecimalType(27,6)) AS average#3]
         +- Join Inner, (item_id#4 = item_id#12)
            :- Project [item_id#4, sr_item_qty#5L, cr_item_qty#9L]
            :  +- Join Inner, (item_id#4 = item_id#8)
            :     :- Aggregate [i_item_id#39], [i_item_id#39 AS item_id#4, sum(cast(sr_return_quantity#28 as bigint)) AS sr_item_qty#5L]
            :     :  +- Project [sr_return_quantity#28, i_item_id#39]
            :     :     +- Join Inner, (sr_returned_date_sk#18 = d_date_sk#60)
            :     :        :- Project [sr_returned_date_sk#18, sr_return_quantity#28, i_item_id#39]
            :     :        :  +- Join Inner, (sr_item_sk#20 = i_item_sk#38)
            :     :        :     :- Project [sr_returned_date_sk#18, sr_item_sk#20, sr_return_quantity#28]
            :     :        :     :  +- Filter (isnotnull(sr_item_sk#20) && isnotnull(sr_returned_date_sk#18))
            :     :        :     :     +- Relation[sr_returned_date_sk#18,sr_return_time_sk#19,sr_item_sk#20,sr_customer_sk#21,sr_cdemo_sk#22,sr_hdemo_sk#23,sr_addr_sk#24,sr_store_sk#25,sr_reason_sk#26,sr_ticket_number#27,sr_return_quantity#28,sr_return_amt#29,sr_return_tax#30,sr_return_amt_inc_tax#31,sr_fee#32,sr_return_ship_cost#33,sr_refunded_cash#34,sr_reversed_charge#35,sr_store_credit#36,sr_net_loss#37] parquet
            :     :        :     +- Project [i_item_sk#38, i_item_id#39]
            :     :        :        +- Filter (isnotnull(i_item_sk#38) && isnotnull(i_item_id#39))
            :     :        :           +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
            :     :        +- Project [d_date_sk#60]
            :     :           +- Join LeftSemi, (d_date#62 = d_date#62#142)
            :     :              :- Project [d_date_sk#60, d_date#62]
            :     :              :  +- Filter isnotnull(d_date_sk#60)
            :     :              :     +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
            :     :              +- Project [d_date#62 AS d_date#62#142]
            :     :                 +- Join LeftSemi, (d_week_seq#64 = d_week_seq#64#145)
            :     :                    :- Project [d_date#62, d_week_seq#64]
            :     :                    :  +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
            :     :                    +- Project [d_week_seq#64 AS d_week_seq#64#145]
            :     :                       +- Filter d_date#62 IN (2000-06-30,2000-09-27,2000-11-17)
            :     :                          +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
            :     +- Aggregate [i_item_id#39], [i_item_id#39 AS item_id#8, sum(cast(cr_return_quantity#106 as bigint)) AS cr_item_qty#9L]
            :        +- Project [cr_return_quantity#106, i_item_id#39]
            :           +- Join Inner, (cr_returned_date_sk#89 = d_date_sk#60)
            :              :- Project [cr_returned_date_sk#89, cr_return_quantity#106, i_item_id#39]
            :              :  +- Join Inner, (cr_item_sk#91 = i_item_sk#38)
            :              :     :- Project [cr_returned_date_sk#89, cr_item_sk#91, cr_return_quantity#106]
            :              :     :  +- Filter (isnotnull(cr_item_sk#91) && isnotnull(cr_returned_date_sk#89))
            :              :     :     +- Relation[cr_returned_date_sk#89,cr_returned_time_sk#90,cr_item_sk#91,cr_refunded_customer_sk#92,cr_refunded_cdemo_sk#93,cr_refunded_hdemo_sk#94,cr_refunded_addr_sk#95,cr_returning_customer_sk#96,cr_returning_cdemo_sk#97,cr_returning_hdemo_sk#98,cr_returning_addr_sk#99,cr_call_center_sk#100,cr_catalog_page_sk#101,cr_ship_mode_sk#102,cr_warehouse_sk#103,cr_reason_sk#104,cr_order_number#105,cr_return_quantity#106,cr_return_amount#107,cr_return_tax#108,cr_return_amt_inc_tax#109,cr_fee#110,cr_return_ship_cost#111,cr_refunded_cash#112,... 3 more fields] parquet
            :              :     +- Project [i_item_sk#38, i_item_id#39]
            :              :        +- Filter (isnotnull(i_item_sk#38) && isnotnull(i_item_id#39))
            :              :           +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
            :              +- Project [d_date_sk#60]
            :                 +- Join LeftSemi, (d_date#62 = d_date#62#143)
            :                    :- Project [d_date_sk#60, d_date#62]
            :                    :  +- Filter isnotnull(d_date_sk#60)
            :                    :     +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
            :                    +- Project [d_date#62 AS d_date#62#143]
            :                       +- Join LeftSemi, (d_week_seq#64 = d_week_seq#64#146)
            :                          :- Project [d_date#62, d_week_seq#64]
            :                          :  +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
            :                          +- Project [d_week_seq#64 AS d_week_seq#64#146]
            :                             +- Filter d_date#62 IN (2000-06-30,2000-09-27,2000-11-17)
            :                                +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
            +- Aggregate [i_item_id#39], [i_item_id#39 AS item_id#12, sum(cast(wr_return_quantity#131 as bigint)) AS wr_item_qty#13L]
               +- Project [wr_return_quantity#131, i_item_id#39]
                  +- Join Inner, (wr_returned_date_sk#117 = d_date_sk#60)
                     :- Project [wr_returned_date_sk#117, wr_return_quantity#131, i_item_id#39]
                     :  +- Join Inner, (wr_item_sk#119 = i_item_sk#38)
                     :     :- Project [wr_returned_date_sk#117, wr_item_sk#119, wr_return_quantity#131]
                     :     :  +- Filter (isnotnull(wr_item_sk#119) && isnotnull(wr_returned_date_sk#117))
                     :     :     +- Relation[wr_returned_date_sk#117,wr_returned_time_sk#118,wr_item_sk#119,wr_refunded_customer_sk#120,wr_refunded_cdemo_sk#121,wr_refunded_hdemo_sk#122,wr_refunded_addr_sk#123,wr_returning_customer_sk#124,wr_returning_cdemo_sk#125,wr_returning_hdemo_sk#126,wr_returning_addr_sk#127,wr_web_page_sk#128,wr_reason_sk#129,wr_order_number#130,wr_return_quantity#131,wr_return_amt#132,wr_return_tax#133,wr_return_amt_inc_tax#134,wr_fee#135,wr_return_ship_cost#136,wr_refunded_cash#137,wr_reversed_charge#138,wr_account_credit#139,wr_net_loss#140] parquet
                     :     +- Project [i_item_sk#38, i_item_id#39]
                     :        +- Filter (isnotnull(i_item_sk#38) && isnotnull(i_item_id#39))
                     :           +- Relation[i_item_sk#38,i_item_id#39,i_rec_start_date#40,i_rec_end_date#41,i_item_desc#42,i_current_price#43,i_wholesale_cost#44,i_brand_id#45,i_brand#46,i_class_id#47,i_class#48,i_category_id#49,i_category#50,i_manufact_id#51,i_manufact#52,i_size#53,i_formulation#54,i_color#55,i_units#56,i_container#57,i_manager_id#58,i_product_name#59] parquet
                     +- Project [d_date_sk#60]
                        +- Join LeftSemi, (d_date#62 = d_date#62#144)
                           :- Project [d_date_sk#60, d_date#62]
                           :  +- Filter isnotnull(d_date_sk#60)
                           :     +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
                           +- Project [d_date#62 AS d_date#62#144]
                              +- Join LeftSemi, (d_week_seq#64 = d_week_seq#64#147)
                                 :- Project [d_date#62, d_week_seq#64]
                                 :  +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet
                                 +- Project [d_week_seq#64 AS d_week_seq#64#147]
                                    +- Filter d_date#62 IN (2000-06-30,2000-09-27,2000-11-17)
                                       +- Relation[d_date_sk#60,d_date_id#61,d_date#62,d_month_seq#63,d_week_seq#64,d_quarter_seq#65,d_year#66,d_dow#67,d_moy#68,d_dom#69,d_qoy#70,d_fy_year#71,d_fy_quarter_seq#72,d_fy_week_seq#73,d_day_name#74,d_quarter_name#75,d_holiday#76,d_weekend#77,d_following_holiday#78,d_first_dom#79,d_last_dom#80,d_same_day_ly#81,d_same_day_lq#82,d_current_day#83,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[item_id#4 ASC NULLS FIRST,sr_item_qty#5L ASC NULLS FIRST], output=[item_id#4,sr_item_qty#5L,sr_dev#0,cr_item_qty#9L,cr_dev#1,wr_item_qty#13L,wr_dev#2,average#3])
+- *(23) Project [item_id#4, sr_item_qty#5L, (((cast(sr_item_qty#5L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS sr_dev#0, cr_item_qty#9L, (((cast(cr_item_qty#9L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS cr_dev#1, wr_item_qty#13L, (((cast(wr_item_qty#13L as double) / cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as double)) / 3.0) * 100.0) AS wr_dev#2, CheckOverflow((promote_precision(cast(cast(((sr_item_qty#5L + cr_item_qty#9L) + wr_item_qty#13L) as decimal(20,0)) as decimal(21,1))) / 3.0), DecimalType(27,6)) AS average#3]
   +- *(23) SortMergeJoin [item_id#4], [item_id#12], Inner
      :- *(15) Project [item_id#4, sr_item_qty#5L, cr_item_qty#9L]
      :  +- *(15) SortMergeJoin [item_id#4], [item_id#8], Inner
      :     :- *(7) Sort [item_id#4 ASC NULLS FIRST], false, 0
      :     :  +- Exchange hashpartitioning(item_id#4, 200)
      :     :     +- *(6) HashAggregate(keys=[i_item_id#39], functions=[sum(cast(sr_return_quantity#28 as bigint))], output=[item_id#4, sr_item_qty#5L])
      :     :        +- Exchange hashpartitioning(i_item_id#39, 200)
      :     :           +- *(5) HashAggregate(keys=[i_item_id#39], functions=[partial_sum(cast(sr_return_quantity#28 as bigint))], output=[i_item_id#39, sum#149L])
      :     :              +- *(5) Project [sr_return_quantity#28, i_item_id#39]
      :     :                 +- *(5) BroadcastHashJoin [sr_returned_date_sk#18], [d_date_sk#60], Inner, BuildRight
      :     :                    :- *(5) Project [sr_returned_date_sk#18, sr_return_quantity#28, i_item_id#39]
      :     :                    :  +- *(5) BroadcastHashJoin [sr_item_sk#20], [i_item_sk#38], Inner, BuildRight
      :     :                    :     :- *(5) Project [sr_returned_date_sk#18, sr_item_sk#20, sr_return_quantity#28]
      :     :                    :     :  +- *(5) Filter (isnotnull(sr_item_sk#20) && isnotnull(sr_returned_date_sk#18))
      :     :                    :     :     +- *(5) FileScan parquet tpcds.store_returns[sr_returned_date_sk#18,sr_item_sk#20,sr_return_quantity#28] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_returned_date_sk)], ReadSchema: struct<sr_returned_date_sk:int,sr_item_sk:int,sr_return_quantity:int>
      :     :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                    :        +- *(1) Project [i_item_sk#38, i_item_id#39]
      :     :                    :           +- *(1) Filter (isnotnull(i_item_sk#38) && isnotnull(i_item_id#39))
      :     :                    :              +- *(1) FileScan parquet tpcds.item[i_item_sk#38,i_item_id#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_item_id)], ReadSchema: struct<i_item_sk:int,i_item_id:string>
      :     :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                       +- *(4) Project [d_date_sk#60]
      :     :                          +- *(4) BroadcastHashJoin [d_date#62], [d_date#62#142], LeftSemi, BuildRight
      :     :                             :- *(4) Project [d_date_sk#60, d_date#62]
      :     :                             :  +- *(4) Filter isnotnull(d_date_sk#60)
      :     :                             :     +- *(4) FileScan parquet tpcds.date_dim[d_date_sk#60,d_date#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_date:string>
      :     :                             +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
      :     :                                +- *(3) Project [d_date#62 AS d_date#62#142]
      :     :                                   +- *(3) BroadcastHashJoin [d_week_seq#64], [d_week_seq#64#145], LeftSemi, BuildRight
      :     :                                      :- *(3) FileScan parquet tpcds.date_dim[d_date#62,d_week_seq#64] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<d_date:string,d_week_seq:int>
      :     :                                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                                         +- *(2) Project [d_week_seq#64 AS d_week_seq#64#145]
      :     :                                            +- *(2) Filter d_date#62 IN (2000-06-30,2000-09-27,2000-11-17)
      :     :                                               +- *(2) FileScan parquet tpcds.date_dim[d_date#62,d_week_seq#64] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [In(d_date, [2000-06-30,2000-09-27,2000-11-17])], ReadSchema: struct<d_date:string,d_week_seq:int>
      :     +- *(14) Sort [item_id#8 ASC NULLS FIRST], false, 0
      :        +- Exchange hashpartitioning(item_id#8, 200)
      :           +- *(13) HashAggregate(keys=[i_item_id#39], functions=[sum(cast(cr_return_quantity#106 as bigint))], output=[item_id#8, cr_item_qty#9L])
      :              +- Exchange hashpartitioning(i_item_id#39, 200)
      :                 +- *(12) HashAggregate(keys=[i_item_id#39], functions=[partial_sum(cast(cr_return_quantity#106 as bigint))], output=[i_item_id#39, sum#151L])
      :                    +- *(12) Project [cr_return_quantity#106, i_item_id#39]
      :                       +- *(12) BroadcastHashJoin [cr_returned_date_sk#89], [d_date_sk#60], Inner, BuildRight
      :                          :- *(12) Project [cr_returned_date_sk#89, cr_return_quantity#106, i_item_id#39]
      :                          :  +- *(12) BroadcastHashJoin [cr_item_sk#91], [i_item_sk#38], Inner, BuildRight
      :                          :     :- *(12) Project [cr_returned_date_sk#89, cr_item_sk#91, cr_return_quantity#106]
      :                          :     :  +- *(12) Filter (isnotnull(cr_item_sk#91) && isnotnull(cr_returned_date_sk#89))
      :                          :     :     +- *(12) FileScan parquet tpcds.catalog_returns[cr_returned_date_sk#89,cr_item_sk#91,cr_return_quantity#106] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_item_sk), IsNotNull(cr_returned_date_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_item_sk:int,cr_return_quantity:int>
      :                          :     +- ReusedExchange [i_item_sk#38, i_item_id#39], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                          +- ReusedExchange [d_date_sk#60], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      +- *(22) Sort [item_id#12 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(item_id#12, 200)
            +- *(21) HashAggregate(keys=[i_item_id#39], functions=[sum(cast(wr_return_quantity#131 as bigint))], output=[item_id#12, wr_item_qty#13L])
               +- Exchange hashpartitioning(i_item_id#39, 200)
                  +- *(20) HashAggregate(keys=[i_item_id#39], functions=[partial_sum(cast(wr_return_quantity#131 as bigint))], output=[i_item_id#39, sum#153L])
                     +- *(20) Project [wr_return_quantity#131, i_item_id#39]
                        +- *(20) BroadcastHashJoin [wr_returned_date_sk#117], [d_date_sk#60], Inner, BuildRight
                           :- *(20) Project [wr_returned_date_sk#117, wr_return_quantity#131, i_item_id#39]
                           :  +- *(20) BroadcastHashJoin [wr_item_sk#119], [i_item_sk#38], Inner, BuildRight
                           :     :- *(20) Project [wr_returned_date_sk#117, wr_item_sk#119, wr_return_quantity#131]
                           :     :  +- *(20) Filter (isnotnull(wr_item_sk#119) && isnotnull(wr_returned_date_sk#117))
                           :     :     +- *(20) FileScan parquet tpcds.web_returns[wr_returned_date_sk#117,wr_item_sk#119,wr_return_quantity#131] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_item_sk), IsNotNull(wr_returned_date_sk)], ReadSchema: struct<wr_returned_date_sk:int,wr_item_sk:int,wr_return_quantity:int>
                           :     +- ReusedExchange [i_item_sk#38, i_item_id#39], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                           +- ReusedExchange [d_date_sk#60], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 5.0 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 83 in stream 0 using template query83.tpl
------------------------------------------------------^^^

