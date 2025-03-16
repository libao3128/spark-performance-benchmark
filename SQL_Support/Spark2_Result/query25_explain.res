== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['i_item_id ASC NULLS FIRST, 'i_item_desc ASC NULLS FIRST, 's_store_id ASC NULLS FIRST, 's_store_name ASC NULLS FIRST], true
      +- 'Aggregate ['i_item_id, 'i_item_desc, 's_store_id, 's_store_name], ['i_item_id, 'i_item_desc, 's_store_id, 's_store_name, 'sum('ss_net_profit) AS store_sales_profit#0, 'sum('sr_net_loss) AS store_returns_loss#1, 'sum('cs_net_profit) AS catalog_sales_profit#2]
         +- 'Filter ((((('d1.d_moy = 4) && ('d1.d_year = 2001)) && (('d1.d_date_sk = 'ss_sold_date_sk) && ('i_item_sk = 'ss_item_sk))) && ((('s_store_sk = 'ss_store_sk) && ('ss_customer_sk = 'sr_customer_sk)) && (('ss_item_sk = 'sr_item_sk) && ('ss_ticket_number = 'sr_ticket_number)))) && (((('sr_returned_date_sk = 'd2.d_date_sk) && (('d2.d_moy >= 4) && ('d2.d_moy <= 10))) && (('d2.d_year = 2001) && ('sr_customer_sk = 'cs_bill_customer_sk))) && ((('sr_item_sk = 'cs_item_sk) && ('cs_sold_date_sk = 'd3.d_date_sk)) && ((('d3.d_moy >= 4) && ('d3.d_moy <= 10)) && ('d3.d_year = 2001)))))
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
i_item_id: string, i_item_desc: string, s_store_id: string, s_store_name: string, store_sales_profit: double, store_returns_loss: double, catalog_sales_profit: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115, store_sales_profit#0, store_returns_loss#1, catalog_sales_profit#2]
      +- Sort [i_item_id#140 ASC NULLS FIRST, i_item_desc#143 ASC NULLS FIRST, s_store_id#111 ASC NULLS FIRST, s_store_name#115 ASC NULLS FIRST], true
         +- Aggregate [i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115], [i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115, sum(ss_net_profit#27) AS store_sales_profit#0, sum(sr_net_loss#47) AS store_returns_loss#1, sum(cs_net_profit#81) AS catalog_sales_profit#2]
            +- Filter (((((d_moy#90 = 4) && (d_year#88 = 2001)) && ((d_date_sk#82 = ss_sold_date_sk#5) && (i_item_sk#139 = ss_item_sk#7))) && (((s_store_sk#110 = ss_store_sk#12) && (ss_customer_sk#8 = sr_customer_sk#31)) && ((ss_item_sk#7 = sr_item_sk#30) && (ss_ticket_number#14 = sr_ticket_number#37)))) && ((((sr_returned_date_sk#28 = d_date_sk#161) && ((d_moy#169 >= 4) && (d_moy#169 <= 10))) && ((d_year#167 = 2001) && (sr_customer_sk#31 = cs_bill_customer_sk#51))) && (((sr_item_sk#30 = cs_item_sk#63) && (cs_sold_date_sk#48 = d_date_sk#189)) && (((d_moy#197 >= 4) && (d_moy#197 <= 10)) && (d_year#195 = 2001)))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- Join Inner
                  :  :  :  :  :- Join Inner
                  :  :  :  :  :  :- Join Inner
                  :  :  :  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
                  :  :  :  :  :  :  :  +- Relation[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
                  :  :  :  :  :  :  +- SubqueryAlias `tpcds`.`store_returns`
                  :  :  :  :  :  :     +- Relation[sr_returned_date_sk#28,sr_return_time_sk#29,sr_item_sk#30,sr_customer_sk#31,sr_cdemo_sk#32,sr_hdemo_sk#33,sr_addr_sk#34,sr_store_sk#35,sr_reason_sk#36,sr_ticket_number#37,sr_return_quantity#38,sr_return_amt#39,sr_return_tax#40,sr_return_amt_inc_tax#41,sr_fee#42,sr_return_ship_cost#43,sr_refunded_cash#44,sr_reversed_charge#45,sr_store_credit#46,sr_net_loss#47] parquet
                  :  :  :  :  :  +- SubqueryAlias `tpcds`.`catalog_sales`
                  :  :  :  :  :     +- Relation[cs_sold_date_sk#48,cs_sold_time_sk#49,cs_ship_date_sk#50,cs_bill_customer_sk#51,cs_bill_cdemo_sk#52,cs_bill_hdemo_sk#53,cs_bill_addr_sk#54,cs_ship_customer_sk#55,cs_ship_cdemo_sk#56,cs_ship_hdemo_sk#57,cs_ship_addr_sk#58,cs_call_center_sk#59,cs_catalog_page_sk#60,cs_ship_mode_sk#61,cs_warehouse_sk#62,cs_item_sk#63,cs_promo_sk#64,cs_order_number#65,cs_quantity#66,cs_wholesale_cost#67,cs_list_price#68,cs_sales_price#69,cs_ext_discount_amt#70,cs_ext_sales_price#71,... 10 more fields] parquet
                  :  :  :  :  +- SubqueryAlias `d1`
                  :  :  :  :     +- SubqueryAlias `tpcds`.`date_dim`
                  :  :  :  :        +- Relation[d_date_sk#82,d_date_id#83,d_date#84,d_month_seq#85,d_week_seq#86,d_quarter_seq#87,d_year#88,d_dow#89,d_moy#90,d_dom#91,d_qoy#92,d_fy_year#93,d_fy_quarter_seq#94,d_fy_week_seq#95,d_day_name#96,d_quarter_name#97,d_holiday#98,d_weekend#99,d_following_holiday#100,d_first_dom#101,d_last_dom#102,d_same_day_ly#103,d_same_day_lq#104,d_current_day#105,... 4 more fields] parquet
                  :  :  :  +- SubqueryAlias `d2`
                  :  :  :     +- SubqueryAlias `tpcds`.`date_dim`
                  :  :  :        +- Relation[d_date_sk#161,d_date_id#162,d_date#163,d_month_seq#164,d_week_seq#165,d_quarter_seq#166,d_year#167,d_dow#168,d_moy#169,d_dom#170,d_qoy#171,d_fy_year#172,d_fy_quarter_seq#173,d_fy_week_seq#174,d_day_name#175,d_quarter_name#176,d_holiday#177,d_weekend#178,d_following_holiday#179,d_first_dom#180,d_last_dom#181,d_same_day_ly#182,d_same_day_lq#183,d_current_day#184,... 4 more fields] parquet
                  :  :  +- SubqueryAlias `d3`
                  :  :     +- SubqueryAlias `tpcds`.`date_dim`
                  :  :        +- Relation[d_date_sk#189,d_date_id#190,d_date#191,d_month_seq#192,d_week_seq#193,d_quarter_seq#194,d_year#195,d_dow#196,d_moy#197,d_dom#198,d_qoy#199,d_fy_year#200,d_fy_quarter_seq#201,d_fy_week_seq#202,d_day_name#203,d_quarter_name#204,d_holiday#205,d_weekend#206,d_following_holiday#207,d_first_dom#208,d_last_dom#209,d_same_day_ly#210,d_same_day_lq#211,d_current_day#212,... 4 more fields] parquet
                  :  +- SubqueryAlias `tpcds`.`store`
                  :     +- Relation[s_store_sk#110,s_store_id#111,s_rec_start_date#112,s_rec_end_date#113,s_closed_date_sk#114,s_store_name#115,s_number_employees#116,s_floor_space#117,s_hours#118,s_manager#119,s_market_id#120,s_geography_class#121,s_market_desc#122,s_market_manager#123,s_division_id#124,s_division_name#125,s_company_id#126,s_company_name#127,s_street_number#128,s_street_name#129,s_street_type#130,s_suite_number#131,s_city#132,s_county#133,... 5 more fields] parquet
                  +- SubqueryAlias `tpcds`.`item`
                     +- Relation[i_item_sk#139,i_item_id#140,i_rec_start_date#141,i_rec_end_date#142,i_item_desc#143,i_current_price#144,i_wholesale_cost#145,i_brand_id#146,i_brand#147,i_class_id#148,i_class#149,i_category_id#150,i_category#151,i_manufact_id#152,i_manufact#153,i_size#154,i_formulation#155,i_color#156,i_units#157,i_container#158,i_manager_id#159,i_product_name#160] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [i_item_id#140 ASC NULLS FIRST, i_item_desc#143 ASC NULLS FIRST, s_store_id#111 ASC NULLS FIRST, s_store_name#115 ASC NULLS FIRST], true
      +- Aggregate [i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115], [i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115, sum(ss_net_profit#27) AS store_sales_profit#0, sum(sr_net_loss#47) AS store_returns_loss#1, sum(cs_net_profit#81) AS catalog_sales_profit#2]
         +- Project [ss_net_profit#27, sr_net_loss#47, cs_net_profit#81, s_store_id#111, s_store_name#115, i_item_id#140, i_item_desc#143]
            +- Join Inner, (i_item_sk#139 = ss_item_sk#7)
               :- Project [ss_item_sk#7, ss_net_profit#27, sr_net_loss#47, cs_net_profit#81, s_store_id#111, s_store_name#115]
               :  +- Join Inner, (s_store_sk#110 = ss_store_sk#12)
               :     :- Project [ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_net_loss#47, cs_net_profit#81]
               :     :  +- Join Inner, (cs_sold_date_sk#48 = d_date_sk#189)
               :     :     :- Project [ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_net_loss#47, cs_sold_date_sk#48, cs_net_profit#81]
               :     :     :  +- Join Inner, (sr_returned_date_sk#28 = d_date_sk#161)
               :     :     :     :- Project [ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_returned_date_sk#28, sr_net_loss#47, cs_sold_date_sk#48, cs_net_profit#81]
               :     :     :     :  +- Join Inner, (d_date_sk#82 = ss_sold_date_sk#5)
               :     :     :     :     :- Project [ss_sold_date_sk#5, ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_returned_date_sk#28, sr_net_loss#47, cs_sold_date_sk#48, cs_net_profit#81]
               :     :     :     :     :  +- Join Inner, ((sr_customer_sk#31 = cs_bill_customer_sk#51) && (sr_item_sk#30 = cs_item_sk#63))
               :     :     :     :     :     :- Project [ss_sold_date_sk#5, ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_returned_date_sk#28, sr_item_sk#30, sr_customer_sk#31, sr_net_loss#47]
               :     :     :     :     :     :  +- Join Inner, (((ss_customer_sk#8 = sr_customer_sk#31) && (ss_item_sk#7 = sr_item_sk#30)) && (ss_ticket_number#14 = sr_ticket_number#37))
               :     :     :     :     :     :     :- Project [ss_sold_date_sk#5, ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_ticket_number#14, ss_net_profit#27]
               :     :     :     :     :     :     :  +- Filter ((((isnotnull(ss_customer_sk#8) && isnotnull(ss_item_sk#7)) && isnotnull(ss_ticket_number#14)) && isnotnull(ss_sold_date_sk#5)) && isnotnull(ss_store_sk#12))
               :     :     :     :     :     :     :     +- Relation[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
               :     :     :     :     :     :     +- Project [sr_returned_date_sk#28, sr_item_sk#30, sr_customer_sk#31, sr_ticket_number#37, sr_net_loss#47]
               :     :     :     :     :     :        +- Filter (((isnotnull(sr_customer_sk#31) && isnotnull(sr_ticket_number#37)) && isnotnull(sr_item_sk#30)) && isnotnull(sr_returned_date_sk#28))
               :     :     :     :     :     :           +- Relation[sr_returned_date_sk#28,sr_return_time_sk#29,sr_item_sk#30,sr_customer_sk#31,sr_cdemo_sk#32,sr_hdemo_sk#33,sr_addr_sk#34,sr_store_sk#35,sr_reason_sk#36,sr_ticket_number#37,sr_return_quantity#38,sr_return_amt#39,sr_return_tax#40,sr_return_amt_inc_tax#41,sr_fee#42,sr_return_ship_cost#43,sr_refunded_cash#44,sr_reversed_charge#45,sr_store_credit#46,sr_net_loss#47] parquet
               :     :     :     :     :     +- Project [cs_sold_date_sk#48, cs_bill_customer_sk#51, cs_item_sk#63, cs_net_profit#81]
               :     :     :     :     :        +- Filter ((isnotnull(cs_bill_customer_sk#51) && isnotnull(cs_item_sk#63)) && isnotnull(cs_sold_date_sk#48))
               :     :     :     :     :           +- Relation[cs_sold_date_sk#48,cs_sold_time_sk#49,cs_ship_date_sk#50,cs_bill_customer_sk#51,cs_bill_cdemo_sk#52,cs_bill_hdemo_sk#53,cs_bill_addr_sk#54,cs_ship_customer_sk#55,cs_ship_cdemo_sk#56,cs_ship_hdemo_sk#57,cs_ship_addr_sk#58,cs_call_center_sk#59,cs_catalog_page_sk#60,cs_ship_mode_sk#61,cs_warehouse_sk#62,cs_item_sk#63,cs_promo_sk#64,cs_order_number#65,cs_quantity#66,cs_wholesale_cost#67,cs_list_price#68,cs_sales_price#69,cs_ext_discount_amt#70,cs_ext_sales_price#71,... 10 more fields] parquet
               :     :     :     :     +- Project [d_date_sk#82]
               :     :     :     :        +- Filter ((((isnotnull(d_moy#90) && isnotnull(d_year#88)) && (d_moy#90 = 4)) && (d_year#88 = 2001)) && isnotnull(d_date_sk#82))
               :     :     :     :           +- Relation[d_date_sk#82,d_date_id#83,d_date#84,d_month_seq#85,d_week_seq#86,d_quarter_seq#87,d_year#88,d_dow#89,d_moy#90,d_dom#91,d_qoy#92,d_fy_year#93,d_fy_quarter_seq#94,d_fy_week_seq#95,d_day_name#96,d_quarter_name#97,d_holiday#98,d_weekend#99,d_following_holiday#100,d_first_dom#101,d_last_dom#102,d_same_day_ly#103,d_same_day_lq#104,d_current_day#105,... 4 more fields] parquet
               :     :     :     +- Project [d_date_sk#161]
               :     :     :        +- Filter (((((isnotnull(d_year#167) && isnotnull(d_moy#169)) && (d_moy#169 >= 4)) && (d_moy#169 <= 10)) && (d_year#167 = 2001)) && isnotnull(d_date_sk#161))
               :     :     :           +- Relation[d_date_sk#161,d_date_id#162,d_date#163,d_month_seq#164,d_week_seq#165,d_quarter_seq#166,d_year#167,d_dow#168,d_moy#169,d_dom#170,d_qoy#171,d_fy_year#172,d_fy_quarter_seq#173,d_fy_week_seq#174,d_day_name#175,d_quarter_name#176,d_holiday#177,d_weekend#178,d_following_holiday#179,d_first_dom#180,d_last_dom#181,d_same_day_ly#182,d_same_day_lq#183,d_current_day#184,... 4 more fields] parquet
               :     :     +- Project [d_date_sk#189]
               :     :        +- Filter (((((isnotnull(d_year#195) && isnotnull(d_moy#197)) && (d_moy#197 >= 4)) && (d_moy#197 <= 10)) && (d_year#195 = 2001)) && isnotnull(d_date_sk#189))
               :     :           +- Relation[d_date_sk#189,d_date_id#190,d_date#191,d_month_seq#192,d_week_seq#193,d_quarter_seq#194,d_year#195,d_dow#196,d_moy#197,d_dom#198,d_qoy#199,d_fy_year#200,d_fy_quarter_seq#201,d_fy_week_seq#202,d_day_name#203,d_quarter_name#204,d_holiday#205,d_weekend#206,d_following_holiday#207,d_first_dom#208,d_last_dom#209,d_same_day_ly#210,d_same_day_lq#211,d_current_day#212,... 4 more fields] parquet
               :     +- Project [s_store_sk#110, s_store_id#111, s_store_name#115]
               :        +- Filter isnotnull(s_store_sk#110)
               :           +- Relation[s_store_sk#110,s_store_id#111,s_rec_start_date#112,s_rec_end_date#113,s_closed_date_sk#114,s_store_name#115,s_number_employees#116,s_floor_space#117,s_hours#118,s_manager#119,s_market_id#120,s_geography_class#121,s_market_desc#122,s_market_manager#123,s_division_id#124,s_division_name#125,s_company_id#126,s_company_name#127,s_street_number#128,s_street_name#129,s_street_type#130,s_suite_number#131,s_city#132,s_county#133,... 5 more fields] parquet
               +- Project [i_item_sk#139, i_item_id#140, i_item_desc#143]
                  +- Filter isnotnull(i_item_sk#139)
                     +- Relation[i_item_sk#139,i_item_id#140,i_rec_start_date#141,i_rec_end_date#142,i_item_desc#143,i_current_price#144,i_wholesale_cost#145,i_brand_id#146,i_brand#147,i_class_id#148,i_class#149,i_category_id#150,i_category#151,i_manufact_id#152,i_manufact#153,i_size#154,i_formulation#155,i_color#156,i_units#157,i_container#158,i_manager_id#159,i_product_name#160] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[i_item_id#140 ASC NULLS FIRST,i_item_desc#143 ASC NULLS FIRST,s_store_id#111 ASC NULLS FIRST,s_store_name#115 ASC NULLS FIRST], output=[i_item_id#140,i_item_desc#143,s_store_id#111,s_store_name#115,store_sales_profit#0,store_returns_loss#1,catalog_sales_profit#2])
+- *(12) HashAggregate(keys=[i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115], functions=[sum(ss_net_profit#27), sum(sr_net_loss#47), sum(cs_net_profit#81)], output=[i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115, store_sales_profit#0, store_returns_loss#1, catalog_sales_profit#2])
   +- Exchange hashpartitioning(i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115, 200)
      +- *(11) HashAggregate(keys=[i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115], functions=[partial_sum(ss_net_profit#27), partial_sum(sr_net_loss#47), partial_sum(cs_net_profit#81)], output=[i_item_id#140, i_item_desc#143, s_store_id#111, s_store_name#115, sum#227, sum#228, sum#229])
         +- *(11) Project [ss_net_profit#27, sr_net_loss#47, cs_net_profit#81, s_store_id#111, s_store_name#115, i_item_id#140, i_item_desc#143]
            +- *(11) BroadcastHashJoin [ss_item_sk#7], [i_item_sk#139], Inner, BuildRight
               :- *(11) Project [ss_item_sk#7, ss_net_profit#27, sr_net_loss#47, cs_net_profit#81, s_store_id#111, s_store_name#115]
               :  +- *(11) BroadcastHashJoin [ss_store_sk#12], [s_store_sk#110], Inner, BuildRight
               :     :- *(11) Project [ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_net_loss#47, cs_net_profit#81]
               :     :  +- *(11) BroadcastHashJoin [cs_sold_date_sk#48], [d_date_sk#189], Inner, BuildRight
               :     :     :- *(11) Project [ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_net_loss#47, cs_sold_date_sk#48, cs_net_profit#81]
               :     :     :  +- *(11) BroadcastHashJoin [sr_returned_date_sk#28], [d_date_sk#161], Inner, BuildRight
               :     :     :     :- *(11) Project [ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_returned_date_sk#28, sr_net_loss#47, cs_sold_date_sk#48, cs_net_profit#81]
               :     :     :     :  +- *(11) BroadcastHashJoin [ss_sold_date_sk#5], [d_date_sk#82], Inner, BuildRight
               :     :     :     :     :- *(11) Project [ss_sold_date_sk#5, ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_returned_date_sk#28, sr_net_loss#47, cs_sold_date_sk#48, cs_net_profit#81]
               :     :     :     :     :  +- *(11) SortMergeJoin [sr_customer_sk#31, sr_item_sk#30], [cs_bill_customer_sk#51, cs_item_sk#63], Inner
               :     :     :     :     :     :- *(3) Sort [sr_customer_sk#31 ASC NULLS FIRST, sr_item_sk#30 ASC NULLS FIRST], false, 0
               :     :     :     :     :     :  +- Exchange hashpartitioning(sr_customer_sk#31, sr_item_sk#30, 200)
               :     :     :     :     :     :     +- *(2) Project [ss_sold_date_sk#5, ss_item_sk#7, ss_store_sk#12, ss_net_profit#27, sr_returned_date_sk#28, sr_item_sk#30, sr_customer_sk#31, sr_net_loss#47]
               :     :     :     :     :     :        +- *(2) BroadcastHashJoin [ss_customer_sk#8, ss_item_sk#7, ss_ticket_number#14], [sr_customer_sk#31, sr_item_sk#30, sr_ticket_number#37], Inner, BuildRight
               :     :     :     :     :     :           :- *(2) Project [ss_sold_date_sk#5, ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_ticket_number#14, ss_net_profit#27]
               :     :     :     :     :     :           :  +- *(2) Filter ((((isnotnull(ss_customer_sk#8) && isnotnull(ss_item_sk#7)) && isnotnull(ss_ticket_number#14)) && isnotnull(ss_sold_date_sk#5)) && isnotnull(ss_store_sk#12))
               :     :     :     :     :     :           :     +- *(2) FileScan parquet tpcds.store_sales[ss_sold_date_sk#5,ss_item_sk#7,ss_customer_sk#8,ss_store_sk#12,ss_ticket_number#14,ss_net_profit#27] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_item_sk), IsNotNull(ss_ticket_number), IsNotNull(ss_sold..., ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ticket_number:int...
               :     :     :     :     :     :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[2, int, true], input[1, int, true], input[3, int, true]))
               :     :     :     :     :     :              +- *(1) Project [sr_returned_date_sk#28, sr_item_sk#30, sr_customer_sk#31, sr_ticket_number#37, sr_net_loss#47]
               :     :     :     :     :     :                 +- *(1) Filter (((isnotnull(sr_customer_sk#31) && isnotnull(sr_ticket_number#37)) && isnotnull(sr_item_sk#30)) && isnotnull(sr_returned_date_sk#28))
               :     :     :     :     :     :                    +- *(1) FileScan parquet tpcds.store_returns[sr_returned_date_sk#28,sr_item_sk#30,sr_customer_sk#31,sr_ticket_number#37,sr_net_loss#47] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_customer_sk), IsNotNull(sr_ticket_number), IsNotNull(sr_item_sk), IsNotNull(sr_retu..., ReadSchema: struct<sr_returned_date_sk:int,sr_item_sk:int,sr_customer_sk:int,sr_ticket_number:int,sr_net_loss...
               :     :     :     :     :     +- *(5) Sort [cs_bill_customer_sk#51 ASC NULLS FIRST, cs_item_sk#63 ASC NULLS FIRST], false, 0
               :     :     :     :     :        +- Exchange hashpartitioning(cs_bill_customer_sk#51, cs_item_sk#63, 200)
               :     :     :     :     :           +- *(4) Project [cs_sold_date_sk#48, cs_bill_customer_sk#51, cs_item_sk#63, cs_net_profit#81]
               :     :     :     :     :              +- *(4) Filter ((isnotnull(cs_bill_customer_sk#51) && isnotnull(cs_item_sk#63)) && isnotnull(cs_sold_date_sk#48))
               :     :     :     :     :                 +- *(4) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#48,cs_bill_customer_sk#51,cs_item_sk#63,cs_net_profit#81] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_bill_customer_sk), IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_bill_customer_sk:int,cs_item_sk:int,cs_net_profit:double>
               :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :        +- *(6) Project [d_date_sk#82]
               :     :     :     :           +- *(6) Filter ((((isnotnull(d_moy#90) && isnotnull(d_year#88)) && (d_moy#90 = 4)) && (d_year#88 = 2001)) && isnotnull(d_date_sk#82))
               :     :     :     :              +- *(6) FileScan parquet tpcds.date_dim[d_date_sk#82,d_year#88,d_moy#90] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,4), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :        +- *(7) Project [d_date_sk#161]
               :     :     :           +- *(7) Filter (((((isnotnull(d_year#167) && isnotnull(d_moy#169)) && (d_moy#169 >= 4)) && (d_moy#169 <= 10)) && (d_year#167 = 2001)) && isnotnull(d_date_sk#161))
               :     :     :              +- *(7) FileScan parquet tpcds.date_dim[d_date_sk#161,d_year#167,d_moy#169] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), GreaterThanOrEqual(d_moy,4), LessThanOrEqual(d_moy,10), Equ..., ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :     :     +- ReusedExchange [d_date_sk#189], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(9) Project [s_store_sk#110, s_store_id#111, s_store_name#115]
               :           +- *(9) Filter isnotnull(s_store_sk#110)
               :              +- *(9) FileScan parquet tpcds.store[s_store_sk#110,s_store_id#111,s_store_name#115] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:int,s_store_id:string,s_store_name:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(10) Project [i_item_sk#139, i_item_id#140, i_item_desc#143]
                     +- *(10) Filter isnotnull(i_item_sk#139)
                        +- *(10) FileScan parquet tpcds.item[i_item_sk#139,i_item_id#140,i_item_desc#143] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_id:string,i_item_desc:string>
Time taken: 4.498 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 25 in stream 0 using template query25.tpl
------------------------------------------------------^^^

