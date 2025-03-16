== Parsed Logical Plan ==
CTE [all_sales]
:  +- 'SubqueryAlias `all_sales`
:     +- 'Aggregate ['d_year, 'i_brand_id, 'i_class_id, 'i_category_id, 'i_manufact_id], ['d_year, 'i_brand_id, 'i_class_id, 'i_category_id, 'i_manufact_id, 'SUM('sales_cnt) AS sales_cnt#12, 'SUM('sales_amt) AS sales_amt#13]
:        +- 'SubqueryAlias `sales_detail`
:           +- 'Distinct
:              +- 'Union
:                 :- 'Distinct
:                 :  +- 'Union
:                 :     :- 'Project ['d_year, 'i_brand_id, 'i_class_id, 'i_category_id, 'i_manufact_id, ('cs_quantity - 'COALESCE('cr_return_quantity, 0)) AS sales_cnt#6, ('cs_ext_sales_price - 'COALESCE('cr_return_amount, 0.0)) AS sales_amt#7]
:                 :     :  +- 'Filter ('i_category = Books)
:                 :     :     +- 'Join LeftOuter, (('cs_order_number = 'cr_order_number) && ('cs_item_sk = 'cr_item_sk))
:                 :     :        :- 'Join Inner, ('d_date_sk = 'cs_sold_date_sk)
:                 :     :        :  :- 'Join Inner, ('i_item_sk = 'cs_item_sk)
:                 :     :        :  :  :- 'UnresolvedRelation `catalog_sales`
:                 :     :        :  :  +- 'UnresolvedRelation `item`
:                 :     :        :  +- 'UnresolvedRelation `date_dim`
:                 :     :        +- 'UnresolvedRelation `catalog_returns`
:                 :     +- 'Project ['d_year, 'i_brand_id, 'i_class_id, 'i_category_id, 'i_manufact_id, ('ss_quantity - 'COALESCE('sr_return_quantity, 0)) AS sales_cnt#8, ('ss_ext_sales_price - 'COALESCE('sr_return_amt, 0.0)) AS sales_amt#9]
:                 :        +- 'Filter ('i_category = Books)
:                 :           +- 'Join LeftOuter, (('ss_ticket_number = 'sr_ticket_number) && ('ss_item_sk = 'sr_item_sk))
:                 :              :- 'Join Inner, ('d_date_sk = 'ss_sold_date_sk)
:                 :              :  :- 'Join Inner, ('i_item_sk = 'ss_item_sk)
:                 :              :  :  :- 'UnresolvedRelation `store_sales`
:                 :              :  :  +- 'UnresolvedRelation `item`
:                 :              :  +- 'UnresolvedRelation `date_dim`
:                 :              +- 'UnresolvedRelation `store_returns`
:                 +- 'Project ['d_year, 'i_brand_id, 'i_class_id, 'i_category_id, 'i_manufact_id, ('ws_quantity - 'COALESCE('wr_return_quantity, 0)) AS sales_cnt#10, ('ws_ext_sales_price - 'COALESCE('wr_return_amt, 0.0)) AS sales_amt#11]
:                    +- 'Filter ('i_category = Books)
:                       +- 'Join LeftOuter, (('ws_order_number = 'wr_order_number) && ('ws_item_sk = 'wr_item_sk))
:                          :- 'Join Inner, ('d_date_sk = 'ws_sold_date_sk)
:                          :  :- 'Join Inner, ('i_item_sk = 'ws_item_sk)
:                          :  :  :- 'UnresolvedRelation `web_sales`
:                          :  :  +- 'UnresolvedRelation `item`
:                          :  +- 'UnresolvedRelation `date_dim`
:                          +- 'UnresolvedRelation `web_returns`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['sales_cnt_diff ASC NULLS FIRST], true
         +- 'Project ['prev_yr.d_year AS prev_year#0, 'curr_yr.d_year AS year#1, 'curr_yr.i_brand_id, 'curr_yr.i_class_id, 'curr_yr.i_category_id, 'curr_yr.i_manufact_id, 'prev_yr.sales_cnt AS prev_yr_cnt#2, 'curr_yr.sales_cnt AS curr_yr_cnt#3, ('curr_yr.sales_cnt - 'prev_yr.sales_cnt) AS sales_cnt_diff#4, ('curr_yr.sales_amt - 'prev_yr.sales_amt) AS sales_amt_diff#5]
            +- 'Filter (((('curr_yr.i_brand_id = 'prev_yr.i_brand_id) && ('curr_yr.i_class_id = 'prev_yr.i_class_id)) && (('curr_yr.i_category_id = 'prev_yr.i_category_id) && ('curr_yr.i_manufact_id = 'prev_yr.i_manufact_id))) && ((('curr_yr.d_year = 2002) && ('prev_yr.d_year = (2002 - 1))) && ((cast('curr_yr.sales_cnt as decimal(17,2)) / cast('prev_yr.sales_cnt as decimal(17,2))) < 0.9)))
               +- 'Join Inner
                  :- 'SubqueryAlias `curr_yr`
                  :  +- 'UnresolvedRelation `all_sales`
                  +- 'SubqueryAlias `prev_yr`
                     +- 'UnresolvedRelation `all_sales`

== Analyzed Logical Plan ==
prev_year: int, year: int, i_brand_id: int, i_class_id: int, i_category_id: int, i_manufact_id: int, prev_yr_cnt: bigint, curr_yr_cnt: bigint, sales_cnt_diff: bigint, sales_amt_diff: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [sales_cnt_diff#4L ASC NULLS FIRST], true
      +- Project [d_year#538 AS prev_year#0, d_year#78 AS year#1, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#230L AS prev_yr_cnt#2L, sales_cnt#12L AS curr_yr_cnt#3L, (sales_cnt#12L - sales_cnt#230L) AS sales_cnt_diff#4L, (sales_amt#13 - sales_amt#231) AS sales_amt_diff#5]
         +- Filter ((((i_brand_id#57 = i_brand_id#389) && (i_class_id#59 = i_class_id#391)) && ((i_category_id#61 = i_category_id#393) && (i_manufact_id#63 = i_manufact_id#395))) && (((d_year#78 = 2002) && (d_year#538 = (2002 - 1))) && (cast(CheckOverflow((promote_precision(cast(cast(sales_cnt#12L as decimal(17,2)) as decimal(17,2))) / promote_precision(cast(cast(sales_cnt#230L as decimal(17,2)) as decimal(17,2)))), DecimalType(37,20)) as decimal(37,20)) < cast(0.9 as decimal(37,20)))))
            +- Join Inner
               :- SubqueryAlias `curr_yr`
               :  +- SubqueryAlias `all_sales`
               :     +- Aggregate [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63], [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sum(cast(sales_cnt#6 as bigint)) AS sales_cnt#12L, sum(sales_amt#7) AS sales_amt#13]
               :        +- SubqueryAlias `sales_detail`
               :           +- Distinct
               :              +- Union
               :                 :- Distinct
               :                 :  +- Union
               :                 :     :- Project [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, (cs_quantity#34 - coalesce(cr_return_quantity#117, 0)) AS sales_cnt#6, (cs_ext_sales_price#39 - coalesce(cr_return_amount#118, cast(0.0 as double))) AS sales_amt#7]
               :                 :     :  +- Filter (i_category#62 = Books)
               :                 :     :     +- Join LeftOuter, ((cs_order_number#33 = cr_order_number#116) && (cs_item_sk#31 = cr_item_sk#102))
               :                 :     :        :- Join Inner, (d_date_sk#72 = cs_sold_date_sk#16)
               :                 :     :        :  :- Join Inner, (i_item_sk#50 = cs_item_sk#31)
               :                 :     :        :  :  :- SubqueryAlias `tpcds`.`catalog_sales`
               :                 :     :        :  :  :  +- Relation[cs_sold_date_sk#16,cs_sold_time_sk#17,cs_ship_date_sk#18,cs_bill_customer_sk#19,cs_bill_cdemo_sk#20,cs_bill_hdemo_sk#21,cs_bill_addr_sk#22,cs_ship_customer_sk#23,cs_ship_cdemo_sk#24,cs_ship_hdemo_sk#25,cs_ship_addr_sk#26,cs_call_center_sk#27,cs_catalog_page_sk#28,cs_ship_mode_sk#29,cs_warehouse_sk#30,cs_item_sk#31,cs_promo_sk#32,cs_order_number#33,cs_quantity#34,cs_wholesale_cost#35,cs_list_price#36,cs_sales_price#37,cs_ext_discount_amt#38,cs_ext_sales_price#39,... 10 more fields] parquet
               :                 :     :        :  :  +- SubqueryAlias `tpcds`.`item`
               :                 :     :        :  :     +- Relation[i_item_sk#50,i_item_id#51,i_rec_start_date#52,i_rec_end_date#53,i_item_desc#54,i_current_price#55,i_wholesale_cost#56,i_brand_id#57,i_brand#58,i_class_id#59,i_class#60,i_category_id#61,i_category#62,i_manufact_id#63,i_manufact#64,i_size#65,i_formulation#66,i_color#67,i_units#68,i_container#69,i_manager_id#70,i_product_name#71] parquet
               :                 :     :        :  +- SubqueryAlias `tpcds`.`date_dim`
               :                 :     :        :     +- Relation[d_date_sk#72,d_date_id#73,d_date#74,d_month_seq#75,d_week_seq#76,d_quarter_seq#77,d_year#78,d_dow#79,d_moy#80,d_dom#81,d_qoy#82,d_fy_year#83,d_fy_quarter_seq#84,d_fy_week_seq#85,d_day_name#86,d_quarter_name#87,d_holiday#88,d_weekend#89,d_following_holiday#90,d_first_dom#91,d_last_dom#92,d_same_day_ly#93,d_same_day_lq#94,d_current_day#95,... 4 more fields] parquet
               :                 :     :        +- SubqueryAlias `tpcds`.`catalog_returns`
               :                 :     :           +- Relation[cr_returned_date_sk#100,cr_returned_time_sk#101,cr_item_sk#102,cr_refunded_customer_sk#103,cr_refunded_cdemo_sk#104,cr_refunded_hdemo_sk#105,cr_refunded_addr_sk#106,cr_returning_customer_sk#107,cr_returning_cdemo_sk#108,cr_returning_hdemo_sk#109,cr_returning_addr_sk#110,cr_call_center_sk#111,cr_catalog_page_sk#112,cr_ship_mode_sk#113,cr_warehouse_sk#114,cr_reason_sk#115,cr_order_number#116,cr_return_quantity#117,cr_return_amount#118,cr_return_tax#119,cr_return_amt_inc_tax#120,cr_fee#121,cr_return_ship_cost#122,cr_refunded_cash#123,... 3 more fields] parquet
               :                 :     +- Project [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, (ss_quantity#137 - coalesce(sr_return_quantity#160, 0)) AS sales_cnt#8, (ss_ext_sales_price#142 - coalesce(sr_return_amt#161, cast(0.0 as double))) AS sales_amt#9]
               :                 :        +- Filter (i_category#62 = Books)
               :                 :           +- Join LeftOuter, ((ss_ticket_number#136 = sr_ticket_number#159) && (ss_item_sk#129 = sr_item_sk#152))
               :                 :              :- Join Inner, (d_date_sk#72 = ss_sold_date_sk#127)
               :                 :              :  :- Join Inner, (i_item_sk#50 = ss_item_sk#129)
               :                 :              :  :  :- SubqueryAlias `tpcds`.`store_sales`
               :                 :              :  :  :  +- Relation[ss_sold_date_sk#127,ss_sold_time_sk#128,ss_item_sk#129,ss_customer_sk#130,ss_cdemo_sk#131,ss_hdemo_sk#132,ss_addr_sk#133,ss_store_sk#134,ss_promo_sk#135,ss_ticket_number#136,ss_quantity#137,ss_wholesale_cost#138,ss_list_price#139,ss_sales_price#140,ss_ext_discount_amt#141,ss_ext_sales_price#142,ss_ext_wholesale_cost#143,ss_ext_list_price#144,ss_ext_tax#145,ss_coupon_amt#146,ss_net_paid#147,ss_net_paid_inc_tax#148,ss_net_profit#149] parquet
               :                 :              :  :  +- SubqueryAlias `tpcds`.`item`
               :                 :              :  :     +- Relation[i_item_sk#50,i_item_id#51,i_rec_start_date#52,i_rec_end_date#53,i_item_desc#54,i_current_price#55,i_wholesale_cost#56,i_brand_id#57,i_brand#58,i_class_id#59,i_class#60,i_category_id#61,i_category#62,i_manufact_id#63,i_manufact#64,i_size#65,i_formulation#66,i_color#67,i_units#68,i_container#69,i_manager_id#70,i_product_name#71] parquet
               :                 :              :  +- SubqueryAlias `tpcds`.`date_dim`
               :                 :              :     +- Relation[d_date_sk#72,d_date_id#73,d_date#74,d_month_seq#75,d_week_seq#76,d_quarter_seq#77,d_year#78,d_dow#79,d_moy#80,d_dom#81,d_qoy#82,d_fy_year#83,d_fy_quarter_seq#84,d_fy_week_seq#85,d_day_name#86,d_quarter_name#87,d_holiday#88,d_weekend#89,d_following_holiday#90,d_first_dom#91,d_last_dom#92,d_same_day_ly#93,d_same_day_lq#94,d_current_day#95,... 4 more fields] parquet
               :                 :              +- SubqueryAlias `tpcds`.`store_returns`
               :                 :                 +- Relation[sr_returned_date_sk#150,sr_return_time_sk#151,sr_item_sk#152,sr_customer_sk#153,sr_cdemo_sk#154,sr_hdemo_sk#155,sr_addr_sk#156,sr_store_sk#157,sr_reason_sk#158,sr_ticket_number#159,sr_return_quantity#160,sr_return_amt#161,sr_return_tax#162,sr_return_amt_inc_tax#163,sr_fee#164,sr_return_ship_cost#165,sr_refunded_cash#166,sr_reversed_charge#167,sr_store_credit#168,sr_net_loss#169] parquet
               :                 +- Project [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, (ws_quantity#188 - coalesce(wr_return_quantity#218, 0)) AS sales_cnt#10, (ws_ext_sales_price#193 - coalesce(wr_return_amt#219, cast(0.0 as double))) AS sales_amt#11]
               :                    +- Filter (i_category#62 = Books)
               :                       +- Join LeftOuter, ((ws_order_number#187 = wr_order_number#217) && (ws_item_sk#173 = wr_item_sk#206))
               :                          :- Join Inner, (d_date_sk#72 = ws_sold_date_sk#170)
               :                          :  :- Join Inner, (i_item_sk#50 = ws_item_sk#173)
               :                          :  :  :- SubqueryAlias `tpcds`.`web_sales`
               :                          :  :  :  +- Relation[ws_sold_date_sk#170,ws_sold_time_sk#171,ws_ship_date_sk#172,ws_item_sk#173,ws_bill_customer_sk#174,ws_bill_cdemo_sk#175,ws_bill_hdemo_sk#176,ws_bill_addr_sk#177,ws_ship_customer_sk#178,ws_ship_cdemo_sk#179,ws_ship_hdemo_sk#180,ws_ship_addr_sk#181,ws_web_page_sk#182,ws_web_site_sk#183,ws_ship_mode_sk#184,ws_warehouse_sk#185,ws_promo_sk#186,ws_order_number#187,ws_quantity#188,ws_wholesale_cost#189,ws_list_price#190,ws_sales_price#191,ws_ext_discount_amt#192,ws_ext_sales_price#193,... 10 more fields] parquet
               :                          :  :  +- SubqueryAlias `tpcds`.`item`
               :                          :  :     +- Relation[i_item_sk#50,i_item_id#51,i_rec_start_date#52,i_rec_end_date#53,i_item_desc#54,i_current_price#55,i_wholesale_cost#56,i_brand_id#57,i_brand#58,i_class_id#59,i_class#60,i_category_id#61,i_category#62,i_manufact_id#63,i_manufact#64,i_size#65,i_formulation#66,i_color#67,i_units#68,i_container#69,i_manager_id#70,i_product_name#71] parquet
               :                          :  +- SubqueryAlias `tpcds`.`date_dim`
               :                          :     +- Relation[d_date_sk#72,d_date_id#73,d_date#74,d_month_seq#75,d_week_seq#76,d_quarter_seq#77,d_year#78,d_dow#79,d_moy#80,d_dom#81,d_qoy#82,d_fy_year#83,d_fy_quarter_seq#84,d_fy_week_seq#85,d_day_name#86,d_quarter_name#87,d_holiday#88,d_weekend#89,d_following_holiday#90,d_first_dom#91,d_last_dom#92,d_same_day_ly#93,d_same_day_lq#94,d_current_day#95,... 4 more fields] parquet
               :                          +- SubqueryAlias `tpcds`.`web_returns`
               :                             +- Relation[wr_returned_date_sk#204,wr_returned_time_sk#205,wr_item_sk#206,wr_refunded_customer_sk#207,wr_refunded_cdemo_sk#208,wr_refunded_hdemo_sk#209,wr_refunded_addr_sk#210,wr_returning_customer_sk#211,wr_returning_cdemo_sk#212,wr_returning_hdemo_sk#213,wr_returning_addr_sk#214,wr_web_page_sk#215,wr_reason_sk#216,wr_order_number#217,wr_return_quantity#218,wr_return_amt#219,wr_return_tax#220,wr_return_amt_inc_tax#221,wr_fee#222,wr_return_ship_cost#223,wr_refunded_cash#224,wr_reversed_charge#225,wr_account_credit#226,wr_net_loss#227] parquet
               +- SubqueryAlias `prev_yr`
                  +- SubqueryAlias `all_sales`
                     +- Aggregate [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395], [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sum(cast(sales_cnt#6 as bigint)) AS sales_cnt#230L, sum(sales_amt#7) AS sales_amt#231]
                        +- SubqueryAlias `sales_detail`
                           +- Distinct
                              +- Union
                                 :- Distinct
                                 :  +- Union
                                 :     :- Project [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, (cs_quantity#34 - coalesce(cr_return_quantity#117, 0)) AS sales_cnt#6, (cs_ext_sales_price#39 - coalesce(cr_return_amount#118, cast(0.0 as double))) AS sales_amt#7]
                                 :     :  +- Filter (i_category#394 = Books)
                                 :     :     +- Join LeftOuter, ((cs_order_number#33 = cr_order_number#116) && (cs_item_sk#31 = cr_item_sk#102))
                                 :     :        :- Join Inner, (d_date_sk#532 = cs_sold_date_sk#16)
                                 :     :        :  :- Join Inner, (i_item_sk#382 = cs_item_sk#31)
                                 :     :        :  :  :- SubqueryAlias `tpcds`.`catalog_sales`
                                 :     :        :  :  :  +- Relation[cs_sold_date_sk#16,cs_sold_time_sk#17,cs_ship_date_sk#18,cs_bill_customer_sk#19,cs_bill_cdemo_sk#20,cs_bill_hdemo_sk#21,cs_bill_addr_sk#22,cs_ship_customer_sk#23,cs_ship_cdemo_sk#24,cs_ship_hdemo_sk#25,cs_ship_addr_sk#26,cs_call_center_sk#27,cs_catalog_page_sk#28,cs_ship_mode_sk#29,cs_warehouse_sk#30,cs_item_sk#31,cs_promo_sk#32,cs_order_number#33,cs_quantity#34,cs_wholesale_cost#35,cs_list_price#36,cs_sales_price#37,cs_ext_discount_amt#38,cs_ext_sales_price#39,... 10 more fields] parquet
                                 :     :        :  :  +- SubqueryAlias `tpcds`.`item`
                                 :     :        :  :     +- Relation[i_item_sk#382,i_item_id#383,i_rec_start_date#384,i_rec_end_date#385,i_item_desc#386,i_current_price#387,i_wholesale_cost#388,i_brand_id#389,i_brand#390,i_class_id#391,i_class#392,i_category_id#393,i_category#394,i_manufact_id#395,i_manufact#396,i_size#397,i_formulation#398,i_color#399,i_units#400,i_container#401,i_manager_id#402,i_product_name#403] parquet
                                 :     :        :  +- SubqueryAlias `tpcds`.`date_dim`
                                 :     :        :     +- Relation[d_date_sk#532,d_date_id#533,d_date#534,d_month_seq#535,d_week_seq#536,d_quarter_seq#537,d_year#538,d_dow#539,d_moy#540,d_dom#541,d_qoy#542,d_fy_year#543,d_fy_quarter_seq#544,d_fy_week_seq#545,d_day_name#546,d_quarter_name#547,d_holiday#548,d_weekend#549,d_following_holiday#550,d_first_dom#551,d_last_dom#552,d_same_day_ly#553,d_same_day_lq#554,d_current_day#555,... 4 more fields] parquet
                                 :     :        +- SubqueryAlias `tpcds`.`catalog_returns`
                                 :     :           +- Relation[cr_returned_date_sk#100,cr_returned_time_sk#101,cr_item_sk#102,cr_refunded_customer_sk#103,cr_refunded_cdemo_sk#104,cr_refunded_hdemo_sk#105,cr_refunded_addr_sk#106,cr_returning_customer_sk#107,cr_returning_cdemo_sk#108,cr_returning_hdemo_sk#109,cr_returning_addr_sk#110,cr_call_center_sk#111,cr_catalog_page_sk#112,cr_ship_mode_sk#113,cr_warehouse_sk#114,cr_reason_sk#115,cr_order_number#116,cr_return_quantity#117,cr_return_amount#118,cr_return_tax#119,cr_return_amt_inc_tax#120,cr_fee#121,cr_return_ship_cost#122,cr_refunded_cash#123,... 3 more fields] parquet
                                 :     +- Project [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, (ss_quantity#137 - coalesce(sr_return_quantity#160, 0)) AS sales_cnt#8, (ss_ext_sales_price#142 - coalesce(sr_return_amt#161, cast(0.0 as double))) AS sales_amt#9]
                                 :        +- Filter (i_category#394 = Books)
                                 :           +- Join LeftOuter, ((ss_ticket_number#136 = sr_ticket_number#159) && (ss_item_sk#129 = sr_item_sk#152))
                                 :              :- Join Inner, (d_date_sk#532 = ss_sold_date_sk#127)
                                 :              :  :- Join Inner, (i_item_sk#382 = ss_item_sk#129)
                                 :              :  :  :- SubqueryAlias `tpcds`.`store_sales`
                                 :              :  :  :  +- Relation[ss_sold_date_sk#127,ss_sold_time_sk#128,ss_item_sk#129,ss_customer_sk#130,ss_cdemo_sk#131,ss_hdemo_sk#132,ss_addr_sk#133,ss_store_sk#134,ss_promo_sk#135,ss_ticket_number#136,ss_quantity#137,ss_wholesale_cost#138,ss_list_price#139,ss_sales_price#140,ss_ext_discount_amt#141,ss_ext_sales_price#142,ss_ext_wholesale_cost#143,ss_ext_list_price#144,ss_ext_tax#145,ss_coupon_amt#146,ss_net_paid#147,ss_net_paid_inc_tax#148,ss_net_profit#149] parquet
                                 :              :  :  +- SubqueryAlias `tpcds`.`item`
                                 :              :  :     +- Relation[i_item_sk#382,i_item_id#383,i_rec_start_date#384,i_rec_end_date#385,i_item_desc#386,i_current_price#387,i_wholesale_cost#388,i_brand_id#389,i_brand#390,i_class_id#391,i_class#392,i_category_id#393,i_category#394,i_manufact_id#395,i_manufact#396,i_size#397,i_formulation#398,i_color#399,i_units#400,i_container#401,i_manager_id#402,i_product_name#403] parquet
                                 :              :  +- SubqueryAlias `tpcds`.`date_dim`
                                 :              :     +- Relation[d_date_sk#532,d_date_id#533,d_date#534,d_month_seq#535,d_week_seq#536,d_quarter_seq#537,d_year#538,d_dow#539,d_moy#540,d_dom#541,d_qoy#542,d_fy_year#543,d_fy_quarter_seq#544,d_fy_week_seq#545,d_day_name#546,d_quarter_name#547,d_holiday#548,d_weekend#549,d_following_holiday#550,d_first_dom#551,d_last_dom#552,d_same_day_ly#553,d_same_day_lq#554,d_current_day#555,... 4 more fields] parquet
                                 :              +- SubqueryAlias `tpcds`.`store_returns`
                                 :                 +- Relation[sr_returned_date_sk#150,sr_return_time_sk#151,sr_item_sk#152,sr_customer_sk#153,sr_cdemo_sk#154,sr_hdemo_sk#155,sr_addr_sk#156,sr_store_sk#157,sr_reason_sk#158,sr_ticket_number#159,sr_return_quantity#160,sr_return_amt#161,sr_return_tax#162,sr_return_amt_inc_tax#163,sr_fee#164,sr_return_ship_cost#165,sr_refunded_cash#166,sr_reversed_charge#167,sr_store_credit#168,sr_net_loss#169] parquet
                                 +- Project [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, (ws_quantity#188 - coalesce(wr_return_quantity#218, 0)) AS sales_cnt#10, (ws_ext_sales_price#193 - coalesce(wr_return_amt#219, cast(0.0 as double))) AS sales_amt#11]
                                    +- Filter (i_category#394 = Books)
                                       +- Join LeftOuter, ((ws_order_number#187 = wr_order_number#217) && (ws_item_sk#173 = wr_item_sk#206))
                                          :- Join Inner, (d_date_sk#532 = ws_sold_date_sk#170)
                                          :  :- Join Inner, (i_item_sk#382 = ws_item_sk#173)
                                          :  :  :- SubqueryAlias `tpcds`.`web_sales`
                                          :  :  :  +- Relation[ws_sold_date_sk#170,ws_sold_time_sk#171,ws_ship_date_sk#172,ws_item_sk#173,ws_bill_customer_sk#174,ws_bill_cdemo_sk#175,ws_bill_hdemo_sk#176,ws_bill_addr_sk#177,ws_ship_customer_sk#178,ws_ship_cdemo_sk#179,ws_ship_hdemo_sk#180,ws_ship_addr_sk#181,ws_web_page_sk#182,ws_web_site_sk#183,ws_ship_mode_sk#184,ws_warehouse_sk#185,ws_promo_sk#186,ws_order_number#187,ws_quantity#188,ws_wholesale_cost#189,ws_list_price#190,ws_sales_price#191,ws_ext_discount_amt#192,ws_ext_sales_price#193,... 10 more fields] parquet
                                          :  :  +- SubqueryAlias `tpcds`.`item`
                                          :  :     +- Relation[i_item_sk#382,i_item_id#383,i_rec_start_date#384,i_rec_end_date#385,i_item_desc#386,i_current_price#387,i_wholesale_cost#388,i_brand_id#389,i_brand#390,i_class_id#391,i_class#392,i_category_id#393,i_category#394,i_manufact_id#395,i_manufact#396,i_size#397,i_formulation#398,i_color#399,i_units#400,i_container#401,i_manager_id#402,i_product_name#403] parquet
                                          :  +- SubqueryAlias `tpcds`.`date_dim`
                                          :     +- Relation[d_date_sk#532,d_date_id#533,d_date#534,d_month_seq#535,d_week_seq#536,d_quarter_seq#537,d_year#538,d_dow#539,d_moy#540,d_dom#541,d_qoy#542,d_fy_year#543,d_fy_quarter_seq#544,d_fy_week_seq#545,d_day_name#546,d_quarter_name#547,d_holiday#548,d_weekend#549,d_following_holiday#550,d_first_dom#551,d_last_dom#552,d_same_day_ly#553,d_same_day_lq#554,d_current_day#555,... 4 more fields] parquet
                                          +- SubqueryAlias `tpcds`.`web_returns`
                                             +- Relation[wr_returned_date_sk#204,wr_returned_time_sk#205,wr_item_sk#206,wr_refunded_customer_sk#207,wr_refunded_cdemo_sk#208,wr_refunded_hdemo_sk#209,wr_refunded_addr_sk#210,wr_returning_customer_sk#211,wr_returning_cdemo_sk#212,wr_returning_hdemo_sk#213,wr_returning_addr_sk#214,wr_web_page_sk#215,wr_reason_sk#216,wr_order_number#217,wr_return_quantity#218,wr_return_amt#219,wr_return_tax#220,wr_return_amt_inc_tax#221,wr_fee#222,wr_return_ship_cost#223,wr_refunded_cash#224,wr_reversed_charge#225,wr_account_credit#226,wr_net_loss#227] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [sales_cnt_diff#4L ASC NULLS FIRST], true
      +- Project [d_year#538 AS prev_year#0, d_year#78 AS year#1, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#230L AS prev_yr_cnt#2L, sales_cnt#12L AS curr_yr_cnt#3L, (sales_cnt#12L - sales_cnt#230L) AS sales_cnt_diff#4L, (sales_amt#13 - sales_amt#231) AS sales_amt_diff#5]
         +- Join Inner, (((((i_brand_id#57 = i_brand_id#389) && (i_class_id#59 = i_class_id#391)) && (i_category_id#61 = i_category_id#393)) && (i_manufact_id#63 = i_manufact_id#395)) && (CheckOverflow((promote_precision(cast(sales_cnt#12L as decimal(17,2))) / promote_precision(cast(sales_cnt#230L as decimal(17,2)))), DecimalType(37,20)) < 0.90000000000000000000))
            :- Aggregate [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63], [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sum(cast(sales_cnt#6 as bigint)) AS sales_cnt#12L, sum(sales_amt#7) AS sales_amt#13]
            :  +- Aggregate [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#6, sales_amt#7], [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#6, sales_amt#7]
            :     +- Union
            :        :- Project [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, (cs_quantity#34 - coalesce(cr_return_quantity#117, 0)) AS sales_cnt#6, (cs_ext_sales_price#39 - coalesce(cr_return_amount#118, 0.0)) AS sales_amt#7]
            :        :  +- Join LeftOuter, ((cs_order_number#33 = cr_order_number#116) && (cs_item_sk#31 = cr_item_sk#102))
            :        :     :- Project [cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, d_year#78]
            :        :     :  +- Join Inner, (d_date_sk#72 = cs_sold_date_sk#16)
            :        :     :     :- Project [cs_sold_date_sk#16, cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
            :        :     :     :  +- Join Inner, (i_item_sk#50 = cs_item_sk#31)
            :        :     :     :     :- Project [cs_sold_date_sk#16, cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39]
            :        :     :     :     :  +- Filter (isnotnull(cs_item_sk#31) && isnotnull(cs_sold_date_sk#16))
            :        :     :     :     :     +- Relation[cs_sold_date_sk#16,cs_sold_time_sk#17,cs_ship_date_sk#18,cs_bill_customer_sk#19,cs_bill_cdemo_sk#20,cs_bill_hdemo_sk#21,cs_bill_addr_sk#22,cs_ship_customer_sk#23,cs_ship_cdemo_sk#24,cs_ship_hdemo_sk#25,cs_ship_addr_sk#26,cs_call_center_sk#27,cs_catalog_page_sk#28,cs_ship_mode_sk#29,cs_warehouse_sk#30,cs_item_sk#31,cs_promo_sk#32,cs_order_number#33,cs_quantity#34,cs_wholesale_cost#35,cs_list_price#36,cs_sales_price#37,cs_ext_discount_amt#38,cs_ext_sales_price#39,... 10 more fields] parquet
            :        :     :     :     +- Project [i_item_sk#50, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
            :        :     :     :        +- Filter ((((((isnotnull(i_category#62) && (i_category#62 = Books)) && isnotnull(i_item_sk#50)) && isnotnull(i_manufact_id#63)) && isnotnull(i_class_id#59)) && isnotnull(i_category_id#61)) && isnotnull(i_brand_id#57))
            :        :     :     :           +- Relation[i_item_sk#50,i_item_id#51,i_rec_start_date#52,i_rec_end_date#53,i_item_desc#54,i_current_price#55,i_wholesale_cost#56,i_brand_id#57,i_brand#58,i_class_id#59,i_class#60,i_category_id#61,i_category#62,i_manufact_id#63,i_manufact#64,i_size#65,i_formulation#66,i_color#67,i_units#68,i_container#69,i_manager_id#70,i_product_name#71] parquet
            :        :     :     +- Project [d_date_sk#72, d_year#78]
            :        :     :        +- Filter ((isnotnull(d_year#78) && (d_year#78 = 2002)) && isnotnull(d_date_sk#72))
            :        :     :           +- Relation[d_date_sk#72,d_date_id#73,d_date#74,d_month_seq#75,d_week_seq#76,d_quarter_seq#77,d_year#78,d_dow#79,d_moy#80,d_dom#81,d_qoy#82,d_fy_year#83,d_fy_quarter_seq#84,d_fy_week_seq#85,d_day_name#86,d_quarter_name#87,d_holiday#88,d_weekend#89,d_following_holiday#90,d_first_dom#91,d_last_dom#92,d_same_day_ly#93,d_same_day_lq#94,d_current_day#95,... 4 more fields] parquet
            :        :     +- Project [cr_item_sk#102, cr_order_number#116, cr_return_quantity#117, cr_return_amount#118]
            :        :        +- Filter (isnotnull(cr_item_sk#102) && isnotnull(cr_order_number#116))
            :        :           +- Relation[cr_returned_date_sk#100,cr_returned_time_sk#101,cr_item_sk#102,cr_refunded_customer_sk#103,cr_refunded_cdemo_sk#104,cr_refunded_hdemo_sk#105,cr_refunded_addr_sk#106,cr_returning_customer_sk#107,cr_returning_cdemo_sk#108,cr_returning_hdemo_sk#109,cr_returning_addr_sk#110,cr_call_center_sk#111,cr_catalog_page_sk#112,cr_ship_mode_sk#113,cr_warehouse_sk#114,cr_reason_sk#115,cr_order_number#116,cr_return_quantity#117,cr_return_amount#118,cr_return_tax#119,cr_return_amt_inc_tax#120,cr_fee#121,cr_return_ship_cost#122,cr_refunded_cash#123,... 3 more fields] parquet
            :        :- Project [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, (ss_quantity#137 - coalesce(sr_return_quantity#160, 0)) AS sales_cnt#8, (ss_ext_sales_price#142 - coalesce(sr_return_amt#161, 0.0)) AS sales_amt#9]
            :        :  +- Join LeftOuter, ((ss_ticket_number#136 = sr_ticket_number#159) && (ss_item_sk#129 = sr_item_sk#152))
            :        :     :- Project [ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, d_year#78]
            :        :     :  +- Join Inner, (d_date_sk#72 = ss_sold_date_sk#127)
            :        :     :     :- Project [ss_sold_date_sk#127, ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
            :        :     :     :  +- Join Inner, (i_item_sk#50 = ss_item_sk#129)
            :        :     :     :     :- Project [ss_sold_date_sk#127, ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142]
            :        :     :     :     :  +- Filter (isnotnull(ss_item_sk#129) && isnotnull(ss_sold_date_sk#127))
            :        :     :     :     :     +- Relation[ss_sold_date_sk#127,ss_sold_time_sk#128,ss_item_sk#129,ss_customer_sk#130,ss_cdemo_sk#131,ss_hdemo_sk#132,ss_addr_sk#133,ss_store_sk#134,ss_promo_sk#135,ss_ticket_number#136,ss_quantity#137,ss_wholesale_cost#138,ss_list_price#139,ss_sales_price#140,ss_ext_discount_amt#141,ss_ext_sales_price#142,ss_ext_wholesale_cost#143,ss_ext_list_price#144,ss_ext_tax#145,ss_coupon_amt#146,ss_net_paid#147,ss_net_paid_inc_tax#148,ss_net_profit#149] parquet
            :        :     :     :     +- Project [i_item_sk#50, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
            :        :     :     :        +- Filter ((((((isnotnull(i_category#62) && (i_category#62 = Books)) && isnotnull(i_item_sk#50)) && isnotnull(i_manufact_id#63)) && isnotnull(i_class_id#59)) && isnotnull(i_category_id#61)) && isnotnull(i_brand_id#57))
            :        :     :     :           +- Relation[i_item_sk#50,i_item_id#51,i_rec_start_date#52,i_rec_end_date#53,i_item_desc#54,i_current_price#55,i_wholesale_cost#56,i_brand_id#57,i_brand#58,i_class_id#59,i_class#60,i_category_id#61,i_category#62,i_manufact_id#63,i_manufact#64,i_size#65,i_formulation#66,i_color#67,i_units#68,i_container#69,i_manager_id#70,i_product_name#71] parquet
            :        :     :     +- Project [d_date_sk#72, d_year#78]
            :        :     :        +- Filter ((isnotnull(d_year#78) && (d_year#78 = 2002)) && isnotnull(d_date_sk#72))
            :        :     :           +- Relation[d_date_sk#72,d_date_id#73,d_date#74,d_month_seq#75,d_week_seq#76,d_quarter_seq#77,d_year#78,d_dow#79,d_moy#80,d_dom#81,d_qoy#82,d_fy_year#83,d_fy_quarter_seq#84,d_fy_week_seq#85,d_day_name#86,d_quarter_name#87,d_holiday#88,d_weekend#89,d_following_holiday#90,d_first_dom#91,d_last_dom#92,d_same_day_ly#93,d_same_day_lq#94,d_current_day#95,... 4 more fields] parquet
            :        :     +- Project [sr_item_sk#152, sr_ticket_number#159, sr_return_quantity#160, sr_return_amt#161]
            :        :        +- Filter (isnotnull(sr_item_sk#152) && isnotnull(sr_ticket_number#159))
            :        :           +- Relation[sr_returned_date_sk#150,sr_return_time_sk#151,sr_item_sk#152,sr_customer_sk#153,sr_cdemo_sk#154,sr_hdemo_sk#155,sr_addr_sk#156,sr_store_sk#157,sr_reason_sk#158,sr_ticket_number#159,sr_return_quantity#160,sr_return_amt#161,sr_return_tax#162,sr_return_amt_inc_tax#163,sr_fee#164,sr_return_ship_cost#165,sr_refunded_cash#166,sr_reversed_charge#167,sr_store_credit#168,sr_net_loss#169] parquet
            :        +- Project [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, (ws_quantity#188 - coalesce(wr_return_quantity#218, 0)) AS sales_cnt#10, (ws_ext_sales_price#193 - coalesce(wr_return_amt#219, 0.0)) AS sales_amt#11]
            :           +- Join LeftOuter, ((ws_order_number#187 = wr_order_number#217) && (ws_item_sk#173 = wr_item_sk#206))
            :              :- Project [ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, d_year#78]
            :              :  +- Join Inner, (d_date_sk#72 = ws_sold_date_sk#170)
            :              :     :- Project [ws_sold_date_sk#170, ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
            :              :     :  +- Join Inner, (i_item_sk#50 = ws_item_sk#173)
            :              :     :     :- Project [ws_sold_date_sk#170, ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193]
            :              :     :     :  +- Filter (isnotnull(ws_item_sk#173) && isnotnull(ws_sold_date_sk#170))
            :              :     :     :     +- Relation[ws_sold_date_sk#170,ws_sold_time_sk#171,ws_ship_date_sk#172,ws_item_sk#173,ws_bill_customer_sk#174,ws_bill_cdemo_sk#175,ws_bill_hdemo_sk#176,ws_bill_addr_sk#177,ws_ship_customer_sk#178,ws_ship_cdemo_sk#179,ws_ship_hdemo_sk#180,ws_ship_addr_sk#181,ws_web_page_sk#182,ws_web_site_sk#183,ws_ship_mode_sk#184,ws_warehouse_sk#185,ws_promo_sk#186,ws_order_number#187,ws_quantity#188,ws_wholesale_cost#189,ws_list_price#190,ws_sales_price#191,ws_ext_discount_amt#192,ws_ext_sales_price#193,... 10 more fields] parquet
            :              :     :     +- Project [i_item_sk#50, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
            :              :     :        +- Filter ((((((isnotnull(i_category#62) && (i_category#62 = Books)) && isnotnull(i_item_sk#50)) && isnotnull(i_manufact_id#63)) && isnotnull(i_class_id#59)) && isnotnull(i_category_id#61)) && isnotnull(i_brand_id#57))
            :              :     :           +- Relation[i_item_sk#50,i_item_id#51,i_rec_start_date#52,i_rec_end_date#53,i_item_desc#54,i_current_price#55,i_wholesale_cost#56,i_brand_id#57,i_brand#58,i_class_id#59,i_class#60,i_category_id#61,i_category#62,i_manufact_id#63,i_manufact#64,i_size#65,i_formulation#66,i_color#67,i_units#68,i_container#69,i_manager_id#70,i_product_name#71] parquet
            :              :     +- Project [d_date_sk#72, d_year#78]
            :              :        +- Filter ((isnotnull(d_year#78) && (d_year#78 = 2002)) && isnotnull(d_date_sk#72))
            :              :           +- Relation[d_date_sk#72,d_date_id#73,d_date#74,d_month_seq#75,d_week_seq#76,d_quarter_seq#77,d_year#78,d_dow#79,d_moy#80,d_dom#81,d_qoy#82,d_fy_year#83,d_fy_quarter_seq#84,d_fy_week_seq#85,d_day_name#86,d_quarter_name#87,d_holiday#88,d_weekend#89,d_following_holiday#90,d_first_dom#91,d_last_dom#92,d_same_day_ly#93,d_same_day_lq#94,d_current_day#95,... 4 more fields] parquet
            :              +- Project [wr_item_sk#206, wr_order_number#217, wr_return_quantity#218, wr_return_amt#219]
            :                 +- Filter (isnotnull(wr_item_sk#206) && isnotnull(wr_order_number#217))
            :                    +- Relation[wr_returned_date_sk#204,wr_returned_time_sk#205,wr_item_sk#206,wr_refunded_customer_sk#207,wr_refunded_cdemo_sk#208,wr_refunded_hdemo_sk#209,wr_refunded_addr_sk#210,wr_returning_customer_sk#211,wr_returning_cdemo_sk#212,wr_returning_hdemo_sk#213,wr_returning_addr_sk#214,wr_web_page_sk#215,wr_reason_sk#216,wr_order_number#217,wr_return_quantity#218,wr_return_amt#219,wr_return_tax#220,wr_return_amt_inc_tax#221,wr_fee#222,wr_return_ship_cost#223,wr_refunded_cash#224,wr_reversed_charge#225,wr_account_credit#226,wr_net_loss#227] parquet
            +- Aggregate [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395], [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sum(cast(sales_cnt#6 as bigint)) AS sales_cnt#230L, sum(sales_amt#7) AS sales_amt#231]
               +- Aggregate [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sales_cnt#6, sales_amt#7], [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sales_cnt#6, sales_amt#7]
                  +- Union
                     :- Project [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, (cs_quantity#34 - coalesce(cr_return_quantity#117, 0)) AS sales_cnt#6, (cs_ext_sales_price#39 - coalesce(cr_return_amount#118, 0.0)) AS sales_amt#7]
                     :  +- Join LeftOuter, ((cs_order_number#33 = cr_order_number#116) && (cs_item_sk#31 = cr_item_sk#102))
                     :     :- Project [cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, d_year#538]
                     :     :  +- Join Inner, (d_date_sk#532 = cs_sold_date_sk#16)
                     :     :     :- Project [cs_sold_date_sk#16, cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395]
                     :     :     :  +- Join Inner, (i_item_sk#382 = cs_item_sk#31)
                     :     :     :     :- Project [cs_sold_date_sk#16, cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39]
                     :     :     :     :  +- Filter (isnotnull(cs_item_sk#31) && isnotnull(cs_sold_date_sk#16))
                     :     :     :     :     +- Relation[cs_sold_date_sk#16,cs_sold_time_sk#17,cs_ship_date_sk#18,cs_bill_customer_sk#19,cs_bill_cdemo_sk#20,cs_bill_hdemo_sk#21,cs_bill_addr_sk#22,cs_ship_customer_sk#23,cs_ship_cdemo_sk#24,cs_ship_hdemo_sk#25,cs_ship_addr_sk#26,cs_call_center_sk#27,cs_catalog_page_sk#28,cs_ship_mode_sk#29,cs_warehouse_sk#30,cs_item_sk#31,cs_promo_sk#32,cs_order_number#33,cs_quantity#34,cs_wholesale_cost#35,cs_list_price#36,cs_sales_price#37,cs_ext_discount_amt#38,cs_ext_sales_price#39,... 10 more fields] parquet
                     :     :     :     +- Project [i_item_sk#382, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395]
                     :     :     :        +- Filter ((((((isnotnull(i_category#394) && (i_category#394 = Books)) && isnotnull(i_item_sk#382)) && isnotnull(i_manufact_id#395)) && isnotnull(i_category_id#393)) && isnotnull(i_brand_id#389)) && isnotnull(i_class_id#391))
                     :     :     :           +- Relation[i_item_sk#382,i_item_id#383,i_rec_start_date#384,i_rec_end_date#385,i_item_desc#386,i_current_price#387,i_wholesale_cost#388,i_brand_id#389,i_brand#390,i_class_id#391,i_class#392,i_category_id#393,i_category#394,i_manufact_id#395,i_manufact#396,i_size#397,i_formulation#398,i_color#399,i_units#400,i_container#401,i_manager_id#402,i_product_name#403] parquet
                     :     :     +- Project [d_date_sk#532, d_year#538]
                     :     :        +- Filter ((isnotnull(d_year#538) && (d_year#538 = 2001)) && isnotnull(d_date_sk#532))
                     :     :           +- Relation[d_date_sk#532,d_date_id#533,d_date#534,d_month_seq#535,d_week_seq#536,d_quarter_seq#537,d_year#538,d_dow#539,d_moy#540,d_dom#541,d_qoy#542,d_fy_year#543,d_fy_quarter_seq#544,d_fy_week_seq#545,d_day_name#546,d_quarter_name#547,d_holiday#548,d_weekend#549,d_following_holiday#550,d_first_dom#551,d_last_dom#552,d_same_day_ly#553,d_same_day_lq#554,d_current_day#555,... 4 more fields] parquet
                     :     +- Project [cr_item_sk#102, cr_order_number#116, cr_return_quantity#117, cr_return_amount#118]
                     :        +- Filter (isnotnull(cr_item_sk#102) && isnotnull(cr_order_number#116))
                     :           +- Relation[cr_returned_date_sk#100,cr_returned_time_sk#101,cr_item_sk#102,cr_refunded_customer_sk#103,cr_refunded_cdemo_sk#104,cr_refunded_hdemo_sk#105,cr_refunded_addr_sk#106,cr_returning_customer_sk#107,cr_returning_cdemo_sk#108,cr_returning_hdemo_sk#109,cr_returning_addr_sk#110,cr_call_center_sk#111,cr_catalog_page_sk#112,cr_ship_mode_sk#113,cr_warehouse_sk#114,cr_reason_sk#115,cr_order_number#116,cr_return_quantity#117,cr_return_amount#118,cr_return_tax#119,cr_return_amt_inc_tax#120,cr_fee#121,cr_return_ship_cost#122,cr_refunded_cash#123,... 3 more fields] parquet
                     :- Project [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, (ss_quantity#137 - coalesce(sr_return_quantity#160, 0)) AS sales_cnt#8, (ss_ext_sales_price#142 - coalesce(sr_return_amt#161, 0.0)) AS sales_amt#9]
                     :  +- Join LeftOuter, ((ss_ticket_number#136 = sr_ticket_number#159) && (ss_item_sk#129 = sr_item_sk#152))
                     :     :- Project [ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, d_year#538]
                     :     :  +- Join Inner, (d_date_sk#532 = ss_sold_date_sk#127)
                     :     :     :- Project [ss_sold_date_sk#127, ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395]
                     :     :     :  +- Join Inner, (i_item_sk#382 = ss_item_sk#129)
                     :     :     :     :- Project [ss_sold_date_sk#127, ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142]
                     :     :     :     :  +- Filter (isnotnull(ss_item_sk#129) && isnotnull(ss_sold_date_sk#127))
                     :     :     :     :     +- Relation[ss_sold_date_sk#127,ss_sold_time_sk#128,ss_item_sk#129,ss_customer_sk#130,ss_cdemo_sk#131,ss_hdemo_sk#132,ss_addr_sk#133,ss_store_sk#134,ss_promo_sk#135,ss_ticket_number#136,ss_quantity#137,ss_wholesale_cost#138,ss_list_price#139,ss_sales_price#140,ss_ext_discount_amt#141,ss_ext_sales_price#142,ss_ext_wholesale_cost#143,ss_ext_list_price#144,ss_ext_tax#145,ss_coupon_amt#146,ss_net_paid#147,ss_net_paid_inc_tax#148,ss_net_profit#149] parquet
                     :     :     :     +- Project [i_item_sk#382, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395]
                     :     :     :        +- Filter ((((((isnotnull(i_category#394) && (i_category#394 = Books)) && isnotnull(i_item_sk#382)) && isnotnull(i_manufact_id#395)) && isnotnull(i_category_id#393)) && isnotnull(i_brand_id#389)) && isnotnull(i_class_id#391))
                     :     :     :           +- Relation[i_item_sk#382,i_item_id#383,i_rec_start_date#384,i_rec_end_date#385,i_item_desc#386,i_current_price#387,i_wholesale_cost#388,i_brand_id#389,i_brand#390,i_class_id#391,i_class#392,i_category_id#393,i_category#394,i_manufact_id#395,i_manufact#396,i_size#397,i_formulation#398,i_color#399,i_units#400,i_container#401,i_manager_id#402,i_product_name#403] parquet
                     :     :     +- Project [d_date_sk#532, d_year#538]
                     :     :        +- Filter ((isnotnull(d_year#538) && (d_year#538 = 2001)) && isnotnull(d_date_sk#532))
                     :     :           +- Relation[d_date_sk#532,d_date_id#533,d_date#534,d_month_seq#535,d_week_seq#536,d_quarter_seq#537,d_year#538,d_dow#539,d_moy#540,d_dom#541,d_qoy#542,d_fy_year#543,d_fy_quarter_seq#544,d_fy_week_seq#545,d_day_name#546,d_quarter_name#547,d_holiday#548,d_weekend#549,d_following_holiday#550,d_first_dom#551,d_last_dom#552,d_same_day_ly#553,d_same_day_lq#554,d_current_day#555,... 4 more fields] parquet
                     :     +- Project [sr_item_sk#152, sr_ticket_number#159, sr_return_quantity#160, sr_return_amt#161]
                     :        +- Filter (isnotnull(sr_item_sk#152) && isnotnull(sr_ticket_number#159))
                     :           +- Relation[sr_returned_date_sk#150,sr_return_time_sk#151,sr_item_sk#152,sr_customer_sk#153,sr_cdemo_sk#154,sr_hdemo_sk#155,sr_addr_sk#156,sr_store_sk#157,sr_reason_sk#158,sr_ticket_number#159,sr_return_quantity#160,sr_return_amt#161,sr_return_tax#162,sr_return_amt_inc_tax#163,sr_fee#164,sr_return_ship_cost#165,sr_refunded_cash#166,sr_reversed_charge#167,sr_store_credit#168,sr_net_loss#169] parquet
                     +- Project [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, (ws_quantity#188 - coalesce(wr_return_quantity#218, 0)) AS sales_cnt#10, (ws_ext_sales_price#193 - coalesce(wr_return_amt#219, 0.0)) AS sales_amt#11]
                        +- Join LeftOuter, ((ws_order_number#187 = wr_order_number#217) && (ws_item_sk#173 = wr_item_sk#206))
                           :- Project [ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, d_year#538]
                           :  +- Join Inner, (d_date_sk#532 = ws_sold_date_sk#170)
                           :     :- Project [ws_sold_date_sk#170, ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395]
                           :     :  +- Join Inner, (i_item_sk#382 = ws_item_sk#173)
                           :     :     :- Project [ws_sold_date_sk#170, ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193]
                           :     :     :  +- Filter (isnotnull(ws_item_sk#173) && isnotnull(ws_sold_date_sk#170))
                           :     :     :     +- Relation[ws_sold_date_sk#170,ws_sold_time_sk#171,ws_ship_date_sk#172,ws_item_sk#173,ws_bill_customer_sk#174,ws_bill_cdemo_sk#175,ws_bill_hdemo_sk#176,ws_bill_addr_sk#177,ws_ship_customer_sk#178,ws_ship_cdemo_sk#179,ws_ship_hdemo_sk#180,ws_ship_addr_sk#181,ws_web_page_sk#182,ws_web_site_sk#183,ws_ship_mode_sk#184,ws_warehouse_sk#185,ws_promo_sk#186,ws_order_number#187,ws_quantity#188,ws_wholesale_cost#189,ws_list_price#190,ws_sales_price#191,ws_ext_discount_amt#192,ws_ext_sales_price#193,... 10 more fields] parquet
                           :     :     +- Project [i_item_sk#382, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395]
                           :     :        +- Filter ((((((isnotnull(i_category#394) && (i_category#394 = Books)) && isnotnull(i_item_sk#382)) && isnotnull(i_manufact_id#395)) && isnotnull(i_category_id#393)) && isnotnull(i_brand_id#389)) && isnotnull(i_class_id#391))
                           :     :           +- Relation[i_item_sk#382,i_item_id#383,i_rec_start_date#384,i_rec_end_date#385,i_item_desc#386,i_current_price#387,i_wholesale_cost#388,i_brand_id#389,i_brand#390,i_class_id#391,i_class#392,i_category_id#393,i_category#394,i_manufact_id#395,i_manufact#396,i_size#397,i_formulation#398,i_color#399,i_units#400,i_container#401,i_manager_id#402,i_product_name#403] parquet
                           :     +- Project [d_date_sk#532, d_year#538]
                           :        +- Filter ((isnotnull(d_year#538) && (d_year#538 = 2001)) && isnotnull(d_date_sk#532))
                           :           +- Relation[d_date_sk#532,d_date_id#533,d_date#534,d_month_seq#535,d_week_seq#536,d_quarter_seq#537,d_year#538,d_dow#539,d_moy#540,d_dom#541,d_qoy#542,d_fy_year#543,d_fy_quarter_seq#544,d_fy_week_seq#545,d_day_name#546,d_quarter_name#547,d_holiday#548,d_weekend#549,d_following_holiday#550,d_first_dom#551,d_last_dom#552,d_same_day_ly#553,d_same_day_lq#554,d_current_day#555,... 4 more fields] parquet
                           +- Project [wr_item_sk#206, wr_order_number#217, wr_return_quantity#218, wr_return_amt#219]
                              +- Filter (isnotnull(wr_item_sk#206) && isnotnull(wr_order_number#217))
                                 +- Relation[wr_returned_date_sk#204,wr_returned_time_sk#205,wr_item_sk#206,wr_refunded_customer_sk#207,wr_refunded_cdemo_sk#208,wr_refunded_hdemo_sk#209,wr_refunded_addr_sk#210,wr_returning_customer_sk#211,wr_returning_cdemo_sk#212,wr_returning_hdemo_sk#213,wr_returning_addr_sk#214,wr_web_page_sk#215,wr_reason_sk#216,wr_order_number#217,wr_return_quantity#218,wr_return_amt#219,wr_return_tax#220,wr_return_amt_inc_tax#221,wr_fee#222,wr_return_ship_cost#223,wr_refunded_cash#224,wr_reversed_charge#225,wr_account_credit#226,wr_net_loss#227] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[sales_cnt_diff#4L ASC NULLS FIRST], output=[prev_year#0,year#1,i_brand_id#57,i_class_id#59,i_category_id#61,i_manufact_id#63,prev_yr_cnt#2L,curr_yr_cnt#3L,sales_cnt_diff#4L,sales_amt_diff#5])
+- *(33) Project [d_year#538 AS prev_year#0, d_year#78 AS year#1, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#230L AS prev_yr_cnt#2L, sales_cnt#12L AS curr_yr_cnt#3L, (sales_cnt#12L - sales_cnt#230L) AS sales_cnt_diff#4L, (sales_amt#13 - sales_amt#231) AS sales_amt_diff#5]
   +- *(33) SortMergeJoin [i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63], [i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395], Inner, (CheckOverflow((promote_precision(cast(sales_cnt#12L as decimal(17,2))) / promote_precision(cast(sales_cnt#230L as decimal(17,2)))), DecimalType(37,20)) < 0.90000000000000000000)
      :- *(16) Sort [i_brand_id#57 ASC NULLS FIRST, i_class_id#59 ASC NULLS FIRST, i_category_id#61 ASC NULLS FIRST, i_manufact_id#63 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, 200)
      :     +- *(15) HashAggregate(keys=[d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63], functions=[sum(cast(sales_cnt#6 as bigint)), sum(sales_amt#7)], output=[d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#12L, sales_amt#13])
      :        +- Exchange hashpartitioning(d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, 200)
      :           +- *(14) HashAggregate(keys=[d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63], functions=[partial_sum(cast(sales_cnt#6 as bigint)), partial_sum(sales_amt#7)], output=[d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sum#618L, sum#619])
      :              +- *(14) HashAggregate(keys=[d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#6, sales_amt#7], functions=[], output=[d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#6, sales_amt#7])
      :                 +- Exchange hashpartitioning(d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#6, sales_amt#7, 200)
      :                    +- *(13) HashAggregate(keys=[d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#6, sales_amt#7], functions=[], output=[d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, sales_cnt#6, sales_amt#7])
      :                       +- Union
      :                          :- *(4) Project [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, (cs_quantity#34 - coalesce(cr_return_quantity#117, 0)) AS sales_cnt#6, (cs_ext_sales_price#39 - coalesce(cr_return_amount#118, 0.0)) AS sales_amt#7]
      :                          :  +- *(4) BroadcastHashJoin [cs_order_number#33, cs_item_sk#31], [cr_order_number#116, cr_item_sk#102], LeftOuter, BuildRight
      :                          :     :- *(4) Project [cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, d_year#78]
      :                          :     :  +- *(4) BroadcastHashJoin [cs_sold_date_sk#16], [d_date_sk#72], Inner, BuildRight
      :                          :     :     :- *(4) Project [cs_sold_date_sk#16, cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
      :                          :     :     :  +- *(4) BroadcastHashJoin [cs_item_sk#31], [i_item_sk#50], Inner, BuildRight
      :                          :     :     :     :- *(4) Project [cs_sold_date_sk#16, cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39]
      :                          :     :     :     :  +- *(4) Filter (isnotnull(cs_item_sk#31) && isnotnull(cs_sold_date_sk#16))
      :                          :     :     :     :     +- *(4) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#16,cs_item_sk#31,cs_order_number#33,cs_quantity#34,cs_ext_sales_price#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_order_number:int,cs_quantity:int,cs_ext_sales_price:...
      :                          :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                          :     :     :        +- *(1) Project [i_item_sk#50, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
      :                          :     :     :           +- *(1) Filter ((((((isnotnull(i_category#62) && (i_category#62 = Books)) && isnotnull(i_item_sk#50)) && isnotnull(i_manufact_id#63)) && isnotnull(i_class_id#59)) && isnotnull(i_category_id#61)) && isnotnull(i_brand_id#57))
      :                          :     :     :              +- *(1) FileScan parquet tpcds.item[i_item_sk#50,i_brand_id#57,i_class_id#59,i_category_id#61,i_category#62,i_manufact_id#63] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category), EqualTo(i_category,Books), IsNotNull(i_item_sk), IsNotNull(i_manufact_id)..., ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int,i_category:string,i_manufact...
      :                          :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                          :     :        +- *(2) Project [d_date_sk#72, d_year#78]
      :                          :     :           +- *(2) Filter ((isnotnull(d_year#78) && (d_year#78 = 2002)) && isnotnull(d_date_sk#72))
      :                          :     :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#72,d_year#78] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
      :                          :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
      :                          :        +- *(3) Project [cr_item_sk#102, cr_order_number#116, cr_return_quantity#117, cr_return_amount#118]
      :                          :           +- *(3) Filter (isnotnull(cr_item_sk#102) && isnotnull(cr_order_number#116))
      :                          :              +- *(3) FileScan parquet tpcds.catalog_returns[cr_item_sk#102,cr_order_number#116,cr_return_quantity#117,cr_return_amount#118] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_item_sk), IsNotNull(cr_order_number)], ReadSchema: struct<cr_item_sk:int,cr_order_number:int,cr_return_quantity:int,cr_return_amount:double>
      :                          :- *(8) Project [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, (ss_quantity#137 - coalesce(sr_return_quantity#160, 0)) AS sales_cnt#8, (ss_ext_sales_price#142 - coalesce(sr_return_amt#161, 0.0)) AS sales_amt#9]
      :                          :  +- *(8) BroadcastHashJoin [ss_ticket_number#136, ss_item_sk#129], [sr_ticket_number#159, sr_item_sk#152], LeftOuter, BuildRight
      :                          :     :- *(8) Project [ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, d_year#78]
      :                          :     :  +- *(8) BroadcastHashJoin [ss_sold_date_sk#127], [d_date_sk#72], Inner, BuildRight
      :                          :     :     :- *(8) Project [ss_sold_date_sk#127, ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
      :                          :     :     :  +- *(8) BroadcastHashJoin [ss_item_sk#129], [i_item_sk#50], Inner, BuildRight
      :                          :     :     :     :- *(8) Project [ss_sold_date_sk#127, ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142]
      :                          :     :     :     :  +- *(8) Filter (isnotnull(ss_item_sk#129) && isnotnull(ss_sold_date_sk#127))
      :                          :     :     :     :     +- *(8) FileScan parquet tpcds.store_sales[ss_sold_date_sk#127,ss_item_sk#129,ss_ticket_number#136,ss_quantity#137,ss_ext_sales_price#142] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ticket_number:int,ss_quantity:int,ss_ext_sales_price...
      :                          :     :     :     +- ReusedExchange [i_item_sk#50, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                          :     :     +- ReusedExchange [d_date_sk#72, d_year#78], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                          :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
      :                          :        +- *(7) Project [sr_item_sk#152, sr_ticket_number#159, sr_return_quantity#160, sr_return_amt#161]
      :                          :           +- *(7) Filter (isnotnull(sr_item_sk#152) && isnotnull(sr_ticket_number#159))
      :                          :              +- *(7) FileScan parquet tpcds.store_returns[sr_item_sk#152,sr_ticket_number#159,sr_return_quantity#160,sr_return_amt#161] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_ticket_number)], ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int,sr_return_quantity:int,sr_return_amt:double>
      :                          +- *(12) Project [d_year#78, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, (ws_quantity#188 - coalesce(wr_return_quantity#218, 0)) AS sales_cnt#10, (ws_ext_sales_price#193 - coalesce(wr_return_amt#219, 0.0)) AS sales_amt#11]
      :                             +- *(12) BroadcastHashJoin [ws_order_number#187, ws_item_sk#173], [wr_order_number#217, wr_item_sk#206], LeftOuter, BuildRight
      :                                :- *(12) Project [ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63, d_year#78]
      :                                :  +- *(12) BroadcastHashJoin [ws_sold_date_sk#170], [d_date_sk#72], Inner, BuildRight
      :                                :     :- *(12) Project [ws_sold_date_sk#170, ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63]
      :                                :     :  +- *(12) BroadcastHashJoin [ws_item_sk#173], [i_item_sk#50], Inner, BuildRight
      :                                :     :     :- *(12) Project [ws_sold_date_sk#170, ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193]
      :                                :     :     :  +- *(12) Filter (isnotnull(ws_item_sk#173) && isnotnull(ws_sold_date_sk#170))
      :                                :     :     :     +- *(12) FileScan parquet tpcds.web_sales[ws_sold_date_sk#170,ws_item_sk#173,ws_order_number#187,ws_quantity#188,ws_ext_sales_price#193] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_order_number:int,ws_quantity:int,ws_ext_sales_price:...
      :                                :     :     +- ReusedExchange [i_item_sk#50, i_brand_id#57, i_class_id#59, i_category_id#61, i_manufact_id#63], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                                :     +- ReusedExchange [d_date_sk#72, d_year#78], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :                                +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
      :                                   +- *(11) Project [wr_item_sk#206, wr_order_number#217, wr_return_quantity#218, wr_return_amt#219]
      :                                      +- *(11) Filter (isnotnull(wr_item_sk#206) && isnotnull(wr_order_number#217))
      :                                         +- *(11) FileScan parquet tpcds.web_returns[wr_item_sk#206,wr_order_number#217,wr_return_quantity#218,wr_return_amt#219] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_item_sk), IsNotNull(wr_order_number)], ReadSchema: struct<wr_item_sk:int,wr_order_number:int,wr_return_quantity:int,wr_return_amt:double>
      +- *(32) Sort [i_brand_id#389 ASC NULLS FIRST, i_class_id#391 ASC NULLS FIRST, i_category_id#393 ASC NULLS FIRST, i_manufact_id#395 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, 200)
            +- *(31) HashAggregate(keys=[d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395], functions=[sum(cast(sales_cnt#6 as bigint)), sum(sales_amt#7)], output=[d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sales_cnt#230L, sales_amt#231])
               +- Exchange hashpartitioning(d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, 200)
                  +- *(30) HashAggregate(keys=[d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395], functions=[partial_sum(cast(sales_cnt#6 as bigint)), partial_sum(sales_amt#7)], output=[d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sum#618L, sum#619])
                     +- *(30) HashAggregate(keys=[d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sales_cnt#6, sales_amt#7], functions=[], output=[d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sales_cnt#6, sales_amt#7])
                        +- Exchange hashpartitioning(d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sales_cnt#6, sales_amt#7, 200)
                           +- *(29) HashAggregate(keys=[d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sales_cnt#6, sales_amt#7], functions=[], output=[d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, sales_cnt#6, sales_amt#7])
                              +- Union
                                 :- *(20) Project [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, (cs_quantity#34 - coalesce(cr_return_quantity#117, 0)) AS sales_cnt#6, (cs_ext_sales_price#39 - coalesce(cr_return_amount#118, 0.0)) AS sales_amt#7]
                                 :  +- *(20) BroadcastHashJoin [cs_order_number#33, cs_item_sk#31], [cr_order_number#116, cr_item_sk#102], LeftOuter, BuildRight
                                 :     :- *(20) Project [cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, d_year#538]
                                 :     :  +- *(20) BroadcastHashJoin [cs_sold_date_sk#16], [d_date_sk#532], Inner, BuildRight
                                 :     :     :- *(20) Project [cs_sold_date_sk#16, cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395]
                                 :     :     :  +- *(20) BroadcastHashJoin [cs_item_sk#31], [i_item_sk#382], Inner, BuildRight
                                 :     :     :     :- *(20) Project [cs_sold_date_sk#16, cs_item_sk#31, cs_order_number#33, cs_quantity#34, cs_ext_sales_price#39]
                                 :     :     :     :  +- *(20) Filter (isnotnull(cs_item_sk#31) && isnotnull(cs_sold_date_sk#16))
                                 :     :     :     :     +- *(20) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#16,cs_item_sk#31,cs_order_number#33,cs_quantity#34,cs_ext_sales_price#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_order_number:int,cs_quantity:int,cs_ext_sales_price:...
                                 :     :     :     +- ReusedExchange [i_item_sk#382, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 :     :        +- *(18) Project [d_date_sk#532, d_year#538]
                                 :     :           +- *(18) Filter ((isnotnull(d_year#538) && (d_year#538 = 2001)) && isnotnull(d_date_sk#532))
                                 :     :              +- *(18) FileScan parquet tpcds.date_dim[d_date_sk#532,d_year#538] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
                                 :     +- ReusedExchange [cr_item_sk#102, cr_order_number#116, cr_return_quantity#117, cr_return_amount#118], BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
                                 :- *(24) Project [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, (ss_quantity#137 - coalesce(sr_return_quantity#160, 0)) AS sales_cnt#8, (ss_ext_sales_price#142 - coalesce(sr_return_amt#161, 0.0)) AS sales_amt#9]
                                 :  +- *(24) BroadcastHashJoin [ss_ticket_number#136, ss_item_sk#129], [sr_ticket_number#159, sr_item_sk#152], LeftOuter, BuildRight
                                 :     :- *(24) Project [ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, d_year#538]
                                 :     :  +- *(24) BroadcastHashJoin [ss_sold_date_sk#127], [d_date_sk#532], Inner, BuildRight
                                 :     :     :- *(24) Project [ss_sold_date_sk#127, ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395]
                                 :     :     :  +- *(24) BroadcastHashJoin [ss_item_sk#129], [i_item_sk#382], Inner, BuildRight
                                 :     :     :     :- *(24) Project [ss_sold_date_sk#127, ss_item_sk#129, ss_ticket_number#136, ss_quantity#137, ss_ext_sales_price#142]
                                 :     :     :     :  +- *(24) Filter (isnotnull(ss_item_sk#129) && isnotnull(ss_sold_date_sk#127))
                                 :     :     :     :     +- *(24) FileScan parquet tpcds.store_sales[ss_sold_date_sk#127,ss_item_sk#129,ss_ticket_number#136,ss_quantity#137,ss_ext_sales_price#142] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_ticket_number:int,ss_quantity:int,ss_ext_sales_price...
                                 :     :     :     +- ReusedExchange [i_item_sk#382, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 :     :     +- ReusedExchange [d_date_sk#532, d_year#538], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 :     +- ReusedExchange [sr_item_sk#152, sr_ticket_number#159, sr_return_quantity#160, sr_return_amt#161], BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
                                 +- *(28) Project [d_year#538, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, (ws_quantity#188 - coalesce(wr_return_quantity#218, 0)) AS sales_cnt#10, (ws_ext_sales_price#193 - coalesce(wr_return_amt#219, 0.0)) AS sales_amt#11]
                                    +- *(28) BroadcastHashJoin [ws_order_number#187, ws_item_sk#173], [wr_order_number#217, wr_item_sk#206], LeftOuter, BuildRight
                                       :- *(28) Project [ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395, d_year#538]
                                       :  +- *(28) BroadcastHashJoin [ws_sold_date_sk#170], [d_date_sk#532], Inner, BuildRight
                                       :     :- *(28) Project [ws_sold_date_sk#170, ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395]
                                       :     :  +- *(28) BroadcastHashJoin [ws_item_sk#173], [i_item_sk#382], Inner, BuildRight
                                       :     :     :- *(28) Project [ws_sold_date_sk#170, ws_item_sk#173, ws_order_number#187, ws_quantity#188, ws_ext_sales_price#193]
                                       :     :     :  +- *(28) Filter (isnotnull(ws_item_sk#173) && isnotnull(ws_sold_date_sk#170))
                                       :     :     :     +- *(28) FileScan parquet tpcds.web_sales[ws_sold_date_sk#170,ws_item_sk#173,ws_order_number#187,ws_quantity#188,ws_ext_sales_price#193] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_order_number:int,ws_quantity:int,ws_ext_sales_price:...
                                       :     :     +- ReusedExchange [i_item_sk#382, i_brand_id#389, i_class_id#391, i_category_id#393, i_manufact_id#395], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                       :     +- ReusedExchange [d_date_sk#532, d_year#538], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                       +- ReusedExchange [wr_item_sk#206, wr_order_number#217, wr_return_quantity#218, wr_return_amt#219], BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
Time taken: 6.041 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 75 in stream 0 using template query75.tpl
------------------------------------------------------^^^

