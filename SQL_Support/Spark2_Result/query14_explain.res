== Parsed Logical Plan ==
CTE [cross_items, avg_sales]
:  :- 'SubqueryAlias `cross_items`
:  :  +- 'Project ['i_item_sk AS ss_item_sk#18]
:  :     +- 'Filter ((('i_brand_id = 'brand_id) && ('i_class_id = 'class_id)) && ('i_category_id = 'category_id))
:  :        +- 'Join Inner
:  :           :- 'UnresolvedRelation `item`
:  :           +- 'SubqueryAlias `__auto_generated_subquery_name`
:  :              +- 'Intersect false
:  :                 :- 'Intersect false
:  :                 :  :- 'Project ['iss.i_brand_id AS brand_id#15, 'iss.i_class_id AS class_id#16, 'iss.i_category_id AS category_id#17]
:  :                 :  :  +- 'Filter ((('ss_item_sk = 'iss.i_item_sk) && ('ss_sold_date_sk = 'd1.d_date_sk)) && (('d1.d_year >= 1999) && ('d1.d_year <= (1999 + 2))))
:  :                 :  :     +- 'Join Inner
:  :                 :  :        :- 'Join Inner
:  :                 :  :        :  :- 'UnresolvedRelation `store_sales`
:  :                 :  :        :  +- 'SubqueryAlias `iss`
:  :                 :  :        :     +- 'UnresolvedRelation `item`
:  :                 :  :        +- 'SubqueryAlias `d1`
:  :                 :  :           +- 'UnresolvedRelation `date_dim`
:  :                 :  +- 'Project ['ics.i_brand_id, 'ics.i_class_id, 'ics.i_category_id]
:  :                 :     +- 'Filter ((('cs_item_sk = 'ics.i_item_sk) && ('cs_sold_date_sk = 'd2.d_date_sk)) && (('d2.d_year >= 1999) && ('d2.d_year <= (1999 + 2))))
:  :                 :        +- 'Join Inner
:  :                 :           :- 'Join Inner
:  :                 :           :  :- 'UnresolvedRelation `catalog_sales`
:  :                 :           :  +- 'SubqueryAlias `ics`
:  :                 :           :     +- 'UnresolvedRelation `item`
:  :                 :           +- 'SubqueryAlias `d2`
:  :                 :              +- 'UnresolvedRelation `date_dim`
:  :                 +- 'Project ['iws.i_brand_id, 'iws.i_class_id, 'iws.i_category_id]
:  :                    +- 'Filter ((('ws_item_sk = 'iws.i_item_sk) && ('ws_sold_date_sk = 'd3.d_date_sk)) && (('d3.d_year >= 1999) && ('d3.d_year <= (1999 + 2))))
:  :                       +- 'Join Inner
:  :                          :- 'Join Inner
:  :                          :  :- 'UnresolvedRelation `web_sales`
:  :                          :  +- 'SubqueryAlias `iws`
:  :                          :     +- 'UnresolvedRelation `item`
:  :                          +- 'SubqueryAlias `d3`
:  :                             +- 'UnresolvedRelation `date_dim`
:  +- 'SubqueryAlias `avg_sales`
:     +- 'Project ['avg(('quantity * 'list_price)) AS average_sales#25]
:        +- 'SubqueryAlias `x`
:           +- 'Union
:              :- 'Union
:              :  :- 'Project ['ss_quantity AS quantity#19, 'ss_list_price AS list_price#20]
:              :  :  +- 'Filter (('ss_sold_date_sk = 'd_date_sk) && (('d_year >= 1999) && ('d_year <= (1999 + 2))))
:              :  :     +- 'Join Inner
:              :  :        :- 'UnresolvedRelation `store_sales`
:              :  :        +- 'UnresolvedRelation `date_dim`
:              :  +- 'Project ['cs_quantity AS quantity#21, 'cs_list_price AS list_price#22]
:              :     +- 'Filter (('cs_sold_date_sk = 'd_date_sk) && (('d_year >= 1999) && ('d_year <= (1999 + 2))))
:              :        +- 'Join Inner
:              :           :- 'UnresolvedRelation `catalog_sales`
:              :           +- 'UnresolvedRelation `date_dim`
:              +- 'Project ['ws_quantity AS quantity#23, 'ws_list_price AS list_price#24]
:                 +- 'Filter (('ws_sold_date_sk = 'd_date_sk) && (('d_year >= 1999) && ('d_year <= (1999 + 2))))
:                    +- 'Join Inner
:                       :- 'UnresolvedRelation `web_sales`
:                       +- 'UnresolvedRelation `date_dim`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['channel ASC NULLS FIRST, 'i_brand_id ASC NULLS FIRST, 'i_class_id ASC NULLS FIRST, 'i_category_id ASC NULLS FIRST], true
         +- 'Aggregate ['rollup('channel, 'i_brand_id, 'i_class_id, 'i_category_id)], ['channel, 'i_brand_id, 'i_class_id, 'i_category_id, unresolvedalias('sum('sales), None), unresolvedalias('sum('number_sales), None)]
            +- 'SubqueryAlias `y`
               +- 'Union
                  :- 'Union
                  :  :- 'UnresolvedHaving ('sum(('ss_quantity * 'ss_list_price)) > scalar-subquery#4 [])
                  :  :  :  +- 'Project ['average_sales]
                  :  :  :     +- 'UnresolvedRelation `avg_sales`
                  :  :  +- 'Aggregate ['i_brand_id, 'i_class_id, 'i_category_id], [store AS channel#0, 'i_brand_id, 'i_class_id, 'i_category_id, 'sum(('ss_quantity * 'ss_list_price)) AS sales#1, 'count(1) AS number_sales#2]
                  :  :     +- 'Filter ((('ss_item_sk IN (list#3 []) && ('ss_item_sk = 'i_item_sk)) && ('ss_sold_date_sk = 'd_date_sk)) && (('d_year = (1999 + 2)) && ('d_moy = 11)))
                  :  :        :  +- 'Project ['ss_item_sk]
                  :  :        :     +- 'UnresolvedRelation `cross_items`
                  :  :        +- 'Join Inner
                  :  :           :- 'Join Inner
                  :  :           :  :- 'UnresolvedRelation `store_sales`
                  :  :           :  +- 'UnresolvedRelation `item`
                  :  :           +- 'UnresolvedRelation `date_dim`
                  :  +- 'UnresolvedHaving ('sum(('cs_quantity * 'cs_list_price)) > scalar-subquery#9 [])
                  :     :  +- 'Project ['average_sales]
                  :     :     +- 'UnresolvedRelation `avg_sales`
                  :     +- 'Aggregate ['i_brand_id, 'i_class_id, 'i_category_id], [catalog AS channel#5, 'i_brand_id, 'i_class_id, 'i_category_id, 'sum(('cs_quantity * 'cs_list_price)) AS sales#6, 'count(1) AS number_sales#7]
                  :        +- 'Filter ((('cs_item_sk IN (list#8 []) && ('cs_item_sk = 'i_item_sk)) && ('cs_sold_date_sk = 'd_date_sk)) && (('d_year = (1999 + 2)) && ('d_moy = 11)))
                  :           :  +- 'Project ['ss_item_sk]
                  :           :     +- 'UnresolvedRelation `cross_items`
                  :           +- 'Join Inner
                  :              :- 'Join Inner
                  :              :  :- 'UnresolvedRelation `catalog_sales`
                  :              :  +- 'UnresolvedRelation `item`
                  :              +- 'UnresolvedRelation `date_dim`
                  +- 'UnresolvedHaving ('sum(('ws_quantity * 'ws_list_price)) > scalar-subquery#14 [])
                     :  +- 'Project ['average_sales]
                     :     +- 'UnresolvedRelation `avg_sales`
                     +- 'Aggregate ['i_brand_id, 'i_class_id, 'i_category_id], [web AS channel#10, 'i_brand_id, 'i_class_id, 'i_category_id, 'sum(('ws_quantity * 'ws_list_price)) AS sales#11, 'count(1) AS number_sales#12]
                        +- 'Filter ((('ws_item_sk IN (list#13 []) && ('ws_item_sk = 'i_item_sk)) && ('ws_sold_date_sk = 'd_date_sk)) && (('d_year = (1999 + 2)) && ('d_moy = 11)))
                           :  +- 'Project ['ss_item_sk]
                           :     +- 'UnresolvedRelation `cross_items`
                           +- 'Join Inner
                              :- 'Join Inner
                              :  :- 'UnresolvedRelation `web_sales`
                              :  +- 'UnresolvedRelation `item`
                              +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
channel: string, i_brand_id: int, i_class_id: int, i_category_id: int, sum(sales): double, sum(number_sales): bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#197 ASC NULLS FIRST, i_brand_id#198 ASC NULLS FIRST, i_class_id#199 ASC NULLS FIRST, i_category_id#200 ASC NULLS FIRST], true
      +- Aggregate [channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, spark_grouping_id#192], [channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, sum(sales#1) AS sum(sales)#187, sum(number_sales#2L) AS sum(number_sales)#188L]
         +- Expand [List(channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sales#1, number_sales#2L, channel#193, i_brand_id#194, i_class_id#195, i_category_id#196, 0), List(channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sales#1, number_sales#2L, channel#193, i_brand_id#194, i_class_id#195, null, 1), List(channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sales#1, number_sales#2L, channel#193, i_brand_id#194, null, null, 3), List(channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sales#1, number_sales#2L, channel#193, null, null, null, 7), List(channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sales#1, number_sales#2L, null, null, null, null, 15)], [channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sales#1, number_sales#2L, channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, spark_grouping_id#192]
            +- Project [channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sales#1, number_sales#2L, channel#0 AS channel#193, i_brand_id#35 AS i_brand_id#194, i_class_id#37 AS i_class_id#195, i_category_id#39 AS i_category_id#196]
               +- SubqueryAlias `y`
                  +- Union
                     :- Union
                     :  :- Project [channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sales#1, number_sales#2L]
                     :  :  +- Filter (sum((cast(ss_quantity#60 as double) * ss_list_price#62))#178 > scalar-subquery#4 [])
                     :  :     :  +- Project [average_sales#25]
                     :  :     :     +- SubqueryAlias `avg_sales`
                     :  :     :        +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
                     :  :     :           +- SubqueryAlias `x`
                     :  :     :              +- Union
                     :  :     :                 :- Union
                     :  :     :                 :  :- Project [ss_quantity#60 AS quantity#19, ss_list_price#62 AS list_price#20]
                     :  :     :                 :  :  +- Filter ((ss_sold_date_sk#50 = d_date_sk#73) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :  :     :                 :  :     +- Join Inner
                     :  :     :                 :  :        :- SubqueryAlias `tpcds`.`store_sales`
                     :  :     :                 :  :        :  +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                     :  :     :                 :  :        +- SubqueryAlias `tpcds`.`date_dim`
                     :  :     :                 :  :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :  :     :                 :  +- Project [cs_quantity#119 AS quantity#21, cs_list_price#121 AS list_price#22]
                     :  :     :                 :     +- Filter ((cs_sold_date_sk#101 = d_date_sk#73) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :  :     :                 :        +- Join Inner
                     :  :     :                 :           :- SubqueryAlias `tpcds`.`catalog_sales`
                     :  :     :                 :           :  +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                     :  :     :                 :           +- SubqueryAlias `tpcds`.`date_dim`
                     :  :     :                 :              +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :  :     :                 +- Project [ws_quantity#153 AS quantity#23, ws_list_price#155 AS list_price#24]
                     :  :     :                    +- Filter ((ws_sold_date_sk#135 = d_date_sk#73) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :  :     :                       +- Join Inner
                     :  :     :                          :- SubqueryAlias `tpcds`.`web_sales`
                     :  :     :                          :  +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                     :  :     :                          +- SubqueryAlias `tpcds`.`date_dim`
                     :  :     :                             +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :  :     +- Aggregate [i_brand_id#35, i_class_id#37, i_category_id#39], [store AS channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sum((cast(ss_quantity#60 as double) * ss_list_price#62)) AS sales#1, count(1) AS number_sales#2L, sum((cast(ss_quantity#60 as double) * ss_list_price#62)) AS sum((cast(ss_quantity#60 as double) * ss_list_price#62))#178]
                     :  :        +- Filter (((ss_item_sk#52 IN (list#3 []) && (ss_item_sk#52 = i_item_sk#28)) && (ss_sold_date_sk#50 = d_date_sk#73)) && ((d_year#79 = (1999 + 2)) && (d_moy#81 = 11)))
                     :  :           :  +- Project [ss_item_sk#18]
                     :  :           :     +- SubqueryAlias `cross_items`
                     :  :           :        +- Project [i_item_sk#28 AS ss_item_sk#18]
                     :  :           :           +- Filter (((i_brand_id#35 = brand_id#15) && (i_class_id#37 = class_id#16)) && (i_category_id#39 = category_id#17))
                     :  :           :              +- Join Inner
                     :  :           :                 :- SubqueryAlias `tpcds`.`item`
                     :  :           :                 :  +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :  :           :                 +- SubqueryAlias `__auto_generated_subquery_name`
                     :  :           :                    +- Intersect false
                     :  :           :                       :- Intersect false
                     :  :           :                       :  :- Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
                     :  :           :                       :  :  +- Filter (((ss_item_sk#52 = i_item_sk#28) && (ss_sold_date_sk#50 = d_date_sk#73)) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :  :           :                       :  :     +- Join Inner
                     :  :           :                       :  :        :- Join Inner
                     :  :           :                       :  :        :  :- SubqueryAlias `tpcds`.`store_sales`
                     :  :           :                       :  :        :  :  +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                     :  :           :                       :  :        :  +- SubqueryAlias `iss`
                     :  :           :                       :  :        :     +- SubqueryAlias `tpcds`.`item`
                     :  :           :                       :  :        :        +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :  :           :                       :  :        +- SubqueryAlias `d1`
                     :  :           :                       :  :           +- SubqueryAlias `tpcds`.`date_dim`
                     :  :           :                       :  :              +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :  :           :                       :  +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                     :  :           :                       :     +- Filter (((cs_item_sk#116 = i_item_sk#28) && (cs_sold_date_sk#101 = d_date_sk#73)) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :  :           :                       :        +- Join Inner
                     :  :           :                       :           :- Join Inner
                     :  :           :                       :           :  :- SubqueryAlias `tpcds`.`catalog_sales`
                     :  :           :                       :           :  :  +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                     :  :           :                       :           :  +- SubqueryAlias `ics`
                     :  :           :                       :           :     +- SubqueryAlias `tpcds`.`item`
                     :  :           :                       :           :        +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :  :           :                       :           +- SubqueryAlias `d2`
                     :  :           :                       :              +- SubqueryAlias `tpcds`.`date_dim`
                     :  :           :                       :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :  :           :                       +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                     :  :           :                          +- Filter (((ws_item_sk#138 = i_item_sk#28) && (ws_sold_date_sk#135 = d_date_sk#73)) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :  :           :                             +- Join Inner
                     :  :           :                                :- Join Inner
                     :  :           :                                :  :- SubqueryAlias `tpcds`.`web_sales`
                     :  :           :                                :  :  +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                     :  :           :                                :  +- SubqueryAlias `iws`
                     :  :           :                                :     +- SubqueryAlias `tpcds`.`item`
                     :  :           :                                :        +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :  :           :                                +- SubqueryAlias `d3`
                     :  :           :                                   +- SubqueryAlias `tpcds`.`date_dim`
                     :  :           :                                      +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :  :           +- Join Inner
                     :  :              :- Join Inner
                     :  :              :  :- SubqueryAlias `tpcds`.`store_sales`
                     :  :              :  :  +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                     :  :              :  +- SubqueryAlias `tpcds`.`item`
                     :  :              :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :  :              +- SubqueryAlias `tpcds`.`date_dim`
                     :  :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :  +- Project [channel#5, i_brand_id#35, i_class_id#37, i_category_id#39, sales#6, number_sales#7L]
                     :     +- Filter (sum((cast(cs_quantity#119 as double) * cs_list_price#121))#181 > scalar-subquery#9 [])
                     :        :  +- Project [average_sales#25]
                     :        :     +- SubqueryAlias `avg_sales`
                     :        :        +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
                     :        :           +- SubqueryAlias `x`
                     :        :              +- Union
                     :        :                 :- Union
                     :        :                 :  :- Project [ss_quantity#60 AS quantity#19, ss_list_price#62 AS list_price#20]
                     :        :                 :  :  +- Filter ((ss_sold_date_sk#50 = d_date_sk#73) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :        :                 :  :     +- Join Inner
                     :        :                 :  :        :- SubqueryAlias `tpcds`.`store_sales`
                     :        :                 :  :        :  +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                     :        :                 :  :        +- SubqueryAlias `tpcds`.`date_dim`
                     :        :                 :  :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :        :                 :  +- Project [cs_quantity#119 AS quantity#21, cs_list_price#121 AS list_price#22]
                     :        :                 :     +- Filter ((cs_sold_date_sk#101 = d_date_sk#73) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :        :                 :        +- Join Inner
                     :        :                 :           :- SubqueryAlias `tpcds`.`catalog_sales`
                     :        :                 :           :  +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                     :        :                 :           +- SubqueryAlias `tpcds`.`date_dim`
                     :        :                 :              +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :        :                 +- Project [ws_quantity#153 AS quantity#23, ws_list_price#155 AS list_price#24]
                     :        :                    +- Filter ((ws_sold_date_sk#135 = d_date_sk#73) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :        :                       +- Join Inner
                     :        :                          :- SubqueryAlias `tpcds`.`web_sales`
                     :        :                          :  +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                     :        :                          +- SubqueryAlias `tpcds`.`date_dim`
                     :        :                             +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :        +- Aggregate [i_brand_id#35, i_class_id#37, i_category_id#39], [catalog AS channel#5, i_brand_id#35, i_class_id#37, i_category_id#39, sum((cast(cs_quantity#119 as double) * cs_list_price#121)) AS sales#6, count(1) AS number_sales#7L, sum((cast(cs_quantity#119 as double) * cs_list_price#121)) AS sum((cast(cs_quantity#119 as double) * cs_list_price#121))#181]
                     :           +- Filter (((cs_item_sk#116 IN (list#8 []) && (cs_item_sk#116 = i_item_sk#28)) && (cs_sold_date_sk#101 = d_date_sk#73)) && ((d_year#79 = (1999 + 2)) && (d_moy#81 = 11)))
                     :              :  +- Project [ss_item_sk#18]
                     :              :     +- SubqueryAlias `cross_items`
                     :              :        +- Project [i_item_sk#28 AS ss_item_sk#18]
                     :              :           +- Filter (((i_brand_id#35 = brand_id#15) && (i_class_id#37 = class_id#16)) && (i_category_id#39 = category_id#17))
                     :              :              +- Join Inner
                     :              :                 :- SubqueryAlias `tpcds`.`item`
                     :              :                 :  +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :              :                 +- SubqueryAlias `__auto_generated_subquery_name`
                     :              :                    +- Intersect false
                     :              :                       :- Intersect false
                     :              :                       :  :- Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
                     :              :                       :  :  +- Filter (((ss_item_sk#52 = i_item_sk#28) && (ss_sold_date_sk#50 = d_date_sk#73)) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :              :                       :  :     +- Join Inner
                     :              :                       :  :        :- Join Inner
                     :              :                       :  :        :  :- SubqueryAlias `tpcds`.`store_sales`
                     :              :                       :  :        :  :  +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                     :              :                       :  :        :  +- SubqueryAlias `iss`
                     :              :                       :  :        :     +- SubqueryAlias `tpcds`.`item`
                     :              :                       :  :        :        +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :              :                       :  :        +- SubqueryAlias `d1`
                     :              :                       :  :           +- SubqueryAlias `tpcds`.`date_dim`
                     :              :                       :  :              +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :              :                       :  +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                     :              :                       :     +- Filter (((cs_item_sk#116 = i_item_sk#28) && (cs_sold_date_sk#101 = d_date_sk#73)) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :              :                       :        +- Join Inner
                     :              :                       :           :- Join Inner
                     :              :                       :           :  :- SubqueryAlias `tpcds`.`catalog_sales`
                     :              :                       :           :  :  +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                     :              :                       :           :  +- SubqueryAlias `ics`
                     :              :                       :           :     +- SubqueryAlias `tpcds`.`item`
                     :              :                       :           :        +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :              :                       :           +- SubqueryAlias `d2`
                     :              :                       :              +- SubqueryAlias `tpcds`.`date_dim`
                     :              :                       :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :              :                       +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                     :              :                          +- Filter (((ws_item_sk#138 = i_item_sk#28) && (ws_sold_date_sk#135 = d_date_sk#73)) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                     :              :                             +- Join Inner
                     :              :                                :- Join Inner
                     :              :                                :  :- SubqueryAlias `tpcds`.`web_sales`
                     :              :                                :  :  +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                     :              :                                :  +- SubqueryAlias `iws`
                     :              :                                :     +- SubqueryAlias `tpcds`.`item`
                     :              :                                :        +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :              :                                +- SubqueryAlias `d3`
                     :              :                                   +- SubqueryAlias `tpcds`.`date_dim`
                     :              :                                      +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :              +- Join Inner
                     :                 :- Join Inner
                     :                 :  :- SubqueryAlias `tpcds`.`catalog_sales`
                     :                 :  :  +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                     :                 :  +- SubqueryAlias `tpcds`.`item`
                     :                 :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                     :                 +- SubqueryAlias `tpcds`.`date_dim`
                     :                    +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     +- Project [channel#10, i_brand_id#35, i_class_id#37, i_category_id#39, sales#11, number_sales#12L]
                        +- Filter (sum((cast(ws_quantity#153 as double) * ws_list_price#155))#184 > scalar-subquery#14 [])
                           :  +- Project [average_sales#25]
                           :     +- SubqueryAlias `avg_sales`
                           :        +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
                           :           +- SubqueryAlias `x`
                           :              +- Union
                           :                 :- Union
                           :                 :  :- Project [ss_quantity#60 AS quantity#19, ss_list_price#62 AS list_price#20]
                           :                 :  :  +- Filter ((ss_sold_date_sk#50 = d_date_sk#73) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                           :                 :  :     +- Join Inner
                           :                 :  :        :- SubqueryAlias `tpcds`.`store_sales`
                           :                 :  :        :  +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                           :                 :  :        +- SubqueryAlias `tpcds`.`date_dim`
                           :                 :  :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                           :                 :  +- Project [cs_quantity#119 AS quantity#21, cs_list_price#121 AS list_price#22]
                           :                 :     +- Filter ((cs_sold_date_sk#101 = d_date_sk#73) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                           :                 :        +- Join Inner
                           :                 :           :- SubqueryAlias `tpcds`.`catalog_sales`
                           :                 :           :  +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                           :                 :           +- SubqueryAlias `tpcds`.`date_dim`
                           :                 :              +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                           :                 +- Project [ws_quantity#153 AS quantity#23, ws_list_price#155 AS list_price#24]
                           :                    +- Filter ((ws_sold_date_sk#135 = d_date_sk#73) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                           :                       +- Join Inner
                           :                          :- SubqueryAlias `tpcds`.`web_sales`
                           :                          :  +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                           :                          +- SubqueryAlias `tpcds`.`date_dim`
                           :                             +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                           +- Aggregate [i_brand_id#35, i_class_id#37, i_category_id#39], [web AS channel#10, i_brand_id#35, i_class_id#37, i_category_id#39, sum((cast(ws_quantity#153 as double) * ws_list_price#155)) AS sales#11, count(1) AS number_sales#12L, sum((cast(ws_quantity#153 as double) * ws_list_price#155)) AS sum((cast(ws_quantity#153 as double) * ws_list_price#155))#184]
                              +- Filter (((ws_item_sk#138 IN (list#13 []) && (ws_item_sk#138 = i_item_sk#28)) && (ws_sold_date_sk#135 = d_date_sk#73)) && ((d_year#79 = (1999 + 2)) && (d_moy#81 = 11)))
                                 :  +- Project [ss_item_sk#18]
                                 :     +- SubqueryAlias `cross_items`
                                 :        +- Project [i_item_sk#28 AS ss_item_sk#18]
                                 :           +- Filter (((i_brand_id#35 = brand_id#15) && (i_class_id#37 = class_id#16)) && (i_category_id#39 = category_id#17))
                                 :              +- Join Inner
                                 :                 :- SubqueryAlias `tpcds`.`item`
                                 :                 :  +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                                 :                 +- SubqueryAlias `__auto_generated_subquery_name`
                                 :                    +- Intersect false
                                 :                       :- Intersect false
                                 :                       :  :- Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
                                 :                       :  :  +- Filter (((ss_item_sk#52 = i_item_sk#28) && (ss_sold_date_sk#50 = d_date_sk#73)) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                                 :                       :  :     +- Join Inner
                                 :                       :  :        :- Join Inner
                                 :                       :  :        :  :- SubqueryAlias `tpcds`.`store_sales`
                                 :                       :  :        :  :  +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                                 :                       :  :        :  +- SubqueryAlias `iss`
                                 :                       :  :        :     +- SubqueryAlias `tpcds`.`item`
                                 :                       :  :        :        +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                                 :                       :  :        +- SubqueryAlias `d1`
                                 :                       :  :           +- SubqueryAlias `tpcds`.`date_dim`
                                 :                       :  :              +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                                 :                       :  +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                                 :                       :     +- Filter (((cs_item_sk#116 = i_item_sk#28) && (cs_sold_date_sk#101 = d_date_sk#73)) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                                 :                       :        +- Join Inner
                                 :                       :           :- Join Inner
                                 :                       :           :  :- SubqueryAlias `tpcds`.`catalog_sales`
                                 :                       :           :  :  +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                                 :                       :           :  +- SubqueryAlias `ics`
                                 :                       :           :     +- SubqueryAlias `tpcds`.`item`
                                 :                       :           :        +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                                 :                       :           +- SubqueryAlias `d2`
                                 :                       :              +- SubqueryAlias `tpcds`.`date_dim`
                                 :                       :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                                 :                       +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                                 :                          +- Filter (((ws_item_sk#138 = i_item_sk#28) && (ws_sold_date_sk#135 = d_date_sk#73)) && ((d_year#79 >= 1999) && (d_year#79 <= (1999 + 2))))
                                 :                             +- Join Inner
                                 :                                :- Join Inner
                                 :                                :  :- SubqueryAlias `tpcds`.`web_sales`
                                 :                                :  :  +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                                 :                                :  +- SubqueryAlias `iws`
                                 :                                :     +- SubqueryAlias `tpcds`.`item`
                                 :                                :        +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                                 :                                +- SubqueryAlias `d3`
                                 :                                   +- SubqueryAlias `tpcds`.`date_dim`
                                 :                                      +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                                 +- Join Inner
                                    :- Join Inner
                                    :  :- SubqueryAlias `tpcds`.`web_sales`
                                    :  :  +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                                    :  +- SubqueryAlias `tpcds`.`item`
                                    :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                                    +- SubqueryAlias `tpcds`.`date_dim`
                                       +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#197 ASC NULLS FIRST, i_brand_id#198 ASC NULLS FIRST, i_class_id#199 ASC NULLS FIRST, i_category_id#200 ASC NULLS FIRST], true
      +- Aggregate [channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, spark_grouping_id#192], [channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, sum(sales#1) AS sum(sales)#187, sum(number_sales#2L) AS sum(number_sales)#188L]
         +- Expand [List(sales#1, number_sales#2L, channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, 0), List(sales#1, number_sales#2L, channel#0, i_brand_id#35, i_class_id#37, null, 1), List(sales#1, number_sales#2L, channel#0, i_brand_id#35, null, null, 3), List(sales#1, number_sales#2L, channel#0, null, null, null, 7), List(sales#1, number_sales#2L, null, null, null, null, 15)], [sales#1, number_sales#2L, channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, spark_grouping_id#192]
            +- Union
               :- Project [sales#1, number_sales#2L, channel#0, i_brand_id#35, i_class_id#37, i_category_id#39]
               :  +- Filter (isnotnull(sum((cast(ss_quantity#60 as double) * ss_list_price#62))#178) && (sum((cast(ss_quantity#60 as double) * ss_list_price#62))#178 > scalar-subquery#4 []))
               :     :  +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
               :     :     +- Union
               :     :        :- Project [ss_quantity#60 AS quantity#19, ss_list_price#62 AS list_price#20]
               :     :        :  +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
               :     :        :     :- Project [ss_sold_date_sk#50, ss_quantity#60, ss_list_price#62]
               :     :        :     :  +- Filter isnotnull(ss_sold_date_sk#50)
               :     :        :     :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
               :     :        :     +- Project [d_date_sk#73]
               :     :        :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :     :        :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :     :        :- Project [cs_quantity#119 AS quantity#21, cs_list_price#121 AS list_price#22]
               :     :        :  +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
               :     :        :     :- Project [cs_sold_date_sk#101, cs_quantity#119, cs_list_price#121]
               :     :        :     :  +- Filter isnotnull(cs_sold_date_sk#101)
               :     :        :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
               :     :        :     +- Project [d_date_sk#73]
               :     :        :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :     :        :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :     :        +- Project [ws_quantity#153 AS quantity#23, ws_list_price#155 AS list_price#24]
               :     :           +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
               :     :              :- Project [ws_sold_date_sk#135, ws_quantity#153, ws_list_price#155]
               :     :              :  +- Filter isnotnull(ws_sold_date_sk#135)
               :     :              :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
               :     :              +- Project [d_date_sk#73]
               :     :                 +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :     :                    +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :     +- Aggregate [i_brand_id#35, i_class_id#37, i_category_id#39], [store AS channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sum((cast(ss_quantity#60 as double) * ss_list_price#62)) AS sales#1, count(1) AS number_sales#2L, sum((cast(ss_quantity#60 as double) * ss_list_price#62)) AS sum((cast(ss_quantity#60 as double) * ss_list_price#62))#178]
               :        +- Project [ss_quantity#60, ss_list_price#62, i_brand_id#35, i_class_id#37, i_category_id#39]
               :           +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
               :              :- Project [ss_sold_date_sk#50, ss_quantity#60, ss_list_price#62, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :  +- Join Inner, (ss_item_sk#52 = i_item_sk#28)
               :              :     :- Join LeftSemi, (ss_item_sk#52 = ss_item_sk#18)
               :              :     :  :- Project [ss_sold_date_sk#50, ss_item_sk#52, ss_quantity#60, ss_list_price#62]
               :              :     :  :  +- Filter (isnotnull(ss_item_sk#52) && isnotnull(ss_sold_date_sk#50))
               :              :     :  :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
               :              :     :  +- Project [i_item_sk#28 AS ss_item_sk#18]
               :              :     :     +- Join Inner, (((i_brand_id#35 = brand_id#15) && (i_class_id#37 = class_id#16)) && (i_category_id#39 = category_id#17))
               :              :     :        :- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :        :  +- Filter ((isnotnull(i_class_id#37) && isnotnull(i_category_id#39)) && isnotnull(i_brand_id#35))
               :              :     :        :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :     :        +- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :     :           +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :              :     :              :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :     :              :  +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :              :     :              :     :- Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
               :              :     :              :     :  +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
               :              :     :              :     :     :- Project [ss_sold_date_sk#50, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :     :     :  +- Join Inner, (ss_item_sk#52 = i_item_sk#28)
               :              :     :              :     :     :     :- Project [ss_sold_date_sk#50, ss_item_sk#52]
               :              :     :              :     :     :     :  +- Filter (isnotnull(ss_item_sk#52) && isnotnull(ss_sold_date_sk#50))
               :              :     :              :     :     :     :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
               :              :     :              :     :     :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :     :     :        +- Filter (((isnotnull(i_item_sk#28) && isnotnull(i_brand_id#35)) && isnotnull(i_class_id#37)) && isnotnull(i_category_id#39))
               :              :     :              :     :     :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :     :              :     :     +- Project [d_date_sk#73]
               :              :     :              :     :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :     :              :     :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :     :              :     +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :        +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
               :              :     :              :           :- Project [cs_sold_date_sk#101, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :           :  +- Join Inner, (cs_item_sk#116 = i_item_sk#28)
               :              :     :              :           :     :- Project [cs_sold_date_sk#101, cs_item_sk#116]
               :              :     :              :           :     :  +- Filter (isnotnull(cs_item_sk#116) && isnotnull(cs_sold_date_sk#101))
               :              :     :              :           :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
               :              :     :              :           :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :           :        +- Filter isnotnull(i_item_sk#28)
               :              :     :              :           :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :     :              :           +- Project [d_date_sk#73]
               :              :     :              :              +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :     :              :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :     :              +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :                 +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
               :              :     :                    :- Project [ws_sold_date_sk#135, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :                    :  +- Join Inner, (ws_item_sk#138 = i_item_sk#28)
               :              :     :                    :     :- Project [ws_sold_date_sk#135, ws_item_sk#138]
               :              :     :                    :     :  +- Filter (isnotnull(ws_item_sk#138) && isnotnull(ws_sold_date_sk#135))
               :              :     :                    :     :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
               :              :     :                    :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :                    :        +- Filter isnotnull(i_item_sk#28)
               :              :     :                    :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :     :                    +- Project [d_date_sk#73]
               :              :     :                       +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :     :                          +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :     +- Join LeftSemi, (i_item_sk#28 = ss_item_sk#18)
               :              :        :- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :        :  +- Filter isnotnull(i_item_sk#28)
               :              :        :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :        +- Project [i_item_sk#28 AS ss_item_sk#18]
               :              :           +- Join Inner, (((i_brand_id#35 = brand_id#15) && (i_class_id#37 = class_id#16)) && (i_category_id#39 = category_id#17))
               :              :              :- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :              :  +- Filter ((isnotnull(i_class_id#37) && isnotnull(i_category_id#39)) && isnotnull(i_brand_id#35))
               :              :              :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :              +- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :                 +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :              :                    :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :                    :  +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :              :                    :     :- Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
               :              :                    :     :  +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
               :              :                    :     :     :- Project [ss_sold_date_sk#50, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :     :     :  +- Join Inner, (ss_item_sk#52 = i_item_sk#28)
               :              :                    :     :     :     :- Project [ss_sold_date_sk#50, ss_item_sk#52]
               :              :                    :     :     :     :  +- Filter (isnotnull(ss_item_sk#52) && isnotnull(ss_sold_date_sk#50))
               :              :                    :     :     :     :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
               :              :                    :     :     :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :     :     :        +- Filter (((isnotnull(i_item_sk#28) && isnotnull(i_brand_id#35)) && isnotnull(i_class_id#37)) && isnotnull(i_category_id#39))
               :              :                    :     :     :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :                    :     :     +- Project [d_date_sk#73]
               :              :                    :     :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :                    :     :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :                    :     +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :        +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
               :              :                    :           :- Project [cs_sold_date_sk#101, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :           :  +- Join Inner, (cs_item_sk#116 = i_item_sk#28)
               :              :                    :           :     :- Project [cs_sold_date_sk#101, cs_item_sk#116]
               :              :                    :           :     :  +- Filter (isnotnull(cs_item_sk#116) && isnotnull(cs_sold_date_sk#101))
               :              :                    :           :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
               :              :                    :           :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :           :        +- Filter isnotnull(i_item_sk#28)
               :              :                    :           :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :                    :           +- Project [d_date_sk#73]
               :              :                    :              +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :                    :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :                    +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                       +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
               :              :                          :- Project [ws_sold_date_sk#135, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                          :  +- Join Inner, (ws_item_sk#138 = i_item_sk#28)
               :              :                          :     :- Project [ws_sold_date_sk#135, ws_item_sk#138]
               :              :                          :     :  +- Filter (isnotnull(ws_item_sk#138) && isnotnull(ws_sold_date_sk#135))
               :              :                          :     :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
               :              :                          :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                          :        +- Filter isnotnull(i_item_sk#28)
               :              :                          :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :                          +- Project [d_date_sk#73]
               :              :                             +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :                                +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              +- Project [d_date_sk#73]
               :                 +- Filter ((((isnotnull(d_year#79) && isnotnull(d_moy#81)) && (d_year#79 = 2001)) && (d_moy#81 = 11)) && isnotnull(d_date_sk#73))
               :                    +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :- Project [sales#6, number_sales#7L, channel#5, i_brand_id#35, i_class_id#37, i_category_id#39]
               :  +- Filter (isnotnull(sum((cast(cs_quantity#119 as double) * cs_list_price#121))#181) && (sum((cast(cs_quantity#119 as double) * cs_list_price#121))#181 > scalar-subquery#9 []))
               :     :  +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
               :     :     +- Union
               :     :        :- Project [ss_quantity#60 AS quantity#19, ss_list_price#62 AS list_price#20]
               :     :        :  +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
               :     :        :     :- Project [ss_sold_date_sk#50, ss_quantity#60, ss_list_price#62]
               :     :        :     :  +- Filter isnotnull(ss_sold_date_sk#50)
               :     :        :     :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
               :     :        :     +- Project [d_date_sk#73]
               :     :        :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :     :        :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :     :        :- Project [cs_quantity#119 AS quantity#21, cs_list_price#121 AS list_price#22]
               :     :        :  +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
               :     :        :     :- Project [cs_sold_date_sk#101, cs_quantity#119, cs_list_price#121]
               :     :        :     :  +- Filter isnotnull(cs_sold_date_sk#101)
               :     :        :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
               :     :        :     +- Project [d_date_sk#73]
               :     :        :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :     :        :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :     :        +- Project [ws_quantity#153 AS quantity#23, ws_list_price#155 AS list_price#24]
               :     :           +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
               :     :              :- Project [ws_sold_date_sk#135, ws_quantity#153, ws_list_price#155]
               :     :              :  +- Filter isnotnull(ws_sold_date_sk#135)
               :     :              :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
               :     :              +- Project [d_date_sk#73]
               :     :                 +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :     :                    +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :     +- Aggregate [i_brand_id#35, i_class_id#37, i_category_id#39], [catalog AS channel#5, i_brand_id#35, i_class_id#37, i_category_id#39, sum((cast(cs_quantity#119 as double) * cs_list_price#121)) AS sales#6, count(1) AS number_sales#7L, sum((cast(cs_quantity#119 as double) * cs_list_price#121)) AS sum((cast(cs_quantity#119 as double) * cs_list_price#121))#181]
               :        +- Project [cs_quantity#119, cs_list_price#121, i_brand_id#35, i_class_id#37, i_category_id#39]
               :           +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
               :              :- Project [cs_sold_date_sk#101, cs_quantity#119, cs_list_price#121, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :  +- Join Inner, (cs_item_sk#116 = i_item_sk#28)
               :              :     :- Join LeftSemi, (cs_item_sk#116 = ss_item_sk#18)
               :              :     :  :- Project [cs_sold_date_sk#101, cs_item_sk#116, cs_quantity#119, cs_list_price#121]
               :              :     :  :  +- Filter (isnotnull(cs_item_sk#116) && isnotnull(cs_sold_date_sk#101))
               :              :     :  :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
               :              :     :  +- Project [i_item_sk#28 AS ss_item_sk#18]
               :              :     :     +- Join Inner, (((i_brand_id#35 = brand_id#15) && (i_class_id#37 = class_id#16)) && (i_category_id#39 = category_id#17))
               :              :     :        :- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :        :  +- Filter ((isnotnull(i_class_id#37) && isnotnull(i_category_id#39)) && isnotnull(i_brand_id#35))
               :              :     :        :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :     :        +- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :     :           +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :              :     :              :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :     :              :  +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :              :     :              :     :- Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
               :              :     :              :     :  +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
               :              :     :              :     :     :- Project [ss_sold_date_sk#50, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :     :     :  +- Join Inner, (ss_item_sk#52 = i_item_sk#28)
               :              :     :              :     :     :     :- Project [ss_sold_date_sk#50, ss_item_sk#52]
               :              :     :              :     :     :     :  +- Filter (isnotnull(ss_item_sk#52) && isnotnull(ss_sold_date_sk#50))
               :              :     :              :     :     :     :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
               :              :     :              :     :     :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :     :     :        +- Filter (((isnotnull(i_item_sk#28) && isnotnull(i_brand_id#35)) && isnotnull(i_class_id#37)) && isnotnull(i_category_id#39))
               :              :     :              :     :     :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :     :              :     :     +- Project [d_date_sk#73]
               :              :     :              :     :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :     :              :     :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :     :              :     +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :        +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
               :              :     :              :           :- Project [cs_sold_date_sk#101, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :           :  +- Join Inner, (cs_item_sk#116 = i_item_sk#28)
               :              :     :              :           :     :- Project [cs_sold_date_sk#101, cs_item_sk#116]
               :              :     :              :           :     :  +- Filter (isnotnull(cs_item_sk#116) && isnotnull(cs_sold_date_sk#101))
               :              :     :              :           :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
               :              :     :              :           :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :              :           :        +- Filter isnotnull(i_item_sk#28)
               :              :     :              :           :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :     :              :           +- Project [d_date_sk#73]
               :              :     :              :              +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :     :              :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :     :              +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :                 +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
               :              :     :                    :- Project [ws_sold_date_sk#135, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :                    :  +- Join Inner, (ws_item_sk#138 = i_item_sk#28)
               :              :     :                    :     :- Project [ws_sold_date_sk#135, ws_item_sk#138]
               :              :     :                    :     :  +- Filter (isnotnull(ws_item_sk#138) && isnotnull(ws_sold_date_sk#135))
               :              :     :                    :     :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
               :              :     :                    :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :     :                    :        +- Filter isnotnull(i_item_sk#28)
               :              :     :                    :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :     :                    +- Project [d_date_sk#73]
               :              :     :                       +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :     :                          +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :     +- Join LeftSemi, (i_item_sk#28 = ss_item_sk#18)
               :              :        :- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :        :  +- Filter isnotnull(i_item_sk#28)
               :              :        :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :        +- Project [i_item_sk#28 AS ss_item_sk#18]
               :              :           +- Join Inner, (((i_brand_id#35 = brand_id#15) && (i_class_id#37 = class_id#16)) && (i_category_id#39 = category_id#17))
               :              :              :- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :              :  +- Filter ((isnotnull(i_class_id#37) && isnotnull(i_category_id#39)) && isnotnull(i_brand_id#35))
               :              :              :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :              +- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :                 +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :              :                    :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
               :              :                    :  +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :              :                    :     :- Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
               :              :                    :     :  +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
               :              :                    :     :     :- Project [ss_sold_date_sk#50, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :     :     :  +- Join Inner, (ss_item_sk#52 = i_item_sk#28)
               :              :                    :     :     :     :- Project [ss_sold_date_sk#50, ss_item_sk#52]
               :              :                    :     :     :     :  +- Filter (isnotnull(ss_item_sk#52) && isnotnull(ss_sold_date_sk#50))
               :              :                    :     :     :     :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
               :              :                    :     :     :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :     :     :        +- Filter (((isnotnull(i_item_sk#28) && isnotnull(i_brand_id#35)) && isnotnull(i_class_id#37)) && isnotnull(i_category_id#39))
               :              :                    :     :     :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :                    :     :     +- Project [d_date_sk#73]
               :              :                    :     :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :                    :     :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :                    :     +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :        +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
               :              :                    :           :- Project [cs_sold_date_sk#101, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :           :  +- Join Inner, (cs_item_sk#116 = i_item_sk#28)
               :              :                    :           :     :- Project [cs_sold_date_sk#101, cs_item_sk#116]
               :              :                    :           :     :  +- Filter (isnotnull(cs_item_sk#116) && isnotnull(cs_sold_date_sk#101))
               :              :                    :           :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
               :              :                    :           :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                    :           :        +- Filter isnotnull(i_item_sk#28)
               :              :                    :           :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :                    :           +- Project [d_date_sk#73]
               :              :                    :              +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :                    :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              :                    +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                       +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
               :              :                          :- Project [ws_sold_date_sk#135, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                          :  +- Join Inner, (ws_item_sk#138 = i_item_sk#28)
               :              :                          :     :- Project [ws_sold_date_sk#135, ws_item_sk#138]
               :              :                          :     :  +- Filter (isnotnull(ws_item_sk#138) && isnotnull(ws_sold_date_sk#135))
               :              :                          :     :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
               :              :                          :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :              :                          :        +- Filter isnotnull(i_item_sk#28)
               :              :                          :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
               :              :                          +- Project [d_date_sk#73]
               :              :                             +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :              :                                +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               :              +- Project [d_date_sk#73]
               :                 +- Filter ((((isnotnull(d_year#79) && isnotnull(d_moy#81)) && (d_year#79 = 2001)) && (d_moy#81 = 11)) && isnotnull(d_date_sk#73))
               :                    +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
               +- Project [sales#11, number_sales#12L, channel#10, i_brand_id#35, i_class_id#37, i_category_id#39]
                  +- Filter (isnotnull(sum((cast(ws_quantity#153 as double) * ws_list_price#155))#184) && (sum((cast(ws_quantity#153 as double) * ws_list_price#155))#184 > scalar-subquery#14 []))
                     :  +- Aggregate [avg((cast(quantity#19 as double) * list_price#20)) AS average_sales#25]
                     :     +- Union
                     :        :- Project [ss_quantity#60 AS quantity#19, ss_list_price#62 AS list_price#20]
                     :        :  +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
                     :        :     :- Project [ss_sold_date_sk#50, ss_quantity#60, ss_list_price#62]
                     :        :     :  +- Filter isnotnull(ss_sold_date_sk#50)
                     :        :     :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                     :        :     +- Project [d_date_sk#73]
                     :        :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                     :        :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :        :- Project [cs_quantity#119 AS quantity#21, cs_list_price#121 AS list_price#22]
                     :        :  +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
                     :        :     :- Project [cs_sold_date_sk#101, cs_quantity#119, cs_list_price#121]
                     :        :     :  +- Filter isnotnull(cs_sold_date_sk#101)
                     :        :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                     :        :     +- Project [d_date_sk#73]
                     :        :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                     :        :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     :        +- Project [ws_quantity#153 AS quantity#23, ws_list_price#155 AS list_price#24]
                     :           +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
                     :              :- Project [ws_sold_date_sk#135, ws_quantity#153, ws_list_price#155]
                     :              :  +- Filter isnotnull(ws_sold_date_sk#135)
                     :              :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                     :              +- Project [d_date_sk#73]
                     :                 +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                     :                    +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                     +- Aggregate [i_brand_id#35, i_class_id#37, i_category_id#39], [web AS channel#10, i_brand_id#35, i_class_id#37, i_category_id#39, sum((cast(ws_quantity#153 as double) * ws_list_price#155)) AS sales#11, count(1) AS number_sales#12L, sum((cast(ws_quantity#153 as double) * ws_list_price#155)) AS sum((cast(ws_quantity#153 as double) * ws_list_price#155))#184]
                        +- Project [ws_quantity#153, ws_list_price#155, i_brand_id#35, i_class_id#37, i_category_id#39]
                           +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
                              :- Project [ws_sold_date_sk#135, ws_quantity#153, ws_list_price#155, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :  +- Join Inner, (ws_item_sk#138 = i_item_sk#28)
                              :     :- Join LeftSemi, (ws_item_sk#138 = ss_item_sk#18)
                              :     :  :- Project [ws_sold_date_sk#135, ws_item_sk#138, ws_quantity#153, ws_list_price#155]
                              :     :  :  +- Filter (isnotnull(ws_item_sk#138) && isnotnull(ws_sold_date_sk#135))
                              :     :  :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                              :     :  +- Project [i_item_sk#28 AS ss_item_sk#18]
                              :     :     +- Join Inner, (((i_brand_id#35 = brand_id#15) && (i_class_id#37 = class_id#16)) && (i_category_id#39 = category_id#17))
                              :     :        :- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :     :        :  +- Filter ((isnotnull(i_class_id#37) && isnotnull(i_category_id#39)) && isnotnull(i_brand_id#35))
                              :     :        :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                              :     :        +- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
                              :     :           +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
                              :     :              :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
                              :     :              :  +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
                              :     :              :     :- Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
                              :     :              :     :  +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
                              :     :              :     :     :- Project [ss_sold_date_sk#50, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :     :              :     :     :  +- Join Inner, (ss_item_sk#52 = i_item_sk#28)
                              :     :              :     :     :     :- Project [ss_sold_date_sk#50, ss_item_sk#52]
                              :     :              :     :     :     :  +- Filter (isnotnull(ss_item_sk#52) && isnotnull(ss_sold_date_sk#50))
                              :     :              :     :     :     :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                              :     :              :     :     :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :     :              :     :     :        +- Filter (((isnotnull(i_item_sk#28) && isnotnull(i_brand_id#35)) && isnotnull(i_class_id#37)) && isnotnull(i_category_id#39))
                              :     :              :     :     :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                              :     :              :     :     +- Project [d_date_sk#73]
                              :     :              :     :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                              :     :              :     :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                              :     :              :     +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                              :     :              :        +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
                              :     :              :           :- Project [cs_sold_date_sk#101, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :     :              :           :  +- Join Inner, (cs_item_sk#116 = i_item_sk#28)
                              :     :              :           :     :- Project [cs_sold_date_sk#101, cs_item_sk#116]
                              :     :              :           :     :  +- Filter (isnotnull(cs_item_sk#116) && isnotnull(cs_sold_date_sk#101))
                              :     :              :           :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                              :     :              :           :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :     :              :           :        +- Filter isnotnull(i_item_sk#28)
                              :     :              :           :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                              :     :              :           +- Project [d_date_sk#73]
                              :     :              :              +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                              :     :              :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                              :     :              +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                              :     :                 +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
                              :     :                    :- Project [ws_sold_date_sk#135, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :     :                    :  +- Join Inner, (ws_item_sk#138 = i_item_sk#28)
                              :     :                    :     :- Project [ws_sold_date_sk#135, ws_item_sk#138]
                              :     :                    :     :  +- Filter (isnotnull(ws_item_sk#138) && isnotnull(ws_sold_date_sk#135))
                              :     :                    :     :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                              :     :                    :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :     :                    :        +- Filter isnotnull(i_item_sk#28)
                              :     :                    :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                              :     :                    +- Project [d_date_sk#73]
                              :     :                       +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                              :     :                          +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                              :     +- Join LeftSemi, (i_item_sk#28 = ss_item_sk#18)
                              :        :- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :        :  +- Filter isnotnull(i_item_sk#28)
                              :        :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                              :        +- Project [i_item_sk#28 AS ss_item_sk#18]
                              :           +- Join Inner, (((i_brand_id#35 = brand_id#15) && (i_class_id#37 = class_id#16)) && (i_category_id#39 = category_id#17))
                              :              :- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :              :  +- Filter ((isnotnull(i_class_id#37) && isnotnull(i_category_id#39)) && isnotnull(i_brand_id#35))
                              :              :     +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                              :              +- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
                              :                 +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
                              :                    :- Aggregate [brand_id#15, class_id#16, category_id#17], [brand_id#15, class_id#16, category_id#17]
                              :                    :  +- Join LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
                              :                    :     :- Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
                              :                    :     :  +- Join Inner, (ss_sold_date_sk#50 = d_date_sk#73)
                              :                    :     :     :- Project [ss_sold_date_sk#50, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :                    :     :     :  +- Join Inner, (ss_item_sk#52 = i_item_sk#28)
                              :                    :     :     :     :- Project [ss_sold_date_sk#50, ss_item_sk#52]
                              :                    :     :     :     :  +- Filter (isnotnull(ss_item_sk#52) && isnotnull(ss_sold_date_sk#50))
                              :                    :     :     :     :     +- Relation[ss_sold_date_sk#50,ss_sold_time_sk#51,ss_item_sk#52,ss_customer_sk#53,ss_cdemo_sk#54,ss_hdemo_sk#55,ss_addr_sk#56,ss_store_sk#57,ss_promo_sk#58,ss_ticket_number#59,ss_quantity#60,ss_wholesale_cost#61,ss_list_price#62,ss_sales_price#63,ss_ext_discount_amt#64,ss_ext_sales_price#65,ss_ext_wholesale_cost#66,ss_ext_list_price#67,ss_ext_tax#68,ss_coupon_amt#69,ss_net_paid#70,ss_net_paid_inc_tax#71,ss_net_profit#72] parquet
                              :                    :     :     :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :                    :     :     :        +- Filter (((isnotnull(i_item_sk#28) && isnotnull(i_brand_id#35)) && isnotnull(i_class_id#37)) && isnotnull(i_category_id#39))
                              :                    :     :     :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                              :                    :     :     +- Project [d_date_sk#73]
                              :                    :     :        +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                              :                    :     :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                              :                    :     +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                              :                    :        +- Join Inner, (cs_sold_date_sk#101 = d_date_sk#73)
                              :                    :           :- Project [cs_sold_date_sk#101, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :                    :           :  +- Join Inner, (cs_item_sk#116 = i_item_sk#28)
                              :                    :           :     :- Project [cs_sold_date_sk#101, cs_item_sk#116]
                              :                    :           :     :  +- Filter (isnotnull(cs_item_sk#116) && isnotnull(cs_sold_date_sk#101))
                              :                    :           :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
                              :                    :           :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :                    :           :        +- Filter isnotnull(i_item_sk#28)
                              :                    :           :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                              :                    :           +- Project [d_date_sk#73]
                              :                    :              +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                              :                    :                 +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                              :                    +- Project [i_brand_id#35, i_class_id#37, i_category_id#39]
                              :                       +- Join Inner, (ws_sold_date_sk#135 = d_date_sk#73)
                              :                          :- Project [ws_sold_date_sk#135, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :                          :  +- Join Inner, (ws_item_sk#138 = i_item_sk#28)
                              :                          :     :- Project [ws_sold_date_sk#135, ws_item_sk#138]
                              :                          :     :  +- Filter (isnotnull(ws_item_sk#138) && isnotnull(ws_sold_date_sk#135))
                              :                          :     :     +- Relation[ws_sold_date_sk#135,ws_sold_time_sk#136,ws_ship_date_sk#137,ws_item_sk#138,ws_bill_customer_sk#139,ws_bill_cdemo_sk#140,ws_bill_hdemo_sk#141,ws_bill_addr_sk#142,ws_ship_customer_sk#143,ws_ship_cdemo_sk#144,ws_ship_hdemo_sk#145,ws_ship_addr_sk#146,ws_web_page_sk#147,ws_web_site_sk#148,ws_ship_mode_sk#149,ws_warehouse_sk#150,ws_promo_sk#151,ws_order_number#152,ws_quantity#153,ws_wholesale_cost#154,ws_list_price#155,ws_sales_price#156,ws_ext_discount_amt#157,ws_ext_sales_price#158,... 10 more fields] parquet
                              :                          :     +- Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
                              :                          :        +- Filter isnotnull(i_item_sk#28)
                              :                          :           +- Relation[i_item_sk#28,i_item_id#29,i_rec_start_date#30,i_rec_end_date#31,i_item_desc#32,i_current_price#33,i_wholesale_cost#34,i_brand_id#35,i_brand#36,i_class_id#37,i_class#38,i_category_id#39,i_category#40,i_manufact_id#41,i_manufact#42,i_size#43,i_formulation#44,i_color#45,i_units#46,i_container#47,i_manager_id#48,i_product_name#49] parquet
                              :                          +- Project [d_date_sk#73]
                              :                             +- Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                              :                                +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
                              +- Project [d_date_sk#73]
                                 +- Filter ((((isnotnull(d_year#79) && isnotnull(d_moy#81)) && (d_year#79 = 2001)) && (d_moy#81 = 11)) && isnotnull(d_date_sk#73))
                                    +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[channel#197 ASC NULLS FIRST,i_brand_id#198 ASC NULLS FIRST,i_class_id#199 ASC NULLS FIRST,i_category_id#200 ASC NULLS FIRST], output=[channel#197,i_brand_id#198,i_class_id#199,i_category_id#200,sum(sales)#187,sum(number_sales)#188L])
+- *(137) HashAggregate(keys=[channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, spark_grouping_id#192], functions=[sum(sales#1), sum(number_sales#2L)], output=[channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, sum(sales)#187, sum(number_sales)#188L])
   +- Exchange hashpartitioning(channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, spark_grouping_id#192, 200)
      +- *(136) HashAggregate(keys=[channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, spark_grouping_id#192], functions=[partial_sum(sales#1), partial_sum(number_sales#2L)], output=[channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, spark_grouping_id#192, sum#230, sum#231L])
         +- *(136) Expand [List(sales#1, number_sales#2L, channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, 0), List(sales#1, number_sales#2L, channel#0, i_brand_id#35, i_class_id#37, null, 1), List(sales#1, number_sales#2L, channel#0, i_brand_id#35, null, null, 3), List(sales#1, number_sales#2L, channel#0, null, null, null, 7), List(sales#1, number_sales#2L, null, null, null, null, 15)], [sales#1, number_sales#2L, channel#197, i_brand_id#198, i_class_id#199, i_category_id#200, spark_grouping_id#192]
            +- Union
               :- *(45) Project [sales#1, number_sales#2L, channel#0, i_brand_id#35, i_class_id#37, i_category_id#39]
               :  +- *(45) Filter (isnotnull(sum((cast(ss_quantity#60 as double) * ss_list_price#62))#178) && (sum((cast(ss_quantity#60 as double) * ss_list_price#62))#178 > Subquery subquery4))
               :     :  +- Subquery subquery4
               :     :     +- *(8) HashAggregate(keys=[], functions=[avg((cast(quantity#19 as double) * list_price#20))], output=[average_sales#25])
               :     :        +- Exchange SinglePartition
               :     :           +- *(7) HashAggregate(keys=[], functions=[partial_avg((cast(quantity#19 as double) * list_price#20))], output=[sum#246, count#247L])
               :     :              +- Union
               :     :                 :- *(2) Project [ss_quantity#60 AS quantity#19, ss_list_price#62 AS list_price#20]
               :     :                 :  +- *(2) BroadcastHashJoin [ss_sold_date_sk#50], [d_date_sk#73], Inner, BuildRight
               :     :                 :     :- *(2) Project [ss_sold_date_sk#50, ss_quantity#60, ss_list_price#62]
               :     :                 :     :  +- *(2) Filter isnotnull(ss_sold_date_sk#50)
               :     :                 :     :     +- *(2) FileScan parquet tpcds.store_sales[ss_sold_date_sk#50,ss_quantity#60,ss_list_price#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_quantity:int,ss_list_price:double>
               :     :                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :                 :        +- *(1) Project [d_date_sk#73]
               :     :                 :           +- *(1) Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :     :                 :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#73,d_year#79] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
               :     :                 :- *(4) Project [cs_quantity#119 AS quantity#21, cs_list_price#121 AS list_price#22]
               :     :                 :  +- *(4) BroadcastHashJoin [cs_sold_date_sk#101], [d_date_sk#73], Inner, BuildRight
               :     :                 :     :- *(4) Project [cs_sold_date_sk#101, cs_quantity#119, cs_list_price#121]
               :     :                 :     :  +- *(4) Filter isnotnull(cs_sold_date_sk#101)
               :     :                 :     :     +- *(4) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#101,cs_quantity#119,cs_list_price#121] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_quantity:int,cs_list_price:double>
               :     :                 :     +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :                 +- *(6) Project [ws_quantity#153 AS quantity#23, ws_list_price#155 AS list_price#24]
               :     :                    +- *(6) BroadcastHashJoin [ws_sold_date_sk#135], [d_date_sk#73], Inner, BuildRight
               :     :                       :- *(6) Project [ws_sold_date_sk#135, ws_quantity#153, ws_list_price#155]
               :     :                       :  +- *(6) Filter isnotnull(ws_sold_date_sk#135)
               :     :                       :     +- *(6) FileScan parquet tpcds.web_sales[ws_sold_date_sk#135,ws_quantity#153,ws_list_price#155] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_quantity:int,ws_list_price:double>
               :     :                       +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     +- *(45) HashAggregate(keys=[i_brand_id#35, i_class_id#37, i_category_id#39], functions=[sum((cast(ss_quantity#60 as double) * ss_list_price#62)), count(1)], output=[channel#0, i_brand_id#35, i_class_id#37, i_category_id#39, sales#1, number_sales#2L, sum((cast(ss_quantity#60 as double) * ss_list_price#62))#178])
               :        +- Exchange hashpartitioning(i_brand_id#35, i_class_id#37, i_category_id#39, 200)
               :           +- *(44) HashAggregate(keys=[i_brand_id#35, i_class_id#37, i_category_id#39], functions=[partial_sum((cast(ss_quantity#60 as double) * ss_list_price#62)), partial_count(1)], output=[i_brand_id#35, i_class_id#37, i_category_id#39, sum#234, count#235L])
               :              +- *(44) Project [ss_quantity#60, ss_list_price#62, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                 +- *(44) BroadcastHashJoin [ss_sold_date_sk#50], [d_date_sk#73], Inner, BuildRight
               :                    :- *(44) Project [ss_sold_date_sk#50, ss_quantity#60, ss_list_price#62, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :  +- *(44) BroadcastHashJoin [ss_item_sk#52], [i_item_sk#28], Inner, BuildRight
               :                    :     :- SortMergeJoin [ss_item_sk#52], [ss_item_sk#18], LeftSemi
               :                    :     :  :- *(2) Sort [ss_item_sk#52 ASC NULLS FIRST], false, 0
               :                    :     :  :  +- Exchange hashpartitioning(ss_item_sk#52, 200)
               :                    :     :  :     +- *(1) Project [ss_sold_date_sk#50, ss_item_sk#52, ss_quantity#60, ss_list_price#62]
               :                    :     :  :        +- *(1) Filter (isnotnull(ss_item_sk#52) && isnotnull(ss_sold_date_sk#50))
               :                    :     :  :           +- *(1) FileScan parquet tpcds.store_sales[ss_sold_date_sk#50,ss_item_sk#52,ss_quantity#60,ss_list_price#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_quantity:int,ss_list_price:double>
               :                    :     :  +- *(21) Sort [ss_item_sk#18 ASC NULLS FIRST], false, 0
               :                    :     :     +- Exchange hashpartitioning(ss_item_sk#18, 200)
               :                    :     :        +- *(20) Project [i_item_sk#28 AS ss_item_sk#18]
               :                    :     :           +- *(20) BroadcastHashJoin [i_brand_id#35, i_class_id#37, i_category_id#39], [brand_id#15, class_id#16, category_id#17], Inner, BuildLeft
               :                    :     :              :- BroadcastExchange HashedRelationBroadcastMode(List(input[1, int, true], input[2, int, true], input[3, int, true]))
               :                    :     :              :  +- *(3) Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :     :              :     +- *(3) Filter ((isnotnull(i_class_id#37) && isnotnull(i_category_id#39)) && isnotnull(i_brand_id#35))
               :                    :     :              :        +- *(3) FileScan parquet tpcds.item[i_item_sk#28,i_brand_id#35,i_class_id#37,i_category_id#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_class_id), IsNotNull(i_category_id), IsNotNull(i_brand_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
               :                    :     :              +- *(20) HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
               :                    :     :                 +- Exchange hashpartitioning(brand_id#15, class_id#16, category_id#17, 200)
               :                    :     :                    +- *(19) HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
               :                    :     :                       +- SortMergeJoin [coalesce(brand_id#15, 0), coalesce(class_id#16, 0), coalesce(category_id#17, 0)], [coalesce(i_brand_id#35, 0), coalesce(i_class_id#37, 0), coalesce(i_category_id#39, 0)], LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :                    :     :                          :- *(14) Sort [coalesce(brand_id#15, 0) ASC NULLS FIRST, coalesce(class_id#16, 0) ASC NULLS FIRST, coalesce(category_id#17, 0) ASC NULLS FIRST], false, 0
               :                    :     :                          :  +- Exchange hashpartitioning(coalesce(brand_id#15, 0), coalesce(class_id#16, 0), coalesce(category_id#17, 0), 200)
               :                    :     :                          :     +- *(13) HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
               :                    :     :                          :        +- Exchange hashpartitioning(brand_id#15, class_id#16, category_id#17, 200)
               :                    :     :                          :           +- *(12) HashAggregate(keys=[brand_id#15, class_id#16, category_id#17], functions=[], output=[brand_id#15, class_id#16, category_id#17])
               :                    :     :                          :              +- SortMergeJoin [coalesce(brand_id#15, 0), coalesce(class_id#16, 0), coalesce(category_id#17, 0)], [coalesce(i_brand_id#35, 0), coalesce(i_class_id#37, 0), coalesce(i_category_id#39, 0)], LeftSemi, (((brand_id#15 <=> i_brand_id#35) && (class_id#16 <=> i_class_id#37)) && (category_id#17 <=> i_category_id#39))
               :                    :     :                          :                 :- *(7) Sort [coalesce(brand_id#15, 0) ASC NULLS FIRST, coalesce(class_id#16, 0) ASC NULLS FIRST, coalesce(category_id#17, 0) ASC NULLS FIRST], false, 0
               :                    :     :                          :                 :  +- Exchange hashpartitioning(coalesce(brand_id#15, 0), coalesce(class_id#16, 0), coalesce(category_id#17, 0), 200)
               :                    :     :                          :                 :     +- *(6) Project [i_brand_id#35 AS brand_id#15, i_class_id#37 AS class_id#16, i_category_id#39 AS category_id#17]
               :                    :     :                          :                 :        +- *(6) BroadcastHashJoin [ss_sold_date_sk#50], [d_date_sk#73], Inner, BuildRight
               :                    :     :                          :                 :           :- *(6) Project [ss_sold_date_sk#50, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :     :                          :                 :           :  +- *(6) BroadcastHashJoin [ss_item_sk#52], [i_item_sk#28], Inner, BuildRight
               :                    :     :                          :                 :           :     :- *(6) Project [ss_sold_date_sk#50, ss_item_sk#52]
               :                    :     :                          :                 :           :     :  +- *(6) Filter (isnotnull(ss_item_sk#52) && isnotnull(ss_sold_date_sk#50))
               :                    :     :                          :                 :           :     :     +- *(6) FileScan parquet tpcds.store_sales[ss_sold_date_sk#50,ss_item_sk#52] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int>
               :                    :     :                          :                 :           :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                    :     :                          :                 :           :        +- *(4) Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :     :                          :                 :           :           +- *(4) Filter (((isnotnull(i_item_sk#28) && isnotnull(i_brand_id#35)) && isnotnull(i_class_id#37)) && isnotnull(i_category_id#39))
               :                    :     :                          :                 :           :              +- *(4) FileScan parquet tpcds.item[i_item_sk#28,i_brand_id#35,i_class_id#37,i_category_id#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk), IsNotNull(i_brand_id), IsNotNull(i_class_id), IsNotNull(i_category_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
               :                    :     :                          :                 :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                    :     :                          :                 :              +- *(5) Project [d_date_sk#73]
               :                    :     :                          :                 :                 +- *(5) Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :                    :     :                          :                 :                    +- *(5) FileScan parquet tpcds.date_dim[d_date_sk#73,d_year#79] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
               :                    :     :                          :                 +- *(11) Sort [coalesce(i_brand_id#35, 0) ASC NULLS FIRST, coalesce(i_class_id#37, 0) ASC NULLS FIRST, coalesce(i_category_id#39, 0) ASC NULLS FIRST], false, 0
               :                    :     :                          :                    +- Exchange hashpartitioning(coalesce(i_brand_id#35, 0), coalesce(i_class_id#37, 0), coalesce(i_category_id#39, 0), 200)
               :                    :     :                          :                       +- *(10) Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :     :                          :                          +- *(10) BroadcastHashJoin [cs_sold_date_sk#101], [d_date_sk#73], Inner, BuildRight
               :                    :     :                          :                             :- *(10) Project [cs_sold_date_sk#101, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :     :                          :                             :  +- *(10) BroadcastHashJoin [cs_item_sk#116], [i_item_sk#28], Inner, BuildRight
               :                    :     :                          :                             :     :- *(10) Project [cs_sold_date_sk#101, cs_item_sk#116]
               :                    :     :                          :                             :     :  +- *(10) Filter (isnotnull(cs_item_sk#116) && isnotnull(cs_sold_date_sk#101))
               :                    :     :                          :                             :     :     +- *(10) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#101,cs_item_sk#116] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int>
               :                    :     :                          :                             :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                    :     :                          :                             :        +- *(8) Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :     :                          :                             :           +- *(8) Filter isnotnull(i_item_sk#28)
               :                    :     :                          :                             :              +- *(8) FileScan parquet tpcds.item[i_item_sk#28,i_brand_id#35,i_class_id#37,i_category_id#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
               :                    :     :                          :                             +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                    :     :                          +- *(18) Sort [coalesce(i_brand_id#35, 0) ASC NULLS FIRST, coalesce(i_class_id#37, 0) ASC NULLS FIRST, coalesce(i_category_id#39, 0) ASC NULLS FIRST], false, 0
               :                    :     :                             +- Exchange hashpartitioning(coalesce(i_brand_id#35, 0), coalesce(i_class_id#37, 0), coalesce(i_category_id#39, 0), 200)
               :                    :     :                                +- *(17) Project [i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :     :                                   +- *(17) BroadcastHashJoin [ws_sold_date_sk#135], [d_date_sk#73], Inner, BuildRight
               :                    :     :                                      :- *(17) Project [ws_sold_date_sk#135, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :     :                                      :  +- *(17) BroadcastHashJoin [ws_item_sk#138], [i_item_sk#28], Inner, BuildRight
               :                    :     :                                      :     :- *(17) Project [ws_sold_date_sk#135, ws_item_sk#138]
               :                    :     :                                      :     :  +- *(17) Filter (isnotnull(ws_item_sk#138) && isnotnull(ws_sold_date_sk#135))
               :                    :     :                                      :     :     +- *(17) FileScan parquet tpcds.web_sales[ws_sold_date_sk#135,ws_item_sk#138] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int>
               :                    :     :                                      :     +- ReusedExchange [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                    :     :                                      +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                    :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                    :        +- SortMergeJoin [i_item_sk#28], [ss_item_sk#18], LeftSemi
               :                    :           :- *(23) Sort [i_item_sk#28 ASC NULLS FIRST], false, 0
               :                    :           :  +- Exchange hashpartitioning(i_item_sk#28, 200)
               :                    :           :     +- *(22) Project [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :           :        +- *(22) Filter isnotnull(i_item_sk#28)
               :                    :           :           +- *(22) FileScan parquet tpcds.item[i_item_sk#28,i_brand_id#35,i_class_id#37,i_category_id#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_class_id:int,i_category_id:int>
               :                    :           +- *(42) Sort [ss_item_sk#18 ASC NULLS FIRST], false, 0
               :                    :              +- ReusedExchange [ss_item_sk#18], Exchange hashpartitioning(ss_item_sk#18, 200)
               :                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                       +- *(43) Project [d_date_sk#73]
               :                          +- *(43) Filter ((((isnotnull(d_year#79) && isnotnull(d_moy#81)) && (d_year#79 = 2001)) && (d_moy#81 = 11)) && isnotnull(d_date_sk#73))
               :                             +- *(43) FileScan parquet tpcds.date_dim[d_date_sk#73,d_year#79,d_moy#81] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
               :- *(90) Project [sales#6, number_sales#7L, channel#5, i_brand_id#35, i_class_id#37, i_category_id#39]
               :  +- *(90) Filter (isnotnull(sum((cast(cs_quantity#119 as double) * cs_list_price#121))#181) && (sum((cast(cs_quantity#119 as double) * cs_list_price#121))#181 > Subquery subquery9))
               :     :  +- Subquery subquery9
               :     :     +- *(8) HashAggregate(keys=[], functions=[avg((cast(quantity#19 as double) * list_price#20))], output=[average_sales#25])
               :     :        +- Exchange SinglePartition
               :     :           +- *(7) HashAggregate(keys=[], functions=[partial_avg((cast(quantity#19 as double) * list_price#20))], output=[sum#246, count#247L])
               :     :              +- Union
               :     :                 :- *(2) Project [ss_quantity#60 AS quantity#19, ss_list_price#62 AS list_price#20]
               :     :                 :  +- *(2) BroadcastHashJoin [ss_sold_date_sk#50], [d_date_sk#73], Inner, BuildRight
               :     :                 :     :- *(2) Project [ss_sold_date_sk#50, ss_quantity#60, ss_list_price#62]
               :     :                 :     :  +- *(2) Filter isnotnull(ss_sold_date_sk#50)
               :     :                 :     :     +- *(2) FileScan parquet tpcds.store_sales[ss_sold_date_sk#50,ss_quantity#60,ss_list_price#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_quantity:int,ss_list_price:double>
               :     :                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :                 :        +- *(1) Project [d_date_sk#73]
               :     :                 :           +- *(1) Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
               :     :                 :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#73,d_year#79] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
               :     :                 :- *(4) Project [cs_quantity#119 AS quantity#21, cs_list_price#121 AS list_price#22]
               :     :                 :  +- *(4) BroadcastHashJoin [cs_sold_date_sk#101], [d_date_sk#73], Inner, BuildRight
               :     :                 :     :- *(4) Project [cs_sold_date_sk#101, cs_quantity#119, cs_list_price#121]
               :     :                 :     :  +- *(4) Filter isnotnull(cs_sold_date_sk#101)
               :     :                 :     :     +- *(4) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#101,cs_quantity#119,cs_list_price#121] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_quantity:int,cs_list_price:double>
               :     :                 :     +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :                 +- *(6) Project [ws_quantity#153 AS quantity#23, ws_list_price#155 AS list_price#24]
               :     :                    +- *(6) BroadcastHashJoin [ws_sold_date_sk#135], [d_date_sk#73], Inner, BuildRight
               :     :                       :- *(6) Project [ws_sold_date_sk#135, ws_quantity#153, ws_list_price#155]
               :     :                       :  +- *(6) Filter isnotnull(ws_sold_date_sk#135)
               :     :                       :     +- *(6) FileScan parquet tpcds.web_sales[ws_sold_date_sk#135,ws_quantity#153,ws_list_price#155] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_quantity:int,ws_list_price:double>
               :     :                       +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     +- *(90) HashAggregate(keys=[i_brand_id#35, i_class_id#37, i_category_id#39], functions=[sum((cast(cs_quantity#119 as double) * cs_list_price#121)), count(1)], output=[channel#5, i_brand_id#35, i_class_id#37, i_category_id#39, sales#6, number_sales#7L, sum((cast(cs_quantity#119 as double) * cs_list_price#121))#181])
               :        +- Exchange hashpartitioning(i_brand_id#35, i_class_id#37, i_category_id#39, 200)
               :           +- *(89) HashAggregate(keys=[i_brand_id#35, i_class_id#37, i_category_id#39], functions=[partial_sum((cast(cs_quantity#119 as double) * cs_list_price#121)), partial_count(1)], output=[i_brand_id#35, i_class_id#37, i_category_id#39, sum#238, count#239L])
               :              +- *(89) Project [cs_quantity#119, cs_list_price#121, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                 +- *(89) BroadcastHashJoin [cs_sold_date_sk#101], [d_date_sk#73], Inner, BuildRight
               :                    :- *(89) Project [cs_sold_date_sk#101, cs_quantity#119, cs_list_price#121, i_brand_id#35, i_class_id#37, i_category_id#39]
               :                    :  +- *(89) BroadcastHashJoin [cs_item_sk#116], [i_item_sk#28], Inner, BuildRight
               :                    :     :- SortMergeJoin [cs_item_sk#116], [ss_item_sk#18], LeftSemi
               :                    :     :  :- *(47) Sort [cs_item_sk#116 ASC NULLS FIRST], false, 0
               :                    :     :  :  +- Exchange hashpartitioning(cs_item_sk#116, 200)
               :                    :     :  :     +- *(46) Project [cs_sold_date_sk#101, cs_item_sk#116, cs_quantity#119, cs_list_price#121]
               :                    :     :  :        +- *(46) Filter (isnotnull(cs_item_sk#116) && isnotnull(cs_sold_date_sk#101))
               :                    :     :  :           +- *(46) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#101,cs_item_sk#116,cs_quantity#119,cs_list_price#121] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_item_sk:int,cs_quantity:int,cs_list_price:double>
               :                    :     :  +- *(66) Sort [ss_item_sk#18 ASC NULLS FIRST], false, 0
               :                    :     :     +- ReusedExchange [ss_item_sk#18], Exchange hashpartitioning(ss_item_sk#18, 200)
               :                    :     +- ReusedExchange [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :                    +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               +- *(135) Project [sales#11, number_sales#12L, channel#10, i_brand_id#35, i_class_id#37, i_category_id#39]
                  +- *(135) Filter (isnotnull(sum((cast(ws_quantity#153 as double) * ws_list_price#155))#184) && (sum((cast(ws_quantity#153 as double) * ws_list_price#155))#184 > Subquery subquery14))
                     :  +- Subquery subquery14
                     :     +- *(8) HashAggregate(keys=[], functions=[avg((cast(quantity#19 as double) * list_price#20))], output=[average_sales#25])
                     :        +- Exchange SinglePartition
                     :           +- *(7) HashAggregate(keys=[], functions=[partial_avg((cast(quantity#19 as double) * list_price#20))], output=[sum#246, count#247L])
                     :              +- Union
                     :                 :- *(2) Project [ss_quantity#60 AS quantity#19, ss_list_price#62 AS list_price#20]
                     :                 :  +- *(2) BroadcastHashJoin [ss_sold_date_sk#50], [d_date_sk#73], Inner, BuildRight
                     :                 :     :- *(2) Project [ss_sold_date_sk#50, ss_quantity#60, ss_list_price#62]
                     :                 :     :  +- *(2) Filter isnotnull(ss_sold_date_sk#50)
                     :                 :     :     +- *(2) FileScan parquet tpcds.store_sales[ss_sold_date_sk#50,ss_quantity#60,ss_list_price#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_quantity:int,ss_list_price:double>
                     :                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :                 :        +- *(1) Project [d_date_sk#73]
                     :                 :           +- *(1) Filter (((isnotnull(d_year#79) && (d_year#79 >= 1999)) && (d_year#79 <= 2001)) && isnotnull(d_date_sk#73))
                     :                 :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#73,d_year#79] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), GreaterThanOrEqual(d_year,1999), LessThanOrEqual(d_year,2001), IsNotNull(d_da..., ReadSchema: struct<d_date_sk:int,d_year:int>
                     :                 :- *(4) Project [cs_quantity#119 AS quantity#21, cs_list_price#121 AS list_price#22]
                     :                 :  +- *(4) BroadcastHashJoin [cs_sold_date_sk#101], [d_date_sk#73], Inner, BuildRight
                     :                 :     :- *(4) Project [cs_sold_date_sk#101, cs_quantity#119, cs_list_price#121]
                     :                 :     :  +- *(4) Filter isnotnull(cs_sold_date_sk#101)
                     :                 :     :     +- *(4) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#101,cs_quantity#119,cs_list_price#121] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_quantity:int,cs_list_price:double>
                     :                 :     +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :                 +- *(6) Project [ws_quantity#153 AS quantity#23, ws_list_price#155 AS list_price#24]
                     :                    +- *(6) BroadcastHashJoin [ws_sold_date_sk#135], [d_date_sk#73], Inner, BuildRight
                     :                       :- *(6) Project [ws_sold_date_sk#135, ws_quantity#153, ws_list_price#155]
                     :                       :  +- *(6) Filter isnotnull(ws_sold_date_sk#135)
                     :                       :     +- *(6) FileScan parquet tpcds.web_sales[ws_sold_date_sk#135,ws_quantity#153,ws_list_price#155] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_quantity:int,ws_list_price:double>
                     :                       +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     +- *(135) HashAggregate(keys=[i_brand_id#35, i_class_id#37, i_category_id#39], functions=[sum((cast(ws_quantity#153 as double) * ws_list_price#155)), count(1)], output=[channel#10, i_brand_id#35, i_class_id#37, i_category_id#39, sales#11, number_sales#12L, sum((cast(ws_quantity#153 as double) * ws_list_price#155))#184])
                        +- Exchange hashpartitioning(i_brand_id#35, i_class_id#37, i_category_id#39, 200)
                           +- *(134) HashAggregate(keys=[i_brand_id#35, i_class_id#37, i_category_id#39], functions=[partial_sum((cast(ws_quantity#153 as double) * ws_list_price#155)), partial_count(1)], output=[i_brand_id#35, i_class_id#37, i_category_id#39, sum#242, count#243L])
                              +- *(134) Project [ws_quantity#153, ws_list_price#155, i_brand_id#35, i_class_id#37, i_category_id#39]
                                 +- *(134) BroadcastHashJoin [ws_sold_date_sk#135], [d_date_sk#73], Inner, BuildRight
                                    :- *(134) Project [ws_sold_date_sk#135, ws_quantity#153, ws_list_price#155, i_brand_id#35, i_class_id#37, i_category_id#39]
                                    :  +- *(134) BroadcastHashJoin [ws_item_sk#138], [i_item_sk#28], Inner, BuildRight
                                    :     :- SortMergeJoin [ws_item_sk#138], [ss_item_sk#18], LeftSemi
                                    :     :  :- *(92) Sort [ws_item_sk#138 ASC NULLS FIRST], false, 0
                                    :     :  :  +- Exchange hashpartitioning(ws_item_sk#138, 200)
                                    :     :  :     +- *(91) Project [ws_sold_date_sk#135, ws_item_sk#138, ws_quantity#153, ws_list_price#155]
                                    :     :  :        +- *(91) Filter (isnotnull(ws_item_sk#138) && isnotnull(ws_sold_date_sk#135))
                                    :     :  :           +- *(91) FileScan parquet tpcds.web_sales[ws_sold_date_sk#135,ws_item_sk#138,ws_quantity#153,ws_list_price#155] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_quantity:int,ws_list_price:double>
                                    :     :  +- *(111) Sort [ss_item_sk#18 ASC NULLS FIRST], false, 0
                                    :     :     +- ReusedExchange [ss_item_sk#18], Exchange hashpartitioning(ss_item_sk#18, 200)
                                    :     +- ReusedExchange [i_item_sk#28, i_brand_id#35, i_class_id#37, i_category_id#39], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                    +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 8.428 seconds, Fetched 1 row(s)
store	1001001	1	1	372549.59	137	store	1001001	1	1	1316197.4300000002	340
store	1001002	1	1	799646.86	229	store	1001002	1	1	775776.6300000002	188
store	1002001	2	1	765555.9099999997	202	store	1002001	2	1	1218310.8200000003	337
store	1002002	2	1	1030666.6499999999	250	store	1002002	2	1	605616.7899999999	166
store	1003001	3	1	547136.9600000001	167	store	1003001	3	1	1172847.0699999998	341
store	1003002	3	1	674007.1199999999	188	store	1003002	3	1	607135.04	154
store	1004001	4	1	682747.23	187	store	1004001	4	1	1171699.7600000002	311
store	1004002	4	1	739362.1600000001	210	store	1004002	4	1	649347.1900000001	184
store	2001001	1	2	688211.0799999996	216	store	2001001	1	2	1342495.4500000004	365
store	2001002	1	2	906330.7000000001	237	store	2001002	1	2	541063.5	153
store	2002001	2	2	868177.8700000001	211	store	2002001	2	2	1514792.1900000002	402
store	2002002	2	2	648459.06	179	store	2002002	2	2	588040.22	159
store	2003001	3	2	559505.3499999999	151	store	2003001	3	2	1171821.55	300
store	2003002	3	2	746714.1100000001	219	store	2003002	3	2	695877.16	171
store	2004001	4	2	772046.7700000001	235	store	2004001	4	2	1371151.7	362
store	2004002	4	2	685029.2899999999	192	store	2004002	4	2	646760.3300000002	187
store	3001001	1	3	596684.0399999999	165	store	3001001	1	3	1059529.8099999998	312
store	3001002	1	3	795882.47	202	store	3001002	1	3	494739.47	142
store	3002001	2	3	661760.5799999998	181	store	3002001	2	3	1411948.7399999995	380
store	3002002	2	3	864250.1100000002	244	store	3002002	2	3	630689.3000000002	183
store	3003001	3	3	749054.1600000003	224	store	3003001	3	3	1513634.7099999995	381
store	3003002	3	3	737460.7100000001	194	store	3003002	3	3	713930.86	164
store	3004001	4	3	508491.02	161	store	3004001	4	3	1082215.4899999998	293
store	3004002	4	3	867362.8299999998	220	store	3004002	4	3	630978.5599999999	172
store	4001001	1	4	847845.3999999999	207	store	4001001	1	4	1291518.9699999995	340
store	4001002	1	4	841877.7999999998	232	store	4001002	1	4	520226.8599999998	143
store	4002001	2	4	543692.2	162	store	4002001	2	4	1378081.9000000004	362
store	4002002	2	4	912449.6799999998	256	store	4002002	2	4	713528.3799999998	200
store	4003001	3	4	725565.2200000001	189	store	4003001	3	4	1285387.4900000002	311
store	4003002	3	4	686780.8799999999	197	store	4003002	3	4	623656.8800000001	160
store	4004001	4	4	665419.7799999997	170	store	4004001	4	4	1141222.24	334
store	4004002	4	4	866534.1399999998	204	store	4004002	4	4	644963.9800000001	168
store	5001001	1	5	626379.58	167	store	5001001	1	5	1258495.8199999998	336
store	5001002	1	5	813592.7000000002	198	store	5001002	1	5	671017.1499999997	179
store	5002001	2	5	468954.69000000006	153	store	5002001	2	5	1195162.4199999995	297
store	5002002	2	5	885968.5999999997	211	store	5002002	2	5	691132.9899999999	183
store	5003001	3	5	709102.6400000001	206	store	5003001	3	5	1369869.3100000005	375
store	5003002	3	5	1019553.8700000002	266	store	5003002	3	5	671873.7200000002	181
store	5004001	4	5	801416.9699999996	219	store	5004001	4	5	1405232.1800000004	371
store	5004002	4	5	1061684.6399999997	273	store	5004002	4	5	790288.6299999999	209
store	6001001	1	6	24105.889999999996	9	store	6001001	1	6	43566.030000000006	11
store	6001002	1	6	40621.70999999999	10	store	6001002	1	6	31684.03	10
store	6001003	1	6	23365.0	8	store	6001003	1	6	25117.82	10
store	6001004	1	6	29458.479999999996	6	store	6001004	1	6	29630.4	7
store	6001005	1	6	40555.76	13	store	6001005	1	6	67204.16	14
store	6001006	1	6	49871.82	10	store	6001006	1	6	17872.8	6
store	6001007	1	6	23526.210000000003	8	store	6001007	1	6	34341.06	13
store	6001008	1	6	74782.9	20	store	6001008	1	6	127415.74000000002	33
store	6002001	2	6	55244.23	15	store	6002001	2	6	67018.71	20
store	6002002	2	6	24045.72	11	store	6002002	2	6	73776.49	17
store	6002004	2	6	45590.44	15	store	6002004	2	6	13288.01	5
store	6002005	2	6	53619.89	11	store	6002005	2	6	57765.0	12
store	6002006	2	6	36339.64	12	store	6002006	2	6	28988.989999999998	8
store	6002007	2	6	33520.91	9	store	6002007	2	6	55473.59	15
store	6002008	2	6	24710.010000000002	7	store	6002008	2	6	25085.33	11
store	6003001	3	6	33165.630000000005	9	store	6003001	3	6	84144.62	18
store	6003002	3	6	52293.57	15	store	6003002	3	6	59151.7	15
store	6003003	3	6	33342.979999999996	7	store	6003003	3	6	43720.12999999999	20
store	6003004	3	6	26344.41	9	store	6003004	3	6	30930.85	9
store	6003005	3	6	89998.03	26	store	6003005	3	6	95491.0	29
store	6003006	3	6	31862.34	10	store	6003006	3	6	24121.07	3
store	6003007	3	6	92405.02999999998	24	store	6003007	3	6	129219.45999999999	31
store	6003008	3	6	46145.619999999995	13	store	6003008	3	6	41948.280000000006	19
store	6004001	4	6	12839.97	5	store	6004001	4	6	74616.31999999999	16
store	6004002	4	6	33700.509999999995	11	store	6004002	4	6	45202.4	11
store	6004003	4	6	31387.04	12	store	6004003	4	6	103532.43999999999	21
store	6004004	4	6	42644.68	8	store	6004004	4	6	15287.71	6
store	6004005	4	6	15515.220000000001	3	store	6004005	4	6	60172.670000000006	16
store	6004006	4	6	60466.479999999996	15	store	6004006	4	6	38426.04	10
store	6004007	4	6	48158.78	12	store	6004007	4	6	55265.06	13
store	6004008	4	6	110528.92000000001	26	store	6004008	4	6	17409.989999999998	6
store	6005001	5	6	38589.67	11	store	6005001	5	6	135393.22999999998	42
store	6005002	5	6	47495.55	8	store	6005002	5	6	49267.29	12
store	6005003	5	6	14884.61	3	store	6005003	5	6	74851.15	16
store	6005005	5	6	37483.24	12	store	6005005	5	6	73876.81999999999	18
store	6005006	5	6	61699.19	17	store	6005006	5	6	52082.17	11
store	6005007	5	6	18764.89	5	store	6005007	5	6	48445.42999999999	16
store	6005008	5	6	25927.83	9	store	6005008	5	6	16473.68	7
store	6006001	6	6	36995.31	14	store	6006001	6	6	69739.81000000001	24
store	6006002	6	6	85385.16	22	store	6006002	6	6	87585.47	23
store	6006003	6	6	47063.869999999995	11	store	6006003	6	6	114431.48000000001	29
store	6006004	6	6	20527.5	8	store	6006004	6	6	23213.370000000003	3
store	6006005	6	6	7442.61	4	store	6006005	6	6	93912.14	18
store	6006006	6	6	25335.020000000004	8	store	6006006	6	6	24102.43	5
store	6006007	6	6	12901.85	6	store	6006007	6	6	107194.04000000001	30
store	6006008	6	6	39533.23	14	store	6006008	6	6	48680.369999999995	14
store	6007001	7	6	9032.59	3	store	6007001	7	6	15183.240000000002	6
store	6007002	7	6	57662.380000000005	13	store	6007002	7	6	42989.38999999999	13
store	6007003	7	6	44207.42	17	store	6007003	7	6	141091.04000000004	39
store	6007004	7	6	147787.95	29	store	6007004	7	6	69437.35	16
store	6007005	7	6	33681.869999999995	9	store	6007005	7	6	49926.37	15
store	6007006	7	6	50875.600000000006	16	store	6007006	7	6	75381.63	24
store	6007007	7	6	13622.220000000001	7	store	6007007	7	6	102650.94	31
store	6007008	7	6	34697.31	12	store	6007008	7	6	49841.130000000005	18
store	6008001	8	6	44993.33	10	store	6008001	8	6	57972.98	22
store	6008002	8	6	39437.31	8	store	6008002	8	6	48434.649999999994	14
store	6008003	8	6	59905.770000000004	14	store	6008003	8	6	60930.66	20
store	6008004	8	6	36676.25	9	store	6008004	8	6	31817.07	9
store	6008005	8	6	84238.28000000001	27	store	6008005	8	6	136071.7	27
store	6008007	8	6	14672.769999999999	7	store	6008007	8	6	59474.20999999999	18
Time taken: 28.281 seconds, Fetched 100 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 14 in stream 0 using template query14.tpl
------------------------------------------------------^^^

