== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['channel ASC NULLS FIRST, 'col_name ASC NULLS FIRST, 'd_year ASC NULLS FIRST, 'd_qoy ASC NULLS FIRST, 'i_category ASC NULLS FIRST], true
      +- 'Aggregate ['channel, 'col_name, 'd_year, 'd_qoy, 'i_category], ['channel, 'col_name, 'd_year, 'd_qoy, 'i_category, 'COUNT(1) AS sales_cnt#9, 'SUM('ext_sales_price) AS sales_amt#10]
         +- 'SubqueryAlias `foo`
            +- 'Union
               :- 'Union
               :  :- 'Project [store AS channel#0, ss_store_sk AS col_name#1, 'd_year, 'd_qoy, 'i_category, 'ss_ext_sales_price AS ext_sales_price#2]
               :  :  +- 'Filter ((isnull('ss_store_sk) && ('ss_sold_date_sk = 'd_date_sk)) && ('ss_item_sk = 'i_item_sk))
               :  :     +- 'Join Inner
               :  :        :- 'Join Inner
               :  :        :  :- 'UnresolvedRelation `store_sales`
               :  :        :  +- 'UnresolvedRelation `item`
               :  :        +- 'UnresolvedRelation `date_dim`
               :  +- 'Project [web AS channel#3, ws_ship_customer_sk AS col_name#4, 'd_year, 'd_qoy, 'i_category, 'ws_ext_sales_price AS ext_sales_price#5]
               :     +- 'Filter ((isnull('ws_ship_customer_sk) && ('ws_sold_date_sk = 'd_date_sk)) && ('ws_item_sk = 'i_item_sk))
               :        +- 'Join Inner
               :           :- 'Join Inner
               :           :  :- 'UnresolvedRelation `web_sales`
               :           :  +- 'UnresolvedRelation `item`
               :           +- 'UnresolvedRelation `date_dim`
               +- 'Project [catalog AS channel#6, cs_ship_addr_sk AS col_name#7, 'd_year, 'd_qoy, 'i_category, 'cs_ext_sales_price AS ext_sales_price#8]
                  +- 'Filter ((isnull('cs_ship_addr_sk) && ('cs_sold_date_sk = 'd_date_sk)) && ('cs_item_sk = 'i_item_sk))
                     +- 'Join Inner
                        :- 'Join Inner
                        :  :- 'UnresolvedRelation `catalog_sales`
                        :  +- 'UnresolvedRelation `item`
                        +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
channel: string, col_name: string, d_year: int, d_qoy: int, i_category: string, sales_cnt: bigint, sales_amt: double
GlobalLimit 100
+- LocalLimit 100
   +- Project [channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49, sales_cnt#9L, sales_amt#10]
      +- Sort [channel#0 ASC NULLS FIRST, col_name#1 ASC NULLS FIRST, d_year#65 ASC NULLS FIRST, d_qoy#69 ASC NULLS FIRST, i_category#49 ASC NULLS FIRST], true
         +- Aggregate [channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49], [channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49, count(1) AS sales_cnt#9L, sum(ext_sales_price#2) AS sales_amt#10]
            +- SubqueryAlias `foo`
               +- Union
                  :- Union
                  :  :- Project [store AS channel#0, ss_store_sk AS col_name#1, d_year#65, d_qoy#69, i_category#49, ss_ext_sales_price#29 AS ext_sales_price#2]
                  :  :  +- Filter ((isnull(ss_store_sk#21) && (ss_sold_date_sk#14 = d_date_sk#59)) && (ss_item_sk#16 = i_item_sk#37))
                  :  :     +- Join Inner
                  :  :        :- Join Inner
                  :  :        :  :- SubqueryAlias `tpcds`.`store_sales`
                  :  :        :  :  +- Relation[ss_sold_date_sk#14,ss_sold_time_sk#15,ss_item_sk#16,ss_customer_sk#17,ss_cdemo_sk#18,ss_hdemo_sk#19,ss_addr_sk#20,ss_store_sk#21,ss_promo_sk#22,ss_ticket_number#23,ss_quantity#24,ss_wholesale_cost#25,ss_list_price#26,ss_sales_price#27,ss_ext_discount_amt#28,ss_ext_sales_price#29,ss_ext_wholesale_cost#30,ss_ext_list_price#31,ss_ext_tax#32,ss_coupon_amt#33,ss_net_paid#34,ss_net_paid_inc_tax#35,ss_net_profit#36] parquet
                  :  :        :  +- SubqueryAlias `tpcds`.`item`
                  :  :        :     +- Relation[i_item_sk#37,i_item_id#38,i_rec_start_date#39,i_rec_end_date#40,i_item_desc#41,i_current_price#42,i_wholesale_cost#43,i_brand_id#44,i_brand#45,i_class_id#46,i_class#47,i_category_id#48,i_category#49,i_manufact_id#50,i_manufact#51,i_size#52,i_formulation#53,i_color#54,i_units#55,i_container#56,i_manager_id#57,i_product_name#58] parquet
                  :  :        +- SubqueryAlias `tpcds`.`date_dim`
                  :  :           +- Relation[d_date_sk#59,d_date_id#60,d_date#61,d_month_seq#62,d_week_seq#63,d_quarter_seq#64,d_year#65,d_dow#66,d_moy#67,d_dom#68,d_qoy#69,d_fy_year#70,d_fy_quarter_seq#71,d_fy_week_seq#72,d_day_name#73,d_quarter_name#74,d_holiday#75,d_weekend#76,d_following_holiday#77,d_first_dom#78,d_last_dom#79,d_same_day_ly#80,d_same_day_lq#81,d_current_day#82,... 4 more fields] parquet
                  :  +- Project [web AS channel#3, ws_ship_customer_sk AS col_name#4, d_year#65, d_qoy#69, i_category#49, ws_ext_sales_price#110 AS ext_sales_price#5]
                  :     +- Filter ((isnull(ws_ship_customer_sk#95) && (ws_sold_date_sk#87 = d_date_sk#59)) && (ws_item_sk#90 = i_item_sk#37))
                  :        +- Join Inner
                  :           :- Join Inner
                  :           :  :- SubqueryAlias `tpcds`.`web_sales`
                  :           :  :  +- Relation[ws_sold_date_sk#87,ws_sold_time_sk#88,ws_ship_date_sk#89,ws_item_sk#90,ws_bill_customer_sk#91,ws_bill_cdemo_sk#92,ws_bill_hdemo_sk#93,ws_bill_addr_sk#94,ws_ship_customer_sk#95,ws_ship_cdemo_sk#96,ws_ship_hdemo_sk#97,ws_ship_addr_sk#98,ws_web_page_sk#99,ws_web_site_sk#100,ws_ship_mode_sk#101,ws_warehouse_sk#102,ws_promo_sk#103,ws_order_number#104,ws_quantity#105,ws_wholesale_cost#106,ws_list_price#107,ws_sales_price#108,ws_ext_discount_amt#109,ws_ext_sales_price#110,... 10 more fields] parquet
                  :           :  +- SubqueryAlias `tpcds`.`item`
                  :           :     +- Relation[i_item_sk#37,i_item_id#38,i_rec_start_date#39,i_rec_end_date#40,i_item_desc#41,i_current_price#42,i_wholesale_cost#43,i_brand_id#44,i_brand#45,i_class_id#46,i_class#47,i_category_id#48,i_category#49,i_manufact_id#50,i_manufact#51,i_size#52,i_formulation#53,i_color#54,i_units#55,i_container#56,i_manager_id#57,i_product_name#58] parquet
                  :           +- SubqueryAlias `tpcds`.`date_dim`
                  :              +- Relation[d_date_sk#59,d_date_id#60,d_date#61,d_month_seq#62,d_week_seq#63,d_quarter_seq#64,d_year#65,d_dow#66,d_moy#67,d_dom#68,d_qoy#69,d_fy_year#70,d_fy_quarter_seq#71,d_fy_week_seq#72,d_day_name#73,d_quarter_name#74,d_holiday#75,d_weekend#76,d_following_holiday#77,d_first_dom#78,d_last_dom#79,d_same_day_ly#80,d_same_day_lq#81,d_current_day#82,... 4 more fields] parquet
                  +- Project [catalog AS channel#6, cs_ship_addr_sk AS col_name#7, d_year#65, d_qoy#69, i_category#49, cs_ext_sales_price#144 AS ext_sales_price#8]
                     +- Filter ((isnull(cs_ship_addr_sk#131) && (cs_sold_date_sk#121 = d_date_sk#59)) && (cs_item_sk#136 = i_item_sk#37))
                        +- Join Inner
                           :- Join Inner
                           :  :- SubqueryAlias `tpcds`.`catalog_sales`
                           :  :  +- Relation[cs_sold_date_sk#121,cs_sold_time_sk#122,cs_ship_date_sk#123,cs_bill_customer_sk#124,cs_bill_cdemo_sk#125,cs_bill_hdemo_sk#126,cs_bill_addr_sk#127,cs_ship_customer_sk#128,cs_ship_cdemo_sk#129,cs_ship_hdemo_sk#130,cs_ship_addr_sk#131,cs_call_center_sk#132,cs_catalog_page_sk#133,cs_ship_mode_sk#134,cs_warehouse_sk#135,cs_item_sk#136,cs_promo_sk#137,cs_order_number#138,cs_quantity#139,cs_wholesale_cost#140,cs_list_price#141,cs_sales_price#142,cs_ext_discount_amt#143,cs_ext_sales_price#144,... 10 more fields] parquet
                           :  +- SubqueryAlias `tpcds`.`item`
                           :     +- Relation[i_item_sk#37,i_item_id#38,i_rec_start_date#39,i_rec_end_date#40,i_item_desc#41,i_current_price#42,i_wholesale_cost#43,i_brand_id#44,i_brand#45,i_class_id#46,i_class#47,i_category_id#48,i_category#49,i_manufact_id#50,i_manufact#51,i_size#52,i_formulation#53,i_color#54,i_units#55,i_container#56,i_manager_id#57,i_product_name#58] parquet
                           +- SubqueryAlias `tpcds`.`date_dim`
                              +- Relation[d_date_sk#59,d_date_id#60,d_date#61,d_month_seq#62,d_week_seq#63,d_quarter_seq#64,d_year#65,d_dow#66,d_moy#67,d_dom#68,d_qoy#69,d_fy_year#70,d_fy_quarter_seq#71,d_fy_week_seq#72,d_day_name#73,d_quarter_name#74,d_holiday#75,d_weekend#76,d_following_holiday#77,d_first_dom#78,d_last_dom#79,d_same_day_ly#80,d_same_day_lq#81,d_current_day#82,... 4 more fields] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [channel#0 ASC NULLS FIRST, col_name#1 ASC NULLS FIRST, d_year#65 ASC NULLS FIRST, d_qoy#69 ASC NULLS FIRST, i_category#49 ASC NULLS FIRST], true
      +- Aggregate [channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49], [channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49, count(1) AS sales_cnt#9L, sum(ext_sales_price#2) AS sales_amt#10]
         +- Union
            :- Project [store AS channel#0, ss_store_sk AS col_name#1, d_year#65, d_qoy#69, i_category#49, ss_ext_sales_price#29 AS ext_sales_price#2]
            :  +- Join Inner, (ss_sold_date_sk#14 = d_date_sk#59)
            :     :- Project [ss_sold_date_sk#14, ss_ext_sales_price#29, i_category#49]
            :     :  +- Join Inner, (ss_item_sk#16 = i_item_sk#37)
            :     :     :- Project [ss_sold_date_sk#14, ss_item_sk#16, ss_ext_sales_price#29]
            :     :     :  +- Filter ((isnull(ss_store_sk#21) && isnotnull(ss_item_sk#16)) && isnotnull(ss_sold_date_sk#14))
            :     :     :     +- Relation[ss_sold_date_sk#14,ss_sold_time_sk#15,ss_item_sk#16,ss_customer_sk#17,ss_cdemo_sk#18,ss_hdemo_sk#19,ss_addr_sk#20,ss_store_sk#21,ss_promo_sk#22,ss_ticket_number#23,ss_quantity#24,ss_wholesale_cost#25,ss_list_price#26,ss_sales_price#27,ss_ext_discount_amt#28,ss_ext_sales_price#29,ss_ext_wholesale_cost#30,ss_ext_list_price#31,ss_ext_tax#32,ss_coupon_amt#33,ss_net_paid#34,ss_net_paid_inc_tax#35,ss_net_profit#36] parquet
            :     :     +- Project [i_item_sk#37, i_category#49]
            :     :        +- Filter isnotnull(i_item_sk#37)
            :     :           +- Relation[i_item_sk#37,i_item_id#38,i_rec_start_date#39,i_rec_end_date#40,i_item_desc#41,i_current_price#42,i_wholesale_cost#43,i_brand_id#44,i_brand#45,i_class_id#46,i_class#47,i_category_id#48,i_category#49,i_manufact_id#50,i_manufact#51,i_size#52,i_formulation#53,i_color#54,i_units#55,i_container#56,i_manager_id#57,i_product_name#58] parquet
            :     +- Project [d_date_sk#59, d_year#65, d_qoy#69]
            :        +- Filter isnotnull(d_date_sk#59)
            :           +- Relation[d_date_sk#59,d_date_id#60,d_date#61,d_month_seq#62,d_week_seq#63,d_quarter_seq#64,d_year#65,d_dow#66,d_moy#67,d_dom#68,d_qoy#69,d_fy_year#70,d_fy_quarter_seq#71,d_fy_week_seq#72,d_day_name#73,d_quarter_name#74,d_holiday#75,d_weekend#76,d_following_holiday#77,d_first_dom#78,d_last_dom#79,d_same_day_ly#80,d_same_day_lq#81,d_current_day#82,... 4 more fields] parquet
            :- Project [web AS channel#3, ws_ship_customer_sk AS col_name#4, d_year#65, d_qoy#69, i_category#49, ws_ext_sales_price#110 AS ext_sales_price#5]
            :  +- Join Inner, (ws_sold_date_sk#87 = d_date_sk#59)
            :     :- Project [ws_sold_date_sk#87, ws_ext_sales_price#110, i_category#49]
            :     :  +- Join Inner, (ws_item_sk#90 = i_item_sk#37)
            :     :     :- Project [ws_sold_date_sk#87, ws_item_sk#90, ws_ext_sales_price#110]
            :     :     :  +- Filter ((isnull(ws_ship_customer_sk#95) && isnotnull(ws_item_sk#90)) && isnotnull(ws_sold_date_sk#87))
            :     :     :     +- Relation[ws_sold_date_sk#87,ws_sold_time_sk#88,ws_ship_date_sk#89,ws_item_sk#90,ws_bill_customer_sk#91,ws_bill_cdemo_sk#92,ws_bill_hdemo_sk#93,ws_bill_addr_sk#94,ws_ship_customer_sk#95,ws_ship_cdemo_sk#96,ws_ship_hdemo_sk#97,ws_ship_addr_sk#98,ws_web_page_sk#99,ws_web_site_sk#100,ws_ship_mode_sk#101,ws_warehouse_sk#102,ws_promo_sk#103,ws_order_number#104,ws_quantity#105,ws_wholesale_cost#106,ws_list_price#107,ws_sales_price#108,ws_ext_discount_amt#109,ws_ext_sales_price#110,... 10 more fields] parquet
            :     :     +- Project [i_item_sk#37, i_category#49]
            :     :        +- Filter isnotnull(i_item_sk#37)
            :     :           +- Relation[i_item_sk#37,i_item_id#38,i_rec_start_date#39,i_rec_end_date#40,i_item_desc#41,i_current_price#42,i_wholesale_cost#43,i_brand_id#44,i_brand#45,i_class_id#46,i_class#47,i_category_id#48,i_category#49,i_manufact_id#50,i_manufact#51,i_size#52,i_formulation#53,i_color#54,i_units#55,i_container#56,i_manager_id#57,i_product_name#58] parquet
            :     +- Project [d_date_sk#59, d_year#65, d_qoy#69]
            :        +- Filter isnotnull(d_date_sk#59)
            :           +- Relation[d_date_sk#59,d_date_id#60,d_date#61,d_month_seq#62,d_week_seq#63,d_quarter_seq#64,d_year#65,d_dow#66,d_moy#67,d_dom#68,d_qoy#69,d_fy_year#70,d_fy_quarter_seq#71,d_fy_week_seq#72,d_day_name#73,d_quarter_name#74,d_holiday#75,d_weekend#76,d_following_holiday#77,d_first_dom#78,d_last_dom#79,d_same_day_ly#80,d_same_day_lq#81,d_current_day#82,... 4 more fields] parquet
            +- Project [catalog AS channel#6, cs_ship_addr_sk AS col_name#7, d_year#65, d_qoy#69, i_category#49, cs_ext_sales_price#144 AS ext_sales_price#8]
               +- Join Inner, (cs_sold_date_sk#121 = d_date_sk#59)
                  :- Project [cs_sold_date_sk#121, cs_ext_sales_price#144, i_category#49]
                  :  +- Join Inner, (cs_item_sk#136 = i_item_sk#37)
                  :     :- Project [cs_sold_date_sk#121, cs_item_sk#136, cs_ext_sales_price#144]
                  :     :  +- Filter ((isnull(cs_ship_addr_sk#131) && isnotnull(cs_item_sk#136)) && isnotnull(cs_sold_date_sk#121))
                  :     :     +- Relation[cs_sold_date_sk#121,cs_sold_time_sk#122,cs_ship_date_sk#123,cs_bill_customer_sk#124,cs_bill_cdemo_sk#125,cs_bill_hdemo_sk#126,cs_bill_addr_sk#127,cs_ship_customer_sk#128,cs_ship_cdemo_sk#129,cs_ship_hdemo_sk#130,cs_ship_addr_sk#131,cs_call_center_sk#132,cs_catalog_page_sk#133,cs_ship_mode_sk#134,cs_warehouse_sk#135,cs_item_sk#136,cs_promo_sk#137,cs_order_number#138,cs_quantity#139,cs_wholesale_cost#140,cs_list_price#141,cs_sales_price#142,cs_ext_discount_amt#143,cs_ext_sales_price#144,... 10 more fields] parquet
                  :     +- Project [i_item_sk#37, i_category#49]
                  :        +- Filter isnotnull(i_item_sk#37)
                  :           +- Relation[i_item_sk#37,i_item_id#38,i_rec_start_date#39,i_rec_end_date#40,i_item_desc#41,i_current_price#42,i_wholesale_cost#43,i_brand_id#44,i_brand#45,i_class_id#46,i_class#47,i_category_id#48,i_category#49,i_manufact_id#50,i_manufact#51,i_size#52,i_formulation#53,i_color#54,i_units#55,i_container#56,i_manager_id#57,i_product_name#58] parquet
                  +- Project [d_date_sk#59, d_year#65, d_qoy#69]
                     +- Filter isnotnull(d_date_sk#59)
                        +- Relation[d_date_sk#59,d_date_id#60,d_date#61,d_month_seq#62,d_week_seq#63,d_quarter_seq#64,d_year#65,d_dow#66,d_moy#67,d_dom#68,d_qoy#69,d_fy_year#70,d_fy_quarter_seq#71,d_fy_week_seq#72,d_day_name#73,d_quarter_name#74,d_holiday#75,d_weekend#76,d_following_holiday#77,d_first_dom#78,d_last_dom#79,d_same_day_ly#80,d_same_day_lq#81,d_current_day#82,... 4 more fields] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[channel#0 ASC NULLS FIRST,col_name#1 ASC NULLS FIRST,d_year#65 ASC NULLS FIRST,d_qoy#69 ASC NULLS FIRST,i_category#49 ASC NULLS FIRST], output=[channel#0,col_name#1,d_year#65,d_qoy#69,i_category#49,sales_cnt#9L,sales_amt#10])
+- *(11) HashAggregate(keys=[channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49], functions=[count(1), sum(ext_sales_price#2)], output=[channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49, sales_cnt#9L, sales_amt#10])
   +- Exchange hashpartitioning(channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49, 200)
      +- *(10) HashAggregate(keys=[channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49], functions=[partial_count(1), partial_sum(ext_sales_price#2)], output=[channel#0, col_name#1, d_year#65, d_qoy#69, i_category#49, count#163L, sum#164])
         +- Union
            :- *(3) Project [store AS channel#0, ss_store_sk AS col_name#1, d_year#65, d_qoy#69, i_category#49, ss_ext_sales_price#29 AS ext_sales_price#2]
            :  +- *(3) BroadcastHashJoin [ss_sold_date_sk#14], [d_date_sk#59], Inner, BuildRight
            :     :- *(3) Project [ss_sold_date_sk#14, ss_ext_sales_price#29, i_category#49]
            :     :  +- *(3) BroadcastHashJoin [ss_item_sk#16], [i_item_sk#37], Inner, BuildRight
            :     :     :- *(3) Project [ss_sold_date_sk#14, ss_item_sk#16, ss_ext_sales_price#29]
            :     :     :  +- *(3) Filter ((isnull(ss_store_sk#21) && isnotnull(ss_item_sk#16)) && isnotnull(ss_sold_date_sk#14))
            :     :     :     +- *(3) FileScan parquet tpcds.store_sales[ss_sold_date_sk#14,ss_item_sk#16,ss_store_sk#21,ss_ext_sales_price#29] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNull(ss_store_sk), IsNotNull(ss_item_sk), IsNotNull(ss_sold_date_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_store_sk:int,ss_ext_sales_price:double>
            :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     :        +- *(1) Project [i_item_sk#37, i_category#49]
            :     :           +- *(1) Filter isnotnull(i_item_sk#37)
            :     :              +- *(1) FileScan parquet tpcds.item[i_item_sk#37,i_category#49] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_category:string>
            :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :        +- *(2) Project [d_date_sk#59, d_year#65, d_qoy#69]
            :           +- *(2) Filter isnotnull(d_date_sk#59)
            :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#59,d_year#65,d_qoy#69] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
            :- *(6) Project [web AS channel#3, ws_ship_customer_sk AS col_name#4, d_year#65, d_qoy#69, i_category#49, ws_ext_sales_price#110 AS ext_sales_price#5]
            :  +- *(6) BroadcastHashJoin [ws_sold_date_sk#87], [d_date_sk#59], Inner, BuildRight
            :     :- *(6) Project [ws_sold_date_sk#87, ws_ext_sales_price#110, i_category#49]
            :     :  +- *(6) BroadcastHashJoin [ws_item_sk#90], [i_item_sk#37], Inner, BuildRight
            :     :     :- *(6) Project [ws_sold_date_sk#87, ws_item_sk#90, ws_ext_sales_price#110]
            :     :     :  +- *(6) Filter ((isnull(ws_ship_customer_sk#95) && isnotnull(ws_item_sk#90)) && isnotnull(ws_sold_date_sk#87))
            :     :     :     +- *(6) FileScan parquet tpcds.web_sales[ws_sold_date_sk#87,ws_item_sk#90,ws_ship_customer_sk#95,ws_ext_sales_price#110] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNull(ws_ship_customer_sk), IsNotNull(ws_item_sk), IsNotNull(ws_sold_date_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_ship_customer_sk:int,ws_ext_sales_price:double>
            :     :     +- ReusedExchange [i_item_sk#37, i_category#49], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            :     +- ReusedExchange [d_date_sk#59, d_year#65, d_qoy#69], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
            +- *(9) Project [catalog AS channel#6, cs_ship_addr_sk AS col_name#7, d_year#65, d_qoy#69, i_category#49, cs_ext_sales_price#144 AS ext_sales_price#8]
               +- *(9) BroadcastHashJoin [cs_sold_date_sk#121], [d_date_sk#59], Inner, BuildRight
                  :- *(9) Project [cs_sold_date_sk#121, cs_ext_sales_price#144, i_category#49]
                  :  +- *(9) BroadcastHashJoin [cs_item_sk#136], [i_item_sk#37], Inner, BuildRight
                  :     :- *(9) Project [cs_sold_date_sk#121, cs_item_sk#136, cs_ext_sales_price#144]
                  :     :  +- *(9) Filter ((isnull(cs_ship_addr_sk#131) && isnotnull(cs_item_sk#136)) && isnotnull(cs_sold_date_sk#121))
                  :     :     +- *(9) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#121,cs_ship_addr_sk#131,cs_item_sk#136,cs_ext_sales_price#144] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNull(cs_ship_addr_sk), IsNotNull(cs_item_sk), IsNotNull(cs_sold_date_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_ship_addr_sk:int,cs_item_sk:int,cs_ext_sales_price:double>
                  :     +- ReusedExchange [i_item_sk#37, i_category#49], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- ReusedExchange [d_date_sk#59, d_year#65, d_qoy#69], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
Time taken: 4.144 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 76 in stream 0 using template query76.tpl
------------------------------------------------------^^^

