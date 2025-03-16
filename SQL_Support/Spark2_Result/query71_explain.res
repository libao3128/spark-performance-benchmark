== Parsed Logical Plan ==
'Sort ['ext_price DESC NULLS LAST, 'i_brand_id ASC NULLS FIRST], true
+- 'Aggregate ['i_brand, 'i_brand_id, 't_hour, 't_minute], ['i_brand_id AS brand_id#12, 'i_brand AS brand#13, 't_hour, 't_minute, 'sum('ext_price) AS ext_price#14]
   +- 'Filter ((('sold_item_sk = 'i_item_sk) && ('i_manager_id = 1)) && (('time_sk = 't_time_sk) && (('t_meal_time = breakfast) || ('t_meal_time = dinner))))
      +- 'Join Inner
         :- 'Join Inner
         :  :- 'UnresolvedRelation `item`
         :  +- 'SubqueryAlias `tmp`
         :     +- 'Union
         :        :- 'Union
         :        :  :- 'Project ['ws_ext_sales_price AS ext_price#0, 'ws_sold_date_sk AS sold_date_sk#1, 'ws_item_sk AS sold_item_sk#2, 'ws_sold_time_sk AS time_sk#3]
         :        :  :  +- 'Filter ((('d_date_sk = 'ws_sold_date_sk) && ('d_moy = 11)) && ('d_year = 1999))
         :        :  :     +- 'Join Inner
         :        :  :        :- 'UnresolvedRelation `web_sales`
         :        :  :        +- 'UnresolvedRelation `date_dim`
         :        :  +- 'Project ['cs_ext_sales_price AS ext_price#4, 'cs_sold_date_sk AS sold_date_sk#5, 'cs_item_sk AS sold_item_sk#6, 'cs_sold_time_sk AS time_sk#7]
         :        :     +- 'Filter ((('d_date_sk = 'cs_sold_date_sk) && ('d_moy = 11)) && ('d_year = 1999))
         :        :        +- 'Join Inner
         :        :           :- 'UnresolvedRelation `catalog_sales`
         :        :           +- 'UnresolvedRelation `date_dim`
         :        +- 'Project ['ss_ext_sales_price AS ext_price#8, 'ss_sold_date_sk AS sold_date_sk#9, 'ss_item_sk AS sold_item_sk#10, 'ss_sold_time_sk AS time_sk#11]
         :           +- 'Filter ((('d_date_sk = 'ss_sold_date_sk) && ('d_moy = 11)) && ('d_year = 1999))
         :              +- 'Join Inner
         :                 :- 'UnresolvedRelation `store_sales`
         :                 +- 'UnresolvedRelation `date_dim`
         +- 'UnresolvedRelation `time_dim`

== Analyzed Logical Plan ==
brand_id: int, brand: string, t_hour: int, t_minute: int, ext_price: double
Project [brand_id#12, brand#13, t_hour#161, t_minute#162, ext_price#14]
+- Sort [ext_price#14 DESC NULLS LAST, brand_id#12 ASC NULLS FIRST], true
   +- Aggregate [i_brand#25, i_brand_id#24, t_hour#161, t_minute#162], [i_brand_id#24 AS brand_id#12, i_brand#25 AS brand#13, t_hour#161, t_minute#162, sum(ext_price#0) AS ext_price#14]
      +- Filter (((sold_item_sk#2 = i_item_sk#17) && (i_manager_id#37 = 1)) && ((time_sk#3 = t_time_sk#158) && ((t_meal_time#167 = breakfast) || (t_meal_time#167 = dinner))))
         +- Join Inner
            :- Join Inner
            :  :- SubqueryAlias `tpcds`.`item`
            :  :  +- Relation[i_item_sk#17,i_item_id#18,i_rec_start_date#19,i_rec_end_date#20,i_item_desc#21,i_current_price#22,i_wholesale_cost#23,i_brand_id#24,i_brand#25,i_class_id#26,i_class#27,i_category_id#28,i_category#29,i_manufact_id#30,i_manufact#31,i_size#32,i_formulation#33,i_color#34,i_units#35,i_container#36,i_manager_id#37,i_product_name#38] parquet
            :  +- SubqueryAlias `tmp`
            :     +- Union
            :        :- Union
            :        :  :- Project [ws_ext_sales_price#62 AS ext_price#0, ws_sold_date_sk#39 AS sold_date_sk#1, ws_item_sk#42 AS sold_item_sk#2, ws_sold_time_sk#40 AS time_sk#3]
            :        :  :  +- Filter (((d_date_sk#73 = ws_sold_date_sk#39) && (d_moy#81 = 11)) && (d_year#79 = 1999))
            :        :  :     +- Join Inner
            :        :  :        :- SubqueryAlias `tpcds`.`web_sales`
            :        :  :        :  +- Relation[ws_sold_date_sk#39,ws_sold_time_sk#40,ws_ship_date_sk#41,ws_item_sk#42,ws_bill_customer_sk#43,ws_bill_cdemo_sk#44,ws_bill_hdemo_sk#45,ws_bill_addr_sk#46,ws_ship_customer_sk#47,ws_ship_cdemo_sk#48,ws_ship_hdemo_sk#49,ws_ship_addr_sk#50,ws_web_page_sk#51,ws_web_site_sk#52,ws_ship_mode_sk#53,ws_warehouse_sk#54,ws_promo_sk#55,ws_order_number#56,ws_quantity#57,ws_wholesale_cost#58,ws_list_price#59,ws_sales_price#60,ws_ext_discount_amt#61,ws_ext_sales_price#62,... 10 more fields] parquet
            :        :  :        +- SubqueryAlias `tpcds`.`date_dim`
            :        :  :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
            :        :  +- Project [cs_ext_sales_price#124 AS ext_price#4, cs_sold_date_sk#101 AS sold_date_sk#5, cs_item_sk#116 AS sold_item_sk#6, cs_sold_time_sk#102 AS time_sk#7]
            :        :     +- Filter (((d_date_sk#73 = cs_sold_date_sk#101) && (d_moy#81 = 11)) && (d_year#79 = 1999))
            :        :        +- Join Inner
            :        :           :- SubqueryAlias `tpcds`.`catalog_sales`
            :        :           :  +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
            :        :           +- SubqueryAlias `tpcds`.`date_dim`
            :        :              +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
            :        +- Project [ss_ext_sales_price#150 AS ext_price#8, ss_sold_date_sk#135 AS sold_date_sk#9, ss_item_sk#137 AS sold_item_sk#10, ss_sold_time_sk#136 AS time_sk#11]
            :           +- Filter (((d_date_sk#73 = ss_sold_date_sk#135) && (d_moy#81 = 11)) && (d_year#79 = 1999))
            :              +- Join Inner
            :                 :- SubqueryAlias `tpcds`.`store_sales`
            :                 :  +- Relation[ss_sold_date_sk#135,ss_sold_time_sk#136,ss_item_sk#137,ss_customer_sk#138,ss_cdemo_sk#139,ss_hdemo_sk#140,ss_addr_sk#141,ss_store_sk#142,ss_promo_sk#143,ss_ticket_number#144,ss_quantity#145,ss_wholesale_cost#146,ss_list_price#147,ss_sales_price#148,ss_ext_discount_amt#149,ss_ext_sales_price#150,ss_ext_wholesale_cost#151,ss_ext_list_price#152,ss_ext_tax#153,ss_coupon_amt#154,ss_net_paid#155,ss_net_paid_inc_tax#156,ss_net_profit#157] parquet
            :                 +- SubqueryAlias `tpcds`.`date_dim`
            :                    +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
            +- SubqueryAlias `tpcds`.`time_dim`
               +- Relation[t_time_sk#158,t_time_id#159,t_time#160,t_hour#161,t_minute#162,t_second#163,t_am_pm#164,t_shift#165,t_sub_shift#166,t_meal_time#167] parquet

== Optimized Logical Plan ==
Sort [ext_price#14 DESC NULLS LAST, brand_id#12 ASC NULLS FIRST], true
+- Aggregate [i_brand#25, i_brand_id#24, t_hour#161, t_minute#162], [i_brand_id#24 AS brand_id#12, i_brand#25 AS brand#13, t_hour#161, t_minute#162, sum(ext_price#0) AS ext_price#14]
   +- Project [i_brand_id#24, i_brand#25, ext_price#0, t_hour#161, t_minute#162]
      +- Join Inner, (time_sk#3 = t_time_sk#158)
         :- Project [i_brand_id#24, i_brand#25, ext_price#0, time_sk#3]
         :  +- Join Inner, (sold_item_sk#2 = i_item_sk#17)
         :     :- Project [i_item_sk#17, i_brand_id#24, i_brand#25]
         :     :  +- Filter ((isnotnull(i_manager_id#37) && (i_manager_id#37 = 1)) && isnotnull(i_item_sk#17))
         :     :     +- Relation[i_item_sk#17,i_item_id#18,i_rec_start_date#19,i_rec_end_date#20,i_item_desc#21,i_current_price#22,i_wholesale_cost#23,i_brand_id#24,i_brand#25,i_class_id#26,i_class#27,i_category_id#28,i_category#29,i_manufact_id#30,i_manufact#31,i_size#32,i_formulation#33,i_color#34,i_units#35,i_container#36,i_manager_id#37,i_product_name#38] parquet
         :     +- Union
         :        :- Project [ws_ext_sales_price#62 AS ext_price#0, ws_item_sk#42 AS sold_item_sk#2, ws_sold_time_sk#40 AS time_sk#3]
         :        :  +- Join Inner, (d_date_sk#73 = ws_sold_date_sk#39)
         :        :     :- Project [ws_sold_date_sk#39, ws_sold_time_sk#40, ws_item_sk#42, ws_ext_sales_price#62]
         :        :     :  +- Filter ((isnotnull(ws_sold_date_sk#39) && isnotnull(ws_item_sk#42)) && isnotnull(ws_sold_time_sk#40))
         :        :     :     +- Relation[ws_sold_date_sk#39,ws_sold_time_sk#40,ws_ship_date_sk#41,ws_item_sk#42,ws_bill_customer_sk#43,ws_bill_cdemo_sk#44,ws_bill_hdemo_sk#45,ws_bill_addr_sk#46,ws_ship_customer_sk#47,ws_ship_cdemo_sk#48,ws_ship_hdemo_sk#49,ws_ship_addr_sk#50,ws_web_page_sk#51,ws_web_site_sk#52,ws_ship_mode_sk#53,ws_warehouse_sk#54,ws_promo_sk#55,ws_order_number#56,ws_quantity#57,ws_wholesale_cost#58,ws_list_price#59,ws_sales_price#60,ws_ext_discount_amt#61,ws_ext_sales_price#62,... 10 more fields] parquet
         :        :     +- Project [d_date_sk#73]
         :        :        +- Filter ((((isnotnull(d_moy#81) && isnotnull(d_year#79)) && (d_moy#81 = 11)) && (d_year#79 = 1999)) && isnotnull(d_date_sk#73))
         :        :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
         :        :- Project [cs_ext_sales_price#124 AS ext_price#4, cs_item_sk#116 AS sold_item_sk#6, cs_sold_time_sk#102 AS time_sk#7]
         :        :  +- Join Inner, (d_date_sk#73 = cs_sold_date_sk#101)
         :        :     :- Project [cs_sold_date_sk#101, cs_sold_time_sk#102, cs_item_sk#116, cs_ext_sales_price#124]
         :        :     :  +- Filter ((isnotnull(cs_sold_date_sk#101) && isnotnull(cs_item_sk#116)) && isnotnull(cs_sold_time_sk#102))
         :        :     :     +- Relation[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_ship_date_sk#103,cs_bill_customer_sk#104,cs_bill_cdemo_sk#105,cs_bill_hdemo_sk#106,cs_bill_addr_sk#107,cs_ship_customer_sk#108,cs_ship_cdemo_sk#109,cs_ship_hdemo_sk#110,cs_ship_addr_sk#111,cs_call_center_sk#112,cs_catalog_page_sk#113,cs_ship_mode_sk#114,cs_warehouse_sk#115,cs_item_sk#116,cs_promo_sk#117,cs_order_number#118,cs_quantity#119,cs_wholesale_cost#120,cs_list_price#121,cs_sales_price#122,cs_ext_discount_amt#123,cs_ext_sales_price#124,... 10 more fields] parquet
         :        :     +- Project [d_date_sk#73]
         :        :        +- Filter ((((isnotnull(d_moy#81) && isnotnull(d_year#79)) && (d_moy#81 = 11)) && (d_year#79 = 1999)) && isnotnull(d_date_sk#73))
         :        :           +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
         :        +- Project [ss_ext_sales_price#150 AS ext_price#8, ss_item_sk#137 AS sold_item_sk#10, ss_sold_time_sk#136 AS time_sk#11]
         :           +- Join Inner, (d_date_sk#73 = ss_sold_date_sk#135)
         :              :- Project [ss_sold_date_sk#135, ss_sold_time_sk#136, ss_item_sk#137, ss_ext_sales_price#150]
         :              :  +- Filter ((isnotnull(ss_sold_date_sk#135) && isnotnull(ss_item_sk#137)) && isnotnull(ss_sold_time_sk#136))
         :              :     +- Relation[ss_sold_date_sk#135,ss_sold_time_sk#136,ss_item_sk#137,ss_customer_sk#138,ss_cdemo_sk#139,ss_hdemo_sk#140,ss_addr_sk#141,ss_store_sk#142,ss_promo_sk#143,ss_ticket_number#144,ss_quantity#145,ss_wholesale_cost#146,ss_list_price#147,ss_sales_price#148,ss_ext_discount_amt#149,ss_ext_sales_price#150,ss_ext_wholesale_cost#151,ss_ext_list_price#152,ss_ext_tax#153,ss_coupon_amt#154,ss_net_paid#155,ss_net_paid_inc_tax#156,ss_net_profit#157] parquet
         :              +- Project [d_date_sk#73]
         :                 +- Filter ((((isnotnull(d_moy#81) && isnotnull(d_year#79)) && (d_moy#81 = 11)) && (d_year#79 = 1999)) && isnotnull(d_date_sk#73))
         :                    +- Relation[d_date_sk#73,d_date_id#74,d_date#75,d_month_seq#76,d_week_seq#77,d_quarter_seq#78,d_year#79,d_dow#80,d_moy#81,d_dom#82,d_qoy#83,d_fy_year#84,d_fy_quarter_seq#85,d_fy_week_seq#86,d_day_name#87,d_quarter_name#88,d_holiday#89,d_weekend#90,d_following_holiday#91,d_first_dom#92,d_last_dom#93,d_same_day_ly#94,d_same_day_lq#95,d_current_day#96,... 4 more fields] parquet
         +- Project [t_time_sk#158, t_hour#161, t_minute#162]
            +- Filter (((t_meal_time#167 = breakfast) || (t_meal_time#167 = dinner)) && isnotnull(t_time_sk#158))
               +- Relation[t_time_sk#158,t_time_id#159,t_time#160,t_hour#161,t_minute#162,t_second#163,t_am_pm#164,t_shift#165,t_sub_shift#166,t_meal_time#167] parquet

== Physical Plan ==
*(11) Sort [ext_price#14 DESC NULLS LAST, brand_id#12 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(ext_price#14 DESC NULLS LAST, brand_id#12 ASC NULLS FIRST, 200)
   +- *(10) HashAggregate(keys=[i_brand#25, i_brand_id#24, t_hour#161, t_minute#162], functions=[sum(ext_price#0)], output=[brand_id#12, brand#13, t_hour#161, t_minute#162, ext_price#14])
      +- Exchange hashpartitioning(i_brand#25, i_brand_id#24, t_hour#161, t_minute#162, 200)
         +- *(9) HashAggregate(keys=[i_brand#25, i_brand_id#24, t_hour#161, t_minute#162], functions=[partial_sum(ext_price#0)], output=[i_brand#25, i_brand_id#24, t_hour#161, t_minute#162, sum#173])
            +- *(9) Project [i_brand_id#24, i_brand#25, ext_price#0, t_hour#161, t_minute#162]
               +- *(9) BroadcastHashJoin [time_sk#3], [t_time_sk#158], Inner, BuildRight
                  :- *(9) Project [i_brand_id#24, i_brand#25, ext_price#0, time_sk#3]
                  :  +- *(9) BroadcastHashJoin [i_item_sk#17], [sold_item_sk#2], Inner, BuildLeft
                  :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     :  +- *(1) Project [i_item_sk#17, i_brand_id#24, i_brand#25]
                  :     :     +- *(1) Filter ((isnotnull(i_manager_id#37) && (i_manager_id#37 = 1)) && isnotnull(i_item_sk#17))
                  :     :        +- *(1) FileScan parquet tpcds.item[i_item_sk#17,i_brand_id#24,i_brand#25,i_manager_id#37] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_manager_id), EqualTo(i_manager_id,1), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_brand:string,i_manager_id:int>
                  :     +- Union
                  :        :- *(3) Project [ws_ext_sales_price#62 AS ext_price#0, ws_item_sk#42 AS sold_item_sk#2, ws_sold_time_sk#40 AS time_sk#3]
                  :        :  +- *(3) BroadcastHashJoin [ws_sold_date_sk#39], [d_date_sk#73], Inner, BuildRight
                  :        :     :- *(3) Project [ws_sold_date_sk#39, ws_sold_time_sk#40, ws_item_sk#42, ws_ext_sales_price#62]
                  :        :     :  +- *(3) Filter ((isnotnull(ws_sold_date_sk#39) && isnotnull(ws_item_sk#42)) && isnotnull(ws_sold_time_sk#40))
                  :        :     :     +- *(3) FileScan parquet tpcds.web_sales[ws_sold_date_sk#39,ws_sold_time_sk#40,ws_item_sk#42,ws_ext_sales_price#62] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_sold_date_sk), IsNotNull(ws_item_sk), IsNotNull(ws_sold_time_sk)], ReadSchema: struct<ws_sold_date_sk:int,ws_sold_time_sk:int,ws_item_sk:int,ws_ext_sales_price:double>
                  :        :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :        :        +- *(2) Project [d_date_sk#73]
                  :        :           +- *(2) Filter ((((isnotnull(d_moy#81) && isnotnull(d_year#79)) && (d_moy#81 = 11)) && (d_year#79 = 1999)) && isnotnull(d_date_sk#73))
                  :        :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#73,d_year#79,d_moy#81] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,11), EqualTo(d_year,1999), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :        :- *(5) Project [cs_ext_sales_price#124 AS ext_price#4, cs_item_sk#116 AS sold_item_sk#6, cs_sold_time_sk#102 AS time_sk#7]
                  :        :  +- *(5) BroadcastHashJoin [cs_sold_date_sk#101], [d_date_sk#73], Inner, BuildRight
                  :        :     :- *(5) Project [cs_sold_date_sk#101, cs_sold_time_sk#102, cs_item_sk#116, cs_ext_sales_price#124]
                  :        :     :  +- *(5) Filter ((isnotnull(cs_sold_date_sk#101) && isnotnull(cs_item_sk#116)) && isnotnull(cs_sold_time_sk#102))
                  :        :     :     +- *(5) FileScan parquet tpcds.catalog_sales[cs_sold_date_sk#101,cs_sold_time_sk#102,cs_item_sk#116,cs_ext_sales_price#124] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cs_sold_date_sk), IsNotNull(cs_item_sk), IsNotNull(cs_sold_time_sk)], ReadSchema: struct<cs_sold_date_sk:int,cs_sold_time_sk:int,cs_item_sk:int,cs_ext_sales_price:double>
                  :        :     +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :        +- *(7) Project [ss_ext_sales_price#150 AS ext_price#8, ss_item_sk#137 AS sold_item_sk#10, ss_sold_time_sk#136 AS time_sk#11]
                  :           +- *(7) BroadcastHashJoin [ss_sold_date_sk#135], [d_date_sk#73], Inner, BuildRight
                  :              :- *(7) Project [ss_sold_date_sk#135, ss_sold_time_sk#136, ss_item_sk#137, ss_ext_sales_price#150]
                  :              :  +- *(7) Filter ((isnotnull(ss_sold_date_sk#135) && isnotnull(ss_item_sk#137)) && isnotnull(ss_sold_time_sk#136))
                  :              :     +- *(7) FileScan parquet tpcds.store_sales[ss_sold_date_sk#135,ss_sold_time_sk#136,ss_item_sk#137,ss_ext_sales_price#150] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk), IsNotNull(ss_sold_time_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_sold_time_sk:int,ss_item_sk:int,ss_ext_sales_price:double>
                  :              +- ReusedExchange [d_date_sk#73], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     +- *(8) Project [t_time_sk#158, t_hour#161, t_minute#162]
                        +- *(8) Filter (((t_meal_time#167 = breakfast) || (t_meal_time#167 = dinner)) && isnotnull(t_time_sk#158))
                           +- *(8) FileScan parquet tpcds.time_dim[t_time_sk#158,t_hour#161,t_minute#162,t_meal_time#167] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/t..., PartitionFilters: [], PushedFilters: [Or(EqualTo(t_meal_time,breakfast),EqualTo(t_meal_time,dinner)), IsNotNull(t_time_sk)], ReadSchema: struct<t_time_sk:int,t_hour:int,t_minute:int,t_meal_time:string>
Time taken: 4.689 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 71 in stream 0 using template query71.tpl
------------------------------------------------------^^^

