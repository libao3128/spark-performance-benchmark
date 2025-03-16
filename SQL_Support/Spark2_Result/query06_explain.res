== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['cnt ASC NULLS FIRST], true
      +- 'UnresolvedHaving ('count(1) >= 10)
         +- 'Aggregate ['a.ca_state], ['a.ca_state AS state#0, 'count(1) AS cnt#1]
            +- 'Filter (((('a.ca_address_sk = 'c.c_current_addr_sk) && ('c.c_customer_sk = 's.ss_customer_sk)) && ('s.ss_sold_date_sk = 'd.d_date_sk)) && ((('s.ss_item_sk = 'i.i_item_sk) && ('d.d_month_seq = scalar-subquery#2 [])) && ('i.i_current_price > (1.2 * scalar-subquery#3 []))))
               :  :- 'Distinct
               :  :  +- 'Project ['d_month_seq]
               :  :     +- 'Filter (('d_year = 2001) && ('d_moy = 1))
               :  :        +- 'UnresolvedRelation `date_dim`
               :  +- 'Project [unresolvedalias('avg('j.i_current_price), None)]
               :     +- 'Filter ('j.i_category = 'i.i_category)
               :        +- 'SubqueryAlias `j`
               :           +- 'UnresolvedRelation `item`
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'Join Inner
                  :  :  :- 'Join Inner
                  :  :  :  :- 'SubqueryAlias `a`
                  :  :  :  :  +- 'UnresolvedRelation `customer_address`
                  :  :  :  +- 'SubqueryAlias `c`
                  :  :  :     +- 'UnresolvedRelation `customer`
                  :  :  +- 'SubqueryAlias `s`
                  :  :     +- 'UnresolvedRelation `store_sales`
                  :  +- 'SubqueryAlias `d`
                  :     +- 'UnresolvedRelation `date_dim`
                  +- 'SubqueryAlias `i`
                     +- 'UnresolvedRelation `item`

== Analyzed Logical Plan ==
state: string, cnt: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cnt#1L ASC NULLS FIRST], true
      +- Project [state#0, cnt#1L]
         +- Filter (count(1)#115L >= cast(10 as bigint))
            +- Aggregate [ca_state#16], [ca_state#16 AS state#0, count(1) AS cnt#1L, count(1) AS count(1)#115L]
               +- Filter ((((ca_address_sk#8 = c_current_addr_sk#25) && (c_customer_sk#21 = ss_customer_sk#42)) && (ss_sold_date_sk#39 = d_date_sk#62)) && (((ss_item_sk#41 = i_item_sk#90) && (d_month_seq#65 = scalar-subquery#2 [])) && (i_current_price#95 > (cast(1.2 as double) * scalar-subquery#3 [i_category#102]))))
                  :  :- Distinct
                  :  :  +- Project [d_month_seq#65]
                  :  :     +- Filter ((d_year#68 = 2001) && (d_moy#70 = 1))
                  :  :        +- SubqueryAlias `tpcds`.`date_dim`
                  :  :           +- Relation[d_date_sk#62,d_date_id#63,d_date#64,d_month_seq#65,d_week_seq#66,d_quarter_seq#67,d_year#68,d_dow#69,d_moy#70,d_dom#71,d_qoy#72,d_fy_year#73,d_fy_quarter_seq#74,d_fy_week_seq#75,d_day_name#76,d_quarter_name#77,d_holiday#78,d_weekend#79,d_following_holiday#80,d_first_dom#81,d_last_dom#82,d_same_day_ly#83,d_same_day_lq#84,d_current_day#85,... 4 more fields] parquet
                  :  +- Aggregate [avg(i_current_price#95) AS avg(i_current_price)#113]
                  :     +- Filter (i_category#102 = outer(i_category#102))
                  :        +- SubqueryAlias `j`
                  :           +- SubqueryAlias `tpcds`.`item`
                  :              +- Relation[i_item_sk#90,i_item_id#91,i_rec_start_date#92,i_rec_end_date#93,i_item_desc#94,i_current_price#95,i_wholesale_cost#96,i_brand_id#97,i_brand#98,i_class_id#99,i_class#100,i_category_id#101,i_category#102,i_manufact_id#103,i_manufact#104,i_size#105,i_formulation#106,i_color#107,i_units#108,i_container#109,i_manager_id#110,i_product_name#111] parquet
                  +- Join Inner
                     :- Join Inner
                     :  :- Join Inner
                     :  :  :- Join Inner
                     :  :  :  :- SubqueryAlias `a`
                     :  :  :  :  +- SubqueryAlias `tpcds`.`customer_address`
                     :  :  :  :     +- Relation[ca_address_sk#8,ca_address_id#9,ca_street_number#10,ca_street_name#11,ca_street_type#12,ca_suite_number#13,ca_city#14,ca_county#15,ca_state#16,ca_zip#17,ca_country#18,ca_gmt_offset#19,ca_location_type#20] parquet
                     :  :  :  +- SubqueryAlias `c`
                     :  :  :     +- SubqueryAlias `tpcds`.`customer`
                     :  :  :        +- Relation[c_customer_sk#21,c_customer_id#22,c_current_cdemo_sk#23,c_current_hdemo_sk#24,c_current_addr_sk#25,c_first_shipto_date_sk#26,c_first_sales_date_sk#27,c_salutation#28,c_first_name#29,c_last_name#30,c_preferred_cust_flag#31,c_birth_day#32,c_birth_month#33,c_birth_year#34,c_birth_country#35,c_login#36,c_email_address#37,c_last_review_date#38] parquet
                     :  :  +- SubqueryAlias `s`
                     :  :     +- SubqueryAlias `tpcds`.`store_sales`
                     :  :        +- Relation[ss_sold_date_sk#39,ss_sold_time_sk#40,ss_item_sk#41,ss_customer_sk#42,ss_cdemo_sk#43,ss_hdemo_sk#44,ss_addr_sk#45,ss_store_sk#46,ss_promo_sk#47,ss_ticket_number#48,ss_quantity#49,ss_wholesale_cost#50,ss_list_price#51,ss_sales_price#52,ss_ext_discount_amt#53,ss_ext_sales_price#54,ss_ext_wholesale_cost#55,ss_ext_list_price#56,ss_ext_tax#57,ss_coupon_amt#58,ss_net_paid#59,ss_net_paid_inc_tax#60,ss_net_profit#61] parquet
                     :  +- SubqueryAlias `d`
                     :     +- SubqueryAlias `tpcds`.`date_dim`
                     :        +- Relation[d_date_sk#62,d_date_id#63,d_date#64,d_month_seq#65,d_week_seq#66,d_quarter_seq#67,d_year#68,d_dow#69,d_moy#70,d_dom#71,d_qoy#72,d_fy_year#73,d_fy_quarter_seq#74,d_fy_week_seq#75,d_day_name#76,d_quarter_name#77,d_holiday#78,d_weekend#79,d_following_holiday#80,d_first_dom#81,d_last_dom#82,d_same_day_ly#83,d_same_day_lq#84,d_current_day#85,... 4 more fields] parquet
                     +- SubqueryAlias `i`
                        +- SubqueryAlias `tpcds`.`item`
                           +- Relation[i_item_sk#90,i_item_id#91,i_rec_start_date#92,i_rec_end_date#93,i_item_desc#94,i_current_price#95,i_wholesale_cost#96,i_brand_id#97,i_brand#98,i_class_id#99,i_class#100,i_category_id#101,i_category#102,i_manufact_id#103,i_manufact#104,i_size#105,i_formulation#106,i_color#107,i_units#108,i_container#109,i_manager_id#110,i_product_name#111] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [cnt#1L ASC NULLS FIRST], true
      +- Project [state#0, cnt#1L]
         +- Filter (count(1)#115L >= 10)
            +- Aggregate [ca_state#16], [ca_state#16 AS state#0, count(1) AS cnt#1L, count(1) AS count(1)#115L]
               +- Project [ca_state#16]
                  +- Join Inner, (ss_item_sk#41 = i_item_sk#90)
                     :- Project [ca_state#16, ss_item_sk#41]
                     :  +- Join Inner, (ss_sold_date_sk#39 = d_date_sk#62)
                     :     :- Project [ca_state#16, ss_sold_date_sk#39, ss_item_sk#41]
                     :     :  +- Join Inner, (c_customer_sk#21 = ss_customer_sk#42)
                     :     :     :- Project [ca_state#16, c_customer_sk#21]
                     :     :     :  +- Join Inner, (ca_address_sk#8 = c_current_addr_sk#25)
                     :     :     :     :- Project [ca_address_sk#8, ca_state#16]
                     :     :     :     :  +- Filter isnotnull(ca_address_sk#8)
                     :     :     :     :     +- Relation[ca_address_sk#8,ca_address_id#9,ca_street_number#10,ca_street_name#11,ca_street_type#12,ca_suite_number#13,ca_city#14,ca_county#15,ca_state#16,ca_zip#17,ca_country#18,ca_gmt_offset#19,ca_location_type#20] parquet
                     :     :     :     +- Project [c_customer_sk#21, c_current_addr_sk#25]
                     :     :     :        +- Filter (isnotnull(c_current_addr_sk#25) && isnotnull(c_customer_sk#21))
                     :     :     :           +- Relation[c_customer_sk#21,c_customer_id#22,c_current_cdemo_sk#23,c_current_hdemo_sk#24,c_current_addr_sk#25,c_first_shipto_date_sk#26,c_first_sales_date_sk#27,c_salutation#28,c_first_name#29,c_last_name#30,c_preferred_cust_flag#31,c_birth_day#32,c_birth_month#33,c_birth_year#34,c_birth_country#35,c_login#36,c_email_address#37,c_last_review_date#38] parquet
                     :     :     +- Project [ss_sold_date_sk#39, ss_item_sk#41, ss_customer_sk#42]
                     :     :        +- Filter ((isnotnull(ss_customer_sk#42) && isnotnull(ss_sold_date_sk#39)) && isnotnull(ss_item_sk#41))
                     :     :           +- Relation[ss_sold_date_sk#39,ss_sold_time_sk#40,ss_item_sk#41,ss_customer_sk#42,ss_cdemo_sk#43,ss_hdemo_sk#44,ss_addr_sk#45,ss_store_sk#46,ss_promo_sk#47,ss_ticket_number#48,ss_quantity#49,ss_wholesale_cost#50,ss_list_price#51,ss_sales_price#52,ss_ext_discount_amt#53,ss_ext_sales_price#54,ss_ext_wholesale_cost#55,ss_ext_list_price#56,ss_ext_tax#57,ss_coupon_amt#58,ss_net_paid#59,ss_net_paid_inc_tax#60,ss_net_profit#61] parquet
                     :     +- Project [d_date_sk#62]
                     :        +- Filter ((isnotnull(d_month_seq#65) && (d_month_seq#65 = scalar-subquery#2 [])) && isnotnull(d_date_sk#62))
                     :           :  +- Aggregate [d_month_seq#65], [d_month_seq#65]
                     :           :     +- Project [d_month_seq#65]
                     :           :        +- Filter (((isnotnull(d_year#68) && isnotnull(d_moy#70)) && (d_year#68 = 2001)) && (d_moy#70 = 1))
                     :           :           +- Relation[d_date_sk#62,d_date_id#63,d_date#64,d_month_seq#65,d_week_seq#66,d_quarter_seq#67,d_year#68,d_dow#69,d_moy#70,d_dom#71,d_qoy#72,d_fy_year#73,d_fy_quarter_seq#74,d_fy_week_seq#75,d_day_name#76,d_quarter_name#77,d_holiday#78,d_weekend#79,d_following_holiday#80,d_first_dom#81,d_last_dom#82,d_same_day_ly#83,d_same_day_lq#84,d_current_day#85,... 4 more fields] parquet
                     :           +- Relation[d_date_sk#62,d_date_id#63,d_date#64,d_month_seq#65,d_week_seq#66,d_quarter_seq#67,d_year#68,d_dow#69,d_moy#70,d_dom#71,d_qoy#72,d_fy_year#73,d_fy_quarter_seq#74,d_fy_week_seq#75,d_day_name#76,d_quarter_name#77,d_holiday#78,d_weekend#79,d_following_holiday#80,d_first_dom#81,d_last_dom#82,d_same_day_ly#83,d_same_day_lq#84,d_current_day#85,... 4 more fields] parquet
                     +- Project [i_item_sk#90]
                        +- Join Inner, ((i_current_price#95 > (1.2 * avg(i_current_price)#113)) && (i_category#102#118 = i_category#102))
                           :- Project [i_item_sk#90, i_current_price#95, i_category#102]
                           :  +- Filter ((isnotnull(i_current_price#95) && isnotnull(i_category#102)) && isnotnull(i_item_sk#90))
                           :     +- Relation[i_item_sk#90,i_item_id#91,i_rec_start_date#92,i_rec_end_date#93,i_item_desc#94,i_current_price#95,i_wholesale_cost#96,i_brand_id#97,i_brand#98,i_class_id#99,i_class#100,i_category_id#101,i_category#102,i_manufact_id#103,i_manufact#104,i_size#105,i_formulation#106,i_color#107,i_units#108,i_container#109,i_manager_id#110,i_product_name#111] parquet
                           +- Filter isnotnull(avg(i_current_price)#113)
                              +- Aggregate [i_category#102], [avg(i_current_price#95) AS avg(i_current_price)#113, i_category#102 AS i_category#102#118]
                                 +- Project [i_current_price#95, i_category#102]
                                    +- Filter isnotnull(i_category#102)
                                       +- Relation[i_item_sk#90,i_item_id#91,i_rec_start_date#92,i_rec_end_date#93,i_item_desc#94,i_current_price#95,i_wholesale_cost#96,i_brand_id#97,i_brand#98,i_class_id#99,i_class#100,i_category_id#101,i_category#102,i_manufact_id#103,i_manufact#104,i_size#105,i_formulation#106,i_color#107,i_units#108,i_container#109,i_manager_id#110,i_product_name#111] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[cnt#1L ASC NULLS FIRST], output=[state#0,cnt#1L])
+- *(14) Project [state#0, cnt#1L]
   +- *(14) Filter (count(1)#115L >= 10)
      +- *(14) HashAggregate(keys=[ca_state#16], functions=[count(1)], output=[state#0, cnt#1L, count(1)#115L])
         +- Exchange hashpartitioning(ca_state#16, 200)
            +- *(13) HashAggregate(keys=[ca_state#16], functions=[partial_count(1)], output=[ca_state#16, count#120L])
               +- *(13) Project [ca_state#16]
                  +- *(13) SortMergeJoin [ss_item_sk#41], [i_item_sk#90], Inner
                     :- *(8) Sort [ss_item_sk#41 ASC NULLS FIRST], false, 0
                     :  +- Exchange hashpartitioning(ss_item_sk#41, 200)
                     :     +- *(7) Project [ca_state#16, ss_item_sk#41]
                     :        +- *(7) BroadcastHashJoin [ss_sold_date_sk#39], [d_date_sk#62], Inner, BuildRight
                     :           :- *(7) Project [ca_state#16, ss_sold_date_sk#39, ss_item_sk#41]
                     :           :  +- *(7) SortMergeJoin [c_customer_sk#21], [ss_customer_sk#42], Inner
                     :           :     :- *(3) Sort [c_customer_sk#21 ASC NULLS FIRST], false, 0
                     :           :     :  +- Exchange hashpartitioning(c_customer_sk#21, 200)
                     :           :     :     +- *(2) Project [ca_state#16, c_customer_sk#21]
                     :           :     :        +- *(2) BroadcastHashJoin [ca_address_sk#8], [c_current_addr_sk#25], Inner, BuildLeft
                     :           :     :           :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :           :     :           :  +- *(1) Project [ca_address_sk#8, ca_state#16]
                     :           :     :           :     +- *(1) Filter isnotnull(ca_address_sk#8)
                     :           :     :           :        +- *(1) FileScan parquet tpcds.customer_address[ca_address_sk#8,ca_state#16] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
                     :           :     :           +- *(2) Project [c_customer_sk#21, c_current_addr_sk#25]
                     :           :     :              +- *(2) Filter (isnotnull(c_current_addr_sk#25) && isnotnull(c_customer_sk#21))
                     :           :     :                 +- *(2) FileScan parquet tpcds.customer[c_customer_sk#21,c_current_addr_sk#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_current_addr_sk), IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int>
                     :           :     +- *(5) Sort [ss_customer_sk#42 ASC NULLS FIRST], false, 0
                     :           :        +- Exchange hashpartitioning(ss_customer_sk#42, 200)
                     :           :           +- *(4) Project [ss_sold_date_sk#39, ss_item_sk#41, ss_customer_sk#42]
                     :           :              +- *(4) Filter ((isnotnull(ss_customer_sk#42) && isnotnull(ss_sold_date_sk#39)) && isnotnull(ss_item_sk#41))
                     :           :                 +- *(4) FileScan parquet tpcds.store_sales[ss_sold_date_sk#39,ss_item_sk#41,ss_customer_sk#42] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_customer_sk), IsNotNull(ss_sold_date_sk), IsNotNull(ss_item_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_item_sk:int,ss_customer_sk:int>
                     :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     :              +- *(6) Project [d_date_sk#62]
                     :                 +- *(6) Filter ((isnotnull(d_month_seq#65) && (d_month_seq#65 = Subquery subquery2)) && isnotnull(d_date_sk#62))
                     :                    :  +- Subquery subquery2
                     :                    :     +- *(2) HashAggregate(keys=[d_month_seq#65], functions=[], output=[d_month_seq#65])
                     :                    :        +- Exchange hashpartitioning(d_month_seq#65, 200)
                     :                    :           +- *(1) HashAggregate(keys=[d_month_seq#65], functions=[], output=[d_month_seq#65])
                     :                    :              +- *(1) Project [d_month_seq#65]
                     :                    :                 +- *(1) Filter (((isnotnull(d_year#68) && isnotnull(d_moy#70)) && (d_year#68 = 2001)) && (d_moy#70 = 1))
                     :                    :                    +- *(1) FileScan parquet tpcds.date_dim[d_month_seq#65,d_year#68,d_moy#70] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,1)], ReadSchema: struct<d_month_seq:int,d_year:int,d_moy:int>
                     :                    +- *(6) FileScan parquet tpcds.date_dim[d_date_sk#62,d_month_seq#65] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_month_seq), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_month_seq:int>
                     :                          +- Subquery subquery2
                     :                             +- *(2) HashAggregate(keys=[d_month_seq#65], functions=[], output=[d_month_seq#65])
                     :                                +- Exchange hashpartitioning(d_month_seq#65, 200)
                     :                                   +- *(1) HashAggregate(keys=[d_month_seq#65], functions=[], output=[d_month_seq#65])
                     :                                      +- *(1) Project [d_month_seq#65]
                     :                                         +- *(1) Filter (((isnotnull(d_year#68) && isnotnull(d_moy#70)) && (d_year#68 = 2001)) && (d_moy#70 = 1))
                     :                                            +- *(1) FileScan parquet tpcds.date_dim[d_month_seq#65,d_year#68,d_moy#70] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2001), EqualTo(d_moy,1)], ReadSchema: struct<d_month_seq:int,d_year:int,d_moy:int>
                     +- *(12) Sort [i_item_sk#90 ASC NULLS FIRST], false, 0
                        +- Exchange hashpartitioning(i_item_sk#90, 200)
                           +- *(11) Project [i_item_sk#90]
                              +- *(11) BroadcastHashJoin [i_category#102], [i_category#102#118], Inner, BuildRight, (i_current_price#95 > (1.2 * avg(i_current_price)#113))
                                 :- *(11) Project [i_item_sk#90, i_current_price#95, i_category#102]
                                 :  +- *(11) Filter ((isnotnull(i_current_price#95) && isnotnull(i_category#102)) && isnotnull(i_item_sk#90))
                                 :     +- *(11) FileScan parquet tpcds.item[i_item_sk#90,i_current_price#95,i_category#102] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_current_price), IsNotNull(i_category), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double,i_category:string>
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(input[1, string, true]))
                                    +- *(10) Filter isnotnull(avg(i_current_price)#113)
                                       +- *(10) HashAggregate(keys=[i_category#102], functions=[avg(i_current_price#95)], output=[avg(i_current_price)#113, i_category#102#118])
                                          +- Exchange hashpartitioning(i_category#102, 200)
                                             +- *(9) HashAggregate(keys=[i_category#102], functions=[partial_avg(i_current_price#95)], output=[i_category#102, sum#123, count#124L])
                                                +- *(9) Project [i_current_price#95, i_category#102]
                                                   +- *(9) Filter isnotnull(i_category#102)
                                                      +- *(9) FileScan parquet tpcds.item[i_current_price#95,i_category#102] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_category)], ReadSchema: struct<i_current_price:double,i_category:string>
Time taken: 4.388 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 52)

== SQL ==

-- end query 6 in stream 0 using template query6.tpl
----------------------------------------------------^^^

