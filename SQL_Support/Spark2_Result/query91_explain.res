== Parsed Logical Plan ==
'Sort ['sum('cr_net_loss) DESC NULLS LAST], true
+- 'Aggregate ['cc_call_center_id, 'cc_name, 'cc_manager, 'cd_marital_status, 'cd_education_status], ['cc_call_center_id AS Call_Center#0, 'cc_name AS Call_Center_Name#1, 'cc_manager AS Manager#2, 'sum('cr_net_loss) AS Returns_Loss#3]
   +- 'Filter ((((('cr_call_center_sk = 'cc_call_center_sk) && ('cr_returned_date_sk = 'd_date_sk)) && ('cr_returning_customer_sk = 'c_customer_sk)) && ((('cd_demo_sk = 'c_current_cdemo_sk) && ('hd_demo_sk = 'c_current_hdemo_sk)) && ('ca_address_sk = 'c_current_addr_sk))) && (((('d_year = 1998) && ('d_moy = 11)) && ((('cd_marital_status = M) && ('cd_education_status = Unknown)) || (('cd_marital_status = W) && ('cd_education_status = Advanced Degree)))) && ('hd_buy_potential LIKE Unknown% && ('ca_gmt_offset = -7))))
      +- 'Join Inner
         :- 'Join Inner
         :  :- 'Join Inner
         :  :  :- 'Join Inner
         :  :  :  :- 'Join Inner
         :  :  :  :  :- 'Join Inner
         :  :  :  :  :  :- 'UnresolvedRelation `call_center`
         :  :  :  :  :  +- 'UnresolvedRelation `catalog_returns`
         :  :  :  :  +- 'UnresolvedRelation `date_dim`
         :  :  :  +- 'UnresolvedRelation `customer`
         :  :  +- 'UnresolvedRelation `customer_address`
         :  +- 'UnresolvedRelation `customer_demographics`
         +- 'UnresolvedRelation `household_demographics`

== Analyzed Logical Plan ==
Call_Center: string, Call_Center_Name: string, Manager: string, Returns_Loss: double
Project [Call_Center#0, Call_Center_Name#1, Manager#2, Returns_Loss#3]
+- Sort [Returns_Loss#3 DESC NULLS LAST], true
   +- Aggregate [cc_call_center_id#7, cc_name#12, cc_manager#17, cd_marital_status#125, cd_education_status#126], [cc_call_center_id#7 AS Call_Center#0, cc_name#12 AS Call_Center_Name#1, cc_manager#17 AS Manager#2, sum(cr_net_loss#63) AS Returns_Loss#3]
      +- Filter (((((cr_call_center_sk#48 = cc_call_center_sk#6) && (cr_returned_date_sk#37 = d_date_sk#64)) && (cr_returning_customer_sk#44 = c_customer_sk#92)) && (((cd_demo_sk#123 = c_current_cdemo_sk#94) && (hd_demo_sk#132 = c_current_hdemo_sk#95)) && (ca_address_sk#110 = c_current_addr_sk#96))) && ((((d_year#70 = 1998) && (d_moy#72 = 11)) && (((cd_marital_status#125 = M) && (cd_education_status#126 = Unknown)) || ((cd_marital_status#125 = W) && (cd_education_status#126 = Advanced Degree)))) && (hd_buy_potential#134 LIKE Unknown% && (ca_gmt_offset#121 = cast(-7 as double)))))
         +- Join Inner
            :- Join Inner
            :  :- Join Inner
            :  :  :- Join Inner
            :  :  :  :- Join Inner
            :  :  :  :  :- Join Inner
            :  :  :  :  :  :- SubqueryAlias `tpcds`.`call_center`
            :  :  :  :  :  :  +- Relation[cc_call_center_sk#6,cc_call_center_id#7,cc_rec_start_date#8,cc_rec_end_date#9,cc_closed_date_sk#10,cc_open_date_sk#11,cc_name#12,cc_class#13,cc_employees#14,cc_sq_ft#15,cc_hours#16,cc_manager#17,cc_mkt_id#18,cc_mkt_class#19,cc_mkt_desc#20,cc_market_manager#21,cc_division#22,cc_division_name#23,cc_company#24,cc_company_name#25,cc_street_number#26,cc_street_name#27,cc_street_type#28,cc_suite_number#29,... 7 more fields] parquet
            :  :  :  :  :  +- SubqueryAlias `tpcds`.`catalog_returns`
            :  :  :  :  :     +- Relation[cr_returned_date_sk#37,cr_returned_time_sk#38,cr_item_sk#39,cr_refunded_customer_sk#40,cr_refunded_cdemo_sk#41,cr_refunded_hdemo_sk#42,cr_refunded_addr_sk#43,cr_returning_customer_sk#44,cr_returning_cdemo_sk#45,cr_returning_hdemo_sk#46,cr_returning_addr_sk#47,cr_call_center_sk#48,cr_catalog_page_sk#49,cr_ship_mode_sk#50,cr_warehouse_sk#51,cr_reason_sk#52,cr_order_number#53,cr_return_quantity#54,cr_return_amount#55,cr_return_tax#56,cr_return_amt_inc_tax#57,cr_fee#58,cr_return_ship_cost#59,cr_refunded_cash#60,... 3 more fields] parquet
            :  :  :  :  +- SubqueryAlias `tpcds`.`date_dim`
            :  :  :  :     +- Relation[d_date_sk#64,d_date_id#65,d_date#66,d_month_seq#67,d_week_seq#68,d_quarter_seq#69,d_year#70,d_dow#71,d_moy#72,d_dom#73,d_qoy#74,d_fy_year#75,d_fy_quarter_seq#76,d_fy_week_seq#77,d_day_name#78,d_quarter_name#79,d_holiday#80,d_weekend#81,d_following_holiday#82,d_first_dom#83,d_last_dom#84,d_same_day_ly#85,d_same_day_lq#86,d_current_day#87,... 4 more fields] parquet
            :  :  :  +- SubqueryAlias `tpcds`.`customer`
            :  :  :     +- Relation[c_customer_sk#92,c_customer_id#93,c_current_cdemo_sk#94,c_current_hdemo_sk#95,c_current_addr_sk#96,c_first_shipto_date_sk#97,c_first_sales_date_sk#98,c_salutation#99,c_first_name#100,c_last_name#101,c_preferred_cust_flag#102,c_birth_day#103,c_birth_month#104,c_birth_year#105,c_birth_country#106,c_login#107,c_email_address#108,c_last_review_date#109] parquet
            :  :  +- SubqueryAlias `tpcds`.`customer_address`
            :  :     +- Relation[ca_address_sk#110,ca_address_id#111,ca_street_number#112,ca_street_name#113,ca_street_type#114,ca_suite_number#115,ca_city#116,ca_county#117,ca_state#118,ca_zip#119,ca_country#120,ca_gmt_offset#121,ca_location_type#122] parquet
            :  +- SubqueryAlias `tpcds`.`customer_demographics`
            :     +- Relation[cd_demo_sk#123,cd_gender#124,cd_marital_status#125,cd_education_status#126,cd_purchase_estimate#127,cd_credit_rating#128,cd_dep_count#129,cd_dep_employed_count#130,cd_dep_college_count#131] parquet
            +- SubqueryAlias `tpcds`.`household_demographics`
               +- Relation[hd_demo_sk#132,hd_income_band_sk#133,hd_buy_potential#134,hd_dep_count#135,hd_vehicle_count#136] parquet

== Optimized Logical Plan ==
Sort [Returns_Loss#3 DESC NULLS LAST], true
+- Aggregate [cc_call_center_id#7, cc_name#12, cc_manager#17, cd_marital_status#125, cd_education_status#126], [cc_call_center_id#7 AS Call_Center#0, cc_name#12 AS Call_Center_Name#1, cc_manager#17 AS Manager#2, sum(cr_net_loss#63) AS Returns_Loss#3]
   +- Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_net_loss#63, cd_marital_status#125, cd_education_status#126]
      +- Join Inner, (hd_demo_sk#132 = c_current_hdemo_sk#95)
         :- Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_net_loss#63, c_current_hdemo_sk#95, cd_marital_status#125, cd_education_status#126]
         :  +- Join Inner, (cd_demo_sk#123 = c_current_cdemo_sk#94)
         :     :- Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_net_loss#63, c_current_cdemo_sk#94, c_current_hdemo_sk#95]
         :     :  +- Join Inner, (ca_address_sk#110 = c_current_addr_sk#96)
         :     :     :- Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_net_loss#63, c_current_cdemo_sk#94, c_current_hdemo_sk#95, c_current_addr_sk#96]
         :     :     :  +- Join Inner, (cr_returning_customer_sk#44 = c_customer_sk#92)
         :     :     :     :- Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_returning_customer_sk#44, cr_net_loss#63]
         :     :     :     :  +- Join Inner, (cr_returned_date_sk#37 = d_date_sk#64)
         :     :     :     :     :- Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_returned_date_sk#37, cr_returning_customer_sk#44, cr_net_loss#63]
         :     :     :     :     :  +- Join Inner, (cr_call_center_sk#48 = cc_call_center_sk#6)
         :     :     :     :     :     :- Project [cc_call_center_sk#6, cc_call_center_id#7, cc_name#12, cc_manager#17]
         :     :     :     :     :     :  +- Filter isnotnull(cc_call_center_sk#6)
         :     :     :     :     :     :     +- Relation[cc_call_center_sk#6,cc_call_center_id#7,cc_rec_start_date#8,cc_rec_end_date#9,cc_closed_date_sk#10,cc_open_date_sk#11,cc_name#12,cc_class#13,cc_employees#14,cc_sq_ft#15,cc_hours#16,cc_manager#17,cc_mkt_id#18,cc_mkt_class#19,cc_mkt_desc#20,cc_market_manager#21,cc_division#22,cc_division_name#23,cc_company#24,cc_company_name#25,cc_street_number#26,cc_street_name#27,cc_street_type#28,cc_suite_number#29,... 7 more fields] parquet
         :     :     :     :     :     +- Project [cr_returned_date_sk#37, cr_returning_customer_sk#44, cr_call_center_sk#48, cr_net_loss#63]
         :     :     :     :     :        +- Filter ((isnotnull(cr_call_center_sk#48) && isnotnull(cr_returned_date_sk#37)) && isnotnull(cr_returning_customer_sk#44))
         :     :     :     :     :           +- Relation[cr_returned_date_sk#37,cr_returned_time_sk#38,cr_item_sk#39,cr_refunded_customer_sk#40,cr_refunded_cdemo_sk#41,cr_refunded_hdemo_sk#42,cr_refunded_addr_sk#43,cr_returning_customer_sk#44,cr_returning_cdemo_sk#45,cr_returning_hdemo_sk#46,cr_returning_addr_sk#47,cr_call_center_sk#48,cr_catalog_page_sk#49,cr_ship_mode_sk#50,cr_warehouse_sk#51,cr_reason_sk#52,cr_order_number#53,cr_return_quantity#54,cr_return_amount#55,cr_return_tax#56,cr_return_amt_inc_tax#57,cr_fee#58,cr_return_ship_cost#59,cr_refunded_cash#60,... 3 more fields] parquet
         :     :     :     :     +- Project [d_date_sk#64]
         :     :     :     :        +- Filter ((((isnotnull(d_year#70) && isnotnull(d_moy#72)) && (d_year#70 = 1998)) && (d_moy#72 = 11)) && isnotnull(d_date_sk#64))
         :     :     :     :           +- Relation[d_date_sk#64,d_date_id#65,d_date#66,d_month_seq#67,d_week_seq#68,d_quarter_seq#69,d_year#70,d_dow#71,d_moy#72,d_dom#73,d_qoy#74,d_fy_year#75,d_fy_quarter_seq#76,d_fy_week_seq#77,d_day_name#78,d_quarter_name#79,d_holiday#80,d_weekend#81,d_following_holiday#82,d_first_dom#83,d_last_dom#84,d_same_day_ly#85,d_same_day_lq#86,d_current_day#87,... 4 more fields] parquet
         :     :     :     +- Project [c_customer_sk#92, c_current_cdemo_sk#94, c_current_hdemo_sk#95, c_current_addr_sk#96]
         :     :     :        +- Filter (((isnotnull(c_customer_sk#92) && isnotnull(c_current_addr_sk#96)) && isnotnull(c_current_cdemo_sk#94)) && isnotnull(c_current_hdemo_sk#95))
         :     :     :           +- Relation[c_customer_sk#92,c_customer_id#93,c_current_cdemo_sk#94,c_current_hdemo_sk#95,c_current_addr_sk#96,c_first_shipto_date_sk#97,c_first_sales_date_sk#98,c_salutation#99,c_first_name#100,c_last_name#101,c_preferred_cust_flag#102,c_birth_day#103,c_birth_month#104,c_birth_year#105,c_birth_country#106,c_login#107,c_email_address#108,c_last_review_date#109] parquet
         :     :     +- Project [ca_address_sk#110]
         :     :        +- Filter ((isnotnull(ca_gmt_offset#121) && (ca_gmt_offset#121 = -7.0)) && isnotnull(ca_address_sk#110))
         :     :           +- Relation[ca_address_sk#110,ca_address_id#111,ca_street_number#112,ca_street_name#113,ca_street_type#114,ca_suite_number#115,ca_city#116,ca_county#117,ca_state#118,ca_zip#119,ca_country#120,ca_gmt_offset#121,ca_location_type#122] parquet
         :     +- Project [cd_demo_sk#123, cd_marital_status#125, cd_education_status#126]
         :        +- Filter ((((cd_marital_status#125 = M) && (cd_education_status#126 = Unknown)) || ((cd_marital_status#125 = W) && (cd_education_status#126 = Advanced Degree))) && isnotnull(cd_demo_sk#123))
         :           +- Relation[cd_demo_sk#123,cd_gender#124,cd_marital_status#125,cd_education_status#126,cd_purchase_estimate#127,cd_credit_rating#128,cd_dep_count#129,cd_dep_employed_count#130,cd_dep_college_count#131] parquet
         +- Project [hd_demo_sk#132]
            +- Filter ((isnotnull(hd_buy_potential#134) && StartsWith(hd_buy_potential#134, Unknown)) && isnotnull(hd_demo_sk#132))
               +- Relation[hd_demo_sk#132,hd_income_band_sk#133,hd_buy_potential#134,hd_dep_count#135,hd_vehicle_count#136] parquet

== Physical Plan ==
*(9) Sort [Returns_Loss#3 DESC NULLS LAST], true, 0
+- Exchange rangepartitioning(Returns_Loss#3 DESC NULLS LAST, 200)
   +- *(8) HashAggregate(keys=[cc_call_center_id#7, cc_name#12, cc_manager#17, cd_marital_status#125, cd_education_status#126], functions=[sum(cr_net_loss#63)], output=[Call_Center#0, Call_Center_Name#1, Manager#2, Returns_Loss#3])
      +- Exchange hashpartitioning(cc_call_center_id#7, cc_name#12, cc_manager#17, cd_marital_status#125, cd_education_status#126, 200)
         +- *(7) HashAggregate(keys=[cc_call_center_id#7, cc_name#12, cc_manager#17, cd_marital_status#125, cd_education_status#126], functions=[partial_sum(cr_net_loss#63)], output=[cc_call_center_id#7, cc_name#12, cc_manager#17, cd_marital_status#125, cd_education_status#126, sum#141])
            +- *(7) Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_net_loss#63, cd_marital_status#125, cd_education_status#126]
               +- *(7) BroadcastHashJoin [c_current_hdemo_sk#95], [hd_demo_sk#132], Inner, BuildRight
                  :- *(7) Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_net_loss#63, c_current_hdemo_sk#95, cd_marital_status#125, cd_education_status#126]
                  :  +- *(7) BroadcastHashJoin [c_current_cdemo_sk#94], [cd_demo_sk#123], Inner, BuildRight
                  :     :- *(7) Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_net_loss#63, c_current_cdemo_sk#94, c_current_hdemo_sk#95]
                  :     :  +- *(7) BroadcastHashJoin [c_current_addr_sk#96], [ca_address_sk#110], Inner, BuildRight
                  :     :     :- *(7) Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_net_loss#63, c_current_cdemo_sk#94, c_current_hdemo_sk#95, c_current_addr_sk#96]
                  :     :     :  +- *(7) BroadcastHashJoin [cr_returning_customer_sk#44], [c_customer_sk#92], Inner, BuildRight
                  :     :     :     :- *(7) Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_returning_customer_sk#44, cr_net_loss#63]
                  :     :     :     :  +- *(7) BroadcastHashJoin [cr_returned_date_sk#37], [d_date_sk#64], Inner, BuildRight
                  :     :     :     :     :- *(7) Project [cc_call_center_id#7, cc_name#12, cc_manager#17, cr_returned_date_sk#37, cr_returning_customer_sk#44, cr_net_loss#63]
                  :     :     :     :     :  +- *(7) BroadcastHashJoin [cc_call_center_sk#6], [cr_call_center_sk#48], Inner, BuildLeft
                  :     :     :     :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     :     :     :     :     :  +- *(1) Project [cc_call_center_sk#6, cc_call_center_id#7, cc_name#12, cc_manager#17]
                  :     :     :     :     :     :     +- *(1) Filter isnotnull(cc_call_center_sk#6)
                  :     :     :     :     :     :        +- *(1) FileScan parquet tpcds.call_center[cc_call_center_sk#6,cc_call_center_id#7,cc_name#12,cc_manager#17] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk)], ReadSchema: struct<cc_call_center_sk:int,cc_call_center_id:string,cc_name:string,cc_manager:string>
                  :     :     :     :     :     +- *(7) Project [cr_returned_date_sk#37, cr_returning_customer_sk#44, cr_call_center_sk#48, cr_net_loss#63]
                  :     :     :     :     :        +- *(7) Filter ((isnotnull(cr_call_center_sk#48) && isnotnull(cr_returned_date_sk#37)) && isnotnull(cr_returning_customer_sk#44))
                  :     :     :     :     :           +- *(7) FileScan parquet tpcds.catalog_returns[cr_returned_date_sk#37,cr_returning_customer_sk#44,cr_call_center_sk#48,cr_net_loss#63] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_call_center_sk), IsNotNull(cr_returned_date_sk), IsNotNull(cr_returning_customer_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_returning_customer_sk:int,cr_call_center_sk:int,cr_net_loss:dou...
                  :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     :     :     :        +- *(2) Project [d_date_sk#64]
                  :     :     :     :           +- *(2) Filter ((((isnotnull(d_year#70) && isnotnull(d_moy#72)) && (d_year#70 = 1998)) && (d_moy#72 = 11)) && isnotnull(d_date_sk#64))
                  :     :     :     :              +- *(2) FileScan parquet tpcds.date_dim[d_date_sk#64,d_year#70,d_moy#72] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                  :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     :     :        +- *(3) Project [c_customer_sk#92, c_current_cdemo_sk#94, c_current_hdemo_sk#95, c_current_addr_sk#96]
                  :     :     :           +- *(3) Filter (((isnotnull(c_customer_sk#92) && isnotnull(c_current_addr_sk#96)) && isnotnull(c_current_cdemo_sk#94)) && isnotnull(c_current_hdemo_sk#95))
                  :     :     :              +- *(3) FileScan parquet tpcds.customer[c_customer_sk#92,c_current_cdemo_sk#94,c_current_hdemo_sk#95,c_current_addr_sk#96] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk), IsNotNull(c_current_cdemo_sk), IsNotNull..., ReadSchema: struct<c_customer_sk:int,c_current_cdemo_sk:int,c_current_hdemo_sk:int,c_current_addr_sk:int>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :     :        +- *(4) Project [ca_address_sk#110]
                  :     :           +- *(4) Filter ((isnotnull(ca_gmt_offset#121) && (ca_gmt_offset#121 = -7.0)) && isnotnull(ca_address_sk#110))
                  :     :              +- *(4) FileScan parquet tpcds.customer_address[ca_address_sk#110,ca_gmt_offset#121] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_gmt_offset), EqualTo(ca_gmt_offset,-7.0), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_gmt_offset:double>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  :        +- *(5) Project [cd_demo_sk#123, cd_marital_status#125, cd_education_status#126]
                  :           +- *(5) Filter ((((cd_marital_status#125 = M) && (cd_education_status#126 = Unknown)) || ((cd_marital_status#125 = W) && (cd_education_status#126 = Advanced Degree))) && isnotnull(cd_demo_sk#123))
                  :              +- *(5) FileScan parquet tpcds.customer_demographics[cd_demo_sk#123,cd_marital_status#125,cd_education_status#126] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [Or(And(EqualTo(cd_marital_status,M),EqualTo(cd_education_status,Unknown)),And(EqualTo(cd_marital..., ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                     +- *(6) Project [hd_demo_sk#132]
                        +- *(6) Filter ((isnotnull(hd_buy_potential#134) && StartsWith(hd_buy_potential#134, Unknown)) && isnotnull(hd_demo_sk#132))
                           +- *(6) FileScan parquet tpcds.household_demographics[hd_demo_sk#132,hd_buy_potential#134] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/h..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_buy_potential), StringStartsWith(hd_buy_potential,Unknown), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_buy_potential:string>
Time taken: 4.072 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 91 in stream 0 using template query91.tpl
------------------------------------------------------^^^

