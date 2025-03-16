Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583117752
== Parsed Logical Plan ==
'Sort ['sum('cr_net_loss) DESC NULLS LAST], true
+- 'Aggregate ['cc_call_center_id, 'cc_name, 'cc_manager, 'cd_marital_status, 'cd_education_status], ['cc_call_center_id AS Call_Center#0, 'cc_name AS Call_Center_Name#1, 'cc_manager AS Manager#2, 'sum('cr_net_loss) AS Returns_Loss#3]
   +- 'Filter ((((('cr_call_center_sk = 'cc_call_center_sk) AND ('cr_returned_date_sk = 'd_date_sk)) AND ('cr_returning_customer_sk = 'c_customer_sk)) AND ((('cd_demo_sk = 'c_current_cdemo_sk) AND ('hd_demo_sk = 'c_current_hdemo_sk)) AND ('ca_address_sk = 'c_current_addr_sk))) AND (((('d_year = 1998) AND ('d_moy = 11)) AND ((('cd_marital_status = M) AND ('cd_education_status = Unknown)) OR (('cd_marital_status = W) AND ('cd_education_status = Advanced Degree)))) AND ('hd_buy_potential LIKE Unknown% AND ('ca_gmt_offset = -7))))
      +- 'Join Inner
         :- 'Join Inner
         :  :- 'Join Inner
         :  :  :- 'Join Inner
         :  :  :  :- 'Join Inner
         :  :  :  :  :- 'Join Inner
         :  :  :  :  :  :- 'UnresolvedRelation [call_center], [], false
         :  :  :  :  :  +- 'UnresolvedRelation [catalog_returns], [], false
         :  :  :  :  +- 'UnresolvedRelation [date_dim], [], false
         :  :  :  +- 'UnresolvedRelation [customer], [], false
         :  :  +- 'UnresolvedRelation [customer_address], [], false
         :  +- 'UnresolvedRelation [customer_demographics], [], false
         +- 'UnresolvedRelation [household_demographics], [], false

== Analyzed Logical Plan ==
Call_Center: string, Call_Center_Name: string, Manager: string, Returns_Loss: double
Sort [Returns_Loss#3 DESC NULLS LAST], true
+- Aggregate [cc_call_center_id#10, cc_name#15, cc_manager#20, cd_marital_status#128, cd_education_status#129], [cc_call_center_id#10 AS Call_Center#0, cc_name#15 AS Call_Center_Name#1, cc_manager#20 AS Manager#2, sum(cr_net_loss#66) AS Returns_Loss#3]
   +- Filter (((((cr_call_center_sk#51 = cc_call_center_sk#9) AND (cr_returned_date_sk#40 = d_date_sk#67)) AND (cr_returning_customer_sk#47 = c_customer_sk#95)) AND (((cd_demo_sk#126 = c_current_cdemo_sk#97) AND (hd_demo_sk#135 = c_current_hdemo_sk#98)) AND (ca_address_sk#113 = c_current_addr_sk#99))) AND ((((d_year#73 = 1998) AND (d_moy#75 = 11)) AND (((cd_marital_status#128 = M) AND (cd_education_status#129 = Unknown)) OR ((cd_marital_status#128 = W) AND (cd_education_status#129 = Advanced Degree)))) AND (hd_buy_potential#137 LIKE Unknown% AND (ca_gmt_offset#124 = cast(-7 as double)))))
      +- Join Inner
         :- Join Inner
         :  :- Join Inner
         :  :  :- Join Inner
         :  :  :  :- Join Inner
         :  :  :  :  :- Join Inner
         :  :  :  :  :  :- SubqueryAlias spark_catalog.tpcds.call_center
         :  :  :  :  :  :  +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#9,cc_call_center_id#10,cc_rec_start_date#11,cc_rec_end_date#12,cc_closed_date_sk#13,cc_open_date_sk#14,cc_name#15,cc_class#16,cc_employees#17,cc_sq_ft#18,cc_hours#19,cc_manager#20,cc_mkt_id#21,cc_mkt_class#22,cc_mkt_desc#23,cc_market_manager#24,cc_division#25,cc_division_name#26,cc_company#27,cc_company_name#28,cc_street_number#29,cc_street_name#30,cc_street_type#31,cc_suite_number#32,... 7 more fields] parquet
         :  :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.catalog_returns
         :  :  :  :  :     +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#40,cr_returned_time_sk#41,cr_item_sk#42,cr_refunded_customer_sk#43,cr_refunded_cdemo_sk#44,cr_refunded_hdemo_sk#45,cr_refunded_addr_sk#46,cr_returning_customer_sk#47,cr_returning_cdemo_sk#48,cr_returning_hdemo_sk#49,cr_returning_addr_sk#50,cr_call_center_sk#51,cr_catalog_page_sk#52,cr_ship_mode_sk#53,cr_warehouse_sk#54,cr_reason_sk#55,cr_order_number#56,cr_return_quantity#57,cr_return_amount#58,cr_return_tax#59,cr_return_amt_inc_tax#60,cr_fee#61,cr_return_ship_cost#62,cr_refunded_cash#63,... 3 more fields] parquet
         :  :  :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
         :  :  :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#67,d_date_id#68,d_date#69,d_month_seq#70,d_week_seq#71,d_quarter_seq#72,d_year#73,d_dow#74,d_moy#75,d_dom#76,d_qoy#77,d_fy_year#78,d_fy_quarter_seq#79,d_fy_week_seq#80,d_day_name#81,d_quarter_name#82,d_holiday#83,d_weekend#84,d_following_holiday#85,d_first_dom#86,d_last_dom#87,d_same_day_ly#88,d_same_day_lq#89,d_current_day#90,... 4 more fields] parquet
         :  :  :  +- SubqueryAlias spark_catalog.tpcds.customer
         :  :  :     +- Relation spark_catalog.tpcds.customer[c_customer_sk#95,c_customer_id#96,c_current_cdemo_sk#97,c_current_hdemo_sk#98,c_current_addr_sk#99,c_first_shipto_date_sk#100,c_first_sales_date_sk#101,c_salutation#102,c_first_name#103,c_last_name#104,c_preferred_cust_flag#105,c_birth_day#106,c_birth_month#107,c_birth_year#108,c_birth_country#109,c_login#110,c_email_address#111,c_last_review_date#112] parquet
         :  :  +- SubqueryAlias spark_catalog.tpcds.customer_address
         :  :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#113,ca_address_id#114,ca_street_number#115,ca_street_name#116,ca_street_type#117,ca_suite_number#118,ca_city#119,ca_county#120,ca_state#121,ca_zip#122,ca_country#123,ca_gmt_offset#124,ca_location_type#125] parquet
         :  +- SubqueryAlias spark_catalog.tpcds.customer_demographics
         :     +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#126,cd_gender#127,cd_marital_status#128,cd_education_status#129,cd_purchase_estimate#130,cd_credit_rating#131,cd_dep_count#132,cd_dep_employed_count#133,cd_dep_college_count#134] parquet
         +- SubqueryAlias spark_catalog.tpcds.household_demographics
            +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#135,hd_income_band_sk#136,hd_buy_potential#137,hd_dep_count#138,hd_vehicle_count#139] parquet

== Optimized Logical Plan ==
Sort [Returns_Loss#3 DESC NULLS LAST], true
+- Aggregate [cc_call_center_id#10, cc_name#15, cc_manager#20, cd_marital_status#128, cd_education_status#129], [cc_call_center_id#10 AS Call_Center#0, cc_name#15 AS Call_Center_Name#1, cc_manager#20 AS Manager#2, sum(cr_net_loss#66) AS Returns_Loss#3]
   +- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_net_loss#66, cd_marital_status#128, cd_education_status#129]
      +- Join Inner, (hd_demo_sk#135 = c_current_hdemo_sk#98)
         :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_net_loss#66, c_current_hdemo_sk#98, cd_marital_status#128, cd_education_status#129]
         :  +- Join Inner, (cd_demo_sk#126 = c_current_cdemo_sk#97)
         :     :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_net_loss#66, c_current_cdemo_sk#97, c_current_hdemo_sk#98]
         :     :  +- Join Inner, (ca_address_sk#113 = c_current_addr_sk#99)
         :     :     :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_net_loss#66, c_current_cdemo_sk#97, c_current_hdemo_sk#98, c_current_addr_sk#99]
         :     :     :  +- Join Inner, (cr_returning_customer_sk#47 = c_customer_sk#95)
         :     :     :     :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_returning_customer_sk#47, cr_net_loss#66]
         :     :     :     :  +- Join Inner, (cr_returned_date_sk#40 = d_date_sk#67)
         :     :     :     :     :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_returned_date_sk#40, cr_returning_customer_sk#47, cr_net_loss#66]
         :     :     :     :     :  +- Join Inner, (cr_call_center_sk#51 = cc_call_center_sk#9)
         :     :     :     :     :     :- Project [cc_call_center_sk#9, cc_call_center_id#10, cc_name#15, cc_manager#20]
         :     :     :     :     :     :  +- Filter isnotnull(cc_call_center_sk#9)
         :     :     :     :     :     :     +- Relation spark_catalog.tpcds.call_center[cc_call_center_sk#9,cc_call_center_id#10,cc_rec_start_date#11,cc_rec_end_date#12,cc_closed_date_sk#13,cc_open_date_sk#14,cc_name#15,cc_class#16,cc_employees#17,cc_sq_ft#18,cc_hours#19,cc_manager#20,cc_mkt_id#21,cc_mkt_class#22,cc_mkt_desc#23,cc_market_manager#24,cc_division#25,cc_division_name#26,cc_company#27,cc_company_name#28,cc_street_number#29,cc_street_name#30,cc_street_type#31,cc_suite_number#32,... 7 more fields] parquet
         :     :     :     :     :     +- Project [cr_returned_date_sk#40, cr_returning_customer_sk#47, cr_call_center_sk#51, cr_net_loss#66]
         :     :     :     :     :        +- Filter (isnotnull(cr_call_center_sk#51) AND (isnotnull(cr_returned_date_sk#40) AND isnotnull(cr_returning_customer_sk#47)))
         :     :     :     :     :           +- Relation spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#40,cr_returned_time_sk#41,cr_item_sk#42,cr_refunded_customer_sk#43,cr_refunded_cdemo_sk#44,cr_refunded_hdemo_sk#45,cr_refunded_addr_sk#46,cr_returning_customer_sk#47,cr_returning_cdemo_sk#48,cr_returning_hdemo_sk#49,cr_returning_addr_sk#50,cr_call_center_sk#51,cr_catalog_page_sk#52,cr_ship_mode_sk#53,cr_warehouse_sk#54,cr_reason_sk#55,cr_order_number#56,cr_return_quantity#57,cr_return_amount#58,cr_return_tax#59,cr_return_amt_inc_tax#60,cr_fee#61,cr_return_ship_cost#62,cr_refunded_cash#63,... 3 more fields] parquet
         :     :     :     :     +- Project [d_date_sk#67]
         :     :     :     :        +- Filter (((isnotnull(d_year#73) AND isnotnull(d_moy#75)) AND ((d_year#73 = 1998) AND (d_moy#75 = 11))) AND isnotnull(d_date_sk#67))
         :     :     :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#67,d_date_id#68,d_date#69,d_month_seq#70,d_week_seq#71,d_quarter_seq#72,d_year#73,d_dow#74,d_moy#75,d_dom#76,d_qoy#77,d_fy_year#78,d_fy_quarter_seq#79,d_fy_week_seq#80,d_day_name#81,d_quarter_name#82,d_holiday#83,d_weekend#84,d_following_holiday#85,d_first_dom#86,d_last_dom#87,d_same_day_ly#88,d_same_day_lq#89,d_current_day#90,... 4 more fields] parquet
         :     :     :     +- Project [c_customer_sk#95, c_current_cdemo_sk#97, c_current_hdemo_sk#98, c_current_addr_sk#99]
         :     :     :        +- Filter ((isnotnull(c_customer_sk#95) AND isnotnull(c_current_addr_sk#99)) AND (isnotnull(c_current_cdemo_sk#97) AND isnotnull(c_current_hdemo_sk#98)))
         :     :     :           +- Relation spark_catalog.tpcds.customer[c_customer_sk#95,c_customer_id#96,c_current_cdemo_sk#97,c_current_hdemo_sk#98,c_current_addr_sk#99,c_first_shipto_date_sk#100,c_first_sales_date_sk#101,c_salutation#102,c_first_name#103,c_last_name#104,c_preferred_cust_flag#105,c_birth_day#106,c_birth_month#107,c_birth_year#108,c_birth_country#109,c_login#110,c_email_address#111,c_last_review_date#112] parquet
         :     :     +- Project [ca_address_sk#113]
         :     :        +- Filter ((isnotnull(ca_gmt_offset#124) AND (ca_gmt_offset#124 = -7.0)) AND isnotnull(ca_address_sk#113))
         :     :           +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#113,ca_address_id#114,ca_street_number#115,ca_street_name#116,ca_street_type#117,ca_suite_number#118,ca_city#119,ca_county#120,ca_state#121,ca_zip#122,ca_country#123,ca_gmt_offset#124,ca_location_type#125] parquet
         :     +- Project [cd_demo_sk#126, cd_marital_status#128, cd_education_status#129]
         :        +- Filter ((((cd_marital_status#128 = M) AND (cd_education_status#129 = Unknown)) OR ((cd_marital_status#128 = W) AND (cd_education_status#129 = Advanced Degree))) AND isnotnull(cd_demo_sk#126))
         :           +- Relation spark_catalog.tpcds.customer_demographics[cd_demo_sk#126,cd_gender#127,cd_marital_status#128,cd_education_status#129,cd_purchase_estimate#130,cd_credit_rating#131,cd_dep_count#132,cd_dep_employed_count#133,cd_dep_college_count#134] parquet
         +- Project [hd_demo_sk#135]
            +- Filter ((isnotnull(hd_buy_potential#137) AND StartsWith(hd_buy_potential#137, Unknown)) AND isnotnull(hd_demo_sk#135))
               +- Relation spark_catalog.tpcds.household_demographics[hd_demo_sk#135,hd_income_band_sk#136,hd_buy_potential#137,hd_dep_count#138,hd_vehicle_count#139] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [Returns_Loss#3 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(Returns_Loss#3 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=164]
      +- HashAggregate(keys=[cc_call_center_id#10, cc_name#15, cc_manager#20, cd_marital_status#128, cd_education_status#129], functions=[sum(cr_net_loss#66)], output=[Call_Center#0, Call_Center_Name#1, Manager#2, Returns_Loss#3])
         +- Exchange hashpartitioning(cc_call_center_id#10, cc_name#15, cc_manager#20, cd_marital_status#128, cd_education_status#129, 200), ENSURE_REQUIREMENTS, [plan_id=161]
            +- HashAggregate(keys=[cc_call_center_id#10, cc_name#15, cc_manager#20, cd_marital_status#128, cd_education_status#129], functions=[partial_sum(cr_net_loss#66)], output=[cc_call_center_id#10, cc_name#15, cc_manager#20, cd_marital_status#128, cd_education_status#129, sum#150])
               +- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_net_loss#66, cd_marital_status#128, cd_education_status#129]
                  +- BroadcastHashJoin [c_current_hdemo_sk#98], [hd_demo_sk#135], Inner, BuildRight, false
                     :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_net_loss#66, c_current_hdemo_sk#98, cd_marital_status#128, cd_education_status#129]
                     :  +- BroadcastHashJoin [c_current_cdemo_sk#97], [cd_demo_sk#126], Inner, BuildRight, false
                     :     :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_net_loss#66, c_current_cdemo_sk#97, c_current_hdemo_sk#98]
                     :     :  +- BroadcastHashJoin [c_current_addr_sk#99], [ca_address_sk#113], Inner, BuildRight, false
                     :     :     :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_net_loss#66, c_current_cdemo_sk#97, c_current_hdemo_sk#98, c_current_addr_sk#99]
                     :     :     :  +- BroadcastHashJoin [cr_returning_customer_sk#47], [c_customer_sk#95], Inner, BuildRight, false
                     :     :     :     :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_returning_customer_sk#47, cr_net_loss#66]
                     :     :     :     :  +- BroadcastHashJoin [cr_returned_date_sk#40], [d_date_sk#67], Inner, BuildRight, false
                     :     :     :     :     :- Project [cc_call_center_id#10, cc_name#15, cc_manager#20, cr_returned_date_sk#40, cr_returning_customer_sk#47, cr_net_loss#66]
                     :     :     :     :     :  +- BroadcastHashJoin [cc_call_center_sk#9], [cr_call_center_sk#51], Inner, BuildLeft, false
                     :     :     :     :     :     :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=136]
                     :     :     :     :     :     :  +- Filter isnotnull(cc_call_center_sk#9)
                     :     :     :     :     :     :     +- FileScan parquet spark_catalog.tpcds.call_center[cc_call_center_sk#9,cc_call_center_id#10,cc_name#15,cc_manager#20] Batched: true, DataFilters: [isnotnull(cc_call_center_sk#9)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cc_call_center_sk)], ReadSchema: struct<cc_call_center_sk:int,cc_call_center_id:string,cc_name:string,cc_manager:string>
                     :     :     :     :     :     +- Filter ((isnotnull(cr_call_center_sk#51) AND isnotnull(cr_returned_date_sk#40)) AND isnotnull(cr_returning_customer_sk#47))
                     :     :     :     :     :        +- FileScan parquet spark_catalog.tpcds.catalog_returns[cr_returned_date_sk#40,cr_returning_customer_sk#47,cr_call_center_sk#51,cr_net_loss#66] Batched: true, DataFilters: [isnotnull(cr_call_center_sk#51), isnotnull(cr_returned_date_sk#40), isnotnull(cr_returning_custo..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_call_center_sk), IsNotNull(cr_returned_date_sk), IsNotNull(cr_returning_customer_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_returning_customer_sk:int,cr_call_center_sk:int,cr_net_loss:dou...
                     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=140]
                     :     :     :     :        +- Project [d_date_sk#67]
                     :     :     :     :           +- Filter ((((isnotnull(d_year#73) AND isnotnull(d_moy#75)) AND (d_year#73 = 1998)) AND (d_moy#75 = 11)) AND isnotnull(d_date_sk#67))
                     :     :     :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#67,d_year#73,d_moy#75] Batched: true, DataFilters: [isnotnull(d_year#73), isnotnull(d_moy#75), (d_year#73 = 1998), (d_moy#75 = 11), isnotnull(d_date..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,1998), EqualTo(d_moy,11), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_moy:int>
                     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=144]
                     :     :     :        +- Filter (((isnotnull(c_customer_sk#95) AND isnotnull(c_current_addr_sk#99)) AND isnotnull(c_current_cdemo_sk#97)) AND isnotnull(c_current_hdemo_sk#98))
                     :     :     :           +- FileScan parquet spark_catalog.tpcds.customer[c_customer_sk#95,c_current_cdemo_sk#97,c_current_hdemo_sk#98,c_current_addr_sk#99] Batched: true, DataFilters: [isnotnull(c_customer_sk#95), isnotnull(c_current_addr_sk#99), isnotnull(c_current_cdemo_sk#97), ..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk), IsNotNull(c_current_cdemo_sk), IsNotNull..., ReadSchema: struct<c_customer_sk:int,c_current_cdemo_sk:int,c_current_hdemo_sk:int,c_current_addr_sk:int>
                     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=148]
                     :     :        +- Project [ca_address_sk#113]
                     :     :           +- Filter ((isnotnull(ca_gmt_offset#124) AND (ca_gmt_offset#124 = -7.0)) AND isnotnull(ca_address_sk#113))
                     :     :              +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#113,ca_gmt_offset#124] Batched: true, DataFilters: [isnotnull(ca_gmt_offset#124), (ca_gmt_offset#124 = -7.0), isnotnull(ca_address_sk#113)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_gmt_offset), EqualTo(ca_gmt_offset,-7.0), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_gmt_offset:double>
                     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=152]
                     :        +- Filter ((((cd_marital_status#128 = M) AND (cd_education_status#129 = Unknown)) OR ((cd_marital_status#128 = W) AND (cd_education_status#129 = Advanced Degree))) AND isnotnull(cd_demo_sk#126))
                     :           +- FileScan parquet spark_catalog.tpcds.customer_demographics[cd_demo_sk#126,cd_marital_status#128,cd_education_status#129] Batched: true, DataFilters: [(((cd_marital_status#128 = M) AND (cd_education_status#129 = Unknown)) OR ((cd_marital_status#12..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [Or(And(EqualTo(cd_marital_status,M),EqualTo(cd_education_status,Unknown)),And(EqualTo(cd_marital..., ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=156]
                        +- Project [hd_demo_sk#135]
                           +- Filter ((isnotnull(hd_buy_potential#137) AND StartsWith(hd_buy_potential#137, Unknown)) AND isnotnull(hd_demo_sk#135))
                              +- FileScan parquet spark_catalog.tpcds.household_demographics[hd_demo_sk#135,hd_buy_potential#137] Batched: true, DataFilters: [isnotnull(hd_buy_potential#137), StartsWith(hd_buy_potential#137, Unknown), isnotnull(hd_demo_sk..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(hd_buy_potential), StringStartsWith(hd_buy_potential,Unknown), IsNotNull(hd_demo_sk)], ReadSchema: struct<hd_demo_sk:int,hd_buy_potential:string>

Time taken: 4.873 seconds, Fetched 1 row(s)
