== Parsed Logical Plan ==
CTE [customer_total_return]
:  +- 'SubqueryAlias `customer_total_return`
:     +- 'Aggregate ['cr_returning_customer_sk, 'ca_state], ['cr_returning_customer_sk AS ctr_customer_sk#1, 'ca_state AS ctr_state#2, 'sum('cr_return_amt_inc_tax) AS ctr_total_return#3]
:        +- 'Filter ((('cr_returned_date_sk = 'd_date_sk) && ('d_year = 2000)) && ('cr_returning_addr_sk = 'ca_address_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation `catalog_returns`
:              :  +- 'UnresolvedRelation `date_dim`
:              +- 'UnresolvedRelation `customer_address`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['c_customer_id ASC NULLS FIRST, 'c_salutation ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 'c_last_name ASC NULLS FIRST, 'ca_street_number ASC NULLS FIRST, 'ca_street_name ASC NULLS FIRST, 'ca_street_type ASC NULLS FIRST, 'ca_suite_number ASC NULLS FIRST, 'ca_city ASC NULLS FIRST, 'ca_county ASC NULLS FIRST, 'ca_state ASC NULLS FIRST, 'ca_zip ASC NULLS FIRST, 'ca_country ASC NULLS FIRST, 'ca_gmt_offset ASC NULLS FIRST, 'ca_location_type ASC NULLS FIRST, 'ctr_total_return ASC NULLS FIRST], true
         +- 'Project ['c_customer_id, 'c_salutation, 'c_first_name, 'c_last_name, 'ca_street_number, 'ca_street_name, 'ca_street_type, 'ca_suite_number, 'ca_city, 'ca_county, 'ca_state, 'ca_zip, 'ca_country, 'ca_gmt_offset, 'ca_location_type, 'ctr_total_return]
            +- 'Filter ((('ctr1.ctr_total_return > scalar-subquery#0 []) && ('ca_address_sk = 'c_current_addr_sk)) && (('ca_state = GA) && ('ctr1.ctr_customer_sk = 'c_customer_sk)))
               :  +- 'Project [unresolvedalias(('avg('ctr_total_return) * 1.2), None)]
               :     +- 'Filter ('ctr1.ctr_state = 'ctr2.ctr_state)
               :        +- 'SubqueryAlias `ctr2`
               :           +- 'UnresolvedRelation `customer_total_return`
               +- 'Join Inner
                  :- 'Join Inner
                  :  :- 'SubqueryAlias `ctr1`
                  :  :  +- 'UnresolvedRelation `customer_total_return`
                  :  +- 'UnresolvedRelation `customer_address`
                  +- 'UnresolvedRelation `customer`

== Analyzed Logical Plan ==
c_customer_id: string, c_salutation: string, c_first_name: string, c_last_name: string, ca_street_number: string, ca_street_name: string, ca_street_type: string, ca_suite_number: string, ca_city: string, ca_county: string, ca_state: string, ca_zip: string, ca_country: string, ca_gmt_offset: double, ca_location_type: string, ctr_total_return: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_customer_id#76 ASC NULLS FIRST, c_salutation#82 ASC NULLS FIRST, c_first_name#83 ASC NULLS FIRST, c_last_name#84 ASC NULLS FIRST, ca_street_number#63 ASC NULLS FIRST, ca_street_name#64 ASC NULLS FIRST, ca_street_type#65 ASC NULLS FIRST, ca_suite_number#66 ASC NULLS FIRST, ca_city#67 ASC NULLS FIRST, ca_county#68 ASC NULLS FIRST, ca_state#69 ASC NULLS FIRST, ca_zip#70 ASC NULLS FIRST, ca_country#71 ASC NULLS FIRST, ca_gmt_offset#72 ASC NULLS FIRST, ca_location_type#73 ASC NULLS FIRST, ctr_total_return#3 ASC NULLS FIRST], true
      +- Project [c_customer_id#76, c_salutation#82, c_first_name#83, c_last_name#84, ca_street_number#63, ca_street_name#64, ca_street_type#65, ca_suite_number#66, ca_city#67, ca_county#68, ca_state#69, ca_zip#70, ca_country#71, ca_gmt_offset#72, ca_location_type#73, ctr_total_return#3]
         +- Filter (((ctr_total_return#3 > scalar-subquery#0 [ctr_state#2]) && (ca_address_sk#61 = c_current_addr_sk#79)) && ((ca_state#69 = GA) && (ctr_customer_sk#1 = c_customer_sk#75)))
            :  +- Aggregate [(avg(ctr_total_return#3) * cast(1.2 as double)) AS (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#94]
            :     +- Filter (outer(ctr_state#2) = ctr_state#2)
            :        +- SubqueryAlias `ctr2`
            :           +- SubqueryAlias `customer_total_return`
            :              +- Aggregate [cr_returning_customer_sk#13, ca_state#69], [cr_returning_customer_sk#13 AS ctr_customer_sk#1, ca_state#69 AS ctr_state#2, sum(cr_return_amt_inc_tax#26) AS ctr_total_return#3]
            :                 +- Filter (((cr_returned_date_sk#6 = d_date_sk#33) && (d_year#39 = 2000)) && (cr_returning_addr_sk#16 = ca_address_sk#61))
            :                    +- Join Inner
            :                       :- Join Inner
            :                       :  :- SubqueryAlias `tpcds`.`catalog_returns`
            :                       :  :  +- Relation[cr_returned_date_sk#6,cr_returned_time_sk#7,cr_item_sk#8,cr_refunded_customer_sk#9,cr_refunded_cdemo_sk#10,cr_refunded_hdemo_sk#11,cr_refunded_addr_sk#12,cr_returning_customer_sk#13,cr_returning_cdemo_sk#14,cr_returning_hdemo_sk#15,cr_returning_addr_sk#16,cr_call_center_sk#17,cr_catalog_page_sk#18,cr_ship_mode_sk#19,cr_warehouse_sk#20,cr_reason_sk#21,cr_order_number#22,cr_return_quantity#23,cr_return_amount#24,cr_return_tax#25,cr_return_amt_inc_tax#26,cr_fee#27,cr_return_ship_cost#28,cr_refunded_cash#29,... 3 more fields] parquet
            :                       :  +- SubqueryAlias `tpcds`.`date_dim`
            :                       :     +- Relation[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
            :                       +- SubqueryAlias `tpcds`.`customer_address`
            :                          +- Relation[ca_address_sk#61,ca_address_id#62,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73] parquet
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `ctr1`
               :  :  +- SubqueryAlias `customer_total_return`
               :  :     +- Aggregate [cr_returning_customer_sk#13, ca_state#69], [cr_returning_customer_sk#13 AS ctr_customer_sk#1, ca_state#69 AS ctr_state#2, sum(cr_return_amt_inc_tax#26) AS ctr_total_return#3]
               :  :        +- Filter (((cr_returned_date_sk#6 = d_date_sk#33) && (d_year#39 = 2000)) && (cr_returning_addr_sk#16 = ca_address_sk#61))
               :  :           +- Join Inner
               :  :              :- Join Inner
               :  :              :  :- SubqueryAlias `tpcds`.`catalog_returns`
               :  :              :  :  +- Relation[cr_returned_date_sk#6,cr_returned_time_sk#7,cr_item_sk#8,cr_refunded_customer_sk#9,cr_refunded_cdemo_sk#10,cr_refunded_hdemo_sk#11,cr_refunded_addr_sk#12,cr_returning_customer_sk#13,cr_returning_cdemo_sk#14,cr_returning_hdemo_sk#15,cr_returning_addr_sk#16,cr_call_center_sk#17,cr_catalog_page_sk#18,cr_ship_mode_sk#19,cr_warehouse_sk#20,cr_reason_sk#21,cr_order_number#22,cr_return_quantity#23,cr_return_amount#24,cr_return_tax#25,cr_return_amt_inc_tax#26,cr_fee#27,cr_return_ship_cost#28,cr_refunded_cash#29,... 3 more fields] parquet
               :  :              :  +- SubqueryAlias `tpcds`.`date_dim`
               :  :              :     +- Relation[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
               :  :              +- SubqueryAlias `tpcds`.`customer_address`
               :  :                 +- Relation[ca_address_sk#61,ca_address_id#62,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73] parquet
               :  +- SubqueryAlias `tpcds`.`customer_address`
               :     +- Relation[ca_address_sk#61,ca_address_id#62,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73] parquet
               +- SubqueryAlias `tpcds`.`customer`
                  +- Relation[c_customer_sk#75,c_customer_id#76,c_current_cdemo_sk#77,c_current_hdemo_sk#78,c_current_addr_sk#79,c_first_shipto_date_sk#80,c_first_sales_date_sk#81,c_salutation#82,c_first_name#83,c_last_name#84,c_preferred_cust_flag#85,c_birth_day#86,c_birth_month#87,c_birth_year#88,c_birth_country#89,c_login#90,c_email_address#91,c_last_review_date#92] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_customer_id#76 ASC NULLS FIRST, c_salutation#82 ASC NULLS FIRST, c_first_name#83 ASC NULLS FIRST, c_last_name#84 ASC NULLS FIRST, ca_street_number#63 ASC NULLS FIRST, ca_street_name#64 ASC NULLS FIRST, ca_street_type#65 ASC NULLS FIRST, ca_suite_number#66 ASC NULLS FIRST, ca_city#67 ASC NULLS FIRST, ca_county#68 ASC NULLS FIRST, ca_state#69 ASC NULLS FIRST, ca_zip#70 ASC NULLS FIRST, ca_country#71 ASC NULLS FIRST, ca_gmt_offset#72 ASC NULLS FIRST, ca_location_type#73 ASC NULLS FIRST, ctr_total_return#3 ASC NULLS FIRST], true
      +- Project [c_customer_id#76, c_salutation#82, c_first_name#83, c_last_name#84, ca_street_number#63, ca_street_name#64, ca_street_type#65, ca_suite_number#66, ca_city#67, ca_county#68, ca_state#69, ca_zip#70, ca_country#71, ca_gmt_offset#72, ca_location_type#73, ctr_total_return#3]
         +- Join Inner, (ca_address_sk#61 = c_current_addr_sk#79)
            :- Project [ctr_total_return#3, c_customer_id#76, c_current_addr_sk#79, c_salutation#82, c_first_name#83, c_last_name#84]
            :  +- Join Inner, (ctr_customer_sk#1 = c_customer_sk#75)
            :     :- Project [ctr_customer_sk#1, ctr_total_return#3]
            :     :  +- Join Inner, ((ctr_total_return#3 > (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#94) && (ctr_state#2 = ctr_state#2#95))
            :     :     :- Filter isnotnull(ctr_total_return#3)
            :     :     :  +- Aggregate [cr_returning_customer_sk#13, ca_state#69], [cr_returning_customer_sk#13 AS ctr_customer_sk#1, ca_state#69 AS ctr_state#2, sum(cr_return_amt_inc_tax#26) AS ctr_total_return#3]
            :     :     :     +- Project [cr_returning_customer_sk#13, cr_return_amt_inc_tax#26, ca_state#69]
            :     :     :        +- Join Inner, (cr_returning_addr_sk#16 = ca_address_sk#61)
            :     :     :           :- Project [cr_returning_customer_sk#13, cr_returning_addr_sk#16, cr_return_amt_inc_tax#26]
            :     :     :           :  +- Join Inner, (cr_returned_date_sk#6 = d_date_sk#33)
            :     :     :           :     :- Project [cr_returned_date_sk#6, cr_returning_customer_sk#13, cr_returning_addr_sk#16, cr_return_amt_inc_tax#26]
            :     :     :           :     :  +- Filter ((isnotnull(cr_returned_date_sk#6) && isnotnull(cr_returning_addr_sk#16)) && isnotnull(cr_returning_customer_sk#13))
            :     :     :           :     :     +- Relation[cr_returned_date_sk#6,cr_returned_time_sk#7,cr_item_sk#8,cr_refunded_customer_sk#9,cr_refunded_cdemo_sk#10,cr_refunded_hdemo_sk#11,cr_refunded_addr_sk#12,cr_returning_customer_sk#13,cr_returning_cdemo_sk#14,cr_returning_hdemo_sk#15,cr_returning_addr_sk#16,cr_call_center_sk#17,cr_catalog_page_sk#18,cr_ship_mode_sk#19,cr_warehouse_sk#20,cr_reason_sk#21,cr_order_number#22,cr_return_quantity#23,cr_return_amount#24,cr_return_tax#25,cr_return_amt_inc_tax#26,cr_fee#27,cr_return_ship_cost#28,cr_refunded_cash#29,... 3 more fields] parquet
            :     :     :           :     +- Project [d_date_sk#33]
            :     :     :           :        +- Filter ((isnotnull(d_year#39) && (d_year#39 = 2000)) && isnotnull(d_date_sk#33))
            :     :     :           :           +- Relation[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
            :     :     :           +- Project [ca_address_sk#61, ca_state#69]
            :     :     :              +- Filter (isnotnull(ca_address_sk#61) && isnotnull(ca_state#69))
            :     :     :                 +- Relation[ca_address_sk#61,ca_address_id#62,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73] parquet
            :     :     +- Filter isnotnull((avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#94)
            :     :        +- Aggregate [ctr_state#2], [(avg(ctr_total_return#3) * 1.2) AS (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#94, ctr_state#2 AS ctr_state#2#95]
            :     :           +- Aggregate [cr_returning_customer_sk#13, ca_state#69], [ca_state#69 AS ctr_state#2, sum(cr_return_amt_inc_tax#26) AS ctr_total_return#3]
            :     :              +- Project [cr_returning_customer_sk#13, cr_return_amt_inc_tax#26, ca_state#69]
            :     :                 +- Join Inner, (cr_returning_addr_sk#16 = ca_address_sk#61)
            :     :                    :- Project [cr_returning_customer_sk#13, cr_returning_addr_sk#16, cr_return_amt_inc_tax#26]
            :     :                    :  +- Join Inner, (cr_returned_date_sk#6 = d_date_sk#33)
            :     :                    :     :- Project [cr_returned_date_sk#6, cr_returning_customer_sk#13, cr_returning_addr_sk#16, cr_return_amt_inc_tax#26]
            :     :                    :     :  +- Filter (isnotnull(cr_returned_date_sk#6) && isnotnull(cr_returning_addr_sk#16))
            :     :                    :     :     +- Relation[cr_returned_date_sk#6,cr_returned_time_sk#7,cr_item_sk#8,cr_refunded_customer_sk#9,cr_refunded_cdemo_sk#10,cr_refunded_hdemo_sk#11,cr_refunded_addr_sk#12,cr_returning_customer_sk#13,cr_returning_cdemo_sk#14,cr_returning_hdemo_sk#15,cr_returning_addr_sk#16,cr_call_center_sk#17,cr_catalog_page_sk#18,cr_ship_mode_sk#19,cr_warehouse_sk#20,cr_reason_sk#21,cr_order_number#22,cr_return_quantity#23,cr_return_amount#24,cr_return_tax#25,cr_return_amt_inc_tax#26,cr_fee#27,cr_return_ship_cost#28,cr_refunded_cash#29,... 3 more fields] parquet
            :     :                    :     +- Project [d_date_sk#33]
            :     :                    :        +- Filter ((isnotnull(d_year#39) && (d_year#39 = 2000)) && isnotnull(d_date_sk#33))
            :     :                    :           +- Relation[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
            :     :                    +- Project [ca_address_sk#61, ca_state#69]
            :     :                       +- Filter (isnotnull(ca_address_sk#61) && isnotnull(ca_state#69))
            :     :                          +- Relation[ca_address_sk#61,ca_address_id#62,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73] parquet
            :     +- Project [c_customer_sk#75, c_customer_id#76, c_current_addr_sk#79, c_salutation#82, c_first_name#83, c_last_name#84]
            :        +- Filter (isnotnull(c_customer_sk#75) && isnotnull(c_current_addr_sk#79))
            :           +- Relation[c_customer_sk#75,c_customer_id#76,c_current_cdemo_sk#77,c_current_hdemo_sk#78,c_current_addr_sk#79,c_first_shipto_date_sk#80,c_first_sales_date_sk#81,c_salutation#82,c_first_name#83,c_last_name#84,c_preferred_cust_flag#85,c_birth_day#86,c_birth_month#87,c_birth_year#88,c_birth_country#89,c_login#90,c_email_address#91,c_last_review_date#92] parquet
            +- Project [ca_address_sk#61, ca_street_number#63, ca_street_name#64, ca_street_type#65, ca_suite_number#66, ca_city#67, ca_county#68, ca_state#69, ca_zip#70, ca_country#71, ca_gmt_offset#72, ca_location_type#73]
               +- Filter ((isnotnull(ca_state#69) && (ca_state#69 = GA)) && isnotnull(ca_address_sk#61))
                  +- Relation[ca_address_sk#61,ca_address_id#62,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[c_customer_id#76 ASC NULLS FIRST,c_salutation#82 ASC NULLS FIRST,c_first_name#83 ASC NULLS FIRST,c_last_name#84 ASC NULLS FIRST,ca_street_number#63 ASC NULLS FIRST,ca_street_name#64 ASC NULLS FIRST,ca_street_type#65 ASC NULLS FIRST,ca_suite_number#66 ASC NULLS FIRST,ca_city#67 ASC NULLS FIRST,ca_county#68 ASC NULLS FIRST,ca_state#69 ASC NULLS FIRST,ca_zip#70 ASC NULLS FIRST,ca_country#71 ASC NULLS FIRST,ca_gmt_offset#72 ASC NULLS FIRST,ca_location_type#73 ASC NULLS FIRST,ctr_total_return#3 ASC NULLS FIRST], output=[c_customer_id#76,c_salutation#82,c_first_name#83,c_last_name#84,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73,ctr_total_return#3])
+- *(14) Project [c_customer_id#76, c_salutation#82, c_first_name#83, c_last_name#84, ca_street_number#63, ca_street_name#64, ca_street_type#65, ca_suite_number#66, ca_city#67, ca_county#68, ca_state#69, ca_zip#70, ca_country#71, ca_gmt_offset#72, ca_location_type#73, ctr_total_return#3]
   +- *(14) BroadcastHashJoin [c_current_addr_sk#79], [ca_address_sk#61], Inner, BuildRight
      :- *(14) Project [ctr_total_return#3, c_customer_id#76, c_current_addr_sk#79, c_salutation#82, c_first_name#83, c_last_name#84]
      :  +- *(14) BroadcastHashJoin [ctr_customer_sk#1], [c_customer_sk#75], Inner, BuildRight
      :     :- *(14) Project [ctr_customer_sk#1, ctr_total_return#3]
      :     :  +- *(14) SortMergeJoin [ctr_state#2], [ctr_state#2#95], Inner, (ctr_total_return#3 > (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#94)
      :     :     :- *(5) Sort [ctr_state#2 ASC NULLS FIRST], false, 0
      :     :     :  +- Exchange hashpartitioning(ctr_state#2, 200)
      :     :     :     +- *(4) Filter isnotnull(ctr_total_return#3)
      :     :     :        +- *(4) HashAggregate(keys=[cr_returning_customer_sk#13, ca_state#69], functions=[sum(cr_return_amt_inc_tax#26)], output=[ctr_customer_sk#1, ctr_state#2, ctr_total_return#3])
      :     :     :           +- Exchange hashpartitioning(cr_returning_customer_sk#13, ca_state#69, 200)
      :     :     :              +- *(3) HashAggregate(keys=[cr_returning_customer_sk#13, ca_state#69], functions=[partial_sum(cr_return_amt_inc_tax#26)], output=[cr_returning_customer_sk#13, ca_state#69, sum#97])
      :     :     :                 +- *(3) Project [cr_returning_customer_sk#13, cr_return_amt_inc_tax#26, ca_state#69]
      :     :     :                    +- *(3) BroadcastHashJoin [cr_returning_addr_sk#16], [ca_address_sk#61], Inner, BuildRight
      :     :     :                       :- *(3) Project [cr_returning_customer_sk#13, cr_returning_addr_sk#16, cr_return_amt_inc_tax#26]
      :     :     :                       :  +- *(3) BroadcastHashJoin [cr_returned_date_sk#6], [d_date_sk#33], Inner, BuildRight
      :     :     :                       :     :- *(3) Project [cr_returned_date_sk#6, cr_returning_customer_sk#13, cr_returning_addr_sk#16, cr_return_amt_inc_tax#26]
      :     :     :                       :     :  +- *(3) Filter ((isnotnull(cr_returned_date_sk#6) && isnotnull(cr_returning_addr_sk#16)) && isnotnull(cr_returning_customer_sk#13))
      :     :     :                       :     :     +- *(3) FileScan parquet tpcds.catalog_returns[cr_returned_date_sk#6,cr_returning_customer_sk#13,cr_returning_addr_sk#16,cr_return_amt_inc_tax#26] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_returned_date_sk), IsNotNull(cr_returning_addr_sk), IsNotNull(cr_returning_customer..., ReadSchema: struct<cr_returned_date_sk:int,cr_returning_customer_sk:int,cr_returning_addr_sk:int,cr_return_am...
      :     :     :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :     :                       :        +- *(1) Project [d_date_sk#33]
      :     :     :                       :           +- *(1) Filter ((isnotnull(d_year#39) && (d_year#39 = 2000)) && isnotnull(d_date_sk#33))
      :     :     :                       :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#33,d_year#39] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
      :     :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :     :                          +- *(2) Project [ca_address_sk#61, ca_state#69]
      :     :     :                             +- *(2) Filter (isnotnull(ca_address_sk#61) && isnotnull(ca_state#69))
      :     :     :                                +- *(2) FileScan parquet tpcds.customer_address[ca_address_sk#61,ca_state#69] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_state)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
      :     :     +- *(11) Sort [ctr_state#2#95 ASC NULLS FIRST], false, 0
      :     :        +- Exchange hashpartitioning(ctr_state#2#95, 200)
      :     :           +- *(10) Filter isnotnull((avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#94)
      :     :              +- *(10) HashAggregate(keys=[ctr_state#2], functions=[avg(ctr_total_return#3)], output=[(avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#94, ctr_state#2#95])
      :     :                 +- Exchange hashpartitioning(ctr_state#2, 200)
      :     :                    +- *(9) HashAggregate(keys=[ctr_state#2], functions=[partial_avg(ctr_total_return#3)], output=[ctr_state#2, sum#100, count#101L])
      :     :                       +- *(9) HashAggregate(keys=[cr_returning_customer_sk#13, ca_state#69], functions=[sum(cr_return_amt_inc_tax#26)], output=[ctr_state#2, ctr_total_return#3])
      :     :                          +- Exchange hashpartitioning(cr_returning_customer_sk#13, ca_state#69, 200)
      :     :                             +- *(8) HashAggregate(keys=[cr_returning_customer_sk#13, ca_state#69], functions=[partial_sum(cr_return_amt_inc_tax#26)], output=[cr_returning_customer_sk#13, ca_state#69, sum#97])
      :     :                                +- *(8) Project [cr_returning_customer_sk#13, cr_return_amt_inc_tax#26, ca_state#69]
      :     :                                   +- *(8) BroadcastHashJoin [cr_returning_addr_sk#16], [ca_address_sk#61], Inner, BuildRight
      :     :                                      :- *(8) Project [cr_returning_customer_sk#13, cr_returning_addr_sk#16, cr_return_amt_inc_tax#26]
      :     :                                      :  +- *(8) BroadcastHashJoin [cr_returned_date_sk#6], [d_date_sk#33], Inner, BuildRight
      :     :                                      :     :- *(8) Project [cr_returned_date_sk#6, cr_returning_customer_sk#13, cr_returning_addr_sk#16, cr_return_amt_inc_tax#26]
      :     :                                      :     :  +- *(8) Filter (isnotnull(cr_returned_date_sk#6) && isnotnull(cr_returning_addr_sk#16))
      :     :                                      :     :     +- *(8) FileScan parquet tpcds.catalog_returns[cr_returned_date_sk#6,cr_returning_customer_sk#13,cr_returning_addr_sk#16,cr_return_amt_inc_tax#26] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cr_returned_date_sk), IsNotNull(cr_returning_addr_sk)], ReadSchema: struct<cr_returned_date_sk:int,cr_returning_customer_sk:int,cr_returning_addr_sk:int,cr_return_am...
      :     :                                      :     +- ReusedExchange [d_date_sk#33], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                                      +- ReusedExchange [ca_address_sk#61, ca_state#69], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :        +- *(12) Project [c_customer_sk#75, c_customer_id#76, c_current_addr_sk#79, c_salutation#82, c_first_name#83, c_last_name#84]
      :           +- *(12) Filter (isnotnull(c_customer_sk#75) && isnotnull(c_current_addr_sk#79))
      :              +- *(12) FileScan parquet tpcds.customer[c_customer_sk#75,c_customer_id#76,c_current_addr_sk#79,c_salutation#82,c_first_name#83,c_last_name#84] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_current_addr_sk:int,c_salutation:string,c_first_n...
      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         +- *(13) Project [ca_address_sk#61, ca_street_number#63, ca_street_name#64, ca_street_type#65, ca_suite_number#66, ca_city#67, ca_county#68, ca_state#69, ca_zip#70, ca_country#71, ca_gmt_offset#72, ca_location_type#73]
            +- *(13) Filter ((isnotnull(ca_state#69) && (ca_state#69 = GA)) && isnotnull(ca_address_sk#61))
               +- *(13) FileScan parquet tpcds.customer_address[ca_address_sk#61,ca_street_number#63,ca_street_name#64,ca_street_type#65,ca_suite_number#66,ca_city#67,ca_county#68,ca_state#69,ca_zip#70,ca_country#71,ca_gmt_offset#72,ca_location_type#73] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,GA), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_street_number:string,ca_street_name:string,ca_street_type:string,ca_s...
Time taken: 4.346 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 81 in stream 0 using template query81.tpl
------------------------------------------------------^^^

