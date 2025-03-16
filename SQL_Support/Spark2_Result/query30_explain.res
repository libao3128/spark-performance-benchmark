== Parsed Logical Plan ==
CTE [customer_total_return]
:  +- 'SubqueryAlias `customer_total_return`
:     +- 'Aggregate ['wr_returning_customer_sk, 'ca_state], ['wr_returning_customer_sk AS ctr_customer_sk#1, 'ca_state AS ctr_state#2, 'sum('wr_return_amt) AS ctr_total_return#3]
:        +- 'Filter ((('wr_returned_date_sk = 'd_date_sk) && ('d_year = 2002)) && ('wr_returning_addr_sk = 'ca_address_sk))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'UnresolvedRelation `web_returns`
:              :  +- 'UnresolvedRelation `date_dim`
:              +- 'UnresolvedRelation `customer_address`
+- 'GlobalLimit 100
   +- 'LocalLimit 100
      +- 'Sort ['c_customer_id ASC NULLS FIRST, 'c_salutation ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 'c_last_name ASC NULLS FIRST, 'c_preferred_cust_flag ASC NULLS FIRST, 'c_birth_day ASC NULLS FIRST, 'c_birth_month ASC NULLS FIRST, 'c_birth_year ASC NULLS FIRST, 'c_birth_country ASC NULLS FIRST, 'c_login ASC NULLS FIRST, 'c_email_address ASC NULLS FIRST, 'c_last_review_date ASC NULLS FIRST, 'ctr_total_return ASC NULLS FIRST], true
         +- 'Project ['c_customer_id, 'c_salutation, 'c_first_name, 'c_last_name, 'c_preferred_cust_flag, 'c_birth_day, 'c_birth_month, 'c_birth_year, 'c_birth_country, 'c_login, 'c_email_address, 'c_last_review_date, 'ctr_total_return]
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
c_customer_id: string, c_salutation: string, c_first_name: string, c_last_name: string, c_preferred_cust_flag: string, c_birth_day: int, c_birth_month: int, c_birth_year: int, c_birth_country: string, c_login: string, c_email_address: string, c_last_review_date: string, ctr_total_return: double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_customer_id#73 ASC NULLS FIRST, c_salutation#79 ASC NULLS FIRST, c_first_name#80 ASC NULLS FIRST, c_last_name#81 ASC NULLS FIRST, c_preferred_cust_flag#82 ASC NULLS FIRST, c_birth_day#83 ASC NULLS FIRST, c_birth_month#84 ASC NULLS FIRST, c_birth_year#85 ASC NULLS FIRST, c_birth_country#86 ASC NULLS FIRST, c_login#87 ASC NULLS FIRST, c_email_address#88 ASC NULLS FIRST, c_last_review_date#89 ASC NULLS FIRST, ctr_total_return#3 ASC NULLS FIRST], true
      +- Project [c_customer_id#73, c_salutation#79, c_first_name#80, c_last_name#81, c_preferred_cust_flag#82, c_birth_day#83, c_birth_month#84, c_birth_year#85, c_birth_country#86, c_login#87, c_email_address#88, c_last_review_date#89, ctr_total_return#3]
         +- Filter (((ctr_total_return#3 > scalar-subquery#0 [ctr_state#2]) && (ca_address_sk#58 = c_current_addr_sk#76)) && ((ca_state#66 = GA) && (ctr_customer_sk#1 = c_customer_sk#72)))
            :  +- Aggregate [(avg(ctr_total_return#3) * cast(1.2 as double)) AS (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#91]
            :     +- Filter (outer(ctr_state#2) = ctr_state#2)
            :        +- SubqueryAlias `ctr2`
            :           +- SubqueryAlias `customer_total_return`
            :              +- Aggregate [wr_returning_customer_sk#13, ca_state#66], [wr_returning_customer_sk#13 AS ctr_customer_sk#1, ca_state#66 AS ctr_state#2, sum(wr_return_amt#21) AS ctr_total_return#3]
            :                 +- Filter (((wr_returned_date_sk#6 = d_date_sk#30) && (d_year#36 = 2002)) && (wr_returning_addr_sk#16 = ca_address_sk#58))
            :                    +- Join Inner
            :                       :- Join Inner
            :                       :  :- SubqueryAlias `tpcds`.`web_returns`
            :                       :  :  +- Relation[wr_returned_date_sk#6,wr_returned_time_sk#7,wr_item_sk#8,wr_refunded_customer_sk#9,wr_refunded_cdemo_sk#10,wr_refunded_hdemo_sk#11,wr_refunded_addr_sk#12,wr_returning_customer_sk#13,wr_returning_cdemo_sk#14,wr_returning_hdemo_sk#15,wr_returning_addr_sk#16,wr_web_page_sk#17,wr_reason_sk#18,wr_order_number#19,wr_return_quantity#20,wr_return_amt#21,wr_return_tax#22,wr_return_amt_inc_tax#23,wr_fee#24,wr_return_ship_cost#25,wr_refunded_cash#26,wr_reversed_charge#27,wr_account_credit#28,wr_net_loss#29] parquet
            :                       :  +- SubqueryAlias `tpcds`.`date_dim`
            :                       :     +- Relation[d_date_sk#30,d_date_id#31,d_date#32,d_month_seq#33,d_week_seq#34,d_quarter_seq#35,d_year#36,d_dow#37,d_moy#38,d_dom#39,d_qoy#40,d_fy_year#41,d_fy_quarter_seq#42,d_fy_week_seq#43,d_day_name#44,d_quarter_name#45,d_holiday#46,d_weekend#47,d_following_holiday#48,d_first_dom#49,d_last_dom#50,d_same_day_ly#51,d_same_day_lq#52,d_current_day#53,... 4 more fields] parquet
            :                       +- SubqueryAlias `tpcds`.`customer_address`
            :                          +- Relation[ca_address_sk#58,ca_address_id#59,ca_street_number#60,ca_street_name#61,ca_street_type#62,ca_suite_number#63,ca_city#64,ca_county#65,ca_state#66,ca_zip#67,ca_country#68,ca_gmt_offset#69,ca_location_type#70] parquet
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias `ctr1`
               :  :  +- SubqueryAlias `customer_total_return`
               :  :     +- Aggregate [wr_returning_customer_sk#13, ca_state#66], [wr_returning_customer_sk#13 AS ctr_customer_sk#1, ca_state#66 AS ctr_state#2, sum(wr_return_amt#21) AS ctr_total_return#3]
               :  :        +- Filter (((wr_returned_date_sk#6 = d_date_sk#30) && (d_year#36 = 2002)) && (wr_returning_addr_sk#16 = ca_address_sk#58))
               :  :           +- Join Inner
               :  :              :- Join Inner
               :  :              :  :- SubqueryAlias `tpcds`.`web_returns`
               :  :              :  :  +- Relation[wr_returned_date_sk#6,wr_returned_time_sk#7,wr_item_sk#8,wr_refunded_customer_sk#9,wr_refunded_cdemo_sk#10,wr_refunded_hdemo_sk#11,wr_refunded_addr_sk#12,wr_returning_customer_sk#13,wr_returning_cdemo_sk#14,wr_returning_hdemo_sk#15,wr_returning_addr_sk#16,wr_web_page_sk#17,wr_reason_sk#18,wr_order_number#19,wr_return_quantity#20,wr_return_amt#21,wr_return_tax#22,wr_return_amt_inc_tax#23,wr_fee#24,wr_return_ship_cost#25,wr_refunded_cash#26,wr_reversed_charge#27,wr_account_credit#28,wr_net_loss#29] parquet
               :  :              :  +- SubqueryAlias `tpcds`.`date_dim`
               :  :              :     +- Relation[d_date_sk#30,d_date_id#31,d_date#32,d_month_seq#33,d_week_seq#34,d_quarter_seq#35,d_year#36,d_dow#37,d_moy#38,d_dom#39,d_qoy#40,d_fy_year#41,d_fy_quarter_seq#42,d_fy_week_seq#43,d_day_name#44,d_quarter_name#45,d_holiday#46,d_weekend#47,d_following_holiday#48,d_first_dom#49,d_last_dom#50,d_same_day_ly#51,d_same_day_lq#52,d_current_day#53,... 4 more fields] parquet
               :  :              +- SubqueryAlias `tpcds`.`customer_address`
               :  :                 +- Relation[ca_address_sk#58,ca_address_id#59,ca_street_number#60,ca_street_name#61,ca_street_type#62,ca_suite_number#63,ca_city#64,ca_county#65,ca_state#66,ca_zip#67,ca_country#68,ca_gmt_offset#69,ca_location_type#70] parquet
               :  +- SubqueryAlias `tpcds`.`customer_address`
               :     +- Relation[ca_address_sk#58,ca_address_id#59,ca_street_number#60,ca_street_name#61,ca_street_type#62,ca_suite_number#63,ca_city#64,ca_county#65,ca_state#66,ca_zip#67,ca_country#68,ca_gmt_offset#69,ca_location_type#70] parquet
               +- SubqueryAlias `tpcds`.`customer`
                  +- Relation[c_customer_sk#72,c_customer_id#73,c_current_cdemo_sk#74,c_current_hdemo_sk#75,c_current_addr_sk#76,c_first_shipto_date_sk#77,c_first_sales_date_sk#78,c_salutation#79,c_first_name#80,c_last_name#81,c_preferred_cust_flag#82,c_birth_day#83,c_birth_month#84,c_birth_year#85,c_birth_country#86,c_login#87,c_email_address#88,c_last_review_date#89] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [c_customer_id#73 ASC NULLS FIRST, c_salutation#79 ASC NULLS FIRST, c_first_name#80 ASC NULLS FIRST, c_last_name#81 ASC NULLS FIRST, c_preferred_cust_flag#82 ASC NULLS FIRST, c_birth_day#83 ASC NULLS FIRST, c_birth_month#84 ASC NULLS FIRST, c_birth_year#85 ASC NULLS FIRST, c_birth_country#86 ASC NULLS FIRST, c_login#87 ASC NULLS FIRST, c_email_address#88 ASC NULLS FIRST, c_last_review_date#89 ASC NULLS FIRST, ctr_total_return#3 ASC NULLS FIRST], true
      +- Project [c_customer_id#73, c_salutation#79, c_first_name#80, c_last_name#81, c_preferred_cust_flag#82, c_birth_day#83, c_birth_month#84, c_birth_year#85, c_birth_country#86, c_login#87, c_email_address#88, c_last_review_date#89, ctr_total_return#3]
         +- Join Inner, (ca_address_sk#58 = c_current_addr_sk#76)
            :- Project [ctr_total_return#3, c_customer_id#73, c_current_addr_sk#76, c_salutation#79, c_first_name#80, c_last_name#81, c_preferred_cust_flag#82, c_birth_day#83, c_birth_month#84, c_birth_year#85, c_birth_country#86, c_login#87, c_email_address#88, c_last_review_date#89]
            :  +- Join Inner, (ctr_customer_sk#1 = c_customer_sk#72)
            :     :- Project [ctr_customer_sk#1, ctr_total_return#3]
            :     :  +- Join Inner, ((ctr_total_return#3 > (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#91) && (ctr_state#2 = ctr_state#2#92))
            :     :     :- Filter isnotnull(ctr_total_return#3)
            :     :     :  +- Aggregate [wr_returning_customer_sk#13, ca_state#66], [wr_returning_customer_sk#13 AS ctr_customer_sk#1, ca_state#66 AS ctr_state#2, sum(wr_return_amt#21) AS ctr_total_return#3]
            :     :     :     +- Project [wr_returning_customer_sk#13, wr_return_amt#21, ca_state#66]
            :     :     :        +- Join Inner, (wr_returning_addr_sk#16 = ca_address_sk#58)
            :     :     :           :- Project [wr_returning_customer_sk#13, wr_returning_addr_sk#16, wr_return_amt#21]
            :     :     :           :  +- Join Inner, (wr_returned_date_sk#6 = d_date_sk#30)
            :     :     :           :     :- Project [wr_returned_date_sk#6, wr_returning_customer_sk#13, wr_returning_addr_sk#16, wr_return_amt#21]
            :     :     :           :     :  +- Filter ((isnotnull(wr_returned_date_sk#6) && isnotnull(wr_returning_addr_sk#16)) && isnotnull(wr_returning_customer_sk#13))
            :     :     :           :     :     +- Relation[wr_returned_date_sk#6,wr_returned_time_sk#7,wr_item_sk#8,wr_refunded_customer_sk#9,wr_refunded_cdemo_sk#10,wr_refunded_hdemo_sk#11,wr_refunded_addr_sk#12,wr_returning_customer_sk#13,wr_returning_cdemo_sk#14,wr_returning_hdemo_sk#15,wr_returning_addr_sk#16,wr_web_page_sk#17,wr_reason_sk#18,wr_order_number#19,wr_return_quantity#20,wr_return_amt#21,wr_return_tax#22,wr_return_amt_inc_tax#23,wr_fee#24,wr_return_ship_cost#25,wr_refunded_cash#26,wr_reversed_charge#27,wr_account_credit#28,wr_net_loss#29] parquet
            :     :     :           :     +- Project [d_date_sk#30]
            :     :     :           :        +- Filter ((isnotnull(d_year#36) && (d_year#36 = 2002)) && isnotnull(d_date_sk#30))
            :     :     :           :           +- Relation[d_date_sk#30,d_date_id#31,d_date#32,d_month_seq#33,d_week_seq#34,d_quarter_seq#35,d_year#36,d_dow#37,d_moy#38,d_dom#39,d_qoy#40,d_fy_year#41,d_fy_quarter_seq#42,d_fy_week_seq#43,d_day_name#44,d_quarter_name#45,d_holiday#46,d_weekend#47,d_following_holiday#48,d_first_dom#49,d_last_dom#50,d_same_day_ly#51,d_same_day_lq#52,d_current_day#53,... 4 more fields] parquet
            :     :     :           +- Project [ca_address_sk#58, ca_state#66]
            :     :     :              +- Filter (isnotnull(ca_address_sk#58) && isnotnull(ca_state#66))
            :     :     :                 +- Relation[ca_address_sk#58,ca_address_id#59,ca_street_number#60,ca_street_name#61,ca_street_type#62,ca_suite_number#63,ca_city#64,ca_county#65,ca_state#66,ca_zip#67,ca_country#68,ca_gmt_offset#69,ca_location_type#70] parquet
            :     :     +- Filter isnotnull((avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#91)
            :     :        +- Aggregate [ctr_state#2], [(avg(ctr_total_return#3) * 1.2) AS (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#91, ctr_state#2 AS ctr_state#2#92]
            :     :           +- Aggregate [wr_returning_customer_sk#13, ca_state#66], [ca_state#66 AS ctr_state#2, sum(wr_return_amt#21) AS ctr_total_return#3]
            :     :              +- Project [wr_returning_customer_sk#13, wr_return_amt#21, ca_state#66]
            :     :                 +- Join Inner, (wr_returning_addr_sk#16 = ca_address_sk#58)
            :     :                    :- Project [wr_returning_customer_sk#13, wr_returning_addr_sk#16, wr_return_amt#21]
            :     :                    :  +- Join Inner, (wr_returned_date_sk#6 = d_date_sk#30)
            :     :                    :     :- Project [wr_returned_date_sk#6, wr_returning_customer_sk#13, wr_returning_addr_sk#16, wr_return_amt#21]
            :     :                    :     :  +- Filter (isnotnull(wr_returned_date_sk#6) && isnotnull(wr_returning_addr_sk#16))
            :     :                    :     :     +- Relation[wr_returned_date_sk#6,wr_returned_time_sk#7,wr_item_sk#8,wr_refunded_customer_sk#9,wr_refunded_cdemo_sk#10,wr_refunded_hdemo_sk#11,wr_refunded_addr_sk#12,wr_returning_customer_sk#13,wr_returning_cdemo_sk#14,wr_returning_hdemo_sk#15,wr_returning_addr_sk#16,wr_web_page_sk#17,wr_reason_sk#18,wr_order_number#19,wr_return_quantity#20,wr_return_amt#21,wr_return_tax#22,wr_return_amt_inc_tax#23,wr_fee#24,wr_return_ship_cost#25,wr_refunded_cash#26,wr_reversed_charge#27,wr_account_credit#28,wr_net_loss#29] parquet
            :     :                    :     +- Project [d_date_sk#30]
            :     :                    :        +- Filter ((isnotnull(d_year#36) && (d_year#36 = 2002)) && isnotnull(d_date_sk#30))
            :     :                    :           +- Relation[d_date_sk#30,d_date_id#31,d_date#32,d_month_seq#33,d_week_seq#34,d_quarter_seq#35,d_year#36,d_dow#37,d_moy#38,d_dom#39,d_qoy#40,d_fy_year#41,d_fy_quarter_seq#42,d_fy_week_seq#43,d_day_name#44,d_quarter_name#45,d_holiday#46,d_weekend#47,d_following_holiday#48,d_first_dom#49,d_last_dom#50,d_same_day_ly#51,d_same_day_lq#52,d_current_day#53,... 4 more fields] parquet
            :     :                    +- Project [ca_address_sk#58, ca_state#66]
            :     :                       +- Filter (isnotnull(ca_address_sk#58) && isnotnull(ca_state#66))
            :     :                          +- Relation[ca_address_sk#58,ca_address_id#59,ca_street_number#60,ca_street_name#61,ca_street_type#62,ca_suite_number#63,ca_city#64,ca_county#65,ca_state#66,ca_zip#67,ca_country#68,ca_gmt_offset#69,ca_location_type#70] parquet
            :     +- Project [c_customer_sk#72, c_customer_id#73, c_current_addr_sk#76, c_salutation#79, c_first_name#80, c_last_name#81, c_preferred_cust_flag#82, c_birth_day#83, c_birth_month#84, c_birth_year#85, c_birth_country#86, c_login#87, c_email_address#88, c_last_review_date#89]
            :        +- Filter (isnotnull(c_customer_sk#72) && isnotnull(c_current_addr_sk#76))
            :           +- Relation[c_customer_sk#72,c_customer_id#73,c_current_cdemo_sk#74,c_current_hdemo_sk#75,c_current_addr_sk#76,c_first_shipto_date_sk#77,c_first_sales_date_sk#78,c_salutation#79,c_first_name#80,c_last_name#81,c_preferred_cust_flag#82,c_birth_day#83,c_birth_month#84,c_birth_year#85,c_birth_country#86,c_login#87,c_email_address#88,c_last_review_date#89] parquet
            +- Project [ca_address_sk#58]
               +- Filter ((isnotnull(ca_state#66) && (ca_state#66 = GA)) && isnotnull(ca_address_sk#58))
                  +- Relation[ca_address_sk#58,ca_address_id#59,ca_street_number#60,ca_street_name#61,ca_street_type#62,ca_suite_number#63,ca_city#64,ca_county#65,ca_state#66,ca_zip#67,ca_country#68,ca_gmt_offset#69,ca_location_type#70] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[c_customer_id#73 ASC NULLS FIRST,c_salutation#79 ASC NULLS FIRST,c_first_name#80 ASC NULLS FIRST,c_last_name#81 ASC NULLS FIRST,c_preferred_cust_flag#82 ASC NULLS FIRST,c_birth_day#83 ASC NULLS FIRST,c_birth_month#84 ASC NULLS FIRST,c_birth_year#85 ASC NULLS FIRST,c_birth_country#86 ASC NULLS FIRST,c_login#87 ASC NULLS FIRST,c_email_address#88 ASC NULLS FIRST,c_last_review_date#89 ASC NULLS FIRST,ctr_total_return#3 ASC NULLS FIRST], output=[c_customer_id#73,c_salutation#79,c_first_name#80,c_last_name#81,c_preferred_cust_flag#82,c_birth_day#83,c_birth_month#84,c_birth_year#85,c_birth_country#86,c_login#87,c_email_address#88,c_last_review_date#89,ctr_total_return#3])
+- *(14) Project [c_customer_id#73, c_salutation#79, c_first_name#80, c_last_name#81, c_preferred_cust_flag#82, c_birth_day#83, c_birth_month#84, c_birth_year#85, c_birth_country#86, c_login#87, c_email_address#88, c_last_review_date#89, ctr_total_return#3]
   +- *(14) BroadcastHashJoin [c_current_addr_sk#76], [ca_address_sk#58], Inner, BuildRight
      :- *(14) Project [ctr_total_return#3, c_customer_id#73, c_current_addr_sk#76, c_salutation#79, c_first_name#80, c_last_name#81, c_preferred_cust_flag#82, c_birth_day#83, c_birth_month#84, c_birth_year#85, c_birth_country#86, c_login#87, c_email_address#88, c_last_review_date#89]
      :  +- *(14) BroadcastHashJoin [ctr_customer_sk#1], [c_customer_sk#72], Inner, BuildRight
      :     :- *(14) Project [ctr_customer_sk#1, ctr_total_return#3]
      :     :  +- *(14) SortMergeJoin [ctr_state#2], [ctr_state#2#92], Inner, (ctr_total_return#3 > (avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#91)
      :     :     :- *(5) Sort [ctr_state#2 ASC NULLS FIRST], false, 0
      :     :     :  +- Exchange hashpartitioning(ctr_state#2, 200)
      :     :     :     +- *(4) Filter isnotnull(ctr_total_return#3)
      :     :     :        +- *(4) HashAggregate(keys=[wr_returning_customer_sk#13, ca_state#66], functions=[sum(wr_return_amt#21)], output=[ctr_customer_sk#1, ctr_state#2, ctr_total_return#3])
      :     :     :           +- Exchange hashpartitioning(wr_returning_customer_sk#13, ca_state#66, 200)
      :     :     :              +- *(3) HashAggregate(keys=[wr_returning_customer_sk#13, ca_state#66], functions=[partial_sum(wr_return_amt#21)], output=[wr_returning_customer_sk#13, ca_state#66, sum#94])
      :     :     :                 +- *(3) Project [wr_returning_customer_sk#13, wr_return_amt#21, ca_state#66]
      :     :     :                    +- *(3) BroadcastHashJoin [wr_returning_addr_sk#16], [ca_address_sk#58], Inner, BuildRight
      :     :     :                       :- *(3) Project [wr_returning_customer_sk#13, wr_returning_addr_sk#16, wr_return_amt#21]
      :     :     :                       :  +- *(3) BroadcastHashJoin [wr_returned_date_sk#6], [d_date_sk#30], Inner, BuildRight
      :     :     :                       :     :- *(3) Project [wr_returned_date_sk#6, wr_returning_customer_sk#13, wr_returning_addr_sk#16, wr_return_amt#21]
      :     :     :                       :     :  +- *(3) Filter ((isnotnull(wr_returned_date_sk#6) && isnotnull(wr_returning_addr_sk#16)) && isnotnull(wr_returning_customer_sk#13))
      :     :     :                       :     :     +- *(3) FileScan parquet tpcds.web_returns[wr_returned_date_sk#6,wr_returning_customer_sk#13,wr_returning_addr_sk#16,wr_return_amt#21] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_returned_date_sk), IsNotNull(wr_returning_addr_sk), IsNotNull(wr_returning_customer..., ReadSchema: struct<wr_returned_date_sk:int,wr_returning_customer_sk:int,wr_returning_addr_sk:int,wr_return_am...
      :     :     :                       :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :     :                       :        +- *(1) Project [d_date_sk#30]
      :     :     :                       :           +- *(1) Filter ((isnotnull(d_year#36) && (d_year#36 = 2002)) && isnotnull(d_date_sk#30))
      :     :     :                       :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#30,d_year#36] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
      :     :     :                       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :     :                          +- *(2) Project [ca_address_sk#58, ca_state#66]
      :     :     :                             +- *(2) Filter (isnotnull(ca_address_sk#58) && isnotnull(ca_state#66))
      :     :     :                                +- *(2) FileScan parquet tpcds.customer_address[ca_address_sk#58,ca_state#66] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_state)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
      :     :     +- *(11) Sort [ctr_state#2#92 ASC NULLS FIRST], false, 0
      :     :        +- Exchange hashpartitioning(ctr_state#2#92, 200)
      :     :           +- *(10) Filter isnotnull((avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#91)
      :     :              +- *(10) HashAggregate(keys=[ctr_state#2], functions=[avg(ctr_total_return#3)], output=[(avg(ctr_total_return) * CAST(1.2 AS DOUBLE))#91, ctr_state#2#92])
      :     :                 +- Exchange hashpartitioning(ctr_state#2, 200)
      :     :                    +- *(9) HashAggregate(keys=[ctr_state#2], functions=[partial_avg(ctr_total_return#3)], output=[ctr_state#2, sum#97, count#98L])
      :     :                       +- *(9) HashAggregate(keys=[wr_returning_customer_sk#13, ca_state#66], functions=[sum(wr_return_amt#21)], output=[ctr_state#2, ctr_total_return#3])
      :     :                          +- Exchange hashpartitioning(wr_returning_customer_sk#13, ca_state#66, 200)
      :     :                             +- *(8) HashAggregate(keys=[wr_returning_customer_sk#13, ca_state#66], functions=[partial_sum(wr_return_amt#21)], output=[wr_returning_customer_sk#13, ca_state#66, sum#94])
      :     :                                +- *(8) Project [wr_returning_customer_sk#13, wr_return_amt#21, ca_state#66]
      :     :                                   +- *(8) BroadcastHashJoin [wr_returning_addr_sk#16], [ca_address_sk#58], Inner, BuildRight
      :     :                                      :- *(8) Project [wr_returning_customer_sk#13, wr_returning_addr_sk#16, wr_return_amt#21]
      :     :                                      :  +- *(8) BroadcastHashJoin [wr_returned_date_sk#6], [d_date_sk#30], Inner, BuildRight
      :     :                                      :     :- *(8) Project [wr_returned_date_sk#6, wr_returning_customer_sk#13, wr_returning_addr_sk#16, wr_return_amt#21]
      :     :                                      :     :  +- *(8) Filter (isnotnull(wr_returned_date_sk#6) && isnotnull(wr_returning_addr_sk#16))
      :     :                                      :     :     +- *(8) FileScan parquet tpcds.web_returns[wr_returned_date_sk#6,wr_returning_customer_sk#13,wr_returning_addr_sk#16,wr_return_amt#21] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_returned_date_sk), IsNotNull(wr_returning_addr_sk)], ReadSchema: struct<wr_returned_date_sk:int,wr_returning_customer_sk:int,wr_returning_addr_sk:int,wr_return_am...
      :     :                                      :     +- ReusedExchange [d_date_sk#30], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     :                                      +- ReusedExchange [ca_address_sk#58, ca_state#66], BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
      :        +- *(12) Project [c_customer_sk#72, c_customer_id#73, c_current_addr_sk#76, c_salutation#79, c_first_name#80, c_last_name#81, c_preferred_cust_flag#82, c_birth_day#83, c_birth_month#84, c_birth_year#85, c_birth_country#86, c_login#87, c_email_address#88, c_last_review_date#89]
      :           +- *(12) Filter (isnotnull(c_customer_sk#72) && isnotnull(c_current_addr_sk#76))
      :              +- *(12) FileScan parquet tpcds.customer[c_customer_sk#72,c_customer_id#73,c_current_addr_sk#76,c_salutation#79,c_first_name#80,c_last_name#81,c_preferred_cust_flag#82,c_birth_day#83,c_birth_month#84,c_birth_year#85,c_birth_country#86,c_login#87,c_email_address#88,c_last_review_date#89] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_customer_id:string,c_current_addr_sk:int,c_salutation:string,c_first_n...
      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         +- *(13) Project [ca_address_sk#58]
            +- *(13) Filter ((isnotnull(ca_state#66) && (ca_state#66 = GA)) && isnotnull(ca_address_sk#58))
               +- *(13) FileScan parquet tpcds.customer_address[ca_address_sk#58,ca_state#66] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_state), EqualTo(ca_state,GA), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string>
Time taken: 4.637 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 30 in stream 0 using template query30.tpl
------------------------------------------------------^^^

