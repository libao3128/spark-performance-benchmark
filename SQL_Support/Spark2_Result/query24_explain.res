== Parsed Logical Plan ==
CTE [ssales]
:  +- 'SubqueryAlias `ssales`
:     +- 'Aggregate ['c_last_name, 'c_first_name, 's_store_name, 'ca_state, 's_state, 'i_color, 'i_current_price, 'i_manager_id, 'i_units, 'i_size], ['c_last_name, 'c_first_name, 's_store_name, 'ca_state, 's_state, 'i_color, 'i_current_price, 'i_manager_id, 'i_units, 'i_size, 'sum('ss_net_paid) AS netpaid#2]
:        +- 'Filter ((((('ss_ticket_number = 'sr_ticket_number) && ('ss_item_sk = 'sr_item_sk)) && ('ss_customer_sk = 'c_customer_sk)) && (('ss_item_sk = 'i_item_sk) && ('ss_store_sk = 's_store_sk))) && ((('c_current_addr_sk = 'ca_address_sk) && NOT ('c_birth_country = 'upper('ca_country))) && (('s_zip = 'ca_zip) && ('s_market_id = 8))))
:           +- 'Join Inner
:              :- 'Join Inner
:              :  :- 'Join Inner
:              :  :  :- 'Join Inner
:              :  :  :  :- 'Join Inner
:              :  :  :  :  :- 'UnresolvedRelation `store_sales`
:              :  :  :  :  +- 'UnresolvedRelation `store_returns`
:              :  :  :  +- 'UnresolvedRelation `store`
:              :  :  +- 'UnresolvedRelation `item`
:              :  +- 'UnresolvedRelation `customer`
:              +- 'UnresolvedRelation `customer_address`
+- 'Sort ['c_last_name ASC NULLS FIRST, 'c_first_name ASC NULLS FIRST, 's_store_name ASC NULLS FIRST], true
   +- 'UnresolvedHaving ('sum('netpaid) > scalar-subquery#1 [])
      :  +- 'Project [unresolvedalias((0.05 * 'avg('netpaid)), None)]
      :     +- 'UnresolvedRelation `ssales`
      +- 'Aggregate ['c_last_name, 'c_first_name, 's_store_name], ['c_last_name, 'c_first_name, 's_store_name, 'sum('netpaid) AS paid#0]
         +- 'Filter ('i_color = pale)
            +- 'UnresolvedRelation `ssales`

== Analyzed Logical Plan ==
c_last_name: string, c_first_name: string, s_store_name: string, paid: double
Sort [c_last_name#108 ASC NULLS FIRST, c_first_name#107 ASC NULLS FIRST, s_store_name#53 ASC NULLS FIRST], true
+- Project [c_last_name#108, c_first_name#107, s_store_name#53, paid#0]
   +- Filter (sum(netpaid#2)#136 > scalar-subquery#1 [])
      :  +- Aggregate [(cast(0.05 as double) * avg(netpaid#2)) AS (CAST(0.05 AS DOUBLE) * avg(netpaid))#133]
      :     +- SubqueryAlias `ssales`
      :        +- Aggregate [c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92], [c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92, sum(ss_net_paid#25) AS netpaid#2]
      :           +- Filter (((((ss_ticket_number#14 = sr_ticket_number#37) && (ss_item_sk#7 = sr_item_sk#30)) && (ss_customer_sk#8 = c_customer_sk#99)) && ((ss_item_sk#7 = i_item_sk#77) && (ss_store_sk#12 = s_store_sk#48))) && (((c_current_addr_sk#103 = ca_address_sk#117) && NOT (c_birth_country#113 = upper(ca_country#127))) && ((s_zip#73 = ca_zip#126) && (s_market_id#58 = 8))))
      :              +- Join Inner
      :                 :- Join Inner
      :                 :  :- Join Inner
      :                 :  :  :- Join Inner
      :                 :  :  :  :- Join Inner
      :                 :  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
      :                 :  :  :  :  :  +- Relation[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
      :                 :  :  :  :  +- SubqueryAlias `tpcds`.`store_returns`
      :                 :  :  :  :     +- Relation[sr_returned_date_sk#28,sr_return_time_sk#29,sr_item_sk#30,sr_customer_sk#31,sr_cdemo_sk#32,sr_hdemo_sk#33,sr_addr_sk#34,sr_store_sk#35,sr_reason_sk#36,sr_ticket_number#37,sr_return_quantity#38,sr_return_amt#39,sr_return_tax#40,sr_return_amt_inc_tax#41,sr_fee#42,sr_return_ship_cost#43,sr_refunded_cash#44,sr_reversed_charge#45,sr_store_credit#46,sr_net_loss#47] parquet
      :                 :  :  :  +- SubqueryAlias `tpcds`.`store`
      :                 :  :  :     +- Relation[s_store_sk#48,s_store_id#49,s_rec_start_date#50,s_rec_end_date#51,s_closed_date_sk#52,s_store_name#53,s_number_employees#54,s_floor_space#55,s_hours#56,s_manager#57,s_market_id#58,s_geography_class#59,s_market_desc#60,s_market_manager#61,s_division_id#62,s_division_name#63,s_company_id#64,s_company_name#65,s_street_number#66,s_street_name#67,s_street_type#68,s_suite_number#69,s_city#70,s_county#71,... 5 more fields] parquet
      :                 :  :  +- SubqueryAlias `tpcds`.`item`
      :                 :  :     +- Relation[i_item_sk#77,i_item_id#78,i_rec_start_date#79,i_rec_end_date#80,i_item_desc#81,i_current_price#82,i_wholesale_cost#83,i_brand_id#84,i_brand#85,i_class_id#86,i_class#87,i_category_id#88,i_category#89,i_manufact_id#90,i_manufact#91,i_size#92,i_formulation#93,i_color#94,i_units#95,i_container#96,i_manager_id#97,i_product_name#98] parquet
      :                 :  +- SubqueryAlias `tpcds`.`customer`
      :                 :     +- Relation[c_customer_sk#99,c_customer_id#100,c_current_cdemo_sk#101,c_current_hdemo_sk#102,c_current_addr_sk#103,c_first_shipto_date_sk#104,c_first_sales_date_sk#105,c_salutation#106,c_first_name#107,c_last_name#108,c_preferred_cust_flag#109,c_birth_day#110,c_birth_month#111,c_birth_year#112,c_birth_country#113,c_login#114,c_email_address#115,c_last_review_date#116] parquet
      :                 +- SubqueryAlias `tpcds`.`customer_address`
      :                    +- Relation[ca_address_sk#117,ca_address_id#118,ca_street_number#119,ca_street_name#120,ca_street_type#121,ca_suite_number#122,ca_city#123,ca_county#124,ca_state#125,ca_zip#126,ca_country#127,ca_gmt_offset#128,ca_location_type#129] parquet
      +- Aggregate [c_last_name#108, c_first_name#107, s_store_name#53], [c_last_name#108, c_first_name#107, s_store_name#53, sum(netpaid#2) AS paid#0, sum(netpaid#2) AS sum(netpaid#2)#136]
         +- Filter (i_color#94 = pale)
            +- SubqueryAlias `ssales`
               +- Aggregate [c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92], [c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92, sum(ss_net_paid#25) AS netpaid#2]
                  +- Filter (((((ss_ticket_number#14 = sr_ticket_number#37) && (ss_item_sk#7 = sr_item_sk#30)) && (ss_customer_sk#8 = c_customer_sk#99)) && ((ss_item_sk#7 = i_item_sk#77) && (ss_store_sk#12 = s_store_sk#48))) && (((c_current_addr_sk#103 = ca_address_sk#117) && NOT (c_birth_country#113 = upper(ca_country#127))) && ((s_zip#73 = ca_zip#126) && (s_market_id#58 = 8))))
                     +- Join Inner
                        :- Join Inner
                        :  :- Join Inner
                        :  :  :- Join Inner
                        :  :  :  :- Join Inner
                        :  :  :  :  :- SubqueryAlias `tpcds`.`store_sales`
                        :  :  :  :  :  +- Relation[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
                        :  :  :  :  +- SubqueryAlias `tpcds`.`store_returns`
                        :  :  :  :     +- Relation[sr_returned_date_sk#28,sr_return_time_sk#29,sr_item_sk#30,sr_customer_sk#31,sr_cdemo_sk#32,sr_hdemo_sk#33,sr_addr_sk#34,sr_store_sk#35,sr_reason_sk#36,sr_ticket_number#37,sr_return_quantity#38,sr_return_amt#39,sr_return_tax#40,sr_return_amt_inc_tax#41,sr_fee#42,sr_return_ship_cost#43,sr_refunded_cash#44,sr_reversed_charge#45,sr_store_credit#46,sr_net_loss#47] parquet
                        :  :  :  +- SubqueryAlias `tpcds`.`store`
                        :  :  :     +- Relation[s_store_sk#48,s_store_id#49,s_rec_start_date#50,s_rec_end_date#51,s_closed_date_sk#52,s_store_name#53,s_number_employees#54,s_floor_space#55,s_hours#56,s_manager#57,s_market_id#58,s_geography_class#59,s_market_desc#60,s_market_manager#61,s_division_id#62,s_division_name#63,s_company_id#64,s_company_name#65,s_street_number#66,s_street_name#67,s_street_type#68,s_suite_number#69,s_city#70,s_county#71,... 5 more fields] parquet
                        :  :  +- SubqueryAlias `tpcds`.`item`
                        :  :     +- Relation[i_item_sk#77,i_item_id#78,i_rec_start_date#79,i_rec_end_date#80,i_item_desc#81,i_current_price#82,i_wholesale_cost#83,i_brand_id#84,i_brand#85,i_class_id#86,i_class#87,i_category_id#88,i_category#89,i_manufact_id#90,i_manufact#91,i_size#92,i_formulation#93,i_color#94,i_units#95,i_container#96,i_manager_id#97,i_product_name#98] parquet
                        :  +- SubqueryAlias `tpcds`.`customer`
                        :     +- Relation[c_customer_sk#99,c_customer_id#100,c_current_cdemo_sk#101,c_current_hdemo_sk#102,c_current_addr_sk#103,c_first_shipto_date_sk#104,c_first_sales_date_sk#105,c_salutation#106,c_first_name#107,c_last_name#108,c_preferred_cust_flag#109,c_birth_day#110,c_birth_month#111,c_birth_year#112,c_birth_country#113,c_login#114,c_email_address#115,c_last_review_date#116] parquet
                        +- SubqueryAlias `tpcds`.`customer_address`
                           +- Relation[ca_address_sk#117,ca_address_id#118,ca_street_number#119,ca_street_name#120,ca_street_type#121,ca_suite_number#122,ca_city#123,ca_county#124,ca_state#125,ca_zip#126,ca_country#127,ca_gmt_offset#128,ca_location_type#129] parquet

== Optimized Logical Plan ==
Sort [c_last_name#108 ASC NULLS FIRST, c_first_name#107 ASC NULLS FIRST, s_store_name#53 ASC NULLS FIRST], true
+- Project [c_last_name#108, c_first_name#107, s_store_name#53, paid#0]
   +- Filter (isnotnull(sum(netpaid#2)#136) && (sum(netpaid#2)#136 > scalar-subquery#1 []))
      :  +- Aggregate [(0.05 * avg(netpaid#2)) AS (CAST(0.05 AS DOUBLE) * avg(netpaid))#133]
      :     +- Aggregate [c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92], [sum(ss_net_paid#25) AS netpaid#2]
      :        +- Project [ss_net_paid#25, s_store_name#53, s_state#72, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97, c_first_name#107, c_last_name#108, ca_state#125]
      :           +- Join Inner, (((c_current_addr_sk#103 = ca_address_sk#117) && NOT (c_birth_country#113 = upper(ca_country#127))) && (s_zip#73 = ca_zip#126))
      :              :- Project [ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97, c_current_addr_sk#103, c_first_name#107, c_last_name#108, c_birth_country#113]
      :              :  +- Join Inner, (ss_customer_sk#8 = c_customer_sk#99)
      :              :     :- Project [ss_customer_sk#8, ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97]
      :              :     :  +- Join Inner, (ss_item_sk#7 = i_item_sk#77)
      :              :     :     :- Project [ss_item_sk#7, ss_customer_sk#8, ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73]
      :              :     :     :  +- Join Inner, (ss_store_sk#12 = s_store_sk#48)
      :              :     :     :     :- Project [ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_net_paid#25]
      :              :     :     :     :  +- Join Inner, ((ss_ticket_number#14 = sr_ticket_number#37) && (ss_item_sk#7 = sr_item_sk#30))
      :              :     :     :     :     :- Project [ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_ticket_number#14, ss_net_paid#25]
      :              :     :     :     :     :  +- Filter (((isnotnull(ss_ticket_number#14) && isnotnull(ss_item_sk#7)) && isnotnull(ss_store_sk#12)) && isnotnull(ss_customer_sk#8))
      :              :     :     :     :     :     +- Relation[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
      :              :     :     :     :     +- Project [sr_item_sk#30, sr_ticket_number#37]
      :              :     :     :     :        +- Filter (isnotnull(sr_ticket_number#37) && isnotnull(sr_item_sk#30))
      :              :     :     :     :           +- Relation[sr_returned_date_sk#28,sr_return_time_sk#29,sr_item_sk#30,sr_customer_sk#31,sr_cdemo_sk#32,sr_hdemo_sk#33,sr_addr_sk#34,sr_store_sk#35,sr_reason_sk#36,sr_ticket_number#37,sr_return_quantity#38,sr_return_amt#39,sr_return_tax#40,sr_return_amt_inc_tax#41,sr_fee#42,sr_return_ship_cost#43,sr_refunded_cash#44,sr_reversed_charge#45,sr_store_credit#46,sr_net_loss#47] parquet
      :              :     :     :     +- Project [s_store_sk#48, s_store_name#53, s_state#72, s_zip#73]
      :              :     :     :        +- Filter (((isnotnull(s_market_id#58) && (s_market_id#58 = 8)) && isnotnull(s_store_sk#48)) && isnotnull(s_zip#73))
      :              :     :     :           +- Relation[s_store_sk#48,s_store_id#49,s_rec_start_date#50,s_rec_end_date#51,s_closed_date_sk#52,s_store_name#53,s_number_employees#54,s_floor_space#55,s_hours#56,s_manager#57,s_market_id#58,s_geography_class#59,s_market_desc#60,s_market_manager#61,s_division_id#62,s_division_name#63,s_company_id#64,s_company_name#65,s_street_number#66,s_street_name#67,s_street_type#68,s_suite_number#69,s_city#70,s_county#71,... 5 more fields] parquet
      :              :     :     +- Project [i_item_sk#77, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97]
      :              :     :        +- Filter isnotnull(i_item_sk#77)
      :              :     :           +- Relation[i_item_sk#77,i_item_id#78,i_rec_start_date#79,i_rec_end_date#80,i_item_desc#81,i_current_price#82,i_wholesale_cost#83,i_brand_id#84,i_brand#85,i_class_id#86,i_class#87,i_category_id#88,i_category#89,i_manufact_id#90,i_manufact#91,i_size#92,i_formulation#93,i_color#94,i_units#95,i_container#96,i_manager_id#97,i_product_name#98] parquet
      :              :     +- Project [c_customer_sk#99, c_current_addr_sk#103, c_first_name#107, c_last_name#108, c_birth_country#113]
      :              :        +- Filter ((isnotnull(c_customer_sk#99) && isnotnull(c_birth_country#113)) && isnotnull(c_current_addr_sk#103))
      :              :           +- Relation[c_customer_sk#99,c_customer_id#100,c_current_cdemo_sk#101,c_current_hdemo_sk#102,c_current_addr_sk#103,c_first_shipto_date_sk#104,c_first_sales_date_sk#105,c_salutation#106,c_first_name#107,c_last_name#108,c_preferred_cust_flag#109,c_birth_day#110,c_birth_month#111,c_birth_year#112,c_birth_country#113,c_login#114,c_email_address#115,c_last_review_date#116] parquet
      :              +- Project [ca_address_sk#117, ca_state#125, ca_zip#126, ca_country#127]
      :                 +- Filter (isnotnull(ca_address_sk#117) && isnotnull(ca_zip#126))
      :                    +- Relation[ca_address_sk#117,ca_address_id#118,ca_street_number#119,ca_street_name#120,ca_street_type#121,ca_suite_number#122,ca_city#123,ca_county#124,ca_state#125,ca_zip#126,ca_country#127,ca_gmt_offset#128,ca_location_type#129] parquet
      +- Aggregate [c_last_name#108, c_first_name#107, s_store_name#53], [c_last_name#108, c_first_name#107, s_store_name#53, sum(netpaid#2) AS paid#0, sum(netpaid#2) AS sum(netpaid#2)#136]
         +- Aggregate [c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92], [c_last_name#108, c_first_name#107, s_store_name#53, sum(ss_net_paid#25) AS netpaid#2]
            +- Project [ss_net_paid#25, s_store_name#53, s_state#72, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97, c_first_name#107, c_last_name#108, ca_state#125]
               +- Join Inner, (((c_current_addr_sk#103 = ca_address_sk#117) && NOT (c_birth_country#113 = upper(ca_country#127))) && (s_zip#73 = ca_zip#126))
                  :- Project [ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97, c_current_addr_sk#103, c_first_name#107, c_last_name#108, c_birth_country#113]
                  :  +- Join Inner, (ss_customer_sk#8 = c_customer_sk#99)
                  :     :- Project [ss_customer_sk#8, ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97]
                  :     :  +- Join Inner, (ss_item_sk#7 = i_item_sk#77)
                  :     :     :- Project [ss_item_sk#7, ss_customer_sk#8, ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73]
                  :     :     :  +- Join Inner, (ss_store_sk#12 = s_store_sk#48)
                  :     :     :     :- Project [ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_net_paid#25]
                  :     :     :     :  +- Join Inner, ((ss_ticket_number#14 = sr_ticket_number#37) && (ss_item_sk#7 = sr_item_sk#30))
                  :     :     :     :     :- Project [ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_ticket_number#14, ss_net_paid#25]
                  :     :     :     :     :  +- Filter (((isnotnull(ss_ticket_number#14) && isnotnull(ss_item_sk#7)) && isnotnull(ss_store_sk#12)) && isnotnull(ss_customer_sk#8))
                  :     :     :     :     :     +- Relation[ss_sold_date_sk#5,ss_sold_time_sk#6,ss_item_sk#7,ss_customer_sk#8,ss_cdemo_sk#9,ss_hdemo_sk#10,ss_addr_sk#11,ss_store_sk#12,ss_promo_sk#13,ss_ticket_number#14,ss_quantity#15,ss_wholesale_cost#16,ss_list_price#17,ss_sales_price#18,ss_ext_discount_amt#19,ss_ext_sales_price#20,ss_ext_wholesale_cost#21,ss_ext_list_price#22,ss_ext_tax#23,ss_coupon_amt#24,ss_net_paid#25,ss_net_paid_inc_tax#26,ss_net_profit#27] parquet
                  :     :     :     :     +- Project [sr_item_sk#30, sr_ticket_number#37]
                  :     :     :     :        +- Filter (isnotnull(sr_ticket_number#37) && isnotnull(sr_item_sk#30))
                  :     :     :     :           +- Relation[sr_returned_date_sk#28,sr_return_time_sk#29,sr_item_sk#30,sr_customer_sk#31,sr_cdemo_sk#32,sr_hdemo_sk#33,sr_addr_sk#34,sr_store_sk#35,sr_reason_sk#36,sr_ticket_number#37,sr_return_quantity#38,sr_return_amt#39,sr_return_tax#40,sr_return_amt_inc_tax#41,sr_fee#42,sr_return_ship_cost#43,sr_refunded_cash#44,sr_reversed_charge#45,sr_store_credit#46,sr_net_loss#47] parquet
                  :     :     :     +- Project [s_store_sk#48, s_store_name#53, s_state#72, s_zip#73]
                  :     :     :        +- Filter (((isnotnull(s_market_id#58) && (s_market_id#58 = 8)) && isnotnull(s_store_sk#48)) && isnotnull(s_zip#73))
                  :     :     :           +- Relation[s_store_sk#48,s_store_id#49,s_rec_start_date#50,s_rec_end_date#51,s_closed_date_sk#52,s_store_name#53,s_number_employees#54,s_floor_space#55,s_hours#56,s_manager#57,s_market_id#58,s_geography_class#59,s_market_desc#60,s_market_manager#61,s_division_id#62,s_division_name#63,s_company_id#64,s_company_name#65,s_street_number#66,s_street_name#67,s_street_type#68,s_suite_number#69,s_city#70,s_county#71,... 5 more fields] parquet
                  :     :     +- Project [i_item_sk#77, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97]
                  :     :        +- Filter ((isnotnull(i_color#94) && (i_color#94 = pale)) && isnotnull(i_item_sk#77))
                  :     :           +- Relation[i_item_sk#77,i_item_id#78,i_rec_start_date#79,i_rec_end_date#80,i_item_desc#81,i_current_price#82,i_wholesale_cost#83,i_brand_id#84,i_brand#85,i_class_id#86,i_class#87,i_category_id#88,i_category#89,i_manufact_id#90,i_manufact#91,i_size#92,i_formulation#93,i_color#94,i_units#95,i_container#96,i_manager_id#97,i_product_name#98] parquet
                  :     +- Project [c_customer_sk#99, c_current_addr_sk#103, c_first_name#107, c_last_name#108, c_birth_country#113]
                  :        +- Filter ((isnotnull(c_customer_sk#99) && isnotnull(c_birth_country#113)) && isnotnull(c_current_addr_sk#103))
                  :           +- Relation[c_customer_sk#99,c_customer_id#100,c_current_cdemo_sk#101,c_current_hdemo_sk#102,c_current_addr_sk#103,c_first_shipto_date_sk#104,c_first_sales_date_sk#105,c_salutation#106,c_first_name#107,c_last_name#108,c_preferred_cust_flag#109,c_birth_day#110,c_birth_month#111,c_birth_year#112,c_birth_country#113,c_login#114,c_email_address#115,c_last_review_date#116] parquet
                  +- Project [ca_address_sk#117, ca_state#125, ca_zip#126, ca_country#127]
                     +- Filter (isnotnull(ca_address_sk#117) && isnotnull(ca_zip#126))
                        +- Relation[ca_address_sk#117,ca_address_id#118,ca_street_number#119,ca_street_name#120,ca_street_type#121,ca_suite_number#122,ca_city#123,ca_county#124,ca_state#125,ca_zip#126,ca_country#127,ca_gmt_offset#128,ca_location_type#129] parquet

== Physical Plan ==
*(9) Sort [c_last_name#108 ASC NULLS FIRST, c_first_name#107 ASC NULLS FIRST, s_store_name#53 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(c_last_name#108 ASC NULLS FIRST, c_first_name#107 ASC NULLS FIRST, s_store_name#53 ASC NULLS FIRST, 200)
   +- *(8) Project [c_last_name#108, c_first_name#107, s_store_name#53, paid#0]
      +- *(8) Filter (isnotnull(sum(netpaid#2)#136) && (sum(netpaid#2)#136 > Subquery subquery1))
         :  +- Subquery subquery1
         :     +- *(8) HashAggregate(keys=[], functions=[avg(netpaid#2)], output=[(CAST(0.05 AS DOUBLE) * avg(netpaid))#133])
         :        +- Exchange SinglePartition
         :           +- *(7) HashAggregate(keys=[], functions=[partial_avg(netpaid#2)], output=[sum#145, count#146L])
         :              +- *(7) HashAggregate(keys=[c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92], functions=[sum(ss_net_paid#25)], output=[netpaid#2])
         :                 +- Exchange hashpartitioning(c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92, 200)
         :                    +- *(6) HashAggregate(keys=[c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92], functions=[partial_sum(ss_net_paid#25)], output=[c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92, sum#142])
         :                       +- *(6) Project [ss_net_paid#25, s_store_name#53, s_state#72, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97, c_first_name#107, c_last_name#108, ca_state#125]
         :                          +- *(6) BroadcastHashJoin [c_current_addr_sk#103, s_zip#73], [ca_address_sk#117, ca_zip#126], Inner, BuildRight, NOT (c_birth_country#113 = upper(ca_country#127))
         :                             :- *(6) Project [ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97, c_current_addr_sk#103, c_first_name#107, c_last_name#108, c_birth_country#113]
         :                             :  +- *(6) BroadcastHashJoin [ss_customer_sk#8], [c_customer_sk#99], Inner, BuildRight
         :                             :     :- *(6) Project [ss_customer_sk#8, ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97]
         :                             :     :  +- *(6) BroadcastHashJoin [ss_item_sk#7], [i_item_sk#77], Inner, BuildRight
         :                             :     :     :- *(6) Project [ss_item_sk#7, ss_customer_sk#8, ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73]
         :                             :     :     :  +- *(6) BroadcastHashJoin [ss_store_sk#12], [s_store_sk#48], Inner, BuildRight
         :                             :     :     :     :- *(6) Project [ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_net_paid#25]
         :                             :     :     :     :  +- *(6) BroadcastHashJoin [ss_ticket_number#14, ss_item_sk#7], [sr_ticket_number#37, sr_item_sk#30], Inner, BuildRight
         :                             :     :     :     :     :- *(6) Project [ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_ticket_number#14, ss_net_paid#25]
         :                             :     :     :     :     :  +- *(6) Filter (((isnotnull(ss_ticket_number#14) && isnotnull(ss_item_sk#7)) && isnotnull(ss_store_sk#12)) && isnotnull(ss_customer_sk#8))
         :                             :     :     :     :     :     +- *(6) FileScan parquet tpcds.store_sales[ss_item_sk#7,ss_customer_sk#8,ss_store_sk#12,ss_ticket_number#14,ss_net_paid#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_ticket_number), IsNotNull(ss_item_sk), IsNotNull(ss_store_sk), IsNotNull(ss_custome..., ReadSchema: struct<ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ticket_number:int,ss_net_paid:double>
         :                             :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
         :                             :     :     :     :        +- *(1) Project [sr_item_sk#30, sr_ticket_number#37]
         :                             :     :     :     :           +- *(1) Filter (isnotnull(sr_ticket_number#37) && isnotnull(sr_item_sk#30))
         :                             :     :     :     :              +- *(1) FileScan parquet tpcds.store_returns[sr_item_sk#30,sr_ticket_number#37] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_ticket_number), IsNotNull(sr_item_sk)], ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int>
         :                             :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                             :     :     :        +- *(2) Project [s_store_sk#48, s_store_name#53, s_state#72, s_zip#73]
         :                             :     :     :           +- *(2) Filter (((isnotnull(s_market_id#58) && (s_market_id#58 = 8)) && isnotnull(s_store_sk#48)) && isnotnull(s_zip#73))
         :                             :     :     :              +- *(2) FileScan parquet tpcds.store[s_store_sk#48,s_store_name#53,s_market_id#58,s_state#72,s_zip#73] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_market_id), EqualTo(s_market_id,8), IsNotNull(s_store_sk), IsNotNull(s_zip)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_market_id:int,s_state:string,s_zip:string>
         :                             :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                             :     :        +- *(3) Project [i_item_sk#77, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97]
         :                             :     :           +- *(3) Filter isnotnull(i_item_sk#77)
         :                             :     :              +- *(3) FileScan parquet tpcds.item[i_item_sk#77,i_current_price#82,i_size#92,i_color#94,i_units#95,i_manager_id#97] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double,i_size:string,i_color:string,i_units:string,i_manager...
         :                             :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
         :                             :        +- *(4) Project [c_customer_sk#99, c_current_addr_sk#103, c_first_name#107, c_last_name#108, c_birth_country#113]
         :                             :           +- *(4) Filter ((isnotnull(c_customer_sk#99) && isnotnull(c_birth_country#113)) && isnotnull(c_current_addr_sk#103))
         :                             :              +- *(4) FileScan parquet tpcds.customer[c_customer_sk#99,c_current_addr_sk#103,c_first_name#107,c_last_name#108,c_birth_country#113] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_birth_country), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int,c_first_name:string,c_last_name:string,c_birth_cou...
         :                             +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, int, true], input[2, string, true]))
         :                                +- *(5) Project [ca_address_sk#117, ca_state#125, ca_zip#126, ca_country#127]
         :                                   +- *(5) Filter (isnotnull(ca_address_sk#117) && isnotnull(ca_zip#126))
         :                                      +- *(5) FileScan parquet tpcds.customer_address[ca_address_sk#117,ca_state#125,ca_zip#126,ca_country#127] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_zip)], ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_zip:string,ca_country:string>
         +- *(8) HashAggregate(keys=[c_last_name#108, c_first_name#107, s_store_name#53], functions=[sum(netpaid#2)], output=[c_last_name#108, c_first_name#107, s_store_name#53, paid#0, sum(netpaid#2)#136])
            +- Exchange hashpartitioning(c_last_name#108, c_first_name#107, s_store_name#53, 200)
               +- *(7) HashAggregate(keys=[c_last_name#108, c_first_name#107, s_store_name#53], functions=[partial_sum(netpaid#2)], output=[c_last_name#108, c_first_name#107, s_store_name#53, sum#140])
                  +- *(7) HashAggregate(keys=[c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92], functions=[sum(ss_net_paid#25)], output=[c_last_name#108, c_first_name#107, s_store_name#53, netpaid#2])
                     +- Exchange hashpartitioning(c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92, 200)
                        +- *(6) HashAggregate(keys=[c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92], functions=[partial_sum(ss_net_paid#25)], output=[c_last_name#108, c_first_name#107, s_store_name#53, ca_state#125, s_state#72, i_color#94, i_current_price#82, i_manager_id#97, i_units#95, i_size#92, sum#142])
                           +- *(6) Project [ss_net_paid#25, s_store_name#53, s_state#72, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97, c_first_name#107, c_last_name#108, ca_state#125]
                              +- *(6) BroadcastHashJoin [c_current_addr_sk#103, s_zip#73], [ca_address_sk#117, ca_zip#126], Inner, BuildRight, NOT (c_birth_country#113 = upper(ca_country#127))
                                 :- *(6) Project [ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97, c_current_addr_sk#103, c_first_name#107, c_last_name#108, c_birth_country#113]
                                 :  +- *(6) BroadcastHashJoin [ss_customer_sk#8], [c_customer_sk#99], Inner, BuildRight
                                 :     :- *(6) Project [ss_customer_sk#8, ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97]
                                 :     :  +- *(6) BroadcastHashJoin [ss_item_sk#7], [i_item_sk#77], Inner, BuildRight
                                 :     :     :- *(6) Project [ss_item_sk#7, ss_customer_sk#8, ss_net_paid#25, s_store_name#53, s_state#72, s_zip#73]
                                 :     :     :  +- *(6) BroadcastHashJoin [ss_store_sk#12], [s_store_sk#48], Inner, BuildRight
                                 :     :     :     :- *(6) Project [ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_net_paid#25]
                                 :     :     :     :  +- *(6) BroadcastHashJoin [ss_ticket_number#14, ss_item_sk#7], [sr_ticket_number#37, sr_item_sk#30], Inner, BuildRight
                                 :     :     :     :     :- *(6) Project [ss_item_sk#7, ss_customer_sk#8, ss_store_sk#12, ss_ticket_number#14, ss_net_paid#25]
                                 :     :     :     :     :  +- *(6) Filter (((isnotnull(ss_ticket_number#14) && isnotnull(ss_item_sk#7)) && isnotnull(ss_store_sk#12)) && isnotnull(ss_customer_sk#8))
                                 :     :     :     :     :     +- *(6) FileScan parquet tpcds.store_sales[ss_item_sk#7,ss_customer_sk#8,ss_store_sk#12,ss_ticket_number#14,ss_net_paid#25] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_ticket_number), IsNotNull(ss_item_sk), IsNotNull(ss_store_sk), IsNotNull(ss_custome..., ReadSchema: struct<ss_item_sk:int,ss_customer_sk:int,ss_store_sk:int,ss_ticket_number:int,ss_net_paid:double>
                                 :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[1, int, true] as bigint), 32) | (cast(input[0, int, true] as bigint) & 4294967295))))
                                 :     :     :     :        +- *(1) Project [sr_item_sk#30, sr_ticket_number#37]
                                 :     :     :     :           +- *(1) Filter (isnotnull(sr_ticket_number#37) && isnotnull(sr_item_sk#30))
                                 :     :     :     :              +- *(1) FileScan parquet tpcds.store_returns[sr_item_sk#30,sr_ticket_number#37] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_ticket_number), IsNotNull(sr_item_sk)], ReadSchema: struct<sr_item_sk:int,sr_ticket_number:int>
                                 :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 :     :     :        +- *(2) Project [s_store_sk#48, s_store_name#53, s_state#72, s_zip#73]
                                 :     :     :           +- *(2) Filter (((isnotnull(s_market_id#58) && (s_market_id#58 = 8)) && isnotnull(s_store_sk#48)) && isnotnull(s_zip#73))
                                 :     :     :              +- *(2) FileScan parquet tpcds.store[s_store_sk#48,s_store_name#53,s_market_id#58,s_state#72,s_zip#73] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_market_id), EqualTo(s_market_id,8), IsNotNull(s_store_sk), IsNotNull(s_zip)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_market_id:int,s_state:string,s_zip:string>
                                 :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 :     :        +- *(3) Project [i_item_sk#77, i_current_price#82, i_size#92, i_color#94, i_units#95, i_manager_id#97]
                                 :     :           +- *(3) Filter ((isnotnull(i_color#94) && (i_color#94 = pale)) && isnotnull(i_item_sk#77))
                                 :     :              +- *(3) FileScan parquet tpcds.item[i_item_sk#77,i_current_price#82,i_size#92,i_color#94,i_units#95,i_manager_id#97] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/i..., PartitionFilters: [], PushedFilters: [IsNotNull(i_color), EqualTo(i_color,pale), IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_current_price:double,i_size:string,i_color:string,i_units:string,i_manager...
                                 :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                 :        +- *(4) Project [c_customer_sk#99, c_current_addr_sk#103, c_first_name#107, c_last_name#108, c_birth_country#113]
                                 :           +- *(4) Filter ((isnotnull(c_customer_sk#99) && isnotnull(c_birth_country#113)) && isnotnull(c_current_addr_sk#103))
                                 :              +- *(4) FileScan parquet tpcds.customer[c_customer_sk#99,c_current_addr_sk#103,c_first_name#107,c_last_name#108,c_birth_country#113] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_birth_country), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_customer_sk:int,c_current_addr_sk:int,c_first_name:string,c_last_name:string,c_birth_cou...
                                 +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, int, true], input[2, string, true]))
                                    +- *(5) Project [ca_address_sk#117, ca_state#125, ca_zip#126, ca_country#127]
                                       +- *(5) Filter (isnotnull(ca_address_sk#117) && isnotnull(ca_zip#126))
                                          +- *(5) FileScan parquet tpcds.customer_address[ca_address_sk#117,ca_state#125,ca_zip#126,ca_country#127] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk), IsNotNull(ca_zip)], ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_zip:string,ca_country:string>
Time taken: 5.083 seconds, Fetched 1 row(s)
Time taken: 14.463 seconds
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 24 in stream 0 using template query24.tpl
------------------------------------------------------^^^

