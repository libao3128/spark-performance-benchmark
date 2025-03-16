== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['substr('r_reason_desc, 1, 20) ASC NULLS FIRST, 'avg('ws_quantity) ASC NULLS FIRST, 'avg('wr_refunded_cash) ASC NULLS FIRST, 'avg('wr_fee) ASC NULLS FIRST], true
      +- 'Aggregate ['r_reason_desc], [unresolvedalias('substr('r_reason_desc, 1, 20), None), unresolvedalias('avg('ws_quantity), None), unresolvedalias('avg('wr_refunded_cash), None), unresolvedalias('avg('wr_fee), None)]
         +- 'Filter ((((('ws_web_page_sk = 'wp_web_page_sk) && ('ws_item_sk = 'wr_item_sk)) && ('ws_order_number = 'wr_order_number)) && ((('ws_sold_date_sk = 'd_date_sk) && ('d_year = 2000)) && ('cd1.cd_demo_sk = 'wr_refunded_cdemo_sk))) && (((('cd2.cd_demo_sk = 'wr_returning_cdemo_sk) && ('ca_address_sk = 'wr_refunded_addr_sk)) && ('r_reason_sk = 'wr_reason_sk)) && ((((((('cd1.cd_marital_status = M) && ('cd1.cd_marital_status = 'cd2.cd_marital_status)) && ('cd1.cd_education_status = Advanced Degree)) && (('cd1.cd_education_status = 'cd2.cd_education_status) && (('ws_sales_price >= 100.00) && ('ws_sales_price <= 150.00)))) || (((('cd1.cd_marital_status = S) && ('cd1.cd_marital_status = 'cd2.cd_marital_status)) && ('cd1.cd_education_status = College)) && (('cd1.cd_education_status = 'cd2.cd_education_status) && (('ws_sales_price >= 50.00) && ('ws_sales_price <= 100.00))))) || (((('cd1.cd_marital_status = W) && ('cd1.cd_marital_status = 'cd2.cd_marital_status)) && ('cd1.cd_education_status = 2 yr Degree)) && (('cd1.cd_education_status = 'cd2.cd_education_status) && (('ws_sales_price >= 150.00) && ('ws_sales_price <= 200.00))))) && ((((('ca_country = United States) && 'ca_state IN (IN,OH,NJ)) && (('ws_net_profit >= 100) && ('ws_net_profit <= 200))) || ((('ca_country = United States) && 'ca_state IN (WI,CT,KY)) && (('ws_net_profit >= 150) && ('ws_net_profit <= 300)))) || ((('ca_country = United States) && 'ca_state IN (LA,IA,AR)) && (('ws_net_profit >= 50) && ('ws_net_profit <= 250)))))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :  :- 'UnresolvedRelation `web_sales`
               :  :  :  :  :  :  +- 'UnresolvedRelation `web_returns`
               :  :  :  :  :  +- 'UnresolvedRelation `web_page`
               :  :  :  :  +- 'SubqueryAlias `cd1`
               :  :  :  :     +- 'UnresolvedRelation `customer_demographics`
               :  :  :  +- 'SubqueryAlias `cd2`
               :  :  :     +- 'UnresolvedRelation `customer_demographics`
               :  :  +- 'UnresolvedRelation `customer_address`
               :  +- 'UnresolvedRelation `date_dim`
               +- 'UnresolvedRelation `reason`

== Analyzed Logical Plan ==
substring(r_reason_desc, 1, 20): string, avg(ws_quantity): double, avg(wr_refunded_cash): double, avg(wr_fee): double
GlobalLimit 100
+- LocalLimit 100
   +- Project [substring(r_reason_desc, 1, 20)#139, avg(ws_quantity)#140, avg(wr_refunded_cash)#141, avg(wr_fee)#142]
      +- Sort [substring(r_reason_desc, 1, 20)#139 ASC NULLS FIRST, aggOrder#144 ASC NULLS FIRST, avg(wr_refunded_cash)#141 ASC NULLS FIRST, avg(wr_fee)#142 ASC NULLS FIRST], true
         +- Aggregate [r_reason_desc#126], [substring(r_reason_desc#126, 1, 20) AS substring(r_reason_desc, 1, 20)#139, avg(cast(ws_quantity#20 as bigint)) AS avg(ws_quantity)#140, avg(wr_refunded_cash#56) AS avg(wr_refunded_cash)#141, avg(wr_fee#54) AS avg(wr_fee)#142, avg(cast(ws_quantity#20 as bigint)) AS aggOrder#144]
            +- Filter (((((ws_web_page_sk#14 = wp_web_page_sk#60) && (ws_item_sk#5 = wr_item_sk#38)) && (ws_order_number#19 = wr_order_number#49)) && (((ws_sold_date_sk#2 = d_date_sk#96) && (d_year#102 = 2000)) && (cd_demo_sk#74 = wr_refunded_cdemo_sk#40))) && ((((cd_demo_sk#127 = wr_returning_cdemo_sk#44) && (ca_address_sk#83 = wr_refunded_addr_sk#42)) && (r_reason_sk#124 = wr_reason_sk#48)) && (((((((cd_marital_status#76 = M) && (cd_marital_status#76 = cd_marital_status#129)) && (cd_education_status#77 = Advanced Degree)) && ((cd_education_status#77 = cd_education_status#130) && ((ws_sales_price#23 >= cast(100.00 as double)) && (ws_sales_price#23 <= cast(150.00 as double))))) || ((((cd_marital_status#76 = S) && (cd_marital_status#76 = cd_marital_status#129)) && (cd_education_status#77 = College)) && ((cd_education_status#77 = cd_education_status#130) && ((ws_sales_price#23 >= cast(50.00 as double)) && (ws_sales_price#23 <= cast(100.00 as double)))))) || ((((cd_marital_status#76 = W) && (cd_marital_status#76 = cd_marital_status#129)) && (cd_education_status#77 = 2 yr Degree)) && ((cd_education_status#77 = cd_education_status#130) && ((ws_sales_price#23 >= cast(150.00 as double)) && (ws_sales_price#23 <= cast(200.00 as double)))))) && (((((ca_country#93 = United States) && ca_state#91 IN (IN,OH,NJ)) && ((ws_net_profit#35 >= cast(100 as double)) && (ws_net_profit#35 <= cast(200 as double)))) || (((ca_country#93 = United States) && ca_state#91 IN (WI,CT,KY)) && ((ws_net_profit#35 >= cast(150 as double)) && (ws_net_profit#35 <= cast(300 as double))))) || (((ca_country#93 = United States) && ca_state#91 IN (LA,IA,AR)) && ((ws_net_profit#35 >= cast(50 as double)) && (ws_net_profit#35 <= cast(250 as double))))))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- Join Inner
                  :  :  :  :  :- Join Inner
                  :  :  :  :  :  :- Join Inner
                  :  :  :  :  :  :  :- SubqueryAlias `tpcds`.`web_sales`
                  :  :  :  :  :  :  :  +- Relation[ws_sold_date_sk#2,ws_sold_time_sk#3,ws_ship_date_sk#4,ws_item_sk#5,ws_bill_customer_sk#6,ws_bill_cdemo_sk#7,ws_bill_hdemo_sk#8,ws_bill_addr_sk#9,ws_ship_customer_sk#10,ws_ship_cdemo_sk#11,ws_ship_hdemo_sk#12,ws_ship_addr_sk#13,ws_web_page_sk#14,ws_web_site_sk#15,ws_ship_mode_sk#16,ws_warehouse_sk#17,ws_promo_sk#18,ws_order_number#19,ws_quantity#20,ws_wholesale_cost#21,ws_list_price#22,ws_sales_price#23,ws_ext_discount_amt#24,ws_ext_sales_price#25,... 10 more fields] parquet
                  :  :  :  :  :  :  +- SubqueryAlias `tpcds`.`web_returns`
                  :  :  :  :  :  :     +- Relation[wr_returned_date_sk#36,wr_returned_time_sk#37,wr_item_sk#38,wr_refunded_customer_sk#39,wr_refunded_cdemo_sk#40,wr_refunded_hdemo_sk#41,wr_refunded_addr_sk#42,wr_returning_customer_sk#43,wr_returning_cdemo_sk#44,wr_returning_hdemo_sk#45,wr_returning_addr_sk#46,wr_web_page_sk#47,wr_reason_sk#48,wr_order_number#49,wr_return_quantity#50,wr_return_amt#51,wr_return_tax#52,wr_return_amt_inc_tax#53,wr_fee#54,wr_return_ship_cost#55,wr_refunded_cash#56,wr_reversed_charge#57,wr_account_credit#58,wr_net_loss#59] parquet
                  :  :  :  :  :  +- SubqueryAlias `tpcds`.`web_page`
                  :  :  :  :  :     +- Relation[wp_web_page_sk#60,wp_web_page_id#61,wp_rec_start_date#62,wp_rec_end_date#63,wp_creation_date_sk#64,wp_access_date_sk#65,wp_autogen_flag#66,wp_customer_sk#67,wp_url#68,wp_type#69,wp_char_count#70,wp_link_count#71,wp_image_count#72,wp_max_ad_count#73] parquet
                  :  :  :  :  +- SubqueryAlias `cd1`
                  :  :  :  :     +- SubqueryAlias `tpcds`.`customer_demographics`
                  :  :  :  :        +- Relation[cd_demo_sk#74,cd_gender#75,cd_marital_status#76,cd_education_status#77,cd_purchase_estimate#78,cd_credit_rating#79,cd_dep_count#80,cd_dep_employed_count#81,cd_dep_college_count#82] parquet
                  :  :  :  +- SubqueryAlias `cd2`
                  :  :  :     +- SubqueryAlias `tpcds`.`customer_demographics`
                  :  :  :        +- Relation[cd_demo_sk#127,cd_gender#128,cd_marital_status#129,cd_education_status#130,cd_purchase_estimate#131,cd_credit_rating#132,cd_dep_count#133,cd_dep_employed_count#134,cd_dep_college_count#135] parquet
                  :  :  +- SubqueryAlias `tpcds`.`customer_address`
                  :  :     +- Relation[ca_address_sk#83,ca_address_id#84,ca_street_number#85,ca_street_name#86,ca_street_type#87,ca_suite_number#88,ca_city#89,ca_county#90,ca_state#91,ca_zip#92,ca_country#93,ca_gmt_offset#94,ca_location_type#95] parquet
                  :  +- SubqueryAlias `tpcds`.`date_dim`
                  :     +- Relation[d_date_sk#96,d_date_id#97,d_date#98,d_month_seq#99,d_week_seq#100,d_quarter_seq#101,d_year#102,d_dow#103,d_moy#104,d_dom#105,d_qoy#106,d_fy_year#107,d_fy_quarter_seq#108,d_fy_week_seq#109,d_day_name#110,d_quarter_name#111,d_holiday#112,d_weekend#113,d_following_holiday#114,d_first_dom#115,d_last_dom#116,d_same_day_ly#117,d_same_day_lq#118,d_current_day#119,... 4 more fields] parquet
                  +- SubqueryAlias `tpcds`.`reason`
                     +- Relation[r_reason_sk#124,r_reason_id#125,r_reason_desc#126] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Project [substring(r_reason_desc, 1, 20)#139, avg(ws_quantity)#140, avg(wr_refunded_cash)#141, avg(wr_fee)#142]
      +- Sort [substring(r_reason_desc, 1, 20)#139 ASC NULLS FIRST, aggOrder#144 ASC NULLS FIRST, avg(wr_refunded_cash)#141 ASC NULLS FIRST, avg(wr_fee)#142 ASC NULLS FIRST], true
         +- Aggregate [r_reason_desc#126], [substring(r_reason_desc#126, 1, 20) AS substring(r_reason_desc, 1, 20)#139, avg(cast(ws_quantity#20 as bigint)) AS avg(ws_quantity)#140, avg(wr_refunded_cash#56) AS avg(wr_refunded_cash)#141, avg(wr_fee#54) AS avg(wr_fee)#142, avg(cast(ws_quantity#20 as bigint)) AS aggOrder#144]
            +- Project [ws_quantity#20, wr_fee#54, wr_refunded_cash#56, r_reason_desc#126]
               +- Join Inner, (r_reason_sk#124 = wr_reason_sk#48)
                  :- Project [ws_quantity#20, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
                  :  +- Join Inner, (ws_sold_date_sk#2 = d_date_sk#96)
                  :     :- Project [ws_sold_date_sk#2, ws_quantity#20, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
                  :     :  +- Join Inner, ((ca_address_sk#83 = wr_refunded_addr_sk#42) && ((((ca_state#91 IN (IN,OH,NJ) && (ws_net_profit#35 >= 100.0)) && (ws_net_profit#35 <= 200.0)) || ((ca_state#91 IN (WI,CT,KY) && (ws_net_profit#35 >= 150.0)) && (ws_net_profit#35 <= 300.0))) || ((ca_state#91 IN (LA,IA,AR) && (ws_net_profit#35 >= 50.0)) && (ws_net_profit#35 <= 250.0))))
                  :     :     :- Project [ws_sold_date_sk#2, ws_quantity#20, ws_net_profit#35, wr_refunded_addr_sk#42, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
                  :     :     :  +- Join Inner, (((cd_demo_sk#127 = wr_returning_cdemo_sk#44) && (cd_marital_status#76 = cd_marital_status#129)) && (cd_education_status#77 = cd_education_status#130))
                  :     :     :     :- Project [ws_sold_date_sk#2, ws_quantity#20, ws_net_profit#35, wr_refunded_addr_sk#42, wr_returning_cdemo_sk#44, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56, cd_marital_status#76, cd_education_status#77]
                  :     :     :     :  +- Join Inner, (((((((cd_marital_status#76 = M) && (cd_education_status#77 = Advanced Degree)) && (ws_sales_price#23 >= 100.0)) && (ws_sales_price#23 <= 150.0)) || ((((cd_marital_status#76 = S) && (cd_education_status#77 = College)) && (ws_sales_price#23 >= 50.0)) && (ws_sales_price#23 <= 100.0))) || ((((cd_marital_status#76 = W) && (cd_education_status#77 = 2 yr Degree)) && (ws_sales_price#23 >= 150.0)) && (ws_sales_price#23 <= 200.0))) && (cd_demo_sk#74 = wr_refunded_cdemo_sk#40))
                  :     :     :     :     :- Project [ws_sold_date_sk#2, ws_quantity#20, ws_sales_price#23, ws_net_profit#35, wr_refunded_cdemo_sk#40, wr_refunded_addr_sk#42, wr_returning_cdemo_sk#44, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
                  :     :     :     :     :  +- Join Inner, (ws_web_page_sk#14 = wp_web_page_sk#60)
                  :     :     :     :     :     :- Project [ws_sold_date_sk#2, ws_web_page_sk#14, ws_quantity#20, ws_sales_price#23, ws_net_profit#35, wr_refunded_cdemo_sk#40, wr_refunded_addr_sk#42, wr_returning_cdemo_sk#44, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
                  :     :     :     :     :     :  +- Join Inner, ((ws_item_sk#5 = wr_item_sk#38) && (ws_order_number#19 = wr_order_number#49))
                  :     :     :     :     :     :     :- Project [ws_sold_date_sk#2, ws_item_sk#5, ws_web_page_sk#14, ws_order_number#19, ws_quantity#20, ws_sales_price#23, ws_net_profit#35]
                  :     :     :     :     :     :     :  +- Filter (((isnotnull(ws_item_sk#5) && isnotnull(ws_order_number#19)) && isnotnull(ws_web_page_sk#14)) && isnotnull(ws_sold_date_sk#2))
                  :     :     :     :     :     :     :     +- Relation[ws_sold_date_sk#2,ws_sold_time_sk#3,ws_ship_date_sk#4,ws_item_sk#5,ws_bill_customer_sk#6,ws_bill_cdemo_sk#7,ws_bill_hdemo_sk#8,ws_bill_addr_sk#9,ws_ship_customer_sk#10,ws_ship_cdemo_sk#11,ws_ship_hdemo_sk#12,ws_ship_addr_sk#13,ws_web_page_sk#14,ws_web_site_sk#15,ws_ship_mode_sk#16,ws_warehouse_sk#17,ws_promo_sk#18,ws_order_number#19,ws_quantity#20,ws_wholesale_cost#21,ws_list_price#22,ws_sales_price#23,ws_ext_discount_amt#24,ws_ext_sales_price#25,... 10 more fields] parquet
                  :     :     :     :     :     :     +- Project [wr_item_sk#38, wr_refunded_cdemo_sk#40, wr_refunded_addr_sk#42, wr_returning_cdemo_sk#44, wr_reason_sk#48, wr_order_number#49, wr_fee#54, wr_refunded_cash#56]
                  :     :     :     :     :     :        +- Filter (((((isnotnull(wr_item_sk#38) && isnotnull(wr_order_number#49)) && isnotnull(wr_refunded_cdemo_sk#40)) && isnotnull(wr_returning_cdemo_sk#44)) && isnotnull(wr_refunded_addr_sk#42)) && isnotnull(wr_reason_sk#48))
                  :     :     :     :     :     :           +- Relation[wr_returned_date_sk#36,wr_returned_time_sk#37,wr_item_sk#38,wr_refunded_customer_sk#39,wr_refunded_cdemo_sk#40,wr_refunded_hdemo_sk#41,wr_refunded_addr_sk#42,wr_returning_customer_sk#43,wr_returning_cdemo_sk#44,wr_returning_hdemo_sk#45,wr_returning_addr_sk#46,wr_web_page_sk#47,wr_reason_sk#48,wr_order_number#49,wr_return_quantity#50,wr_return_amt#51,wr_return_tax#52,wr_return_amt_inc_tax#53,wr_fee#54,wr_return_ship_cost#55,wr_refunded_cash#56,wr_reversed_charge#57,wr_account_credit#58,wr_net_loss#59] parquet
                  :     :     :     :     :     +- Project [wp_web_page_sk#60]
                  :     :     :     :     :        +- Filter isnotnull(wp_web_page_sk#60)
                  :     :     :     :     :           +- Relation[wp_web_page_sk#60,wp_web_page_id#61,wp_rec_start_date#62,wp_rec_end_date#63,wp_creation_date_sk#64,wp_access_date_sk#65,wp_autogen_flag#66,wp_customer_sk#67,wp_url#68,wp_type#69,wp_char_count#70,wp_link_count#71,wp_image_count#72,wp_max_ad_count#73] parquet
                  :     :     :     :     +- Project [cd_demo_sk#74, cd_marital_status#76, cd_education_status#77]
                  :     :     :     :        +- Filter ((isnotnull(cd_demo_sk#74) && isnotnull(cd_marital_status#76)) && isnotnull(cd_education_status#77))
                  :     :     :     :           +- Relation[cd_demo_sk#74,cd_gender#75,cd_marital_status#76,cd_education_status#77,cd_purchase_estimate#78,cd_credit_rating#79,cd_dep_count#80,cd_dep_employed_count#81,cd_dep_college_count#82] parquet
                  :     :     :     +- Project [cd_demo_sk#127, cd_marital_status#129, cd_education_status#130]
                  :     :     :        +- Filter ((isnotnull(cd_marital_status#129) && isnotnull(cd_demo_sk#127)) && isnotnull(cd_education_status#130))
                  :     :     :           +- Relation[cd_demo_sk#127,cd_gender#128,cd_marital_status#129,cd_education_status#130,cd_purchase_estimate#131,cd_credit_rating#132,cd_dep_count#133,cd_dep_employed_count#134,cd_dep_college_count#135] parquet
                  :     :     +- Project [ca_address_sk#83, ca_state#91]
                  :     :        +- Filter ((isnotnull(ca_country#93) && (ca_country#93 = United States)) && isnotnull(ca_address_sk#83))
                  :     :           +- Relation[ca_address_sk#83,ca_address_id#84,ca_street_number#85,ca_street_name#86,ca_street_type#87,ca_suite_number#88,ca_city#89,ca_county#90,ca_state#91,ca_zip#92,ca_country#93,ca_gmt_offset#94,ca_location_type#95] parquet
                  :     +- Project [d_date_sk#96]
                  :        +- Filter ((isnotnull(d_year#102) && (d_year#102 = 2000)) && isnotnull(d_date_sk#96))
                  :           +- Relation[d_date_sk#96,d_date_id#97,d_date#98,d_month_seq#99,d_week_seq#100,d_quarter_seq#101,d_year#102,d_dow#103,d_moy#104,d_dom#105,d_qoy#106,d_fy_year#107,d_fy_quarter_seq#108,d_fy_week_seq#109,d_day_name#110,d_quarter_name#111,d_holiday#112,d_weekend#113,d_following_holiday#114,d_first_dom#115,d_last_dom#116,d_same_day_ly#117,d_same_day_lq#118,d_current_day#119,... 4 more fields] parquet
                  +- Project [r_reason_sk#124, r_reason_desc#126]
                     +- Filter isnotnull(r_reason_sk#124)
                        +- Relation[r_reason_sk#124,r_reason_id#125,r_reason_desc#126] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[substring(r_reason_desc, 1, 20)#139 ASC NULLS FIRST,aggOrder#144 ASC NULLS FIRST,avg(wr_refunded_cash)#141 ASC NULLS FIRST,avg(wr_fee)#142 ASC NULLS FIRST], output=[substring(r_reason_desc, 1, 20)#139,avg(ws_quantity)#140,avg(wr_refunded_cash)#141,avg(wr_fee)#142])
+- *(9) HashAggregate(keys=[r_reason_desc#126], functions=[avg(cast(ws_quantity#20 as bigint)), avg(wr_refunded_cash#56), avg(wr_fee#54)], output=[substring(r_reason_desc, 1, 20)#139, avg(ws_quantity)#140, avg(wr_refunded_cash)#141, avg(wr_fee)#142, aggOrder#144])
   +- Exchange hashpartitioning(r_reason_desc#126, 200)
      +- *(8) HashAggregate(keys=[r_reason_desc#126], functions=[partial_avg(cast(ws_quantity#20 as bigint)), partial_avg(wr_refunded_cash#56), partial_avg(wr_fee#54)], output=[r_reason_desc#126, sum#156, count#157L, sum#158, count#159L, sum#160, count#161L])
         +- *(8) Project [ws_quantity#20, wr_fee#54, wr_refunded_cash#56, r_reason_desc#126]
            +- *(8) BroadcastHashJoin [wr_reason_sk#48], [r_reason_sk#124], Inner, BuildRight
               :- *(8) Project [ws_quantity#20, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
               :  +- *(8) BroadcastHashJoin [ws_sold_date_sk#2], [d_date_sk#96], Inner, BuildRight
               :     :- *(8) Project [ws_sold_date_sk#2, ws_quantity#20, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
               :     :  +- *(8) BroadcastHashJoin [wr_refunded_addr_sk#42], [ca_address_sk#83], Inner, BuildRight, ((((ca_state#91 IN (IN,OH,NJ) && (ws_net_profit#35 >= 100.0)) && (ws_net_profit#35 <= 200.0)) || ((ca_state#91 IN (WI,CT,KY) && (ws_net_profit#35 >= 150.0)) && (ws_net_profit#35 <= 300.0))) || ((ca_state#91 IN (LA,IA,AR) && (ws_net_profit#35 >= 50.0)) && (ws_net_profit#35 <= 250.0)))
               :     :     :- *(8) Project [ws_sold_date_sk#2, ws_quantity#20, ws_net_profit#35, wr_refunded_addr_sk#42, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
               :     :     :  +- *(8) BroadcastHashJoin [wr_returning_cdemo_sk#44, cd_marital_status#76, cd_education_status#77], [cd_demo_sk#127, cd_marital_status#129, cd_education_status#130], Inner, BuildRight
               :     :     :     :- *(8) Project [ws_sold_date_sk#2, ws_quantity#20, ws_net_profit#35, wr_refunded_addr_sk#42, wr_returning_cdemo_sk#44, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56, cd_marital_status#76, cd_education_status#77]
               :     :     :     :  +- *(8) BroadcastHashJoin [wr_refunded_cdemo_sk#40], [cd_demo_sk#74], Inner, BuildRight, ((((((cd_marital_status#76 = M) && (cd_education_status#77 = Advanced Degree)) && (ws_sales_price#23 >= 100.0)) && (ws_sales_price#23 <= 150.0)) || ((((cd_marital_status#76 = S) && (cd_education_status#77 = College)) && (ws_sales_price#23 >= 50.0)) && (ws_sales_price#23 <= 100.0))) || ((((cd_marital_status#76 = W) && (cd_education_status#77 = 2 yr Degree)) && (ws_sales_price#23 >= 150.0)) && (ws_sales_price#23 <= 200.0)))
               :     :     :     :     :- *(8) Project [ws_sold_date_sk#2, ws_quantity#20, ws_sales_price#23, ws_net_profit#35, wr_refunded_cdemo_sk#40, wr_refunded_addr_sk#42, wr_returning_cdemo_sk#44, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
               :     :     :     :     :  +- *(8) BroadcastHashJoin [ws_web_page_sk#14], [wp_web_page_sk#60], Inner, BuildRight
               :     :     :     :     :     :- *(8) Project [ws_sold_date_sk#2, ws_web_page_sk#14, ws_quantity#20, ws_sales_price#23, ws_net_profit#35, wr_refunded_cdemo_sk#40, wr_refunded_addr_sk#42, wr_returning_cdemo_sk#44, wr_reason_sk#48, wr_fee#54, wr_refunded_cash#56]
               :     :     :     :     :     :  +- *(8) BroadcastHashJoin [ws_item_sk#5, ws_order_number#19], [wr_item_sk#38, wr_order_number#49], Inner, BuildRight
               :     :     :     :     :     :     :- *(8) Project [ws_sold_date_sk#2, ws_item_sk#5, ws_web_page_sk#14, ws_order_number#19, ws_quantity#20, ws_sales_price#23, ws_net_profit#35]
               :     :     :     :     :     :     :  +- *(8) Filter (((isnotnull(ws_item_sk#5) && isnotnull(ws_order_number#19)) && isnotnull(ws_web_page_sk#14)) && isnotnull(ws_sold_date_sk#2))
               :     :     :     :     :     :     :     +- *(8) FileScan parquet tpcds.web_sales[ws_sold_date_sk#2,ws_item_sk#5,ws_web_page_sk#14,ws_order_number#19,ws_quantity#20,ws_sales_price#23,ws_net_profit#35] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(ws_item_sk), IsNotNull(ws_order_number), IsNotNull(ws_web_page_sk), IsNotNull(ws_sold_..., ReadSchema: struct<ws_sold_date_sk:int,ws_item_sk:int,ws_web_page_sk:int,ws_order_number:int,ws_quantity:int,...
               :     :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List((shiftleft(cast(input[0, int, true] as bigint), 32) | (cast(input[5, int, true] as bigint) & 4294967295))))
               :     :     :     :     :     :        +- *(1) Project [wr_item_sk#38, wr_refunded_cdemo_sk#40, wr_refunded_addr_sk#42, wr_returning_cdemo_sk#44, wr_reason_sk#48, wr_order_number#49, wr_fee#54, wr_refunded_cash#56]
               :     :     :     :     :     :           +- *(1) Filter (((((isnotnull(wr_item_sk#38) && isnotnull(wr_order_number#49)) && isnotnull(wr_refunded_cdemo_sk#40)) && isnotnull(wr_returning_cdemo_sk#44)) && isnotnull(wr_refunded_addr_sk#42)) && isnotnull(wr_reason_sk#48))
               :     :     :     :     :     :              +- *(1) FileScan parquet tpcds.web_returns[wr_item_sk#38,wr_refunded_cdemo_sk#40,wr_refunded_addr_sk#42,wr_returning_cdemo_sk#44,wr_reason_sk#48,wr_order_number#49,wr_fee#54,wr_refunded_cash#56] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wr_item_sk), IsNotNull(wr_order_number), IsNotNull(wr_refunded_cdemo_sk), IsNotNull(wr..., ReadSchema: struct<wr_item_sk:int,wr_refunded_cdemo_sk:int,wr_refunded_addr_sk:int,wr_returning_cdemo_sk:int,...
               :     :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :     :        +- *(2) Project [wp_web_page_sk#60]
               :     :     :     :     :           +- *(2) Filter isnotnull(wp_web_page_sk#60)
               :     :     :     :     :              +- *(2) FileScan parquet tpcds.web_page[wp_web_page_sk#60] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/w..., PartitionFilters: [], PushedFilters: [IsNotNull(wp_web_page_sk)], ReadSchema: struct<wp_web_page_sk:int>
               :     :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :     :     :        +- *(3) Project [cd_demo_sk#74, cd_marital_status#76, cd_education_status#77]
               :     :     :     :           +- *(3) Filter ((isnotnull(cd_demo_sk#74) && isnotnull(cd_marital_status#76)) && isnotnull(cd_education_status#77))
               :     :     :     :              +- *(3) FileScan parquet tpcds.customer_demographics[cd_demo_sk#74,cd_marital_status#76,cd_education_status#77] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_demo_sk), IsNotNull(cd_marital_status), IsNotNull(cd_education_status)], ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
               :     :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, int, true], input[1, string, true], input[2, string, true]))
               :     :     :        +- *(4) Project [cd_demo_sk#127, cd_marital_status#129, cd_education_status#130]
               :     :     :           +- *(4) Filter ((isnotnull(cd_marital_status#129) && isnotnull(cd_demo_sk#127)) && isnotnull(cd_education_status#130))
               :     :     :              +- *(4) FileScan parquet tpcds.customer_demographics[cd_demo_sk#127,cd_marital_status#129,cd_education_status#130] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(cd_marital_status), IsNotNull(cd_demo_sk), IsNotNull(cd_education_status)], ReadSchema: struct<cd_demo_sk:int,cd_marital_status:string,cd_education_status:string>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(5) Project [ca_address_sk#83, ca_state#91]
               :     :           +- *(5) Filter ((isnotnull(ca_country#93) && (ca_country#93 = United States)) && isnotnull(ca_address_sk#83))
               :     :              +- *(5) FileScan parquet tpcds.customer_address[ca_address_sk#83,ca_state#91,ca_country#93] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_country), EqualTo(ca_country,United States), IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_state:string,ca_country:string>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(6) Project [d_date_sk#96]
               :           +- *(6) Filter ((isnotnull(d_year#102) && (d_year#102 = 2000)) && isnotnull(d_date_sk#96))
               :              +- *(6) FileScan parquet tpcds.date_dim[d_date_sk#96,d_year#102] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int>
               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                  +- *(7) Project [r_reason_sk#124, r_reason_desc#126]
                     +- *(7) Filter isnotnull(r_reason_sk#124)
                        +- *(7) FileScan parquet tpcds.reason[r_reason_sk#124,r_reason_desc#126] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/r..., PartitionFilters: [], PushedFilters: [IsNotNull(r_reason_sk)], ReadSchema: struct<r_reason_sk:int,r_reason_desc:string>
Time taken: 4.795 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 54)

== SQL ==

-- end query 85 in stream 0 using template query85.tpl
------------------------------------------------------^^^

