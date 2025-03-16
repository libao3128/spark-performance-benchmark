== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['s_store_name ASC NULLS FIRST], true
      +- 'Aggregate ['s_store_name], ['s_store_name, unresolvedalias('sum('ss_net_profit), None)]
         +- 'Filter (((('ss_store_sk = 's_store_sk) && ('ss_sold_date_sk = 'd_date_sk)) && ('d_qoy = 2)) && (('d_year = 1998) && ('substr('s_zip, 1, 2) = 'substr('V1.ca_zip, 1, 2))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation `store_sales`
               :  :  +- 'UnresolvedRelation `date_dim`
               :  +- 'UnresolvedRelation `store`
               +- 'SubqueryAlias `V1`
                  +- 'Project ['ca_zip]
                     +- 'SubqueryAlias `A2`
                        +- 'Intersect false
                           :- 'Project ['substr('ca_zip, 1, 5) AS ca_zip#0]
                           :  +- 'Filter 'substr('ca_zip, 1, 5) IN (24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,54601,67897,22752,86284,18376,38607,45200,21756,29741,96765,23932,89360,29839,25989,28898,91068,72550,10390,18845,47770,82636,41367,76638,86198,81312,37126,39192,88424,72175,81426,53672,10445,42666,66864,66708,41248,48583,82276,18842,78890,49448,14089,38122,34425,79077,19849,43285,39861,66162,77610,13695,99543,83444,83041,12305,57665,68341,25003,57834,62878,49130,81096,18840,27700,23470,50412,21195,16021,76107,71954,68309,18119,98359,64544,10336,86379,27068,39736,98569,28915,24206,56529,57647,54917,42961,91110,63981,14922,36420,23006,67467,32754,30903,20260,31671,51798,72325,85816,68621,13955,36446,41766,68806,16725,15146,22744,35850,88086,51649,18270,52867,39972,96976,63792,11376,94898,13595,10516,90225,58943,39371,94945,28587,96576,57855,28488,26105,83933,25858,34322,44438,73171,30122,34102,22685,71256,78451,54364,13354,45375,40558,56458,28286,45266,47305,69399,83921,26233,11101,15371,69913,35942,15882,25631,24610,44165,99076,33786,70738,26653,14328,72305,62496,22152,10144,64147,48425,14663,21076,18799,30450,63089,81019,68893,24996,51200,51211,45692,92712,70466,79994,22437,25280,38935,71791,73134,56571,14060,19505,72425,56575,74351,68786,51650,20004,18383,76614,11634,18906,15765,41368,73241,76698,78567,97189,28545,76231,75691,22246,51061,90578,56691,68014,51103,94167,57047,14867,73520,15734,63435,25733,35474,24676,94627,53535,17879,15559,53268,59166,11928,59402,33282,45721,43933,68101,33515,36634,71286,19736,58058,55253,67473,41918,19515,36495,19430,22351,77191,91393,49156,50298,87501,18652,53179,18767,63193,23968,65164,68880,21286,72823,58470,67301,13394,31016,70372,67030,40604,24317,45748,39127,26065,77721,31029,31880,60576,24671,45549,13376,50016,33123,19769,22927,97789,46081,72151,15723,46136,51949,68100,96888,64528,14171,79777,28709,11489,25103,32213,78668,22245,15798,27156,37930,62971,21337,51622,67853,10567,38415,15455,58263,42029,60279,37125,56240,88190,50308,26859,64457,89091,82136,62377,36233,63837,58078,17043,30010,60099,28810,98025,29178,87343,73273,30469,64034,39516,86057,21309,90257,67875,40162,11356,73650,61810,72013,30431,22461,19512,13375,55307,30625,83849,68908,26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576)
                           :     +- 'UnresolvedRelation `customer_address`
                           +- 'Project ['ca_zip]
                              +- 'SubqueryAlias `A1`
                                 +- 'UnresolvedHaving ('count(1) > 10)
                                    +- 'Aggregate ['ca_zip], ['substr('ca_zip, 1, 5) AS ca_zip#1, 'count(1) AS cnt#2]
                                       +- 'Filter (('ca_address_sk = 'c_current_addr_sk) && ('c_preferred_cust_flag = Y))
                                          +- 'Join Inner
                                             :- 'UnresolvedRelation `customer_address`
                                             +- 'UnresolvedRelation `customer`

== Analyzed Logical Plan ==
s_store_name: string, sum(ss_net_profit): double
GlobalLimit 100
+- LocalLimit 100
   +- Project [s_store_name#63, sum(ss_net_profit)#123]
      +- Sort [s_store_name#63 ASC NULLS FIRST], true
         +- Aggregate [s_store_name#63], [s_store_name#63, sum(ss_net_profit#29) AS sum(ss_net_profit)#123]
            +- Filter ((((ss_store_sk#14 = s_store_sk#58) && (ss_sold_date_sk#7 = d_date_sk#30)) && (d_qoy#40 = 2)) && ((d_year#36 = 1998) && (substring(s_zip#83, 1, 2) = substring(ca_zip#0, 1, 2))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- SubqueryAlias `tpcds`.`store_sales`
                  :  :  :  +- Relation[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
                  :  :  +- SubqueryAlias `tpcds`.`date_dim`
                  :  :     +- Relation[d_date_sk#30,d_date_id#31,d_date#32,d_month_seq#33,d_week_seq#34,d_quarter_seq#35,d_year#36,d_dow#37,d_moy#38,d_dom#39,d_qoy#40,d_fy_year#41,d_fy_quarter_seq#42,d_fy_week_seq#43,d_day_name#44,d_quarter_name#45,d_holiday#46,d_weekend#47,d_following_holiday#48,d_first_dom#49,d_last_dom#50,d_same_day_ly#51,d_same_day_lq#52,d_current_day#53,... 4 more fields] parquet
                  :  +- SubqueryAlias `tpcds`.`store`
                  :     +- Relation[s_store_sk#58,s_store_id#59,s_rec_start_date#60,s_rec_end_date#61,s_closed_date_sk#62,s_store_name#63,s_number_employees#64,s_floor_space#65,s_hours#66,s_manager#67,s_market_id#68,s_geography_class#69,s_market_desc#70,s_market_manager#71,s_division_id#72,s_division_name#73,s_company_id#74,s_company_name#75,s_street_number#76,s_street_name#77,s_street_type#78,s_suite_number#79,s_city#80,s_county#81,... 5 more fields] parquet
                  +- SubqueryAlias `V1`
                     +- Project [ca_zip#0]
                        +- SubqueryAlias `A2`
                           +- Intersect false
                              :- Project [substring(ca_zip#96, 1, 5) AS ca_zip#0]
                              :  +- Filter substring(ca_zip#96, 1, 5) IN (24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,54601,67897,22752,86284,18376,38607,45200,21756,29741,96765,23932,89360,29839,25989,28898,91068,72550,10390,18845,47770,82636,41367,76638,86198,81312,37126,39192,88424,72175,81426,53672,10445,42666,66864,66708,41248,48583,82276,18842,78890,49448,14089,38122,34425,79077,19849,43285,39861,66162,77610,13695,99543,83444,83041,12305,57665,68341,25003,57834,62878,49130,81096,18840,27700,23470,50412,21195,16021,76107,71954,68309,18119,98359,64544,10336,86379,27068,39736,98569,28915,24206,56529,57647,54917,42961,91110,63981,14922,36420,23006,67467,32754,30903,20260,31671,51798,72325,85816,68621,13955,36446,41766,68806,16725,15146,22744,35850,88086,51649,18270,52867,39972,96976,63792,11376,94898,13595,10516,90225,58943,39371,94945,28587,96576,57855,28488,26105,83933,25858,34322,44438,73171,30122,34102,22685,71256,78451,54364,13354,45375,40558,56458,28286,45266,47305,69399,83921,26233,11101,15371,69913,35942,15882,25631,24610,44165,99076,33786,70738,26653,14328,72305,62496,22152,10144,64147,48425,14663,21076,18799,30450,63089,81019,68893,24996,51200,51211,45692,92712,70466,79994,22437,25280,38935,71791,73134,56571,14060,19505,72425,56575,74351,68786,51650,20004,18383,76614,11634,18906,15765,41368,73241,76698,78567,97189,28545,76231,75691,22246,51061,90578,56691,68014,51103,94167,57047,14867,73520,15734,63435,25733,35474,24676,94627,53535,17879,15559,53268,59166,11928,59402,33282,45721,43933,68101,33515,36634,71286,19736,58058,55253,67473,41918,19515,36495,19430,22351,77191,91393,49156,50298,87501,18652,53179,18767,63193,23968,65164,68880,21286,72823,58470,67301,13394,31016,70372,67030,40604,24317,45748,39127,26065,77721,31029,31880,60576,24671,45549,13376,50016,33123,19769,22927,97789,46081,72151,15723,46136,51949,68100,96888,64528,14171,79777,28709,11489,25103,32213,78668,22245,15798,27156,37930,62971,21337,51622,67853,10567,38415,15455,58263,42029,60279,37125,56240,88190,50308,26859,64457,89091,82136,62377,36233,63837,58078,17043,30010,60099,28810,98025,29178,87343,73273,30469,64034,39516,86057,21309,90257,67875,40162,11356,73650,61810,72013,30431,22461,19512,13375,55307,30625,83849,68908,26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576)
                              :     +- SubqueryAlias `tpcds`.`customer_address`
                              :        +- Relation[ca_address_sk#87,ca_address_id#88,ca_street_number#89,ca_street_name#90,ca_street_type#91,ca_suite_number#92,ca_city#93,ca_county#94,ca_state#95,ca_zip#96,ca_country#97,ca_gmt_offset#98,ca_location_type#99] parquet
                              +- Project [ca_zip#1]
                                 +- SubqueryAlias `A1`
                                    +- Project [ca_zip#1, cnt#2L]
                                       +- Filter (count(1)#119L > cast(10 as bigint))
                                          +- Aggregate [ca_zip#96], [substring(ca_zip#96, 1, 5) AS ca_zip#1, count(1) AS cnt#2L, count(1) AS count(1)#119L]
                                             +- Filter ((ca_address_sk#87 = c_current_addr_sk#104) && (c_preferred_cust_flag#110 = Y))
                                                +- Join Inner
                                                   :- SubqueryAlias `tpcds`.`customer_address`
                                                   :  +- Relation[ca_address_sk#87,ca_address_id#88,ca_street_number#89,ca_street_name#90,ca_street_type#91,ca_suite_number#92,ca_city#93,ca_county#94,ca_state#95,ca_zip#96,ca_country#97,ca_gmt_offset#98,ca_location_type#99] parquet
                                                   +- SubqueryAlias `tpcds`.`customer`
                                                      +- Relation[c_customer_sk#100,c_customer_id#101,c_current_cdemo_sk#102,c_current_hdemo_sk#103,c_current_addr_sk#104,c_first_shipto_date_sk#105,c_first_sales_date_sk#106,c_salutation#107,c_first_name#108,c_last_name#109,c_preferred_cust_flag#110,c_birth_day#111,c_birth_month#112,c_birth_year#113,c_birth_country#114,c_login#115,c_email_address#116,c_last_review_date#117] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#63 ASC NULLS FIRST], true
      +- Aggregate [s_store_name#63], [s_store_name#63, sum(ss_net_profit#29) AS sum(ss_net_profit)#123]
         +- Project [ss_net_profit#29, s_store_name#63]
            +- Join Inner, (substring(s_zip#83, 1, 2) = substring(ca_zip#0, 1, 2))
               :- Project [ss_net_profit#29, s_store_name#63, s_zip#83]
               :  +- Join Inner, (ss_store_sk#14 = s_store_sk#58)
               :     :- Project [ss_store_sk#14, ss_net_profit#29]
               :     :  +- Join Inner, (ss_sold_date_sk#7 = d_date_sk#30)
               :     :     :- Project [ss_sold_date_sk#7, ss_store_sk#14, ss_net_profit#29]
               :     :     :  +- Filter (isnotnull(ss_sold_date_sk#7) && isnotnull(ss_store_sk#14))
               :     :     :     +- Relation[ss_sold_date_sk#7,ss_sold_time_sk#8,ss_item_sk#9,ss_customer_sk#10,ss_cdemo_sk#11,ss_hdemo_sk#12,ss_addr_sk#13,ss_store_sk#14,ss_promo_sk#15,ss_ticket_number#16,ss_quantity#17,ss_wholesale_cost#18,ss_list_price#19,ss_sales_price#20,ss_ext_discount_amt#21,ss_ext_sales_price#22,ss_ext_wholesale_cost#23,ss_ext_list_price#24,ss_ext_tax#25,ss_coupon_amt#26,ss_net_paid#27,ss_net_paid_inc_tax#28,ss_net_profit#29] parquet
               :     :     +- Project [d_date_sk#30]
               :     :        +- Filter ((((isnotnull(d_qoy#40) && isnotnull(d_year#36)) && (d_qoy#40 = 2)) && (d_year#36 = 1998)) && isnotnull(d_date_sk#30))
               :     :           +- Relation[d_date_sk#30,d_date_id#31,d_date#32,d_month_seq#33,d_week_seq#34,d_quarter_seq#35,d_year#36,d_dow#37,d_moy#38,d_dom#39,d_qoy#40,d_fy_year#41,d_fy_quarter_seq#42,d_fy_week_seq#43,d_day_name#44,d_quarter_name#45,d_holiday#46,d_weekend#47,d_following_holiday#48,d_first_dom#49,d_last_dom#50,d_same_day_ly#51,d_same_day_lq#52,d_current_day#53,... 4 more fields] parquet
               :     +- Project [s_store_sk#58, s_store_name#63, s_zip#83]
               :        +- Filter (isnotnull(s_store_sk#58) && isnotnull(s_zip#83))
               :           +- Relation[s_store_sk#58,s_store_id#59,s_rec_start_date#60,s_rec_end_date#61,s_closed_date_sk#62,s_store_name#63,s_number_employees#64,s_floor_space#65,s_hours#66,s_manager#67,s_market_id#68,s_geography_class#69,s_market_desc#70,s_market_manager#71,s_division_id#72,s_division_name#73,s_company_id#74,s_company_name#75,s_street_number#76,s_street_name#77,s_street_type#78,s_suite_number#79,s_city#80,s_county#81,... 5 more fields] parquet
               +- Aggregate [ca_zip#0], [ca_zip#0]
                  +- Join LeftSemi, (ca_zip#0 <=> ca_zip#1)
                     :- Project [substring(ca_zip#96, 1, 5) AS ca_zip#0]
                     :  +- Filter (substring(ca_zip#96, 1, 5) INSET (56910,69952,63792,39371,74351,11101,25003,97189,57834,73134,62377,51200,32754,22752,86379,14171,91110,40162,98569,28709,13394,66162,25733,25782,26065,18383,51949,87343,50298,83849,33786,64528,23470,67030,46136,25280,46820,77721,99076,18426,31880,17871,98235,45748,49156,18652,72013,51622,43848,78567,41248,13695,44165,67853,54917,53179,64034,10567,71791,68908,55565,59402,64147,85816,57855,61547,27700,68100,28810,58263,15723,83933,51103,58058,90578,82276,81096,81426,96451,77556,38607,76638,18906,62971,57047,48425,35576,11928,30625,83444,73520,51650,57647,60099,30122,94983,24128,10445,41368,26233,26859,21756,24676,19849,36420,38193,58470,39127,13595,87501,24317,15455,69399,98025,81019,48033,11376,39516,67875,92712,14867,38122,29741,42961,30469,51211,56458,15559,16021,33123,33282,33515,72823,54601,76698,56240,72175,60279,20004,68806,72325,28488,43933,50412,45200,22246,78668,79777,96765,67301,73273,49448,82636,23932,47305,29839,39192,18799,61265,37125,58943,64457,88424,24610,84935,89360,68893,30431,28898,10336,90257,59166,46081,26105,96888,36634,86284,35258,39972,22927,73241,53268,24206,27385,99543,31671,14663,30903,39861,24996,63089,88086,83921,21076,67897,66708,45721,60576,25103,52867,30450,36233,30010,96576,73171,56571,56575,64544,13955,78451,43285,18119,16725,83041,76107,79994,54364,35942,56691,19769,63435,34102,18845,22744,13354,75691,45549,23968,31387,83144,13375,15765,28577,88190,19736,73650,37930,25989,83926,94898,51798,39736,22437,55253,38415,71256,18376,42029,25858,44438,19515,38935,51649,71954,15882,18767,63193,25486,49130,37126,40604,34425,17043,12305,11634,26653,94167,36446,10516,67473,66864,72425,63981,18842,22461,42666,47770,69035,70372,28587,45266,15371,15798,45375,90225,16807,31016,68014,21337,19505,50016,10144,84093,21286,19430,34322,91068,94945,72305,24671,58048,65084,28545,21195,20548,22245,77191,96976,48583,76231,15734,61810,11356,68621,68786,98359,41367,26689,69913,76614,68101,88885,50308,79077,18270,28915,29178,53672,62878,10390,14922,68341,56529,41766,68309,56616,15126,61860,97789,11489,45692,41918,72151,72550,27156,36495,70738,17879,53535,17920,68880,78890,35850,14089,58078,65164,27068,26231,13376,57665,32213,77610,87816,21309,15146,86198,91137,55307,67467,40558,94627,82136,22351,89091,20260,23006,91393,47537,62496,98294,18840,71286,81312,31029,70466,35458,14060,22685,28286,25631,19512,40081,63837,14328,35474,22152,76232,51061,86057,17183) && isnotnull(substring(ca_zip#96, 1, 5)))
                     :     +- Relation[ca_address_sk#87,ca_address_id#88,ca_street_number#89,ca_street_name#90,ca_street_type#91,ca_suite_number#92,ca_city#93,ca_county#94,ca_state#95,ca_zip#96,ca_country#97,ca_gmt_offset#98,ca_location_type#99] parquet
                     +- Project [ca_zip#1]
                        +- Filter (count(1)#119L > 10)
                           +- Aggregate [ca_zip#96], [substring(ca_zip#96, 1, 5) AS ca_zip#1, count(1) AS count(1)#119L]
                              +- Project [ca_zip#96]
                                 +- Join Inner, (ca_address_sk#87 = c_current_addr_sk#104)
                                    :- Project [ca_address_sk#87, ca_zip#96]
                                    :  +- Filter isnotnull(ca_address_sk#87)
                                    :     +- Relation[ca_address_sk#87,ca_address_id#88,ca_street_number#89,ca_street_name#90,ca_street_type#91,ca_suite_number#92,ca_city#93,ca_county#94,ca_state#95,ca_zip#96,ca_country#97,ca_gmt_offset#98,ca_location_type#99] parquet
                                    +- Project [c_current_addr_sk#104]
                                       +- Filter ((isnotnull(c_preferred_cust_flag#110) && (c_preferred_cust_flag#110 = Y)) && isnotnull(c_current_addr_sk#104))
                                          +- Relation[c_customer_sk#100,c_customer_id#101,c_current_cdemo_sk#102,c_current_hdemo_sk#103,c_current_addr_sk#104,c_first_shipto_date_sk#105,c_first_sales_date_sk#106,c_salutation#107,c_first_name#108,c_last_name#109,c_preferred_cust_flag#110,c_birth_day#111,c_birth_month#112,c_birth_year#113,c_birth_country#114,c_login#115,c_email_address#116,c_last_review_date#117] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[s_store_name#63 ASC NULLS FIRST], output=[s_store_name#63,sum(ss_net_profit)#123])
+- *(12) HashAggregate(keys=[s_store_name#63], functions=[sum(ss_net_profit#29)], output=[s_store_name#63, sum(ss_net_profit)#123])
   +- Exchange hashpartitioning(s_store_name#63, 200)
      +- *(11) HashAggregate(keys=[s_store_name#63], functions=[partial_sum(ss_net_profit#29)], output=[s_store_name#63, sum#131])
         +- *(11) Project [ss_net_profit#29, s_store_name#63]
            +- *(11) BroadcastHashJoin [substring(s_zip#83, 1, 2)], [substring(ca_zip#0, 1, 2)], Inner, BuildRight
               :- *(11) Project [ss_net_profit#29, s_store_name#63, s_zip#83]
               :  +- *(11) BroadcastHashJoin [ss_store_sk#14], [s_store_sk#58], Inner, BuildRight
               :     :- *(11) Project [ss_store_sk#14, ss_net_profit#29]
               :     :  +- *(11) BroadcastHashJoin [ss_sold_date_sk#7], [d_date_sk#30], Inner, BuildRight
               :     :     :- *(11) Project [ss_sold_date_sk#7, ss_store_sk#14, ss_net_profit#29]
               :     :     :  +- *(11) Filter (isnotnull(ss_sold_date_sk#7) && isnotnull(ss_store_sk#14))
               :     :     :     +- *(11) FileScan parquet tpcds.store_sales[ss_sold_date_sk#7,ss_store_sk#14,ss_net_profit#29] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_net_profit:double>
               :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :     :        +- *(1) Project [d_date_sk#30]
               :     :           +- *(1) Filter ((((isnotnull(d_qoy#40) && isnotnull(d_year#36)) && (d_qoy#40 = 2)) && (d_year#36 = 1998)) && isnotnull(d_date_sk#30))
               :     :              +- *(1) FileScan parquet tpcds.date_dim[d_date_sk#30,d_year#36,d_qoy#40] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/d..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,2), EqualTo(d_year,1998), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
               :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               :        +- *(2) Project [s_store_sk#58, s_store_name#63, s_zip#83]
               :           +- *(2) Filter (isnotnull(s_store_sk#58) && isnotnull(s_zip#83))
               :              +- *(2) FileScan parquet tpcds.store[s_store_sk#58,s_store_name#63,s_zip#83] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/s..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_zip)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_zip:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(substring(input[0, string, true], 1, 2)))
                  +- *(10) HashAggregate(keys=[ca_zip#0], functions=[], output=[ca_zip#0])
                     +- Exchange hashpartitioning(ca_zip#0, 200)
                        +- *(9) HashAggregate(keys=[ca_zip#0], functions=[], output=[ca_zip#0])
                           +- SortMergeJoin [coalesce(ca_zip#0, )], [coalesce(ca_zip#1, )], LeftSemi, (ca_zip#0 <=> ca_zip#1)
                              :- *(4) Sort [coalesce(ca_zip#0, ) ASC NULLS FIRST], false, 0
                              :  +- Exchange hashpartitioning(coalesce(ca_zip#0, ), 200)
                              :     +- *(3) Project [substring(ca_zip#96, 1, 5) AS ca_zip#0]
                              :        +- *(3) Filter (substring(ca_zip#96, 1, 5) INSET (56910,69952,63792,39371,74351,11101,25003,97189,57834,73134,62377,51200,32754,22752,86379,14171,91110,40162,98569,28709,13394,66162,25733,25782,26065,18383,51949,87343,50298,83849,33786,64528,23470,67030,46136,25280,46820,77721,99076,18426,31880,17871,98235,45748,49156,18652,72013,51622,43848,78567,41248,13695,44165,67853,54917,53179,64034,10567,71791,68908,55565,59402,64147,85816,57855,61547,27700,68100,28810,58263,15723,83933,51103,58058,90578,82276,81096,81426,96451,77556,38607,76638,18906,62971,57047,48425,35576,11928,30625,83444,73520,51650,57647,60099,30122,94983,24128,10445,41368,26233,26859,21756,24676,19849,36420,38193,58470,39127,13595,87501,24317,15455,69399,98025,81019,48033,11376,39516,67875,92712,14867,38122,29741,42961,30469,51211,56458,15559,16021,33123,33282,33515,72823,54601,76698,56240,72175,60279,20004,68806,72325,28488,43933,50412,45200,22246,78668,79777,96765,67301,73273,49448,82636,23932,47305,29839,39192,18799,61265,37125,58943,64457,88424,24610,84935,89360,68893,30431,28898,10336,90257,59166,46081,26105,96888,36634,86284,35258,39972,22927,73241,53268,24206,27385,99543,31671,14663,30903,39861,24996,63089,88086,83921,21076,67897,66708,45721,60576,25103,52867,30450,36233,30010,96576,73171,56571,56575,64544,13955,78451,43285,18119,16725,83041,76107,79994,54364,35942,56691,19769,63435,34102,18845,22744,13354,75691,45549,23968,31387,83144,13375,15765,28577,88190,19736,73650,37930,25989,83926,94898,51798,39736,22437,55253,38415,71256,18376,42029,25858,44438,19515,38935,51649,71954,15882,18767,63193,25486,49130,37126,40604,34425,17043,12305,11634,26653,94167,36446,10516,67473,66864,72425,63981,18842,22461,42666,47770,69035,70372,28587,45266,15371,15798,45375,90225,16807,31016,68014,21337,19505,50016,10144,84093,21286,19430,34322,91068,94945,72305,24671,58048,65084,28545,21195,20548,22245,77191,96976,48583,76231,15734,61810,11356,68621,68786,98359,41367,26689,69913,76614,68101,88885,50308,79077,18270,28915,29178,53672,62878,10390,14922,68341,56529,41766,68309,56616,15126,61860,97789,11489,45692,41918,72151,72550,27156,36495,70738,17879,53535,17920,68880,78890,35850,14089,58078,65164,27068,26231,13376,57665,32213,77610,87816,21309,15146,86198,91137,55307,67467,40558,94627,82136,22351,89091,20260,23006,91393,47537,62496,98294,18840,71286,81312,31029,70466,35458,14060,22685,28286,25631,19512,40081,63837,14328,35474,22152,76232,51061,86057,17183) && isnotnull(substring(ca_zip#96, 1, 5)))
                              :           +- *(3) FileScan parquet tpcds.customer_address[ca_zip#96] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ca_zip:string>
                              +- *(8) Sort [coalesce(ca_zip#1, ) ASC NULLS FIRST], false, 0
                                 +- Exchange hashpartitioning(coalesce(ca_zip#1, ), 200)
                                    +- *(7) Project [ca_zip#1]
                                       +- *(7) Filter (count(1)#119L > 10)
                                          +- *(7) HashAggregate(keys=[ca_zip#96], functions=[count(1)], output=[ca_zip#1, count(1)#119L])
                                             +- Exchange hashpartitioning(ca_zip#96, 200)
                                                +- *(6) HashAggregate(keys=[ca_zip#96], functions=[partial_count(1)], output=[ca_zip#96, count#133L])
                                                   +- *(6) Project [ca_zip#96]
                                                      +- *(6) BroadcastHashJoin [ca_address_sk#87], [c_current_addr_sk#104], Inner, BuildLeft
                                                         :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                                                         :  +- *(5) Project [ca_address_sk#87, ca_zip#96]
                                                         :     +- *(5) Filter isnotnull(ca_address_sk#87)
                                                         :        +- *(5) FileScan parquet tpcds.customer_address[ca_address_sk#87,ca_zip#96] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_zip:string>
                                                         +- *(6) Project [c_current_addr_sk#104]
                                                            +- *(6) Filter ((isnotnull(c_preferred_cust_flag#110) && (c_preferred_cust_flag#110 = Y)) && isnotnull(c_current_addr_sk#104))
                                                               +- *(6) FileScan parquet tpcds.customer[c_current_addr_sk#104,c_preferred_cust_flag#110] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/Users/angela/Desktop/spark-2.4.8-bin-hadoop2.7/spark-warehouse/tpcds.db/c..., PartitionFilters: [], PushedFilters: [IsNotNull(c_preferred_cust_flag), EqualTo(c_preferred_cust_flag,Y), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_current_addr_sk:int,c_preferred_cust_flag:string>
Time taken: 4.447 seconds, Fetched 1 row(s)
Error in query: 
mismatched input '<EOF>' expecting {'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}(line 2, pos 52)

== SQL ==

-- end query 8 in stream 0 using template query8.tpl
----------------------------------------------------^^^

