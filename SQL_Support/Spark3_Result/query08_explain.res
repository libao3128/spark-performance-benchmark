Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741579748256
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['s_store_name ASC NULLS FIRST], true
      +- 'Aggregate ['s_store_name], ['s_store_name, unresolvedalias('sum('ss_net_profit), None)]
         +- 'Filter (((('ss_store_sk = 's_store_sk) AND ('ss_sold_date_sk = 'd_date_sk)) AND ('d_qoy = 2)) AND (('d_year = 1998) AND ('substr('s_zip, 1, 2) = 'substr('V1.ca_zip, 1, 2))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation [store_sales], [], false
               :  :  +- 'UnresolvedRelation [date_dim], [], false
               :  +- 'UnresolvedRelation [store], [], false
               +- 'SubqueryAlias V1
                  +- 'Project ['ca_zip]
                     +- 'SubqueryAlias A2
                        +- 'Intersect false
                           :- 'Project ['substr('ca_zip, 1, 5) AS ca_zip#0]
                           :  +- 'Filter 'substr('ca_zip, 1, 5) IN (24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,54601,67897,22752,86284,18376,38607,45200,21756,29741,96765,23932,89360,29839,25989,28898,91068,72550,10390,18845,47770,82636,41367,76638,86198,81312,37126,39192,88424,72175,81426,53672,10445,42666,66864,66708,41248,48583,82276,18842,78890,49448,14089,38122,34425,79077,19849,43285,39861,66162,77610,13695,99543,83444,83041,12305,57665,68341,25003,57834,62878,49130,81096,18840,27700,23470,50412,21195,16021,76107,71954,68309,18119,98359,64544,10336,86379,27068,39736,98569,28915,24206,56529,57647,54917,42961,91110,63981,14922,36420,23006,67467,32754,30903,20260,31671,51798,72325,85816,68621,13955,36446,41766,68806,16725,15146,22744,35850,88086,51649,18270,52867,39972,96976,63792,11376,94898,13595,10516,90225,58943,39371,94945,28587,96576,57855,28488,26105,83933,25858,34322,44438,73171,30122,34102,22685,71256,78451,54364,13354,45375,40558,56458,28286,45266,47305,69399,83921,26233,11101,15371,69913,35942,15882,25631,24610,44165,99076,33786,70738,26653,14328,72305,62496,22152,10144,64147,48425,14663,21076,18799,30450,63089,81019,68893,24996,51200,51211,45692,92712,70466,79994,22437,25280,38935,71791,73134,56571,14060,19505,72425,56575,74351,68786,51650,20004,18383,76614,11634,18906,15765,41368,73241,76698,78567,97189,28545,76231,75691,22246,51061,90578,56691,68014,51103,94167,57047,14867,73520,15734,63435,25733,35474,24676,94627,53535,17879,15559,53268,59166,11928,59402,33282,45721,43933,68101,33515,36634,71286,19736,58058,55253,67473,41918,19515,36495,19430,22351,77191,91393,49156,50298,87501,18652,53179,18767,63193,23968,65164,68880,21286,72823,58470,67301,13394,31016,70372,67030,40604,24317,45748,39127,26065,77721,31029,31880,60576,24671,45549,13376,50016,33123,19769,22927,97789,46081,72151,15723,46136,51949,68100,96888,64528,14171,79777,28709,11489,25103,32213,78668,22245,15798,27156,37930,62971,21337,51622,67853,10567,38415,15455,58263,42029,60279,37125,56240,88190,50308,26859,64457,89091,82136,62377,36233,63837,58078,17043,30010,60099,28810,98025,29178,87343,73273,30469,64034,39516,86057,21309,90257,67875,40162,11356,73650,61810,72013,30431,22461,19512,13375,55307,30625,83849,68908,26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576)
                           :     +- 'UnresolvedRelation [customer_address], [], false
                           +- 'Project ['ca_zip]
                              +- 'SubqueryAlias A1
                                 +- 'UnresolvedHaving ('count(1) > 10)
                                    +- 'Aggregate ['ca_zip], ['substr('ca_zip, 1, 5) AS ca_zip#1, 'count(1) AS cnt#2]
                                       +- 'Filter (('ca_address_sk = 'c_current_addr_sk) AND ('c_preferred_cust_flag = Y))
                                          +- 'Join Inner
                                             :- 'UnresolvedRelation [customer_address], [], false
                                             +- 'UnresolvedRelation [customer], [], false

== Analyzed Logical Plan ==
s_store_name: string, sum(ss_net_profit): double
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#66 ASC NULLS FIRST], true
      +- Aggregate [s_store_name#66], [s_store_name#66, sum(ss_net_profit#32) AS sum(ss_net_profit)#141]
         +- Filter ((((ss_store_sk#17 = s_store_sk#61) AND (ss_sold_date_sk#10 = d_date_sk#33)) AND (d_qoy#43 = 2)) AND ((d_year#39 = 1998) AND (substr(s_zip#86, 1, 2) = substr(ca_zip#0, 1, 2))))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias spark_catalog.tpcds.store_sales
               :  :  :  +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
               :  :  +- SubqueryAlias spark_catalog.tpcds.date_dim
               :  :     +- Relation spark_catalog.tpcds.date_dim[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
               :  +- SubqueryAlias spark_catalog.tpcds.store
               :     +- Relation spark_catalog.tpcds.store[s_store_sk#61,s_store_id#62,s_rec_start_date#63,s_rec_end_date#64,s_closed_date_sk#65,s_store_name#66,s_number_employees#67,s_floor_space#68,s_hours#69,s_manager#70,s_market_id#71,s_geography_class#72,s_market_desc#73,s_market_manager#74,s_division_id#75,s_division_name#76,s_company_id#77,s_company_name#78,s_street_number#79,s_street_name#80,s_street_type#81,s_suite_number#82,s_city#83,s_county#84,... 5 more fields] parquet
               +- SubqueryAlias V1
                  +- Project [ca_zip#0]
                     +- SubqueryAlias A2
                        +- Intersect false
                           :- Project [substr(ca_zip#99, 1, 5) AS ca_zip#0]
                           :  +- Filter substr(ca_zip#99, 1, 5) IN (24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,54601,67897,22752,86284,18376,38607,45200,21756,29741,96765,23932,89360,29839,25989,28898,91068,72550,10390,18845,47770,82636,41367,76638,86198,81312,37126,39192,88424,72175,81426,53672,10445,42666,66864,66708,41248,48583,82276,18842,78890,49448,14089,38122,34425,79077,19849,43285,39861,66162,77610,13695,99543,83444,83041,12305,57665,68341,25003,57834,62878,49130,81096,18840,27700,23470,50412,21195,16021,76107,71954,68309,18119,98359,64544,10336,86379,27068,39736,98569,28915,24206,56529,57647,54917,42961,91110,63981,14922,36420,23006,67467,32754,30903,20260,31671,51798,72325,85816,68621,13955,36446,41766,68806,16725,15146,22744,35850,88086,51649,18270,52867,39972,96976,63792,11376,94898,13595,10516,90225,58943,39371,94945,28587,96576,57855,28488,26105,83933,25858,34322,44438,73171,30122,34102,22685,71256,78451,54364,13354,45375,40558,56458,28286,45266,47305,69399,83921,26233,11101,15371,69913,35942,15882,25631,24610,44165,99076,33786,70738,26653,14328,72305,62496,22152,10144,64147,48425,14663,21076,18799,30450,63089,81019,68893,24996,51200,51211,45692,92712,70466,79994,22437,25280,38935,71791,73134,56571,14060,19505,72425,56575,74351,68786,51650,20004,18383,76614,11634,18906,15765,41368,73241,76698,78567,97189,28545,76231,75691,22246,51061,90578,56691,68014,51103,94167,57047,14867,73520,15734,63435,25733,35474,24676,94627,53535,17879,15559,53268,59166,11928,59402,33282,45721,43933,68101,33515,36634,71286,19736,58058,55253,67473,41918,19515,36495,19430,22351,77191,91393,49156,50298,87501,18652,53179,18767,63193,23968,65164,68880,21286,72823,58470,67301,13394,31016,70372,67030,40604,24317,45748,39127,26065,77721,31029,31880,60576,24671,45549,13376,50016,33123,19769,22927,97789,46081,72151,15723,46136,51949,68100,96888,64528,14171,79777,28709,11489,25103,32213,78668,22245,15798,27156,37930,62971,21337,51622,67853,10567,38415,15455,58263,42029,60279,37125,56240,88190,50308,26859,64457,89091,82136,62377,36233,63837,58078,17043,30010,60099,28810,98025,29178,87343,73273,30469,64034,39516,86057,21309,90257,67875,40162,11356,73650,61810,72013,30431,22461,19512,13375,55307,30625,83849,68908,26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576)
                           :     +- SubqueryAlias spark_catalog.tpcds.customer_address
                           :        +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#90,ca_address_id#91,ca_street_number#92,ca_street_name#93,ca_street_type#94,ca_suite_number#95,ca_city#96,ca_county#97,ca_state#98,ca_zip#99,ca_country#100,ca_gmt_offset#101,ca_location_type#102] parquet
                           +- Project [ca_zip#1]
                              +- SubqueryAlias A1
                                 +- Filter (cnt#2L > cast(10 as bigint))
                                    +- Aggregate [ca_zip#130], [substr(ca_zip#130, 1, 5) AS ca_zip#1, count(1) AS cnt#2L]
                                       +- Filter ((ca_address_sk#121 = c_current_addr_sk#107) AND (c_preferred_cust_flag#113 = Y))
                                          +- Join Inner
                                             :- SubqueryAlias spark_catalog.tpcds.customer_address
                                             :  +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#121,ca_address_id#122,ca_street_number#123,ca_street_name#124,ca_street_type#125,ca_suite_number#126,ca_city#127,ca_county#128,ca_state#129,ca_zip#130,ca_country#131,ca_gmt_offset#132,ca_location_type#133] parquet
                                             +- SubqueryAlias spark_catalog.tpcds.customer
                                                +- Relation spark_catalog.tpcds.customer[c_customer_sk#103,c_customer_id#104,c_current_cdemo_sk#105,c_current_hdemo_sk#106,c_current_addr_sk#107,c_first_shipto_date_sk#108,c_first_sales_date_sk#109,c_salutation#110,c_first_name#111,c_last_name#112,c_preferred_cust_flag#113,c_birth_day#114,c_birth_month#115,c_birth_year#116,c_birth_country#117,c_login#118,c_email_address#119,c_last_review_date#120] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_store_name#66 ASC NULLS FIRST], true
      +- Aggregate [s_store_name#66], [s_store_name#66, sum(ss_net_profit#32) AS sum(ss_net_profit)#141]
         +- Project [ss_net_profit#32, s_store_name#66]
            +- Join Inner, (substr(s_zip#86, 1, 2) = substr(ca_zip#0, 1, 2))
               :- Project [ss_net_profit#32, s_store_name#66, s_zip#86]
               :  +- Join Inner, (ss_store_sk#17 = s_store_sk#61)
               :     :- Project [ss_store_sk#17, ss_net_profit#32]
               :     :  +- Join Inner, (ss_sold_date_sk#10 = d_date_sk#33)
               :     :     :- Project [ss_sold_date_sk#10, ss_store_sk#17, ss_net_profit#32]
               :     :     :  +- Filter (isnotnull(ss_sold_date_sk#10) AND isnotnull(ss_store_sk#17))
               :     :     :     +- Relation spark_catalog.tpcds.store_sales[ss_sold_date_sk#10,ss_sold_time_sk#11,ss_item_sk#12,ss_customer_sk#13,ss_cdemo_sk#14,ss_hdemo_sk#15,ss_addr_sk#16,ss_store_sk#17,ss_promo_sk#18,ss_ticket_number#19,ss_quantity#20,ss_wholesale_cost#21,ss_list_price#22,ss_sales_price#23,ss_ext_discount_amt#24,ss_ext_sales_price#25,ss_ext_wholesale_cost#26,ss_ext_list_price#27,ss_ext_tax#28,ss_coupon_amt#29,ss_net_paid#30,ss_net_paid_inc_tax#31,ss_net_profit#32] parquet
               :     :     +- Project [d_date_sk#33]
               :     :        +- Filter (((isnotnull(d_qoy#43) AND isnotnull(d_year#39)) AND ((d_qoy#43 = 2) AND (d_year#39 = 1998))) AND isnotnull(d_date_sk#33))
               :     :           +- Relation spark_catalog.tpcds.date_dim[d_date_sk#33,d_date_id#34,d_date#35,d_month_seq#36,d_week_seq#37,d_quarter_seq#38,d_year#39,d_dow#40,d_moy#41,d_dom#42,d_qoy#43,d_fy_year#44,d_fy_quarter_seq#45,d_fy_week_seq#46,d_day_name#47,d_quarter_name#48,d_holiday#49,d_weekend#50,d_following_holiday#51,d_first_dom#52,d_last_dom#53,d_same_day_ly#54,d_same_day_lq#55,d_current_day#56,... 4 more fields] parquet
               :     +- Project [s_store_sk#61, s_store_name#66, s_zip#86]
               :        +- Filter (isnotnull(s_store_sk#61) AND isnotnull(s_zip#86))
               :           +- Relation spark_catalog.tpcds.store[s_store_sk#61,s_store_id#62,s_rec_start_date#63,s_rec_end_date#64,s_closed_date_sk#65,s_store_name#66,s_number_employees#67,s_floor_space#68,s_hours#69,s_manager#70,s_market_id#71,s_geography_class#72,s_market_desc#73,s_market_manager#74,s_division_id#75,s_division_name#76,s_company_id#77,s_company_name#78,s_street_number#79,s_street_name#80,s_street_type#81,s_suite_number#82,s_city#83,s_county#84,... 5 more fields] parquet
               +- Aggregate [ca_zip#0], [ca_zip#0]
                  +- Join LeftSemi, (ca_zip#0 <=> ca_zip#1)
                     :- Project [substr(ca_zip#99, 1, 5) AS ca_zip#0]
                     :  +- Filter (substr(ca_zip#99, 1, 5) INSET 10144, 10336, 10390, 10445, 10516, 10567, 11101, 11356, 11376, 11489, 11634, 11928, 12305, 13354, 13375, 13376, 13394, 13595, 13695, 13955, 14060, 14089, 14171, 14328, 14663, 14867, 14922, 15126, 15146, 15371, 15455, 15559, 15723, 15734, 15765, 15798, 15882, 16021, 16725, 16807, 17043, 17183, 17871, 17879, 17920, 18119, 18270, 18376, 18383, 18426, 18652, 18767, 18799, 18840, 18842, 18845, 18906, 19430, 19505, 19512, 19515, 19736, 19769, 19849, 20004, 20260, 20548, 21076, 21195, 21286, 21309, 21337, 21756, 22152, 22245, 22246, 22351, 22437, 22461, 22685, 22744, 22752, 22927, 23006, 23470, 23932, 23968, 24128, 24206, 24317, 24610, 24671, 24676, 24996, 25003, 25103, 25280, 25486, 25631, 25733, 25782, 25858, 25989, 26065, 26105, 26231, 26233, 26653, 26689, 26859, 27068, 27156, 27385, 27700, 28286, 28488, 28545, 28577, 28587, 28709, 28810, 28898, 28915, 29178, 29741, 29839, 30010, 30122, 30431, 30450, 30469, 30625, 30903, 31016, 31029, 31387, 31671, 31880, 32213, 32754, 33123, 33282, 33515, 33786, 34102, 34322, 34425, 35258, 35458, 35474, 35576, 35850, 35942, 36233, 36420, 36446, 36495, 36634, 37125, 37126, 37930, 38122, 38193, 38415, 38607, 38935, 39127, 39192, 39371, 39516, 39736, 39861, 39972, 40081, 40162, 40558, 40604, 41248, 41367, 41368, 41766, 41918, 42029, 42666, 42961, 43285, 43848, 43933, 44165, 44438, 45200, 45266, 45375, 45549, 45692, 45721, 45748, 46081, 46136, 46820, 47305, 47537, 47770, 48033, 48425, 48583, 49130, 49156, 49448, 50016, 50298, 50308, 50412, 51061, 51103, 51200, 51211, 51622, 51649, 51650, 51798, 51949, 52867, 53179, 53268, 53535, 53672, 54364, 54601, 54917, 55253, 55307, 55565, 56240, 56458, 56529, 56571, 56575, 56616, 56691, 56910, 57047, 57647, 57665, 57834, 57855, 58048, 58058, 58078, 58263, 58470, 58943, 59166, 59402, 60099, 60279, 60576, 61265, 61547, 61810, 61860, 62377, 62496, 62878, 62971, 63089, 63193, 63435, 63792, 63837, 63981, 64034, 64147, 64457, 64528, 64544, 65084, 65164, 66162, 66708, 66864, 67030, 67301, 67467, 67473, 67853, 67875, 67897, 68014, 68100, 68101, 68309, 68341, 68621, 68786, 68806, 68880, 68893, 68908, 69035, 69399, 69913, 69952, 70372, 70466, 70738, 71256, 71286, 71791, 71954, 72013, 72151, 72175, 72305, 72325, 72425, 72550, 72823, 73134, 73171, 73241, 73273, 73520, 73650, 74351, 75691, 76107, 76231, 76232, 76614, 76638, 76698, 77191, 77556, 77610, 77721, 78451, 78567, 78668, 78890, 79077, 79777, 79994, 81019, 81096, 81312, 81426, 82136, 82276, 82636, 83041, 83144, 83444, 83849, 83921, 83926, 83933, 84093, 84935, 85816, 86057, 86198, 86284, 86379, 87343, 87501, 87816, 88086, 88190, 88424, 88885, 89091, 89360, 90225, 90257, 90578, 91068, 91110, 91137, 91393, 92712, 94167, 94627, 94898, 94945, 94983, 96451, 96576, 96765, 96888, 96976, 97189, 97789, 98025, 98235, 98294, 98359, 98569, 99076, 99543 AND isnotnull(substr(ca_zip#99, 1, 5)))
                     :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#90,ca_address_id#91,ca_street_number#92,ca_street_name#93,ca_street_type#94,ca_suite_number#95,ca_city#96,ca_county#97,ca_state#98,ca_zip#99,ca_country#100,ca_gmt_offset#101,ca_location_type#102] parquet
                     +- Project [ca_zip#1]
                        +- Filter (cnt#2L > 10)
                           +- Aggregate [ca_zip#130], [substr(ca_zip#130, 1, 5) AS ca_zip#1, count(1) AS cnt#2L]
                              +- Project [ca_zip#130]
                                 +- Join Inner, (ca_address_sk#121 = c_current_addr_sk#107)
                                    :- Project [ca_address_sk#121, ca_zip#130]
                                    :  +- Filter isnotnull(ca_address_sk#121)
                                    :     +- Relation spark_catalog.tpcds.customer_address[ca_address_sk#121,ca_address_id#122,ca_street_number#123,ca_street_name#124,ca_street_type#125,ca_suite_number#126,ca_city#127,ca_county#128,ca_state#129,ca_zip#130,ca_country#131,ca_gmt_offset#132,ca_location_type#133] parquet
                                    +- Project [c_current_addr_sk#107]
                                       +- Filter ((isnotnull(c_preferred_cust_flag#113) AND (c_preferred_cust_flag#113 = Y)) AND isnotnull(c_current_addr_sk#107))
                                          +- Relation spark_catalog.tpcds.customer[c_customer_sk#103,c_customer_id#104,c_current_cdemo_sk#105,c_current_hdemo_sk#106,c_current_addr_sk#107,c_first_shipto_date_sk#108,c_first_sales_date_sk#109,c_salutation#110,c_first_name#111,c_last_name#112,c_preferred_cust_flag#113,c_birth_day#114,c_birth_month#115,c_birth_year#116,c_birth_country#117,c_login#118,c_email_address#119,c_last_review_date#120] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- TakeOrderedAndProject(limit=100, orderBy=[s_store_name#66 ASC NULLS FIRST], output=[s_store_name#66,sum(ss_net_profit)#141])
   +- HashAggregate(keys=[s_store_name#66], functions=[sum(ss_net_profit#32)], output=[s_store_name#66, sum(ss_net_profit)#141])
      +- Exchange hashpartitioning(s_store_name#66, 200), ENSURE_REQUIREMENTS, [plan_id=173]
         +- HashAggregate(keys=[s_store_name#66], functions=[partial_sum(ss_net_profit#32)], output=[s_store_name#66, sum#143])
            +- Project [ss_net_profit#32, s_store_name#66]
               +- BroadcastHashJoin [substr(s_zip#86, 1, 2)], [substr(ca_zip#0, 1, 2)], Inner, BuildRight, false
                  :- Project [ss_net_profit#32, s_store_name#66, s_zip#86]
                  :  +- BroadcastHashJoin [ss_store_sk#17], [s_store_sk#61], Inner, BuildRight, false
                  :     :- Project [ss_store_sk#17, ss_net_profit#32]
                  :     :  +- BroadcastHashJoin [ss_sold_date_sk#10], [d_date_sk#33], Inner, BuildRight, false
                  :     :     :- Filter (isnotnull(ss_sold_date_sk#10) AND isnotnull(ss_store_sk#17))
                  :     :     :  +- FileScan parquet spark_catalog.tpcds.store_sales[ss_sold_date_sk#10,ss_store_sk#17,ss_net_profit#32] Batched: true, DataFilters: [isnotnull(ss_sold_date_sk#10), isnotnull(ss_store_sk#17)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_sold_date_sk), IsNotNull(ss_store_sk)], ReadSchema: struct<ss_sold_date_sk:int,ss_store_sk:int,ss_net_profit:double>
                  :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=140]
                  :     :        +- Project [d_date_sk#33]
                  :     :           +- Filter ((((isnotnull(d_qoy#43) AND isnotnull(d_year#39)) AND (d_qoy#43 = 2)) AND (d_year#39 = 1998)) AND isnotnull(d_date_sk#33))
                  :     :              +- FileScan parquet spark_catalog.tpcds.date_dim[d_date_sk#33,d_year#39,d_qoy#43] Batched: true, DataFilters: [isnotnull(d_qoy#43), isnotnull(d_year#39), (d_qoy#43 = 2), (d_year#39 = 1998), isnotnull(d_date_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(d_qoy), IsNotNull(d_year), EqualTo(d_qoy,2), EqualTo(d_year,1998), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:int,d_year:int,d_qoy:int>
                  :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=144]
                  :        +- Filter (isnotnull(s_store_sk#61) AND isnotnull(s_zip#86))
                  :           +- FileScan parquet spark_catalog.tpcds.store[s_store_sk#61,s_store_name#66,s_zip#86] Batched: true, DataFilters: [isnotnull(s_store_sk#61), isnotnull(s_zip#86)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk), IsNotNull(s_zip)], ReadSchema: struct<s_store_sk:int,s_store_name:string,s_zip:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(substr(input[0, string, true], 1, 2)),false), [plan_id=168]
                     +- HashAggregate(keys=[ca_zip#0], functions=[], output=[ca_zip#0])
                        +- Exchange hashpartitioning(ca_zip#0, 200), ENSURE_REQUIREMENTS, [plan_id=165]
                           +- HashAggregate(keys=[ca_zip#0], functions=[], output=[ca_zip#0])
                              +- SortMergeJoin [coalesce(ca_zip#0, ), isnull(ca_zip#0)], [coalesce(ca_zip#1, ), isnull(ca_zip#1)], LeftSemi
                                 :- Sort [coalesce(ca_zip#0, ) ASC NULLS FIRST, isnull(ca_zip#0) ASC NULLS FIRST], false, 0
                                 :  +- Exchange hashpartitioning(coalesce(ca_zip#0, ), isnull(ca_zip#0), 200), ENSURE_REQUIREMENTS, [plan_id=158]
                                 :     +- Project [substr(ca_zip#99, 1, 5) AS ca_zip#0]
                                 :        +- Filter (substr(ca_zip#99, 1, 5) INSET 10144, 10336, 10390, 10445, 10516, 10567, 11101, 11356, 11376, 11489, 11634, 11928, 12305, 13354, 13375, 13376, 13394, 13595, 13695, 13955, 14060, 14089, 14171, 14328, 14663, 14867, 14922, 15126, 15146, 15371, 15455, 15559, 15723, 15734, 15765, 15798, 15882, 16021, 16725, 16807, 17043, 17183, 17871, 17879, 17920, 18119, 18270, 18376, 18383, 18426, 18652, 18767, 18799, 18840, 18842, 18845, 18906, 19430, 19505, 19512, 19515, 19736, 19769, 19849, 20004, 20260, 20548, 21076, 21195, 21286, 21309, 21337, 21756, 22152, 22245, 22246, 22351, 22437, 22461, 22685, 22744, 22752, 22927, 23006, 23470, 23932, 23968, 24128, 24206, 24317, 24610, 24671, 24676, 24996, 25003, 25103, 25280, 25486, 25631, 25733, 25782, 25858, 25989, 26065, 26105, 26231, 26233, 26653, 26689, 26859, 27068, 27156, 27385, 27700, 28286, 28488, 28545, 28577, 28587, 28709, 28810, 28898, 28915, 29178, 29741, 29839, 30010, 30122, 30431, 30450, 30469, 30625, 30903, 31016, 31029, 31387, 31671, 31880, 32213, 32754, 33123, 33282, 33515, 33786, 34102, 34322, 34425, 35258, 35458, 35474, 35576, 35850, 35942, 36233, 36420, 36446, 36495, 36634, 37125, 37126, 37930, 38122, 38193, 38415, 38607, 38935, 39127, 39192, 39371, 39516, 39736, 39861, 39972, 40081, 40162, 40558, 40604, 41248, 41367, 41368, 41766, 41918, 42029, 42666, 42961, 43285, 43848, 43933, 44165, 44438, 45200, 45266, 45375, 45549, 45692, 45721, 45748, 46081, 46136, 46820, 47305, 47537, 47770, 48033, 48425, 48583, 49130, 49156, 49448, 50016, 50298, 50308, 50412, 51061, 51103, 51200, 51211, 51622, 51649, 51650, 51798, 51949, 52867, 53179, 53268, 53535, 53672, 54364, 54601, 54917, 55253, 55307, 55565, 56240, 56458, 56529, 56571, 56575, 56616, 56691, 56910, 57047, 57647, 57665, 57834, 57855, 58048, 58058, 58078, 58263, 58470, 58943, 59166, 59402, 60099, 60279, 60576, 61265, 61547, 61810, 61860, 62377, 62496, 62878, 62971, 63089, 63193, 63435, 63792, 63837, 63981, 64034, 64147, 64457, 64528, 64544, 65084, 65164, 66162, 66708, 66864, 67030, 67301, 67467, 67473, 67853, 67875, 67897, 68014, 68100, 68101, 68309, 68341, 68621, 68786, 68806, 68880, 68893, 68908, 69035, 69399, 69913, 69952, 70372, 70466, 70738, 71256, 71286, 71791, 71954, 72013, 72151, 72175, 72305, 72325, 72425, 72550, 72823, 73134, 73171, 73241, 73273, 73520, 73650, 74351, 75691, 76107, 76231, 76232, 76614, 76638, 76698, 77191, 77556, 77610, 77721, 78451, 78567, 78668, 78890, 79077, 79777, 79994, 81019, 81096, 81312, 81426, 82136, 82276, 82636, 83041, 83144, 83444, 83849, 83921, 83926, 83933, 84093, 84935, 85816, 86057, 86198, 86284, 86379, 87343, 87501, 87816, 88086, 88190, 88424, 88885, 89091, 89360, 90225, 90257, 90578, 91068, 91110, 91137, 91393, 92712, 94167, 94627, 94898, 94945, 94983, 96451, 96576, 96765, 96888, 96976, 97189, 97789, 98025, 98235, 98294, 98359, 98569, 99076, 99543 AND isnotnull(substr(ca_zip#99, 1, 5)))
                                 :           +- FileScan parquet spark_catalog.tpcds.customer_address[ca_zip#99] Batched: true, DataFilters: [substr(ca_zip#99, 1, 5) INSET 10144, 10336, 10390, 10445, 10516, 10567, 11101, 11356, 11376, 114..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ca_zip:string>
                                 +- Sort [coalesce(ca_zip#1, ) ASC NULLS FIRST, isnull(ca_zip#1) ASC NULLS FIRST], false, 0
                                    +- Exchange hashpartitioning(coalesce(ca_zip#1, ), isnull(ca_zip#1), 200), ENSURE_REQUIREMENTS, [plan_id=159]
                                       +- Project [ca_zip#1]
                                          +- Filter (cnt#2L > 10)
                                             +- HashAggregate(keys=[ca_zip#130], functions=[count(1)], output=[ca_zip#1, cnt#2L])
                                                +- Exchange hashpartitioning(ca_zip#130, 200), ENSURE_REQUIREMENTS, [plan_id=152]
                                                   +- HashAggregate(keys=[ca_zip#130], functions=[partial_count(1)], output=[ca_zip#130, count#145L])
                                                      +- Project [ca_zip#130]
                                                         +- BroadcastHashJoin [ca_address_sk#121], [c_current_addr_sk#107], Inner, BuildLeft, false
                                                            :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=147]
                                                            :  +- Filter isnotnull(ca_address_sk#121)
                                                            :     +- FileScan parquet spark_catalog.tpcds.customer_address[ca_address_sk#121,ca_zip#130] Batched: true, DataFilters: [isnotnull(ca_address_sk#121)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(ca_address_sk)], ReadSchema: struct<ca_address_sk:int,ca_zip:string>
                                                            +- Project [c_current_addr_sk#107]
                                                               +- Filter ((isnotnull(c_preferred_cust_flag#113) AND (c_preferred_cust_flag#113 = Y)) AND isnotnull(c_current_addr_sk#107))
                                                                  +- FileScan parquet spark_catalog.tpcds.customer[c_current_addr_sk#107,c_preferred_cust_flag#113] Batched: true, DataFilters: [isnotnull(c_preferred_cust_flag#113), (c_preferred_cust_flag#113 = Y), isnotnull(c_current_addr_..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/angela/Desktop/spark-3.5.5-bin-hadoop3/spark-warehouse/tpc..., PartitionFilters: [], PushedFilters: [IsNotNull(c_preferred_cust_flag), EqualTo(c_preferred_cust_flag,Y), IsNotNull(c_current_addr_sk)], ReadSchema: struct<c_current_addr_sk:int,c_preferred_cust_flag:string>

Time taken: 3.273 seconds, Fetched 1 row(s)
