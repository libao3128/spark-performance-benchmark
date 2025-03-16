== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Project ['d_date, 'make_date(2022, 'd_moy, 'd_dom) AS new_date_format#0, 'timestamp_millis('unix_timestamp()) AS ts_millis#1]
      +- 'UnresolvedRelation `date_dim`

== Analyzed Logical Plan ==
org.apache.spark.sql.AnalysisException: Undefined function: 'make_date'. This function is neither a registered temporary function nor a permanent function registered in the database 'tpcds'.; line 2 pos 7
org.apache.spark.sql.AnalysisException: Undefined function: 'make_date'. This function is neither a registered temporary function nor a permanent function registered in the database 'tpcds'.; line 2 pos 7
== Optimized Logical Plan ==
org.apache.spark.sql.AnalysisException: Undefined function: 'make_date'. This function is neither a registered temporary function nor a permanent function registered in the database 'tpcds'.; line 2 pos 7
== Physical Plan ==
org.apache.spark.sql.AnalysisException: Undefined function: 'make_date'. This function is neither a registered temporary function nor a permanent function registered in the database 'tpcds'.; line 2 pos 7
Time taken: 1.902 seconds, Fetched 1 row(s)
