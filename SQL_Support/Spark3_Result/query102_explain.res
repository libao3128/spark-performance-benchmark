Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark Web UI available at http://mac.lan:4040
Spark master: local[*], Application Id: local-1741583555966
== Parsed Logical Plan ==
ExplainCommand 'Project ['i_item_sk, 'w_warehouse_sk], ExtendedMode

== Analyzed Logical Plan ==
plan: string
ExplainCommand 'Project ['i_item_sk, 'w_warehouse_sk], ExtendedMode

== Optimized Logical Plan ==
ExplainCommand 'Project ['i_item_sk, 'w_warehouse_sk], ExtendedMode

== Physical Plan ==
Execute ExplainCommand
   +- ExplainCommand 'Project ['i_item_sk, 'w_warehouse_sk], ExtendedMode

Time taken: 0.402 seconds, Fetched 1 row(s)
