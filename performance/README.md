# Core Engine and Performance Test
This test aims to measure the performance differences across different versions of Spark. In this repo, we took Spark 2.4.8 and Spark 3.5.5 as examples. By leveraging the existing benchmark [spark-tpc-ds-performance-test](https://github.com/IBM/spark-tpc-ds-performance-test.git), we run TPC-DS on Spark and collect the metrics files. We focus on execution time, memory usage, and stage analysis.

## Setup
### Install Spark
First, install prebuilt Spark from the website [https://archive.apache.org/dist/spark/](https://archive.apache.org/dist/spark/).

### Setup Environment Variable
Edit `performance\spark-tpc-ds-performance-test\bin\tpcdsenv.sh` to set up the environment variables.

### Include config file
Move the metrics config file to your Spark home, this will enable the metrics collection.

```
cp performance/metrics.properties  ${SPARK_HOME}/conf/
```

## Usage
### Create Table and Run the Test

Run the test script, the benchmark will collect the metrics files automatically.

```
 $ cd spark-tpc-ds-performance-test
 $ bin/tpcdsspark.sh 

==============================================
TPC-DS On Spark Menu
----------------------------------------------
SETUP
 (1) Create spark tables
RUN
 (2) Run a subset of TPC-DS queries
 (3) Run All (99) TPC-DS Queries
CLEANUP
 (4) Cleanup
 (Q) Quit
----------------------------------------------
Please enter your choice followed by [ENTER]: 
```

1. Enter 1 to create tables.
2. Enter 3 to run the complete test.

### Analyze the Report
Rerun `performance\performance_analysis.ipynb`, and the report will be generated.