# Spark Performance Benchmark

## Introduction
This project aims to evaluate the performance of Apache Spark by running various benchmarks to analyze its performance under different scenarios.

## Installation
1. Clone this repository:
    ```bash
    git clone https://github.com/yourusername/spark-performance-benchmark.git
    ```
2. Navigate to the project directory:
    ```bash
    cd spark-performance-benchmark
    ```
3. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage
We provide multiple test, each test has its own set up procedure and usage. Please follow the guidence below.

### Performance Test
1. Follow the guidence in `performance\spark-tpc-ds-performance-test\README.md` to set up the submoudle.
2. Move the spark config file to enable metrics collection.
```bash
cd performance/
mv metrics.properties ${SPARK_HOME}/conf/
```

3. Follow the instruction in `performance\spark-tpc-ds-performance-test\README.md` to run the test.
```bash
cd spark-tpc-ds-performance-test
bin/tpcdsspark.sh 
```

4. Check the metrics files exist.

## Contributing
We welcome issues and pull requests to contribute to this project. Please make sure to read the [contributing guidelines](CONTRIBUTING.md) before submitting.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.