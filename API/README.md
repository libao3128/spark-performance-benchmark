# Apache Spark API Benchmarking

## Overview
This repository contains Jupyter notebooks designed to evaluate and compare the API enhancements between Apache Spark 2.4.8 and Apache Spark 3.0.2. The tests focus on performance benchmarks for DataFrame operations, transformations, and User-Defined Functions (UDFs). The repository also includes conda environment configurations to facilitate easy setup and execution.

## File Structure
```
API/
│── SparkAPI/
│   ├── Spark2API.ipynb  # Benchmarking Spark 2 API
│   ├── Spark3API.ipynb  # Benchmarking Spark 3 API
│
│── environment/
│   ├── spark2_env.yml  # Conda environment for Spark 2
│   ├── spark3_env.yml  # Conda environment for Spark 3
```

## Setup Instructions
To run the Jupyter notebooks, you need to set up  two separate environments for Spark 2.x and Spark 3.x.

### 1. Install Conda (if not installed)
If you don’t have Conda installed, download and install it from [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/).

### 2. Create and Activate the Spark 2 Environment
```bash
conda env create -f environment/spark2_env.yml
conda activate spark2_new
```
If PySpark is not installed, install it manually:
```bash
pip install pyspark==2.4.8
```
Run `Spark2API.ipynb` in Jupyter Notebook:
```bash
jupyter notebook
```
Then, open `Spark2API.ipynb` in the Jupyter interface.

### 3. Create and Activate the Spark 3 Environment
```bash
conda env create -f environment/spark3_env.yml
conda activate spark3_new
```
If PySpark is not installed, install it manually:
```bash
pip install pyspark==3.0.2
```
Run `Spark3API.ipynb` in Jupyter Notebook:
```bash
jupyter notebook
```
Then, open `Spark3API.ipynb` in the Jupyter interface.

## Contents of the Notebooks

### `Spark2API.ipynb`
- Benchmarks API performance on Spark 2.4.8.
- Tests DataFrame operations, transformations, and UDFs.
- Provides execution time comparisons for core Spark 2 operations.

### `Spark3API.ipynb`
- Benchmarks API performance on Spark 3.0.2.
- Tests improvements such as Adaptive Query Execution (AQE) and Pandas UDFs.
- Compares execution times with Spark 2 to analyze performance gains and trade-offs.

## Dependencies
The conda environment files include all necessary dependencies:
- **Python 3.8**
- **PySpark (2.4.8 or 3.0.2 depending on the environment)**
- **Findspark** (for configuring Spark in Jupyter)
- **Pandas & Matplotlib** (for data handling and visualization)
- **SparkMeasure** (for tracking execution metrics)

## Notes
- Make sure to activate the correct environment (`spark2_new` or `spark3_new`) before running the corresponding notebook.
- Some transformations in Spark 3 may require additional memory, so adjust your system configurations if necessary.

## Contributing
Feel free to fork the repository and contribute improvements or new benchmarks.

## License
This project is open-source and available under the MIT License.

