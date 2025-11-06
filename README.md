
# Data Engineering Challenge | Sales ETL Pipeline

A scalable PySpark-based ETL pipeline implementing a medallion architecture (Bronze → Silver → Gold) for processing sales data.

## 🏗️ Architecture

## 📋 Features

- **Bronze Layer**: Raw data ingestion with schema inference and metadata tracking
- **Silver Layer**: Data cleaning, standardization, and quality checks
- **Gold Layer**: Business-focused datasets (Sales & Customer analytics)
- **Incremental Loading**: Merge strategy with deduplication
- **Data Quality Checks**: Validation at each layer
- **Partitioning**: Optimized storage with date-based partitioning
- **Logging**: Comprehensive logging for monitoring and debugging

## 🚀 Quick Start


### Prerequisites

This project uses **PySpark 3.5.1**, which supports the following Python versions:

- **Python 3.9 to 3.12 (✅ 3.10 Recommended)**

> **Note:** Python **3.8 and below** are **not supported** by PySpark 3.5.x.
- Java 11/17 (required for PySpark)

### Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd <repo-name>
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Add your sales data**
   - Place your CSV file in `data/raw/` path
   - Or use the provided sample data

## Running the Pipeline

### Run the full ETL pipeline
```bash
python main.py
 ```

## 🔧 Configuration

Edit `config/config.yaml` to customize:
- Spark application settings
- Data paths
- Logging levels

## 📊 Data Quality Checks

Each layer includes automated quality checks:

- **Bronze**: Duplicate detection, missing values, date consistency
- **Silver**: Type validation, standardization verification
- **Gold**: Business rule validation, aggregation checks

## 🐛 Debugging

Use the debug utility to inspect any layer after running the pipeline:

```bash
# View Bronze data
python debug_main.py --layer bronze --rows 50
 ```

```bash
# View Silver data
python debug_main.py --layer silver
 ```

```bash
# View Gold sales
python debug_main.py --layer gold_sales
 ```

## 📤 Exporting Data to CSV

The pipeline stores data in Parquet format for optimal performance, but you may need CSV exports for analysis or reporting. Use the `retrieve_tabular_data.py` utility to convert any layer to CSV:

```bash
python retrieve_tabular_data.py
```

This script will automatically export all layers:
- **Bronze Layer** → `exports/bronze_data.csv`
- **Silver Layer** → `exports/silver_data.csv`
- **Gold Sales** → `exports/gold_sales.csv`
- **Gold Customers** → `exports/gold_customers.csv`

### Features:
- **Single CSV Output**: Consolidates partitioned Parquet files into one CSV
- **Data Preview**: Shows first 10 rows in console during export
- **Statistics**: Displays row/column counts and file size
- **Auto-directory Creation**: Creates export folders automatically


## 📝 Logging

Logs are stored in `logs/` directory with timestamps:
- Console output (INFO level)
- File output (detailed logs with DEBUG level)


## (Part 2 - AWS Design) 
Regarding the second part of the assignment, you can find the AWS Diagram attached (aws_diagram.png)





