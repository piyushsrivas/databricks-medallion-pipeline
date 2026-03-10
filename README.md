# Databricks Medallion Architecture Data Pipeline

This project implements a **Medallion Architecture data pipeline (Bronze → Silver → Gold)** using **Databricks, PySpark, Delta Lake, and Auto Loader**.

The pipeline ingests raw CSV datasets using **Databricks Auto Loader**, processes them through transformation layers, and stores curated datasets for analytics.

---

## Architecture

![Architecture](architecture/architecture.png)

### Medallion Layers

**Bronze Layer**

* Raw data ingestion using **Databricks Auto Loader**
* Incremental file ingestion from source storage
* Data stored as **Delta Lake tables**
* Pipeline triggered in **batch mode using trigger(once=True)**

**Silver Layer**

* Data cleaning and transformation using **PySpark**
* Schema enforcement and data type conversions
* Data quality validation

**Gold Layer**

* Business-level aggregated datasets
* Intended for analytics and reporting
* Included as a future enhancement in this project

---

## Tech Stack

* Databricks
* PySpark
* Delta Lake
* Databricks Auto Loader
* Medallion Architecture

---

## Project Structure

```id="xag56k"
databricks-medallion-pipeline
│
├── README.md
├── architecture
│   └── architecture.png
│
├── notebooks
│   ├── 01_Setup.py
│   ├── 02_SrcParametres.py
│   ├── 03_BronzeLayer.py
│   ├── 04_SilverNotebook.py
│   ├── 05_dltPipeline.py
│   └── 06_GOLD_DIMS.py
```

---

## Pipeline Execution Order

1. **Setup.py**

   * Creates storage volumes and schemas.

2. **SrcParametres.py**

   * Defines input datasets for ingestion.

3. **BronzeLayer.py**

   * Ingests raw CSV files using **Databricks Auto Loader**.

4. **SilverNotebook.py**

   * Performs data transformation and cleaning.

5. **dltPipeline.py**

   * Builds structured tables using **Delta Live Tables**.

6. **GOLD_DIMS.py**

   * Example Gold layer query for analytics.

---

## Features

* Auto Loader incremental ingestion
* Batch-triggered streaming ingestion
* Delta Lake storage
* Medallion Architecture pipeline
* Data transformation with PySpark

---

## Future Improvements

* Implement complete **Gold layer analytical tables**
* Add orchestration workflows
* Build dashboards on curated datasets
