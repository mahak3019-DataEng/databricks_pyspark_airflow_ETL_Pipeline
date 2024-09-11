# Apache Spark End-to-End Data Engineering Project Using Databricks

## Overview

This project demonstrates a comprehensive end-to-end data engineering pipeline using Apache Spark and Databricks. The pipeline is designed to efficiently handle ETL operations and integrate with Apache Airflow for workflow automation.

## Key Features

1. **Databricks Integration**:
   - Utilized Databricks to manage and process data.

2. **ETL Pipeline**:
   - Developed an ETL pipeline adhering to company standards.
   - Implemented using classes and definitions to ensure modularity and reusability.

3. **Design Patterns**:
   - Implemented a low-level design pattern using the Factory pattern.
   - Created `ReadFactory` and `LoadFactory` classes to handle different data formats and transformations.

4. **PySpark Transformations**:
   - Applied various PySpark transformations including `join`, `collect_set`, and `collect_list` to manipulate and aggregate data.

5. **Automated Transformations**:
   - Automated the execution of all data transformation functions in the final class.

6. **Apache Airflow Docker Image**:
   - Created a custom Docker image for Apache Airflow.
   - Configured `airflow.providers.databricks` to manage Databricks connections and tasks.

7. **Workflow Automation**:
   - Fully automated the data processing workflow using Apache Airflow.
   - Integrated ETL pipeline and transformation steps into Airflow DAGs for seamless execution.

## Project Structure

- `dags/`:
  - Contains Airflow DAGs defining the workflow and tasks.

- `plugins/`:
  - Includes custom Airflow plugins if any.


- `src/`:
  - Source code for the ETL pipeline, including `LoadFactory` and `TransformFactory` implementations.
