# ETL Process Documentation

## Overview
This document explains the ETL (Extract, Transform, Load) process for user interaction data, which is ingested from a parquet file, cleaned, transformed, and loaded into a SQLite database.

## Steps

### 1. Data Ingestion
- Reads data from `interaction_data.csv`.

### 2. Data Cleaning
- Handles missing values by removing rows with any missing data.
- Converts `timestamp` column to datetime format.

### 3. Data Transformation
- Calculates the number of interactions per user and product.
- Adds `interaction_count_user` and `interaction_count_user_product` columns.

### 4. Data Loading
- Loads the cleaned and transformed data into a SQLite database (`interaction_data.db`).

## Running the Script
1. Ensure `pandas` and `sqlite3` are installed:
    ```sh
    pip install pandas sqlite3
    ```
2. Place `interaction_data.csv` in the same directory as the script.
3. Run the Jupyter Notebook cells in order.

## Dependencies
- pandas
- sqlite3

## Data Structure

### interaction_data.csv
- `interaction_id`: Unique ID for each interaction
- `user_id`: Unique ID for each user
- `product_id`: Unique ID for each product
- `action`: Type of action performed by the user
- `timestamp`: Timestamp of the interaction

### SQLite Database Table
- `interaction_id`: Unique ID for each interaction
- `user_id`: Unique ID for each user
- `product_id`: Unique ID for each product
- `action`: Type of action performed by the user
- `timestamp`: Timestamp of the interaction
- `interaction_count_user`: Number of interactions per user
- `interaction_count_user_product`: Number of interactions per user per product
