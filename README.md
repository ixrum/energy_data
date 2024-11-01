# Energy Data Pipeline Project

## Overview

This project is focused on ingesting, transforming, and processing energy data from the Energy Charts API ([https://api.energy-charts.info/](https://api.energy-charts.info/)). The pipeline is implemented using Python for data ingestion, Apache Spark for data processing, and Apache Airflow for scheduling ETL tasks. The ingested data includes public power data, price data, and installed power data.

## Project Structure

- **src folder**: Contains three Python files for ingestion, each corresponding to a different type of data:

  - `public.power.py`: Ingests public power data.
  - `price_data.py`: Ingests price data.
  - `installed.py`: Ingests installed power data.

- **JSON Parsing**: After data ingestion, the data is parsed from JSON format for further processing.

- **Spark Setup**: Spark is used for data transformation. The project requires Java to be installed as Spark relies on it. All required dependencies are listed in the `requirements.txt` file.

- **Table Loading and Processing**: The processed data is loaded into tables for further analysis. Queries related to data insights are located in the `bi_queries` folder, which contains three SQL queries.

- **Airflow Setup**: Apache Airflow is used to schedule the ETL jobs for automating the ingestion, transformation, and loading processes.

## Requirements

- Python (version specified in `requirements.txt`)
- Apache Spark
- Java (for Spark)
- Apache Airflow
- Packages listed in `requirements.txt`

## How to Run the Project

1. Set up a virtual environment and install dependencies:

   ```sh
   python -m venv pipeline
   source pipeline/bin/activate  # On Windows, use `pipeline\Scripts\activate`
   pip install -r requirements.txt
   ```

2. Configure Apache Spark and ensure Java is installed.

3. Run the ingestion scripts in the `src` folder to ingest and parse the data.

4. Use Apache Spark to load the data into tables and execute the SQL queries in the `bi_queries` folder for analysis.

5. Use Apache Airflow to schedule and automate the ETL jobs.

## Folders

- `src/`: Python scripts for data ingestion.
- `bi_queries/`: Contains SQL queries for data analysis.
- `requirements.txt`: Lists all dependencies required for the project.

## Notes

- Make sure Java is properly installed and configured, as Apache Spark requires it.
- Airflow needs to be properly set up for scheduling the ETL jobs.

## Download
To download this project, clone the repository using the following command:

```sh
git clone https://github.com/ixrum/energy_data.git
```