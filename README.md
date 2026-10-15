# Data Engineering Project (Azure-ADF-Databricks-PowerBI)
This project implements an end-to-end data engineering pipeline using Microsoft Azure services and the AdventureWorks dataset sourced from Kaggle.

The project demonstrates how raw CSV files are ingested from GitHub into Azure Data Lake Storage Gen2, processed using Azure Databricks and PySpark, and loaded into a Silver layer using Azure Data Factory. The transformed data is then connected to Power BI to create visualizations and gain meaningful business insights.

🏗️ Project Architecture

Kaggle AdventureWorks Dataset → GitHub → Azure Data Factory → Azure Data Lake Storage Gen2 (Bronze) → Azure Databricks (ETL) → Azure Data Factory → Silver Layer → Power BI Dashboard

🛠️ Technologies Used

Azure Data Factory (ADF): Created and orchestrated pipelines to ingest data from GitHub and move transformed data into the Silver layer.

Azure Data Lake Storage Gen2 (ADLS Gen2): Stored raw and processed CSV files in Bronze and Silver layers.

Azure Databricks: Performed ETL operations and data transformations using PySpark.

Python / PySpark: Used for data processing and transformation.

Power BI: Connected to the Silver layer to build visualizations and explore business data.

GitHub: Used to access source CSV files and maintain project documentation.

🔄 Project Workflow

1. Data Collection

Downloaded the AdventureWorks dataset from Kaggle.

Prepared the CSV files and uploaded them to a GitHub repository as the source for data ingestion.

2. Data Ingestion Using Azure Data Factory

Created Azure Data Factory pipelines to fetch CSV files from GitHub.

Configured the pipelines to load the raw data into Azure Data Lake Storage Gen2.

Stored the ingested data in the Bronze layer.

3. ETL Processing Using Azure Databricks

Read CSV files from the Bronze layer into Azure Databricks.

Performed ETL operations using PySpark.

Transformed the raw data into a structured format for downstream analytics.

4. Loading Data into the Silver Layer

Used Azure Data Factory pipelines to load the transformed files into the Silver layer of Azure Data Lake Storage Gen2.

Organized the processed data for analytical consumption.

5. Data Visualization Using Power BI

Connected Power BI to the Silver layer.

Imported the processed data for analysis.

Created visualizations to explore the AdventureWorks data and present business insights.
<img width="960" height="534" alt="Screenshot 2026-10-15 123819" src="https://github.com/user-attachments/assets/ed896a4f-2861-4215-a0ed-3f67d57aa16a" />

🥉 Medallion Architecture

This project uses a Bronze and Silver layer approach to organize data processing.

Bronze Layer - Stores raw CSV files ingested from GitHub.
Silver layer - Stores transformed and processed data ready for analysis.

🎯 Key Learning Outcomes

Built an end-to-end cloud data pipeline using Azure services.

Implemented data ingestion and pipeline orchestration using Azure Data Factory.

Worked with Bronze and Silver layers in Azure Data Lake Storage Gen2.

Performed ETL transformations using Azure Databricks and PySpark.

Integrated processed cloud data with Power BI.

Developed practical experience in cloud-based data engineering and analytics.

🚀 Future Enhancements

Implement incremental data loading.

Add data quality checks and validation.

Configure pipeline monitoring and error handling.

Optimize data processing and pipeline performance.

Implement CI/CD for automated deployment.
