# End-To-End-NYC-Yellow-Taxi-Rides-Analytics-Azure-DE-Project
This project showcases an end-to-end data engineering pipeline focusing on NYC Yellow Taxi Rides past 3 months data. The project follows a multi-phase process initiation with ingesting the data, transforming and loading it, then modeling data for analysis. Final stages focus on insightful visualizations and report generation in Power BI.

# Project Overview:
This project delves into a comprehensive analysis of NYC Yellow Taxi rides based on three months' worth of data. Leveraging Azure Data Lake Storage Gen2 and Azure Databricks, the project seamlessly transforms Parquet data into CSV format using PySpark. The ETL pipeline involves executing PySpark scripts, merging CSV files into a consolidated dataset using Azure Data Factory, and further refining the data with PySpark to create essential dimensions and fact tables.

The analytical layer is constructed within Azure SQL Database, providing a structured foundation for drawing insightful conclusions. The entire process is orchestrated to uncover patterns, trends, and key metrics in NYC Yellow Taxi rides. The project culminates with the integration of Azure SQL Database into Power BI for visually compelling and interactive dashboards.

# Tools & Technologies
* Cloud - [Azure](https://azure.microsoft.com/en-us/free/search/?ef_id=_k_CjwKCAiA-P-rBhBEEiwAQEXhH2TmMccfTpk3ywwIKt4994lTYzscRnt3KKI0KDpwpeHu9OLhb7d8pBoCvckQAvD_BwE_k_&OCID=AIDcmm5edswduu_SEM__k_CjwKCAiA-P-rBhBEEiwAQEXhH2TmMccfTpk3ywwIKt4994lTYzscRnt3KKI0KDpwpeHu9OLhb7d8pBoCvckQAvD_BwE_k_&gad_source=1&gclid=CjwKCAiA-P-rBhBEEiwAQEXhH2TmMccfTpk3ywwIKt4994lTYzscRnt3KKI0KDpwpeHu9OLhb7d8pBoCvckQAvD_BwE)
* Data Storage - [Azure Data Lake Gen2](https://azure.microsoft.com/en-us/products/data-lake-analytics/?ef_id=_k_CjwKCAiA-P-rBhBEEiwAQEXhH0YMq88gk--X9G24_yjpUp_hhyBqgLcx8zHwAcGCaC_BykmHOied0RoCppkQAvD_BwE_k_&OCID=AIDcmm5edswduu_SEM__k_CjwKCAiA-P-rBhBEEiwAQEXhH0YMq88gk--X9G24_yjpUp_hhyBqgLcx8zHwAcGCaC_BykmHOied0RoCppkQAvD_BwE_k_&gad_source=1&gclid=CjwKCAiA-P-rBhBEEiwAQEXhH0YMq88gk--X9G24_yjpUp_hhyBqgLcx8zHwAcGCaC_BykmHOied0RoCppkQAvD_BwE)
* Data Orchestration - [Azure Data Factory](https://azure.microsoft.com/en-us/products/data-factory/?ef_id=_k_CjwKCAiA-P-rBhBEEiwAQEXhH24gZ1zmnilx1TrNMAgwpfVtz0jrCgfihBVkKLjaedrmGJGesKt9lxoCzoUQAvD_BwE_k_&OCID=AIDcmm5edswduu_SEM__k_CjwKCAiA-P-rBhBEEiwAQEXhH24gZ1zmnilx1TrNMAgwpfVtz0jrCgfihBVkKLjaedrmGJGesKt9lxoCzoUQAvD_BwE_k_&gad_source=1&gclid=CjwKCAiA-P-rBhBEEiwAQEXhH24gZ1zmnilx1TrNMAgwpfVtz0jrCgfihBVkKLjaedrmGJGesKt9lxoCzoUQAvD_BwE)
* Data Ingestion,Processing - [Azure Databricks](https://azure.microsoft.com/en-us/free/databricks/search/?ef_id=_k_CjwKCAiA-P-rBhBEEiwAQEXhHz3LhiDccUVaLKr9gCSCn3RAWPG7qZ8Eqy6c_-81FDhFCQbFTYFeQxoCM1MQAvD_BwE_k_&OCID=AIDcmm5edswduu_SEM__k_CjwKCAiA-P-rBhBEEiwAQEXhHz3LhiDccUVaLKr9gCSCn3RAWPG7qZ8Eqy6c_-81FDhFCQbFTYFeQxoCM1MQAvD_BwE_k_&gad_source=1&gclid=CjwKCAiA-P-rBhBEEiwAQEXhHz3LhiDccUVaLKr9gCSCn3RAWPG7qZ8Eqy6c_-81FDhFCQbFTYFeQxoCM1MQAvD_BwE)
* Data Modelling - [Databricks Pyspark](https://www.databricks.com/glossary/pyspark)
* Database - [Azure SQL Database](https://azure.microsoft.com/en-us/products/azure-sql/database/?ef_id=_k_CjwKCAiA-P-rBhBEEiwAQEXhH8HWOS7vMF4bASoTYCEvieKGnyW-NMo8mzkMuEn68gdl-_IiSQ3HHRoC4kUQAvD_BwE_k_&OCID=AIDcmm5edswduu_SEM__k_CjwKCAiA-P-rBhBEEiwAQEXhH8HWOS7vMF4bASoTYCEvieKGnyW-NMo8mzkMuEn68gdl-_IiSQ3HHRoC4kUQAvD_BwE_k_&gad_source=1&gclid=CjwKCAiA-P-rBhBEEiwAQEXhH8HWOS7vMF4bASoTYCEvieKGnyW-NMo8mzkMuEn68gdl-_IiSQ3HHRoC4kUQAvD_BwE)
* Data Visualization - [Power BI](https://powerbi.microsoft.com)
* Programming Language - [SQL](https://learn.microsoft.com/en-us/azure-data-studio/tutorial-sql-editor),[PySpark](https://spark.apache.org/docs/latest/api/python/index.html)

# Architecture
![Flowchart Template (1)](https://github.com/CharanTejaV/End-To-End-NYC-Yellow-Taxi-Rides-Analytics-Azure-DE-Project/assets/143735053/6121f460-39c5-43c3-a29b-0cfa7ad3add9)

# Dataset used

The dataset utilized in this project is the TLC Trip Record Data, sourced from the NYC Taxi and Limousine Commission (TLC) website. The data is comprised of Yellow taxi trip records, encompassing essential fields such as pick-up and drop-off dates/times, locations, distances, fares, rate types, payment types, and driver-reported passenger counts. 
**Note:** Only three months of data have been considered for this project, focusing specifically on the months of July, August, and September. The repository contains these selected datasets, providing a snapshot for analysis and visualization within the outlined scope of the project.

[Data Source](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | [Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

# Step-by-Step Workflow

## Step 1: Data Acquisition and Preparation
* Download NYC Yellow Taxi Rides dataset and related files (data dictionary, parquet files, shape files, database files) from the official website (Duration: July, August, September)
* Load the acquired data into Azure Data Lake Gen 2 containers for further processing.
![image](https://github.com/CharanTejaV/End-To-End-NYC-Yellow-Taxi-Rides-Analytics-Azure-DE-Project/assets/143735053/69e27f23-2988-4c00-87f8-e2e7d3482bd9)

## Step 2: Data Processing Azure Databricks Notebook (Pyspark)
* Launch Azure Databricks, create Compute to initiate the process of creating a new cluster for running PySpark notebooks and establish a connection to Azure Data Lake Gen 2.
* Create a Python notebook using PySpark to convert Parquet files into CSV format and store them back in Data Lake Gen2.

![image](https://github.com/CharanTejaV/End-To-End-NYC-Yellow-Taxi-Rides-Analytics-Azure-DE-Project/assets/143735053/0d0eac59-4a9c-403e-b507-e3ea4a34501e)

## Step 3: Pipeline Orchestration with Azure Data Factory
* Launch Azure Data Factory and design two key pipelines:
    -Databricks pipeline: Execute the Python script in Databricks for Parquet to CSV conversion.
    -Copy data transformation: Merge all CSV files into a consolidated CSV file for efficient storage.

![image](https://github.com/CharanTejaV/End-To-End-NYC-Yellow-Taxi-Rides-Analytics-Azure-DE-Project/assets/143735053/b28ce5dc-abcc-44f7-a7a1-5092de4f1abd)

## Step 4: Data Cleaning and Modeling
  * Utilize Azure Databricks Python Notebook with PySpark for thorough data cleaning and perform data modeling,load lookup files to create Dimension and Fact tables, ensuring a structured and optimized dataset.
  * Convert shapefiles (.shx) and database files (.dbf) into CSV format. These CSV files will be used as lookup files in the table creation process.
 
## Step 5: Database Creation and Data Insertion
  * Establish an Azure SQL Database to store the cleaned data of Dimension and Fact tables.

## Step 6: Analytical Layer Development
  * Create an Analytical Layer by selecting only the required columns and joining Fact and Dimension tables.
  * Ensure the analytical layer provides a solid foundation for insightful analysis.
![image](https://github.com/CharanTejaV/End-To-End-NYC-Yellow-Taxi-Rides-Analytics-Azure-DE-Project/assets/143735053/95e05864-57ad-45c2-a19b-bf81fa6e623e)

## Step 7: Power BI Reporting
  * Connect to the Azure SQL Database and create detailed and visually appealing reports for effective data representation.
[Power BI Report]()
![image](https://github.com/CharanTejaV/End-To-End-NYC-Yellow-Taxi-Rides-Analytics-Azure-DE-Project/assets/143735053/e69868c2-704b-4be2-8e86-aeafb84c5bc4)

![image](https://github.com/CharanTejaV/End-To-End-NYC-Yellow-Taxi-Rides-Analytics-Azure-DE-Project/assets/143735053/69bcde37-c8a4-4e0c-97d9-decbbad7d878)

![image](https://github.com/CharanTejaV/End-To-End-NYC-Yellow-Taxi-Rides-Analytics-Azure-DE-Project/assets/143735053/e74dd51f-7532-4548-9325-c62a37ff9be0)

## Step 8: Stakeholder Engagement and Insights
  * Upon completion of the Power BI report, publish it to clients/stakeholders for review.
  * Draw key insights and answer pertinent questions derived from the processed data.

# Conclusion
In this intricate journey through the NYC Yellow Taxi Analytics project, we embarked on a transformative data engineering expedition. This comprehensive end-to-end data engineering project encompasses data acquisition, transformation, cleaning, modeling, storage, analysis, and reporting, culminating in the generation of meaningful insights and actionable reports. It is not just a data engineering endeavor; it is a narrative of extracting meaning from raw information. The journey concludes not just with reports but with a newfound understanding, derived from the amalgamation of data, technology, and thoughtful analysis.

#THANK YOU
