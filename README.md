# E-Commerce Data Layer - Big Data Analysis

This project focuses on analyzing large datasets related to e-commerce products and sales logs. Leveraging Big Data technologies and a structured data pipeline based on the Medallion Architecture (Bronze, Silver, Gold layers), the project ingests raw data, enhances its quality through cleaning and enrichment processes, and transforms it into a refined format ready for advanced analysis and business intelligence.

## Architecture
### Overview

The system processes batch data. E-commerce product and sales data is periodically extracted and placed into the Raw Data Zone. It is then batch processed through the Transformation Zone, where data cleansing, normalization, and enrichment occur. Finally, the refined data is stored in the Curated Zone, making it ready for advanced analytics and data-driven decision-making.

From the Curated Zone, the cleaned and structured data is used to build analysis dashboards.

The entire system is containerized and managed by an orchestration tool.

<p align="center">
  <img alt="Untitled Diagram drawio (12)" src="https://github.com/user-attachments/assets/a81dc310-a0c1-418c-bd26-b75ecc302ae2" />

</p>

### Tools and Components

**Batch Data Source**: 
- Python Data Generator


**Extraction Tool**: 
- Apache NiFi  


**Data Zones**:
- **Raw Zone (Bronze)**: 
  - HDFS (Hadoop Distributed File System)

- **Transformation Zone (Silver)**: 
  - HDFS (Hadoop Distributed File System)
  - Apache Hive

- **Curated Zone (Gold)**: 
  - PostgreSQL  

 
**Batch Processing**: 
- Apache Spark


**Dashboards**: 
- Metabase


**Containerization**:
- Docker
- Docker Compose


**Orchestration**:
- Jenkins

<p align="center">
  <img alt="Untitled Diagram drawio (14)" src="https://github.com/user-attachments/assets/214f0c9f-6df8-4a50-8655-800a7e90d518" />
</p>

## Analytical Questions
### Batch Processing Questions
  1. Who are the top buyers globally?
  2. Who are the top buyers by country?
  3. Who are the top buyers country region?
  4. What percentage of buyers have made purchases from multiple countries?
  5. What percentage of buyers have made purchases from multiple country regions?
  6. What does the global spending distribution look like across countries?
  7. How do countries rank globally based on total money spent?
  8. What percentage of total national spending comes from each region?
  9. What is the distribution of the number of orders per user?
  10. What is the distribution of the number of items per order?
  11. What are the trends in product prices over time?
  12. What are the percentage changes in product prices over time?
  13. How does user activity vary by hour of the day?
  14. What are the trends in historical user activity over time?
  15. What is the total amount of money spent by each user over their entire purchase history?
  16. What is the total amount of money spent by each user per date?
  17. What is the total number of articles bought by each user?
  18. What does the invoice history look like for each user?
  19. What does the list of unusual orders look like (large quantities)?
  20. What are the unusual orders made by each user (large quantities)?
  21. What are the unusual orders made for each product (large quantities)?

## Batch Processing
### Data Source
Source: Python Data Generator.

The data includes transaction logs (generated each hour) containing:
  1. Invoice number.
  2. Product stock code.
  3. Product quantity.
  4. Invoice date.
  5. Customer's ID.
  6. Country of order with the country region.

The data also includes country information (generated each day) containing:
  1. Country's ID.
  2. Country name.

The data also includes product information (generated each day) containing:
  1. Product stock code.
  2. Product name.
  3. Product description.
  4. Unit price.
  5. Price date.

<p align="center">
  <img alt="Python-logo-notext svg" src="https://github.com/user-attachments/assets/1fde3009-f7f8-44b6-8ab4-6b29bcd35c77" />
</p>

### Extraction
Apache NiFi was used for the extraction process, which consists of the following phases:
1. Load batch data from a folder where new files arrive.
2.  For both product and log data, the destination path is set based on the timestamp, ensuring organized storage.
4. Place the files into the **Raw Data Zone** in HDFS.

<p align="center">
  <img alt="Kaggle_Logo" src="https://github.com/user-attachments/assets/1b8dcb5f-2b06-43c7-a61b-aea6fee4b0cc" />
</p>

<p align="center">
  <img alt="Screenshot_20250501_165920" src="https://github.com/user-attachments/assets/5e795c98-cfa1-4ae8-8943-1e209da544c4" />
</p>


### Data Zones

The data pipeline follows the **Medallion Architecture**, which organizes the data into three distinct zones:
1. **Raw Data Zone (Bronze)**
2. **Transformation Zone (Silver)**
3. **Curated Zone (Gold)**

<p align="center">
  <img width="410" alt="Screenshot-2024-06-30-at-18 57 11" src="https://github.com/user-attachments/assets/c3a0c95f-4051-4d7d-b8d2-fda5294c9387" />
</p>

#### Raw Zone (Bronze)
The **Raw Zone** is located in HDFS and stores the raw data without any transformations. It serves as the initial repository for all incoming batch data, preserving it in its original format for future processing.

<p align="center">
  <img src="https://github.com/user-attachments/assets/2652d4ff-2c10-465d-b1a6-ebd06782e841" alt="Hadoop_logo_new" />
</p>

#### Transformation Zone (Silver)
The **Transformation Zone** is located in HDFS and Apache Hive and contains data that has been cleaned and transformed into a format suitable for further processing and analysis. This zone ensures that the data is structured and ready for more advanced operations.

<p align="center">
  <img src="https://github.com/user-attachments/assets/2652d4ff-2c10-465d-b1a6-ebd06782e841" alt="Hadoop_logo_new" />
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/b7d31bbf-607d-4ffc-8667-472cfc0b1455" height="100" alt="Apache_Hive_logo svg" />
</p>

#### Curated Zone (Gold)
The **Curated Zone (Gold)** is located in PostgreSQL and it is where the final, high-quality data is stored. This zone is typically optimized for reporting, dashboarding, and machine learning tasks. The data in the **Curated Zone** is fully cleaned, aggregated, and enriched, ensuring that it's in the most usable form for advanced analysis. This data is ready for decision-making processes, business intelligence tools, and further data science operations.

<p align="center">
  <img src="https://github.com/user-attachments/assets/d6300905-4fe6-4034-88dd-2753a5b767f5" height="150" alt="Postgresql_elephant svg" />
</p>

### Data Processing
Data is processed using **Apache Spark** and **Python (PySpark)** in 5 main phases for batch processing:
- **Red Phase**: Clean the data from the **Raw Zone (Bronze)** and move it into the **Transformation Zone (Silver)** by designing initial batch processing jobs.
- **Green Phase**: Create a Spark job to combine all cleaned and structured data, and move it into the **Curated Zone (Gold)**.
- **Yellow Phase**: Build the Data Warehouse to store the data for analysis and reporting.
- **Purple Phase**: Implement an alarming system to detect unusually large or irregular orders, and build dashboards for data analytics using.
- **Blue Section**: Represents the user-facing application layer. While not part of the development responsibilities, this application will consume and utilize the processed data delivered by the pipeline.


> **Note**: Metabase was used to build dashboards instead of Tableau.
<p align="center">
  <img src="https://github.com/user-attachments/assets/b9976478-3954-4ef8-9ba3-4ca852ddd8c9" alt="Screenshot_20250501_171625" />
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/90ba6383-4e94-44f3-afb3-c358cdbc35c5" alt="Apache_Spark_logo" />

</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/6ba9ec8a-e005-4633-b365-408bec307153" alt="Python-logo-notext svg" />
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/d5ed7711-37fb-40dd-94e6-f5229ede2023" alt="Scala-full-color svg" />
</p>

#### Red Phase
This phase moves raw data into a structured and clean format using three Spark jobs:
1. **`loadlogsintohive`**: Cleans and loads transactional log data.
2. **`loadcountriesintohive`**: Processes and loads country and region data.
3. **`loadproductsintohive`**: Prepares and loads product information.


#### Green Phase, Yellow Phase and Purple Phase
After the data is cleaned and structured in the **Transformation Zone (Silver)**, it is further processed and enriched to move to the **Curated Zone (Gold)**. This phase consists of several Spark jobs, each designed to analyze and aggregate different aspects of the used car data. These jobs ensure that the data in the **Curated Zone (Gold)** is ready for advanced analysis and reporting.

The following five Spark jobs are used in this phase:
1. **`analyze_e_commerce_data.py`**: Analyzes and enriches e-commerce transaction data, joins various data sources, detects unusual orders, and sends notifications to administrators via email.
2. **`load_unprocessed_logs_into_hive.py`** – Separates the unprocessed logs that lack the necessary information for processing into a separate table, allowing for future repeated analysis.

These jobs help transform and enrich the data, making it ready for visualization, reporting, and advanced analysis in the **Curated Zone (Gold)**.

### Dashboards

The transformed and enriched data in the **Curated Zone (Gold)** is presented and visualized using **Metabase**, an open-source business intelligence tool. For each of the five Spark jobs, a dedicated dashboard is created to visualize the key insights and metrics derived from the processed data.

<p align="center">
  <img src="https://github.com/user-attachments/assets/62024e89-6f60-4ec7-b595-99dc22d3adad" width="200" alt="metabase-logo" />
</p>

Dedicated dashboards are created to visualize the key insights and metrics derived from the processed data:

1. **Top buyers dashboard**:  
   - Visualizes the top buyers globally, by country, and by country region

<p align="center">
  <img src="https://github.com/user-attachments/assets/0fea7b13-87d3-4529-a6ba-ef4b00a86933" alt="Screenshot_20250501_173947" />
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/c1474217-d5f4-4f0f-8f5d-6f139d08d31c" alt="Screenshot_20250501_174009" />
</p>


<p align="center">
  <img src="https://github.com/user-attachments/assets/dfc50aaf-e844-4891-9db5-29a3a519357f" alt="Screenshot_20250501_174027" />
</p>


2. **Multiregional buyers dashboard**:  
   - Visualizes the percentage of buyers making purchases across multiple countries and regions.

<p align="center">
  <img src="https://github.com/user-attachments/assets/1d4478bd-5ac8-42b5-abbb-10d83d92d716" alt="Screenshot_20250501_174356" />
</p>


3. **Geographical spending analysis dashboard**:  
   - Visualizes global spending distribution, country rankings by total spending, and regional contributions to national spending.

<p align="center">
  <img src="https://github.com/user-attachments/assets/4d18b275-7b73-4ee4-8d4e-e0acf174dad9" alt="Screenshot_20250501_174717" />
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/711e293d-1237-493e-ac60-9cbdc6540358" alt="Screenshot_20250501_174753" />
</p>


4. **Distribution of orders dashboard**:  
   - Visualizes the distribution of the number of orders per user and the number of items per order.

<p align="center">
  <img src="https://github.com/user-attachments/assets/2804f4ea-0ab7-45ff-8da3-7db96ad0f789" alt="Screenshot_20250501_175056" />
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/91c6b069-959c-4031-a9c7-0a34d38ff691" alt="Screenshot_20250501_175145" />
</p>


5. **Product trends dashboard**:  
   - Visualizes trends in product prices over time and the percentage changes in those prices.

<p align="center">
  <img src="https://github.com/user-attachments/assets/25c6ac1c-a331-4a3a-ba99-48077d58d300" alt="Screenshot_20250501_175404" />
</p>


6. **User activity dashboard**:  
   - Visualizes user activity variations by hour of the day and historical trends over time.
   - 
<p align="center">
  <img src="https://github.com/user-attachments/assets/c1d7008f-b386-4aaf-9143-15c0c594704d" alt="Screenshot_20250501_175647" />
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/b12a63de-2241-4509-9a0d-3c8524e65923" alt="Screenshot_20250501_175712" />
</p>


7. **User history dashboard**:  
   - Visualizes trends in user activity over time, total money spent per user, money spent per user per date, total articles bought by each user, and the invoice history for each user.

<p align="center">
  <img src="https://github.com/user-attachments/assets/2c4fc4d7-16d8-49d2-897c-0faad2640559" alt="Screenshot_20250501_180028" />
</p>


<p align="center">
  <img src="https://github.com/user-attachments/assets/adec9449-6bf3-4f35-9823-ee9c7e8e61ae" alt="Screenshot_20250501_180050" />
</p>




8. **Unusual orders dashboard**:  
   - Visualizes the list of unusual orders with large quantities, unusual orders made by each user, and unusual orders for each product based on large quantities.

<p align="center">
  <img src="https://github.com/user-attachments/assets/4ba837ac-edc2-4013-8153-9054f05a2ea2" alt="Screenshot_20250501_180317" />
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/f4026f16-2141-4f3e-b1a5-1694a3d6eb0b" alt="Screenshot_20250501_180342" />
</p>


<p align="center">
  <img src="https://github.com/user-attachments/assets/98879255-3c50-4b6d-8ad1-49cd2621c67c" alt="Screenshot_20250501_180404" />
</p>




### Orchestration

Orchestration of the batch data pipeline is managed using **Jenkins**, which schedules and automates Spark job execution at defined intervals.

<p align="center">
  <img src="https://github.com/user-attachments/assets/7528d37b-3234-4dc8-91a6-bb625fdb069e" alt="Jenkins_logo_with_title svg" />
</p>

Four Jenkins jobs are scheduled as follows:

- **loadlogsintohive** — Runs every 4 hours at **00:05, 04:05, 08:05, 12:05, 16:05, 20:05** to process and load incoming log data.
- **loadcountriesintohive** — Runs daily at **00:05** to update country data.
- **loadproductsintohive** — Runs daily at **00:05** to refresh product information.
- **analyzeecommercedata** — Runs every 4 hours at **00:10, 04:10, 08:10, 12:10, 16:10, 20:10** to analyze and enrich data, detect unusual orders, and send alerts.

## Containerization
Containerization of the application was achieved using Docker and Docker Compose.
<p align="center">
  <img src="https://github.com/user-attachments/assets/5b1fd6a3-22b3-464c-b01a-2fd035f74bbd" alt="Docker_logo" />
</p>

### Apache NiFi
<p align="center">
  <img alt="Kaggle_Logo" src="https://github.com/user-attachments/assets/1b8dcb5f-2b06-43c7-a61b-aea6fee4b0cc" />
</p>

The NiFi setup consists of the following containers:

- **niFi** (`apache/nifi:1.15.3`):
  - Manages data flows, data ingestion, and processing. Provides the main interface for creating and managing data pipelines.

- **nifi-registry** (`apache/nifi-registry:1.15.3`):
  - Stores and manages versioned NiFi data flows for tracking and version control of flow configurations.

### Hadoop
<p align="center">
  <img src="https://github.com/user-attachments/assets/2652d4ff-2c10-465d-b1a6-ebd06782e841" alt="Hadoop_logo_new" />
</p>
<p align="center">
  <img src="https://github.com/user-attachments/assets/b7d31bbf-607d-4ffc-8667-472cfc0b1455" height="100" alt="Apache_Hive_logo svg" />
</p>
<p align="center">
  <img src="https://github.com/user-attachments/assets/d6300905-4fe6-4034-88dd-2753a5b767f5" height="150" alt="Postgresql_elephant svg" />
</p>
<p align="center">
  <img src="https://github.com/user-attachments/assets/b0481116-f441-4461-89be-eda0f12e72b3" alt="Hue_official_logo" />
</p>

The Hadoop setup consists of the following containers:

- **namenode** (`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`):
  - Manages metadata and the HDFS directory structure. Only one **Namenode** is used.

- **datanode1 & datanode2** (`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`):
  - Store and manage the actual data. Two **Datanodes** are used for redundancy.

- **hive-server** (`bde2020/hive:2.3.2-postgresql-metastore`)  
  - Provides HiveServer2 for SQL querying capabilities with Hive.

- **hive-metastore** (`bde2020/hive:2.3.2-postgresql-metastore`)  
  - Runs the Hive Metastore service to manage table schemas and metadata.

- **hive-metastore-postgresql** (`bde2020/hive-metastore-postgresql:2.3.0`)  
  - PostgreSQL database used by the Hive Metastore for storing metadata.

- **hue** (`gethue/hue:20201111-135001`):
  - Provides a web-based UI for interacting with the Hadoop ecosystem.

### PostgreSQL
<p align="center">
  <img src="https://github.com/user-attachments/assets/d6300905-4fe6-4034-88dd-2753a5b767f5" height="150" alt="Postgresql_elephant svg" />
</p>

The PostgreSQL setup consists of the following containers:

- **postgres** (`postgres:14.0`):
  - A relational database container that stores data for the Curated Zone (Gold).

### Apache Spark
<p align="center">
  <img src="https://github.com/user-attachments/assets/90ba6383-4e94-44f3-afb3-c358cdbc35c5" alt="Apache_Spark_logo" />
</p>

The Spark setup consists of the following containers:

- **spark-master** (`bitnami/spark:3.2.2`):
  - Manages the overall cluster, coordinates the job distribution, and acts as the main entry point for Spark applications. The **Spark Master** container exposes the necessary ports for both the web UI and Spark cluster communication.

- **spark-worker1, spark-worker2, and spark-worker3** (`bitnami/spark:3.2.2`):
  - These containers perform the actual computation and run the tasks assigned by the **Spark Master**. Each worker container is allocated specific resources (cores and memory) for parallel task execution.

### Metabase
<p align="center">
  <img src="https://github.com/user-attachments/assets/62024e89-6f60-4ec7-b595-99dc22d3adad" width="200" alt="metabase-logo" />
</p>

The Metabase setup consists of the following containers:

- **metabase** (`metabase/metabase:v0.53.x`):
  - Provides an easy-to-use interface for data visualization and analytics.
 
### Apache Airflow
<p align="center">
  <img src="https://github.com/user-attachments/assets/7528d37b-3234-4dc8-91a6-bb625fdb069e" alt="Jenkins_logo_with_title svg" />
</p>

The Jenkins setup consists of the following containers:

- **jenkins** (`jenkins/jenkins:lts`)  
  - An automation server used to schedule and manage batch processing jobs in the data pipeline.  


