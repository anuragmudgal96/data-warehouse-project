# Data Warehouse and Analytics Project
This project showcases an end-to-end data warehousing and analytics solution, covering everything from ETL processes and data integration to building a data warehouse and deriving actionable insights.

---
## Data Architecture
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:

<img src="https://github.com/anuragmudgal96/data-warehouse-project/blob/main/docs/data_architecture.drawio.png">

1) **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2) **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3) **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.
