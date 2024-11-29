# Data-Pipeline-Project
As a Data Science student, I wanted to explore the data engineering processes that are crucial for delivering data to various teams within a company, such as the Data Science team, Business Intelligence team, and others. This repository focuses on transforming a part of a 3NF normalized database into a data warehouse with a star schema.
For this project, I used the AdventureWorks2022 database, concentrating on specific sections of the database rather than the entire dataset.
![BigDataInfrastructure](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/BigDataInfrastructure.png)

![ETLPipeline](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/ETLPipeline1.png)

**Tools Used**
- MS SQL Server 2022
- MS SQL Server Management Studio (SSMS)
- MS Visual Studio Community 2019 (with Data Tools extension installed)
- MS SQL Server Integration Services (SSIS)


# Adventure Works from normalized schema to star schema (Data Warehouse).

## 1. Defining the Schema

**a. Identify the Business Purpose**
- The scope of the data we want to represent: Sales
- Specific questions we want to answer: AdventureWorks's Transactional Metrics:
    - **Total Transactions:** Count of sales transactions.
    - **Total Units Sold:** Sum of OrderQty across all transactions.
    - **Average Order Quantity:** Average of OrderQty per transaction = Sum(OrderQty) / Count(SalesOrderID)
- These transactional metrics we want filtered and ordered in respect to:
    - **Customer Distribution:** City, State.
    - **Product:** Name, Color, Size, Weight
    - **Special Offer:** Type, Category
    - **Sales Person**: Name 

**b. Define wanted Granularity (Level of Detail)**
Examples are:
- Transactional-level: One row per transaction. (Chosen)
- Daily-level: Aggregated daily metrics.
- Monthly-level: Aggregated monthly metrics.

**c. Find the tables of interest**
Folowing what we want to do in a. The table of interest are:
- Sales.SalesOrderHeader
    1. SalesOrderID: Primary Key
    2. OrderDate: DateTime
    3. SubTotal: Number

- Sales.SalesOrderDetail
    1. SalesOrderID: Foreign Key
    2. OrderQty: Number

For **Customer Distribution:** City
- Person.Address: AddressID, City 

For **Product:** Name, Color, Size, Weight
- Production.Product: ProductID, Name, Color, Size, Weight (Throught linking table Sales.SpecialOfferProduct)

For **Special Offer:** Type, Category
- Sales.SpecialOffer: SpecialOfferID, Type, Category  

For **Sales Person**: Name, Marital Status, Gender, Hire Date
- HumanResources.Employee: MaritalStatus, Gender, HireDate
- Person.Person: FirstName, LastName




**d. Design Fact Table** (FactSales)
1. SalesOrderID: Primary Key, Clustered Index
2. CityKey: Unclustered Index
3. DateKey: Unclustered Index
4. ProductKey: Unclustered Index
5. SpecialOfferKey: Unclustered Index
6. SalesPersonKey: Unclustered Index
7. OrderQty: Number
8. SubTotal: Number

**e. Design Dimension Tables**
1. DimCity: CityKey, City
2. DimDate: DateKey, Date
3. DimProduct: ProductKey, Name, Color, Size, Weight
4. DimSpecialOffer: SpecialOfferKey, Type, Category
5. DimSalesPerson: SalesPersonKey, Name, Gender, MaritalStatus, HireDate




**Quick Definitions:**

**Clustered Index:**
- Defines the physical order of the rows in the table.
- Points directly to the data (leaf nodes contain the actual rows).
- There is only one clustered index per table.
- Best for queries that filter, join, or sort based on the indexed column(s).
- Ideal for queries with equality conditions and range-based filtering.

**Unclustered Index:**
- Provides an alternative structure to optimize for different query patterns.
- Points to the clustered index (or row identifiers in heap tables).
- Stored separately and adds memory overhead.
- Best for queries with range conditions (BETWEEN, <, >) and for supporting joins and some sorting operations (ORDER BY).
​

**Schema of the normalized tables we will be using**
![Schema of the normalized tables we will be using](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/AW%20Original%20Normalized%20Schema.png)



## 2 Building the star database
The scripts to create the tables, keys and indexes are included in the repo. We obtain this schema:

![Our target star schema](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/StarSchema.png)

## 3 Creating and Populating a Staging Table
While using a global staging table is not typically considered best practice due to performance concerns (especially with large databases) and limited scalability, we will use it in this case for its simplicity and practicality. Below is the schema for the staging table:
![Staging Table Schema](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/StagingTable.png)

**Our strategy**, to be fully implemented in Microsoft SSIS using Visual Studio, will follow these steps:
1. Create Temporary Staging Dimension Tables:
    - Create temporary staging tables for each dimension (e.g., DimDate) as OLE DB sources.
    - Populate these tables with relevant qualitative entries (e.g., dates for the DimDate table).
    - Generate auto-incrementing surrogate keys (e.g., DateKey) during the population process.

2. Load Data into Dimension Tables:
    - Transfer the selected entries from the temporary staging tables into the corresponding dimension tables, ensuring that all dimension tables are fully populated.

3. Populate Fact Table:
    - Use LEFT JOIN operations between the global staging table and each of the dimension tables.
    - This allows us to link the surrogate keys from the dimension tables to the fact table, populating the fact table with the appropriate foreign keys and quantitative data.
  

## 4 SSIS ETL Package to fill our data warehouse

**SSIS (SQL Server Integration Services):**
SQL Server Integration Services (SSIS) is a powerful data integration and workflow automation tool from Microsoft, installed as a seperate package but used within **Visual Studio** GUI. It allows you to **ETL** data between various sources and destinations. SSIS supports relational databases, flat files, and cloud-based storage. It is commonly used for tasks like data migration, **data warehousing**, and automating business processes. SSIS provides a visual design environment with drag-and-drop functionality to build complex data workflows and transformation logic.

**ETL (Extract, Transform, Load):**
ETL stands for Extract, Transform, Load, which is a process used in **data warehousing** to move data from source systems to a destination, typically a data warehouse or data lake. The process involves:
- Extract: Gathering data from various sources such as databases, APIs, or flat files.
- Transform: Cleaning, filtering, and transforming the data into the desired format. This may involve data validation, aggregation, and applying business rules.
- Load: Inserting the transformed data into the destination storage system, such as a relational database, data warehouse, or analytics platform.


### 4.1 SSIS Package Overview
In our SSIS package, we will use the following components:
- **"Execute SQL"** Task: This will be used to truncate all the data warehouse tables before loading new data, ensuring that we avoid any primary key conflicts.
- **"Data Flow"** Task: This task will first populate the dimension tables and then the fact tables, ensuring that all necessary data is loaded in the correct sequence.
The arrows in the SSIS control flow enforce the execution sequence, ensuring that the tasks are executed in the specified order.
![SSIS Package Overview](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/PackageOverview.png)


### 4.2 Truncating the data warehouse tables
**TRUNCATE** removes all rows from a table, resetting the table to its empty state without logging individual row deletions, unlike **DELETE**.
```sql
TRUNCATE TABLE FactSales

ALTER TABLE FactSales DROP CONSTRAINT FK_FactSales_SalesPerson
TRUNCATE TABLE DimSalesPerson

ALTER TABLE FactSales
ADD CONSTRAINT FK_FactSales_SalesPerson FOREIGN KEY (SalesPersonKey) REFERENCES DimSalesPerson (SalesPersonKey);
```
We remove the constraints because it is not possible to truncate a table that is referenced by a foreign key in another table (e.g., the FactSales table).


### 4.3 Populating Dimension Tables
This is the 1st data flow, we'll be taking as an example the dimension table **DimSalesPerson**
![Populating Dim Tables in SSIS](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/1stDataFlow.png)
Here is how to create the staging table for this dimension table:
```sql
WITH SalesPersonCTE AS (
    SELECT DISTINCT SalesPersonName, SalesPersonGender, SalesPersonMaritalStatus, SalesPersonHireDate
    FROM StagingTable
)

SELECT
    ROW_NUMBER() OVER (ORDER BY SalesPersonName) AS SalesPersonKey,
    SalesPersonName, SalesPersonGender, SalesPersonMaritalStatus, SalesPersonHireDate
FROM SalesPersonCTE;
```
After running this query, you’ll just map the columns. Here, they are preset to have the corresponding names.
![Populating Dim Tables in SSIS](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/MappingDimSalesPerson.png)


### 4.4 Populating Fact Table
Now that all the dimension tables are populated, we’ll use the global staging table and perform joins on the filled dimension tables to create the staging table for the Fact table.
![Populating Fact Table in SSIS](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/2ndDataFlow.png)
```sql
SELECT st.SalesOrderID,
	dc.CityKey,
	dt.DateKey,
	dp.ProductKey,
	dso.SpecialOfferKey,
	dsp.SalesPersonKey,
	st.OrderQty,
	st.SubTotal

FROM StagingTable st
LEFT JOIN DimCity dc ON st.CityName = dc.CityName
LEFT JOIN DimDate dt ON st.[Date] = dt.[Date]
LEFT JOIN DimProduct dp ON st.ProductName = dp.ProductName
LEFT JOIN DimSpecialOffer dso ON st.SpecialOfferType = dso.SpecialOfferType
LEFT JOIN DimSalesPerson dsp ON st.SalesPersonName = dsp.SalesPersonName

ORDER BY st.SalesOrderID
```
This step takes noticeably more time because of the multiple joins.
![Populating Fact Table in SSIS](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/MappingFactSales.png)


## 5 Performance Testing on the newly created Data Warehouse (More like Data Mart)
Let's say that we want to analyze the performance of every male salesperson in the two years, 2012 and 2013. We can use these queries and measure their execution times:
```sql
USE AdventureWorks2022;
-- Measure execution time for the normalized database
SET STATISTICS TIME ON;
SELECT
    (per.FirstName + ' ' + per.LastName) AS SalesPersonName,
	SUM(sod.LineTotal) AS Total

FROM Sales.SalesOrderHeader so
JOIN Sales.SalesOrderDetail sod ON so.SalesOrderID = sod.SalesOrderID
JOIN HumanResources.Employee he ON so.SalesPersonID = he.BusinessEntityID
JOIN Person.Person per ON so.SalesPersonID = per.BusinessEntityID
WHERE so.OrderDate BETWEEN '2012-01-01 00:00:00' AND '2014-01-01 00:00:00'
AND he.Gender = 'M'
GROUP BY  per.FirstName, per.LastName;
SET STATISTICS TIME OFF;


USE AdventureWorksCustomDW;
-- Measure execution time for data warehouse query
SET STATISTICS TIME ON;
SELECT
	dsp.SalesPersonName,
    SUM(f.SubTotal) AS Total
FROM FactSales f
JOIN DimSalesPerson dsp ON f.SalesPersonKey = dsp.SalesPersonKey
JOIN DimDate dt ON f.DateKey = dt.DateKey
WHERE dt.[Date] BETWEEN '2012-01-01 00:00:00' AND '2014-01-01 00:00:00'
AND dsp.SalesPersonGender = 'M'
GROUP BY dsp.SalesPersonName;
SET STATISTICS TIME OFF;
```

We find this difference:
![Execution Time Difference](https://github.com/AnasRouam/Data-Pipeline-Project/blob/main/DocsAssets/ExecutionTime.png)

It takes half the time to run the query, involving almost 80,000 rows, on the data warehouse (FactSales and DimSalesPerson tables) as compared to the normalized database (SalesOrderHeader and related tables).

In real-world scenarios, data sizes can grow exponentially to reach tens or hundreds of millions of rows. the difference in execution times becomes even more significant. The data warehouse model is optimized for reporting and analytical queries (OLAP), which makes it more efficient than the normalized model in such cases. Therefore, the time savings would scale accordingly, leading to a substantial improvement in performance, particularly for complex and frequent queries over large datasets.

This improved performance not only reduces the time needed for business intelligence tasks but also provides better user experience and faster decision-making capabilities in data-driven environments.



# Further Explorations
- **Expand Schema Design:** Explore snowflake schemas and advanced relationships like bridge tables.
- **Visualize Insights:** Create dashboards with Power BI, Tableau, or Python to analyze and present data.
- **Automate ETL Pipelines:** Use tools like Apache Airflow or dbt for automation and scalability.
- **Optimize Performance:** Experiment with indexing, partitioning, and other performance-tuning strategies.
- **Data Science on Data Lake:** Build a data lake to perform preprocessing, exploratory analysis, and other data science tasks on raw and semi-structured data.
- **Data Science on Data Warehouse:** Use the data warehouse to streamline data for feature engineering and training predictive models.
