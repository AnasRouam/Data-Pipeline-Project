# Data-Pipeline-Project
I'll try to create a data pipeline for an imaginary company.

**Full Pipeline Example:**
1. Extract data from multiple sources (CSV, APIs, web scraping, etc.).
2. Transform the data by cleaning, aggregating, and combining it.
3. Load the data into a SQL database or a data warehouse.
4. Visualize the data in Power BI (create reports and dashboards).
5. Train a predictive model (e.g., regression or classification) on the data using Python.
6. Automate the pipeline (e.g., using cron jobs, Airflow, or Python scripts).
7. Deploy the model and use Power BI to update dashboards with new predictions.



# 1. Adventure Works from normalized schema to star schema.

## 1.1 Defining the schema

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
![Schema of the normalized tables we will be using](AW Original Normalized Schema.png)



## 1.2 Building the star database
