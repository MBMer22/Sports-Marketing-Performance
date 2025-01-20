-- Create Database and set it as the active database
CREATE DATABASE SportsMarketingDB;
USE DATABASE SportsMarketingDB;

-- Create Schema and set it as the active schema
CREATE SCHEMA MarketingSchema;
USE SCHEMA MARKETINGSCHEMA;

-- Create the stage for data loading
CREATE STAGE sports_stage;

-- Create the staging table for customer data
CREATE OR REPLACE TABLE Staging_CustomerData (
    Customer_ID INT,
    Customer_Name STRING,
    Age INT,
    Gender STRING,
    Location STRING,
    Favorite_Sport STRING,
    Marketing_Channel STRING,
    Campaign_Type STRING,
    Interaction_Date DATE,
    Purchase_Amount FLOAT,
    A_B_Test_Group STRING,
    Customer_Segment STRING,
    Journey_Stage STRING
);

-- Create the fact and dimension tables
CREATE OR REPLACE TABLE Fact_Sales (
    Sale_ID INT AUTOINCREMENT PRIMARY KEY,
    Customer_ID INT,
    Campaign_ID INT,
    Interaction_Date DATE,
    Purchase_Amount FLOAT
);

CREATE OR REPLACE TABLE Dim_Customers (
    Customer_ID INT PRIMARY KEY,
    Customer_Name STRING,
    Age INT,
    Gender STRING,
    Location STRING,
    Favorite_Sport STRING
);

CREATE OR REPLACE TABLE Dim_Campaigns (
    Campaign_ID INT AUTOINCREMENT PRIMARY KEY,
    Campaign_Type STRING,
    Marketing_Channel STRING,
    A_B_Test_Group STRING
);

CREATE OR REPLACE TABLE Dim_JourneyStages (
    Stage_ID INT AUTOINCREMENT PRIMARY KEY,
    Journey_Stage STRING
);

-- List files in the stage
LIST @sports_stage;

-- Load data into the staging customer data table
COPY INTO STAGING_CUSTOMERDATA
FROM @sports_stage/sports_marketing_dataset.csv
FILE_FORMAT = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Describe the structure of the staging customer data table
DESCRIBE TABLE STAGING_CUSTOMERDATA;

-- Validate data in staging table
SELECT * FROM STAGING_CUSTOMERDATA LIMIT 10;

-- Check for missing values in key columns
SELECT * FROM STAGING_CUSTOMERDATA WHERE CUSTOMER_ID IS NULL OR AGE IS NULL;

-- Update inconsistent Gender values
UPDATE STAGING_CUSTOMERDATA
SET Gender = 'Male'
WHERE Gender IN ('M', 'male');

-- Insert data into fact and dimension tables
INSERT INTO DIM_CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, AGE, GENDER, LOCATION, FAVORITE_SPORT)
SELECT DISTINCT CUSTOMER_ID, CUSTOMER_NAME, AGE, GENDER, LOCATION, FAVORITE_SPORT
FROM STAGING_CUSTOMERDATA;

INSERT INTO DIM_CAMPAIGNS (CAMPAIGN_TYPE, MARKETING_CHANNEL, A_B_TEST_GROUP)
SELECT DISTINCT CAMPAIGN_TYPE, MARKETING_CHANNEL, A_B_TEST_GROUP
FROM STAGING_CUSTOMERDATA;

INSERT INTO DIM_JOURNEYSTAGES (JOURNEY_STAGE)
SELECT DISTINCT JOURNEY_STAGE
FROM STAGING_CUSTOMERDATA;

INSERT INTO FACT_SALES (Customer_ID, Campaign_ID, Interaction_Date, Purchase_Amount)
SELECT
    sc.Customer_ID,
    dc.Campaign_ID,
    sc.Interaction_Date,
    sc.Purchase_Amount
FROM STAGING_CUSTOMERDATA sc
JOIN DIM_CAMPAIGNS dc
ON sc.campaign_type = dc.campaign_type
AND sc.marketing_channel = dc.marketing_channel
AND sc.a_b_test_group = dc.a_b_test_group;

-- Query to aggregate Total Sales by Campaign Type
SELECT dc.Campaign_Type, SUM(fs.Purchase_Amount) AS Total_Sales
FROM FACT_SALES fs
JOIN DIM_CAMPAIGNS dc ON fs.campaign_id = dc.campaign_id
GROUP BY dc.campaign_type;

-- Query to calculate Average Purchase Amount by Customer Segment
SELECT sc.Customer_Segment, AVG(fs.Purchase_Amount) AS Avg_Purchase
FROM FACT_SALES fs
JOIN STAGING_CUSTOMERDATA sc ON fs.customer_id = sc.customer_id
GROUP BY sc.customer_segment;

-- Query to calculate Average Purchase Amount by A/B Test Group
SELECT sc.A_B_Test_Group, AVG(fs.Purchase_Amount) AS Avg_Purchase
FROM FACT_SALES fs
JOIN STAGING_CUSTOMERDATA sc ON fs.customer_id = sc.customer_id
GROUP BY sc.a_b_test_group;

-- Create view for A/B Testing Insights
CREATE OR REPLACE VIEW vw_ABTestingInsights AS
SELECT
    dc.Campaign_Type,
    dc.A_B_Test_Group,
    COUNT(DISTINCT fs.Customer_ID) AS Unique_Customers,
    SUM(fs.Purchase_Amount) AS Total_Sales,
    AVG(fs.Purchase_Amount) AS Avg_Sales_Per_Customer
FROM FACT_SALES fs
JOIN DIM_CAMPAIGNS dc ON fs.campaign_id = dc.campaign_id
GROUP BY dc.campaign_type, dc.a_b_test_group;

-- Create view for Customer Segmentation
CREATE OR REPLACE VIEW vw_CustomerSegmentation AS
SELECT
    dc.Customer_Segment,
    COUNT(DISTINCT fs.Customer_ID) AS Total_Customers,
    SUM(fs.Purchase_Amount) AS Total_Sales,
    AVG(fs.Purchase_Amount) AS Avg_Sales_Per_Customer
FROM FACT_SALES fs
JOIN DIM_CUSTOMERS dc ON fs.customer_id = dc.customer_id
GROUP BY dc.customer_segment;

-- Create view for Customer Journey Mapping
CREATE OR REPLACE VIEW vw_CustomerJourney AS
SELECT
    dj.Journey_Stage,
    COUNT(DISTINCT fs.Customer_ID) AS Unique_Customers,
    SUM(fs.Purchase_Amount) AS Total_Purchase_Amount
FROM FACT_SALES fs
JOIN DIM_JOURNEYSTAGES dj ON fs.stage_id = dj.stage_id
GROUP BY dj.journey_stage;

-- Validate data in Dimension and Fact Tables
SELECT * FROM DIM_JOURNEYSTAGES;
SELECT * FROM FACT_SALES WHERE STAGE_ID IS NULL;
SELECT * FROM FACT_SALES LIMIT 10;
SELECT * FROM DIM_CAMPAIGNS LIMIT 10;
SELECT * FROM DIM_CUSTOMERS LIMIT 10;
SELECT * FROM DIM_JOURNEYSTAGES LIMIT 10;