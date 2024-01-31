use master
create database real_data_on_logistics;

SELECT TOP 5 *
FROM training_raw_file; --understand data(cols and rows)

/*              the most important columns to do the analysis 
ID,Country,Shipment Mode,Scheduled Delivery Date,Delivered to Client Date,Unit of Measure (Per Pack),Line Item Quantity,Line Item Value,Pack Price,Unit Price
*/

/*                                               Data wrangling
                preparing raw data for analysis via convert raw data into analysis-ready data.
*/

-- Check data types for any type mismatch
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'training_raw_file';

/*
After conducting a thorough check for type mismatch within the dataset, these columns contain type mismatch:
So ID column is numerical but does not have measurement unit and no mean to order or perform operations. So must be qualitative data(categorical).(type 1 error)
Scheduled Delivery Date, and Delivered to Client Date columns are numbers but must be datetime.(type 2 error)
Freight Cost (USD) and Weight (Kilograms) columns are numerical but there are some strings.(type 3 error)
*/

-- Handle type mismatch
ALTER TABLE training_raw_file
ALTER COLUMN ID VARCHAR(MAX);
ALTER TABLE training_raw_file
ALTER COLUMN Project_Code VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN PQ VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN PO_SO VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN ASN_DN VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN Country VARCHAR(300);
ALTER TABLE training_raw_file
ALTER COLUMN Managed_By VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN Fulfill_Via VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN Vendor_INCO_Term VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN Shipment_Mode VARCHAR(100);
ALTER TABLE training_raw_file
ALTER COLUMN PQ_First_Sent_to_Client_Date VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN PO_Sent_to_Vendor_Date VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN Scheduled_Delivery_Date DATE;
ALTER TABLE training_raw_file
ALTER COLUMN Delivered_to_Client_Date DATE;
ALTER TABLE training_raw_file
ALTER COLUMN Delivery_Recorded_Date DATE;
ALTER TABLE training_raw_file
ALTER COLUMN Product_Group VARCHAR(100);
ALTER TABLE training_raw_file
ALTER COLUMN Sub_Classification VARCHAR(100);
ALTER TABLE training_raw_file
ALTER COLUMN Vendor VARCHAR(MAX);
ALTER TABLE training_raw_file
ALTER COLUMN Item_Description VARCHAR(MAX);
ALTER TABLE training_raw_file
ALTER COLUMN Molecule_Test_Type VARCHAR(MAX);
ALTER TABLE training_raw_file
ALTER COLUMN Brand VARCHAR(100);
ALTER TABLE training_raw_file
ALTER COLUMN Dosage VARCHAR(100);
ALTER TABLE training_raw_file
ALTER COLUMN Dosage_Form VARCHAR(100);
ALTER TABLE training_raw_file
ALTER COLUMN Unit_of_Measure_Per_Pack SMALLINT;
ALTER TABLE training_raw_file
ALTER COLUMN Line_Item_Quantity INT;
ALTER TABLE training_raw_file
ALTER COLUMN Line_Item_Value FLOAT;
ALTER TABLE training_raw_file
ALTER COLUMN Pack_Price FLOAT;
ALTER TABLE training_raw_file
ALTER COLUMN Unit_Price FLOAT;
ALTER TABLE training_raw_file
ALTER COLUMN Manufacturing_Site VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN First_Line_Designation CHAR(3);
ALTER TABLE training_raw_file
ALTER COLUMN Weight_Kilograms VARCHAR(255);
ALTER TABLE training_raw_file
ALTER COLUMN Freight_Cost_USD VARCHAR(255);

-- Check missing data
DECLARE @columnName NVARCHAR(255) = 'Line_Item_Quantity';
DECLARE @tableName NVARCHAR(255) = 'training_raw_file';
EXEC('SELECT * FROM ' + @tableName + ' WHERE [' + @columnName + '] IS NULL;'); -- Check for NULL values
EXEC('SELECT * FROM ' + @tableName + ' WHERE [' + @columnName + '] = '''';'); -- Check for rows with blank values
EXEC('SELECT * FROM ' + @tableName + ' WHERE [' + @columnName + '] = 0;'); -- Check for rows with 0

/*
After conducting a thorough check for missing data within the dataset, these columns contain missing data:
Shipment Mode(rows contain null), Line Item Value and Pack Price and Unit Price(rows contain 0.0 maybe missing data or an offer)
and Line Item Insurance (USD) (rows contain null and zeros) 
*/

-- Handle missing data
DELETE FROM training_raw_file
WHERE [Shipment_Mode] IS NULL; -- Delete rows where 'Shipment_Mode' is NULL
DELETE FROM training_raw_file
WHERE [Line_Item_Insurance_USD] IS NULL; -- Delete rows where 'Line_Item_Insurance_USD' is NULL

-- Check duplicates
SELECT *
FROM training_raw_file
WHERE ID IN (
    SELECT ID
    FROM training_raw_file
    GROUP BY ID
    HAVING COUNT(*) > 1
); -- Find duplicate rows across the 'ID' column

/*
After conducting a thorough check for duplicates within the dataset, all issues have been addressed
and the repetition seems meaningful and precise.
*/

/*                      Data mining and analysis   or   Exploratory Data Analysis (EDA)
							  extracting knowledge(insights) from data begins
*/

-- Handle outliers
WITH QuartilesCTE AS ( -- Calculate quartiles
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Line_Item_Quantity) OVER () AS Q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY Line_Item_Quantity) OVER () AS Q2,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Line_Item_Quantity) OVER () AS Q3
    FROM training_raw_file
)

UPDATE training_raw_file -- Identify and update outliers based on IQR
SET Line_Item_Quantity = 
    CASE
        WHEN Line_Item_Quantity < (Q1 - 1.5 * (Q3 - Q1)) THEN Q1
        WHEN Line_Item_Quantity > (Q3 + 1.5 * (Q3 - Q1)) THEN Q3
        ELSE Line_Item_Quantity
    END
FROM training_raw_file
CROSS JOIN QuartilesCTE;

-- I did the same for Line_Item_Value, Unit_Price, Pack_Price, Line_Item_Insurance_USD and Unit_of_Measure_Per_Pack

/*
The dealing with outlier alternates between activation and deactivation to ensure the acquisition of accurate and comprehensive information from actual data.
It identifies outliers, storing them for further study and insight extraction purposes.
*/

-- Handle type mismatch
ALTER TABLE training_raw_file
ALTER COLUMN Line_Item_Insurance_USD FLOAT;

-- Summary Statistics:
SELECT
    COUNT(Line_Item_Quantity) AS CountForLineItemQuantity,
    AVG(Line_Item_Quantity) AS AverageForLineItemQuantity,
    MIN(Line_Item_Quantity) AS MinimumForLineItemQuantity,
    MAX(Line_Item_Quantity) AS MaximumForLineItemQuantity,
	STDEV(Line_Item_Quantity) AS StandardDeviationForLineItemQuantity
FROM training_raw_file;

SELECT TOP 1
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Line_Item_Quantity) OVER () AS MedianForLineItemQuantity
FROM training_raw_file;

SELECT TOP 1
    Line_Item_Quantity AS ModeForLineItemQuantity,
    COUNT(*) AS Frequency
FROM training_raw_file
GROUP BY Line_Item_Quantity
ORDER BY COUNT(*) DESC;

-- Frequency Distribution:
SELECT 
    Product_Group AS Product_Group,
    COUNT(*) AS Frequency
FROM training_raw_file
GROUP BY Product_Group
ORDER BY COUNT(*) DESC;

-- Top N Items:
SELECT TOP 10
    Product_Group,
    SUM(Line_Item_Value) AS TotalSales
FROM training_raw_file
GROUP BY Product_Group
ORDER BY TotalSales DESC;

-- Time Series Analysis:
SELECT
    Scheduled_Delivery_Date,
    COUNT(*) AS OrderCount
FROM training_raw_file
GROUP BY Scheduled_Delivery_Date
ORDER BY Scheduled_Delivery_Date;

-- Aggregate Metrics:
SELECT
    Product_Group,
    AVG(Line_Item_Value) AS AverageSales,
    SUM(Line_Item_Value) AS TotalSales
FROM training_raw_file
GROUP BY Product_Group;

SELECT
    Country,
    COUNT(DISTINCT ID) AS UniqueCount
FROM training_raw_file
GROUP BY Country;

-- Ranking:
-- Top Countries by Order Quantity:
SELECT
    Country,
    SUM(Line_Item_Quantity) AS TotalQuantity
FROM training_raw_file
GROUP BY Country
ORDER BY TotalQuantity DESC

-- Cross-Tabulation of Product Groups and Shipment Modes:
SELECT
    Product_Group,
    Shipment_Mode,
    COUNT(*) AS Count
FROM training_raw_file
GROUP BY Product_Group, Shipment_Mode;

-- Comparison of Line Item Values between Different Countries:
SELECT
    Country,
    AVG(Line_Item_Value) AS AvgValue
FROM training_raw_file
GROUP BY Country
HAVING COUNT(Line_Item_Value) > 10
ORDER BY AvgValue DESC;

-- Date Range Analysis of Delivered Orders:
SELECT
    YEAR(Delivered_to_Client_Date) AS DeliveryYear,
    MONTH(Delivered_to_Client_Date) AS DeliveryMonth,
    COUNT(*) AS OrderCount
FROM training_raw_file
WHERE Delivered_to_Client_Date IS NOT NULL
GROUP BY YEAR(Delivered_to_Client_Date), MONTH(Delivered_to_Client_Date)
ORDER BY DeliveryYear, DeliveryMonth;

-- Cohort Analysis of Order Quantity by Project Code:
SELECT
    Project_Code,
    YEAR(Scheduled_Delivery_Date) AS CohortYear,
    MONTH(Scheduled_Delivery_Date) AS CohortMonth,
    COUNT(DISTINCT ID) AS CustomerCount
FROM training_raw_file
GROUP BY Project_Code, YEAR(Scheduled_Delivery_Date), MONTH(Scheduled_Delivery_Date)
ORDER BY Project_Code, CohortYear, CohortMonth;

-- Pareto Analysis of Product Groups by Sales:
WITH ProductSales AS (
    SELECT
        Product_Group,
        SUM(Line_Item_Value) AS TotalSales
    FROM training_raw_file
    GROUP BY Product_Group
)

SELECT
    Product_Group,
    TotalSales,
    TotalSales / SUM(TotalSales) OVER () AS CumulativePercentage
FROM ProductSales
ORDER BY TotalSales DESC;

-- 7-Day Moving Average of Line Item Quantity:
SELECT
    Scheduled_Delivery_Date,
    AVG(Line_Item_Quantity) OVER (ORDER BY Scheduled_Delivery_Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS SevenDayMovingAvg
FROM training_raw_file
ORDER BY Scheduled_Delivery_Date;
