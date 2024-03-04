-- Part 4 Creating ProductCategoryDimension, ProductCategoryAggregateFact and DailyStoreSnapshot
CREATE TABLE ProductCategoryDimension AS
SELECT DISTINCT CategoryID, CategoryName FROM ProductDimension

ALTER TABLE ProductCategoryDimension ADD CategoryKey INT NOT NULL AUTO_INCREMENT,
ADD PRIMARY KEY (CategoryKey)

CREATE TABLE ProductCategoryAggregateFact AS
SELECT SUM(cf.UnitsSold) AS UnitsSold,SUM(cf.RevenueGenerated) AS RevenueGenerated, cf.CalendarKey,
cf.CustomerKey, cf.StoreKey, pcd.CategoryKey
FROM CoreFact cf, ProductDimension pd, ProductCategoryDimension pcd
WHERE cf.ProductKey = pd.ProductKey AND
pd.CategoryID = pcd.CategoryID
GROUP BY cf.CalendarKey, cf.CustomerKey, cf.StoreKey, pd.CategoryID;

-- Add this to DW

CREATE TABLE ProductCategoryDimension AS
SELECT * FROM poudyas_ZAGIMORE_ds.ProductCategoryDimension;

CREATE TABLE ProductCategoryAggregateFact AS
SELECT * FROM poudyas_ZAGIMORE_ds.ProductCategoryAggregateFact;

ALTER TABLE ProductCategoryAggregateFact
ADD Foreign Key (CalendarKey) REFERENCES Calendar_Dimension(CalendarKey),
ADD FOREIGN KEY (CustomerKey) REFERENCES Customer_Dimension(CustomerKey),
ADD FOREIGN KEY (LocationKey) REFERENCES Store_Dimension(StoreKey),
ADD FOREIGN KEY (CategoryKey) REFERENCES ProductCategoryDimension(CategoryKey)

INSERT INTO poudyas_ZAGIMORE_DW.ProductCategoryAggregateFact(CalendarKey,
CategoryKey, CustomerKey,RevenueGenerated,LocationKey,UnitsSold)
SELECT CalendarKey,CategoryKey, CustomerKey,RevenueGenerated,LocationKey,UnitsSold
FROM ProductCategoryAggregateFact

CREATE TABLE DailyStoreSnapShot AS
SELECT SUM(RevenueGenerated) AS TotalRevenue, COUNT(DISTINCT tid)
AS NumTransactions, SUM(RevenueGenerated)/COUNT(DISTINCT tid)
AS AvgRevenuePerTransaction, cf.StoreKey, cf.CalendarKey
FROM CoreFact cf
GROUP BY StoreKey, CalendarKey

CREATE TABLE poudyas_ZAGIMORE_DW.DailyStoreSnapShot
AS SELECT *
FROM DailyStoreSnapShot

ALTER TABLE DailyStoreSnapShot
ADD FOREIGN KEY (LocationKey) REFERENCES Store_Dimension(StoreKey),
ADD FOREIGN KEY (CalendarKey) REFERENCES Calendar_Dimension(CalendarKey)


SELECT SUM(cf.UnitsSold), cf.LocationKey, cf.CalendarKey AS FootwearUnitsSold
FROM CoreFact cf, Product_Dimension pd
WHERE pd.CategoryName="Footwear" AND cf.ProductKey=pd.ProductKey
GROUP BY cf.LocationKey, cf.CalendarKey

--- Adding ProductCategory_Dimension, OneWayAggregateRevenueByCategory, DailyStoreSnapshot into DataStaging without Foreign Keys

CREATE TABLE ProductCategory_Dimension
(
ProductCategoryKey INT NOT NULL,
CategoryID INT NOT NULL,
CategoryName VARCHAR(50) NOT NULL,
PRIMARY KEY (ProductCategoryKey)
);

CREATE TABLE OneWayAggregateRevenueByCategory
(
DollarGenerated INT NOT NULL,
CalendarKey INT NOT NULL,
CustomerKey INT NOT NULL,
LocationKey INT NOT NULL,
ProductCategoryKey INT NOT NULL
);

CREATE TABLE DailyStoreSnapshot
(
TotalDollarGenerated INT NOT NULL,
TotalNoOfTransactions INT NOT NULL,
AverageRevenuePerTransaction INT NOT NULL,
Calendar_key INT NOT NULL,
LocationKey INT NOT NULL
);


-- Extracting Product Category Columns from Product_Dimension into ProductCategory_Dimension

Insert into ProductCategory_Dimension(CategoryID, CategoryName)
Select Distinct CategoryId, CategoryName from Product_Dimension

Insert into OneWayAggregateRevenueByCategory(DollarGenerated, LocationKey, CalendarKey, CustomerKey, ProductCategoryKey, RevenueType)
Select SUM(r.RevenueGenerated), r.LocationKey, r.CalendarKey, r.CustomerKey, pcd.CategoryKey, r.RevenueType
from poudyas_ZAGIMORE_ds.CoreFact r, poudyas_ZAGIMORE_ds.Product_Dimension p, poudyas_ZAGIMORE_ds.ProductCategoryDimension pcd
Where r.ProductKey = p.ProductKey and
p.CategoryID = pcd.CategoryID
group by r.LocationKey, r.CalendarKey, r.CustomerKey, p.CategoryID, r.RevenueType

-- Extracting Data from Revenue Fact Table into DailyStoreSnapshot

Select SUM(r.DollarGenerated), COUNT(r.TransactionID), r.LocationKey, r.Calendar_Key
from poudyas_ZAGIMORE_ds.Revenue r
group by r.LocationKey, r.Calendar_Key

Select SUM(r.DollarGenerated), r.LocationKey, r.Calendar_Key
from poudyas_ZAGIMORE_ds.Revenue r, poudyas_ZAGIMORE_ds.Product_Dimension p
Where p.CategoryName = 'Footwear' and r.ProductKey = p.ProductKey
group by r.LocationKey, r.Calendar_Key


-- Adding ProductCategoryDimension, OneWayAggregateRevenueByCategory, DailyStoreSnapshot into DataWarehouse with Foreign Keys

CREATE TABLE ProductCategoryDimension
(
ProductCategoryKey INT NOT NULL,
CategoryID INT NOT NULL,
CategoryName VARCHAR(50) NOT NULL,
PRIMARY KEY (ProductCategoryKey)
);

CREATE TABLE OneWayAggregateRevenueByCategory
(
DollarGenerated INT NOT NULL,
Calendar_key INT NOT NULL,
CustomerKey INT NOT NULL,
LocationKey INT NOT NULL,
ProductCategoryKey INT NOT NULL,
FOREIGN KEY (Calendar_key) REFERENCES Calendar_Dimension(Calendar_key),
FOREIGN KEY (CustomerKey) REFERENCES Customer_Dimension(CustomerKey),
FOREIGN KEY (LocationKey) REFERENCES Location_Dimension(LocationKey),
FOREIGN KEY (ProductCategoryKey) REFERENCES ProductCategory_Dimension(ProductCategoryKey)
);

CREATE TABLE DailyStoreSnapshot
(
TotalDollarGenerated INT NOT NULL,
TotalNoOfTransactions INT NOT NULL,
AverageRevenuePerTransaction INT NOT NULL,
Calendar_key INT NOT NULL,
LocationKey INT NOT NULL,
FOREIGN KEY (Calendar_key) REFERENCES Calendar_Dimension(Calendar_key),
FOREIGN KEY (LocationKey) REFERENCES Location_Dimension(LocationKey)
);