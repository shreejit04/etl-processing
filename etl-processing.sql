
-- Data Warehouse Creation

CREATE TABLE Calendar_Dimension
(
CalendarKey INT AUTO_INCREMENT NOT NULL,
Fulldate DATE NOT NULL,
CalendarMonth INT NOT NULL,
CalendarYear INT NOT NULL,
MonthYear VARCHAR(10) NOT NULL,
PRIMARY KEY (CalendarKey)
);

CREATE TABLE Store_Dimension
(
StoreKey INT AUTO_INCREMENT NOT NULL,
StoreID VARCHAR(3) NOT NULL,
RegionName VARCHAR(15) NOT NULL,
StoreZip VARCHAR(5) NOT NULL,
RegionID VARCHAR(3) NOT NULL,
PRIMARY KEY (StoreKey)
);

CREATE TABLE Product_Dimension
(
ProductKey INT AUTO_INCREMENT NOT NULL,
ProductID CHAR(3) NOT NULL,
ProductName VARCHAR(25) NOT NULL,
VendorID VARCHAR(3) NOT NULL,
VendorName VARCHAR(25) NOT NULL,
CategoryID VARCHAR(2) NOT NULL,
CategoryName VARCHAR(25) NOT NULL,
ProductType VARCHAR(15) NOT NULL,
PRIMARY KEY (ProductKey)
);

CREATE TABLE Customer_Dimension
(
CustomerKey INT AUTO_INCREMENT NOT NULL,
CustomerID VARCHAR(7) NOT NULL,
CustomerName VARCHAR(15) NOT NULL,
CustomerZip VARCHAR(5) NOT NULL,
PRIMARY KEY (CustomerKey)
);

CREATE TABLE Product_Price
(
ProductPrice DECIMAL(7,2),
ProductPriceType VARCHAR(20),
ProductKey INT NOT NULL,
PRIMARY KEY (ProductPrice, ProductPriceType, ProductKey),
FOREIGN KEY (ProductKey) REFERENCES Product_Dimension(ProductKey)
);

CREATE TABLE CoreFact
(
UnitsSold INT NOT NULL,
RevenueGenerated NUMERIC(9,2) NOT NULL,
tid VARCHAR(8) NOT NULL,
FOREIGN KEY (CalendarKey) REFERENCES Calendar_Dimension(CalendarKey),
FOREIGN KEY (StoreKey) REFERENCES Store_Dimension(StoreKey),
FOREIGN KEY (ProductKey) REFERENCES Product_Dimension(ProductKey),
FOREIGN KEY (CustomerKey) REFERENCES Customer_Dimension(CustomerKey)
);

ALTER TABLE Product_Price ADD (
DateValidFrom DATE,
DateValidUntil DATE,
STATUS CHAR( 1 ))

ALTER TABLE Customer_Dimension ADD (
DateValidFrom DATE,
DateValidUntil DATE,
STATUS CHAR( 1 ))

ALTER TABLE Store_Dimension ADD (
DateValidFrom DATE,
DateValidUntil DATE,
STATUS CHAR( 1 ))

ALTER TABLE Product_Dimension ADD (
DateValidFrom DATE,
DateValidUntil DATE,
STATUS CHAR( 1 ))

-- Data Staging 

CREATE TABLE Calendar_Dimension
(
CalendarKey INT NOT NULL,
Fulldate DATE NOT NULL,
CalendarMonth INT NOT NULL,
CalendarYear INT NOT NULL,
MonthYear VARCHAR(10) NOT NULL,
PRIMARY KEY (CalendarKey)
);

CREATE TABLE Store_Dimension
(
StoreKey INT AUTO_INCREMENT NOT NULL,
StoreID VARCHAR(3) NOT NULL,
RegionName VARCHAR(15) NOT NULL,
StoreZip VARCHAR(5) NOT NULL,
RegionID VARCHAR(3) NOT NULL,
PRIMARY KEY (StoreKey)
);

CREATE TABLE Product_Dimension
(
ProductKey INT AUTO_INCREMENT NOT NULL,
ProductID CHAR(3) NOT NULL,
ProductName VARCHAR(25) NOT NULL,
VendorID VARCHAR(3) NOT NULL,
VendorName VARCHAR(25) NOT NULL,
CategoryID VARCHAR(2) NOT NULL,
CategoryName VARCHAR(25) NOT NULL,
ProductType VARCHAR(15) NOT NULL,
PRIMARY KEY (ProductKey)
);

CREATE TABLE Customer_Dimension
(
CustomerKey INT AUTO_INCREMENT NOT NULL,
CustomerID VARCHAR(7) NOT NULL,
CustomerName VARCHAR(15) NOT NULL,
CustomerZip VARCHAR(5) NOT NULL,
PRIMARY KEY (CustomerKey)
);

CREATE TABLE Product_Price
(
ProductPrice DECIMAL(7,2),
ProductPriceType VARCHAR(20),
ProductKey INT NOT NULL,
PRIMARY KEY (ProductPrice, ProductPriceType, ProductKey),
FOREIGN KEY (ProductKey) REFERENCES Product_Dimension(ProductKey)
);

CREATE TABLE CoreFact
(
UnitsSold INT NOT NULL,
RevenueGenerated NUMERIC(9,2) NOT NULL,
tid VARCHAR(8) NOT NULL,
CustomerKey INT NOT NULL,
LocationKey INT NOT NULL,
ProductKey INT NOT NULL,
CalendarKey INT NOT NULL
);

CREATE PROCEDURE populateCalendar()

BEGIN

  DECLARE i INT DEFAULT 0;   

myloop: LOOP

    

	INSERT INTO Calendar_Dimension(Fulldate)

	SELECT DATE_ADD('2013-01-01', INTERVAL i DAY);

	SET i=i+1;

    IF i=4000 then

            LEAVE myloop;

    END IF;

END LOOP myloop;



UPDATE Calendar_Dimension

SET CalendarMonth = MONTH(Fulldate), CalendarYear = YEAR(Fulldate);


END;

update Calendar_Dimension
set MonthYear = concat(Year(FullDate),lpad(Month(FullDate),2,'0'));

--to verify that it sorts properly

select Distinct MonthYear from Calendar_Dimension
Order by MonthYear

--inserting sales product price data into product price table
INSERT INTO Product_Price(ProductKey, ProductPrice, ProductPriceType)
SELECT pd.ProductKey, p.productprice, 'Unit Sales Price'
FROM ZAGIMORE_bandarb.product p, ZAGIMORE_DataStaging_bandarb.Product_Dimension pd
WHERE pd.ProductID = p.productid
AND pd.ProductType = 'Sales'

--inserting daily rental product price data into product price table
INSERT INTO Product_Price(ProductKey, ProductPrice, ProductPriceType)
SELECT pd.ProductKey, r.productpricedaily, 'Daily Rental Price'
FROM ZAGIMORE_bandarb.rentalProducts r, ZAGIMORE_DataStaging_bandarb.Product_Dimension pd
WHERE pd.productid = r.productid
AND pd.producttype = 'Rental'

--inserting weekly rental product price data into product price table
INSERT INTO Product_Price(ProductKey, ProductPrice, ProductPriceType)
SELECT pd.ProductKey, r.productpriceweekly, 'Weekly Rental Price'
FROM ZAGIMORE_bandarb.rentalProducts r, ZAGIMORE_DataStaging_bandarb.Product_Dimension pd
WHERE pd.productid = r.productid
AND pd.producttype = 'Rental'

--adding few columns in product price table
ALTER TABLE Product_Price
ADD (DateValidFrom DATE, DateValidUntil DATE, Status CHAR(1))

--updating status in product price table
UPDATE Product_Price
SET DateValidFrom = '2013-01-01', DateValidUntil = '2030-01-01', Status = 'C'

-- Extracting the revenue data from the ZAGIMORE source database into the data staging database

--Adding Rev Type degenerate dimension column to RevenueFact table

--BOTH data staging and data warehouse
ALTER TABLE CoreFact
ADD RevenueType VARCHAR(15)

-- creating an intermediate core fact table and extracting revenue and unit sold facts, --and tid from ZAGIMORE, as well as corresponding operational key attributes for all --the dimensions

-- creating an intermediate core fact table and extracting revenue and unit sold facts, --and tid from ZAGIMORE, as well as corresponding operational key attributes for all --the dimensions

CREATE TABLE intermediateRevenueFactTable as
SELECT sv.noofitems as UnitsSold, sv.noofitems*p.productprice as revenueGenerated, st.customerid, st.storeid, sv.productid, st.tdate, st.tid
FROM poudyas_ZAGIMORE.product p, poudyas_ZAGIMORE.soldvia sv, poudyas_ZAGIMORE.salestransaction st
WHERE p.productid = sv.productid
AND sv.tid = st.tid

--add RevenueType column to the intermediateRevenueFactTable

ALTER TABLE intermediateRevenueFactTable
ADD RevenueType VARCHAR(20);

UPDATE intermediateRevenueFactTable
SET RevenueType ='Sales'

--adding revenue fact rows from Daily rentals (assume that each rental is one unit only)

INSERT INTO intermediateRevenueFactTable(UnitsSold, revenueGenerated, CustomerID, StoreID, ProductID, tdate, RevenueType, tid)
SELECT 0, rp.productpricedaily*rv.duration, rt.customerid, rt.storeid, rv.productid, rt.tdate, 'Rental, Daily', rt.tid
FROM poudyas_ZAGIMORE.rentalProducts rp, poudyas_ZAGIMORE.rentvia rv, poudyas_ZAGIMORE.rentaltransaction rt
WHERE rp.productid = rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'D'

--adding revenue fact rows from Weekly rentals (assume that each rental is one unit only)

INSERT INTO intermediateRevenueFactTable(CustomerID,ProductID,revenueGenerated,RevenueType,StoreID,tdate,tid,UnitsSold)
SELECT rt.CustomerID,rv.ProductID,rp.productpriceweekly*rv.duration,'Rental, Weekly',rt.StoreID, rt.tdate,rt.tid,0
FROM poudyas_ZAGIMORE.rentalProducts rp, poudyas_ZAGIMORE.rentvia rv, poudyas_ZAGIMORE.rentaltransaction rt
WHERE rp.productid = rv.productid
AND rv.tid = rt.tid
AND rv.rentaltype = 'W'

INSERT INTO CoreFact(UnitsSold, RevenueGenerated, tid, CustomerKey, LocationKey, ProductKey, CalendarKey, RevenueType)
SELECT i.UnitsSold, i.revenueGenerated, i.tid, c.CustomerKey, s.StoreKey, p.ProductKey,ca.CalendarKey, i.RevenueType
FROM intermediateRevenueFactTable i, Customer_Dimension c,
Store_Dimension s, Product_Dimension p, Calendar_Dimension ca
WHERE i.customerid = c.CustomerID
AND i.storeid = s.StoreID
AND i.productid = p.ProductID
AND i.tdate = ca.FullDate
AND LEFT(i.RevenueType,1) = LEFT(p.ProductType,1)

--- Populating the Calendar Dimension in Data Warehouse from Data Staging ---

INSERT INTO poudyas_ZAGIMORE_DW.Calendar_Dimension(CalendarKey,CalendarMonth,CalendarYear,Fulldate,MonthYear)
SELECT c.CalendarKey,c.CalendarMonth,c.CalendarYear,c.Fulldate,c.MonthYear
FROM Calendar_Dimension AS c

-- loading from Data Staging to Data Warehouse --

INSERT INTO poudyas_ZAGIMORE_DW.Store_Dimension (StoreKey, StoreID, StoreZip, RegionID,
RegionName, DateValidFrom, DateValidUntil, Status)
SELECT StoreKey, StoreID, StoreZip, RegionID,
RegionName, DateValidFrom, DateValidUntil, Status
FROM Store_Dimension

INSERT INTO poudyas_ZAGIMORE_DW.Product_Dimension(ProductID,ProductName,VendorID,VendorName,CategoryID,ProductKey,DateValidFrom,DateValidUntil,Status,CategoryName,ProductType)
SELECT ProductID, ProductName, VendorID, VendorName, CategoryID, ProductKey,DateValidFrom,DateValidUntil,Status,CategoryName,ProductType
FROM Product_Dimension

INSERT INTO poudyas_ZAGIMORE_DW.Customer_Dimension(CustomerID,CustomerName,CustomerZip,CustomerKey,DateValidFrom,DateValidUntil,Status)
SELECT CustomerID,CustomerName,CustomerZip, CustomerKey,DateValidFrom,DateValidUntil,Status
FROM Customer_Dimension

INSERT INTO poudyas_ZAGIMORE_DW.CoreFact(UnitsSold,RevenueGenerated,tid,CustomerKey,StoreKey,ProductKey,CalendarKey,RevenueType)
SELECT UnitsSold,RevenueGenerated,tid,CustomerKey,LocationKey,ProductKey,CalendarKey,RevenueType
FROM CoreFact
