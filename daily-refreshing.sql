---- ETL Part 8 ----
-- DAILY REFRESHING OF DIMENSION TABLES: ProductDimension example

--in ZAGIMORE_DataStaging, add colmuns "timestamp" AND "load status" to the ProductDimension table

ALTER TABLE Product_Dimension ADD loaded BOOLEAN NOT NULL ,

ADD ExtractionTimeStamp TIMESTAMP NOT NULL ;

--SET extraction time of all current product dimension VALUES to current time - 10 days for all timestamp VALUES for all instances of ProductDimension so far

--- updating ProductDimension Current Status
UPDATE Product_Dimension
SET Status='C', DateValidUntil='2030-01-01'

--SETting "loaded" VALUES to 1 for all instances of ProductDimension so far

UPDATE ProductDimension

SET loaded = 1,

ExtractionTimeStamp = NOW()- INTERVAL 10 day;


-- SETting up an example of changes in the product dimension by creating a new product
-- creating a new product
INSERT INTO product(productid,productname,productprice,vendorid,categoryid)
VALUES ('9X1','Fancy Bike',800,'MK','CY')

create table ipd as
SELECT p.categoryid, p.productid, p.productname, p.vendorid, c.categoryname, v.vendorname
FROM poudyas_ZAGIMORE.product p, poudyas_ZAGIMORE.category c, poudyas_ZAGIMORE.vendor v
WHERE p.categoryid = c.categoryid
AND p.vendorid = v.vendorid

-- testing: adding only new instances of Product Dimension in the Product Dimension table in Data Staging area

INSERT INTO Product_Dimension(ProductID, ProductName, VendorID, VendorName, CategoryID, DateValidFROM, DateValidUntil, Status, CategoryName,ProductType, loaded, ExtractionTimeStamp)
SELECT i.productid,i.productname,i.vendorid,i.vendorname,i.categoryid, date(now()),'2030-01-01', 'C', i.categoryname,'Sales',0,now()
FROM ipd i
WHERE i.productid NOT IN
(SELECT ProductID FROM Product_Dimension);

-- testing: now adding the same into the Data Warehouse
INSERT INTO poudyas_ZAGIMORE_DW.Product_Dimension(ProductID,ProductName,VendorID,VendorName,CategoryID,ProductKey,DateValidFROM,DateValidUntil,Status,CategoryName,ProductType)
SELECT r.ProductID,r.ProductName,r.VendorID,r.VendorName,r.CategoryID,r.ProductKey,r.DateValidFROM,r.DateValidUntil,r.Status,r.CategoryName,r.ProductType
FROM poudyas_ZAGIMORE_ds.Product_Dimension r
WHERE r.loaded = 0;


SELECT COUNT(*)
FROM poudyas_ZAGIMORE.soldvia
UNION 
SELECT COUNT(*)
FROM poudyas_ZAGIMORE.rentvia
UNION 
SELECT COUNT(*)
FROM poudyas_ZAGIMORE_ds.Product_Dimension
UNION 
SELECT COUNT(*)
FROM poudyas_ZAGIMORE_DW.Product_Dimension


--- UPDATE fields in the datawarehouse
UPDATE Product_Dimension
SET Status='C'

-- Creating procedure

CREATE PROCEDURE ETLProductDimensionAppendNewProducts()
BEGIN
	TRUNCATE ipd;

	INSERT INTO ipd(categoryid, productid, productname, vendorid, categoryname, vendorname)
	SELECT p.categoryid, p.productid, p.productname, p.vendorid, c.categoryname, v.vendorname
	FROM poudyas_ZAGIMORE.product p, poudyas_ZAGIMORE.category c, poudyas_ZAGIMORE.vendor v
	WHERE p.categoryid = c.categoryid
	AND p.vendorid = v.vendorid;

	INSERT INTO Product_Dimension(ProductID,ProductName,VendorID,VendorName,CategoryID, DateValidFROM,DateValidUntil,Status, CategoryName,ProductType,loaded,ExtractionTimeStamp)
	SELECT i.productid,i.productname,i.vendorid,i.vendorname,i.categoryid, date(now()),'2030-01-01', 'C', i.categoryname,'Sales',0,now()
	FROM ipd i
	WHERE i.productid NOT IN
	(SELECT ProductID FROM ProductDimension);

	INSERT INTO poudyas_ZAGIMORE_DW.Product_Dimension(ProductID,ProductName,VendorID,VendorName,CategoryID,ProductKey,DateValidFROM,DateValidUntil,Status,CategoryName,ProductType)
	SELECT r.ProductID,r.ProductName,r.VendorID,r.VendorName,r.CategoryID,r.ProductKey,r.DateValidFROM,r.DateValidUntil,r.Status,r.CategoryName,r.ProductType
	FROM poudyas_ZAGIMORE_ds.Product_Dimension r
	WHERE r.loaded = 0;

	UPDATE Product_Dimension
	SET loaded = 1
	WHERE loaded = 0;

END


--adding two new product to test procedure
INSERT INTO product(productid,productname,productprice,vendorid,categoryid)
VALUES ('9X2','Fanciest Bike',1800,'MK','CY');

INSERT INTO product(productid,productname,productprice,vendorid,categoryid
VALUES ('9X3','Electric Scooter',100,'OA','EL');


--- tesing the procedure
CALL ETLProductDimensionAppendNewProducts()

--Adding tested procedure to Scheduled event (Not supported by current version)
CREATE EVENT dailyETL
ON SCHEDULE AT '23:59:59'
EVERY 1 DAY
DO
BEGIN
CALL ETLRevenueFactAppend();
CALL ETLProductDimensionAppendNewProducts();
END