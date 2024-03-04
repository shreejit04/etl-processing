---- next recording Part 9 (final video for ETL)----

--Changing the values in existing product 1x3 name to solar charger

update product
set productname = 'Solar Charger'
where productname = 'Sunny Charger'

--- test each part of the procedure to be created ----

TRUNCATE ipd;

-- in the operational db we changed product name -> sunny charger into solar charger
INSERT INTO ipd(categoryid, productid, productname, vendorid, categoryname, vendorname)
SELECT p.categoryid, p.productid, p.productname, p.vendorid, c.categoryname, v.vendorname
FROM poudyas_ZAGIMORE.product p, poudyas_ZAGIMORE.category c, poudyas_ZAGIMORE.vendor v
WHERE p.categoryid = c.categoryid
AND p.vendorid = v.vendorid;

-- Checking for which columns have changed among the columns that are possible to change (product, vendor and category names)
-- Inserting the instances of product dim that have undergone type 2 change as new rows in product dim in DS
INSERT INTO Product_Dimension(CategoryID, ProductID, ProductName, VendorID, CategoryName, VendorName, DateValidFrom, DateValidUntil, loaded, ExtractionTimeStamp, ProductType, Current_Status)
SELECT i.categoryid, i.productid, i.productname, i.vendorid, i.categoryname, i.vendorname, DATE(NOW()), '2030-01-01', 0, NOW(), 'Sales', 'C'
FROM ipd i
WHERE i.productname NOT IN
(SELECT ProductName
FROM Product_Dimension)
OR i.vendorname NOT IN
(SELECT VendorName
FROM Product_Dimension)
OR i.categoryname NOT IN
(SELECT CategoryName
FROM Product_Dimension);


-- changing the current status and datevaliduntil of the older instance of the product dimension that just went under type 2 changes
--Temp view containing productids from the product dimension table, with the same productID, that appear more than once
Create View m1 AS
SELECT ProductID FROM Product_Dimension
WHERE ProductType= 'S'
GROUP By ProductID
Having COUNT(*) > 1

-- updating the fields datevaliduntil and status for the old version of the product dim instance that has undergone type 2 change.
-- method one using nested q and view
UPDATE Product_Dimension
SET DateValidUntil = Date(NOW()) - Interval 1 day,
Current_Status = 'N'
WHERE loaded = 1
AND ProductID IN (SELECT * from m1);

--DROP before running this next time
DROP VIEW m1;

-- Inserting the instances of product dim that have undergone type 2 change as new rows in product dim in DW
INSERT INTO poudyas_ZAGIMORE_DW.Product_Dimension(ProductKey, CategoryID, ProductID, ProductName, VendorID, CategoryName, VendorName, DateValidFrom, DateValidUntil, ProductType, Status)
SELECT p.ProductKey, p.CategoryID, p.ProductID, p.ProductName, p.VendorID, p.CategoryName, p.VendorName, p.DateValidFrom, p.DateValidUntil, p.ProductType, p.Status
FROM poudyas_ZAGIMORE_ds.Product_Dimension p
WHERE p.loaded = 0;

-- updating the fields datevaliduntil and status for the old version of the product dim instance that has undergone type 2 change.
-- Alternative method using self-join
-- Comparing date valid from of the old version and the new version of the same dim instance
-- -> and changing datevaliduntil and status for the older of the 2
-- if the same row has undergone type 2 change more than once, we need to make sure we only compare the lastest 2 versions of the same prod instace
UPDATE poudyas_ZAGIMORE_DW.Product_Dimension dwp1, poudyas_ZAGIMORE_DW.Product_Dimension dwp2
SET dwp1.DateValidUntil = Date(NOW()) - Interval 1 day,
dwp1.Status = 'N'
WHERE dwp1.ProductID = dwp2.ProductID
AND dwp2.DateValidFrom > dwp1.DateValidFrom
AND dwp1.Status = 'C';

--update ds.pd
UPDATE Product_Dimension
SET loaded = 1
WHERE loaded = 0;


---- now creating the prodedure ----
-- for type 2 assuming: that following attributes can be changed.
-- Product name, vendor name, category name.
CREATE PROCEDURE ELTProduct_DimensionAppendType2Changes()
BEGIN
TRUNCATE ipd;


-- in the operational db we changed product name -> sunny charger into solar charger
INSERT INTO ipd(categoryid, productid, productname, vendorid, categoryname, vendorname)
SELECT p.categoryid, p.productid, p.productname, p.vendorid, c.categoryname, v.vendorname
FROM poudyas_ZAGIMORE.product p, poudyas_ZAGIMORE.category c, poudyas_ZAGIMORE.vendor v
WHERE p.categoryid = c.categoryid
AND p.vendorid = v.vendorid;

-- Checking for which columns have changed among the columns that are possible to change (product, vendor and category names)
-- Inserting the instances of product dim that have undergone type 2 change as new rows in product dim in DS
INSERT INTO Product_Dimension(CategoryID, ProductID, ProductName, VendorID, CategoryName, VendorName, DateValidFrom, DateValidUntil, loaded, ExtractionTimeStamp, ProductType, Status)
SELECT i.categoryid, i.productid, i.productname, i.vendorid, i.categoryname, i.vendorname, DATE(NOW()), '2030-01-01', 0, NOW(), 'Sales', 'C'
FROM ipd i
WHERE i.productname NOT IN
(SELECT ProductName
FROM Product_Dimension)
OR i.vendorname NOT IN
(SELECT VendorName
FROM Product_Dimension)
OR i.categoryname NOT IN
(SELECT CategoryName
FROM Product_Dimension);


-- changing the current status and datevaliduntil of the older instance of the product dimension that just went under type 2 changes
--Temp view containing productids from the product dimension table, with the same productID, that appear more than once
Create View m1 AS
SELECT ProductID FROM Product_Dimension
WHERE ProductType= 'S'
GROUP By ProductID
Having COUNT(*) > 1;

-- updating the fields datevaliduntil and status for the old version of the product dim instance that has undergone type 2 change.
-- method one using nested q and view
UPDATE Product_Dimension
SET DateValidUntil = Date(NOW()) - Interval 1 day,
Status = 'N'
WHERE loaded = 1
AND ProductID IN (SELECT * from m1);

--DROP before running this next time
DROP VIEW m1;

-- Inserting the instances of product dim that have undergone type 2 change as new rows in product dim in DW
INSERT INTO poudyas_ZAGIMORE_DW.Product_Dimension(ProductKey, CategoryID, ProductID, ProductName, VendorID, CategoryName, VendorName, DateValidFrom, DateValidUntil, ProductType, Current_Status)
SELECT p.ProductKey, p.CategoryID, p.ProductID, p.ProductName, p.VendorID, p.CategoryName, p.VendorName, p.DateValidFrom, p.DateValidUntil, p.ProductType, p.Current_Status
FROM poudyas_ZAGIMORE_ds.Product_Dimension p
WHERE p.loaded = 0;

-- updating the fields datevaliduntil and status for the old version of the product dim instance that has undergone type 2 change.
-- Alternative method using self-join
-- Comparing date valid from of the old version and the new version of the same dim instance
-- -> and changing datevaliduntil and status for the older of the 2
-- if the same row has undergone type 2 change more than once, we need to make sure we only compare the lastest 2 versions of the same prod instace
UPDATE poudyas_ZAGIMORE_DW.Product_Dimension dwp1, poudyas_ZAGIMORE_DW.Product_Dimension dwp2
SET dwp1.DateValidUntil = Date(NOW()) - Interval 1 day,
dwp1.Status = 'N'
WHERE dwp1.ProductID = dwp2.ProductID
AND dwp2.DateValidFrom > dwp1.DateValidFrom
AND dwp1.Status = 'C';

--update ds.pd
UPDATE Product_Dimension
SET loaded = 1
WHERE loaded = 0;

END


-- Testing the newly created proc by conducting type 2 changes on 2 more instances of diff things
-- Name change and vendor name change, start with product name changes:
update product
set productname = 'Yyy Bag'
where productname = 'Zzz Bag';
update product
set productname = 'Ooo Stove'
where productname = 'Mmm Stove';

Call ELTProduct_DimensionAppendType2Changes()

-- test changing the vendor name:
update vendor
set vendorname= 'Wild Adventures'
where vendorname='Wilderness Limited'

Call ELTProduct_DimensionAppendType2Changes()

-- alternative way to populate dw with type 2 changes without update
--- it inserts all new rows that undergone with changes
-- not supported by our version of MYSQL
-- Syntax:
replace into poudyas_ZAGIMORE_DW.Product_Dimension(productKey,productid, productName, productPrice, vendorid, vendorName, categoryid, categoryName, datestart, dateend, Status)
-- Example:
REPLACE INTO poudyas_ZAGIMORE_DW.Product_Dimension(ProductKey, CategoryID, ProductID, ProductName, VendorID, CategoryName, VendorName, DateValidFrom, DateValidUntil, ProductType, Status)
SELECT p.ProductKey, p.CategoryID, p.ProductID, p.ProductName, p.VendorID, p.CategoryName, p.VendorName, p.DateValidFrom, p.DateValidUntil, p.ProductType, p.Status
FROM poudyas_ZAGIMORE_ds.Product_Dimension p
WHERE p.LoadedStatus = 'N';