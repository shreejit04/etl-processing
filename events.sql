---Part 7----
-- adding new events in the transaction tables--

INSERT INTO poudyas_ZAGIMORE.salestransaction (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('T22222', '3-4-555', 'S10', '2021-03-15');
INSERT INTO poudyas_ZAGIMORE.soldvia (`productid`, `tid`, `noofitems`) VALUES ('1X3', 'T22222', '5'), ('6X6', 'T22222', '10');

INSERT INTO poudyas_ZAGIMORE.rentaltransaction (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('T33333', '3-4-555', 'S10', '2021-03-15');
INSERT INTO poudyas_ZAGIMORE.rentvia (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('5X5', 'T33333', 'D', '3');

INSERT INTO poudyas_ZAGIMORE.rentaltransaction (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('T44444', '3-4-555', 'S10', '2021-03-15');
INSERT INTO `poudyas_ZAGIMORE`.`rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('5X5', 'T44444', 'W', '6');

INSERT INTO poudyas_ZAGIMORE.rentaltransaction (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('T55555', '3-4-555', 'S10', '2021-03-15');
INSERT INTO `poudyas_ZAGIMORE`.`rentvia` (`productid`, `tid`, `rentaltype`, `duration`) VALUES ('5X5', 'T55555', 'W', '6');

--- creating the Revenue Fact update procedure---
create procedure ETLRevenueFactAppend()
begin

	DROP TABLE intermediateRevenueFactTable;

	CREATE TABLE intermediateRevenueFactTable as
	SELECT sv.noofitems, sv.noofitems*p.productprice as RevenueGenerated, st.CustomerID, st.StoreID, sv.ProductID, st.tdate, st.tid, 'Sales' AS RevenueType
	FROM poudyas_ZAGIMORE.product p, poudyas_ZAGIMORE.soldvia sv, poudyas_ZAGIMORE.salestransaction st
	WHERE p.ProductID = sv.ProductID
	AND sv.Tid = st.Tid
	AND st.tdate > (SELECT DATE((SELECT MAX(f_timestamp) FROM poudyas_ZAGIMORE_ds.CoreFact)));

	ALTER TABLE `intermediateRevenueFactTable` CHANGE `RevenueType` `RevenueType` VARCHAR( 15 );

	INSERT INTO intermediateRevenueFactTable(noofitems,CustomerID, tdate, StoreID,ProductID, RevenueGenerated, tid, RevenueType)
	SELECT '1', c.customerid, st.tdate, s.storeid, p.productid, p.productpricedaily * sv.duration, st.tid, 'Rentals,Daily'
	FROM poudyas_ZAGIMORE.rentalProducts p, poudyas_ZAGIMORE.rentvia sv, poudyas_ZAGIMORE.rentaltransaction st, poudyas_ZAGIMORE.customer c, poudyas_ZAGIMORE.store s
	WHERE p.productid = sv.productid
	AND sv.tid = st.tid
	AND st.customerid = c.customerid
	AND st.storeid=s.storeid
	AND sv.rentaltype='D'
	AND st.tdate > (SELECT DATE((SELECT MAX(f_timestamp) FROM poudyas_ZAGIMORE_ds.CoreFact)));

	INSERT INTO intermediateRevenueFactTable (noofitems,CustomerID, tdate, StoreID,ProductID, RevenueGenerated, tid, RevenueType)
	SELECT '1', c.customerid, st.tdate, s.storeid, p.productid, p.productpricedaily * sv.duration, st.tid, 'Rentals,Weekly'
	FROM poudyas_ZAGIMORE.rentalProducts p, poudyas_ZAGIMORE.rentvia sv, poudyas_ZAGIMORE.rentaltransaction st, poudyas_ZAGIMORE.customer c, poudyas_ZAGIMORE.store s
	WHERE p.productid = sv.productid
	AND sv.tid = st.tid
	AND st.customerid = c.customerid
	AND st.storeid=s.storeid
	AND sv.rentaltype='W'
	AND st.tdate > (SELECT DATE((SELECT MAX(f_timestamp) FROM poudyas_ZAGIMORE_ds.CoreFact)));
	
end

-- extracting data from new intermediate fact table, and loading it into the Core Fact in data staging, while substituting surrogate keys instead of operational keys
INSERT INTO CoreFact(CustomerKey, ProductKey, LocationKey, UnitsSold, RevenueGenerated, CalendarKey, tid, f_timestamp, loaded, RevenueType)
SELECT c.CustomerKey, p.ProductKey, s.StoreKey, i.noofitems,
i.RevenueGenerated, ca.CalendarKey, i.tid, NOW(), FALSE, RevenueType
FROM intermediateRevenueFactTable i, Customer_Dimension c,
Store_Dimension s, Product_Dimension p, Calendar_Dimension ca
WHERE i.CustomerID = c.CustomerID
AND i.StoreID = s.StoreID
AND i.ProductID = p.ProductID
AND i.tdate = ca.fulldate;

-- finally, loading the data from the Core Fact in data staging into the Core Fact table in the data warehouse. only the new facts will be loaded, those whose value of the loaded attribute is 0
INSERT INTO poudyas_ZAGIMORE_DW.CoreFact(CustomerKey, ProductKey, StoreKey, UnitsSold, RevenueGenerated, CalendarKey, tid, RevenueType)
SELECT CustomerKey, ProductKey, LocationKey, UnitsSold, RevenueGenerated, CalendarKey, tid, RevenueType
FROM CoreFact
WHERE loaded = 0;

--- update and set loaded equal to 1
UPDATE CoreFact
SET loaded = 1;
end

---testing the Revenue Fact procedure---


CALL ETLRevenueFactAppend()

INSERT INTO poudyas_ZAGIMORE.salestransaction (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('T99999', '3-4-555', 'S10', '2021-03-16');
INSERT INTO poudyas_ZAGIMORE.soldvia (`productid`, `tid`, `noofitems`) VALUES ('3X3', 'T99999', '8')

----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------

-- Modified code for part 7b: type 2 changes

TRUNCATE ipd;

INSERT INTO ipd(categoryid, productid, productname, vendorid, categoryname, vendorname)
SELECT p.categoryid, p.productid, p.productname, p.vendorid, c.categoryname, v.vendorname
FROM esheats_ZAGI_Retail.product p, esheats_ZAGI_Retail.category c, esheats_ZAGI_Retail.vendor v
WHERE p.categoryid = c.categoryid
AND p.vendorid = v.vendorid;

INSERT INTO Product_Dimension(CategoryID, ProductID, ProductName, VendorID, CategoryName, VendorName, DateValidFrom, DateValidUntil, loaded, ExtractionTimeStamp, ProductType)
SELECT i.categoryid, i.productid, i.productname, i.vendorid, i.categoryname, i.vendorname, DATE(NOW()), '2030-01-01', 0, NOW(), 'Sales'
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


Create View m1 AS
SELECT pd1.ProductID
FROM Product_Dimension pd1, Product_Dimension pd2
WHERE pd1.ProductType LIKE 'S%'
AND pd1.ProductID = pd2.ProductID
AND pd2.loaded = 0
GROUP By pd1.ProductID
Having COUNT(pd1.ProductID) > 1;


UPDATE Product_Dimension
SET DateValidUntil = Date(NOW()) - Interval 1 day
WHERE loaded = 1
AND ProductID IN (SELECT * from m1);

DROP VIEW m1;

INSERT INTO poudyas_ZAGIMORE_DW.Product_Dimension(ProductKey, CategoryID, ProductID, ProductName, VendorID, CategoryName, VendorName, DateValidFrom, DateValidUntil, ProductType)
SELECT p.ProductKey, p.CategoryID, p.ProductID, p.ProductName, p.VendorID, p.CategoryName, p.VendorName, p.DateValidFrom, p.DateValidUntil, p.ProductType
FROM poudyas_ZAGIMORE_ds.Product_Dimension p
WHERE p.loaded = 0;

UPDATE poudyas_ZAGIMORE_DW.Product_Dimension dwp1, poudyas_ZAGIMORE_DW.Product_Dimension dwp2, poudyas_ZAGIMORE_ds.Product_Dimension dsp
SET dwp1.DateValidUntil = Date(NOW()) - Interval 1 day
WHERE dwp1.ProductID = dwp2.ProductID
AND dwp1.ProductID = dsp.ProductID
AND dsp.loaded = 0
AND dwp2.DateValidFrom > dwp1.DateValidFrom;

UPDATE Product_Dimension
SET loaded = 1
WHERE loaded = 0;

END

--change 2 more products & run again

--alternative (simpler) version for updating the DW (not supprted by our version of MYSQL)
REPLACE INTO kyadad_ZAGIMORE_DW.Product_Dimension(ProductKey, CategoryID, ProductID, ProductName, VendorID, CategoryName, VendorName, DateValidFrom, DateValidUntil, ProductType, Status)
SELECT p.ProductKey, p.CategoryID, p.ProductID, p.ProductName, p.VendorID, p.CategoryName, p.VendorName, p.DateValidFrom, p.DateValidUntil, p.ProductType, p.Status
FROM kyadad_ZAGIMORE_DS.Product_Dimension p
WHERE p.LoadedStatus = 'N';

--, auto scheduler for all ETL events, (not supprted by our version of MYSQL)
CREATE EVENT dailyETL
ON SCHEDULE AT '23:59:59'
EVERY 1 DAY
DO
BEGIN
CALL ETLRevenueFactAppend();
CALL ETLProductDimensionAppendNewProducts();
END