---- Guided part 6:
-- DAILY REFRESHING OF FACT TABLE

--in ZAGIMORE_DataStaging, add colmuns "timestamp" and "loaded" to the fact table

ALTER TABLE CoreFact ADD loaded BOOLEAN NOT NULL ,

ADD f_timestamp TIMESTAMP NOT NULL ;

--setting "loaded" values to 1 for all facts so far

UPDATE CoreFact SET loaded = TRUE,
f_timestamp = NOW()- INTERVAL 10 day;


------add two new facts (this code is from previous year make sure date of transaction is from that current day you are writing the code)
INSERT INTO poudyas_ZAGIMORE.salestransaction (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('T10000', '3-4-555', 'S10', '2023-03-28');
INSERT INTO poudyas_ZAGIMORE.soldvia (`productid`, `tid`, `noofitems`) VALUES ('1X3', 'T10000', '4'), ('6X6', 'T10000', '1');


-- extracting only new facts (that occurred since the last load, as signified by the f_timestamp value)

DROP TABLE intermediateRevenueFactTable;
CREATE TABLE intermediateRevenueFactTable as
SELECT sv.noofitems, sv.noofitems*p.productprice as RevenueGenerated, st.customerid, st.storeid, sv.productid, st.tdate, st.tid,'Sales' as RevenueType
FROM poudyas_ZAGIMORE.product p, poudyas_ZAGIMORE.soldvia sv, poudyas_ZAGIMORE.salestransaction st
WHERE p.productid = sv.productid
AND sv.tid = st.tid
AND st.tdate > (SELECT DATE((SELECT MAX(f_timestamp) FROM poudyas_ZAGIMORE_ds.CoreFact)));


INSERT INTO CoreFact(CustomerKey, ProductKey, StoreKey, UnitsSold, RevenueGenerated, CalendarKey,TransactionID, f_timestamp, loaded, RevenueType)
SELECT c.CustomerKey, p.ProductKey, s.StoreKey, i.noofitems, i.RevenueGenerated, ca.CalendarKey, i.tid, NOW(), FALSE, 'Sales'
FROM intermediateRevenueFactTable i, Customer_Dimension c, Store_Dimension s, Product_Dimension p, Calendar_Dimension ca
WHERE i.CustomerID = c.CustomerID
AND i.StoreID = s.StoreID
AND i.ProductID = p.ProductID
AND LEFT(i.RevenueType,1) = LEFT(p.ProductType, 1)
AND i.tdate = ca.fulldate

-----Part 6 ends here-----


---- Short clip after part 6 -----
-- Inserting from CoreFact in data staging (the two new facts)
--loading new facts and updating f_timestamp and loaded status

INSERT INTO poudyas_ZAGIMORE_DW.CoreFact(CustomerKey, ProductKey, StoreKey, UnitsSold, RevenueGenerated, CalendarKey, tid)
SELECT CustomerKey, ProductKey, LocationKey, UnitsSold, RevenueGenerated, CalendarKey, tid
FROM CoreFact
WHERE loaded = 0;

-- now setting status to true of two new facts, to signify they have been loaded
UPDATE CoreFact
SET loaded = 1

INSERT INTO poudyas_ZAGIMORE.salestransaction (`tid`, `customerid`, `storeid`, `tdate`) VALUES ('T4949', '3-4-555', 'S10', '2023-03-28');
INSERT INTO poudyas_ZAGIMORE.soldvia (`productid`, `tid`, `noofitems`) VALUES ('1X3', 'T4949', '8'), ('6X6', 'T4949', '4');

-- Create new sales transaction and run the code below

SELECT sv.noofitems, sv.noofitems*p.productprice as RevenueGenerated, st.customerid, st.storeid, sv.productid, st.tdate, st.tid,'Sales' as RevenueType
FROM poudyas_ZAGIMORE.product p, poudyas_ZAGIMORE.soldvia sv, poudyas_ZAGIMORE.salestransaction st
WHERE p.productid = sv.productid
AND sv.tid = st.tid
AND st.tdate > (SELECT DATE((SELECT MAX(f_timestamp) FROM poudyas_ZAGIMORE_ds.CoreFact)));

-- procedure

SET DELIMITER $$ 

CREATE PROCEDURE DailyFactRefresh()

BEGIN 
	DROP TABLE intermediateRevenueFactTable;
	CREATE TABLE intermediateRevenueFactTable as
	SELECT sv.noofitems, sv.noofitems*p.productprice as RevenueGenerated, st.customerid, st.storeid, sv.productid, st.tdate, st.tid,'Sales' as RevenueType
	FROM poudyas_ZAGIMORE.product p, poudyas_ZAGIMORE.soldvia sv, poudyas_ZAGIMORE.salestransaction st
	WHERE p.productid = sv.productid
	AND sv.tid = st.tid
	AND st.tdate > (SELECT DATE((SELECT MAX(f_timestamp) FROM poudyas_ZAGIMORE_ds.CoreFact)));

	INSERT INTO CoreFact(CustomerKey, ProductKey, StoreKey, UnitsSold, RevenueGenerated, CalendarKey,TransactionID, f_timestamp, loaded, RevenueType)
	SELECT c.CustomerKey, p.ProductKey, s.StoreKey, i.noofitems, i.RevenueGenerated, ca.CalendarKey, i.tid, NOW(), FALSE, 'Sales'
	FROM intermediateRevenueFactTable i, Customer_Dimension c, Store_Dimension s, Product_Dimension p, Calendar_Dimension ca
	WHERE i.CustomerID = c.CustomerID
	AND i.StoreID = s.StoreID
	AND i.ProductID = p.ProductID
	AND LEFT(i.RevenueType,1) = LEFT(p.ProductType, 1)
	AND i.tdate = ca.fulldate;

	INSERT INTO poudyas_ZAGIMORE_DW.CoreFact(CustomerKey, ProductKey, StoreKey, UnitsSold, RevenueGenerated, CalendarKey, tid)
	SELECT CustomerKey, ProductKey, LocationKey, UnitsSold, RevenueGenerated, CalendarKey, tid
	FROM CoreFact
	WHERE loaded = 0;

	UPDATE CoreFact
	SET loaded = 1;
END 

-- 
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
