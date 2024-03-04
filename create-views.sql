---- Part 5------
-- adding total number of footwear items sold to our daily snapshot (Alternatively, you may do sum of revenue if you do not have UnitsSold)
CREATE TABLE FootwearTemp
SELECT SUM(cf.UnitsSold) AS FootwearUnitsSold, cf.StoreKey, cf.CalendarKey
FROM CoreFact cf, Product_Dimension pd
WHERE pd.CategoryName="Footwear" AND cf.ProductKey=pd.ProductKey
GROUP BY cf.StoreKey, cf.CalendarKey

-- merging daily snapshot and footwear temp table
CREATE TABLE DailySnapshotWithFootwear AS
SELECT ds.AvgRevenuePerTransaction, ds.CalendarKey, ds.NumTransactions, ds.StoreKey, ds.TotalRevenue, ft.FootwearUnitsSold
FROM DailyStoreSnapShot ds LEFT JOIN FootwearTemp ft
ON(ds.CalendarKey = ft.CalendarKey) AND (ds.StoreKey = ft.StoreKey)


--extracting transaction count over $100 into the daily snapshot
CREATE TABLE ExpensiveTemp as
SELECT SUM(t.TransactionCount) as ExpensiveTransationCount, t.LocationKey, t.CalendarKey
FROM
(
SELECT COUNT(DISTINCT cf.tid) as TransactionCount, SUM(cf.RevenueGenerated) as TotalRevenueGenerated, cf.LocationKey, cf.CalendarKey, cf.tid
FROM CoreFact cf
GROUP BY cf.CalendarKey, cf.LocationKey, cf.tid
) as t
WHERE t.TotalRevenueGenerated > 100
GROUP BY t.CalendarKey, t.LocationKey

-- adding revenue by local customers to our daily snapshot
CREATE TABLE LocalRevenueTemp AS
SELECT SUM(cf.RevenueGenerated) AS TotalLocalRevenue, cf.StoreKey, cf.CalendarKey
FROM CoreFact cf, StoreDimension sd, CustomerDimension cd
WHERE cf.StoreKey = sd.StoreKey
AND cf.CustomerKey = cd.CustomerKey
AND LEFT(cd.CustomerZip,2)=LEFT(sd.StoreZIP,2)
GROUP BY cf.StoreKey, cf.CalendarKey


-- merging daily snapshot w/ footwear and local transaction temp table
CREATE TABLE DailySnapshotWithFootwearAndLocal AS SELECT ds.AvgRevenuePerTransaction, ds.CalendarKey, ds.NumTransactions, ds.LocationKey, ds.TotalRevenue, ds.FootwearUnitsSold, lt.TotalLocalRevenue
FROM DailySnapshotWithFootwear ds
LEFT JOIN LocalRevenueTemp lt ON ( ds.CalendarKey = lt.CalendarKey )
AND (ds.LocationKey = lt.LocationKey)

-- merging daily snapshot with everything
CREATE TABLE FullDailySnapshot AS
SELECT ds.AvgRevenuePerTransaction, ds.CalendarKey, ds.NumTransactions, ds.LocationKey, ds.TotalRevenue, ds.FootwearUnitsSold, ds.TotalLocalRevenue, et.ExpensiveTransationCount
FROM DailySnapshotWithFootwearAndLocal ds LEFT JOIN ExpensiveTemp et
ON(ds.CalendarKey = et.CalendarKey) AND (ds.LocationKey = et.LocationKey)

-- update nulls
UPDATE FullDailySnapshot
SET FootwearUnitsSold = 0
WHERE FootwearUnitsSold IS NULL;

UPDATE FullDailySnapshot
SET TotalLocalRevenue = 0
WHERE TotalLocalRevenue IS NULL;

UPDATE FullDailySnapshot
SET ExpensiveTransationCount = 0
WHERE ExpensiveTransationCount IS NULL;

-- create snapshot in data warehouse (runs in data staging)
CREATE TABLE poudyas_ZAGIMORE_DW.DailyStoreSnapshot AS
SELECT * FROM FullDailySnapshot;

--update the fk's in the Data Warehouse
ALTER TABLE DailyStoreSnapshot
ADD Foreign Key (CalendarKey) REFERENCES Calendar_Dimension(CalendarKey),
ADD Foreign Key (LocationKey) REFERENCES Store_Dimension(StoreKey)

ALTERNATIVE WAY TO ADD EXTRA TO THE DAILY SNAPSHOT:

-- DONE ^

CREATE TABLE FootwearTempRevenue
SELECT SUM(cf.UnitsSold) AS FootwearUnitsSold, sum(cf.RevenueGenerated) as FootwearRevenue, cf.LocationKey, cf.CalendarKey
FROM CoreFact cf, Product_Dimension pd
WHERE pd.CategoryName="Footwear" AND cf.ProductKey=pd.ProductKey
GROUP BY cf.LocationKey, cf.CalendarKey

--footwear metrics--
CREATE FootwearTemp
SELECT SUM(cf.UnitsSold) AS FootwearUnitsSold, sum(cf.RevenueGenerated) as FootwearRevenue, cf.LocationKey, cf.CalendarKey
FROM CoreFact cf, Product_Dimension pd
WHERE pd.CategoryName="Footwear" AND cf.ProductKey=pd.ProductKey
GROUP BY cf.LocationKey, cf.CalendarKey

UPDATE DailyStoreSnapShot DSS, FootwearTemp FT
SET DSS.FootwearUnitsSold = FT.FootwearUnitsSold, DSS.FootwearRevenue = FT.FootwearRevenue
WHERE DSS.LocationKey=cf.LocationKey and DSS.CalendarKey=cf.CalendarKey

--expensive transactions--
CREATE Table poudyas_ZAGIMORE_DW.ExpensiveTemp AS
SELECT * FROM ExpensiveTemp

UPDATE DailyStoreSnapShot DSS, ExpensiveTemp ET
SET DSS.ExpensiveTransationCount = ET.ExpensiveTransationCount
WHERE DSS.LocationKey=ET.LocationKey and DSS.CalendarKey=ET.CalendarKey

--local revenue--
CREATE VIEW poudyas_ZAGIMORE_DW.LocalRevenueTemp AS
SELECT * FROM LocalRevenueTemp

UPDATE DailyStoreSnapShot DSS, LocalRevenueTemp LRT
SET DSS.TotalLocalRevenue = LRT.TotalLocalRevenue
WHERE DSS.LocationKey=LRT.LocationKey and DSS.CalendarKey=LRT.CalendarKey

--updating DW DSS with data from DSS in DS--
UPDATE poudyas_ZAGIMORE_ds.DailyStoreSnapShot DS, poudyas_ZAGIMORE_DW.DailyStoreSnapShot DW
SET DW.TotalLocalRevenue=DS.TotalLocalRevenue, DW.FootwearItemsSold=DS.FootwearItemsSold, DW.FootwearRevenue=DS.FootwearRevenue, DW.ExpensiveTransationCount=DS.ExpensiveTransationCount
WHERE DW.LocationKey=DS.LocationKey and DW.CalendarKey=DS.CalendarKey