USE TradingDB;
GO

-- The ETL_Process_Trades Procedure
CREATE OR ALTER PROCEDURE ETL_Process_Trades
AS
BEGIN
    SET NOCOUNT ON; -- Prevents extra "rows affected" messages for performance

    -- 1. Create a Temp Table to hold our "Clean" data before final insertion
    -- This shows off memory-efficient processing
    IF OBJECT_ID('tempdb..#Clean_Trades') IS NOT NULL DROP TABLE #Clean_Trades;

    CREATE TABLE #Clean_Trades (
        TradeID INT,
        SecurityID INT,
        TradeTimestamp DATETIME,
        TradePrice DECIMAL(18, 4),
        TradeVolume INT
    );

    -- 2. VALIDATION & TRANSFORMATION
    -- We filter out the junk and join with Dim_Security to get the Foreign Key
    INSERT INTO #Clean_Trades (TradeID, SecurityID, TradeTimestamp, TradePrice, TradeVolume)
    SELECT 
        CAST(R.TradeID AS INT),
        S.SecurityID,
        CAST(R.TradeDate AS DATETIME),
        CAST(R.Price AS DECIMAL(18,4)),
        CAST(R.Volume AS INT)
    FROM Raw_Trade_Data R
    INNER JOIN Dim_Security S ON R.Ticker = S.Ticker -- Lookup transformation
    WHERE R.Price IS NOT NULL           -- Validation: Remove rows with no price
      AND R.Volume > 0                  -- Validation: Remove -999 or 0 volumes
      AND ISNUMERIC(R.Price) = 1;       -- Validation: Ensure price is a number

    -- 3. LOAD INTO PRODUCTION (Fact_Trades)
    -- We only insert rows that don't already exist (Avoids duplicates)
    INSERT INTO Fact_Trades (TradeID, SecurityID, TradeTimestamp, TradePrice, TradeVolume)
    SELECT CT.TradeID, CT.SecurityID, CT.TradeTimestamp, CT.TradePrice, CT.TradeVolume
    FROM #Clean_Trades CT
    LEFT JOIN Fact_Trades FT ON CT.TradeID = FT.TradeID
    WHERE FT.TradeID IS NULL; -- Only insert if TradeID doesn't already exist

    -- 4. CLEANUP (Optional)
    -- Truncate staging table so it's ready for the next batch
    TRUNCATE TABLE Raw_Trade_Data;

    PRINT 'ETL Process Completed Successfully!';
END;
GO

EXEC ETL_Process_Trades;

-- Test that the procedure worked
SELECT COUNT(*) FROM Fact_Trades; -- It should be around 1,900 rows—the messy rows should be gone
SELECT * FROM Fact_Trades WHERE TradeVolume < 0; -- This should return zero rows now!
SELECT * FROM Raw_Trade_Data; -- This should be empty, showing your pipeline is ready for the next day's data

