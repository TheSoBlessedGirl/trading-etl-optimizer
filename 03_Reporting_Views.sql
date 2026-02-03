USE TradingDB;
GO

-- 1. PERFORMANCE OPTIMIZATION
-- We create a Non-Clustered Index on SecurityID and TradeTimestamp.
-- This makes searching for a specific stock's history lightning fast.
CREATE NONCLUSTERED INDEX IX_FactTrades_Security_Date 
ON Fact_Trades (SecurityID, TradeTimestamp);
GO

-- 2. BUSINESS INTELLIGENCE (The "Gold" Layer)
-- This view simplifies the data for a dashboard or a portfolio manager.
CREATE VIEW vw_DailySecurityPerformance AS
SELECT 
    S.Ticker,
    S.SecurityName,
    CAST(F.TradeTimestamp AS DATE) AS TradeDate,
    COUNT(F.FactTradeID) AS TotalTrades,
    SUM(F.TradeVolume) AS TotalVolume,
    AVG(F.TradePrice) AS AvgPrice,
    SUM(F.TransactionValue) AS TotalMarketValue
FROM Fact_Trades F
JOIN Dim_Security S ON F.SecurityID = S.SecurityID
GROUP BY S.Ticker, S.SecurityName, CAST(F.TradeTimestamp AS DATE);
GO

SELECT * FROM vw_DailySecurityPerformance
ORDER BY TradeDate DESC, TotalMarketValue DESC;


CREATE OR ALTER FUNCTION fn_Get_Trade_Volume_For_Security(@Ticker VARCHAR(10))
RETURNS INT
AS
BEGIN
    DECLARE @TotalVolume INT;

    SELECT @TotalVolume = SUM(F.TradeVolume)
    FROM Fact_Trades F
    JOIN Dim_Security S ON F.SecurityID = S.SecurityID
    WHERE S.Ticker = @Ticker;

    RETURN ISNULL(@TotalVolume, 0);
END;
GO

SELECT dbo.fn_Get_Trade_Volume_For_Security('AAPL') AS AppleVolume;
SELECT dbo.fn_Get_Trade_Volume_For_Security('GOOGL') AS GoogleVolume;
SELECT dbo.fn_Get_Trade_Volume_For_Security('MSFT') AS MicrosoftVolume;