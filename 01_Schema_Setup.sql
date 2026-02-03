-- Create the database for the staging area

CREATE DATABASE TradingDB;
GO

USE TradingDB;
GO

-- Create the tables
-- 1. THE LANDING ZONE (Loose types to accept "messy" data)
CREATE TABLE Raw_Trade_Data (
    TradeID VARCHAR(50),
    TradeDate VARCHAR(50),
    Ticker VARCHAR(10),
    Price VARCHAR(50), -- VARCHAR because we expect some NULLs or "NA"
    Volume VARCHAR(50) -- VARCHAR to handle those -999 errors
);--Dropped because it will be imported

DROP TABLE Raw_Trade_Data

-- 2. THE LOOKUP TABLE (Dimension)
CREATE TABLE Dim_Security (
    SecurityID INT IDENTITY(1,1) PRIMARY KEY,
    Ticker VARCHAR(10) UNIQUE,
    SecurityName VARCHAR(100)
);

-- 3. THE PRODUCTION TABLE (Fact - Strict, precise types)
CREATE TABLE Fact_Trades (
    FactTradeID INT PRIMARY KEY IDENTITY(1,1),
    TradeID INT, 
    SecurityID INT,
    TradeTimestamp DATETIME,
    TradePrice DECIMAL(18, 4), -- High precision for finance
    TradeVolume INT,
    TransactionValue AS (TradePrice * TradeVolume), -- Calculated column
    CONSTRAINT FK_Security FOREIGN KEY (SecurityID) REFERENCES Dim_Security(SecurityID)
);

-- Populate the look up table
INSERT INTO Dim_Security (Ticker, SecurityName)
VALUES ('AAPL', 'Apple Inc.'),
       ('GOOGL', 'Alphabet Inc.'),
       ('MSFT', 'Microsoft Corp.'),
       ('TSLA', 'Tesla, Inc.');


-- Ensuring Raw_Trade_Data is successfully imported
SELECT TOP 10 * FROM Raw_Trade_Data;
SELECT * FROM Raw_Trade_Data WHERE Price IS NULL OR Volume = '-999';





