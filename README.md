# End-to-end High-Volume Trading ETL & Data Optimizer

## 📌 Project Overview
This project simulates a real-world FinTech data pipeline. It takes "messy" raw trading data (simulated via Python/Yahoo Finance), cleans it using SQL Server (T-SQL), and transforms it into a production-ready analytical format.

**Goal:** To ensure data integrity and high-performance reporting for 2,000+ daily trade records.

---

## 🛠️ Tech Stack
* **Python:** Data generation & "chaos injection" (creating nulls/errors).
* **SQL Server (SSMS):** Staging, ETL processing, and indexing.
* **T-SQL:** Advanced stored procedures and views.

---

## 🚀 The Pipeline (Medallion Architecture)

### 1. The Landing Zone (Bronze)
* Data is imported from `messy_stock_data.csv` into `Raw_Trade_Data`.
* Types are kept as `VARCHAR` to ensure no data loss during ingestion.

### 2. The Clean Room (Silver)
* **Validation:** Stored procedure `ETL_Process_Trades` filters out negative volumes and null prices.
* **Transformation:** Tickers are mapped to `SecurityIDs` using a relational join.
* **Deduplication:** The pipeline ensures only new `TradeIDs` are processed (Idempotency).

### 3. The Analytics Layer (Gold)
* A non-clustered index was added to `Fact_Trades` to optimize query performance.
* The `vw_DailySecurityPerformance` view provides instant business insights (Total Volume, Avg Price, Market Value).

---

## 📈 Key SQL Features Demonstrated
* **Stored Procedures:** Automated the entire cleaning logic.
* **Temp Tables:** Used for memory-efficient data processing.
* **Error Handling:** Implemented logic to catch anomalous financial data.
* **Data Modeling:** Established a Fact/Dimension relationship.

---

## 📝 How to Run
1. Execute `01_Schema_Setup.sql` to build the database.
2. Import the provided CSV using the SSMS Import Wizard and save the file as "Raw_Trade_Data".
3. Run `02_ETL_Stored_Procedure.sql` and execute the procedure.
4. View results via `03_Reporting_Views.sql`.
