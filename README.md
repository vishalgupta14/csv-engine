# 🚀 CSV Engine

**CSV Engine** is a powerful Java-based transformation and analysis engine for CSV files. It combines **in-memory** and **SQL-style** processing via pluggable backends like **H2** and **PostgreSQL**, making it ideal for ETL pipelines, analytics, and data engineering workflows.

---

## ✨ Features

### ✅ Data Loading
- Load CSV into memory or DB (`H2` / user-provided)
- Auto fallback to DB for large files
- Schema inference from header/data

### ✅ In-Memory Processing
- Stream API: `.stream()`, `.mapTo(Class)`, `.toList()`
- Filtering, limiting, skipping
- Grouping and aggregation
- Write back to CSV
- Schema validation

### ✅ SQL-Like DB Mode (via `CsvDbLoader`)
- Joins: `INNER`, `LEFT`, `RIGHT`, `FULL`, `NATURAL`
- Unions: `UNION`, `UNION ALL`
- Multi-table joins via `joinMultiple(...)`
- Preview data with `.preview("view", n)`
- Query with raw SQL
- Create reusable views with `.createView(...)`

### ✅ Smart Detection & Optimizations
- `detectDelimiter()` → auto-detect `,`, `;`, `\t`
- `autoFallbackToDbIfLarge()` → load to DB if file > X MB
- `detectDuplicates(...)` → row-level duplicate keys
- `detectDataAnomalies()` → outlier/high variance checks

### ✅ Schema & Validation
- `.getHeaders()` → get column names
- `.inferSchema()` → detect column data types (int, double, string)
- `.validateAgainstSchema()` → strict schema enforcement
- `.checkRequiredColumns("id", "name")` → verify essential columns exist

### ✅ Backend Support
- Default: H2 in-memory
- Pluggable: PostgreSQL, MySQL, etc. via `DatabaseBackend` interface

---

## 🏗 Architecture

```text
 CsvSource (InMemory)       CsvDbLoader (SQL-backed)
      |                            |
   stream()                  loadToDb()
      |                            |
   map, filter, etc         joinWith(...), unionWith(...)
      |                            |
  toList(), writeToCsv()     query(), preview(), createView()
