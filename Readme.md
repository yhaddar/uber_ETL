# 🚖 Uber ETL Pipeline & BI Dashboard

![Python](https://img.shields.io/badge/Python-3.14-blue?style=for-the-badge\&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-orange?style=for-the-badge\&logo=apachespark)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20Warehouse-29B5E8?style=for-the-badge\&logo=snowflake)
![PowerBI](https://img.shields.io/badge/PowerBI-Dashboard-yellow?style=for-the-badge\&logo=powerbi)

---

# 📌 Project Overview

This project is a complete **ETL Pipeline** built using **PySpark**, **Snowflake**, and **Power BI**.
The goal of this project is to extract Uber booking data from Kaggle, clean and transform the data using SparkSQL, load it into a Snowflake Data Warehouse, and finally build an interactive BI dashboard in Power BI.

---

# 🛠️ Technologies Used

| Technology   | Description                    |
| ------------ | ------------------------------ |
| 🐍 Python    | Main programming language      |
| ⚡ PySpark    | Big Data processing            |
| 🗄️ SparkSQL | Data transformation            |
| ❄️ Snowflake | Cloud Data Warehouse           |
| 📊 Power BI  | Dashboard & Data Visualization |

---

# 📂 ETL Architecture

```text
Kaggle Excel Dataset
        ↓
Extract Data using PySpark
        ↓
Transform Data using SparkSQL
        ↓
Load Clean Data into Snowflake
        ↓
Build BI Dashboard using Power BI
```

---

# 📥 Extract Phase

The Uber dataset was downloaded from Kaggle in Excel format.

### ✅ Extraction Process

* Load Excel dataset using PySpark
* Create Spark Session
* Read dataset into DataFrame
* Validate schema and columns

---

# 🔄 Transform Phase

Data cleaning and transformation were done using **SparkSQL**.

## ✅ Data Cleaning Operations

### ✔ Remove Duplicate Rows

```python
1224 duplicated rows removed
148767 unique rows remaining
```

### ✔ Handle Missing Values

The following columns had null values cleaned and updated:

* Cancelled Rides by Customer
* Reason for cancelling by Customer
* Cancelled Rides by Driver
* Driver Cancellation Reason
* Incomplete Rides
* Incomplete Rides Reason
* Booking Value
* Ride Distance
* Driver Ratings
* Customer Rating
* Payment Method

### ✔ Data Standardization

* Convert column names to lowercase
* Replace spaces with underscores
* Format Date & Time columns
* Round ratings to 2 decimal places

---

# 🧠 Spark Transformation Logs

```logs
2026-05-09 13:52:02,986 - spark session - INFO - Start to connecting to Spark...
2026-05-09 13:52:10,161 - spark session - INFO - Connected To Spark

2026-05-09 13:52:10,162 - transform - INFO - remove duplicate rows...
2026-05-09 13:52:11,227 - transform - INFO - 1224 row was removed because is duplicated
2026-05-09 13:52:11,568 - transform - INFO - 148767 unique rows

2026-05-09 13:52:12,652 - transform - INFO - updating Cancelled Rides by Customer row...
2026-05-09 13:52:14,070 - transform - INFO - updating Reason for cancelling by Customer...
2026-05-09 13:52:14,793 - transform - INFO - updating Cancelled Rides by Driver row...
2026-05-09 13:52:15,334 - transform - INFO - updating Incomplete Rides row...
2026-05-09 13:52:16,038 - transform - INFO - updating Driver Cancellation Reason...
2026-05-09 13:52:16,666 - transform - INFO - updating Incomplete Rides Reason row...
2026-05-09 13:52:17,096 - transform - INFO - updating Book value row...
2026-05-09 13:52:17,704 - transform - INFO - updating Ride Distance row...
2026-05-09 13:52:18,564 - transform - INFO - updating Driver Ratings row...
2026-05-09 13:52:19,259 - transform - INFO - updating Customer Rating row...
2026-05-09 13:52:19,806 - transform - INFO - updating Payment Method row...
2026-05-09 13:52:19,813 - transform - INFO - updating Type of Date row...
2026-05-09 13:52:19,819 - transform - INFO - updating Type of Time row...
2026-05-09 13:52:20,701 - transform - INFO - verifying data...
2026-05-09 13:52:22,659 - load - INFO - creating data warehouse using Hive in progress...
```

---

# ☁️ Load Phase

After transforming the data:

* Clean data was loaded into **Snowflake Cloud Data Warehouse**
* Tables were created automatically
* Data was validated after loading

---

# 📊 Power BI Dashboard

The final step was creating an interactive dashboard in Power BI.

## Dashboard Features

### 📈 KPIs

* Total Bookings
* Completed Bookings
* Revenue
* Average Distance
* Lost Bookings
* Driver Rating
* Customer Rating

### 📉 Visualizations

* Revenue by Vehicle Type
* Monthly Booking Trends
* Revenue Analysis
* Customer & Driver Ratings
* Booking Status Distribution

---

# 🖼️ Dashboard Preview

![Dashboard](https://raw.githubusercontent.com/yhaddar/uber_ETL/refs/heads/main/images/dashboard.png)

---

# 🚀 How To Run The Project

## 1️⃣ Clone Repository

```bash
git clone https://github.com/your-username/uber-dashboard.git
cd uber-dashboard
```

## 2️⃣ Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate
```

## 3️⃣ Install Requirements

```bash
pip install pyspark
```

## 4️⃣ Run ETL Pipeline

```bash
python main.py
```

---

# 📁 Project Structure

```text
uber_dashboard/
│
├── logs/
│    ├── uber.log
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── spark_session.py
│   └── logs.py
│   └── config.py
│
├── dashboard/
├── main.py
└── README.md
```

---

# 🎯 Project Goals

* Practice Big Data Engineering
* Build a complete ETL pipeline
* Work with SparkSQL transformations
* Use Snowflake as cloud warehouse
* Create professional BI dashboards

---

# 👨‍💻 Author

## Youssef Haddar

Backend Developer • Data Engineering Enthusiast • BI Developer

---

# ⭐ Result

This project demonstrates a complete modern data engineering workflow:

✅ Data Extraction

✅ Data Cleaning & Transformation

✅ Cloud Data Warehouse Integration

✅ Business Intelligence Dashboard

✅ ETL Logging & Monitoring

✅ Big Data Processing with Spark
