# Apache PySpark Lending Club Project

## Overview

This project implements a loan score calculation system using Apache PySpark. The loan score is determined by three key factors that help assess the creditworthiness of loan applicants.

## Loan Score Calculation Factors

The loan score calculation depends on 3 primary factors:

1. **Loan Payment History** - Loan repayments history for previous loans (if any)
2. **Customer's Financial Health** - Overall financial stability indicators  
3. **Loan Defaulters History** - Delinquencies, Public records, Bankruptcies, Enquiries

### Required Tables

To calculate the loan score, two important tables are required:
- **Delinq** - Delinquency records
- **Details Table** - Contains public records, bankruptcies, and enquiries information

## Data Processing Pipeline

### 1. Data Cleaning and Processing

The raw loan defaulter data is cleaned and processed using the following transformations:

```python
# Clean delinq_2yrs column
loans_def_processed_df = loans_def_raw_df.withColumn("delinq_2yrs", 
    col("delinq_2yrs").cast("integer")).fillna(0, subset = ["delinq_2yrs"])

# Clean pub_rec column  
loans_def_processed_df.withColumn("pub_rec", 
    col("pub_rec").cast("integer")).fillna(0, subset = ["pub_rec"])

# Clean pub_rec_bankruptcies column
loans_def_processed_pub_rec_bankruptcies_df = loans_def_processed_pub_rec_df
    .withColumn("pub_rec_bankruptcies", 
    col("pub_rec_bankruptcies").cast("integer")).fillna(0, subset = ["pub_rec_bankruptcies"])

# Clean inq_last_6mths column
Loans_def_processed_inq_last_6mths_df = loans_def_processed_pub_rec_bankruptcies_df
    .withColumn("inq_last_6mths", 
    col("inq_last_6mths").cast("integer")).fillna(0, subset = ["inq_last_6mths"])
```

### 2. Creating Temporary Views

```python
loans_def_p_inq_last_6mths_df.createOrReplaceTempView("loan_defaulters")
```

### 3. Data Export

The processed data is exported in both CSV and Parquet formats:

```python
# CSV Format
loans_def_detail_records_enq_df.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path"), "/user/itv019319/lendingclubproject/raw/cleaned/loans_def_detail_records_enq_df_csv") \
    .save()

# Parquet Format  
loans_def_detail_records_enq_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path"), "/user/itv019319/lendingclubproject/raw/cleaned/loans_def_detail_records_enq_df_parquet") \
    .save()
```

### Final Cleaned Datasets

![Final Cleaned Datasets](https://github.com/BakarAhmad/Lendingclubproject/blob/main/images/Screenshot%202025-08-23%20at%204.41.57%E2%80%AFPM.png)

## Database and Table Management

### Business Requirement 1: Permanent Table Creation

Multiple teams require access to cleaned data through simple SQL queries. 

#### Table Types:

1. **Managed Table**: Data and metadata stored in default location. Dropping the table removes both data and metadata.
2. **External Table**: Data stored externally. Dropping only removes metadata, preserving actual data.

**Recommendation**: Use External Tables for multi-team access to prevent accidental data loss.

### Spark Session Configuration

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()
spark = SparkSession. \
    builder. \
    config('spark.ui.port','0'). \
    config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()
```

### Creating Permanent Tables

#### Step 1: Create Database
```sql
CREATE DATABASE itv019319_lending_club
```

#### Step 2: Create External Table
```sql
CREATE EXTERNAL TABLE itv019319_lending_club.customers(
    member_id string, emp_title string, emp_length int,
    home_ownership string, annual_income float, address_state string, 
    address_zipcode string, address_country string, grade string, 
    sub_grade string, verification_status string, total_high_credit_limit float,
    application_type string, join_annual_income float,
    verification_status_joint string, ingest_date timestamp)
STORED AS parquet 
LOCATION '/public/trendytech/lendingclubproject/cleaned/customers_parquet'
```

#### Step 3: View Data
```sql
SELECT * FROM itv019319_lending_club.customers
```

### Business Requirement 2: Consolidated View

Create a single consolidated view of all datasets with latest data:

```sql
CREATE OR REPLACE VIEW itv019319_lending_club.customers_loan_v AS 
SELECT 
    l.loan_id, c.member_id, c.emp_title, c.emp_length, c.home_ownership,
    c.annual_income, c.address_state, c.address_zipcode, c.address_country,
    c.grade, c.sub_grade, c.verification_status, c.total_high_credit_limit,
    c.application_type, c.join_annual_income, c.verification_status_joint,
    l.loan_amount, l.funded_amount, l.loan_term_years, l.interest_rate,
    l.monthly_installment, l.issue_date, l.loan_status, l.loan_purpose,
    r.total_principal_received, r.total_interest_received, r.total_late_fee_received,
    r.last_payment_date, r.next_payment_date, d.delinq_2yrs, d.delinq_amnt,
    d.mths_since_last_delinq, e.pub_rec, e.pub_rec_bankruptcies, e.inq_last_6mths
FROM itv019319_lending_club.customers c
LEFT JOIN itv019319_lending_club.loans l ON c.member_id = l.member_id
LEFT JOIN itv019319_lending_club.loans_repayments r ON l.loan_id = r.loan_id
LEFT JOIN itv019319_lending_club.loans_defaulters_delinq d ON c.member_id = d.member_id
LEFT JOIN itv019319_lending_club.loans_defaulters_detail_rec_enq e ON c.member_id = e.member_id
```

**Note**: Views are fast to create but slow to query due to real-time joins. For faster access, consider pre-calculated tables with weekly updates.

## Loan Score Calculation

### Scoring Criteria

Higher loan scores indicate better chances of loan approval.

### Factor Weights

- **Loan Repayment History**: 20%
- **Loan Defaulters History**: 45% 
- **Financial Health**: 35%

### Data Quality Management

#### Identifying Bad Data
```sql
SELECT member_id FROM (
    SELECT member_id, count(*) as total 
    FROM itv019319_lending_club.customers
    GROUP BY member_id 
    HAVING total > 1
)
```

#### Cleaning Process
1. Identify duplicate member_ids
2. Create consolidated bad data CSV file
3. Share with upstream teams for correction
4. Create clean datasets excluding bad data

### Scoring Configuration

![Scoring Configuration](YOUR_IMAGE_LINK_HERE)

```python
# Configure scoring thresholds
spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
spark.conf.set("spark.sql.very_bad_rated_pts", 100)
spark.conf.set("spark.sql.bad_rated_pts", 250)
spark.conf.set("spark.sql.good_rated_pts", 500)
spark.conf.set("spark.sql.very_good_rated_pts", 650)
spark.conf.set("spark.sql.excellent_rated_pts", 800)
```

### Factor 1: Loan Repayment History (20%)

Calculates points based on last payment amount and total payments received compared to loan obligations.

### Factor 2: Loan Defaulters History (45%)

Evaluates:
- Delinquencies in past 2 years
- Public records
- Bankruptcies  
- Recent credit inquiries

### Factor 3: Financial Health (35%)

Assesses:
- Loan status
- Home ownership
- Credit utilization
- Grade ratings

### Final Score Calculation

```python
loan_score = spark.sql("""
    SELECT member_id,
        ((last_payment_pts + total_payment_pts) * 0.20) as payment_history_pts,
        ((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) as defaulters_history_pts,
        ((loan_status_pts + home_pts + credit_limit_pts + grade_pts) * 0.35) as financial_health_pts
    FROM fh_ldh_ph_pts
""")

final_loan_score = loan_score.withColumn('loan_score', 
    loan_score.payment_history_pts + 
    loan_score.defaulters_history_pts + 
    loan_score.financial_health_pts)
```

### Grade Assignment

Final grades (A-F) are assigned based on score thresholds:
- **A**: Excellent (> very_good_grade_pts)
- **B**: Very Good 
- **C**: Good
- **D**: Poor
- **E**: Very Poor
- **F**: Unacceptable

## Output

The final loan scores and grades are stored in the processed folder in HDFS for downstream consumption.

## Usage

1. Set up Spark session with Hive support
2. Load and clean raw data
3. Create external tables and views
4. Configure scoring parameters
5. Run loan score calculation pipeline
6. Export results to processed data folder

## Requirements

- Apache Spark with Hive support
- HDFS access
- Python/PySpark environment
- Access to lending club datasets