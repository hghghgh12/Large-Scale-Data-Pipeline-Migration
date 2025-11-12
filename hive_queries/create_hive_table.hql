CREATE DATABASE IF NOT EXISTS hr_warehouse;
USE hr_warehouse;
CREATE EXTERNAL TABLE IF NOT EXISTS employee_data (
  emp_id INT,
  name STRING,
  location STRING,
  salary DOUBLE,
  hire_date DATE,
  experience_years INT,
  salary_band STRING,
  tenure_category STRING
)
PARTITIONED BY (department STRING, year INT)
STORED AS PARQUET
LOCATION 'hdfs://localhost:4444/user/divithraju/hr_project/processed/employee_data';
