CREATE DATABASE IF NOT EXISTS Employees_data;
USE Employees_data;

DROP TABLE IF EXISTS employee_data;
CREATE TABLE employee_data (
  emp_id INT PRIMARY KEY,
  name VARCHAR(100),
  department VARCHAR(50),
  location VARCHAR(50),
  salary DOUBLE,
  hire_date DATE
);

-- Use LOAD DATA LOCAL INFILE to populate from data/input/employee_data.csv
