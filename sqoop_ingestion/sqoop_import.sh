#!/bin/bash
set -euo pipefail
MYSQL_HOST="localhost"
MYSQL_PORT=3306
MYSQL_DB="Employees_data"
MYSQL_USER="divithraju"
MYSQL_PASS="divith4321"
TABLE="employee_data"
HDFS_RAW="/user/divithraju/hr_project/raw/employee_data"
NUM_MAPPERS=4
SPLIT_BY=emp_id

hdfs dfs -mkdir -p ${HDFS_RAW} || true
hdfs dfs -chown -R divithraju: ${HDFS_RAW} || true

sqoop import \
  --connect jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB} \
  --username ${MYSQL_USER} --password ${MYSQL_PASS} \
  --table ${TABLE} \
  --target-dir ${HDFS_RAW} \
  --as-textfile --fields-terminated-by ',' \
  --split-by ${SPLIT_BY} --num-mappers ${NUM_MAPPERS}

echo "$(date -u) - SQOOP import finished"
