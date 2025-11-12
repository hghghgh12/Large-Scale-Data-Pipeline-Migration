#!/usr/bin/env python3
import yaml
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_date, year, when, row_number, lit

cfg = yaml.safe_load(open('config/pipeline_config.yaml'))
HDFS_RAW = cfg['hdfs_base'] + '/raw/employee_data'
HDFS_PROCESSED = cfg['hdfs_base'] + '/processed/employee_data'
HDFS_LOGS = cfg['logs_path']

def get_spark():
    return SparkSession.builder.appName(cfg['spark']['app_name']).getOrCreate()

def main():
    spark = get_spark()
    start = datetime.utcnow()
    df_raw = spark.read.option('header','true').csv(HDFS_RAW)
    df = (df_raw.withColumn('emp_id', col('emp_id').cast('int'))
               .withColumn('salary', col('salary').cast('double'))
               .withColumn('hire_date', to_date(col('hire_date'), 'yyyy-MM-dd'))
               .dropDuplicates(['emp_id']).filter(col('emp_id').isNotNull()).filter(col('salary').isNotNull()))

    current_year = datetime.utcnow().year
    df = df.withColumn('experience_years', lit(current_year) - year(col('hire_date')))
    df = df.withColumn('salary_band', when(col('salary') < 50000, 'Low').when((col('salary') >= 50000) & (col('salary') < 80000), 'Medium').otherwise('High'))
    df = df.withColumn('tenure_category', when(col('experience_years') < 2, 'Junior').when((col('experience_years') >= 2) & (col('experience_years') < 6), 'Mid').otherwise('Senior'))

    window = Window.partitionBy('location').orderBy(col('salary').desc())
    df_top = df.withColumn('rn', row_number().over(window)).filter(col('rn') <= 5).drop('rn')

    df_final = df.withColumn('year', lit(current_year))
    df_final.write.mode('overwrite').partitionBy('department','year').parquet(HDFS_PROCESSED)
    df_top.write.mode('overwrite').parquet(cfg['hdfs_base'] + '/processed/top_earners_by_location')

    end = datetime.utcnow()
    duration = (end - start).total_seconds()
    log_msg = f"{datetime.utcnow().isoformat()} - ETL completed in {duration} seconds\n"
    spark.sparkContext.parallelize([log_msg]).saveAsTextFile(HDFS_LOGS + '/spark_runs/' + datetime.utcnow().strftime('%Y%m%d%H%M%S'))

    spark.sql('CREATE DATABASE IF NOT EXISTS ' + cfg['hive']['database'])
    spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} (emp_id INT, name STRING, location STRING, salary DOUBLE, hire_date DATE, experience_years INT, salary_band STRING, tenure_category STRING) PARTITIONED BY (department STRING, year INT) STORED AS PARQUET LOCATION '{}'""".format(cfg['hive']['database'], cfg['hive']['table'], HDFS_PROCESSED))

    spark.stop()

if __name__ == '__main__':
    main()
