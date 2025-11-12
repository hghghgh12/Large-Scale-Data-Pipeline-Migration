#!/usr/bin/env python3
import yaml, json, tempfile, subprocess
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
cfg = yaml.safe_load(open('config/pipeline_config.yaml'))
hdfs_processed = cfg['hdfs_base'] + '/processed/employee_data'
hdfs_validation = cfg['validation_path']
local_source = 'data/input/employee_data.csv'

spark = SparkSession.builder.appName('DataValidation').getOrCreate()
df_proc = spark.read.parquet(hdfs_processed)
proc_count = df_proc.count()
proc_salary_sum = df_proc.groupBy().sum('salary').collect()[0][0] or 0.0
src_df = pd.read_csv(local_source)
src_count = len(src_df)
src_salary_sum = src_df['salary'].sum()
report = {'timestamp': datetime.utcnow().isoformat(), 'source_count': int(src_count), 'processed_count': int(proc_count), 'source_salary_sum': float(src_salary_sum), 'processed_salary_sum': float(proc_salary_sum), 'count_match': src_count == proc_count, 'salary_sum_close': abs(src_salary_sum - proc_salary_sum) < 1.0}
tmp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
tmp.write(json.dumps(report, indent=2))
tmp.close()
subprocess.run(['hdfs','dfs','-mkdir','-p',hdfs_validation], check=False)
subprocess.run(['hdfs','dfs','-put','-f', tmp.name, hdfs_validation + '/report_' + datetime.utcnow().strftime('%Y%m%d%H%M%S') + '.json'], check=True)
print('Validation report uploaded to HDFS')
spark.stop()
