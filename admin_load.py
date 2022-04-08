#!/usr/bin/env python
# coding: utf-8

# In[ ]:
from pyspark.conf import SparkConf 
from pyspark.sql import SparkSession
from datetime import datetime
from dateutil.parser import parse
import pyspark.sql.functions as f
from pyspark.sql.types import *
#from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pyspark
import pandas as pd
import numpy as np
import re

def main():
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
        
    schema = StructType([ 
        StructField("IP", StringType(), True), 
        StructField("email", StringType(), True),
        StructField("request", StringType(), True),
        StructField("url", StringType(), True),
        StructField("status_code", StringType(), True),
        StructField("bytesize", IntegerType(), True),
        StructField("referer", StringType(), True),
        StructField("processing time", FloatType(), True),
        StructField("browser", StringType(), True),
        StructField("os", StringType(), True),
        StructField("device", StringType(), True),
        StructField("datetime", TimestampType(), True),
    ])
    
    for idx in ("01","02","03"):
        file = open(f"/home/jmyeong/tarfile/meta/admin_log/{idx}/admin_filename.txt", "r")
        strings = file.readlines()
        for string in strings:
            string = string.strip()
            web_log = spark.read.csv(f"hdfs://192.168.56.101:9000/admin_log/log_{idx}_final/{string}", schema=schema)
            
            # PostgreSQL 적재 
            web_log.write.mode("append").jdbc("jdbc:postgresql://localhost:5432/superset", f"public.admin{idx}",properties={"user": "postgres", "password": "1234"})
            
            print(f"{string}+' 적재완료'")
        file.close()
        
        
if __name__ == '__main__':
    main()

