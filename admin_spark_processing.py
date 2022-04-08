#!/usr/bin/env python
# coding: utf-8



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

class UserAgentParser: 
    from user_agents import parse #// VERSION: 2.2.0
    from collections import namedtuple
    UA3Layers = namedtuple(typename='UA3Layers', field_names=['str_browser_name', 'str_operating_system_name', 'str_hardware_type_name'])
    
    class HardwareType:
        SERVER   = 'Server'
        TABLET   = 'Tablet'
        PHONE    = 'Phone'
        COMPUTER = 'Computer'
        OTHER    = 'Other'
        
    def get_3layers(self, str_user_agent):
        user_agent = self.__class__.parse(str_user_agent)
        return self.__class__.UA3Layers(self.get_browser_name(user_agent), self.get_operating_system_name(user_agent), self.get_hardware_type(user_agent))
    
    def get_browser_name(self, user_agent):
        """
        Return: ['Chrome', 'Firefox', 'Opera', 'IE', 'Edge', 'Safari', ...]
        변환로직:
            'IE Mobile' >>> 'IE'
            'Mobile Safari' >>> 'Safari'
        """
        str_browser_name = user_agent.browser.family
        str_browser_name = str_browser_name.replace('Mobile', '').strip()
        return str_browser_name
    
    def get_operating_system_name(self, user_agent):
        """
        Return: ['Windows','Linux','Mac OS X','iOS','Android','OpenBSD','BlackBerry OS','Chrome OS',...]
        """
        str_operating_system_name = user_agent.os.family
        return str_operating_system_name
    
    def get_hardware_type(self, user_agent):
        if user_agent.is_bot:
            str_hardware_type_name = self.__class__.HardwareType.SERVER
        elif user_agent.is_tablet:
            str_hardware_type_name = self.__class__.HardwareType.TABLET
        elif user_agent.is_mobile:
            str_hardware_type_name = self.__class__.HardwareType.PHONE
        elif user_agent.is_pc:
            str_hardware_type_name = self.__class__.HardwareType.COMPUTER
        else:
            str_hardware_type_name = self.__class__.HardwareType.OTHER
        return str_hardware_type_name

def extract_browser(ua_string):
    userAgentParser = UserAgentParser()
    UA_info = userAgentParser.get_3layers(ua_string)
    return UA_info[0]

def extract_os(ua_string):
    userAgentParser = UserAgentParser()
    UA_info = userAgentParser.get_3layers(ua_string)
    return UA_info[1]

def extract_device(ua_string):
    userAgentParser = UserAgentParser()
    UA_info = userAgentParser.get_3layers(ua_string)
    return UA_info[2]


def trans_time(t):
    if t != '-':
        return str(pd.to_datetime(t,format='%d/%b/%Y:%H:%M:%S'))
    return t


def main():
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    
    schema = StructType([ 
        StructField("IP", StringType(), True), 
        StructField("identifier", StringType(), True), 
        StructField("email", StringType(), True),
        StructField("time", StringType(), True),
        StructField("request", StringType(), True),
        StructField("url", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("status_code", StringType(), True),
        StructField("bytesize", StringType(), True),
        StructField("referer", StringType(), True),
        StructField("user-agent", StringType(), True),
        StructField("go-agent", StringType(), True),
        StructField("processing time", StringType(), True),
    ])
    columns=["IP","identifier","email","time","request","url","protocol","status_code","bytesize","referer","user-agent","go-agent","processing time"]

    for idx in ("01","02","03"):
        file = open(f"/home/jmyeong/tarfile/meta/admin_log/{idx}/admin_filename.txt", "r")
        strings = file.readlines()
        for string in strings:
            string = string.strip()
            
            ##### 1. schema를 적용한 Dataframe 형태로 만들기 #####
            df = spark.read.csv(f"hdfs://192.168.56.101:9000/admin_log/log_{idx}/{string}", schema=schema)
             
            ##### 2. User Agents의 browser, OS, device 컬럼생성 및 기존컬럼제거 #####
            ext_browser_UDF = f.udf(lambda x: extract_browser(x))
            ext_os_UDF = f.udf(lambda x: extract_os(x))
            ext_device_UDF = f.udf(lambda x: extract_device(x))

            df = df.withColumn("browser", ext_browser_UDF(f.col("user-agent")))
            df = df.withColumn("os", ext_os_UDF(f.col("user-agent")))
            df = df.withColumn("device", ext_device_UDF(f.col("user-agent")))
            df = df.drop("user-agent")

            ##### 3. 발생할 수 있는 오류 확인 및 제거 #####
            df = df.filter((df.time  != "-") | (df.protocol  == "HTTP/1.0") | (df.protocol  == "HTTP/1.1"))


            ##### 4. row 제거 #####
            # device : bot ,개발과정에서 발생한 로그(크롤링, curl,Zabbix, okhttp 등) 제거 
            df = df.filter((df.device  != "Server") | (df.device  != "Other"))

            # browser : 개발과정에서 발생한 로그 제외
            select_browser = ["Chrome",'Other', 'Whale', 'Safari UI/WKWebView', 'IE', 'Edge', 'Chrome WebView', 'Safari', 'Firefox', 'Outlook', 'Samsung Internet', 'Opera','Chrome iOS','Firefox iOS','Outlook-iOS']
            df = df.filter(df.browser.isin(select_browser))

            # os : 알 수 없는 os, 개발과정에서 발생한 로그 제외
            df = df.filter((df.os != "Other"))
            
            ##### 5. column 제거 #####
            # identifier : identifier count 0
            df = df.drop("identifier")

            # go-agent :  user-agent의 (browser, os, device)를 파악함으로써 go-agent의 정보를 대체할 수 있다.
            df = df.drop("go-agent")

            # protocol : http/1.0으로 중복, status_code를 통하여 http/1.0 protocol를 유추가능
            df = df.drop("protocol")
            
            ##### 6. 변환 #####
            # 처리시간
            df = df.withColumn('processing time',                                 f.when(df["processing time"].isin('-'),f.regexp_replace(df["processing time"],'-',''))                                 .otherwise(df["processing time"]))

            # 요청 시간을 datetime string으로 변환
            def trans_time(t):
                return str(pd.to_datetime(t,format='%d/%b/%Y:%H:%M:%S'))

            time_UDF = f.udf(lambda x: trans_time(x))
            df = df.withColumn("datetime", time_UDF(f.col("time")))
            df = df.drop("time")


            #  형 변환 (Integer, Float, Datetime)
            df = df.withColumn('bytesize',                                 f.when(df["bytesize"].isin('-'),f.regexp_replace(df["bytesize"],'-','0')) .otherwise(df["bytesize"]))
            df = df.withColumn("bytesize", df["bytesize"].cast(IntegerType()))                     

            df = df.withColumn('processing time',                                 f.when(df["processing time"].isin('-'),f.regexp_replace(df["processing time"],'-','0')) .otherwise(df["processing time"]))
            df = df.withColumn("processing time", df["processing time"].cast(FloatType()))                

            df = df.withColumn("datetime",f.to_timestamp("datetime"))

            df.write.options(header='False', delimiter=',').csv(f"hdfs://192.168.56.101:9000/admin_log/log_{idx}_final/{string}")
            
            print(f"{string}+' 완료'")
        file.close()
        
        
if __name__ == '__main__':
    main()

 





