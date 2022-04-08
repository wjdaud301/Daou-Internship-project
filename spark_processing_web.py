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
import os
import findspark
findspark.find()
findspark.init(os.environ.get("$SPARK_HOME"))

## UserAgentParser
# 1/6 ~ 3/17
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

#sc = pyspark.SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))


def col_extract0(s): 
    if s[0]: return s[0] 
    else : return '-'
def col_extract1(s): 
    if s[1]: return s[1] 
    else : return '-'
def col_extract2(s): 
    if s[2]: return s[2] 
    else : return '-'
def col_extract3(s): 
    if s[3]: return s[3] 
    else : return '-'
def col_extract4(s): 
    if s[4]: return s[4] 
    else : return '-'
def col_extract5(s): 
    if s[5]: return s[5] 
    else : return '-'
def col_extract6(s): 
    if s[6]: return s[6] 
    else : return '-'
def col_extract7(s): 
    if s[7]: return s[7] 
    else : return '-'
def col_extract8(s): 
    if s[8]: return s[8] 
    else : return '-'
def col_extract9(s): 
    if s[9]: return s[9]
    else : return '-'
def col_extract10(s): 
    if s[10]: return s[10] 
    else : return '-' 
def col_extract11(s): 
    if s[11]: return s[11] 
    else : return '-'
def col_extract12(s): 
    if s[12]: return s[12] 
    else : return '-'



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

def trans_email(t):
    if t != '': 
        return t.split("@")[0]
    return t

def make_df(s):
    row, tmp = [], []
    s = s.replace('[', '"').replace(']','"')
    s =s.split('"')
    for i in s:
        if i == ' ':
            continue
        tmp.append(i)

    for i in tmp[:3]:
        row += i.split()

    row += tmp[3:]

    del row[4]
    if len(row) != 13:
        return
    return row

''' processing : 
1. 읽은 파일을 데이터 프레임으로 변환
2. 컬럼 제거 / 제거기준 : 클라이언트 식별자, 프로토콜 ,go-agent 식별정보
3. email -> userid를 뽑아내기
4. 요청 시간 -> datetime형태로 변환
5. UA -> browser, OS, device -> 기존컬럼제거
6  요청처리시간 오류 제거
7. type 변환
'''

def main():
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    cnt = 0
    for idx in ("01","02","03"):
        
        file = open(f"/home/jmyeong/tarfile/meta/web_log/{idx}/web_filename.txt", "r")
        strings = file.readlines()
        for string in strings:
            string = string.strip()
            web_log = spark.read.text(f"hdfs://192.168.56.101:9000/web_log/log_{idx}/{string}")
            columns=["IP","identifier","email","time","request","url","protocol","status_code","bytesize","referer","user-agent","go-agent","processing time"]
            try:

                # 1. Dataframe으로 변환
                make_UDF = f.udf(lambda x: make_df(x))
                tmp = web_log.select(make_UDF(f.col("value")).alias("value"))


                # 2. 컬럼생성
                col_ex0 = f.udf(lambda x: col_extract0(x))
                col_ex1 = f.udf(lambda x: col_extract1(x))
                col_ex2 = f.udf(lambda x: col_extract2(x))
                col_ex3 = f.udf(lambda x: col_extract3(x))
                col_ex4 = f.udf(lambda x: col_extract4(x))
                col_ex5 = f.udf(lambda x: col_extract5(x))
                col_ex6 = f.udf(lambda x: col_extract6(x))
                col_ex7 = f.udf(lambda x: col_extract7(x))
                col_ex8 = f.udf(lambda x: col_extract8(x))
                col_ex9 = f.udf(lambda x: col_extract9(x))
                col_ex10 = f.udf(lambda x: col_extract10(x))
                col_ex11 = f.udf(lambda x: col_extract11(x))
                col_ex12 = f.udf(lambda x: col_extract12(x))
                tmp =tmp.withColumn(columns[0],col_ex0(f.col("value")))
                tmp =tmp.withColumn(columns[1],col_ex1(f.col("value")))
                tmp =tmp.withColumn(columns[2],col_ex2(f.col("value")))
                tmp =tmp.withColumn(columns[3],col_ex3(f.col("value")))
                tmp =tmp.withColumn(columns[4],col_ex4(f.col("value")))
                tmp = tmp.withColumn(columns[5],col_ex5(f.col("value")))
                tmp = tmp.withColumn(columns[6],col_ex6(f.col("value")))
                tmp = tmp.withColumn(columns[7],col_ex7(f.col("value")))
                tmp = tmp.withColumn(columns[8],col_ex8(f.col("value")))
                tmp = tmp.withColumn(columns[9],col_ex9(f.col("value")))
                tmp = tmp.withColumn(columns[10],col_ex10(f.col("value")))
                tmp = tmp.withColumn(columns[11],col_ex11(f.col("value")))
                tmp = tmp.withColumn(columns[12],col_ex12(f.col("value")))

                # 3. 컬럼제거
                tmp = tmp.drop('value','identifier','protocol','go-agent')

                # 4. email -> userid를 뽑아내기
                email_UDF = f.udf(lambda x: trans_email(x))
                tmp = tmp.withColumn("user_id", email_UDF(f.col("email")))
                tmp = tmp.drop("email")

                # 5. 요청 시간을 datetime변환
                time_UDF = f.udf(lambda x: trans_time(x))
                tmp = tmp.withColumn("datetime", time_UDF(f.col("time")))
                tmp = tmp.drop("time")

                # 6. User Agents의 browser, OS, device 컬럼생성 및 기존컬럼제거
                ext_browser_UDF = f.udf(lambda x: extract_browser(x))
                ext_os_UDF = f.udf(lambda x: extract_os(x))
                ext_device_UDF = f.udf(lambda x: extract_device(x))

                tmp = tmp.withColumn("browser", ext_browser_UDF(f.col("user-agent")))
                tmp = tmp.withColumn("os", ext_os_UDF(f.col("user-agent")))
                tmp = tmp.withColumn("device", ext_device_UDF(f.col("user-agent")))
                tmp = tmp.drop("user-agent")


                # 7 요청처리시간 오류 제거
                tmp = tmp.withColumn('processing time',f.when(tmp["processing time"].isin('-'),f.regexp_replace(tmp["processing time"],'-','')).otherwise(tmp["processing time"]))

                # 7. type변환 (int, float, timestamp)
                tmp = tmp.withColumn('bytesize', f.when(tmp["bytesize"].isin('-'),f.regexp_replace(tmp["bytesize"],'-','0')) .otherwise(tmp["bytesize"]))
                tmp = tmp.withColumn("bytesize", tmp["bytesize"].cast(IntegerType()))                     
                tmp = tmp.withColumn('processing time',f.when(tmp["processing time"].isin('-'),f.regexp_replace(tmp["processing time"],'-','0')).otherwise(tmp["processing time"]))
                tmp = tmp.withColumn("processing time", tmp["processing time"].cast(FloatType()))                
                tmp = tmp.withColumn("datetime",f.to_timestamp("datetime"))
                
                
                # 8. 시스템 요소를 배제하기 위해 userID로 필터링
                tmp = tmp.filter(f.col("user_id") != "-")

                # PostgreSQL 적재 
                tmp.write.mode("append").jdbc("jdbc:postgresql://localhost:5432/superset", f"public.web_{idx}",properties={"user": "postgres", "password": "1234"})
            except IndexError:
                print(f"{string} 오류")
                cnt += 1
                pass
                
            print(f"{string} 완료'")
            print(cnt)
        file.close()
        
        
if __name__ == '__main__':
    main()




