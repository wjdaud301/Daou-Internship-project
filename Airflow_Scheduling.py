#!/usr/bin/env python
# coding: utf-8

# In[3]:


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(seconds=20),
    'depends_on_past': False
}


dag = DAG(
    'dag_Spark',
    start_date=days_ago(2),
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False,
)


#끝을 알리는 dummy
task_finish = DummyOperator(
    task_id='finish',
    trigger_rule='all_done',
    dag=dag,
)



# load
admin_load_cmd = """
bash $SPARK_HOME/bin/spark-submit --driver-class-path $SPARK_HOME/jars/postgresql-42.3.3.jar /home/jmyeong/process/admin_load.py
"""
web_load_cmd = """
bash $SPARK_HOME/bin/spark-submit --driver-class-path $SPARK_HOME/jars/postgresql-42.3.3.jar /home/jmyeong/process/web_load.py
"""


admin_Load = BashOperator(
    task_id='admin_load_task',
    dag=dag,
    bash_command=admin_load_cmd,
)
web_Load = BashOperator(
    task_id='web_load_task',
    dag=dag,
    bash_command=web_load_cmd,
)




# 의존 관계 구성

[admin_Load, web_Load]>> task_finish

