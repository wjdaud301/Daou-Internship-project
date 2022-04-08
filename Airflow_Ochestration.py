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
    'dag_Spark_jmyeong',
    start_date=days_ago(2),
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False,
)

#시작을 알리는 dummy
task_start = DummyOperator(
    task_id='start',
    dag=dag,
)

#시작이 끝나고 다음단계로 진행되었음을 나타내는 dummy
task_next = DummyOperator(
    task_id='next',
    trigger_rule='all_success',
    dag=dag,
)
#끝을 알리는 dummy
task_finish = DummyOperator(
    task_id='finish',
    trigger_rule='all_done',
    dag=dag,
)


# extract
admin_command = """
bash /home/jmyeong/tarfile/admin_decomp.sh
"""

web_command = """
bash /home/jmyeong/tarfile/web_decomp.sh
"""

# processing
admin_spark_cmd = """
bash $SPARK_HOME/bin/spark-submit /home/jmyeong/process/admin_spark_processing.py
"""
web_spark_cmd = """
bash $SPARK_HOME/bin/spark-submit /home/jmyeong/process/web_spark_processing.py
"""


# load
admin_load_cmd = """
bash $SPARK_HOME/bin/spark-submit --driver-class-path $SPARK_HOME/jars/postgresql-42.3.3.jar /home/jmyeong/process/admin_load.py
"""
web_load_cmd = """
bash $SPARK_HOME/bin/spark-submit --driver-class-path $SPARK_HOME/jars/postgresql-42.3.3.jar /home/jmyeong/process/web_load.py
"""



admin_bash = BashOperator(
    task_id='admin_log',
    bash_command= admin_command,
    dag=dag,
)
web_bash = BashOperator(
    task_id='web_log',
    bash_command= web_command,
    dag=dag,
)


admin_PySpark = BashOperator(
    task_id='admin_spark_submit_task',
    dag=dag,
    bash_command=admin_spark_cmd,
)
web_PySpark = BashOperator(
    task_id='web_spark_submit_task',
    dag=dag,
    bash_command=web_spark_cmd,
)


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
task_start >> [admin_bash, web_bash] >> task_next
task_next >> [admin_PySpark, web_PySpark]
admin_PySpark >> admin_Load
web_PySpark >> web_Load
[admin_Load, web_Load]>> task_finish

