import airflow
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import date, datetime, timedelta


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'


default_args = {
    'start_date':datetime(2020, 10, 1),
    'end_date':datetime(2022, 6, 21),
    'owner': 's19290263'
}

dag_spark = DAG(
    dag_id="datalake_etl",
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=6, 
    catchup=True
    )

t1 = BashOperator(
    task_id='task_raw_to_odd',
    bash_command='''
        spark-submit --master yarn --num-executors 10 \
            --deploy-mode cluster \
            --executor-memory 4g \
            --executor-cores 4 \
            --driver-memory 2g \
            --driver-cores 4 \
            /lessons/partition.py \
            {{ ds }} /user/master/data/events /user/s19290263/data/events
        ''',
    retries=3,
    dag=dag_spark
)

t1

# spark_submit_tags_7 = SparkSubmitOperator(
#     task_id='task_tags_analytics_7',
#     dag=dag_spark,
#     application ='/lessons/verified_tags_candidates.py' ,
#     conn_id= 'yarn_spark',
#     application_args = [
#         '2022-05-31',
#         '7',
#         '100',
#         '/user/s19290263/data/events',
#         '/user/master/data/snapshots/tags_verified/actual',
#         '/user/s19290263/5.2.4/analytics/verified_tags_candidates_d5'
#     ],
#     conf={"spark.driver.maxResultSize": "20g"},
#     executor_cores = 4,
#     executor_memory = '4g'
#     )

# spark_submit_tags_84 = SparkSubmitOperator(
#     task_id='task_tags_analytics_84',
#     dag=dag_spark,
#     application ='/lessons/verified_tags_candidates.py' ,
#     conn_id= 'yarn_spark',
#     application_args = [
#         '2022-05-31',
#         '84',
#         '1000',
#         '/user/s19290263/data/events',
#         '/user/master/data/snapshots/tags_verified/actual',
#         '/user/s19290263/5.2.4/analytics/verified_tags_candidates_d5'
#     ],
#     conf={"spark.driver.maxResultSize": "20g"},
#     executor_cores = 4,
#     executor_memory = '4g'
#     )

# t1 >> [spark_submit_tags_7, spark_submit_tags_84]
