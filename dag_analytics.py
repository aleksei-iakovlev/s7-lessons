import airflow
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'


default_args = {
    'start_date':datetime(2020, 10, 1),
    'owner': 's19290263'
}

dag_spark_analytics = DAG(
    dag_id="datalake_etl_analytics",
    default_args=default_args,
    schedule_interval=None
    )

spark_submit_connection_interests_d7 = SparkSubmitOperator(
    task_id='connection_interests_d7',
    dag=dag_spark_analytics,
    application ='/lessons/connection_interests.py' ,
    conn_id='yarn_spark',
    application_args = [
        '2022-05-25',
        '7',
        '/user/s19290263/data/events',
        '/user/s19290263/analytics',
        '/user/s19290263/data/analytics'
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores = 4,
    executor_memory = '4g'
    )

# task_user_interests_d7 = BashOperator(
#     task_id='user_interests_d7',
#     bash_command='''
#     spark-submit --master yarn --num-executors 10 --deploy-mode cluster \
#         --executor-memory 4g \
#         --executor-cores 4 \
#         --driver-memory 4g \
#         --driver-cores 4 \
#         /lessons/user_interests.py \
#         2022-05-25 7 /user/s19290263/data/events /user/s19290263/analytics/
#         ''',
#     retries=3,
#     dag=dag_spark_analytics
# )

# task_user_interests_d28 = BashOperator(
#     task_id='user_interests_d28',
#     bash_command='''
#     spark-submit --master yarn --num-executors 10 --deploy-mode cluster \
#         --executor-memory 4g \
#         --executor-cores 4 \
#         --driver-memory 4g \
#         --driver-cores 4 \
#         /lessons/user_interests.py \
#         2022-05-25 28 /user/s19290263/data/events /user/s19290263/analytics/
#         ''',
#     retries=3,
#     dag=dag_spark_analytics
# )

# spark_submit_tags_d7 = SparkSubmitOperator(
#     task_id='tags_analytics_d7',
#     dag=dag_spark_analytics,
#     application ='/lessons/verified_tags_candidates.py' ,
#     conn_id='yarn_spark',
#     application_args = [
#         '2022-05-25',
#         '7',
#         '100',
#         '/user/s19290263/data/events',
#         '/user/master/data/snapshots/tags_verified/actual',
#         '/user/s19290263/analytics/verified_tags_candidates_d7/date=2022-05-25'
#     ],
#     conf={"spark.driver.maxResultSize": "20g"},
#     executor_cores = 4,
#     executor_memory = '4g'
#     )

# spark_submit_tags_d84 = SparkSubmitOperator(
#     task_id='tags_analytics_d84',
#     dag=dag_spark_analytics,
#     application ='/lessons/verified_tags_candidates.py' ,
#     conn_id='yarn_spark',
#     application_args = [
#         '2022-05-25',
#         '84',
#         '1000',
#         '/user/s19290263/data/events',
#         '/user/master/data/snapshots/tags_verified/actual',
#         '/user/s19290263/5.2.4/analytics/verified_tags_candidates_d84'
#     ],
#     conf={"spark.driver.maxResultSize": "20g"},
#     executor_cores = 4,
#     executor_memory = '4g'
#     )

# spark_submit_tags_d7 >> 
spark_submit_connection_interests_d7
# [spark_submit_tags_d7, spark_submit_tags_d84]
