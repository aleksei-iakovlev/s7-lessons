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
    'owner': 's19290263'
}

dag_spark = DAG(
    dag_id="datalake_etl",
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=6
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



dag_spark_analytics = DAG(
    dag_id="datalake_etl_analytics",
    default_args=default_args,
    schedule_interval=None
    )

user_interests_d7 = BashOperator(
    task_id='user_interests_d7',
    bash_command='''
    spark-submit --master yarn --num-executors 10 --deploy-mode cluster \
        --executor-memory 4g \
        --executor-cores 4 \
        --driver-memory 4g \
        --driver-cores 4 \
        /lessons/user_interests.py\
        2022-05-25 7 /user/s19290263/data/events /user/s19290263/analytics/
        ''',
    retries=3,
    dag=dag_spark_analytics
)

task_user_interests_d28 = BashOperator(
    task_id='user_interests_d28',
    bash_command='''
    spark-submit --master yarn --num-executors 10 --deploy-mode cluster \
        --executor-memory 4g \
        --executor-cores 4 \
        --driver-memory 4g \
        --driver-cores 4 \
        /lessons/user_interests.py\
        2022-05-25 28 /user/s19290263/data/events /user/s19290263/analytics/
        ''',
    retries=3,
    dag=dag_spark_analytics
)

spark_submit_tags_7 = SparkSubmitOperator(
    task_id='task_tags_analytics_7',
    dag=dag_spark,
    application ='/lessons/verified_tags_candidates.py' ,
    conn_id= 'yarn_spark',
    application_args = [
        '2022-05-25',
        '7',
        '100',
        '/user/s19290263/data/events',
        '/user/master/data/snapshots/tags_verified/actual',
        '/user/s19290263/5.2.4/analytics/verified_tags_candidates_d7'
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores = 4,
    executor_memory = '4g'
    )

spark_submit_tags_84 = SparkSubmitOperator(
    task_id='task_tags_analytics_84',
    dag=dag_spark,
    application ='/lessons/verified_tags_candidates.py' ,
    conn_id= 'yarn_spark',
    application_args = [
        '2022-05-25',
        '84',
        '1000',
        '/user/s19290263/data/events',
        '/user/master/data/snapshots/tags_verified/actual',
        '/user/s19290263/5.2.4/analytics/verified_tags_candidates_d84'
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores = 4,
    executor_memory = '4g'
    )

[task_user_interests_d7, task_user_interests_d28, spark_submit_tags_7, spark_submit_tags_84]
