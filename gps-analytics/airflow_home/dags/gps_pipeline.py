#!/usr/bin/env python

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

wd = os.getcwd()

os.environ['SPARK_HOME']='/usr/local/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'],'bin'))
os.environ['HADOOP_CONF_DIR']='/usr/local/hadoop/etc/hadoop'


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        # If we want to select a specific launch date
        # 'start_date': datetime(2020,4,29),
        # 'start_date': '2020-01-01',
        'start_date': days_ago(7),
        'email': ['olanglechimal@worldbank.org'],
        'email_on_failure': False,
        'email_on_retry': False,
        'concurrency': 1,
        'retry_delay': timedelta(minutes=5),
        'retries': 0,
        }

dag = DAG(
    'gpspipeline',
    default_args=default_args,
    description='GPS mobility pipeline',
    schedule_interval=timedelta(days=10),
#     This can also be a common cron job
#     schedule_interval= '*/5 * * * *'
)

geocoded_pings = SparkSubmitOperator(
        task_id='geocodePings',
        application_file='/src/pipeline/get_geocoded_pings.scala ',
        arguments=['veraset', 'country'],
        dag = dag)

tz_offset = SparkSubmitOperator(
        task_id='tzOffset',
        application_file='/src/pipeline/tz_offset.scala ', \
        arguments=['veraset', 'country'],
        dag = dag)

stop_locations = SparkSubmitOperator(
        task_id='stopLocations',
        application_file='/src/pipeline/stop_locations.py ', \
        arguments=['--country', 'country', '--stop_locations_dir', '/mnt/stops/', '--radius','50','--stay_time','300','--min_pts_per_stop_location','2','--max_time_stop_location','3600','--max_accuracy','100','--db_scan_radius','50'],
        dag = dag)


geocode_stops = SparkSubmitOperator(
        task_id='geocodeStops',
        application_file='/src/pipeline/geocode_stop_locations.scala ', \
        arguments=['veraset', 'country'],
        dag = dag)


labeling = SparkSubmitOperator(
        task_id='HWlabeling',
        application_file='/src/pipeline/compute_home_and_work_locations.py ', \
        arguments=['--country', 'country', '--stop_locations_dir', '/mnt/stops/', '--radius','50','--stay_time','300',
        '--min_pts_per_stop_location','2','--max_time_stop_location','3600','--max_accuracy','100','--db_scan_radius','50',
        '--result_path_spark','/mnt/results_spark', '--start_hour_day', '5', '--end_hour_day', '11', '--min_pings_home_cluster_label','2',
        '--work_activity_average','0.2', '--hw', '49', '--ww', '49', '--mph', '49', '--mpw', '49'],
        dag = dag)


geocodePings >> tzOffset >> stopLocations >> geocodeStops >> HWlabeling