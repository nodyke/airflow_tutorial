from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from pprint import pprint

SCHEDULE_INTERVAL = "schedule_interval"
START_DATE = "start_date"
DAG_ID_PREFIX = "gridu_dag_insert_to_"
TABLE_NAME = "table_name"
DAG_ID = "dag_id"

tables = ['table_1', 'table_2', "table_3"]
config = {}


def test_pyth_operator(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


def print_start(ds, **kwargs):
    dag_id = kwargs[DAG_ID]
    table_name = kwargs[TABLE_NAME]
    return dag_id+' start processing table:'+table_name


for table in tables:
    config.update({DAG_ID_PREFIX + table: {SCHEDULE_INTERVAL: "", START_DATE: datetime(2019, 1, 1), TABLE_NAME: table}})

for conf in config:
    dag_id = conf
    table_name = config.get(dag_id).get(TABLE_NAME)
    dag = DAG(dag_id, start_date=config.get(dag_id).get(START_DATE))
    print_start_task = PythonOperator(
        task_id='log_start',
        provide_context=True,
        python_callable=print_start,
        op_kwargs={DAG_ID:dag_id,TABLE_NAME:table_name},
        dag=dag)
    insert_to_db_task = DummyOperator(
        task_id='insert_to_db',
        dag=dag)
    check_result_in_db_task = DummyOperator(
        task_id="check_result_in_db"
    )
    print_start_task >> insert_to_db_task >> check_result_in_db_task
    globals()[dag_id] = dag
