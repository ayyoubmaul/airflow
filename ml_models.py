from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2022, 4, 17)
}

def choose_ml_models():
    acc = 0.6
    if acc > 0.5:
        return 'accurate'
    return 'inaccurate'

with DAG('choosing_ml_model', default_args=default_args, schedule_interval='@daily', catchup = False) as dag:
    start = DummyOperator(
        task_id='start'
    )

    choose_model = BranchPythonOperator(
        task_id='choose_model',
        python_callable=choose_ml_models
    )

    accurate = PythonOperator(
        task_id='accurate',
        python_callable=lambda: print('this is the right model')
    )

    inaccurate = PythonOperator(
        task_id='inaccurate',
        python_callable=lambda: print('this is the wrong model')
    )
    
    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )

    start >> choose_model >> [accurate, inaccurate] >> end
