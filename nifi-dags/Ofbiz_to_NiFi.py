import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

def get_filename(**context):
    ti = context['ti']
    filename = context["dag_run"].conf.get("airload")
    print("------------")
    print(filename)
    print("------------")
    ti.xcom_push(key="file_name",value=filename)
    with open('data/nifi/file.json', 'w') as f:
    	json.dump(filename,f)

with DAG(
        dag_id='Ofbiz_to_NiFi',
        schedule_interval=None,
        start_date=datetime(2022, 7, 22),
        catchup=False
) as dag:

    # Send request to run service
    task_send_filename = SimpleHttpOperator(
        task_id='send_filename',
        http_conn_id='local_NiFi',
        endpoint='receive_filename',
             data= json.dumps({"filename":"{{task_instance.xcom_pull(task_ids='task_get_filename',key='file_name')}}"}),
        log_response=True
    )
    
        

    task_get_filename = PythonOperator(
        task_id='task_get_filename',
        python_callable=get_filename
    )


task_get_filename>>task_send_filename
