import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

def get_filename(**context):
    ti = context['ti']
    filename = context["dag_run"].conf.get("filename")
    print("-----------------------------")
    print(filename)
    ti.xcom_push(key="file_name",value=filename)
    with open('data/nifi/file.json', 'w') as f:
    	json.dump(filename,f)


with DAG(
        dag_id='airflow_to_Ofbiz',
        schedule_interval=None,
        start_date=datetime(2022, 7, 22),
        catchup=False
) as dag:


    
    # Task to read JSON
    task_get_filename = PythonOperator(
        task_id='task_get_filename',
        python_callable=get_filename
    )

    # Send request to run service
    task_schedule_service = SimpleHttpOperator(
        task_id='schedule_service',
        http_conn_id='local_ofbiz',
        endpoint='/api/service/executeServiceFromAirflow',
             data= json.dumps({"dagId":"Ofbiz_to_NiFi","airflowPayload":{"filename":"{{task_instance.xcom_pull(task_ids='task_get_filename',key='file_name')}}"},"serviceName":"ftpExportProductThresholdCsv","payload":{"facilityId":[
            "WH"],"threshold":"1","searchPreferenceId":"10002","JOB_NAME":"AirflowRule5"}}),
        headers={"Content-Type": "application/json","Authorization":"Basic aG90d2F4LnVzZXI6aG90d2F4QDc4Ng=="},
        log_response=True,
        extra_options={"verify":False}
    )
    

task_get_filename>>task_schedule_service
