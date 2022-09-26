import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

# Read JSON file in loop and push task params
def read_json(ti) -> None:
    with open('dags/rulesDaily.json', 'r') as openfile:
        json_object = json.load(openfile)
        for x in json_object:
            ruleStatus = json_object.get(x).get("ruleStatus")
            if(ruleStatus=="Pending"):
                facilityId = json_object.get(x).get("facilityId")
                jobName = json_object.get(x).get("JOB_NAME")
                systemJobEnumId =json_object.get(x).get("systemJobEnumId")
                productStoreId = json_object.get(x).get("jobFields").get("productStoreId")  
                searchPreferenceId = json_object.get(x).get("searchPreferenceId")
                service_time = json_object.get(x).get("SERVICE_TIME")
                threshold = json_object.get(x).get("threshold")
                # Push individual values to xcom
                ti.xcom_push(key="service_time",value=service_time)
                ti.xcom_push(key="jobName",value=jobName)
                ti.xcom_push(key="facilityId",value=facilityId)
                ti.xcom_push(key="systemJobEnumId",value=systemJobEnumId)
                ti.xcom_push(key="productStoreId",value=productStoreId)
                ti.xcom_push(key="searchPreferenceId",value=searchPreferenceId)
                ti.xcom_push(key="threshold",value=threshold)
                json_object[x]["ruleStatus"]="Triggered"
                with open('dags/rulesDaily.json', 'w') as f:
                    json.dump(json_object,f)
                print("----------------------------")
                print(json_object.get(x).get("ruleStatus"))    
                break
            else:
                print("Task over")

with DAG(
        dag_id='execute_rules',
        schedule_interval=None,
        start_date=datetime(2022, 7, 22),
        catchup=False
) as dag:

    # Send request to run service
    task_schedule_service = SimpleHttpOperator(
        task_id='schedule_service',
        http_conn_id='sm-uat',
        endpoint='executeServiceFromAirflow',
             data= json.dumps({"dagId":"update_rules2","serviceName":"ftpExportProductThresholdCsv","payload":{"facilityId":["{{task_instance.xcom_pull(task_ids='read_rules',key='facilityId')}}"],
                               "threshold":"{{task_instance.xcom_pull(task_ids='read_rules',key='threshold')}}",
                               "searchPreferenceId":"{{task_instance.xcom_pull(task_ids='read_rules',key='searchPreferenceId')}}",
                               "JOB_NAME":"{{task_instance.xcom_pull(task_ids='read_rules',key='jobName')}}"}}),
        headers={"Content-Type": "application/json","Authorization":"Basic xxxxxxxxxxxxxxxxx="},
        log_response=True,
        extra_options={"verify":False}
    )
    
    # Task to read JSON
    task_read_rules = PythonOperator(
        task_id='read_rules',
        python_callable=read_json
    )

task_read_rules>>task_schedule_service
