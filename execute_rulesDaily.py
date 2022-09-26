import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

# Read JSON file in loop and push task params
def read_rules(ti) -> None:
    with open('data/case2.1/rulesDaily.json', 'r') as openfile:
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
                break
            else:
                prin("Task over")
        # Push individual values to xcom
        ti.xcom_push(key="service_time",value=service_time)
        ti.xcom_push(key="jobName",value=jobName)
        ti.xcom_push(key="facilityId",value=facilityId)
        ti.xcom_push(key="systemJobEnumId",value=systemJobEnumId)
        ti.xcom_push(key="productStoreId",value=productStoreId)
        ti.xcom_push(key="searchPreferenceId",value=searchPreferenceId)
        ti.xcom_push(key="threshold",value=threshold)

with DAG(
        dag_id='execute_rulesDaily',
        schedule_interval=None,
        start_date=datetime(2022, 7, 22),
        catchup=False
) as dag:

    # Send request to run service
    task_schedule_service = SimpleHttpOperator(
        task_id='schedule_service',
        http_conn_id='dev_apps',
        endpoint='/scheduleService',
             data= json.dumps({"facilityId":["{{task_instance.xcom_pull(task_ids='read_rules',key='facilityId')}}"],"propertyResource":"FTP_EXP_CONFIG","threshold":"{{task_instance.xcom_pull(task_ids='read_rules',key='threshold')}}","searchPreferenceId":"{{task_instance.xcom_pull(task_ids='read_rules',key='searchPreferenceId')}}",
                               "JOB_NAME":"{{task_instance.xcom_pull(task_ids='read_rules',key='jobName')}}","SERVICE_NAME":"ftpExportProductThresholdCsv","SERVICE_COUNT":"0","SERVICE_TIME":"{{task_instance.xcom_pull(task_ids='read_rules',key='service_time')}}",
                               "jobFields":{"productStoreId":"{{task_instance.xcom_pull(task_ids='read_rules',key='productStoreId')}}","systemJobEnumId":"{{task_instance.xcom_pull(task_ids='read_rules',key='systemJobEnumId')}}","maxRecurrenceCount":"-1",
                               "recurrenceTimeZone":"Asia/Kolkata"},"statusId":"SERVICE_PENDING","systemJobEnumId":"{{task_instance.xcom_pull(task_ids='read_rules',key='systemJobEnumId')}}"}),
        headers={"Content-Type": "application/json","Authorization":"Basic xxxxxxxxxx=="},
        log_response=True
    )
    
    # Task to read JSON
    task_read_rules = PythonOperator(
        task_id='read_rules',
        python_callable=read_rules
    )

task_read_rules>>task_schedule_service
