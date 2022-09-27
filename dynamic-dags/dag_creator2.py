from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator
import json


def create_dag(dag_id,
               schedule,
               default_args,conn_id):
    def read_json(ti) -> None:
        with open('data/case2.1/rulesOnce.json', 'r') as openfile:
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
                    json_object[x]["ruleStatus"]="Triggered"
                    with open('data/case2.1/rulesOnce.json', 'w') as f:
                        json.dump(json_object,f)
                    break
                else:
                    print("Task over")
            # Push individual values to xcom
            ti.xcom_push(key="service_time",value=service_time)
            ti.xcom_push(key="jobName",value=jobName)
            ti.xcom_push(key="facilityId",value=facilityId)
            ti.xcom_push(key="systemJobEnumId",value=systemJobEnumId)
            ti.xcom_push(key="productStoreId",value=productStoreId)
            ti.xcom_push(key="searchPreferenceId",value=searchPreferenceId)
            ti.xcom_push(key="threshold",value=threshold)

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
    # Task to read JSON
        task_read_rules = PythonOperator(
            task_id='read_rules3',
            python_callable=read_json)
    # Send request to run service
        task_schedule_service = SimpleHttpOperator(
            task_id='schedule_service',
            http_conn_id=conn_id,
            endpoint='/executeServiceFromAirflow',
            data= json.dumps({"dagId":"update_rules2","serviceName":"ftpExportProductThresholdCsv","payload":{"facilityId":["{{task_instance.xcom_pull(task_ids='read_rules3',key='facilityId')}}"],
                                                                                                          "threshold":"{{task_instance.xcom_pull(task_ids='read_rules3',key='threshold')}}",
                                                                                                          "searchPreferenceId":"{{task_instance.xcom_pull(task_ids='read_rules3',key='searchPreferenceId')}}",
                                                                                                          "JOB_NAME":"{{task_instance.xcom_pull(task_ids='read_rules3',key='jobName')}}"}}),
            headers={"Content-Type": "application/json","Authorization":"Basic xxxxxxxxxxxxxxxxxxxxx=="},
            log_response=True,
            extra_options={"verify":False}
    )
    task_read_rules>>task_schedule_service
    return dag

#build dag
default_args = {'owner': 'airflow',
                'start_date': datetime(2022, 1, 1)
                }

with open('data/case2.2/dag_json.json', 'r') as openfile:
    json_object = json.load(openfile)
    for x in json_object:
        dag_id=json_object.get(x).get("dagid")
        schedule=json_object.get(x).get("schedule")
        globals()[dag_id] = create_dag(dag_id,
                                       schedule,
                                       default_args,'local_ofbiz')

