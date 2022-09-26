import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Read JSON file and push task params
def read_json():
    with open('rules2.json', 'r') as openfile:
        json_object = json.load(openfile)
        
        for x in json_object:
            
            ruleStatus = json_object.get(x).get("ruleStatus")
            
            if(ruleStatus=="Pending"):
                facilityId = json_object.get(x).get("facilityId")
                systemJobEnumId =json_object.get(x).get("systemJobEnumId")
                productStoreId = json_object.get(x).get("jobFields").get("productStoreId")  
                searchPreferenceId = json_object.get(x).get("searchPreferenceId")
                service_time = json_object.get(x).get("SERVICE_TIME")
                threshold = json_object.get(x).get("threshold")
                print(facilityId,service_time,systemJobEnumId,productStoreId,service_time,threshold)

def update_service(**context):
    ti = context['ti']
    user_ruleId = context["dag_run"].conf.get("ruleId")
    
    with open('rules2.json', 'r') as openfile:
        json_object = json.load(openfile)
        
        for x in json_object:
            ruleStatus_old = json_object.get(x).get("ruleStatus")
            ruleId = json_object.get(x).get("ruleId")

            if(ruleId==user_ruleId):
                json_object[x]["ruleStatus"]="Completed"
            ruleStatus_new = json_object.get(x).get("ruleStatus")
            print(ruleStatus_old)
            print("-------------------------------")
            print(ruleStatus_new)
        with open('rules2.json', 'w') as f:
            json.dump(json_object,f)
        

with DAG(
        dag_id='update_rules',
        schedule_interval=None,
        start_date=datetime(2022, 7, 22),
        catchup=False
) as dag:

    # Update
    task_update_rules = PythonOperator(
        task_id='update_rules',
        python_callable=update_service,
        provide_context=True
    )

    # Tridder 2nd DAG
    task_schedule_service = TriggerDagRunOperator(
        task_id="trigger_Dag2",
        trigger_dag_id="execute_rules2"
    )

#Dependency to first update status then trigger second DAG
task_update_rules>>task_schedule_service
