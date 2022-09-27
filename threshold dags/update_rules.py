from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def update_service(**context):
    import json
    ti = context['ti']

    with open('dags/rulesDaily.json', 'r') as openfile:
        json_object = json.load(openfile)
        
        for x in json_object:
            ruleStatus_old = json_object.get(x).get("ruleStatus")
            if(ruleStatus_old=="Triggered"):
                json_object[x]["ruleStatus"]="Completed"
                return True
            else:
                return False
        with open('dags/rulesDaily.json', 'w') as f:
            json.dump(json_object,f)
                
with DAG(
        dag_id='update_rules',
        schedule_interval=None,
        start_date=datetime(2022, 7, 22),
        catchup=False
) as dag:

    # Update
    task_update_rules = ShortCircuitOperator(
        task_id='update_rules',
        python_callable=update_service,
        provide_context=True
    )

    # Tridder 2nd DAG
    task_trigger_execute = TriggerDagRunOperator(
        task_id="trigger_execute_rules",
        trigger_dag_id="execute_rules"
    )

task_update_rules>>task_trigger_execute
