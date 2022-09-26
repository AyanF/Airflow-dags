import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Read JSON file daily
def read_rules_daily(ti) -> None:
    updated_json={}
    with open('data/case2.1/rules.json', 'r') as openfile:
        json_rule = json.load(openfile)
    with open('data/case2.1/sequence.json', 'r') as openfile:
        json_sequence = json.load(openfile)
    updated_json={}
    #Code to write a new JSON file to merge these files into one 
        #Logic to merge files and add rule status param
    for x in json_sequence.get("seq"):
        json_rule.get(x)["ruleStatus"]="Pending"
        updated_json[x]=json_rule.get(x)
    with open('data/case2.1/rulesDaily.json', 'w') as f:
        json.dump(updated_json,f)

            
with DAG(
        dag_id='read_rulesDaily',
        schedule_interval=None,
        start_date=datetime(2022, 7, 26),
        catchup=False
) as dag:

    # get rules
    task_read_rules = PythonOperator(
        task_id='read_rules',
        python_callable=read_rules_daily,
        provide_context=True
    )

    # Tridder 2nd DAG
    task_schedule_service = TriggerDagRunOperator(
        task_id="trigger_Dag2",
        trigger_dag_id="execute_rulesDaily"
    )

    

task_read_rules>>task_schedule_service