import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
  
def get_rules(**context):
    ti = context['ti']
    payload = context["dag_run"].conf
    with open('data/rules2.json', 'w') as f:
        json.dump(payload,f)
        
with DAG(
        dag_id='get_rules2',
        schedule_interval=None,
        start_date=datetime(2022, 7, 22),
        catchup=False
) as dag:

    # get rules
    task_read_rules = PythonOperator(
        task_id='read_rules',
        python_callable=get_rules,
        provide_context=True
    )

    # Tridder 2nd DAG
    task_schedule_service = TriggerDagRunOperator(
        task_id="trigger_Dag2",
        trigger_dag_id="execute_rules2"
    )

    
#Tas kdependency to read rules first then trigger second DAG  
task_read_rules>>task_schedule_service