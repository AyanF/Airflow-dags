import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
  
def get_rules(**context):
    ti = context['ti']
    payload = context["dag_run"].conf
    with open('data/case2.1/rulesOnce.json', 'w') as f:
        json.dump(payload,f)
        
with DAG(
        dag_id='get_rules3',
        schedule_interval=None,
        start_date=datetime(2022, 7, 22),
        catchup=False
) as dag:

    # get rules
    task_read_rules = PythonOperator(
        task_id='read_rules3',
        python_callable=get_rules,
        provide_context=True
    )

    # Tridder 2nd DAG
    task_schedule_service = TriggerDagRunOperator(
        task_id="trigger_Dag2",
        trigger_dag_id="execute_rules3"
    )

    

task_read_rules>>task_schedule_service