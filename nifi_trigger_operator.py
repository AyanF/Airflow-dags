from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests 
import json

def get_processor(url_nifi_api: str, processor_id: str, token=None):
    
    # Authorization header
    header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(token),
    }
    response = requests.get(url_nifi_api + f"processors/{processor_id}", headers=header,verify=False)
    return json.loads(response.content)

def get_token(url_nifi_api: str, access_payload: dict):
    
    header = {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "*/*",
    }
    response = requests.post(
        url_nifi_api + "access/token", headers=header, data=access_payload, verify=False
    )
    return response.content.decode("ascii")

def update_processor_status(processor_id: str, new_state: str, token, url_nifi_api):

    # Retrieve processor 
    processor = get_processor(url_nifi_api, processor_id, token)

    # Create a JSON with the new state and the processor's revision
    put_dict = {
        "revision": processor["revision"],
        "state": new_state,
        "disconnectedNodeAcknowledged": True,
    }

    # Dump JSON and POST processor
    payload = json.dumps(put_dict).encode("utf8")
    header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(token),
    }
    response = requests.put(
        url_nifi_api + f"processors/{processor_id}/run-status",
        headers=header,
        data=payload,
        verify=False
        )
    return response

def startup():
    
    url_nifi_api = "https://localhost:8443/nifi-api/"
    processor_id = (
        "a521714d-0182-1000-5cc2-7aafb9c5afda"  
    )
    access_payload = {
        "username": "admin",
        "password": "nifiadmin@786",
    }  

    token = get_token(url_nifi_api, access_payload)
    response = update_processor_status(processor_id, "RUNNING", token, url_nifi_api)
    print(response)

with DAG(
        dag_id="trigger_nifi",
        schedule_interval=None,
        start_date=days_ago(2),
        catchup=False,
) as dag:

    startup_task = PythonOperator(
        task_id="startup_task",
        python_callable=startup,
    )
    
startup_task 