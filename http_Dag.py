import json
import os 
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
#
from HttpOperatorAsync2 import HttpOperatorAsync

def save_posts(ti) -> None:
    posts = ti.xcom_pull(task_ids='get_posts')
    with open('posts.json', 'w') as f:
        json.dump(posts,f)
                  
with DAG(
        dag_id='api_dag',
        schedule_interval='@daily',
        start_date=datetime(2022, 6, 13),
        catchup=False
) as dag:
    #  Check if the API is up
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='api_posts',
        endpoint='posts/'
    )

    # Get the posts
    task_get_posts = SimpleHttpOperator(
        task_id='get_posts',
        http_conn_id='api_posts',
        endpoint='posts/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    # Save posts
    task_save = PythonOperator(
        task_id='save_posts',
        python_callable=save_posts
    )
    
    #HttpOperatorAsync
    task_get_posts_async = HttpOperatorAsync(
        
        task_id='get_posts_async',
        http_conn_id='api_posts',
        endpoint='posts/',
        method='GET',
        name="sample",
        log_response=True,
        
    )
    
    
    #Async task 
    # async_task = HttpSensorAsync(
    # task_id='async_task',
    # http_conn_id='api_posts',
    # endpoint = 'posts/',
    # 
    # request_params = {}
    # )
    
    task_is_api_active>>task_get_posts>>task_get_posts_async>>task_save
    #File is edited
