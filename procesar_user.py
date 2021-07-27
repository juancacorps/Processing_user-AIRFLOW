from airflow.models import DAG
from datetime import datetime
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
from pandas import json_normalize
import json
from datetime import datetime
default_args = {'start_date': datetime(2021,7,12)}

date = datetime.now().strftime('%Y-%m-%d')
def _processing_user(ti):
    response =  ti.xcom_pull(task_ids=['extract_user'])
    print(response)
    user_information = {}
    if  'results' in response[0]:
        user = response[0]['results'][0]
        user_information = json_normalize({
            'firt_name': user['name']['first'],
            'lastname': user['name']['last'],
            'country': user['location']['country'],
            'nickname': user['login']['username'],
            'password': user['login']['password'],
            'email': user['email'],
        })
        dataframe = pd.DataFrame(user_information)
        path_file = f'/home/juancacorps/airflow/dags/files/data/users_information.csv'
        dataframe.to_csv(path_file,index=False,header=False)
    else:
        raise ValueError('Not have information for proccesing')

with DAG('procesar_usuario',schedule_interval='@daily', 
default_args=default_args, 
catchup = False, tags=['proccesing user pipeline']) as dag:

    api_available = HttpSensor(
        task_id = 'api_available',
        http_conn_id = 'user_api',
        endpoint = 'api/'
    )

    extract_user = SimpleHttpOperator(
        task_id = 'extract_user',
        http_conn_id = 'user_api',
        endpoint = 'api/',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )

    processing_user = PythonOperator(
        task_id = 'processing_user',
        python_callable = _processing_user
    )

    create_table = SqliteOperator(
        task_id = 'create_table',
        sqlite_conn_id = 'db_sqlite',
        sql = '''
        CREATE TABLE IF NOT EXISTS users(
            firt_name TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            nickname TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL PRIMARY KEY
        );'''
    )
    almacenar_usuario = BashOperator(
        task_id = 'almacenar_usuario',
        
        bash_command = 'echo -e ".separator ","\n.import /home/juancacorps/airflow/dags/files/data/users_information.csv users" | sqlite3 /home/juancacorps/airflow/airflow.db'
    )

    # Order of Pipeline
    create_table >> api_available
    api_available >> extract_user
    extract_user >> processing_user
    processing_user >> almacenar_usuario