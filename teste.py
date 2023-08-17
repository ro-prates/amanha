from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

default_args = {
    'owner': 'seu_usuario',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'exemplo_python_virtualenv',
    default_args=default_args,
    description='Exemplo de uso do PythonVirtualenvOperator',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

start_task = DummyOperator(task_id='inicio', dag=dag)

def minha_tarefa_python():
    import numpy as np
    import pandas as pd
    data = {'Nome': ['Alice', 'Bob', 'Carol'],
            'Idade': [25, 30, 28]}
    df = pd.DataFrame(data)
    print('funciona!!!')

python_task = PythonVirtualenvOperator(
    task_id='tarefa_python_virtualenv',
    python_callable=minha_tarefa_python,
    requirements=['pandas', 'openai', 'numpy'],  # Lista de pacotes a serem instalados no ambiente virtual
    system_site_packages=False,  # Se True, permite acesso aos pacotes do sistema
    dag=dag,
)

end_task = DummyOperator(task_id='fim', dag=dag)

start_task >> python_task >> end_task
