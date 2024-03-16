
from airflow import DAG
from airflow.operators.python import PythonOperator
from function import scrapping,produce_to_kafka,consumer_kafka,simple_cleanning,load_to_db
from datetime import datetime

# Spécifiez vos arguments par défaut
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'schedule_interval': '@daily',
    'catchup': False,
}

# Créez votre DAG
dag = DAG(
    'data',
    default_args=default_args,
    description='DAG for scraping and saving transfermarkt data',
)
# Utilisez l'opérateur PythonOperator pour exécuter la fonction de scraping
scrape_task = PythonOperator(
    task_id='scrapping',
    python_callable=scrapping,
   # provide_context=True,  # Permet d'accéder au contexte Airflow
    dag=dag,
)
#charger les donnes dans kafka
producer_task = PythonOperator(
    task_id='produce_to_kafka',
    python_callable=produce_to_kafka,
   # provide_context=True,  # Permet d'accéder au contexte Airflow
    dag=dag,
)
#consumer depuis kafka
consumer_task = PythonOperator(
    task_id='consumer_kafka',
    python_callable=consumer_kafka,
    #provide_context=True,  # Permet d'accéder au contexte Airflow
    dag=dag,
)
#cleanner les données
transform_task = PythonOperator(
    task_id='simple_cleanning',
    python_callable=simple_cleanning,
    #provide_context=True,  # Permet d'accéder au contexte Airflow
    dag=dag,
)
#charger les donnees dans mongo db
load_task = PythonOperator(
    task_id='load_to_db',
    python_callable=load_to_db,
   # provide_context=True,  # Permet d'accéder au contexte Airflow
    dag=dag,
)
scrape_task >> producer_task >> consumer_task >> transform_task >> load_task
