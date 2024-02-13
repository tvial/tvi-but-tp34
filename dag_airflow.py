'''
Modèle de DAG Airflow pour le chargement BigQuery
'''

import datetime

from airflow import models
from airflow.operators import python_operator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



# Paramètres par défaut. On met une date dans le passé pour que le DAG
# soit éligible à l'exécution, même manuelle
default_dag_args = {
    "start_date": datetime.datetime(2024, 2, 1),
}


# On déclare le DAG
with models.DAG(
    ### CHANGE ME ###
    schedule_interval=datetime.timedelta(days=30),
    default_args=default_dag_args,
) as dag:
    def greeting():
        import logging

        logging.info("Hello World!")

    # Un "operator" = une étape du DAG = une tâche Airflow
    # Celui-ci est un simple appel de fonction Python
    hello_python = python_operator.PythonOperator(
        task_id="hello", python_callable=greeting
    )

    # Renseigner les paramètres suivants :
    # - destination_project_dataset_table : où sera créée la table
    # - bucket : nom du bucket où se trouvent les fichiers
    # - source_objects : emplacement des fichiers à charger dans le bucket (cf. question 2)
    # - source_format : nom du format des données sources ; pour du JSON c'est 'NEWLINE_DELIMITED_JSON'
    # - autodetect : booléen pour demander à Big Query de détecter le schéma dans les données
    create_external_table = GCSToBigQueryOperator(
        task_id="load_raw_vectors",
        ### CHANGE ME ###
    )


    # Enchaîner les 2 étapes, avec la syntaxe opérateur1 >> opérateur2
    ### CHANGE ME ###