"""
DAG: EARTHQUAKE-STREAMING

Version Spark Structured Streaming (sans Kafka) du TP Flink (BAC5 seance7).
Le producer est un service Docker long-running qui depose des JSONL dans
/data/incoming. Le job Spark est egalement long-running (streaming).

Ce DAG orchestre la preparation (health check + DDL Postgres), declenche
le job Spark en mode long-running, et en parallele lance une tache de
validation qui attend quelques minutes puis compte les evenements traites
(Postgres + Parquet) pour confirmer que le pipeline produit bien.

Flow:
    health_check -> init_postgres -> [submit_spark_streaming, validate_run]

Remarque: validate_run s'execute en PARALLELE de submit_spark_streaming
car ce dernier ne termine jamais (streaming long-running). Mettre validate
en aval l'empecherait de se declencher.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.exceptions import AirflowFailException
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator
# pyrefly: ignore [missing-import]
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# pyrefly: ignore [missing-import]
from airflow.providers.postgres.hooks.postgres import PostgresHook
# pyrefly: ignore [missing-import]
from airflow.providers.postgres.operators.postgres import PostgresOperator

logger = logging.getLogger(__name__)

VALIDATION_WAIT_SECONDS = 180  # 3 min: 2 poll producer + 1 micro-batch Spark
CLEAN_DIR = "/data/clean/earthquakes"

# Charger le script SQL depuis le fichier
DAG_DIR = Path(__file__).parent.parent
SQL_INIT_TABLES = (DAG_DIR / "postgres_job" / "create_tables.sql").read_text()

default_args = {
    "owner": "bac4",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=24),
}

PG_JDBC_JAR = "/opt/airflow/jars/postgresql-42.7.3.jar"


def validate_run(**context) -> None:
    """Attend quelques minutes puis verifie que le pipeline produit bien.

    - Compte les lignes en Postgres (table earthquake_enriched)
    - Compte les fichiers Parquet sur le volume partage
    - Echoue si aucun des deux sinks n'a produit
    """
    logger.info("Waiting %ds for first micro-batches to land...", VALIDATION_WAIT_SECONDS)
    time.sleep(VALIDATION_WAIT_SECONDS)

    hook = PostgresHook(postgres_conn_id="postgres_earthquakes")
    total, by_severity, latest = hook.get_first("""
        WITH agg AS (
            SELECT COUNT(*) AS total,
                   MAX(processed_at) AS latest
            FROM earthquake_enriched
        )
        SELECT total,
               (SELECT json_object_agg(severity, n)
                FROM (SELECT severity, COUNT(*) AS n
                      FROM earthquake_enriched
                      GROUP BY severity) s),
               latest
        FROM agg;
    """)

    parquet_files = 0
    if os.path.isdir(CLEAN_DIR):
        for root, _, files in os.walk(CLEAN_DIR):
            parquet_files += sum(1 for f in files if f.endswith(".parquet"))

    logger.info("=" * 60)
    logger.info("VALIDATION REPORT")
    logger.info("=" * 60)
    logger.info("Postgres  - earthquake_enriched rows : %s", total)
    logger.info("Postgres  - by severity              : %s", by_severity)
    logger.info("Postgres  - latest processed_at      : %s", latest)
    logger.info("Parquet   - files under %s : %d", CLEAN_DIR, parquet_files)
    logger.info("=" * 60)

    if (total or 0) == 0 and parquet_files == 0:
        raise AirflowFailException(
            "No events processed after "
            f"{VALIDATION_WAIT_SECONDS}s - check producer and spark job logs."
        )

    logger.info("Pipeline is producing. Validation OK.")

with DAG(
    dag_id="EARTHQUAKE-STREAMING",
    description="USGS -> files -> Spark Structured Streaming -> Parquet + Postgres",
    start_date=datetime(2026, 4, 14),
    catchup=False,
    schedule_interval=None,
    default_args=default_args,
    tags=["seance9", "spark", "streaming"],
) as dag:

    # ========================================================================
    # TACHE 1 : health_check (BashOperator)
    # ========================================================================
    # TODO: Compléter cette tâche
    #
    # Objectif: vérifier que les 3 dossiers partagés existent et sont accessibles
    #
    # Instructions:
    # - Créer un BashOperator avec task_id="health_check"
    # - bash_command doit :
    #   1. Créer les 3 dossiers: /data/incoming, /data/clean/earthquakes,
    #      /data/checkpoints/earthquake
    #      Indice: mkdir -p /data/... /data/... /data/...
    #   2. Lister leurs propriétés avec "ls -ld"
    #   3. Afficher un message de succès "echo 'Filesystem OK'"
    # - Chaîner les commandes avec " && "
    #
    # Format: "command1 && command2 && command3"
    health_check = BashOperator(
        task_id="health_check",
        bash_command="""
            # TODO: A COMPLÉTER
            raise NotImplementedError("À implémenter")
        """,
    )

    # ========================================================================
    # TACHE 2 : init_postgres (PostgresOperator)
    # ========================================================================
    # TODO: Compléter cette tâche
    #
    # Objectif: créer les tables Postgres nécessaires pour les événements
    #
    # Instructions:
    # - Créer un PostgresOperator avec :
    #   - task_id="init_postgres"
    #   - postgres_conn_id="postgres_earthquakes" (fourni)
    #   - sql=SQL_INIT_TABLES (variable chargée au-dessus, contient le DDL)
    #
    # La variable SQL_INIT_TABLES contient :
    # - CREATE TABLE earthquake_enriched (...)
    # - CREATE TABLE earthquake_enriched_stg (...)
    init_postgres = PostgresOperator(
        task_id="init_postgres",
        # TODO: A COMPLÉTER
        # Vous devez ajouter: postgres_conn_id et sql
    )

    # ========================================================================
    # TACHE 3 : submit_spark_streaming (SparkSubmitOperator)
    # ========================================================================
    # TODO: Compléter cette tâche
    #
    # Objectif: lancer le job Spark Structured Streaming
    #
    # Instructions:
    # - Créer un SparkSubmitOperator avec les paramètres suivants:
    #
    #   task_id="submit_spark_streaming"
    #   conn_id="spark_default"                                   (maître Spark)
    #   application="/opt/airflow/spark_jobs/earthquake_streaming.py"
    #   name="earthquake-structured-streaming"                    (nom du job)
    #   jars=PG_JDBC_JAR                                          (driver JDBC)
    #
    #   conf={                                                    (config Spark)
    #       "spark.executor.memory": "1g",
    #       "spark.driver.memory": "1g",
    #       "spark.sql.shuffle.partitions": "4",
    #       "spark.hadoop.fs.permissions.umask-mode": "000",
    #   }
    #
    #   env_vars={                                                (vars d'environnement)
    #       "INCOMING_DIR": "/data/incoming",
    #       "CLEAN_DIR": "/data/clean/earthquakes",
    #       "CHECKPOINT_ROOT": "/data/checkpoints/earthquake",
    #       "PG_URL": "jdbc:postgresql://postgres:5432/earthquakes",
    #       "PG_USER": "airflow",
    #       "PG_PASSWORD": "airflow",
    #       "PG_TABLE": "earthquake_enriched",
    #   }
    #
    #   execution_timeout=timedelta(hours=24)
    submit_spark_streaming = SparkSubmitOperator(
        # TODO: A COMPLÉTER
        # Vous devez ajouter: task_id, conn_id, application, name, jars, conf, env_vars, execution_timeout
    )

    # ========================================================================
    # TACHE 4 : validate_run (PythonOperator)
    # ========================================================================
    # TODO: Compléter cette tâche
    #
    # Objectif: vérifier que le pipeline produit bien après quelques minutes
    #
    # Instructions:
    # - Créer un PythonOperator avec les paramètres suivants:
    #
    #   task_id="validate_run"
    #   python_callable=validate_run              (fonction définie ci-dessus)
    #   execution_timeout=timedelta(minutes=10)   (timeout de 10 minutes)
    #
    # Cette tâche va:
    # 1. Attendre 3 minutes (VALIDATION_WAIT_SECONDS)
    # 2. Compter les lignes en Postgres
    # 3. Compter les fichiers Parquet
    # 4. Afficher un rapport de validation
    validate_run_task = PythonOperator(
        # TODO: A COMPLÉTER
        # Vous devez ajouter: task_id, python_callable, execution_timeout
    )

    # ========================================================================
    # FLOW: Définir les dépendances entre tâches
    # ========================================================================
    # TODO: Compléter les dépendances
    #
    # Ordre d'exécution attendu:
    #
    #   health_check
    #       |
    #       v
    #   init_postgres
    #      /        \
    #     v          v
    # submit_spark_streaming    validate_run
    #
    # En syntaxe Airflow:
    # health_check >> init_postgres >> [submit_spark_streaming, validate_run_task]
    #
    # Cela veut dire:
    # - health_check PUIS init_postgres (séquentiel avec >>)
    # - PUIS submit_spark_streaming ET validate_run_task EN PARALLELE (avec [])
    #
    # TODO: Écrire la ligne de dépendance ci-dessous
    # health_check >> ... >> [...]
