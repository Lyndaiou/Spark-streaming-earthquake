# earthquake_streaming — Spark Structured Streaming (sans Kafka)

Version **temps réel** du TP Séance 9 : l'API USGS est interrogée en
continu par un producer Docker, et un job **Spark Structured Streaming**
long-running consomme les fichiers déposés, enrichit les événements, puis
écrit en parallèle sur **Parquet** (stockage froid) et sur **PostgreSQL**
(analytique).

Pas de Kafka : on utilise un **file source** sur un volume partagé. C'est
le pattern "poor man's streaming" — idéal pour un TP pédagogique.

---

## 1. Architecture

```
                    ┌────────────────────────┐
                    │   USGS earthquake API  │
                    └───────────┬────────────┘
                                │ poll toutes les 30s
                                ▼
                  ┌──────────────────────────┐
                  │  earthquake-producer     │   (container long-running)
                  │  (python + requests)     │
                  └────────────┬─────────────┘
                               │ écriture atomique .tmp + os.replace()
                               ▼
              /data/incoming/eq_<YYYYMMDDTHHMMSS>.json       (JSONL)
                               │
                               ▼
                  ┌──────────────────────────┐
                  │  Spark Structured        │
                  │  Streaming (file source) │
                  │  - readStream.json()     │
                  │  - enrichissement        │
                  └────────┬─────────┬───────┘
                           │         │
                           ▼         ▼
        writeStream (parquet)    foreachBatch (JDBC)
                │                         │
                ▼                         ▼
   /data/clean/earthquakes/     Postgres: earthquake_enriched
   (partitionné event_date)     (table analytique)
```

Deux `writeStream` partagent la même DataFrame source : Spark n'effectue
**qu'un seul scan** des fichiers par micro-batch, mais chaque requête a
son **propre checkpoint** dans `/data/checkpoints/earthquake/`.

---

## 2. Structure du projet

```
earthquake_streaming/
├── README.md                            <-- ce fichier
├── dags/
│   └── earthquake_streaming_dag.py      DAG Airflow (health → DDL → submit)
├── spark_jobs/
│   └── earthquake_streaming.py          Job Structured Streaming
├── producer/
│   └── earthquake_producer.py           Poller USGS long-running
└── infra/
    ├── docker-compose.yml               Stack complète (Spark + Airflow + Postgres + producer)
    ├── Dockerfile.airflow               Airflow 2.9 + pyspark 3.3 + JDBC jar
    ├── init-postgres.sql                CREATE DATABASE airflow_db + earthquakes
    └── .env.airflow                     Vars d'env Airflow + Spark + Postgres
```

---

## 3. Composants détaillés

### 3.1 Producer — [`producer/earthquake_producer.py`](producer/earthquake_producer.py)

- Poll l'API USGS `fdsnws/event/1/query` toutes les `POLL_INTERVAL`
  secondes (30 par défaut).
- Initial lookback : `INITIAL_LOOKBACK_DAYS` jours (7 par défaut) pour
  avoir tout de suite des données à traiter.
- Filtre : `MIN_MAGNITUDE` (2.5 par défaut).
- **Écriture atomique** : chaque batch est écrit dans `eq_<ts>.json.tmp`
  puis renommé via `os.replace()` en `eq_<ts>.json`. C'est essentiel car
  Spark file source indexe les fichiers dès qu'ils apparaissent — sans
  cette astuce, il pourrait lire un fichier partiellement écrit.
- Format de sortie : **JSONL** (une ligne = un événement). Colonnes :
  `event_id`, `magnitude`, `mag_type`, `place`, `event_time`, `latitude`,
  `longitude`, `depth_km`, `significance`, `tsunami`, `status`, `type`,
  `title`, `ingested_at`.

### 3.2 Job Spark — [`spark_jobs/earthquake_streaming.py`](spark_jobs/earthquake_streaming.py)

**Lecture** : `readStream.json(/data/incoming)` avec un schéma explicite
(plus rapide + robuste aux fichiers mal formés). `maxFilesPerTrigger=5`.

**Enrichissement** ajoute quatre colonnes :

| Colonne          | Règle |
|------------------|-------|
| `depth_category` | `<70` = Superficiel · `<300` = Intermediaire · sinon Profond |
| `severity`       | `tsunami=1` → TSUNAMI · ≥8 Devastateur · ≥7 Majeur · ≥6 Fort · ≥5 Modere · sinon Mineur |
| `is_significant` | 1 si `significance > 500`, sinon 0 |
| `processed_at`   | `current_timestamp()` |

Plus `event_ts` (timestamp parsé) et `event_date` (partition key).

**Sink 1 — Parquet** (`writeStream.format("parquet")`)
- Path : `/data/clean/earthquakes/` partitionné par `event_date`
- Checkpoint : `/data/checkpoints/earthquake/parquet`
- Trigger : `processingTime=10 seconds`

**Sink 2 — Postgres** (`foreachBatch` → JDBC)
- Table : `earthquake_enriched` (DDL créée par Airflow avant le submit)
- Checkpoint : `/data/checkpoints/earthquake/postgres`
- Trigger : `processingTime=10 seconds`
- Mode `append` sur chaque micro-batch non vide.

**Long-running** : `spark.streams.awaitAnyTermination()` — le job reste
vivant tant qu'aucune des deux queries ne plante.

### 3.3 DAG Airflow — [`dags/earthquake_streaming_dag.py`](dags/earthquake_streaming_dag.py)

```
health_check  ──►  init_postgres  ──►  ┬──►  submit_spark_streaming   (long-running)
                                       └──►  validate_run             (sanity check apres 3 min)
```

| Tâche                    | Opérateur              | Rôle |
|--------------------------|------------------------|------|
| `health_check`           | BashOperator           | Vérifie que `/data/incoming`, `/data/clean/earthquakes`, `/data/checkpoints/earthquake` sont bien montés |
| `init_postgres`          | PostgresOperator       | `CREATE TABLE IF NOT EXISTS earthquake_enriched (...)` sur la DB `earthquakes` |
| `submit_spark_streaming` | SparkSubmitOperator    | Lance le job Spark avec `--jars postgresql-42.7.3.jar` et les env vars Postgres |
| `validate_run`           | PythonOperator         | Attend 3 min puis compte les événements en Postgres + fichiers Parquet. Fail si les deux sinks sont vides |

**Pourquoi `validate_run` est en parallèle de `submit_spark_streaming`** :
le job Spark ne termine jamais (streaming long-running), donc une tâche
downstream avec trigger_rule par défaut ne se déclencherait jamais. On la
met en **sibling** : elle démarre dès que `init_postgres` est OK, dort 3
minutes pour laisser le temps au producer (poll 30s) et au premier
micro-batch Spark (trigger 10s) de s'exécuter, puis :

1. Query Postgres : `SELECT COUNT(*)` + répartition par `severity` + dernier `processed_at`
2. Compte les fichiers `.parquet` sous `/data/clean/earthquakes/`
3. Log un rapport de validation
4. **Fail** si `rows == 0` ET `parquet_files == 0` (rien n'a été produit)

Les logs de `validate_run` sont visibles dans l'UI Airflow — c'est le
moyen le plus rapide de confirmer "combien d'événements ont été traités".

`schedule_interval=None` : le DAG se déclenche **manuellement**. La tâche
`submit_spark_streaming` reste en état `running` tant que le job streaming
tourne (c'est normal — `execution_timeout=24h` par défaut).

### 3.4 Stack Docker — [`infra/docker-compose.yml`](infra/docker-compose.yml)

| Service               | Image                                  | Rôle |
|-----------------------|----------------------------------------|------|
| `data-init`           | busybox                                | Crée les dirs sur le volume `eq_data` avec les bons droits (uid 50000) |
| `postgres`            | postgres:15                            | Deux DB : `airflow_db` (metadata) et `earthquakes` (analytique) |
| `earthquake-producer` | python:3.11-slim + requests            | Long-running, poll USGS → JSONL dans `/data/incoming` |
| `spark-master`        | bde2020/spark-master:3.3.0-hadoop3.3   | Master du cluster Standalone (port 8080, 7077) |
| `spark-worker`        | bde2020/spark-worker:3.3.0-hadoop3.3   | 2 cores, 2 Go RAM |
| `airflow-init`        | build local                            | `db migrate` + création user admin |
| `airflow-webserver`   | build local                            | UI Airflow (port 8085) |
| `airflow-scheduler`   | build local                            | Scheduler + driver `spark-submit` |

**Volumes partagés** :
- `eq_data` → monté dans producer, spark-master, spark-worker,
  airflow-scheduler. Sert de bus entre le producer (producteur de fichiers)
  et Spark (consommateur).
- `eq_postgres_data`, `eq_airflow_logs`.

---

## 4. Démarrage

```bash
cd infra/
docker compose build
docker compose up -d
```

Vérifier que les conteneurs sont `running` et `healthy` :
```bash
docker compose ps
```

| UI                  | URL                            | Login             |
|---------------------|--------------------------------|-------------------|
| Airflow             | http://localhost:8085          | `admin` / `admin` |
| Spark Master        | http://localhost:8080          | —                 |
| Spark Worker        | http://localhost:8081          | —                 |

Dans Airflow, activer + déclencher le DAG **`EARTHQUAKE-STREAMING`**.
La tâche `submit_spark_streaming` passe en `running` puis y reste.

---

## 5. Vérifier les résultats

### Producer — est-ce qu'il produit ?
```bash
docker compose logs -f earthquake-producer
docker exec -it eq-spark-worker ls -lh /data/incoming/ | head
```

### Parquet — sink 1
```bash
docker exec -it eq-spark-worker ls -R /data/clean/earthquakes/
```

Lire avec Spark :
```bash
docker exec -it eq-spark-worker /spark/bin/spark-shell --master local \
    -e 'spark.read.parquet("/data/clean/earthquakes").show(10, false)'
```

### Postgres — sink 2
```bash
docker exec -it eq-postgres psql -U airflow -d earthquakes \
    -c "SELECT event_id, magnitude, severity, processed_at
        FROM earthquake_enriched
        ORDER BY processed_at DESC LIMIT 10;"
```

Compter par sévérité :
```bash
docker exec -it eq-postgres psql -U airflow -d earthquakes \
    -c "SELECT severity, COUNT(*) FROM earthquake_enriched GROUP BY severity;"
```

### Spark UI
- Jobs en cours : http://localhost:8080 → application
  `earthquake-structured-streaming`
- L'onglet **Structured Streaming** montre les deux queries
  `sink-parquet` et `sink-postgres` avec leur input/processing rate.

---

## 6. Variables d'environnement

Toutes modifiables via [`infra/.env.airflow`](infra/.env.airflow) et la
section `environment:` du producer dans `docker-compose.yml`.

| Var                  | Défaut                                     | Où |
|----------------------|--------------------------------------------|----|
| `POLL_INTERVAL`      | `30`                                       | producer |
| `MIN_MAGNITUDE`      | `2.5`                                      | producer |
| `INITIAL_LOOKBACK_DAYS` | `7`                                     | producer |
| `INCOMING_DIR`       | `/data/incoming`                           | producer + job Spark |
| `CLEAN_DIR`          | `/data/clean/earthquakes`                  | job Spark |
| `CHECKPOINT_ROOT`    | `/data/checkpoints/earthquake`             | job Spark |
| `PG_URL`             | `jdbc:postgresql://postgres:5432/earthquakes` | job Spark |
| `PG_USER` / `PG_PASSWORD` | `airflow` / `airflow`                 | job Spark |
| `PG_TABLE`           | `earthquake_enriched`                      | job Spark |

---

## 7. Points techniques notables

- **Écriture atomique obligatoire** — Spark file source indexe un fichier
  dès qu'il apparaît dans le dossier. Si le producer `open()` → `write()`
  → `close()` directement sur le nom final, Spark peut lire un fichier à
  moitié écrit et crasher. On écrit donc en `.tmp` puis `os.replace()`.
- **Double umask (OS + Hadoop)** — sur volume Docker partagé, spark-worker
  tourne en root (uid 0) mais airflow-scheduler en uid 50000. Sans forcer
  les droits, le commit Spark échoue au `rename _temporary/... → final`.
  Deux correctifs cumulés :
  - OS umask : `command: ["/bin/bash", "-c", "umask 000 && /bin/bash /master.sh"]`
    dans `spark-master` et `spark-worker`.
  - Hadoop umask : `spark.hadoop.fs.permissions.umask-mode=000` dans le
    `conf` du `SparkSubmitOperator` (Hadoop applique son propre umask
    indépendant de celui de l'OS, défaut 022 → dirs 755).
- **Bug provider Spark 4.1.5** — `SparkSubmitHook` concatène
  `{host}:{port}` sans préserver le scheme `spark://`. Contournement : on
  encode le scheme dans le *host* de l'URI Airflow :
  `AIRFLOW_CONN_SPARK_DEFAULT=spark://spark%3A%2F%2Fspark-master:7077`.
- **JDBC driver** — `postgresql-42.7.3.jar` téléchargé dans
  `/opt/airflow/jars` au build de `Dockerfile.airflow`, puis passé à
  `spark-submit` via `jars=PG_JDBC_JAR`.
- **Checkpoints séparés** — chaque sink a son propre dossier de
  checkpoint. Ne **jamais** les mutualiser, sinon Spark se plaint d'un
  mismatch d'offsets et refuse de redémarrer.

---

## 8. Arrêt / reset

```bash
cd infra/
docker compose down           # stop + remove containers, garde les volumes
docker compose down -v        # + supprime eq_data, eq_postgres_data, eq_airflow_logs
```

Si tu veux repartir d'un état propre mais sans reconstruire les images,
`down -v` puis `up -d` suffit.
