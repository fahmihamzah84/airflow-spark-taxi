### Spark SQL, GCS and BigQuery

- First, you need to modify google_credentials.json with your details or replace the file
- Build container that contain spark, airflow and all necessary tools
```bash
cd docker && docker compose up
```
- You can open Spark on 8181 port
```bash
localhost:8181
```
- You can open Airflow WebUI on 
```bash
localhost:8282
```
- The queries used in this data pipeline is located at this [File](https://github.com/fahmihamzah84/airflow-spark-taxi/blob/master/spark/app/read-postgres.py)
