### Spark SQL, GCS and BigQuery

The for-hire vehicle (FHV) trips that have been tracked and provided to the TLC since 2015 make up the example dataset. This project includes scripts that assist you in loading the example dataset into Google BigQuery from various data sources. In order to query data from Google BigQuery and load the results into Google BigQuery, four SQL scripts will be executed. Apache Spark will handle the query jobs, and Apache Airflow will schedule them. The bash script and Docker Compose file both contain compilations of all necessary tools and dependencies.

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
- The queries used in this data pipeline is located at this [File](airflow-spark-taxi/spark/app/read-postgres.py)
