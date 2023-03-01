# Week 2

## Airflow Setup
1. Create a sub-directory called ``airflow`` in your ``project`` directory.

2. Import the official Image & Setup from the latest Airflow version:
        
        curl LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml

3. 

4. Set the Airflow User:

        mkdir -p ./dags ./logs ./plugins
        echo -e "AIRFLOW_UID=$(d -u)" > .env

5. Docker Build:

        docker-compose build

6. Docker Compose:

        docker-compose up airflow-init
        docker-compose up
        docker-compose ps

7. 


## Anatomy of DAGS