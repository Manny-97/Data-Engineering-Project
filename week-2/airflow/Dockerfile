FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

# SHELL ["/bin/bash", "-o", "pipefail", "-e", "-e", "-u", "-x", "-c"]

# ARG CLOUD_SDK_VERSION=322.0.0
# ENV GCLOUD_HOME=/home/google-cloud-sdk

# ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

# RUN DOWNLOAD_URL=""


# WORKDIR $AIRFLOW_HOME

# USER $AIRFLOW_UID