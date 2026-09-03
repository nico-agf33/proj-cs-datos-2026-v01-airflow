FROM astrocrpublic.azurecr.io/runtime:3.3-7

ENV PYTHONPATH="/usr/local/airflow/include:${PYTHONPATH}"
