from datetime import datetime, timedelta
import os

YESTERDAY = datetime.today() - timedelta(days=1)
YAML_DIR = os.getenv("YAML_DAGS_DIR") or "/yaml_dags"
AIRFLOW_WORKSPACE_DIR = os.getenv("AIRFLOW_WORKSPACE_DIR")
AIRFLOW_DATA_DIR = os.getenv("AIRFLOW_DATA_DIR")
AIRFLOW_INPUT_DATA_DIR = os.getenv("AIRFLOW_INPUT_DATA_DIR")
HOST_WORKSPACE_DIR = os.getenv("HOST_WORKSPACE_DIR")
HOST_DATA_DIR = os.getenv("HOST_DATA_DIR")
HOST_INPUT_DATA_DIR = os.getenv("HOST_INPUT_DATA_DIR")
