from __future__ import annotations
from yaml_parser.yaml_parser import YamlParser

parser = YamlParser()
dags = parser.build_dags()
for i, dag in enumerate(dags):
    globals()[f"dag_{i}"] = dag

from airflow.sdk import DAG

with DAG(dag_id="empty") as dag:
    pass
