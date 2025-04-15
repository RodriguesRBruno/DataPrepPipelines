from __future__ import annotations
from airflow.datasets import Dataset

from dags.dag_utils import make_dag, make_default_display_name, read_yaml_dags

dags_from_yaml = read_yaml_dags()
for dag_from_yaml in dags_from_yaml:
    first_dag = True
    dag_list = dag_from_yaml["dags"]
    datasets_list = []
    subjects_list = None

    for i, dag in enumerate(dag_list):
        dag_id = dag.pop("dag_id")
        dag_display_name = dag.pop(
            "dag_display_name", make_default_display_name(dag_id)
        )
        tags = dag.pop("tags", None)
        doc_md = dag.pop("doc_md", None)
        operators = dag.pop("operators", [])
        schedule = "@once" if i == 0 else datasets_list
        datasets_list = []

        make_dag(
            dag_id=dag_id,
            dag_display_name=dag_display_name,
            tags=tags,
            doc_md=doc_md,
            schedule=schedule,
            operators=operators,
            datasets_list=datasets_list,
        )
        datasets_list.append(Dataset(dag_id))
