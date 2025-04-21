from __future__ import annotations
from dag_utils import read_yaml_steps
from operator_factory import operator_factory
from airflow.models.dag import DAG

steps_from_yaml = read_yaml_steps()
operator_mapping = {step["id"]: operator_factory(**step) for step in steps_from_yaml}

with DAG(dag_id="yaml_test", dag_display_name="YAML TEST") as dag:

    for operator_id, operator_builder in operator_mapping.items():
        if operator_builder.next_id is None:
            continue

        next_operator = operator_mapping[operator_builder.next_id]
        operator_builder.airflow_operator >> next_operator.airflow_operator
