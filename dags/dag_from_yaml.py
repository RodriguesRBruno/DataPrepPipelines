from __future__ import annotations
from dag_utils import read_yaml_steps, map_operators_from_yaml

steps_from_yaml = read_yaml_steps()
dag_builders_list = map_operators_from_yaml(steps_from_yaml)

for dag_builder in dag_builders_list:
    # Make sure to return DAGs at top level so they are added to Airflow
    current_dags = dag_builder.build_dag()
