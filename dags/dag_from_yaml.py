from __future__ import annotations
from dag_utils import read_yaml_definition, map_operators_from_yaml

yaml_info = read_yaml_definition()
dag_builders_list = map_operators_from_yaml(yaml_info)

for dag_builder in dag_builders_list:
    # Make sure to return DAGs at top level so they are added to Airflow
    current_dags = dag_builder.build_dag()
