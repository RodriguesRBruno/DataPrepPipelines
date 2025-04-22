from __future__ import annotations
from dag_utils import read_yaml_steps, map_operators_from_yaml
from airflow.models import DAG
from constants import YESTERDAY

steps_from_yaml = read_yaml_steps()
dag_builders_list = map_operators_from_yaml(steps_from_yaml)

# TODO this is assuming tasks are sequential in the YAML file!! Has to be improved
# DAGs must be declared as top level code to be loaded by airflow,
# so the "with DAG(...) as dag" block MUST be top-level
for dag_builder in dag_builders_list:

    if dag_builder.inlets:
        schedule = dag_builder.inlets
    else:
        schedule = "@once"
    with DAG(
        dag_id=dag_builder.dag_id,
        dag_display_name=dag_builder.dag_display_name,
        catchup=False,
        max_active_runs=1,
        schedule=schedule,
        start_date=YESTERDAY,
        is_paused_upon_creation=False,
        # doc_md=dag_builder.dag_doc,
        tags=dag_builder.tags,
    ) as dag:

        dag_builder.build_task_dependencies()
