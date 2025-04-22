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
    operator_id_to_obj = {
        operator_builder.operator_id: operator_builder
        for operator_builder in dag_builder.operator_builders
    }

    if dag_builder.inlets:
        schedule = dag_builder.inlets
    else:
        schedule = "@once"

    tags = [dag_builder.dag_display_name]
    if dag_builder.dag_id_suffix:
        tags.append(dag_builder.dag_id_suffix)

    with DAG(
        dag_id=dag_builder.dag_id,
        dag_display_name=dag_builder.dag_display_name,
        catchup=False,
        max_active_runs=1,
        schedule=schedule,
        start_date=YESTERDAY,
        is_paused_upon_creation=False,
        # doc_md=dag_builder.dag_doc,
        tags=tags,
    ) as dag:

        generated_operators = {}

        for operator_builder in dag_builder.operator_builders:
            if operator_builder.operator_id not in generated_operators:
                generated_operators[operator_builder.operator_id] = (
                    operator_builder.get_airflow_operator()
                )

            if operator_builder.next_id is None:
                continue

            if operator_builder.next_id not in generated_operators:
                next_builder = operator_id_to_obj[operator_builder.next_id]
                generated_operators[next_builder.operator_id] = (
                    next_builder.get_airflow_operator()
                )

            current_operator = generated_operators[operator_builder.operator_id]
            next_operator = generated_operators[next_builder.operator_id]
            current_operator >> next_operator
