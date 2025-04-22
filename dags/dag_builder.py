from operator_factory import OperatorBuilder
from airflow.datasets import Dataset
import re
from airflow.models import DAG
from constants import YESTERDAY


class DagBuilder:

    def __init__(
        self,
        dag_id_prefix: str,
        dag_id_suffix: str = None,
        operator_builders: list[OperatorBuilder] = None,
        inlets: list[Dataset] = None,
        outlets: list[Dataset] = None,
    ):
        self.dag_id_suffix = dag_id_suffix
        self.dag_id = self.create_legal_dag_id(dag_id_prefix)
        self.dag_display_name = self.create_dag_display_name(dag_id_prefix)
        self.operator_builders = operator_builders
        self.inlets = inlets
        self.outlets = outlets

    def create_legal_dag_id(self, id_prefix, replace_char="_"):
        if self.dag_id_suffix:
            raw_id = f"{id_prefix}_{self.dag_id_suffix}"
        else:
            raw_id = id_prefix
        legal_chars = "A-Za-z0-9_-"
        legal_id = re.sub(rf"[^{legal_chars}]", replace_char, raw_id)
        return legal_id

    def create_dag_display_name(self, id_prefix):
        display_name = id_prefix.replace("_", " ").title()
        if self.dag_id_suffix:
            display_name += f" - {self.dag_id_suffix}"
        return display_name

    def build_dag(self):
        operator_id_to_obj = {
            operator_builder.operator_id: operator_builder
            for operator_builder in self.operator_builders
        }

        if self.inlets:
            schedule = self.inlets
        else:
            schedule = "@once"

        tags = [self.dag_display_name]
        if self.dag_id_suffix:
            tags.append(self.dag_id_suffix)

        with DAG(
            dag_id=self.dag_id,
            dag_display_name=self.dag_display_name,
            catchup=False,
            max_active_runs=1,
            schedule=schedule,
            start_date=YESTERDAY,
            is_paused_upon_creation=False,
            # doc_md=self.dag_doc,
            tags=tags,
        ) as dag:

            if len(self.operator_builders) == 1:
                self.operator_builders[0].airflow_operator

            else:
                for operator_builder in self.operator_builders:
                    if operator_builder.next_id is None:
                        continue
                    next_operator_builder = operator_id_to_obj[operator_builder.next_id]

                    current_operator = operator_builder.airflow_operator
                    next_operator = (next_operator_builder.airflow_operator,)

                    current_operator >> next_operator
        return dag
