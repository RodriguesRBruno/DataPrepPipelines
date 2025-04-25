from operator_builders.operator_builder import OperatorBuilder
from airflow.datasets import Dataset
import re


class DagBuilder:

    def __init__(
        self,
        operator_builders: list[OperatorBuilder],
        dag_id_suffix: str = None,
        inlets: list[Dataset] = None,
    ):
        self.dag_id_suffix = dag_id_suffix
        # TODO Assumes tasks are ordered. Improve!
        self._id_prefix = None
        self.operator_builders = operator_builders
        self._first_task = self.operator_builders[0]
        self._final_task = self.operator_builders[-1]
        self.dag_id = self.create_legal_dag_id()
        self.dag_display_name = self.create_dag_display_name()
        self.inlets = inlets
        self.tags = self.create_dag_tags()
        self._generated_operators = {}
        self._operator_id_to_builder_obj = {
            operator_builder.operator_id: operator_builder
            for operator_builder in self.operator_builders
        }

    @property
    def id_prefix(self):
        if self._id_prefix is None:
            if self._final_task.operator_id == self._first_task.operator_id:
                self._id_prefix = self._first_task.operator_id
            else:
                self._id_prefix = f"from_{self._first_task.operator_id}_until_{self._final_task.operator_id}"
        return self._id_prefix

    def create_legal_dag_id(self, replace_char="_"):
        raw_id = self.id_prefix
        if self.dag_id_suffix:
            raw_id = f"{raw_id}_{self.dag_id_suffix}"

        legal_chars = "A-Za-z0-9_-"
        legal_id = re.sub(rf"[^{legal_chars}]", replace_char, raw_id)
        return legal_id

    def create_dag_tags(self):
        tags = {
            self._first_task.display_name,
            self._final_task.display_name,
        }
        if self.dag_id_suffix is not None:
            tags.add(self.dag_id_suffix)
        return list(tags)

    @staticmethod
    def _prettify_name(base_name):
        return base_name.replace("_", " ").title()

    def create_dag_display_name(
        self,
    ):
        display_name = self._prettify_name(self.id_prefix)
        if self.dag_id_suffix:
            display_name += f" - {self.dag_id_suffix}"
        return display_name

    def _get_generated_operator_by_id(self, operator_id):
        if operator_id not in self._generated_operators:
            builder_for_this_operator = self._operator_id_to_builder_obj[operator_id]
            self._generated_operators[operator_id] = (
                builder_for_this_operator.get_airflow_operator()
            )

        return self._generated_operators[operator_id]

    def build_task_dependencies(self):
        for operator_builder in self.operator_builders:
            current_operator = self._get_generated_operator_by_id(
                operator_builder.operator_id
            )

            for next_id in operator_builder.next_ids:

                next_operator = self._get_generated_operator_by_id(next_id)
                current_operator >> next_operator
