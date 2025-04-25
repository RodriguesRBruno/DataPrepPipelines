from __future__ import annotations
from operator_builders.operator_builder import OperatorBuilder
from airflow.datasets import Dataset
import re
from airflow.models import DAG
from constants import YESTERDAY
from airflow.models import DagBag
from collections import deque
from dataclasses import dataclass


@dataclass
class TaskToModify:
    task_builder: OperatorBuilder
    next_ids_to_modify: list[str]


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
        self._sub_builders: list[DagBuilder] = self._set_sub_builders()

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

    def _set_sub_builders(self):
        # find tasks that start the dag (not next_id of anything)
        # check for cycles
        # if found, deal with them
        sub_builders = []
        all_next_ids = set()
        for operator_builder in self.operator_builders:
            all_next_ids.update(operator_builder.next_ids)

        starting_task_ids = [
            task_id
            for task_id in self._operator_id_to_builder_obj.keys()
            if task_id not in all_next_ids
        ]

        for starting_task_id in starting_task_ids:
            tasks_to_modify: list[TaskToModify] = []
            ids_to_check = deque()
            ids_to_check.append(starting_task_id)
            tasks_in_this_cycle = []
            while ids_to_check:
                task_id = ids_to_check.pop()
                tasks_in_this_cycle.append(task_id)
                task_builder = self._operator_id_to_builder_obj[task_id]
                next_ids_to_modify = []
                for next_id in task_builder.next_ids:
                    if next_id in tasks_in_this_cycle:
                        next_ids_to_modify.append(next_id)
                        continue
                    ids_to_check.append(next_id)
                if next_ids_to_modify:
                    task_to_modify = TaskToModify(
                        task_builder=task_builder, next_ids_to_modify=next_ids_to_modify
                    )
                    tasks_to_modify.append(task_to_modify)

            if tasks_to_modify:
                for task_to_modify in tasks_to_modify:
                    # TODO for now assumes we can return to the start of the DAG
                    # Later generalize to arbitrary cycles (ie in the middle of the DAG) which will require further splits
                    for next_id in task_to_modify.next_ids_to_modify:
                        task_to_modify.task_builder.remove_next_id(next_id)

                    task_to_modify.task_builder.add_outlets(self.inlets)

                builder_objs = [
                    self._operator_id_to_builder_obj[operator_id]
                    for operator_id in tasks_in_this_cycle
                ]
                sub_builder = self.__class__(
                    operator_builders=builder_objs,
                    dag_id_suffix=self.dag_id_suffix,
                    inlets=self.inlets,
                )
                sub_builders.append(sub_builder)

    def build_task_dependices(self) -> DAG:
        if self.inlets:
            schedule = self.inlets
        else:
            schedule = "@once"
        with DAG(
            dag_id=self.dag_id,
            dag_display_name=self.dag_display_name,
            catchup=False,
            max_active_runs=1,
            schedule=schedule,
            start_date=YESTERDAY,
            is_paused_upon_creation=False,
            tags=self.tags,
            auto_register=True,
        ) as dag:
            for operator_builder in self.operator_builders:
                current_operator = self._get_generated_operator_by_id(
                    operator_builder.operator_id
                )

                for next_id in operator_builder.next_ids:

                    next_operator = self._get_generated_operator_by_id(next_id)
                    current_operator >> next_operator

        return dag

    def build_dag(self) -> list[DAG]:
        dag_list = []
        if self._sub_builders:

            for sub_builder in self._sub_builders:
                dag_list.extend(sub_builder.build_dag())
                return

        dag = self.build_task_dependices()
        return [dag]
