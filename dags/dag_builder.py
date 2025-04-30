from __future__ import annotations
from operator_builders.operator_builder import OperatorBuilder
from airflow.datasets import Dataset
import re
from airflow.models import DAG
from constants import YESTERDAY
from collections import deque
from dataclasses import dataclass
from collections import defaultdict


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
        self._id_prefix = None
        self.operator_builders = operator_builders
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
    def num_operators(self) -> int:
        return len(self.operator_builders)

    def __str__(self):
        return (
            f"{self.__class__.__name__}(id={self.dag_id}, num_ops={self.num_operators})"
        )

    def __repr__(self):
        return str(self)

    @property
    def id_prefix(self):
        first_task = self.operator_builders[0]
        final_task = self.operator_builders[-1]
        if self._id_prefix is None:
            if final_task.operator_id == first_task.operator_id:
                self._id_prefix = first_task.operator_id
            else:
                self._id_prefix = (
                    f"from_{first_task.operator_id}_until_{final_task.operator_id}"
                )
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
            builder.display_name
            for builder in self.operator_builders
            if builder.from_yaml
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

    def _get_operator_to_upstream_operators(
        self, starting_task: OperatorBuilder
    ) -> dict[OperatorBuilder, set[OperatorBuilder]]:
        starting_task_id = starting_task.operator_id
        ids_to_check = deque()
        ids_to_check.append(starting_task_id)
        tasks_in_this_cycle = []
        operator_to_upstream_operators = defaultdict(set)

        while ids_to_check:
            task_id = ids_to_check.popleft()
            task_builder = self._operator_id_to_builder_obj[task_id]
            tasks_in_this_cycle.append(task_builder)

            for next_id in task_builder.next_ids:
                next_task = self._operator_id_to_builder_obj[next_id]
                operator_to_upstream_operators[next_task].add(task_builder)
                if next_task in tasks_in_this_cycle:
                    continue
                ids_to_check.append(next_id)

        return operator_to_upstream_operators

    def _get_partial_sub_builders(
        self,
        starting_task: OperatorBuilder,
        operator_to_upstream_operators: dict[OperatorBuilder, set[OperatorBuilder]],
    ) -> list[DagBuilder]:
        """
        This will make partial DAG SubBuilders for all tasks with more than one upstream
        operator. This is a very general condition; will also be True for simple branches 
        like
                  / --if some condition ----> Task 2 --\
        Task 1 --|                                      |--> Task 4
                  \ --if other condition --> Task 3 --/
        But DAGs should be constructed in a functional way regardless
        IMPORTANT!!! This assumes that only one of Task 2 or Task 3 will be executed. Therefore
        completing either of them will Trigger Task 4. This does NOT support running both Task 2
        and Task 3 in parallel to only then execute Task 4.
        """
        operators_with_multiple_inlets = [
            operator
            for operator in operator_to_upstream_operators
            if len(operator_to_upstream_operators[operator]) > 1
        ]
        if not operators_with_multiple_inlets:
            return []

        sub_builders = []
        operator_to_inlets: dict[OperatorBuilder, list[Dataset]] = defaultdict(list)
        operators_with_added_outlets: list[OperatorBuilder] = []
        for operator in operators_with_multiple_inlets:
            new_dataset = self._create_dataset_for_subbuilders(operator)
            operator_to_inlets[operator].append(new_dataset)
            tasks_to_modify = operator_to_upstream_operators[operator]
            for task in tasks_to_modify:
                task.remove_next_id(operator.operator_id)
                task.add_outlets([new_dataset])
                operators_with_added_outlets.append(task)

        starting_tasks = [starting_task, *operators_with_multiple_inlets]
        for starting_task in starting_tasks:
            tasks_in_subcycle = [starting_task]
            tasks_to_check = deque()
            tasks_to_check.append(starting_task)

            while tasks_to_check:
                current_task: OperatorBuilder = tasks_to_check.popleft()
                next_tasks = [
                    self._operator_id_to_builder_obj[next_id]
                    for next_id in current_task.next_ids
                ]
                tasks_to_check.extend(next_tasks)
                tasks_in_subcycle.extend(next_tasks)

            inlets = operator_to_inlets[starting_task] or None
            sub_builder = DagBuilder(
                tasks_in_subcycle, dag_id_suffix=self.dag_id_suffix, inlets=inlets
            )
            sub_builders.append(sub_builder)
        return sub_builders

    def _set_sub_builders(self):
        """
        By definition, DAGs (Directed Acyclic Graphs) cannot have cycles.
        This method will check for cycles and, if any are found, it will break down the the DAG Builder for this DAG
        into multiple smaller DAG Builders, the sub builders.
        This is done using Airflow Datasets, to make something similar to:
        DAG 1: Pipeline Start -> Task 1 -> Task 2 -> Dataset 2
        DAG 2: Dataset 2 -> Task 3 -> Task 4 ---if some condition---> end
                                             \--if other condition--> Dataset 2
        Having Task 4 re-trigger Dataset 2 is equivalent to having Task 4 go into Task 3 if "other condition" is met,
        but will not be marked as a cycle in Airflow.

        If no cycles are found, sub_builders will be set to an empty list and will not be used.
        """
        sub_builders = []
        all_next_ids = set()
        for operator_builder in self.operator_builders:
            all_next_ids.update(operator_builder.next_ids)

        starting_tasks = [
            task
            for task in self.operator_builders
            if task.operator_id not in all_next_ids
        ]

        for starting_task in starting_tasks:
            operator_to_upstream_operators = self._get_operator_to_upstream_operators(
                starting_task
            )

            this_sub_builders = self._get_partial_sub_builders(
                starting_task, operator_to_upstream_operators
            )

            sub_builders.extend(this_sub_builders)

        return sub_builders

    def _create_dataset_for_subbuilders(
        self, next_operator: OperatorBuilder
    ) -> Dataset:
        dataset_name = f"ds_before_{next_operator.operator_id}"
        if self.dag_id_suffix:
            dataset_name += f"_{self.dag_id_suffix}"
        return Dataset(dataset_name)

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
        if self._sub_builders:
            dag_list = [sub_builder.build_dag() for sub_builder in self._sub_builders]
            return dag_list

        dag = self.build_task_dependices()
        return [dag]
