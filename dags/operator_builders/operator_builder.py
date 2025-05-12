from __future__ import annotations
from airflow.sdk import Asset, BaseOperator
from abc import ABC, abstractmethod
from copy import deepcopy
from constants import ALWAYS_CONDITION
from api_client.client import AirflowAPIClient
from airflow.sdk.api.client import ServerResponseError
import os
from dataclasses import dataclass, field


@dataclass
class PoolInfo:
    name: str
    slots: int
    include_deferred: bool = False
    description: str = field(init=False)

    def __post_init__(self):
        self.description = f"Pool to limit execution of tasks with ID {self.name} to up to {self.slots} parallel executions."


class OperatorBuilder(ABC):

    def __init__(
        self,
        operator_id: str,
        raw_id: str,
        next_ids: list[str] | str = None,
        limit: int = None,
        from_yaml: bool = True,
        make_outlet: bool = True,
        on_error: str = None,
        **kwargs,
    ):
        # TODO add logic to import on_error as a callable
        # Always call this init during subclass inits
        self.operator_id = operator_id
        self.raw_id = raw_id
        self.display_name = self.raw_id.replace("_", " ").title()
        if not next_ids:
            self.next_ids = []

        self.outlets = self._make_outlets(make_outlet)

        self.from_yaml = from_yaml
        if limit is None:
            self.pool_info = None
        else:
            self.pool_info = PoolInfo(name=self.raw_id, slots=limit)

        self.partition = kwargs.get("partition")
        self.tags = [self.raw_id]
        if self.partition:
            self.tags.append(self.partition)
            self.display_name += f" - {self.partition}"

    def __str__(self):
        return f"{self.__class__.__name__}(operator_id={self.operator_id})"

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.operator_id)

    def _make_outlets(self, make_outlet):
        if make_outlet:
            outlets = [Asset(self.operator_id)]
        else:
            outlets = []
        return outlets

    def get_airflow_operator(self) -> BaseOperator:
        base_operator = self._define_base_operator()
        if self.pool_info is not None and os.getenv("IS_DAG_PROCESSOR"):

            try:
                with AirflowAPIClient() as airflow_client:
                    pool_response = airflow_client.pools.create_or_update_pool(
                        name=self.pool_info.name,
                        slots=self.pool_info.slots,
                        description=self.pool_info.description,
                        include_deferred=self.pool_info.include_deferred,
                    )
                base_operator.pool = pool_response["name"]
            except ServerResponseError:
                pass

        if self.outlets:
            base_operator.outlets = self.outlets

        return base_operator

    @abstractmethod
    def _define_base_operator(self) -> BaseOperator:
        """
        Returns the initial definition of the operator object, without defining pools or outlets.
        These, if defined, are patched later in get_airflow_operator.
        """
        pass

    @classmethod
    def build_operator_list(cls, **kwargs) -> list[OperatorBuilder]:
        """
        Helper method to build a list of required Operators for a DAG Builder.
        Usually will return a list with a single element that is the desired operator
        If conditional next_ids are sent from the YAML file, then this will return a list including
        a Python Sensor Operator and a Python Branching Operator, which are both used to deal with branching
        """
        operator_list = []
        kwargs["operator_id"] = kwargs.pop("id", None)

        id_info = kwargs.pop("next", [])

        if isinstance(id_info, dict):
            # If we have a branching condition in YAML, we return three operators:
            # OperatorFromYAML -> PythonSensorOperator -> PythonBranchOperator -> EmptyOperator -> NextOperatorFromYAML
            # OperatorFromYAML runs as defind by the YAML File.
            # A PythonSensorOperator then waits for any of the defind conditions to be True and
            # forwards the True condition to the PythonBranchOperator, which then branches accordingly.
            # The Sensor and Branch Operators are defined here, so we can adapt the input arguments of the first operator accordingly
            # (ie make it go into sensor that goes into branch which then goes into other operators from the YAML file)
            # Empty operators are used between the branch operator and next operator from YAML to simplify breaking DAG cycles, if any
            # are present. If DAG cycles are not present, the Empty operators do not interfere with DAG execution.
            from .branch_from_sensor_operator_builder import (
                BranchFromSensorOperatorBuilder,
            )
            from .python_sensor_builder import PythonSensorBuilder
            from .empty_operator_builder import EmptyOperatorBuilder

            conditions_definitions = kwargs.pop(
                "conditions_definitions", []
            )  # [{'id': 'condition_1', 'type': 'function', 'function_name': 'function_name'}...]
            conditions_definitions = {
                condition["id"]: {
                    key: value for key, value in condition.items() if key != "id"
                }
                for condition in conditions_definitions
            }  # {'condition_1: {'type': 'function', 'function_name': 'function_name'}, ...}

            branching_info: list[dict[str, str]] = id_info.pop("if")
            sensor_id = f'conditions_from_{kwargs["operator_id"]}'
            branching_id = f'branch_from_{kwargs["operator_id"]}'
            wait_time = id_info.pop("wait", None)
            default_condition = id_info.pop("else", None)
            kwargs["next_ids"] = sensor_id

            conditions = branching_info
            if default_condition and default_condition != kwargs["operator_id"]:
                conditions.append(
                    {"condition": ALWAYS_CONDITION, "target": default_condition}
                )
            processed_conditions = []
            for condition in conditions:
                processed_condition = {
                    "condition": condition["condition"],
                    "target": f"empty_between_{sensor_id}_and_{condition['target']}",
                }
                processed_conditions.append(processed_condition)

            empty_ids = [condition["target"] for condition in processed_conditions]
            ids_after_empty = [condition["target"] for condition in conditions]

            sensor_operator = PythonSensorBuilder(
                conditions=processed_conditions,
                wait_time=wait_time,
                operator_id=sensor_id,
                next_ids=[branching_id],
                conditions_definitions=conditions_definitions,
                from_yaml=False,
                make_outlet=False,
            )

            # TODO this will break!
            empty_operators = [
                EmptyOperatorBuilder(operator_id=empty_id, from_yaml=False)
                for empty_id, next_id in zip(empty_ids, ids_after_empty)
            ]

            branch_operator = BranchFromSensorOperatorBuilder(
                next_ids=[empty_id for empty_id in empty_ids],
                previous_sensor=sensor_operator,
                operator_id=branching_id,
                from_yaml=False,
                make_outlet=False,
            )
            operator_list.extend([sensor_operator, branch_operator, *empty_operators])

        this_operator = cls(**kwargs)
        operator_list = [this_operator, *operator_list]
        return operator_list
