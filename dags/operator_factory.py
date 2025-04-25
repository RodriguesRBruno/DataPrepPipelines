from __future__ import annotations
from operator_builders.branch_from_sensor_operator_builder import (
    BranchFromSensorOperatorBuilder,
)
from operator_builders.docker_operator_buider import DockerOperatorBuilder
from operator_builders.empty_operator_builder import EmptyOperatorBuilder
from operator_builders.manual_approval_buider import ManualApprovalBuilder
from operator_builders.operator_builder import OperatorBuilder
from operator_builders.python_sensor_builder import PythonSensorBuilder
from constants import ALWAYS_CONDITION


def operator_factory(type, **kwargs) -> list[OperatorBuilder]:

    return_list = []
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

        branching_info: list[dict[str, str]] = id_info.pop("if")
        sensor_id = f'sensor_from_{kwargs["operator_id"]}'
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
                "condititon": condition["condition"],
                "target": f"empty_before_{condition['target']}",
            }
            processed_conditions.append(processed_condition)

        empty_ids = [condition["target"] for condition in processed_conditions]
        ids_after_empty = [condition["target"] for condition in conditions]

        sensor_operator = PythonSensorBuilder(
            conditions=processed_conditions,
            wait_time=wait_time,
            operator_id=sensor_id,
            next_ids=[branching_id],
        )

        empty_operators = [
            EmptyOperatorBuilder(operator_id=empty_id, next_ids=[next_id])
            for empty_id, next_id in zip(empty_ids, ids_after_empty)
        ]

        branch_operator = BranchFromSensorOperatorBuilder(
            next_ids=[empty_id for empty_id in empty_ids],
            previous_sensor=sensor_operator,
            operator_id=branching_id,
        )
        return_list.extend([sensor_operator, branch_operator, *empty_operators])

    else:
        kwargs["next_ids"] = id_info

    if type == "container":
        # TODO different check (env config?) for docker vs singularity, for now just docker:
        return_list.append(DockerOperatorBuilder(**kwargs))

    elif type == "manual_approval":
        return_list.append(ManualApprovalBuilder(**kwargs))

    else:
        raise TypeError(f"Tasks of type {type} are not supported!")

    return return_list
