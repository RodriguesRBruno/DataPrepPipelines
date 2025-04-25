from __future__ import annotations
from operator_builders.branch_from_sensor_operator_builder import (
    BranchFromSensorOperatorBuilder,
)
from operator_builders.docker_operator_buider import DockerOperatorBuilder
from operator_builders.empty_operator_builder import EmptyOperatorBuilder
from operator_builders.manual_approval_buider import ManualApprovalBuilder
from operator_builders.operator_builder import OperatorBuilder
from operator_builders.python_sensor_builder import PythonSensorBuilder


def operator_factory(type, **kwargs) -> list[OperatorBuilder]:

    return_list = []
    kwargs["operator_id"] = kwargs.pop("id", None)
    id_info = kwargs.pop("next", [])

    if isinstance(id_info, dict):
        # If we have a branching condition in YAML, we return three operators:
        # OperatorFromYAML -> PythonSensorOperator -> PythonBranchOperator
        # OperatorFromYAML runs as defind by the YAML File.
        # A PythonSensorOperator then waits for any of the defind conditions to be True and
        # forwards the True condition to the PythonBranchOperator, which then branches accordingly.
        # The Sensor and Branch Operators are defined here, so we can adapt the input arguments of the first operator accordingly
        # (ie make it go into sensor that goes into branch which then goes into other operators from the YAML file)

        branching_info = id_info.pop("if")
        sensor_id = f'sensor_from_{kwargs["operator_id"]}'
        branching_id = f'branch_from_{kwargs["operator_id"]}'
        wait_time = id_info.pop("wait", None)
        default_condition = id_info.pop("else", None)
        kwargs["next_ids"] = sensor_id

        sensor_operator = PythonSensorBuilder(
            conditions=branching_info,
            wait_time=wait_time,
            default_condition=default_condition,
            operator_id=sensor_id,
            previous_task_id=kwargs["operator_id"],
            next_ids=[branching_id],
        )
        branch_operator = BranchFromSensorOperatorBuilder(
            previous_sensor=sensor_operator,
            operator_id=branching_id,
        )
        return_list.extend([sensor_operator, branch_operator])

        # TODO implement elegant solution for the tasks after branching
        # now implementing only emptyoperators to test branching itself
        # Later deal with going to previous stages, etc
        if "brain_extraction" in branch_operator.next_ids:
            branch_operator.next_ids.remove("brain_extraction")
            dummy_id = "brain_extraction_DUMMY"
            branch_operator.next_ids.append(dummy_id)
            dummy_operator = EmptyOperatorBuilder(operator_id=dummy_id, next_ids=[])
            return_list.append(dummy_operator)
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
