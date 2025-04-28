from __future__ import annotations
from .operator_builder import OperatorBuilder
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from copy import deepcopy
from pipeline_state import PipelineState
from constants import ALWAYS_CONDITION


class Condition:

    def __init__(
        self,
        condition_id: str,
        next_id: str,
        conditions_definitions: dict[str, dict[str, str]],
    ):
        self.condition_id = condition_id
        self.next_id = next_id

        if self.condition_id == ALWAYS_CONDITION:
            self.type = ALWAYS_CONDITION
            self.complete_function_name = None

        else:
            this_definition = conditions_definitions[self.condition_id]
            self.type = this_definition["type"]  # Currently unused
            self.complete_function_name = this_definition["function_name"]


def evaluate_external_condition(condition: Condition, pipeline_state: PipelineState):
    # TODO implement properly! Should call the external python files and define a
    # pipeline object from airflow_kwargs that is sent to the Python callable.
    # For this first proof of concept, hardcode functions just to validate functionality.
    if condition.condition_id == ALWAYS_CONDITION:
        return True

    import importlib

    condition_module, condition_function = condition.complete_function_name.rsplit(
        ".", maxsplit=1
    )
    imported_module = importlib.import_module(condition_module)
    condition_function_obj = getattr(imported_module, condition_function)
    return condition_function_obj(pipeline_state)


class PythonSensorBuilder(OperatorBuilder):

    def __init__(
        self,
        conditions: list[dict[str, str]],
        conditions_definitions: dict[str, dict[str, str]],
        wait_time: float = 60,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conditions = [
            Condition(
                condition_id=condition["condition"],
                next_id=condition["target"],
                conditions_definitions=conditions_definitions,
            )
            for condition in conditions
        ]
        self.wait_time = wait_time
        self.running_subject = None

    def create_per_subject(self, subject_slash_timepoint: str) -> PythonSensorBuilder:
        """
        Returns a copy of this object with modifications necessary to run on a per-subject basis,
        if necessary.
        In this class, simply returns an unchanged copy. Modify in subclasses as necessary.
        """
        copy_obj = deepcopy(self)
        copy_obj.running_subject = subject_slash_timepoint
        return copy_obj

    def _define_base_operator(self):

        @task.sensor(
            poke_interval=self.wait_time,
            mode="reschedule",
            task_id=self.operator_id,
            task_display_name=self.display_name,
        )
        def wait_for_conditions(**kwargs):
            pipeline_state = PipelineState(
                airflow_kwargs=kwargs, running_subject=self.running_subject
            )

            for condition in self.conditions:
                if evaluate_external_condition(condition, pipeline_state):
                    return PokeReturnValue(is_done=True, xcom_value=condition.next_id)

            return False

        return wait_for_conditions()
