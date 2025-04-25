from __future__ import annotations
from .operator_builder import OperatorBuilder
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from copy import deepcopy
from pipeline_state import PipelineState
from constants import ALWAYS_CONDITION

import os


def mock_annotation_done(pipeline_state: PipelineState):

    base_review_dir = os.path.join(
        pipeline_state.airflow_data_dir,
        "manual_review",
        "tumor_extraction",
        pipeline_state.running_subject,
    )
    finalized_dir = os.path.join(base_review_dir, "finalized")
    dir_files = os.listdir(finalized_dir)

    if len(dir_files) == 0:
        print("Reviewed annotation not Found!")
        return False

    elif len(dir_files) > 1:
        print(
            "More than one annotation found! Please only keep one file in the finalized directory"
        )
        return False

    formatted_subject = pipeline_state.running_subject.replace("/", "_")
    proper_name = f"{formatted_subject}_tumorMask_model_0.nii.gz"
    if dir_files[0] != proper_name:
        print(
            f"Reviewed file has been renamed! Please make sure the file is named\n{proper_name}\nto ensure the pipeline runs correctly!"
        )
        return False
    return True


def mock_brain_mask_changed(pipeline_state: PipelineState):

    base_review_dir = os.path.join(
        pipeline_state.airflow_data_dir,
        "manual_review",
        "brain_mask",
        pipeline_state.running_subject,
    )
    finalized_dir = os.path.join(base_review_dir, "finalized")
    dir_files = os.listdir(finalized_dir)

    if len(dir_files) == 0:
        print("No brain mask change detected.")
        return False

    elif len(dir_files) > 1:
        print(
            "More than one brain mask correction found! Please only keep one file in the finalized directory."
        )
        return False

    proper_name = f"brainMask_fused.nii.gz"
    if dir_files[0] != proper_name:
        print(
            f"Brain Mask file has been renamed! Please make sure the file is named\n{proper_name}\nto ensure the pipeline runs correctly!"
        )
        return False
    return True


def evaluate_external_condition(condition_name: str, pipeline_state: PipelineState):
    # TODO implement properly! Should call the external python files and define a
    # pipeline object from airflow_kwargs that is sent to the Python callable.
    # For this first proof of concept, hardcode functions just to validate functionality.
    if condition_name == ALWAYS_CONDITION:
        return True
    elif condition_name == "annotation_done":
        return mock_annotation_done(pipeline_state)
    elif condition_name == "brain_mask_changed":
        return mock_brain_mask_changed(pipeline_state)
    raise ValueError(f"Unknown condition {condition_name}!")


class PythonSensorBuilder(OperatorBuilder):

    def __init__(
        self,
        conditions: list[dict[str, str]],
        wait_time: float = 60,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conditions = conditions
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

    def get_airflow_operator(self):

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
                condition_name = condition["condition"]
                target_id = condition["target"]
                if evaluate_external_condition(condition_name, pipeline_state):
                    return PokeReturnValue(is_done=True, xcom_value=target_id)

            return False

        return wait_for_conditions()
