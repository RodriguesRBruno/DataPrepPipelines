from __future__ import annotations
from abc import ABC, abstractmethod
from constants import HOST_DATA_DIR, HOST_INPUT_DATA_DIR, HOST_WORKSPACE_DIR
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import BaseOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.datasets import Dataset
from copy import deepcopy
from airflow.sensors.base import PokeReturnValue
from airflow.operators.empty import EmptyOperator
from airflow.models.taskinstance import TaskInstance
from constants import (
    HOST_DATA_DIR,
    HOST_WORKSPACE_DIR,
    HOST_INPUT_DATA_DIR,
    AIRFLOW_DATA_DIR,
    AIRFLOW_INPUT_DATA_DIR,
    AIRFLOW_WORKSPACE_DIR,
)
import os


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


class OperatorBuilder(ABC):

    def __init__(
        self,
        operator_id: str,
        next_ids: list[str] | str,
        on_error: str = None,
        outlets: list[Dataset] = None,
        **kwargs,
    ):
        # TODO add logic to import on_error as a callable
        # Always call this init during subclass inits
        self.operator_id = operator_id
        if not next_ids:
            self.next_ids = []

        elif isinstance(next_ids, str):
            self.next_ids = [next_ids]
        else:
            self.next_ids = next_ids
        self.outlets = outlets or []

    @property
    def display_name(self) -> str:
        return self.operator_id.replace("_", " ").title()

    @abstractmethod
    def get_airflow_operator(self) -> BaseOperator:
        pass

    def add_outlets(self, outlet_list: list[Dataset]):
        self.outlets.extend(outlet_list)
        self.next_ids = []

    def create_per_subject(self, subject_slash_timepoint: str) -> OperatorBuilder:
        """
        Returns a copy of this object with modifications necessary to run on a per-subject basis,
        if necessary.
        In this class, simply returns an unchanged copy. Modify in subclasses as necessary.
        """
        return deepcopy(self)


class ContainerOperatorBuilder(OperatorBuilder):

    def __init__(
        self, image: str, command: str | list[str], mounts: list[str], **kwargs
    ):
        self.image = image
        if isinstance(command, str):
            self.command = command.split(" ")
        else:
            self.command = command
        self.mounts = self.build_mounts(mounts)
        super().__init__(**kwargs)

    def replace_with_host_paths(self, yaml_str):
        mount_replacements = {
            "INPUT_DATA_DIR": HOST_INPUT_DATA_DIR,
            "DATA_DIR": HOST_DATA_DIR,
            "WORKSPACE_DIR": HOST_WORKSPACE_DIR,
        }

        for original_value, new_value in mount_replacements.items():
            yaml_str = yaml_str.replace(original_value, new_value)
        return yaml_str

    @abstractmethod
    def build_mounts(self):
        pass

    def create_per_subject(self, subject_slash_timepoint) -> ContainerOperatorBuilder:
        base_copy = deepcopy(self)
        extra_command = ["--subject-subdir", subject_slash_timepoint]
        base_copy.command.extend(extra_command)
        return base_copy


class DockerOperatorBuilder(ContainerOperatorBuilder):

    def build_mounts(self, mounts):
        docker_mounts = []
        for mount in mounts:
            host_path, docker_path = mount.rsplit(":", maxsplit=1)
            host_path = self.replace_with_host_paths(host_path)
            docker_mounts.append(
                Mount(source=host_path, target=docker_path, type="bind")
            )
        return docker_mounts

    def get_airflow_operator(self) -> DockerOperator:
        return EmptyOperator(
            task_id=self.operator_id,
            task_display_name=self.display_name,
            outlets=self.outlets,
        )

        return DockerOperator(
            image=self.image,
            command=self.command,
            mounts=self.mounts,
            task_id=self.operator_id,
            task_display_name=self.display_name,
            outlets=self.outlets,
            auto_remove="success",
        )


class ManualApprovalBuilder(OperatorBuilder):
    def get_airflow_operator(self):

        @task(task_id=self.operator_id, task_display_name=self.display_name)
        def auto_fail():
            raise AirflowException("This task must be approved manually!")

        task_instance = auto_fail()
        return task_instance


class PipelineState:
    # TODO properly define

    def __init__(self, running_subject, airflow_kwargs):
        self.running_subject = running_subject
        self.airflow_kwargs = airflow_kwargs
        self.host_input_data_dir = HOST_INPUT_DATA_DIR
        self.airflow_input_data_dir = AIRFLOW_INPUT_DATA_DIR
        self.host_data_dir = HOST_DATA_DIR
        self.airflow_data_dir = AIRFLOW_DATA_DIR
        self.airflow_workspace_dir = AIRFLOW_WORKSPACE_DIR
        self.host_workspace_dir = HOST_WORKSPACE_DIR


def mock_annotation_done(pipeline_state: PipelineState):

    base_review_dir = os.path.join(
        AIRFLOW_DATA_DIR,
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
        AIRFLOW_DATA_DIR, "manual_review", "brain_mask", pipeline_state.running_subject
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
    if condition_name == "annotation_done":
        return mock_annotation_done(pipeline_state)
    elif condition_name == "brain_mask_changed":
        return mock_brain_mask_changed(pipeline_state)
    raise ValueError(f"Unknown condition {condition_name}!")


class PythonSensorBuilder(OperatorBuilder):

    def __init__(
        self,
        conditions: dict[str, str],
        wait_time: float = 60,
        default_condition: str = None,
        previous_task_id: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conditions = conditions
        if default_condition == previous_task_id:
            default_condition = None
        self.default_condition = default_condition
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

            if self.default_condition is not None:
                return PokeReturnValue(is_done=True, xcom_value=self.default_condition)

            return False

        return wait_for_conditions()


class BranchFromSensorOperatorBuilder(OperatorBuilder):

    def __init__(
        self,
        previous_sensor: PythonSensorBuilder,
        **kwargs,
    ):

        next_ids = [condition["target"] for condition in previous_sensor.conditions]
        if previous_sensor.default_condition is not None:
            next_ids.append(previous_sensor.default_condition)

        self.sensor_task_id = previous_sensor.operator_id
        super().__init__(next_ids=next_ids, **kwargs)

    def get_airflow_operator(self):

        @task.branch(task_id=self.operator_id, task_display_name=self.display_name)
        def branching(task_instance: TaskInstance):
            """Read next task from the Sensor XCom (which detected any of the branching conditions)
            and branch into that"""
            print(f"{self.next_ids=}")
            xcom_data = task_instance.xcom_pull(task_ids=self.sensor_task_id)
            return [xcom_data]

        return branching()


class EmptyOperatorBuilder(OperatorBuilder):

    def get_airflow_operator(self):
        return EmptyOperator(
            task_id=self.operator_id, task_display_name=self.display_name
        )
