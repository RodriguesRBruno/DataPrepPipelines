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


def operator_factory(type, **kwargs) -> OperatorBuilder:

    kwargs["operator_id"] = kwargs.pop("id", None)
    kwargs["next_ids"] = kwargs.pop("next", [])

    if type == "container":
        # TODO different check (env config?) for docker vs singularity, for now just docker:
        return DockerOperatorBuilder(**kwargs)

    elif type == "manual_approval":
        return ManualApprovalBuilder(**kwargs)

    elif type == "branch":
        return BranchOperatorBuilder(**kwargs)
    else:
        raise TypeError(f"Tasks of type {type} are not supported!")


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
        # Always call this init at the end of subclass inits
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


class BranchOperatorBuilder(OperatorBuilder):
    pass
