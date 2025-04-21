from __future__ import annotations
from abc import ABC, abstractmethod
from dag_utils import HOST_DATA_DIR, HOST_INPUT_DATA_DIR, HOST_WORKSPACE_DIR
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import BaseOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException


def operator_factory(type, **kwargs) -> OperatorBuilder:

    kwargs["operator_id"] = kwargs.pop("id", None)
    kwargs["next_id"] = kwargs.pop("next", None)

    if type == "container":
        # TODO different check (env config?) for docker vs singularity, for now just docker:
        return DockerOperatorBuilder(**kwargs)

    elif type == "manual_approval":
        return ManualApprovalBuilder(**kwargs)

    else:
        raise TypeError(f"Tasks of type {type} are not supported!")


class OperatorBuilder(ABC):

    def __init__(self, operator_id: str, next_id: str, on_error: str = None, **kwargs):
        # TODO add logic to import on_error as a callable
        # Always call this init at the end of subclass inits
        self.operator_id = operator_id
        self.next_id = next_id
        self._airflow_operator = None

    @property
    def airflow_operator(self) -> BaseOperator:
        if self._airflow_operator is None:
            self._airflow_operator = self._get_airflow_operator()
        return self._airflow_operator

    @property
    def display_name(self) -> str:
        return self.operator_id.replace("_", " ").title()

    @abstractmethod
    def _get_airflow_operator(self) -> BaseOperator:
        pass


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

    def _get_airflow_operator(self) -> DockerOperator:
        return DockerOperator(
            image=self.image,
            command=self.command,
            mounts=self.mounts,
            task_id=self.operator_id,
            task_display_name=self.display_name,
        )


class ManualApprovalBuilder(OperatorBuilder):
    def _get_airflow_operator(self):

        @task(task_id=self.operator_id, task_display_name=self.display_name)
        def auto_fail():
            raise AirflowException("This task must be approved manually!")

        task_instance = auto_fail()
        return task_instance
