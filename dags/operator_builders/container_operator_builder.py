from __future__ import annotations
from .operator_builder import OperatorBuilder
from abc import abstractmethod
from copy import deepcopy


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

    @abstractmethod
    def build_mounts(self):
        pass

    def create_per_subject(self, subject_slash_timepoint) -> ContainerOperatorBuilder:
        base_copy = deepcopy(self)
        extra_command = ["--subject-subdir", subject_slash_timepoint]
        base_copy.command.extend(extra_command)
        return base_copy
