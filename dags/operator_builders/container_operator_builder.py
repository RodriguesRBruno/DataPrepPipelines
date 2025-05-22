from __future__ import annotations
from .operator_builder import OperatorBuilder
from abc import abstractmethod
from copy import deepcopy


class ContainerOperatorBuilder(OperatorBuilder):

    def __init__(
        self,
        image: str,
        command: str | list[str],
        mounts: list[str],
        cpu_share: float = 1.0,
        mem_limit: str = None,
        **kwargs,
    ):
        self.image = image
        if isinstance(command, str):
            self.command = command.split(" ")
        else:
            self.command = command
        self.mounts = self.build_mounts(mounts)
        self.cpu_share = cpu_share
        self.mem_limit = mem_limit
        super().__init__(**kwargs)

    @abstractmethod
    def build_mounts(self):
        pass

    def _get_command(self):
        if self.partition:
            command = [*self.command, "--subject-subdir", self.partition]
        else:
            command = self.command
        return command
