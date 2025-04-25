from __future__ import annotations
from airflow.datasets import Dataset
from airflow.models import BaseOperator
from abc import ABC, abstractmethod
from copy import deepcopy


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
