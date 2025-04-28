from __future__ import annotations
from airflow.datasets import Dataset
from airflow.models import BaseOperator
from abc import ABC, abstractmethod
from copy import deepcopy
from airflow.models import Pool


class OperatorBuilder(ABC):

    def __init__(
        self,
        operator_id: str,
        next_ids: list[str] | str,
        on_error: str = None,
        outlets: list[Dataset] = None,
        limit: int = None,
        from_yaml: bool = True,
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

        self.from_yaml = from_yaml

        if limit is not None:
            pool_obj = Pool.create_or_update_pool(
                name=self.operator_id,
                slots=limit,
                description=f"Pool to limit execution of tasks with ID {self.operator_id} to up to {limit} parallel executions.",
                include_deferred=False,
            )
            self.pool = pool_obj.pool
        else:
            self.pool = None

    @property
    def display_name(self) -> str:
        return self.operator_id.replace("_", " ").title()

    def get_airflow_operator(self) -> BaseOperator:
        base_operator = self._define_base_operator()
        if self.pool is not None:
            base_operator.pool = self.pool

        if self.outlets:
            base_operator.outlets = self.outlets

        return base_operator

    @abstractmethod
    def _define_base_operator(self) -> BaseOperator:
        """
        Returns the initial definition of the operator object, without defining pools or outlets.
        These, if defined, are patched later in get_airflow_operator.
        """
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

    def remove_next_id(self, next_id):
        self.next_ids.remove(next_id)
