from .operator_builder import OperatorBuilder
from airflow.operators.empty import EmptyOperator


class EmptyOperatorBuilder(OperatorBuilder):

    def get_airflow_operator(self):
        return EmptyOperator(
            task_id=self.operator_id, task_display_name=self.display_name
        )
