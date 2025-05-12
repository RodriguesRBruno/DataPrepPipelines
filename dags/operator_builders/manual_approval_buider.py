from .operator_builder import OperatorBuilder
from airflow.sdk import task
from airflow.exceptions import AirflowException


class ManualApprovalBuilder(OperatorBuilder):
    def _define_base_operator(self):

        @task(
            task_id=self.operator_id,
            task_display_name=self.display_name,
            outlets=self.outlets,
        )
        def auto_fail():
            raise AirflowException("This task must be approved manually!")

        task_instance = auto_fail()
        return task_instance
