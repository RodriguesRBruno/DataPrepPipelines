from .operator_builder import OperatorBuilder
from airflow.decorators import task
from airflow.exceptions import AirflowException


class ManualApprovalBuilder(OperatorBuilder):
    def get_airflow_operator(self):

        @task(task_id=self.operator_id, task_display_name=self.display_name)
        def auto_fail():
            raise AirflowException("This task must be approved manually!")

        task_instance = auto_fail()
        return task_instance
