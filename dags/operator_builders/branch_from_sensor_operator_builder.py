from .python_sensor_builder import PythonSensorBuilder
from .operator_builder import OperatorBuilder

from airflow.decorators import task
from airflow.models.taskinstance import TaskInstance


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
