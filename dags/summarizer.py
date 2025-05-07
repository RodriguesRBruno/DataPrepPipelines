"""
This DAG is responsible for summarizing the status of the pipeline into a
yaml file that can be sent to the MedPerf stage. This summary can be used
by the Benchmark Comitte to track how Data Preparation is going at each
participant and assist users that appear to be struggling.
"""

from __future__ import annotations
from airflow.decorators import task
from airflow.models.dag import DAG
from dag_utils import ReportSummary
from constants import YESTERDAY
from datetime import timedelta
from airflow.utils.state import TaskInstanceState
from collections import defaultdict
from dag_utils import read_yaml_steps
from typing import TYPE_CHECKING, Any
from api_client.client import get_client_instance

if TYPE_CHECKING:
    from api_client.client import AirflowAPIClient

SUMMARIZER_ID = "pipeline_summarizer"
SUMMARIZER_TAG = "Pipeline Summarizer"
steps_from_yaml, _ = read_yaml_steps()
ordered_step_ids = [step["id"] for step in steps_from_yaml]

with DAG(
    dag_id=SUMMARIZER_ID,
    dag_display_name="Summarizer",
    catchup=False,
    max_active_runs=1,
    schedule=timedelta(minutes=30),
    start_date=YESTERDAY,
    is_paused_upon_creation=False,
    doc_md="This DAG generates and periodically updates the report_summary.yaml file that is sent to the MedPerf servers.",
    tags=[SUMMARIZER_TAG],
) as dag:

    def _get_dag_id_to_dag_dict(client: AirflowAPIClient) -> dict[str, dict[str, Any]]:
        all_dags = client.dags.get_all_dags()["dags"]

        all_dags = {
            dag["dag_id"]: dag for dag in all_dags if dag["dag_id"] != SUMMARIZER_ID
        }

        return all_dags

    def _get_most_recent_dag_runs(
        all_dags: dict[str, dict[str, Any]], client: AirflowAPIClient
    ) -> dict[str, dict[str, Any] | None]:
        most_recent_dag_runs = {}

        for dag_id in all_dags.keys():
            most_recent_run = client.dag_runs.get_most_recent_dag_run(dag_id=dag_id)[
                "dag_runs"
            ]
            if not most_recent_run:
                most_recent_run = None
            else:
                most_recent_run = most_recent_run[0]
            print(f"{dag_id=}\n{most_recent_run=}\n\n")
            most_recent_dag_runs[dag_id] = most_recent_run

        print(f"{most_recent_dag_runs=}")

        return most_recent_dag_runs

    def _sort_column(col):
        sorted_indices = []
        for task_id in col:
            if task_id in ordered_step_ids:
                sorted_indices.append(ordered_step_ids.index(task_id))
            else:
                sorted_indices.append(0)

        return sorted_indices

    def _get_report_summary(
        most_recent_dag_runs: dict[str, dict[str, Any] | None],
        client: AirflowAPIClient,
    ):
        import pandas as pd  # Import in task to not slow down dag parsing

        progress_df = pd.DataFrame(
            {
                "DAG ID": [],
                "Task Name": [],
                "Task ID": [],
                "Task Status": [],
            }
        )

        for dag_id, run_dict in most_recent_dag_runs.items():
            if run_dict is None:
                task_list = client.tasks.get_tasks(dag_id=dag_id)["tasks"]
            else:
                task_list = client.task_instances.get_task_instances_in_dag_run(
                    dag_id=dag_id, dag_run_id=run_dict["dag_run_id"]
                )["task_instances"]

            print(f"{task_list=}")
            for task_dict in task_list:
                task_id = task_dict["task_id"]
                if task_id not in ordered_step_ids:
                    continue

                update_dict = {
                    "Task Name": task_dict["task_display_name"],
                    "Task ID": task_id,
                    "DAG ID": dag_id,
                    "Task Status": task_dict.get("state", None),
                }
                task_df = pd.DataFrame([update_dict])
                progress_df = pd.concat([progress_df, task_df])

        progress_df = progress_df.sort_values(
            by=["Task ID"],
            key=_sort_column,
        )
        all_task_ids = progress_df["Task ID"].unique()
        summary_dict = defaultdict(lambda: dict())

        for task_id in all_task_ids:

            relevant_df = progress_df[progress_df["Task ID"] == task_id]

            task_success_ratio = len(
                relevant_df[relevant_df["Task Status"] == TaskInstanceState.SUCCESS]
            ) / len(relevant_df)
            sucess_percentage = round(task_success_ratio * 100, 3)
            for task_name in relevant_df["Task Name"].unique():
                summary_dict[task_name] = sucess_percentage

        summary_dict = dict(summary_dict)

        execution_status = "done"
        for task_name, success_percentage in summary_dict.items():
            if success_percentage < 100.0:
                execution_status = "running"
                break

        report_summary = ReportSummary(
            execution_status=execution_status, progress_dict=summary_dict
        )
        return report_summary

    @task(task_id="pipeline_summarizer", task_display_name="Pipeline Summarizer")
    def rano_summarizer():
        airflow_client = get_client_instance()
        all_dags = _get_dag_id_to_dag_dict(airflow_client)
        most_recent_dag_runs = _get_most_recent_dag_runs(all_dags, airflow_client)
        report_summary = _get_report_summary(most_recent_dag_runs, airflow_client)
        report_summary.write_yaml()

    rano_summarizer()
