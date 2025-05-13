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
from yaml_parser.yaml_parser import YamlParser
from typing import TYPE_CHECKING, Any
from api_client.client import AirflowAPIClient
from airflow.utils.state import DagRunState
from collections import defaultdict

if TYPE_CHECKING:
    from api_client.client import AirflowAPIClient

SUMMARIZER_ID = "pipeline_summarizer"
SUMMARIZER_TAG = "Pipeline Summarizer"
yaml_parser = YamlParser()
steps_from_yaml = yaml_parser.raw_steps
ordered_step_ids = [step["id"] for step in steps_from_yaml]

with DAG(
    dag_id=SUMMARIZER_ID,
    dag_display_name="Summarizer DAG",
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
            most_recent_dag_runs[dag_id] = most_recent_run

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
        all_dags: dict[str, dict[str, Any]],
        most_recent_dag_runs: dict[str, dict[str, Any] | None],
    ):
        import pandas as pd  # Import in task to not slow down dag parsing

        dag_info_dicts = []
        for dag_id, run_dict in most_recent_dag_runs.items():
            this_dag = all_dags[dag_id]
            if run_dict is None:
                run_state = None
            else:
                run_state = run_dict["state"]

            dag_step_tags = [
                tag["name"]
                for tag in this_dag["tags"]
                if tag["name"] in ordered_step_ids
            ]
            update_dict = {
                "DAG ID": dag_id,
                "DAG Display Name": this_dag["dag_display_name"],
                "DAG Run State": run_state,
                "DAG Step Tag": dag_step_tags,
            }

            dag_info_dicts.append(update_dict)

        progress_df = pd.DataFrame(dag_info_dicts)
        progress_df = progress_df.explode("DAG Step Tag")
        progress_df = progress_df.sort_values(
            by=["DAG Step Tag"],
            key=_sort_column,
        )
        all_dag_tags = progress_df["DAG Step Tag"].unique()
        summary_dict = defaultdict(lambda: dict())

        for dag_tag in all_dag_tags:

            relevant_df = progress_df[progress_df["DAG Step Tag"] == dag_tag]
            if relevant_df.empty:
                continue
            task_success_ratio = len(
                relevant_df[relevant_df["DAG Run State"] == DagRunState.SUCCESS]
            ) / len(relevant_df)
            success_percentage = round(task_success_ratio * 100, 3)
            summary_dict[dag_tag] = success_percentage

        summary_dict = dict(summary_dict)

        if all(
            dag_run_state == DagRunState.SUCCESS
            for dag_run_state in relevant_df["DAG Run State"]
        ):
            execution_status = "done"
        else:
            execution_status = "running"

        report_summary = ReportSummary(
            execution_status=execution_status, progress_dict=summary_dict
        )
        return report_summary

    @task(
        task_id="pipeline_summarizer",
        task_display_name="Pipeline Summarizer",
        retries=2,
        retry_delay=30,
    )
    def rano_summarizer():
        with AirflowAPIClient() as airflow_client:
            all_dags = _get_dag_id_to_dag_dict(airflow_client)
            most_recent_dag_runs = _get_most_recent_dag_runs(all_dags, airflow_client)
            report_summary = _get_report_summary(all_dags, most_recent_dag_runs)
        report_summary.write_yaml()

    rano_summarizer()
