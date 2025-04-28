"""
This DAG is responsible for summarizing the status of the pipeline into a
yaml file that can be sent to the MedPerf stage. This summary can be used
by the Benchmark Comitte to track how Data Preparation is going at each
participant and assist users that appear to be struggling.
"""

from __future__ import annotations
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from dag_utils import ReportSummary
from constants import YESTERDAY
from datetime import timedelta
from airflow.utils.state import State
from collections import defaultdict
from dag_utils import read_yaml_steps

SUMMARIZER_ID = "pipeline_summarizer"
SUMMARIZER_TAG = "Pípeline Summarizer"
AGGREGATE_DAG_TAG = "Aggregate DAG"
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

    def _get_dags_and_subject_tags() -> tuple[dict[str, DAG], set[str]]:
        dag_bag: DagBag = DagBag(include_examples=False)
        relevant_dags: list[DAG] = [
            dag for dag in dag_bag.dags.values() if dag.dag_id != SUMMARIZER_ID
        ]

        all_dags = {dag.dag_id: dag for dag in relevant_dags}
        all_dags = {dag_id: all_dags[dag_id] for dag_id in sorted(all_dags)}

        return all_dags

    def _get_most_recent_dag_runs(all_dags: dict[str, DAG]) -> dict[str, DagRun | None]:

        most_recent_dag_runs = {
            dag_id: dag.get_last_dagrun(include_externally_triggered=True)
            for dag_id, dag in all_dags.items()
        }

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
        all_dags: dict[str, DAG],
        most_recent_dag_runs: dict[str, DagRun | None],
    ):
        import pandas as pd  # Import in task to not slow down dag parsing

        progress_df = pd.DataFrame(
            {
                "DAG Tags": [],
                "DAG ID": [],
                "Task Name": [],
                "Task ID": [],
                "Task Status": [],
            }
        )

        for dag_id, run_obj in most_recent_dag_runs.items():
            if run_obj is None:
                task_list = all_dags[dag_id].tasks
                for task in task_list:
                    task.state = State.NONE
                run_state = State.NONE
            else:
                task_list = run_obj.get_task_instances()
                run_state = run_obj.state

            for task in task_list:
                task_id = task.task_id
                if task_id not in ordered_step_ids:
                    continue

                update_dict = {
                    "Task Name": task.task_display_name,
                    "Task ID": task_id,
                    "DAG ID": dag_id,
                    "Task Status": task.state,
                    "Run Status": run_state,
                    "Task Complete ID": f"{dag_id}.{task_id}",
                }
                task_df = pd.DataFrame([update_dict])
                progress_df = pd.concat([progress_df, task_df])

        progress_df = progress_df.sort_values(
            by=["Task ID"],
            key=_sort_column,
        )
        all_tasks = progress_df["Task Complete ID"].unique()
        summary_dict = defaultdict(lambda: dict())

        for task_complete_id in all_tasks:
            relevant_df = progress_df[
                progress_df["Task Complete ID"] == task_complete_id
            ]
            task_success_ratio = len(
                relevant_df[relevant_df["Task Status"] == State.SUCCESS]
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

        all_dags = _get_dags_and_subject_tags()
        most_recent_dag_runs = _get_most_recent_dag_runs(all_dags)
        report_summary = _get_report_summary(all_dags, most_recent_dag_runs)
        report_summary.write_yaml()

    rano_summarizer()
