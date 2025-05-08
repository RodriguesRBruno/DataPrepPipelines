import os
import re
import yaml
from typing import Literal, Any, Optional, Union
from operator_builders.operator_builder import OperatorBuilder
from operator_factory import operator_factory
from dag_builder import DagBuilder
from constants import (
    AIRFLOW_INPUT_DATA_DIR,
    AIRFLOW_WORKSPACE_DIR,
    YAML_DIR,
)
from copy import deepcopy
from airflow.datasets import Dataset


class ReportSummary:

    _REPORT_SUMMARY_FAILE = os.path.join(
        AIRFLOW_WORKSPACE_DIR, "report_summary.yaml"
    )  # TODO maybe use Workspace dir?

    def __init__(
        self,
        execution_status: Literal["running", "failure", "done"],
        progress_dict: dict[str, Any] = None,
    ):
        self.execution_status = execution_status
        self.progress_dict = progress_dict if progress_dict is not None else {}

    def to_dict(self):
        report_dict = {
            "execution_status": self.execution_status,
            "progress": self.progress_dict,
        }
        return report_dict

    def write_yaml(self):
        report_dict = self.to_dict()
        with open(self._REPORT_SUMMARY_FAILE, "w") as f:
            yaml.dump(
                report_dict,
                f,
                sort_keys=False,
            )


def read_yaml_definition():
    yaml_files = [yaml_file for yaml_file in os.listdir(YAML_DIR)]
    yaml_files = [os.path.join(YAML_DIR, yaml_file) for yaml_file in yaml_files]
    yaml_files = [
        yaml_file
        for yaml_file in yaml_files
        if os.path.isfile(yaml_file)
        and (yaml_file.endswith(".yaml") or yaml_file.endswith(".yml"))
    ]

    yaml_file = yaml_files[0]
    try:
        with open(yaml_file, "r") as f:
            raw_content = f.read()
            expanded_content = os.path.expandvars(raw_content)
            yaml_info = yaml.safe_load(expanded_content)
    except Exception:
        print(f"Unable to load YAML file {yaml_file}. It will be skipped.")

    return yaml_info


def get_per_subject_from_step(step: Optional[dict[str, str]] = None):
    if step is None:
        return None
    else:
        return step.get("per_subject", False)


def make_dag_builder_list(
    previous_per_subject: bool,
    steps_for_dag: list[OperatorBuilder],
    subject_subdirectories: list[str],
    inlets=list[Dataset],
    add_outlet_to_final_task: bool = True,
) -> tuple[list[DagBuilder], list[Dataset]]:
    dags_list = []
    outlets = []
    inlets = inlets.copy()
    if previous_per_subject:
        tmp_outlets = []
        for subject_slash_timepoint in subject_subdirectories:
            this_dag_task_list = []
            outlets = []
            for i, dag_task in enumerate(steps_for_dag):
                new_dag_task = dag_task.create_per_subject(subject_slash_timepoint)
                final_task = new_dag_task

                if add_outlet_to_final_task and i == len(steps_for_dag) - 1:
                    outlets = [
                        Dataset(
                            f"ds_{new_dag_task.operator_id}_{subject_slash_timepoint}"
                        )
                    ]
                    new_dag_task.add_outlets(outlets)

                this_dag_task_list.append(final_task)

            this_dag = DagBuilder(
                dag_id_suffix=subject_slash_timepoint,
                operator_builders=this_dag_task_list,
                inlets=inlets,
            )
            tmp_outlets.extend(outlets)
            dags_list.append(this_dag)
        outlets = tmp_outlets

    else:
        final_task = steps_for_dag[-1]
        if add_outlet_to_final_task:
            outlets = [Dataset(f"ds_{final_task.operator_id}")]
            final_task.add_outlets(outlets)
        this_dag = DagBuilder(operator_builders=steps_for_dag, inlets=inlets)
        dags_list.append(this_dag)

    return dags_list, outlets


def map_operators_from_yaml(
    yaml_info: dict[str, Union[list[dict[str, str]], dict[str, str]]],
) -> list[DagBuilder]:
    steps_from_yaml = yaml_info["steps"]
    conditions_from_yaml = yaml_info.get("conditions", [])
    per_subject_definition = yaml_info.get("per_subject_def", {})

    subject_subdirectories = read_subject_directories(per_subject_definition)
    dags_list = []
    steps_for_dag: list[OperatorBuilder] = []
    previous_outlets = []
    steps_from_yaml = [None, *steps_from_yaml]
    for previous_step, current_step in zip(steps_from_yaml[:-1], steps_from_yaml[1:]):
        previous_per_subject = get_per_subject_from_step(previous_step)
        per_subject = get_per_subject_from_step(current_step)

        changed_per_subject = per_subject != previous_per_subject

        if changed_per_subject and steps_for_dag:
            tmp_dags_list, tmp_outlets = make_dag_builder_list(
                previous_per_subject=previous_per_subject,
                steps_for_dag=steps_for_dag,
                subject_subdirectories=subject_subdirectories,
                inlets=previous_outlets,
                add_outlet_to_final_task=True,
            )
            dags_list.extend(tmp_dags_list)
            previous_outlets = tmp_outlets
            steps_for_dag = []

        current_step["conditions_definitions"] = conditions_from_yaml
        new_operators = operator_factory(**current_step)
        steps_for_dag.extend(new_operators)
        previous_per_subject = per_subject

    final_dags_list, _ = make_dag_builder_list(
        previous_per_subject=previous_per_subject,
        steps_for_dag=steps_for_dag,
        subject_subdirectories=subject_subdirectories,
        inlets=previous_outlets,
        add_outlet_to_final_task=False,
    )
    dags_list.extend(final_dags_list)

    return dags_list


def add_to_dags_dict(
    steps_for_dag: list[OperatorBuilder], dags_dict, subject_suffix=""
):
    last_step = steps_for_dag[-1]
    dag_id = last_step.operator_id
    if subject_suffix:
        dag_id = f"{dag_id} - {subject_suffix}"

    dag_id = create_legal_dag_id(dag_id)
    dags_dict[dag_id] = steps_for_dag


def read_subject_directories(per_subject_definition):
    from pipeline_state import PipelineState

    if not per_subject_definition:
        return []

    per_subjection_function_name = per_subject_definition["function_name"]
    per_subject_function_obj = import_external_python_function(
        per_subjection_function_name
    )
    subject_list = per_subject_function_obj(PipelineState())
    return subject_list


def import_external_python_function(function_path: str):
    import importlib
    import sys

    sys.path.append(
        "/Users/brunorodrigues/MLCommons/DataPrepPipelines/pipeline_examples/rano/dags_from_yaml"
    )
    condition_module, condition_function = function_path.rsplit(".", maxsplit=1)
    imported_module = importlib.import_module(condition_module)
    function_obj = getattr(imported_module, condition_function)

    return function_obj


def create_legal_dag_id(subject_slash_timepoint, replace_char="_"):
    legal_chars = "A-Za-z0-9_-"
    legal_id = re.sub(rf"[^{legal_chars}]", replace_char, subject_slash_timepoint)
    return legal_id
