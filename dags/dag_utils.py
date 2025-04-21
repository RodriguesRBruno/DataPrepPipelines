from datetime import datetime, timedelta
import os
import re
import yaml
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from typing import Literal, Any

YESTERDAY = datetime.today() - timedelta(days=1)
YAML_DIR = "/yaml_dags"

AIRFLOW_WORKSPACE_DIR = os.getenv("AIRFLOW_WORKSPACE_DIR")
AIRFLOW_DATA_DIR = os.getenv("AIRFLOW_DATA_DIR")
AIRFLOW_INPUT_DATA_DIR = os.getenv("AIRFLOW_INPUT_DATA_DIR")
HOST_WORKSPACE_DIR = os.getenv("HOST_WORKSPACE_DIR")
HOST_DATA_DIR = os.getenv("HOST_DATA_DIR")
HOST_INPUT_DATA_DIR = os.getenv("HOST_INPUT_DATA_DIR")


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


def read_yaml_steps():
    yaml_dag_files = [yaml_file for yaml_file in os.listdir(YAML_DIR)]
    yaml_dag_files = [os.path.join(YAML_DIR, yaml_file) for yaml_file in yaml_dag_files]
    yaml_dag_files = [
        yaml_file
        for yaml_file in yaml_dag_files
        if os.path.isfile(yaml_file)
        and (yaml_file.endswith(".yaml") or yaml_file.endswith(".yml"))
    ]

    yaml_file = yaml_dag_files[0]
    try:
        with open(yaml_file, "r") as f:
            yaml_dag_info = yaml.safe_load(f)
    except Exception:
        print(f"Unable to load YAML file {yaml_file}. It will be skipped.")

    return yaml_dag_info["steps"]


def create_legal_dag_id(subject_slash_timepoint, replace_char="_"):
    legal_chars = "A-Za-z0-9_-"
    legal_id = re.sub(rf"[^{legal_chars}]", replace_char, subject_slash_timepoint)
    return legal_id
