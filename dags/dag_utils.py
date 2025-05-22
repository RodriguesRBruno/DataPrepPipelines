import os
import re
import yaml
from typing import Literal, Any
from constants import AIRFLOW_WORKSPACE_DIR


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


def import_external_python_function(function_path: str):
    import importlib

    condition_module, condition_function = function_path.rsplit(".", maxsplit=1)
    imported_module = importlib.import_module(condition_module)
    function_obj = getattr(imported_module, condition_function)

    return function_obj


def create_legal_dag_id(subject_slash_timepoint, replace_char="_"):
    legal_chars = "A-Za-z0-9_-"
    legal_id = re.sub(rf"[^{legal_chars}]", replace_char, subject_slash_timepoint)
    return legal_id
