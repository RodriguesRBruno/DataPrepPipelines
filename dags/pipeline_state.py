from constants import (
    HOST_DATA_DIR,
    HOST_INPUT_DATA_DIR,
    HOST_WORKSPACE_DIR,
    AIRFLOW_DATA_DIR,
    AIRFLOW_INPUT_DATA_DIR,
    AIRFLOW_WORKSPACE_DIR,
)


class PipelineState:
    # TODO properly define

    def __init__(self, running_subject, airflow_kwargs):
        self.running_subject = running_subject
        self.airflow_kwargs = airflow_kwargs
        self.host_input_data_dir = HOST_INPUT_DATA_DIR
        self.airflow_input_data_dir = AIRFLOW_INPUT_DATA_DIR
        self.host_data_dir = HOST_DATA_DIR
        self.airflow_data_dir = AIRFLOW_DATA_DIR
        self.airflow_workspace_dir = AIRFLOW_WORKSPACE_DIR
        self.host_workspace_dir = HOST_WORKSPACE_DIR
