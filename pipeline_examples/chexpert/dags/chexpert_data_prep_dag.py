from __future__ import annotations
from airflow.models.dag import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
import os
from docker.types import Mount


YESTERDAY = datetime.now() - timedelta(days=1)
HOST_WORKSPACE = os.getenv("HOST_WORKSPACE_DIR")
HOST_DATA_DIR = os.getenv("HOST_DATA_DIR")
HOST_INPUT_DATA_DIR = os.getenv("HOST_INPUT_DATA_DIR")

DOCKER_IMAGE = "mlcommons/chexpert-prep:0.1.0"
mounts_list = [
    Mount(
        source=HOST_WORKSPACE,
        target="/workspace",
        type="bind",
    ),
    Mount(
        source=HOST_INPUT_DATA_DIR,
        target="/workspace/CheXPert-v1.0-small",
        type="bind",
        read_only=True,
    ),
    Mount(
        source=HOST_DATA_DIR,
        target="/data",
        type="bind",
    ),
]

with DAG(
    dag_id="chexpert_pipeline_explicit_dag",
    dag_display_name="CheXpert Pipeline - Explicit DAG",
    catchup=True,
    max_active_runs=1,
    schedule="@once",
    start_date=YESTERDAY,
    is_paused_upon_creation=True,
    doc_md="Example DAG for the CheXpert data preparation",
    tags=["All Subjects"],
) as dag:

    preprocess = DockerOperator(
        image=DOCKER_IMAGE,
        command=[
            "prepare",
            "--data_path",
            "/workspace/CheXPert-v1.0-small",
            "--labels_path",
            "/workspace/CheXPert-v1.0-small",
            "--parameters_file",
            "/workspace/parameters.yaml",
            "--output_path",
            "/data",
        ],
        mounts=mounts_list,
        auto_remove="success",
        mount_tmp_dir=False,
        task_id="prepare",
        task_display_name="Data Preparation",
    )

    sanity_check = DockerOperator(
        image=DOCKER_IMAGE,
        command=[
            "sanity_check",
            "--data_path",
            "/data",
            "--parameters_file",
            "/workspace/parameters.yaml",
        ],
        mounts=mounts_list,
        auto_remove="success",
        mount_tmp_dir=False,
        task_id="sanity_check",
        task_display_name="Sanity Check",
    )

    statistics = DockerOperator(
        image=DOCKER_IMAGE,
        command=[
            "statistics",
            "--data_path",
            "/data",
            "--parameters_file",
            "/workspace/parameters.yaml",
            "--output_path",
            "/data/statistics.yaml",
        ],
        mounts=mounts_list,
        auto_remove="success",
        mount_tmp_dir=False,
        task_id="statistics",
        task_display_name="Generate Statistics",
    )

    preprocess >> sanity_check >> statistics
