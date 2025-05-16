# CheXpert Pipeline

This pipeline runs a simple data preparation procedure on the CheXpert Dataset. The two pipeline is defined in YAML file located at `dags_from_yaml/chexpert.yaml` 

## 1. Get the CheXpert Data
This pipeline example uses the [CheXpert-v1.0-small dataset, available from Kaggle](https://www.kaggle.com/datasets/ashery/chexpert). Unzip the downloaded contents and place them inside the  `workspace` directory. The final directory structure will be similar to what is shown below:

```
.
chexpert
├── workspace
│   ├── CheXpert-v1.0-small
│   │   ├── valid
│   │  	└── valid.csv
│   └── parameters.yaml
```


## 2. Configure the .env file for the CheXpert Pipeline
The Airflow Docker image is built to automatically deploy the pipeline based on a given YAML file. However, we must first configure a `.env` file with additional configuration, including the path to the YAML files that define the pipeline workflow. The `.env.example` file located in this directory can be used as a starting point. First, make a copy of this file, changing its name:

```shell
cp .env.example .env.chexpert
```

Then, modify each value of the new `.env.chexpert` file according to the instructions below.

- **AIRFLOW_IMAGE_NAME**: use the same image name as was used in [Section 1](#1-generate-the-airflow-docker-image). If the name provided in the tutorial was not changed, you can leave this value as is.
- **AIRFLOW_UID**: Set this to your user's UID in your system. This can be found by running `id -u`. This value is necessary to run Airflow as your current user in Docker, otherwise Airflow will be run as root inside the Docker container. If running as root in the container is acceptable, this value may be removed from the configuration file.
- **YAML_DAGS_DIR**: Set the **absolute** path to the `dags_from_yaml` directory in this subdirectory.
- **WORKSPACE_DIR**: Set the **absolute** path to the `workspace` directory from this repository.
- **INPUT_DATA_DIR**: Set the **absolute** path to the `workspace/CheXPert-v1.0-small` directory created in [Section 1.](#1-get-the-chexpert-data)
- **DATA_DIR**: Set the **absolute** path to the directory where output data should be written. We recommend setting this as the **absolute** path to a `data` directory created inside the `workspace` directory (that is, the **absolute** path to `workspace/data`)
- **_AIRFLOW_USER**: Username for the Admin user in the Airflow UI, used to monitor runs.
- **_AIRFLOW_PASSWORD**: Password for the Admin user in the Airflow UI, used to monitor runs.
- **_AIRFLOW_POSTGRES_USER**: Username for the Postgres user that Airflow creates.
- **_AIRFLOW_POSTGRES_PASSWORD**: Password for the Postgres user that Airflow creates.
- **_AIRFLOW_POSTGRES_DB**: Name for the Database that Airflow creates in Postgres. 
  
## 4. Running the Pipeline
Once the `.env.chexpert` file is properly configured, Airflow can be started via Docker Compose:

```shell
docker compose --env-file .env.chexpert -p chexpert up
```

This command starts a Docker Compose project named `chexpert` based on the env file `.env.chexpert`. The Airflow image is configured so that the pipeline will start immediately after the initial Airflow start up. The Airflow Web UI can be accessed at (http://localhost:8080/), using the **_AIRFLOW_USER** and **_AIRFLOW_PASSWORD** values defined in the `.env.chexpert` file as the Username and Password to monitor runs.

During execution, the pipeline outputs a progress report to `${WORKSPACE_DIR}/report_summary.yaml`, with `WORKSPACE_DIR` defined in the `.env.chexpert` file. This progress summary displays the percentage (from 0 to 100.0) of Airflow tasks completed, for each task in the YAML file. The `report_summary.yaml` file is updated every 30 minutes the pipeline is running. For this simple pipeline, the summary should be simply 0.0 for incomplete tasks or 100 for complete tasks. The RANO Pipeline example (see the README.md located at `pipeline_examples/rano/README.md`) can display more complex situations in its report summary.

Once the pipeline finishes executing, output data will be located at the **DATA_DIR**  defined in the `.env.chexpert` file.