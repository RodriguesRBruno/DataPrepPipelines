# Data Preparation Pipelines

This repository contains two Data Preparation Pipeline  examples to be run in Airflow. One pipeline runs a simple data preparation procedure on the CheXpert Dataset and the other one is modified version of the pipeline used in the RANO study. The two pipelines are defined in YAML files located at `pipeline_examples/chexpert/dags_from_yaml/chexpert.yaml` and `pipeline_examples/rano/dags_from_yaml/rano.yaml`. This README file explains how to run the CheXpert pipeline, as it is the simpler of the two. A separate README file located at `pipeline_examples/rano/README.md` goes over how to run the RANO Data Preparation Pipeline.

## 1. Generate the Airflow Docker Image
The pipelines run on an Airflow Docker image built from this repository. The image contains the Python file to auto-generate DAGs from YAML files.

To generate this Docker image, run the following command inside the same directory as this README file:

```shell
docker build . -t local/pipeline-airflow:0.0.1
```

Note the image name used, `local/pipeline-airflow:0.0.1`. This name will be used in later configuration files. If a different name is used, the same name must be used later.

## 2. Get the CheXpert Data
This pipeline example uses the [CheXpert-v1.0-small dataset, available from Kaggle](https://www.kaggle.com/datasets/ashery/chexpert). Unzip the downloaded contents and place them inside the  `pipeline_examples/chexpert/workspace` directory. The final directory structure will be similar to what is shown below:

```
.
chexpert
├── workspace
│   ├── CheXpert-v1.0-small
│   │   ├── valid
│   │  	└── valid.csv
│   └── parameters.yaml
```


## 3. Configure the .env file for the CheXpert Pipeline
The Airflow Docker image is built to automatically deploy the pipeline based on a given YAML file. However, we must first configure a `.env` file with additional configuration, including the path to the YAML files that define the pipeline workflow. The `.env.example` file located in this directory can be used as a starting point. First, make a copy of this file, changing its name:

```shell
cp .env.example .env.chexpert
```

Then, modify each value of the new `.env.chexpert` file according to the instructions below.

- **AIRFLOW_IMAGE_NAME**: use the same image name as was used in [Section 1](#1-generate-the-airflow-docker-image). If the name provided in the tutorial was not changed, you can leave this value as is.
- **AIRFLOW_UID**: Set this to your user's UID in your system. This can be found by running `id -u`. This value is necessary to run Airflow as your current user in Docker, otherwise Airflow will be run as root inside the Docker container. If running as root in the container is acceptable, this value may be removed from the configuration file.
- **YAML_DAGS_DIR**: Set the **absolute** path to the `pipeline_examples/chexpert/dags_from_yaml` directory in this repository.
- **WORKSPACE_DIR**: Set the **absolute** path to the `pipeline_examples/chexpert/workspace` directory from this repository.
- **INPUT_DATA_DIR**: Set the **absolute** path to the `pipeline_examples/chexpert/workspace/CheXPert-v1.0-small` directory created in [Section 2.](#2-get-the-chexpert-data)
- **DATA_DIR**: Set the **absolute** path to the directory where output data should be written. We recommend setting this as the **absolute** path to a `data` directory created inside the `workspace` directory (that is, the **absolute** path to `pipeline_examples/chexpert/workspace/data`)
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


## 5. Building your own YAML Pipeline.
The `pipeline_examples/chexpert/dags_from_yaml/chexpert.yaml` may be used as a starting point to create your own YAML Pipelines. The expected format is described below. The main idea of this format is to simplify the creation of pipelines where each step is run on a Container, with simpler syntax when compared to directly defining Airflow DAGs.

```yaml
steps:
  - id: unique_id_for_each_step
    image: name of the Container image to be used. Can be an image available at a registry or a local image.
    command: command to run on the Container for this step in the pipeline.
    mounts:
      - /path/to/input_data/directory/in/host/system:/path/to/input_data/in/container
      - /some/other/mount/in/host:/some/other/mount/in/container
      - The environment variables ${WORKSPACE_DIR}, ${DATA_DIR} and ${INPUT_DATA_DIR} may be used here as shorthands to the paths defined in the .env file.
    next: id of the step to execute after this, or null if this is the last step of the pipeline. Optionally, can support conditional next steps as described below.
       - if: only include this if optional next steps are desired!
         - condition: condition_id
           target: id of the step to execute when the above condition is True.
         - condition: another_condition_id
           target: another_step_id
      - else: step to be executed if no conditions are met. If not included (or if included with the same ID as this step), the pipeline will instead keep executing the conditions defined above until one of them is True.
      - wait: Time in seconds to wait between re-evaluations of next step conditions, when a default step is not defined.

conditions: only include this if at least one condition is defined during the steps definition
  - id: condition_id
    type: function. Currently only function conditions are supported.
    function_name: python_file_name.python_function for a Python function that returns True if this condition is met, otherwise False. The Python file must be in the same directory as this YAML file.

  - id: another_condition_id
    type: function
    function_name: python_file_name.other_python_function
```

Examples of conditional steps can be found in the YAML file for the RANO Pipeline located at `pipeline_examples/rano/dags_from_yaml/rano.yaml`.