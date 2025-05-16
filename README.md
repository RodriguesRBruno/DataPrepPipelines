# Data Preparation Pipelines

This repository contains code to build an Airflow image capable of executing workflows defined in YAML files. [Section 1](#1-generate-the-airflow-docker-image) details how to build the Airflow Docker image used in this repository, [Section 2](#2-building-your-own-yaml-pipeline) has instructions on building your own YAML pipeline, [Section 3](#3-configure-the-env-file-for-the-your-pipeline) explains how to configure a .env file for execution and [Section 4](#4-running-the-pipeline) has instructions for running the Airflow instance using Docker Compose. Examples of practical pipelines built in this framework are available in [Section 5](#5-examples), linking to resources in the `pipeline_examples` directory of this repo.

## 1. Generate the Airflow Docker Image
The pipelines run on an Airflow Docker image built from this repository. The image contains the Python file to auto-generate DAGs from YAML files.

To generate this Docker image, run the following command inside the same directory as this README file:

```shell
docker build . -t local/pipeline-airflow:1.0.1
```

Note the image name used, `local/pipeline-airflow:1.0.1`. This name will be used in later configuration files. If a different name is used, the same name must be used later.

## 2. Building your own YAML Pipeline.
The YAML format used for the pipelines is described below. Two pipeline examples are available in [Section X]() which may be used as starting points in building your own pipeline.

```yaml
steps:
  - id: unique_id_for_each_step
    type: one of 'container', 'manual_approval' or 'empty'. See 2.1 Types of Steps below for more information.
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
    per_subject: either 'true' or 'false'. Defaults to 'false' if ommited. If 'true', will create separate steps per subject found at ${INPUT_DATA_DIR} for parallel execution.
    limit: int. If per_subject is 'true', limits the number of parallel executions to this number. If per_subject is 'false', this value is not used.

conditions: only include this if at least one condition is defined during the steps definition
  - id: condition_id
    type: function. Currently only function conditions are supported.
    function_name: python_file_name.python_function for a Python function that returns True if this condition is met, otherwise False. The Python file must be in the same directory as this YAML file.

  - id: another_condition_id
    type: function
    function_name: python_file_name.other_python_function

per_subject_def: only include this if at least one step has the "per_subject" option set to true
  type: function. Currently only function definitions are supported.
  function_name: python_file_name.python_function for a Python function that returns a list of files or directories. Each file/directory in the list must correspond to one subject to split by in steps with "per subject" set to true.
```


### 2.1 Types of Steps
The currently implemented types of steps available are:

- `container`: run this step as a Container, providing an `image` and a `command`. This step is recommended for most types of pipeline steps.
  
- `manual_approval`: this creates a step that will by default fail, and must be manually set as successful by the user for the pipeline to proceed. [An example of this step is available at the RANO Pipeline documentation.](./pipeline_examples/rano/README.md#52-final-confirmation)

- `dummy`: creates a step that does literally nothing. May be used to quickly test different workflow architectures.

## 3. Configure the .env file for the your Pipeline
The Airflow Docker image is built to automatically deploy the pipeline based on a given YAML file. However, we must first configure a `.env` file with additional configuration, including the path to the YAML files that define the pipeline workflow. The `.env.example` file located in this directory can be used as a starting point. First, make a copy of this file, changing its name:

```shell
cp .env.example .env
```

Then, modify each value of the new `.env` file according to the instructions below.

- **AIRFLOW_IMAGE_NAME**: use the same image name as was used in [Section 1](#1-generate-the-airflow-docker-image). If the name provided in the tutorial was not changed, you can leave this value as is.
- **AIRFLOW_UID**: Set this to your user's UID in your system. This can be found by running `id -u`. This value is necessary to run Airflow as your current user in Docker, otherwise Airflow will be run as root inside the Docker container. If running as root in the container is acceptable, this value may be removed from the configuration file.
- **YAML_DAGS_DIR**: Set the **absolute** path to the directory containing your YAML file that defines the pipeline.
- **WORKSPACE_DIR**: Set the **absolute** path to the a workspace directory used for temporary files and outputs from the container(s) used in the pipeline.
- **INPUT_DATA_DIR**: Set the **absolute** path to the the directory containing input data for the container(s) used in the pipeline.
- **DATA_DIR**: Set the **absolute** path to the directory where output data should be written. We recommend setting this as the **absolute** path to a `data` directory created inside the `${WORKSPACE_DIR}` directory.
- **_AIRFLOW_USER**: Username for the Admin user in the Airflow UI, used to monitor runs.
- **_AIRFLOW_PASSWORD**: Password for the Admin user in the Airflow UI, used to monitor runs.
- **_AIRFLOW_POSTGRES_USER**: Username for the Postgres user that Airflow creates.
- **_AIRFLOW_POSTGRES_PASSWORD**: Password for the Postgres user that Airflow creates.
- **_AIRFLOW_POSTGRES_DB**: Name for the Database that Airflow creates in Postgres. 
  
  
## 4. Running the Pipeline
Once the `.env` file is properly configured, Airflow can be started via Docker Compose:

```shell
docker compose --env-file .env -p <your_project_name> up
```

This command starts a Docker Compose project named `<your_project_name>` based on the env file `.env`. The Airflow image is configured so that the pipeline will start immediately after the initial Airflow start up. The Airflow Web UI can be accessed at (http://localhost:8080/), using the **_AIRFLOW_USER** and **_AIRFLOW_PASSWORD** values defined in the `.env.chexpert` file as the Username and Password to monitor runs.

During execution, the pipeline outputs a progress report to `${WORKSPACE_DIR}/report_summary.yaml`, with `WORKSPACE_DIR` defined in the `.env.chexpert` file. This progress summary displays the percentage (from 0 to 100.0) of Airflow DAGs completed, for each task in the YAML file. The `report_summary.yaml` file is updated every 30 minutes the pipeline is running.

Once the pipeline finishes executing, output data will be located at the **DATA_DIR**  defined in the `.env` file.

## 5. Examples

This repository contains two Data Preparation Pipeline  examples to be run in Airflow. 

### 5.1 CheXpert
A simple pipeline for processing data from the CheXpert Dataset. [A README.md file detailing this example is available by clicking here.](./pipeline_examples/chexpert/README.md)

### 5.2 RANO
A modified version of the pipeline used in the RANO study is available as an example of a more complex pipeline. [A README.md file detailing this example is available by clicking here.](./pipeline_examples/rano/README.md)

