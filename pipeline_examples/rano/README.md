# RANO Data Preparation

Here, a modified version of the data preparation pipeline used in the RANO study is presented as a second use case for the YAML Pipelins in Airflow. The YAML file defining this pipeline is located at `dags_from_yaml/rano.yaml`, with auxiliary Python code used to evaluate conditional steps in the pipeline located at `dags_from_yaml/conditions.py`.

The Docker image used in this example is currently not available on a registry and must be therefore built from this repo. [Section 1](#1-building-the-rano-pipeline-docker-image) will go over the process of building this image and obtaining sample data to test the pipeline. [Section 2](#2-configuring-the-env-file-for-the-rano-pipeline) will then go over configuring the `.env.rano` file to run this pipeline and [Section 3](#3-running-the-rano-pipeline) will go over running the pipeline, including monitoring it in Airflow.

## 1. Building the RANO Pipeline Docker image
In the same directory as this file (that is, the `rano` directory), run the following command to generate the RANO Pipelin Docker image:

```shell
cd pipeline
sh build.sh
```
This may take some time to finish. At the end of the execution, two new Docker images will be created: `local/rano-data-prep-mlcube:1.0.14,` which is the RANO Data Preparation Pipeline image, and `local/fets_tool`, an intermediary image used in generating the Pipeline image. Note that the version tag in the local/rano-data-prep-mlcube may be different if the image is updated on a later date.

Real data for running the pipeline is not available in this repository. However, some downsampled data used for testing is available, as described in [Section 1.1](#12-using-the-development-image-for-testing) below.

### 1.1 Structuring your data

#### 1.1.1 Workspace Directory
If using real data, prepare a `workspace` directory in your Machine. It can be plcaed under any directory you prefer. Keep note of the complete path to this directory for the configuration step in [Section 2](#2-configuring-the-env-file-for-the-rano-pipeline).

#### 1.1.2 Output Data
Prepare a directory in your Machine to be the directory of the output data. This directory can be placed anywhere and have any name, but we recommend using a directory named `data` inside the `workspace` directory defined above. In this case, the directory structure would be:

```
.
├── workspace
│   ├── data
```
Keep note of the complete path to this directory for the configuration step in [Section 2](#2-configuring-the-env-file-for-the-rano-pipeline).

#### 1.1.3 Input Data
ou may create your `input_data` directory anywhere, but please ensure that it is in a location with relatively fast read/write access and with at least 2x more free disk space than your dataset currently occupies. Inside the `input_data` directory, your data needs to follow a folder hierarchy where images are separated by \<PatientID>/\<Timepoint>/\<Series>. We recommend making the `input_data` a subdirectory of the `workspace` directory defined previously, on the same level as the `data` directory. Keep note of the complete path to this directory for the configuration step in [Section 2](#2-configuring-the-env-file-for-the-rano-pipeline).

**Please note**: For the RANO study, Series-level folders must use the following abbreviations: t2f (T2-weighted FLAIR), t1n (T1-weighted non-contrast), t1c (T1-weighted with contrast), and t2w (T2-weighted). For more information about the required series, please refer to the FeTS 2.0 manual. PatientID and Timepoint must be unique between and within patients, respectively, and Timepoint should be sortable into chronologic order.

```
├── workspace
   ├── data
   ├── input_data
   ├── AAAC_0
   │   ├── 2008.03.30
   │   │   ├── t2f
   │   │   │   ├── t2_Flair_axial-2_echo1_S0002_I000001.dcm
   │   │   │   └── ...
   │   │   ├── t1n
   │   │   │   ├── t1_axial-3_echo1_S0003_I000001.dcm
   │   │   │   └── ...
   │   │   ├── t1c
   │   │   │   ├── t1_axial_stealth-post-14_echo1_S0014_I000001.dcm
   │   │   │   └── ...
   │   │   └── t2w
   │   │   │   ├── T2_SAG_SPACE-4_echo1_S0004_I000001.dcm
   │   │   │   └── ...

```


### 1.2 Using the Development image for testing
Downsampled images may be used for testing purposes, along with a modified Docker image to run them. Follow these instructions to run the development version of the pipeline with test data:

- Download and extract (sha256: 701fbba8b253fc5b2f54660837c493a38dec986df9bdbf3d97f07c8bc276a965):
<https://storage.googleapis.com/medperf-storage/rano_test_assets/dev.tar.gz>

- Create a `workspace` directory at the same level as this file and move the `additional_files` and `input_data` directories to `workspace/additional_files`and `workspace/input_data` respectively. Also create a `data` directory inside `workspace`.

- Move the `tmpmodel`directory and `atlasImage_0.125.nii.gz` file into the `pipeline/project` directory.

- Build the Dev Docker Image from the Dockerfile.dev file located at `pipeline/project/Dockerfile.dev`. Starting from the same directory as this README.md file (`rano`), this step can be done with the following commands:

```shell
cd pipeline/project
docker build . -t local/rano-data-prep-mlcube-dev:1.0.14 -f Dockerfile.dev
```

- Edit the pipeline YAML file, located at `dags_from_yaml/rano.yaml` so that the `image` field (line 3) uses the dev image name (`local/rano-data-prep-mlcube-dev:1.0.14`)
  

## 2. Configuring the .env file for the RANO Pipeline
A `.env.rano` configuration file must be created to run this pipeline, similarly to what is described in the README.md file at the root of this repository for the CheXpert Pipeline.

Go back to the root of the repository and make a copy of the `.env.example` file, changing its name:

```shell
cp .env.example .env.rano
```

Then, modify each value of the new `.env.rano` file according to the instructions below.

- **AIRFLOW_IMAGE_NAME**: use the same image name as was used in [Section 1](#1-generate-the-airflow-docker-image). If the name provided in the tutorial was not changed, you can leave this value as is.
- **AIRFLOW_UID**: Set this to your user's UID in your system. This can be found by running `id -u`. This value is necessary to run Airflow as your current user in Docker, otherwise Airflow will be run as root inside the Docker container. If running as root in the container is acceptable, this value may be removed from the configuration file.
- **YAML_DAGS_DIR**: Set the **absolute** path to the `pipeline_examples/rano/dags_from_yaml` directory in this repository.
- **WORKSPACE_DIR**: If running on real data, this is **absolute** path to the `workspace` directory defined in [Section 1.1.1](#111-workspace-directory). If running the development dataset, set the **absolute** path to the `pipeline_examples/rano/workspace` directory.
- **INPUT_DATA_DIR**: If running on real data, this is **absolute** path to the `input_data` directory defined in [Section 1.1.3](#113-input-data). If running the development dataset, set the **absolute** path to the `pipeline_examples/rano/workspace/input_data` directory.
- **DATA_DIR**: If running on real data, this is **absolute** path to the `data` directory defined in [Section 1.1.2](#112-output-data). If running the development dataset, set the **absolute** path to the `pipeline_examples/rano/workspace/data` directory.
- **_AIRFLOW_USER**: Username for the Admin user in the Airflow UI, used to monitor runs.
- **_AIRFLOW_PASSWORD**: Password for the Admin user in the Airflow UI, used to monitor runs.
- **_AIRFLOW_POSTGRES_USER**: Username for the Postgres user that Airflow creates.
- **_AIRFLOW_POSTGRES_PASSWORD**: Password for the Postgres user that Airflow creates.
- **_AIRFLOW_POSTGRES_DB**: Name for the Database that Airflow creates in Postgres. 
  

## 3. Running the RANO Pipeline
Once the `.env.rano` file is properly configured, Airflow can be started via Docker Compose, at the root directory of this repo:

```shell
docker compose --env-file .env.rano -p rano up
```

This command starts a Docker Compose project named `rano` based on the env file `.env.crano`. The Airflow image is configured so that the pipeline will start immediately after the initial Airflow start up. The Airflow Web UI can be accessed at (http://localhost:8080/), using the **_AIRFLOW_USER** and **_AIRFLOW_PASSWORD** values defined in the `.env.rano` file as the Username and Password to monitor runs.

### 3.1 Monitoring in Airflow