import os
import shutil
from pandas import DataFrame
from tqdm import tqdm
from functools import reduce
from pathlib import Path
import hashlib
import yaml
import pandas as pd

from .env_vars import DATA_DIR, DATA_SUBDIR
from .mlcube_constants import (
    OUT_CSV,
    AUX_FILES_PATH,
    FINALIZED_PATH,
    MANUAL_REVIEW_PATH,
    UNDER_REVIEW_PATH,
    TUMOR_EXTRACTION_REVIEW_PATH,
    REPORT_FILE,
    CHANGED_VOXELS_FILE,
)


def convert_path_to_index(path: str):
    as_list = path.split(os.sep)
    as_index = "|".join(as_list)
    return as_index


# Taken from https://code.activestate.com/recipes/577879-create-a-nested-dictionary-from-oswalk/
def get_directory_structure(rootdir):
    """
    Creates a nested dictionary that represents the folder structure of rootdir
    """
    dir = {}
    rootdir = rootdir.rstrip(os.sep)
    start = rootdir.rfind(os.sep) + 1
    for path, dirs, files in os.walk(rootdir):
        folders = path[start:].split(os.sep)
        subdir = dict.fromkeys(files)
        parent = reduce(dict.get, folders[:-1], dir)
        parent[folders[-1]] = subdir
    return dir


def get_subdirectories(base_directory: str):
    return [
        subdir
        for subdir in os.listdir(base_directory)
        if os.path.isdir(os.path.join(base_directory, subdir))
    ]


def has_prepared_folder_structure(data_path, labels_path) -> bool:
    data_struct = list(get_directory_structure(data_path).values())[0]
    labels_struct = list(get_directory_structure(labels_path).values())[0]

    expected_data_files = [
        "brain_t1c.nii.gz",
        "brain_t1n.nii.gz",
        "brain_t2f.nii.gz",
        "brain_t2w.nii.gz",
    ]
    expected_labels_files = ["final_seg.nii.gz"]

    if "splits.csv" not in data_struct:
        return False

    for id in data_struct.keys():
        if data_struct[id] is None:
            # This is a file, ignore
            continue
        for tp in data_struct[id].keys():
            expected_subject_data_files = set(
                ["_".join([id, tp, file]) for file in expected_data_files]
            )
            expected_subject_labels_files = set(
                ["_".join([id, tp, file]) for file in expected_labels_files]
            )

            found_data_files = set(data_struct[id][tp].keys())
            found_labels_files = set(labels_struct[id][tp].keys())

            data_files_diff = len(expected_subject_data_files - found_data_files)
            labels_files_diff = len(expected_subject_labels_files - found_labels_files)
            if data_files_diff or labels_files_diff:
                return False

    # Passed all checks
    return True


def normalize_path(path: str) -> str:
    """Remove mlcube-specific components from the given path

    Args:
        path (str): mlcube path

    Returns:
        str: normalized path
    """
    # for this specific problem, we know that all paths start with `/mlcube_io*`
    # and that this pattern won't change, shrink or grow. We can therefore write a
    # simple, specific solution
    if path.startswith("/mlcube_io"):
        return path[12:]

    # In case the path has already been normalized
    return path


def unnormalize_path(path: str, parent: str) -> str:
    """Add back mlcube-specific components to the given path

    Args:
        path (str): normalized path

    Returns:
        str: mlcube-specific path
    """
    if path.startswith(os.path.sep):
        path = path[1:]
    return os.path.join(parent, path)


def load_report(subject_id: str, timepoint: str) -> pd.DataFrame:
    report_path = get_report_yaml_filepath(subject_id, timepoint)

    try:
        with open(report_path, "r") as f:
            report_data = yaml.safe_load(f)
    except FileNotFoundError:
        report_data = None

    report_df = pd.DataFrame(report_data)
    return report_df


def normalize_report_paths(report: DataFrame) -> DataFrame:
    """Ensures paths are normalized and converts them to relative paths for the local machine

    Args:
        report (DataFrame): report to normalize

    Returns:
        DataFrame: report with transformed paths
    """
    pattern = DATA_SUBDIR
    report["data_path"] = report["data_path"].str.split(pattern).str[-1]
    report["labels_path"] = report["labels_path"].str.split(pattern).str[-1]
    return report


def write_report(report: DataFrame, subject_id: str, timepoint: str):
    filepath = get_report_yaml_filepath(subject_id, timepoint)
    report_dict = report.to_dict()

    # Use a temporary file to avoid quick writes collisions and corruption
    temp_path = Path(filepath).parent / ".report.yaml"
    with open(temp_path, "w") as f:
        yaml.dump(report_dict, f)
    os.rename(temp_path, filepath)


def get_id_tp(index: str):
    return index.split("|")


def set_files_read_only(path):
    for root, dirs, files in os.walk(path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            os.chmod(file_path, 0o444)  # Set read-only permission for files

        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            set_files_read_only(
                dir_path
            )  # Recursively call the function for subdirectories


def cleanup_storage(remove_folders):
    for folder in remove_folders:
        print(f"Deleting directory {folder}...")
        shutil.rmtree(folder, ignore_errors=True)


def copy_files(src_dir, dest_dir):
    # Ensure the destination directory exists
    os.makedirs(dest_dir, exist_ok=True)

    # Iterate through the files in the source directory
    for filename in os.listdir(src_dir):
        src_file = os.path.join(src_dir, filename)
        dest_file = os.path.join(dest_dir, filename)

        # Check if the item is a file (not a directory)
        if os.path.isfile(src_file):
            shutil.copy2(src_file, dest_file)  # Copy the file


# Taken from https://stackoverflow.com/questions/24937495/how-can-i-calculate-a-hash-for-a-filesystem-directory-using-python
def md5_update_from_dir(directory, hash):
    assert Path(directory).is_dir()
    for path in sorted(Path(directory).iterdir(), key=lambda p: str(p).lower()):
        hash.update(path.name.encode())
        if path.is_file():
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash.update(chunk)
        elif path.is_dir():
            hash = md5_update_from_dir(path, hash)
    return hash


def md5_dir(directory):
    return md5_update_from_dir(directory, hashlib.md5()).hexdigest()


def md5_file(filepath):
    return hashlib.md5(open(filepath, "rb").read()).hexdigest()


class MockTqdm(tqdm):
    def __getattr__(self, attr):
        return lambda *args, **kwargs: None


def get_aux_files_dir(subject_subdir):
    return os.path.join(DATA_DIR, AUX_FILES_PATH, subject_subdir)


def get_report_yaml_filepath(subject_id, timepoint):
    yaml_dir = os.path.join(
        DATA_DIR, AUX_FILES_PATH, os.path.join(subject_id, timepoint)
    )
    return os.path.join(yaml_dir, REPORT_FILE)


def get_data_csv_filepath(subject_subdir):
    csv_dir = get_aux_files_dir(subject_subdir)
    return os.path.join(csv_dir, OUT_CSV)


def find_finalized_subjects():
    base_finalized_dir = os.path.join(
        DATA_DIR, MANUAL_REVIEW_PATH, TUMOR_EXTRACTION_REVIEW_PATH
    )
    subject_and_timepoint_list = []

    candidate_subjects = get_subdirectories(base_finalized_dir)
    for candidate_subject in candidate_subjects:
        subject_path = os.path.join(base_finalized_dir, candidate_subject)
        timepoint_dirs = get_subdirectories(subject_path)

        for timepoint in timepoint_dirs:
            timepoint_complete_path = os.path.join(subject_path, timepoint)
            finalized_path = os.path.join(timepoint_complete_path, FINALIZED_PATH)
            try:
                path_exists = os.path.exists(finalized_path)
                path_is_dir = os.path.isdir(finalized_path)
                only_one_case = len(os.listdir(finalized_path)) == 1
                if path_exists and path_is_dir and only_one_case:
                    subject_timepoint_dict = {
                        "SubjectID": candidate_subject,
                        "Timepoint": timepoint,
                    }
                    subject_and_timepoint_list.append(subject_timepoint_dict)
            except OSError:
                pass
    return subject_and_timepoint_list


def get_manual_approval_base_path(subject_id, timepoint, approval_type):
    manual_approval_base_path = os.path.join(
        DATA_DIR,
        MANUAL_REVIEW_PATH,
        approval_type,
        subject_id,
        timepoint,
    )
    return manual_approval_base_path


def get_manual_approval_finalized_path(subject_id, timepoint, approval_type):
    base_path = get_manual_approval_base_path(subject_id, timepoint, approval_type)
    full_path = os.path.join(base_path, FINALIZED_PATH)
    return full_path


def get_manual_approval_under_review_path(subject_id, timepoint, approval_type):
    base_path = get_manual_approval_base_path(subject_id, timepoint, approval_type)
    full_path = os.path.join(base_path, UNDER_REVIEW_PATH)
    return full_path


def safe_remove(path_to_remove: str):
    try:
        os.remove(path_to_remove)
    except FileNotFoundError:
        pass


def delete_empty_directory(path_to_directory: str):
    if os.path.isdir(path_to_directory):
        inside_this_dir = os.listdir(path_to_directory)
        for subdir in inside_this_dir:
            complete_path = os.path.join(path_to_directory, subdir)
            delete_empty_directory(complete_path)

        # List again, could be empty now
        inside_this_dir = os.listdir(path_to_directory)
        if not inside_this_dir:
            shutil.rmtree(path_to_directory)


def get_changed_voxels_file(subject_id, timepoint):
    return os.path.join(
        DATA_DIR, AUX_FILES_PATH, subject_id, timepoint, CHANGED_VOXELS_FILE
    )
