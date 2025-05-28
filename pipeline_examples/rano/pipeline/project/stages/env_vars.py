import os

from .mlcube_constants import RAW_PATH

WORKSPACE_DIR = os.getenv("WORKSPACE_DIRECTORY")
DATA_DIR = os.getenv("DATA_DIR") or os.path.join(WORKSPACE_DIR, "data")
DATA_SUBDIR = os.path.join(*DATA_DIR.split(os.sep)[-2:])
INPUT_DIR = os.getenv("INPUT_DIR") or os.path.join(WORKSPACE_DIR, "input_data")
RAW_DATA_DIR = os.getenv("RAW_DIR") or os.path.join(DATA_DIR, RAW_PATH)
