from pathlib import Path
import SimpleITK as sitk

BASE_DIR = Path("/workspace")
INPUT_PATH = BASE_DIR.joinpath("input_data")
OUTPUT_PATH = BASE_DIR.joinpath("data")
TEMP_DATA_PATH = BASE_DIR.joinpath(".tmp")
NORMALISER_PKL = "normaliser.pkl"
AFFINE_TRANSFORM_PKL = "affine_transform.hdf"
MOVING_RESAMPLED_AFFINE = "moving_resampled_affine.npy"
INTERPOLATOR = sitk.sitkLanczosWindowedSinc
