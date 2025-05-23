from typing import Union
from tqdm import tqdm
import os
import shutil
import time
import SimpleITK as sitk
import subprocess

from .extract import Extract
from .PrepareDataset import (
    Preparator,
    FINAL_FOLDER,
    generate_tumor_segmentation_fused_images,
    save_screenshot,
)
from .utils import (
    get_id_tp,
    get_manual_approval_under_review_path,
    get_manual_approval_finalized_path,
)
from .mlcube_constants import (
    GROUND_TRUTH_PATH,
    TUMOR_EXTRACTION_REVIEW_PATH,
    BRAIN_MASK_REVIEW_PATH,
    BRAIN_MASK_FILE,
)
from .constants import INTERIM_FOLDER, FINAL_FOLDER

MODALITY_MAPPING = {
    "t1c": "t1c",
    "t1ce": "t1c",
    "t1": "t1n",
    "t1n": "t1n",
    "t2": "t2w",
    "t2w": "t2w",
    "t2f": "t2f",
    "flair": "t2f",
}

MODALITY_VARIANTS = {
    "t1c": "T1GD",
    "t1ce": "T1GD",
    "t1": "T1",
    "t1n": "T1",
    "t2": "T2",
    "t2w": "T2",
    "t2f": "FLAIR",
    "flair": "FLAIR",
}


class ExtractNnUNet(Extract):
    def __init__(
        self,
        data_csv: str,
        out_path: str,
        subpath: str,
        prev_stage_path: str,
        prev_subpath: str,
        status_code: int,
        extra_labels_path=None,
        nnunet_executable: str = "/nnunet_env/bin/nnUNet_predict",
    ):
        self.data_csv = data_csv
        self.out_path = out_path
        self.subpath = subpath
        self.data_subpath = FINAL_FOLDER
        self.prev_path = prev_stage_path
        self.prev_subpath = prev_subpath
        os.makedirs(self.out_path, exist_ok=True)
        self.prep = Preparator(data_csv, out_path, "BraTSPipeline")
        self.pbar = tqdm()
        self.failed = False
        self.exception = None
        self.__status_code = status_code
        self.extra_labels_path = extra_labels_path or []
        self.nnunet_executable = nnunet_executable

    @property
    def name(self) -> str:
        return "nnUNet Tumor Extraction"

    @property
    def status_code(self) -> str:
        return self.__status_code

    def __get_models(self):
        models_path = os.path.join(os.environ["RESULTS_FOLDER"], "nnUNet", "3d_fullres")
        return os.listdir(models_path)

    def __get_mod_order(self, model):
        order_path = os.path.join(
            os.environ["RESULTS_FOLDER"],
            os.pardir,
            "nnUNet_modality_order",
            model,
            "order",
        )
        with open(order_path, "r") as f:
            order_str = f.readline()
        # remove 'order = ' from the splitted list
        modalities = order_str.split()[2:]
        modalities = [MODALITY_MAPPING[mod] for mod in modalities]
        return modalities

    def __prepare_case(self, path, id, tp, order):
        tmp_subject = f"{id}-{tp}"
        tmp_path = os.path.join(path, "tmp-data")
        tmp_subject_path = os.path.join(tmp_path, tmp_subject)
        tmp_out_path = os.path.join(path, "tmp-out", tmp_subject)
        shutil.rmtree(tmp_subject_path, ignore_errors=True)
        shutil.rmtree(tmp_out_path, ignore_errors=True)
        os.makedirs(tmp_subject_path)
        os.makedirs(tmp_out_path)
        in_modalities_path = os.path.join(path, FINAL_FOLDER, id, tp)
        input_modalities = {}
        for modality_file in os.listdir(in_modalities_path):
            if not modality_file.endswith(".nii.gz"):
                continue
            modality = modality_file[:-7].split("_")[-1]
            norm_mod = MODALITY_MAPPING[modality]
            mod_idx = order.index(norm_mod)
            mod_idx = str(mod_idx).zfill(4)

            out_modality_file = f"{tmp_subject}_{mod_idx}.nii.gz"
            in_file = os.path.join(in_modalities_path, modality_file)
            out_file = os.path.join(tmp_subject_path, out_modality_file)
            input_modalities[MODALITY_VARIANTS[modality]] = in_file
            shutil.copyfile(in_file, out_file)

        return tmp_subject_path, tmp_out_path, input_modalities

    def __run_model(self, model, data_path, out_path):
        # models are named Task<ID>_..., where <ID> is always 3 numbers
        task_id = model[4:7]
        cmd = f"{self.nnunet_executable} -i {data_path} -o {out_path} -t {task_id}"
        print(cmd)
        print(os.listdir(data_path))
        start = time.time()
        subprocess.call(cmd, shell=True)
        end = time.time()
        total_time = end - start
        print(f"Total time elapsed is {total_time} seconds")

    def __finalize_pred(self, tmp_out_path, out_pred_filepath, *copy_paths):
        # We assume there's only one file in out_path
        pred = None
        for file in os.listdir(tmp_out_path):
            if file.endswith(".nii.gz"):
                pred = file

        if pred is None:
            raise RuntimeError("No tumor segmentation was found")

        pred_filepath = os.path.join(tmp_out_path, pred)
        pred_dir = os.path.dirname(pred_filepath)
        os.makedirs(pred_dir, exist_ok=True)
        shutil.move(pred_filepath, out_pred_filepath)
        for copy_path in copy_paths:
            copy_dir = os.path.dirname(copy_path)
            os.makedirs(copy_dir, exist_ok=True)
            shutil.copy(out_pred_filepath, copy_path)
        return out_pred_filepath

    def _process_case(self, index: Union[str, int]):
        id, tp = get_id_tp(index)
        subject_id = f"{id}_{tp}"

        models = self.__get_models()
        outputs = []
        images_for_fusion = []
        out_path = os.path.join(self.out_path, INTERIM_FOLDER, id, tp)
        out_pred_path = os.path.join(out_path, GROUND_TRUTH_PATH)
        finalized_tumor_path = get_manual_approval_finalized_path(
            id, tp, TUMOR_EXTRACTION_REVIEW_PATH
        )
        under_review_tumor_path = get_manual_approval_under_review_path(
            id, tp, TUMOR_EXTRACTION_REVIEW_PATH
        )
        finalized_brain_mask_path = get_manual_approval_finalized_path(
            id, tp, BRAIN_MASK_REVIEW_PATH
        )
        under_review_brain_mask_path = get_manual_approval_under_review_path(
            id, tp, BRAIN_MASK_REVIEW_PATH
        )
        brain_mask_filepath = os.path.join(out_path, BRAIN_MASK_FILE)
        brain_mask_review_filepath = os.path.join(
            under_review_brain_mask_path, BRAIN_MASK_FILE
        )

        os.makedirs(out_pred_path, exist_ok=True)

        for i, model in enumerate(models):
            order = self.__get_mod_order(model)
            tmp_data_path, tmp_out_path, input_modalities = self.__prepare_case(
                self.out_path, id, tp, order
            )
            filename = f"{id}_{tp}_tumorMask_model_{i}.nii.gz"
            out_pred_filepath = os.path.join(out_pred_path, filename)
            under_review_filepath = os.path.join(under_review_tumor_path, filename)
            self.__run_model(model, tmp_data_path, tmp_out_path)
            output = self.__finalize_pred(
                tmp_out_path, out_pred_filepath, under_review_filepath
            )
            outputs.append(output)
            images_for_fusion.append(sitk.ReadImage(output, sitk.sitkUInt8))

            # cleanup
            shutil.rmtree(tmp_data_path, ignore_errors=True)
            shutil.rmtree(tmp_out_path, ignore_errors=True)

        fused_outputs = generate_tumor_segmentation_fused_images(
            images_for_fusion, out_pred_path, subject_id
        )
        outputs += fused_outputs

        for output in outputs:
            # save the screenshot
            tumor_mask_id = os.path.basename(output).replace(".nii.gz", "")
            save_screenshot(
                input_modalities,
                os.path.join(
                    out_path,
                    f"{tumor_mask_id}_summary.png",
                ),
                output,
            )

        os.makedirs(under_review_brain_mask_path, exist_ok=True)
        shutil.copy(brain_mask_filepath, brain_mask_review_filepath)
        os.makedirs(finalized_brain_mask_path, exist_ok=True)
        os.makedirs(finalized_tumor_path, exist_ok=True)
