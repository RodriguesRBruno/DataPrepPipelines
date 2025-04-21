from utils.utils import read_subject_directories, create_legal_id

SUBJECT_TIMEPOINT_LIST = read_subject_directories()


def _make_dag_id_dict(id_str, prefix="0"):
    return {
        subject: f"{prefix}_rano_{id_str}_{create_legal_id(subject)}"
        for subject in SUBJECT_TIMEPOINT_LIST
    }


SETUP = "1_rano_setup"
NIFTI_CONVERSION = _make_dag_id_dict("nifti", "2")
TUMOR_EXTRACTION = _make_dag_id_dict("tumor", "3")
MANUAL_APPROVAL = _make_dag_id_dict("manual", "4")
FINISH = "5_rano_end"
