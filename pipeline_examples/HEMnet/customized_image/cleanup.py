import os
import shutil
from mod_constants import TEMP_DATA_PATH

if os.path.exists(TEMP_DATA_PATH):
    shutil.rmtree(TEMP_DATA_PATH)
