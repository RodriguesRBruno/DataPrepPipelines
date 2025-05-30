import argparse
from mod_utils import (
    load_numpy_array,
    load_slides_by_prefix,
    save_train_tiles,
    load_df,
    dump_df,
)
from mod_constants import (
    OUTPUT_PATH,
    U_MASK_FILTERED,
    NON_C_MASK_FILTERED,
    C_MASK_FILTERED,
    T_MASK_FILTERED,
)
from slide import tile_gen_at_mag

from HEMnet_train_dataset import restricted_float
import time
import numpy as np
import os


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--subject-subdir",
        type=str,
        required=True,
        help="Prefix that defines the slides used in this step.",
    )
    parser.add_argument(
        "-m",
        "--tile_mag",
        type=float,
        default=10,
        help="Magnification for generating tiles",
    )
    parser.add_argument(
        "-ts", "--tile_size", type=int, default=224, help="Output tile size in pixels"
    )
    parser.add_argument(
        "-v", "--verbosity", action="store_true", help="Increase output verbosity"
    )
    parser.add_argument("-p", "--performance-csv", type=str)
    parser.add_argument("-um", "--u-mask", type=str)
    parser.add_argument("-cm", "--c-mask", type=str)
    parser.add_argument("-ncm", "--non-c-mask", type=str)
    parser.add_argument("-tm", "--t-mask", type=str)
    parser.add_argument(
        "-n",
        "--normaliser-path",
        help="Path to normaliser.pkl from image registration step.",
    )
    args = parser.parse_args()

    # PATHS
    PREFIX = args.subject_subdir
    U_MASK_PATH = args.u_mask
    C_MASK_PATH = args.c_mask
    NON_C_MASK_PATH = args.non_c_mask
    T_MASK_PATH = args.t_mask
    PERFORMANCE_CSV_PATH = args.performance_csv
    NORMALISER_PATH = args.normaliser_path

    # User selectable parameters
    VERBOSE = args.verbosity
    TILE_MAG = args.tile_mag
    OUTPUT_TILE_SIZE = args.tile_size

    print("Saving tiles from Slide: {0}".format(PREFIX))

    start = time.perf_counter()
    he_slide, _ = load_slides_by_prefix(PREFIX)
    u_mask_filtered = load_numpy_array(U_MASK_FILTERED, PREFIX, fullpath=U_MASK_PATH)
    c_mask_filtered = load_numpy_array(C_MASK_FILTERED, PREFIX, fullpath=C_MASK_PATH)
    non_c_mask_filtered = load_numpy_array(
        NON_C_MASK_FILTERED, PREFIX, fullpath=NON_C_MASK_PATH
    )
    t_mask_filtered = load_numpy_array(T_MASK_FILTERED, PREFIX, fullpath=T_MASK_PATH)
    performance_df = load_df(subdir=PREFIX, fullpath=PERFORMANCE_CSV_PATH)
    end = time.perf_counter()
    print(f"Time spent on reloading normaliser and slides: {end-start}s")

    ##############
    # Save Tiles #
    ##############

    # Make Directory to save tiles
    TILES_PATH = OUTPUT_PATH.joinpath("tiles_" + str(TILE_MAG) + "x")
    os.makedirs(TILES_PATH, exist_ok=True)

    # Save tiles
    tgen = tile_gen_at_mag(he_slide, TILE_MAG, OUTPUT_TILE_SIZE)
    save_train_tiles(
        TILES_PATH,
        tgen,
        c_mask_filtered,
        t_mask_filtered,
        u_mask_filtered,
        prefix=PREFIX,
        normaliser_path=NORMALISER_PATH,
    )

    non_cancer_tiles = np.invert(non_c_mask_filtered).sum()

    uncertain_tiles = np.invert(u_mask_filtered).sum()

    cancer_tiles = np.invert(c_mask_filtered).sum()

    performance_df["Cancer_Tiles"] = cancer_tiles
    performance_df["Uncertain_Tiles"] = uncertain_tiles
    performance_df["Non_Cancer_Tiles"] = non_cancer_tiles

    dump_df(performance_df, subdir=PREFIX)
