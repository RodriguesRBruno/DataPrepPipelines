import argparse
from mod_utils import (
    load_slides_by_prefix,
    save_img,
    load_data,
    dump_data,
)
import SimpleITK as sitk
from mod_constants import OUTPUT_PATH, INTERPOLATOR, NORMALISER_PKL
from utils import (
    get_itk_from_pil,
    sitk_transform_rgb,
    PlotImageAlignment,
    show_alignment,
    calculate_mutual_info,
    get_pil_from_itk,
)
import numpy as np


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
        "-a",
        "--align_mag",
        type=float,
        default=2,
        help="Magnification for aligning H&E and TP53 slide",
    )
    parser.add_argument(
        "-v", "--verbosity", action="store_true", help="Increase output verbosity"
    )

    args = parser.parse_args()
    # PATHS
    PREFIX = args.subject_subdir

    # User selectable parameters
    ALIGNMENT_MAG = args.align_mag
    VERBOSE = args.verbosity

    print("Processing Slide: {0}".format(PREFIX))

    he, tp53 = load_slides_by_prefix(PREFIX, ALIGNMENT_MAG)
    normaliser = load_data(data_name=NORMALISER_PKL)

    # Normalise H&E Slide
    normaliser.fit_source(he)
    he_norm = normaliser.transform_tile(he)
    dump_data(data_name=NORMALISER_PKL, data_obj=normaliser, subdir=PREFIX)

    if VERBOSE:
        save_img(
            he_norm.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + str(ALIGNMENT_MAG) + "x_normalised.jpeg"),
            "JPEG",
        )

    ######################
    # Image Registration #
    ######################

    # Convert to grayscale
    tp53_gray = tp53.convert("L")
    he_gray = he_norm.convert("L")
    # Convert to ITK format
    tp53_itk = get_itk_from_pil(tp53_gray)
    he_itk = get_itk_from_pil(he_gray)
    # Set fixed and moving images
    fixed_img = he_itk
    moving_img = tp53_itk

    # Check initial registration
    # Centre the two images, then compare their alignment
    initial_transform = sitk.CenteredTransformInitializer(
        fixed_img,
        moving_img,
        sitk.Euler2DTransform(),
        sitk.CenteredTransformInitializerFilter.GEOMETRY,
    )
    moving_rgb = sitk_transform_rgb(tp53, he_norm, initial_transform)

    # Visualise and save alignment
    if VERBOSE:
        align_plotter = PlotImageAlignment("vertical", 300)
        comparison_pre_v_stripes = align_plotter.plot_images(he, moving_rgb)
        save_img(
            comparison_pre_v_stripes.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + "comparison_pre_align_v_stripes.jpeg"),
            "JPEG",
        )

        align_plotter = PlotImageAlignment("horizontal", 300)
        comparison_pre_h_stripes = align_plotter.plot_images(he, moving_rgb)
        save_img(
            comparison_pre_h_stripes.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + "comparison_pre_align_h_stripes.jpeg"),
            "JPEG",
        )

        align_plotter = PlotImageAlignment("mosaic", 300)
        comparison_pre_mosaic = align_plotter.plot_images(he, moving_rgb)
        save_img(
            comparison_pre_mosaic.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + "comparison_pre_align_mosaic.jpeg"),
            "JPEG",
        )

        comparison_pre_colour_overlay = show_alignment(
            he_norm, moving_rgb, prefilter=True
        )
        save_img(
            comparison_pre_colour_overlay.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + "comparison_pre_align_colour_overlay.jpeg"),
            "JPEG",
        )

    # Compute the mutual information between the two images before registration
    moving_resampled_initial = sitk.Resample(
        moving_img,
        fixed_img,
        initial_transform,
        INTERPOLATOR,
        0.0,
        moving_img.GetPixelID(),
    )
    initial_mutual_info = calculate_mutual_info(
        np.array(he_gray), np.array(get_pil_from_itk(moving_resampled_initial))
    )
    if VERBOSE:
        print("Initial mutual information metric: {0}".format(initial_mutual_info))
    # performance_df.loc[SLIDE_NUM, "Affine_Mutual_Info"] = affine_mutual_info  # Figure out how to add this
