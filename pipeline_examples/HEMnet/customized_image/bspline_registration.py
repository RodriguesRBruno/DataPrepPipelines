import argparse
from mod_utils import (
    load_slides_by_prefix,
    load_sitk_transform,
    save_fig,
    save_img,
    load_data,
)
import matplotlib.pyplot as plt
import SimpleITK as sitk
from mod_constants import (
    OUTPUT_PATH,
    INTERPOLATOR,
    NORMALISER_PKL,
    MOVING_RESAMPLED_AFFINE,
    AFFINE_TRANSFORM_PKL,
)
from utils import (
    get_itk_from_pil,
    calculate_mutual_info,
    get_pil_from_itk,
    start_plot,
    update_multires_iterations,
    update_plot,
    plot_metric,
    end_plot,
    sitk_transform_rgb,
    PlotImageAlignment,
    filter_green,
    filter_grays,
    show_alignment,
)
import time
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

    start = time.perf_counter()
    he, tp53 = load_slides_by_prefix(PREFIX, ALIGNMENT_MAG)
    normaliser = load_data(data_name=NORMALISER_PKL, subdir=PREFIX)
    he_norm = normaliser.transform_tile(he)
    end = time.perf_counter()

    # Convert to grayscale
    tp53_gray = tp53.convert("L")
    he_gray = he_norm.convert("L")
    # Convert to ITK format
    tp53_itk = get_itk_from_pil(tp53_gray)
    he_itk = get_itk_from_pil(he_gray)
    # Set fixed and moving images
    fixed_img = he_itk
    moving_img = tp53_itk

    moving_resampled_affine = load_sitk_transform(
        data_name=MOVING_RESAMPLED_AFFINE, subdir=PREFIX
    )
    affine_transform = load_sitk_transform(
        data_name=AFFINE_TRANSFORM_PKL, subdir=PREFIX
    )
    print(f"Time spent on reloading normaliser and slides: {end-start}s")
    bspline_method = sitk.ImageRegistrationMethod()

    # Similarity metric settings.
    bspline_method.SetMetricAsMattesMutualInformation(numberOfHistogramBins=50)
    bspline_method.SetMetricSamplingStrategy(bspline_method.RANDOM)
    bspline_method.SetMetricSamplingPercentage(0.15)

    bspline_method.SetInterpolator(INTERPOLATOR)

    # Optimizer settings.
    bspline_method.SetOptimizerAsGradientDescent(
        learningRate=1,
        numberOfIterations=200,
        convergenceMinimumValue=1e-6,
        convergenceWindowSize=10,
    )
    bspline_method.SetOptimizerScalesFromPhysicalShift()

    # Setup for the multi-resolution framework.
    bspline_method.SetShrinkFactorsPerLevel(shrinkFactors=[2, 1])
    bspline_method.SetSmoothingSigmasPerLevel(smoothingSigmas=[1, 0])
    bspline_method.SmoothingSigmasAreSpecifiedInPhysicalUnitsOn()

    # Don't optimize in-place, we would possibly like to run this cell multiple times.
    transformDomainMeshSize = [8] * moving_resampled_affine.GetDimension()
    initial_transform = sitk.BSplineTransformInitializer(
        fixed_img, transformDomainMeshSize
    )
    bspline_method.SetInitialTransform(initial_transform, inPlace=False)

    # Connect all of the observers so that we can perform plotting during registration.
    bspline_method.AddCommand(sitk.sitkStartEvent, start_plot)
    bspline_method.AddCommand(
        sitk.sitkMultiResolutionIterationEvent, update_multires_iterations
    )
    bspline_method.AddCommand(
        sitk.sitkIterationEvent, lambda: update_plot(bspline_method)
    )

    bspline_transform = bspline_method.Execute(
        sitk.Cast(fixed_img, sitk.sitkFloat32),
        sitk.Cast(moving_resampled_affine, sitk.sitkFloat32),
    )

    if VERBOSE:
        bspline_fig = plot_metric(
            "Plot of mutual information cost in B-spline registration"
        )
        plt.show()

        save_fig(bspline_fig, OUTPUT_PATH.joinpath(PREFIX + "bspline_metric_plot.jpeg"))
        end_plot()

        print(
            "B-spline Optimizer's stopping condition, {0}".format(
                bspline_method.GetOptimizerStopConditionDescription()
            )
        )

    # Compute the mutual information between the two images after B-spline registration
    moving_resampled_final = sitk.Resample(
        moving_resampled_affine,
        fixed_img,
        bspline_transform,
        INTERPOLATOR,
        0.0,
        moving_img.GetPixelID(),
    )
    bspline_mutual_info = calculate_mutual_info(
        np.array(he_gray), np.array(get_pil_from_itk(moving_resampled_final))
    )
    if VERBOSE:
        print("B-spline mutual information metric: {0}".format(bspline_mutual_info))
    # TODO figure out how to add this
    # performance_df.loc[SLIDE_NUM, "Final_Mutual_Info"] = bspline_mutual_info

    # Transform the original TP53 into the aligned TP53 image
    moving_rgb_affine = sitk_transform_rgb(
        tp53, he_norm, affine_transform, INTERPOLATOR
    )
    tp53_aligned = sitk_transform_rgb(
        moving_rgb_affine, he_norm, bspline_transform, INTERPOLATOR
    )

    # Visualise and save alignment
    if VERBOSE:
        align_plotter = PlotImageAlignment("vertical", 300)
        comparison_post_v_stripes = align_plotter.plot_images(he, tp53_aligned)
        save_img(
            comparison_post_v_stripes.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + "comparison_post_align_v_stripes.jpeg"),
            "JPEG",
        )

        align_plotter = PlotImageAlignment("horizontal", 300)
        comparison_post_h_stripes = align_plotter.plot_images(he, tp53_aligned)
        save_img(
            comparison_post_h_stripes.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + "comparison_post_align_h_stripes.jpeg"),
            "JPEG",
        )

        align_plotter = PlotImageAlignment("mosaic", 300)
        comparison_post_mosaic = align_plotter.plot_images(he, tp53_aligned)
        save_img(
            comparison_post_mosaic.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + "comparison_post_align_mosaic.jpeg"),
            "JPEG",
        )

    # Remove backgrounds from TP53 and H&E images
    tp53_filtered = filter_green(tp53_aligned)
    he_filtered = filter_green(he_norm)
    tp53_filtered = filter_grays(tp53_filtered, tolerance=2)
    he_filtered = filter_grays(he_filtered, tolerance=15)

    # Visually compare alignment between the registered TP53 and original H&E image
    if VERBOSE:
        comparison_post_colour_overlay = show_alignment(he_filtered, tp53_filtered)
        save_img(
            comparison_post_colour_overlay.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + "comparison_post_align_colour_overlay.jpeg"),
            "JPEG",
        )

        save_img(
            tp53_aligned.convert("RGB"),
            OUTPUT_PATH.joinpath(PREFIX + str(ALIGNMENT_MAG) + "x_TP53_aligned.jpeg"),
            "JPEG",
        )
        save_img(
            tp53_filtered.convert("RGB"),
            OUTPUT_PATH.joinpath(
                PREFIX + str(ALIGNMENT_MAG) + "x_TP53_aligned_white.jpeg"
            ),
            "JPEG",
        )
