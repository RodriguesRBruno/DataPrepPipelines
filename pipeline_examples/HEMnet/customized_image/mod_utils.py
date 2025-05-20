import argparse
import os
from openslide import open_slide
from mod_constants import INPUT_PATH, BASE_NORMALISER, NORMALISER_PATH
from slide import read_slide_at_mag
from normaliser import IterativeNormaliser
import pickle


def save_img(img, path, img_type):
    img.save(path, img_type)


def save_fig(fig, path, dpi=300):
    fig.savefig(path, dpi=dpi)


def restricted_float(x):
    # Restrict argument to float between 0 and 1 (inclusive)
    try:
        x = float(x)
    except ValueError:
        raise argparse.ArgumentTypeError("{0} not a floating point literal".format(x))
    if x < 0.0 or x > 1.0:
        raise argparse.ArgumentTypeError("{0} not in range [0.0, 1.0]".format(x))
    return x


def create_target_fitted_normaliser(
    template_slide_path, alignment_mag, normaliser_method, standardise_luminosity
) -> IterativeNormaliser:
    if template_slide_path is None:
        input_dir = str(INPUT_PATH)
        template_dir = os.path.join(input_dir, "template")
        try:
            slides = [
                file for file in os.listdir(template_dir) if file.endswith(".svs")
            ]
            template_slide_path = os.path.join(template_dir, slides[0])
            if len(slides) > 1:
                print(
                    f"More than 1 slide found at {template_dir}. Using {template_slide_path} as the template."
                )
        except OSError:
            raise ValueError(
                f"Please provide an explicit template slide either with the -t option or by setting a single .svs file at the {os.path.join(template_dir)} directory!"
            )

    print(
        f"Using slide located at {template_slide_path} as the template to instantiate normaliser."
    )
    template_slide = open_slide(str(template_slide_path))
    template_img = read_slide_at_mag(template_slide, alignment_mag).convert("RGB")

    normaliser = IterativeNormaliser(normaliser_method, standardise_luminosity)
    normaliser.fit_target(template_img)

    return normaliser


def _get_normaliser_full_path(normaliser_name):
    pickle_file = normaliser_name + ".pkl"
    os.makedirs(NORMALISER_PATH, exist_ok=True)
    full_pickle_path = NORMALISER_PATH.joinpath(pickle_file)
    return full_pickle_path


def dump_normaliser(normalizer_obj, normaliser_name: str = BASE_NORMALISER):
    full_path = _get_normaliser_full_path(normaliser_name)
    with open(full_path, "wb") as f:
        pickle.dump(normalizer_obj, f)
    print(f"Successfully dumped normalizer at {full_path}.")


def load_normaliser(normaliser_name: str = BASE_NORMALISER) -> IterativeNormaliser:
    full_path = _get_normaliser_full_path(normaliser_name)
    with open(full_path, "rb") as f:
        normalizer_obj = pickle.load(f)
    print(f"Successfully loaded normalizer from {full_path}.")
    return normalizer_obj


def load_slides_by_prefix(prefix: str, aligment_mag: float):
    print(f"Loading slides with prefix {prefix}")
    input_dir_str = str(INPUT_PATH)
    relevant_filenames = [file for file in os.listdir(input_dir_str) if prefix in file]
    relevant_filepaths = sorted(
        [os.path.join(input_dir_str, file) for file in relevant_filenames]
    )

    he_path, tp53_path = relevant_filepaths

    tp53_slide = open_slide(tp53_path)
    he_slide = open_slide(he_path)

    # Load Slides
    he = read_slide_at_mag(he_slide, aligment_mag)
    tp53 = read_slide_at_mag(tp53_slide, aligment_mag)
    print(
        f"Successfully loaded slides with prefix {prefix}.\nSlides loaded:\n{' '.join(relevant_filepaths)}"
    )
    return he, tp53
