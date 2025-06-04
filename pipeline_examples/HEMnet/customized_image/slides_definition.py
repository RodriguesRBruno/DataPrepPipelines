from mod_constants import INPUT_PATH
import argparse


def slides_definition():
    """
    This preserves the behavior of the original code of assuming the user only
    sends paired slides into the input directory.
    If unpaired slides are sent, this code does NOT check for that!
    """

    slides = []
    for slide in INPUT_PATH.glob("*.svs"):
        name = slide.name
        slides.append(name)
    slides.sort()

    TP53_slides = [slide for slide in slides if "TP53" in slide]
    HE_slides = [slide for slide in slides if "HandE" in slide]
    Paired_slides = list(zip(TP53_slides, HE_slides))
    prefixes = [paired_slide[0][:-10] for paired_slide in Paired_slides]
    return prefixes


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l",
        "--lines",
        action="store_true",
        help="Output one pair per line, rater than as a list",
    )
    args = parser.parse_args()
    prefixes = slides_definition()

    if args.lines:
        print(*prefixes, sep="\n")
    else:
        print(prefixes)
