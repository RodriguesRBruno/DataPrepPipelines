Running this simple example on CWL Tools:

1. Get the CheXpert data according to [Section 1 in the main README.md](../README.md#1-get-the-chexpert-data) of the `chexpert` directory in this repo.

2. Install CWL Tool in your current virtual environment
`pip install cwltool`

3. In this directory, run the following command:
`cwltool --outdir output chexpert-workflow.cwl chexpert.yaml`
NOTE: if the input data from item 1 is saved at a path different from what is shown in the [main README.md](../README.md#1-get-the-chexpert-data) the paths in `chexpert.yaml` must be modified accordingly.

4. A directory named `output` should be created.

5. Outputs from CheXpert will be located at:
   - `output/images`: images
   - `output/data.csv`: labels of each image from the above directory