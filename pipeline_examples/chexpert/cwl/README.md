Running this simple example on CWL Tools:

1. Install CWL Tool in your current virtual environment
`pip install cwltool`

2. In this directory, run the following command:
`cwltool --outdir output chexpert-workflow.cwl chexpert.yaml`

1. A directory named `output` should be created.

2. Outputs from CheXpert will be located at:
   - `output/images`: images
   - `output/data.csv`: labels of each image from the above directory