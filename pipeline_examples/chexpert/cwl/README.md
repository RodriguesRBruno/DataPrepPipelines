Running this simple example on CWL Tools:

1. Install CWL Tool in your current virtual environment
`pip install cwltool`

2. In this directory, run the following command:
`cwltool cwltool chexpert-workflow.cwl chexpert.yaml`

3. A directory named `docker_tmp{random_string}` should be created.

4. Outputs from CheXpert will be located at:
   - `docker_tmp{random_string}/images`: images
   - `docker_tmp{random_string}/data.csv`: labels of each image from the above directory

TODO: can we make it so we specify the name of the output directory (i.e just `data` or similar) instead of the auto-generated name?