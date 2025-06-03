Running this simple example on MiniWDL:

1. Get the CheXpert data according to [Section 1 in the main README.md](../README.md#1-get-the-chexpert-data) of the `chexpert` directory in this repo.

2. Install MiniWDL in your current virtual environment
`pip install miniwdl`

3. In this directory, run the following command:
`miniwdl run chexpert.wdl -i chexpert_input.json`
NOTE: if the input data from item 1 is saved at a path different from what is shown in the [main README.md](../README.md#1-get-the-chexpert-data) the paths in `chexpert_input.json` must be modified accordingly.

4. A directory named `_LAST` will be created, along with a timestamped directory for each execution made, in the format `YYYYMMDD_HHmmss_chexpert`. The `_LAST` directory is a symlink to the most recent run.

5. Outputs from CheXpert will be located at:
   - `{run_directory}/out/images_dir/images`: images
   - `{run_directory}/out/data_csv/data.csv`: labels of each image from the above directory