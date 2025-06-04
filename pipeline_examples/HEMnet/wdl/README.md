This simple WDL tool runs a slightly modified version of HEMnet. The original version is available in the [HEMnet repository.](https://github.com/BiomedicalMachineLearning/HEMnet)

1. Build the Customized HEMnet Docker imagpies from this repo
```bash
cd ../customized_image
docker build . -t local/hemnet:0.0.9
```
NOTE: if a different tag is used, it must be updated in all tasks in all the files located at `./workflows/tasks`

2. Set the SVS slides to be used as input to the pipeline in a single directory
2.1 In the same directory as the above step, create a subdirectory called `template` and put a copy of one of the slides in this directory to be used as the template slide.

3. Install MiniWDL  in your current virtual environment
`pip install miniwdl`

4. Go to the `workflows` directory.
```bash
cd workflows
```

5. In the `hemnet_input.json` file, set the `hemnet.input_data_dir` attribute as the path to the directory defined in step 2.

6. In he `workflows` directory, run the following command to execute the pipeline:
```bash
miniwdl hemnet.wdl -i hemnet_input.json --as-me
```
NOTES: 
- Alternatively, the `miniwdl run hemnet.wdl -j` command may be run to display a more complete template with various optional configuration arguments for the pipeline. The provided `hemnet_input.json` file contains only the minimal necessary inputs. The other inputs have default values and are not strictly necessary for runs.

4. A directory named `_LAST` will be created, along with a timestamped directory for each execution made, in the format `YYYYMMDD_HHmmss_hemnet`. The `_LAST` directory is a symlink to the most recent run.

8. Outputs from HEMnet will be located at:
   - `{run_directory}/out/cancer_tiles/tiles_10x`: cancer tiles magnified by 10x
   - `{run_directory}/out/non_cancer_tiles/tiles_10x`: non-cancer tiles magnified by 10x
   - `{run_directory}/out/uncertain/tiles_10x`: uncertain tiles magnified by 10x
   - `{run_directory}/out/performance_csv/performance_metrics.csv`: performance metrics for the pipeline run