This simple CWL tool runs a slightly modified version of HEMnet. The original version is available in the [HEMnet repository.](https://github.com/BiomedicalMachineLearning/HEMnet)

1. Build the Customized HEMnet Docker images from this repo
```bash
cd ../customized_image
docker build . -t local/hemnet:0.0.8
```
NOTE: if a different tag is used, it must be updated in all files located at `./individual_steps`

2. Set the SVS slides to be used as input to the pipeline in a single directory
2.1 In the same directory as the above step, create a subdirectory called `template` and put a copy of one of the slides in this directory to be used as the template slide.


3. Install CWL Tool in your current virtual environment
`pip install cwltool`

4. Go to the `workflows` directory.
```bash
cd workflows
```

5. In the `hemnet_input.yaml` file, set the `input_data.path` attribute as the path to the directory defined in step 2.

6. In he `workflows` directory, run the following command to execute the pipeline:
```bash
cwltool --outdir output hemnet.cwl hemnet_input.yaml
```
NOTES: 
- An optional `--cachedir <CACHE_DIR>` argument may be used to cache the pipeline during execution for debugging purposes.
- Alternatively, the `cwltool --make-template hemnet.cwl >> template.yaml` command may be run to create a more complete template file with various optional configuration arguments for the pipeline. The provided `hemnet_input.yaml` file contains only the minimal necessary inputs. The other inputs have default values and are not strictly necessary for runs.

7. A directory named `output` should be created.

8. Outputs from HEMnet will be located at:
   - `output/tiles_10x`: tiles magnified by 10x (default option; can be specified in `hemnet_input.yaml` for different values if desired), categorized as cancer tiles, non-cancer tiles or uncertain tiles.
   - `output/*.jpeg`: various images saved during execution for debugging purposes. Only appear if `verbosity: true` is specified in the `hemnet_input.yaml` file (default)
   - `output/performance_metrics.csv`: performance metrics for the pipeline run