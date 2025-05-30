#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: CommandLineTool

hints:
  DockerRequirement:
    dockerPull: local/hemnet:0.0.8
  
requirements:
  EnvVarRequirement:
    envDef:
      WORKSPACE_PATH: $(runtime.outdir)
      OUTPUT_PATH: $(runtime.outdir)/data
      INPUT_PATH: $(inputs.input_data_dir.path)
      TEMP_DATA_PATH: $(runtime.outdir)/.tmp

baseCommand: python 
arguments: ["/HEMnet/HEMnet/save_tiles.py", "-v"]

inputs:
  input_data_dir:
    type: Directory
  
  image_prefix:
    type: string
    inputBinding:
      position: 3
      prefix: --subject-subdir

  performance_csv: 
    type: File
    inputBinding:
      position: 4
      prefix: --performance-csv

  specific_normaliser:
    type: File
    inputBinding:
      position: 5
      prefix: --normaliser-path

  u_mask_filtered:
    type: File
    inputBinding:
      position: 6
      prefix: --u-mask

  c_mask_filtered:
    type: File
    inputBinding:
      position: 7
      prefix: --c-mask

  non_c_mask_filtered:
    type: File
    inputBinding:
      position: 8
      prefix: --non-c-mask
  
  t_mask_filtered:
    type: File
    inputBinding:
      position: 9
      prefix: --t-mask

  

outputs:
  performance_csv:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/performance_metrics.csv"

  cancer_tiles:
    type: File[]
    outputBinding:
      glob: "$(runtime.outdir)/data/tiles_*/cancer/*.jpeg"
  
  non_cancer_tiles:
    type: File[]
    outputBinding:
      glob: "$(runtime.outdir)/data/tiles_*/non_cancer/*.jpeg"

  uncertain_tiles:
    type: File[]
    outputBinding:
      glob: "$(runtime.outdir)/data/tiles_*/uncertain/*.jpeg"

  tiles_dir:
    type: Directory
    outputBinding:
      glob: "$(runtime.outdir)/data/tiles_*/"