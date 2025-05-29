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
arguments: ["/HEMnet/HEMnet/bspline_registration.py", "-v"]

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

  he_gray_img:
    type: File
    inputBinding:
      position: 5
      prefix: --he-gray

  tp53_gray_img:
    type: File
    inputBinding:
      position: 6
      prefix: --tp53-gray

  he_norm_img:
    type: File
    inputBinding:
      position: 7
      prefix: --he-norm

  moving_resampled_affine:
    type: File
    inputBinding:
      position: 8
      prefix: --moving-affine
  
  affine_transform:
    type: File
    inputBinding:
      position: 9
      prefix: --affine-transform

  

outputs:
  performance_csv:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/performance_metrics.csv"

  verbose_images:
    type: File[]
    outputBinding:
        glob: "$(runtime.outdir)/data/*.jpeg"

  he_filtered:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/he_filtered.npy"

  tp53_filtered:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/tp53_filtered.npy"