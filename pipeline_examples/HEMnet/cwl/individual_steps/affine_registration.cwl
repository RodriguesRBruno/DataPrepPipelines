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
arguments: ["/HEMnet/HEMnet/affine_registration.py", "-v"]

inputs:
  input_data_dir:
    type: Directory

  performance_csv: 
    type: File
    inputBinding:
      position: 3
      prefix: --performance-csv

  he_gray_img:
    type: File
    inputBinding:
      position: 4
      prefix: --he-gray

  tp53_gray_img:
    type: File
    inputBinding:
      position: 5
      prefix: --tp53-gray

  image_prefix:
    type: string
    inputBinding:
      position: 6
      prefix: --subject-subdir

outputs:
  performance_csv:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/performance_metrics.csv"

  moving_resampled_affine:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/moving_resampled_affine.npy"

  affine_transform:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/affine_transform.hdf"
  
  verbose_images:
    type: File[]
    outputBinding:
      glob: "$(runtime.outdir)/data/*.jpeg"