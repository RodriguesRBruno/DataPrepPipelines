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
  ResourceRequirement:
    coresMin: 3
    coresMax: 5
    ramMin: 3000
    ramMax: 3800
    
baseCommand: python 
arguments: ["/HEMnet/HEMnet/bspline_registration.py"]

inputs:
  input_data_dir:
    type: Directory
  
  image_prefix:
    type: string
    inputBinding:
      position: 3
      prefix: --subject-subdir

  performance_csv_in: 
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

  alignment_magnify:
    type: float
    default: 2
    inputBinding:
      position: 10
      prefix: "--align_mag"
  
  verbosity:
    type: boolean
    default: true
    inputBinding:
      position: 11
      prefix: "-v"

outputs:
  performance_csv_out:
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