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
arguments: ["/HEMnet/HEMnet/image_registration.py", "-v"]

inputs:
  input_data_dir:
    type: Directory

  base_normaliser_pkl: 
    type: File
    inputBinding:
      position: 3
      prefix: --normaliser-path
    
  image_prefix:
    type: string
    inputBinding:
      position: 4
      prefix: --subject-subdir

outputs:
  performance_csv:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/performance_metrics.csv"

  specific_normaliser:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/normaliser.pkl"

  he_norm_img:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/he_norm.npy"

  he_gray_img:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/he_gray.npy"

  tp53_gray_img:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/tp53_gray.npy"

  verbose_images:
    type: File[]
    outputBinding:
      glob: "$(runtime.outdir)/data/*.jpeg"