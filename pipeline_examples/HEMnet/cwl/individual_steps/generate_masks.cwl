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
arguments: ["/HEMnet/HEMnet/generate_masks.py", "-v"]

inputs:
  input_data_dir:
    type: Directory
  
  image_prefix:
    type: string
    inputBinding:
      position: 3
      prefix: --subject-subdir

  he_filtered:
    type: File
    inputBinding:
      position: 4
      prefix: --he-filtered

  tp53_filtered:
    type: File
    inputBinding:
      position: 5
      prefix: --tp53-filtered


outputs:
  verbose_images:
    type: File[]
    outputBinding:
        glob: "$(runtime.outdir)/data/*.jpeg"

  u_mask_filtered:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/u_mask_filtered.npy"

  c_mask_filtered:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/c_mask_filtered.npy"

  non_c_mask_filtered:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/non_c_mask_filtered.npy"

  t_mask_filtered:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/$(inputs.image_prefix)/t_mask_filtered.npy"