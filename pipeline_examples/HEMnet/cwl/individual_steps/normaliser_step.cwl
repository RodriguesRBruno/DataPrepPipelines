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
      TEMP_DATA_PATH: $(runtime.outdir)/data/.tmp

baseCommand: python 
arguments: ["/HEMnet/HEMnet/normaliser_step.py"]

inputs:
  input_data_dir:
    type: Directory

outputs:
  base_normaliser_pkl:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/.tmp/normaliser.pkl"