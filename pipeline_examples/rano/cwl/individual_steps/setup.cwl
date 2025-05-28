#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: CommandLineTool

hints:
  DockerRequirement:
    dockerPull: local/rano-data-prep-cwl:0.0.1

requirements:
  EnvVarRequirement:
    envDef:
      WORKSPACE_DIRECTORY: $(runtime.outdir)
      INPUT_DIR: $(inputs.input_data.path)
    
baseCommand: initial_setup
inputs:
  input_data:
    type: Directory

outputs:
  data_dir:
    type: Directory
    outputBinding:
      glob: "$(runtime.outdir)/data"
  
  raw_data_dir:
    type: Directory
    outputBinding:
      glob: "$(runtime.outdir)/data/raw"