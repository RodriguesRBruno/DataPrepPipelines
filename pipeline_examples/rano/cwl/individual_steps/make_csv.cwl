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
      DATA_DIR: $(runtime.outdir)/data
  
  InitialWorkDirRequirement:
    listing:
      - $(inputs.data_dir) 
      
baseCommand: make_csv
inputs:
  data_dir:
    type: Directory

  subject_subdir:
    type: string
    inputBinding:
        position: 1
        prefix: --subject-subdir

outputs:
  csv_output:
    type: Directory
    outputBinding:
      glob: "$(runtime.outdir)/data"