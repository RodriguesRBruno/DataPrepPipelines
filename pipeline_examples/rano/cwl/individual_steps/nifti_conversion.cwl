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
    
baseCommand: convert_nifti
inputs:
  subject_subdir:
    type: string
    inputBinding:
        position: 1
        prefix: --subject-subdir

  data_dir:
    type: Directory

  csv_dir:
    type: Directory

    
outputs:
  nifti_output:
    type: Directory
    outputBinding:
      glob: "$(runtime.outdir)/data"