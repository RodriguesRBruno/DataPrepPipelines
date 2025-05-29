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
      TEMP_DATA_PATH: $(runtime.outdir)/data/.tmp

baseCommand: python 
arguments: ["/HEMnet/HEMnet/consolidate_metrics.py"]

inputs:
  performance_csv_files:
    type: File[]
    inputBinding:
      position: 2
      prefix: "--performance-csv-files"

outputs:
  performance_csv:
    type: File
    outputBinding:
      glob: "$(runtime.outdir)/data/performance_metrics.csv"