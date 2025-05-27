#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: CommandLineTool

hints:
  DockerRequirement:
    dockerPull: mlcommons/chexpert-prep:0.1.0

baseCommand: prepare
arguments: ["--output_path", "$(runtime.outdir)"]
inputs:
  data_path:
    type: Directory
    inputBinding:
      position: 1
      prefix: --data_path

  labels_path:
    type: Directory
    inputBinding:
      position: 2
      prefix: --labels_path
    
  parameters_file:
    type: File 
    inputBinding:
      position: 3
      prefix: --parameters_file

outputs:
  output_dir:
    type: Directory
    outputBinding:
      glob: $(runtime.outdir)
    