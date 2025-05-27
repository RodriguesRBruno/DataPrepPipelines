cwlVersion: v1.2
class: Workflow

inputs:
  data_path: Directory
  labels_path: Directory  
  parameters_file: File 

outputs:
  output_images:
    type: Directory
    outputSource: prepare/output_dir

steps:
  prepare:
    run: chexpert-prepare.cwl
    in:
      data_path: data_path
      labels_path: labels_path
      parameters_file: parameters_file
    out: [output_dir]