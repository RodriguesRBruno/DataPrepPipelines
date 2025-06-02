cwlVersion: v1.2
class: Workflow

inputs:
  data_path: Directory
  labels_path: Directory  
  parameters_file: File 

outputs:
  images_dir:
    type: Directory
    outputSource: prepare/images_dir

  data_csv:
    type: File
    outputSource: prepare/data_csv

steps:
  prepare:
    run: chexpert-prepare.cwl
    in:
      data_path: data_path
      labels_path: labels_path
      parameters_file: parameters_file
    out: [images_dir, data_csv]