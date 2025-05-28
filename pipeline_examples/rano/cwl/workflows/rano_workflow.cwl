cwlVersion: v1.2
class: Workflow

requirements:
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}

inputs:
  input_data: Directory

outputs:
    out:
        type: Directory[]
        outputSource: extract_tumor/output_data_dir

steps:
  setup:
    run: ../individual_steps/setup.cwl
    in:
      input_data: input_data
    out: [data_dir, raw_data_dir]
  
  scatter_subjects:
    run: ../individual_steps/scatter_subjects.cwl
    in:
        raw_data_dir: setup/raw_data_dir
    out: [subject_subdirs]

  convert_file_to_array:
    run: ../individual_steps/convert_file_to_array.cwl
    in:
      subject_subdirs_file: scatter_subjects/subject_subdirs
    out: [subjects_array]

  extract_tumor:
    run: tumor_extraction.cwl
    scatter: subject_subdir
    in:
      subject_subdir: convert_file_to_array/subjects_array
      data_dir: setup/data_dir
    out: [output_data_dir]

  
    