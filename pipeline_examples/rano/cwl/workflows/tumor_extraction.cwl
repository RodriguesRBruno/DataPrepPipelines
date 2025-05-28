cwlVersion: v1.2
class: Workflow

requirements:
  ScatterFeatureRequirement: {}

inputs:
  data_dir: Directory
  subject_subdir: string

outputs:
    output_data_dir:
        type: Directory
        outputSource: nifti_conversion/nifti_output

steps:
  make_csv:
    run: ../individual_steps/make_csv.cwl
    in:
      subject_subdir: subject_subdir
      data_dir: data_dir
    out: [csv_output]

  nifti_conversion:
    run: ../individual_steps/nifti_conversion.cwl
    in:
      subject_subdir: subject_subdir
      data_dir: data_dir
      csv_dir: make_csv/csv_output
    out: [nifti_output]
    