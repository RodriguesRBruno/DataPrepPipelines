cwlVersion: v1.2
class: Workflow

requirements:
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  MultipleInputFeatureRequirement: {}

inputs:
  input_data: Directory

outputs:
    performance_csv:
      type: File
      outputSource: consolidate_metrics/performance_csv

    tiles_dirs:
      type: Directory[]
      outputSource: per_image_extraction/tiles_dir

    verbose_images:
      type: File[]
      outputSource: merge_verbose_images/merged_images

steps:
  normaliser_step:
    run: ../individual_steps/normaliser_step.cwl
    in:
      input_data_dir: input_data
    out: [base_normaliser_pkl]
  
  scatter_images:
    run: ../individual_steps/scatter_images.cwl 
    in:
      input_data_dir: input_data
    out: [image_prefixes]

  convert_file_to_array:
    run: ../individual_steps/convert_file_to_array.cwl
    in:
      image_prefixes_file: scatter_images/image_prefixes
    out: [image_prefixes_array]

  per_image_extraction:
    run: per_image_extraction.cwl
    scatter: image_prefix
    in:
      input_data_dir: input_data
      base_normaliser_pkl: normaliser_step/base_normaliser_pkl
      image_prefix: convert_file_to_array/image_prefixes_array
    out: [performance_csv, verbose_images, tiles_dir]

  merge_verbose_images:
    run: ../individual_steps/merge_verbose_images_in_main_workflow.cwl
    in: 
      verbose_images: per_image_extraction/verbose_images
    out: [merged_images]

  consolidate_metrics:
    run: ../individual_steps/consolidate_metrics.cwl
    in:
      performance_csv_files: per_image_extraction/performance_csv
    out: [performance_csv]