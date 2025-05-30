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

    cancer_tiles_dir:
      type: Directory
      outputSource: merge_cancer_tiles/merged_tiles_dir

    non_cancer_tiles_dir:
      type: Directory
      outputSource: merge_non_cancer_tiles/merged_tiles_dir

    uncertain_tiles_dir:
      type: Directory
      outputSource: merge_uncertain_tiles/merged_tiles_dir

    verbose_images:
      type: 
        type: array
        items:
          type: array
          items: [File]

      outputSource: per_image_extraction/verbose_images


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
    out: [performance_csv, verbose_images, tiles_dir, cancer_tiles, uncertain_tiles, non_cancer_tiles]

  merge_cancer_tiles:
    run: ../individual_steps/merge_tiles_files.cwl
    in:
      tiles_files: per_image_extraction/cancer_tiles
      final_dir_name: 
        valueFrom: cancer
    out: [merged_tiles_dir]
  
  merge_non_cancer_tiles:
    run: ../individual_steps/merge_tiles_files.cwl
    in:
      tiles_files: per_image_extraction/non_cancer_tiles
      final_dir_name: 
        valueFrom: non_cancer
    out: [merged_tiles_dir]

  merge_uncertain_tiles:
    run: ../individual_steps/merge_tiles_files.cwl
    in:
      tiles_files: per_image_extraction/uncertain_tiles
      final_dir_name: 
        valueFrom: uncertain
    out: [merged_tiles_dir]

  # merge_tiles_dirs:
  #   run: ../individual_steps/merge_tiles_dirs.cwl
  #   in:
  #     tiles_dirs: per_image_extraction/tiles_dir
  #   out: [merged_tiles_dir]

  consolidate_metrics:
    run: ../individual_steps/consolidate_metrics.cwl
    in:
      performance_csv_files: per_image_extraction/performance_csv
    out: [performance_csv]