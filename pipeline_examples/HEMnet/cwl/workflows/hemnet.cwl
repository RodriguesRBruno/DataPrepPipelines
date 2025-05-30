cwlVersion: v1.2
class: Workflow

requirements:
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  MultipleInputFeatureRequirement: {}

inputs:
  input_data: Directory
  alignment_magnify:
    type: float
    default: 2

  tile_magnify:
    type: float
    default: 10

  output_tile_size:
    type: int
    default: 224

  disable_luminosity_standardisation:
    type: boolean?

  normaliser:
    type:
    - type: enum
      symbols:
        - vahadane
        - macenko
        - reinhard
        - "none"
    default: vahadane

  cancer_thresh:
    type: float
    default: 0.39

  non_cancer_thresh:
    type: float
    default: 0.40

  verbosity:
    type: boolean
    default: true

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
      disable_luminosity_standardisation: disable_luminosity_standardisation
      alignment_magnify: alignment_magnify
      normaliser: normaliser
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
      alignment_magnify: alignment_magnify
      tile_magnify: tile_magnify
      output_tile_size: output_tile_size
      cancer_thresh: cancer_thresh
      non_cancer_thresh: non_cancer_thresh
      verbosity: verbosity
    out: [performance_csv, verbose_images, tiles_dir, cancer_tiles, uncertain_tiles, non_cancer_tiles]

  merge_cancer_tiles:
    run: ../individual_steps/merge_tiles_files.cwl
    in:
      tiles_files: per_image_extraction/cancer_tiles
      final_dir_name: 
        valueFrom: cancer
      tile_magnify: tile_magnify
    out: [merged_tiles_dir]
  
  merge_non_cancer_tiles:
    run: ../individual_steps/merge_tiles_files.cwl
    in:
      tiles_files: per_image_extraction/non_cancer_tiles
      final_dir_name: 
        valueFrom: non_cancer
      tile_magnify: tile_magnify
    out: [merged_tiles_dir]

  merge_uncertain_tiles:
    run: ../individual_steps/merge_tiles_files.cwl
    in:
      tiles_files: per_image_extraction/uncertain_tiles
      final_dir_name: 
        valueFrom: uncertain
      tile_magnify: tile_magnify
    out: [merged_tiles_dir]

  consolidate_metrics:
    run: ../individual_steps/consolidate_metrics.cwl
    in:
      performance_csv_files: per_image_extraction/performance_csv
    out: [performance_csv]