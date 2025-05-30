cwlVersion: v1.2
class: Workflow

requirements:
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  ResourceRequirement:
    coresMin: 3
    coresMax: 5
    ramMin: 3000
    ramMax: 3800

inputs:
  input_data_dir: Directory
  base_normaliser_pkl: File
  image_prefix: string

  alignment_magnify:
    type: float
    default: 2

  tile_magnify:
    type: float
    default: 10

  output_tile_size:
    type: int
    default: 224
  
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
      outputSource: save_tiles/performance_csv
    
    cancer_tiles:
      type: File[]
      outputSource: save_tiles/cancer_tiles
    
    non_cancer_tiles:
      type: File[]
      outputSource: save_tiles/non_cancer_tiles

    uncertain_tiles:
      type: File[]
      outputSource: save_tiles/uncertain_tiles

    tiles_dir:
      type: Directory
      outputSource: save_tiles/tiles_dir

    verbose_images:
      type: File[]
      outputSource: merge_verbose_images/merged_images

steps:
  image_registration:
    run: ../individual_steps/image_registration.cwl
    in:
        input_data_dir: input_data_dir
        base_normaliser_pkl: base_normaliser_pkl
        image_prefix: image_prefix
        verbosity: verbosity
        alignment_magnify: alignment_magnify
    out: [performance_csv, specific_normaliser, he_norm_img, he_gray_img, tp53_gray_img, verbose_images]

  affine_registration:
    run: ../individual_steps/affine_registration.cwl
    in:
        input_data_dir: input_data_dir
        performance_csv: image_registration/performance_csv
        he_gray_img: image_registration/he_gray_img
        tp53_gray_img: image_registration/tp53_gray_img
        image_prefix: image_prefix
        alignment_magnify: alignment_magnify
        verbosity: verbosity
    out: [performance_csv, moving_resampled_affine, affine_transform, verbose_images]
  
  bspline_registration:
    run: ../individual_steps/bspline_registration.cwl
    in:
      input_data_dir: input_data_dir
      image_prefix: image_prefix
      performance_csv: affine_registration/performance_csv
      he_gray_img: image_registration/he_gray_img
      tp53_gray_img: image_registration/tp53_gray_img
      he_norm_img: image_registration/he_norm_img
      moving_resampled_affine: affine_registration/moving_resampled_affine
      affine_transform: affine_registration/affine_transform
      alignment_magnify: alignment_magnify
      verbosity: verbosity
    out: [performance_csv, he_filtered, tp53_filtered, verbose_images]

  generate_masks:
    run: ../individual_steps/generate_masks.cwl
    in:
      input_data_dir: input_data_dir
      image_prefix: image_prefix
      he_filtered: bspline_registration/he_filtered
      tp53_filtered: bspline_registration/tp53_filtered
      alignment_magnify: alignment_magnify
      tile_magnify: tile_magnify
      tile_size: output_tile_size
      cancer_thresh: cancer_thresh
      non_cancer_thresh: non_cancer_thresh
      verbosity: verbosity
    out: [verbose_images, u_mask_filtered, c_mask_filtered, non_c_mask_filtered, t_mask_filtered]

  save_tiles:
    run: ../individual_steps/save_tiles.cwl
    in:
      input_data_dir: input_data_dir
      image_prefix: image_prefix
      performance_csv: bspline_registration/performance_csv
      specific_normaliser: image_registration/specific_normaliser
      u_mask_filtered: generate_masks/u_mask_filtered
      c_mask_filtered: generate_masks/c_mask_filtered
      non_c_mask_filtered: generate_masks/non_c_mask_filtered
      t_mask_filtered: generate_masks/t_mask_filtered
      tile_magnify: tile_magnify
      tile_size: output_tile_size
    out: [tiles_dir, performance_csv, cancer_tiles, non_cancer_tiles, uncertain_tiles]

  merge_verbose_images:
    run: ../individual_steps/merge_verbose_images.cwl
    in:
      verbose_images_registration: image_registration/verbose_images
      verbose_images_affine: affine_registration/verbose_images
      verbose_images_bspline: bspline_registration/verbose_images
      verbose_images_generate_masks: generate_masks/verbose_images
    out: [merged_images]