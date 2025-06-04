version development

import "tasks/aggregated_tasks.wdl"
import "tasks/per_slide_tasks.wdl"

workflow hemnet {
  input {
    Directory input_data_dir
    Float alignment_magnify=2
    Float tile_magnify=10
    Int output_tile_size=224
    Boolean disable_luminosity_standardisation=false
    String normaliser="vahadane"
    Float cancer_thresh=0.39
    Float non_cancer_thresh=0.40
    Boolean verbosity=false
  }

  call aggregated_tasks.normaliser_step{
    input:
      input_data_dir=input_data_dir,
      normaliser=normaliser,
      alignment_magnify=alignment_magnify,
      disable_luminosity_standardisation=disable_luminosity_standardisation
  }

  call aggregated_tasks.scatter_images{
    input:
      input_data_dir=input_data_dir
  }

  scatter (image_prefix in scatter_images.image_prefixes) {
    call per_slide_tasks.image_registration{
      input:
        input_data_dir=input_data_dir,
        base_normaliser_pkl=normaliser_step.base_normaliser_pkl,
        image_prefix=image_prefix,
        alignment_magnify=alignment_magnify,
        verbosity=verbosity
    }

    call per_slide_tasks.affine_registration{
      input:
        input_data_dir=input_data_dir,
        image_prefix=image_prefix,
        performance_csv_in=image_registration.performance_csv,
        he_gray_img=image_registration.he_gray_img,
        tp53_gray_img=image_registration.tp53_gray_img,
        alignment_magnify=alignment_magnify,
        verbosity=verbosity
    }

    call per_slide_tasks.bspline_registration{
      input:
        input_data_dir=input_data_dir,
        image_prefix=image_prefix,
        performance_csv_in=affine_registration.performance_csv,
        he_gray_img=image_registration.he_gray_img,
        he_norm_img=image_registration.he_norm_img,
        tp53_gray_img=image_registration.tp53_gray_img,
        moving_resampled_affine=affine_registration.moving_resampled_affine,
        affine_transform=affine_registration.affine_transform,
        alignment_magnify=alignment_magnify,
        verbosity=verbosity
    }

    call per_slide_tasks.generate_masks{
      input:
        input_data_dir=input_data_dir,
        image_prefix=image_prefix,
        he_filtered=bspline_registration.he_filtered,
        tp53_filtered=bspline_registration.tp53_filtered,
        alignment_magnify=alignment_magnify,
        tile_magnify=tile_magnify,
        tile_size=output_tile_size,
        cancer_thresh=cancer_thresh,
        non_cancer_thresh=non_cancer_thresh,
        verbosity=verbosity
    }

    call per_slide_tasks.save_tiles{
      input:
        input_data_dir=input_data_dir,
        image_prefix=image_prefix,
        performance_csv_in=bspline_registration.performance_csv,
        specific_normaliser=image_registration.specific_normaliser,
        u_mask_filtered=generate_masks.u_mask_filtered,
        c_mask_filtered=generate_masks.c_mask_filtered,
        non_c_mask_filtered=generate_masks.non_c_mask_filtered,
        t_mask_filtered=generate_masks.t_mask_filtered,
        tile_magnify=tile_magnify,
        tile_size=output_tile_size
    }
  }

  # Array[File] verbose_images_registration = if defined(image_registration.verbose_images) then flatten(image_registration.verbose_images) else []
  # Array[File]? verbose_images_affine = flatten(affine_registration.verbose_images)
  # Array[File]? verbose_images_bspline = flatten(bspline_registration.verbose_images)
  # Array[File]? verbose_images_masks= flatten(generate_masks.verbose_images)

  # Array[File]? result_verbose_images = flatten(
  #   [verbose_images_registration, verbose_images_affine, 
  #   verbose_images_bspline, verbose_images_masks]
  #   )

  call aggregated_tasks.consolidate_metrics{
    input:
      performance_csv_files=save_tiles.performance_csv
  }

  output {
    File performance_csv = consolidate_metrics.performance_csv
    # Array[File]? verbose_images = result_verbose_images
    Array[File] cancer_tiles = flatten(save_tiles.cancer_tiles)
    Array[File] non_cancer_tiles = flatten(save_tiles.non_cancer_tiles)
    Array[File] uncertain_tiles = flatten(save_tiles.uncertain_tiles)
  }
}
