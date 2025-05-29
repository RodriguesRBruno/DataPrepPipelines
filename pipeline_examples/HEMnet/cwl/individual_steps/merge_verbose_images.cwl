#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: ExpressionTool

hints:
  DockerRequirement:
    dockerPull: local/hemnet:0.0.8

requirements:
  InlineJavascriptRequirement: {}

inputs:
  verbose_images_registration: File[]
  verbose_images_affine: File[]
  verbose_images_bspline: File[]
  verbose_images_generate_masks: File[]

outputs:
  merged_images: File[]

expression: |
 ${ 
  return {"merged_images": inputs.verbose_images_registration.concat(inputs.verbose_images_affine, 
                                                                     inputs.verbose_images_bspline, 
                                                                     inputs.verbose_images_generate_masks)}; 
  }