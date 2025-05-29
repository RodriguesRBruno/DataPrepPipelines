#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: CommandLineTool

hints:
  DockerRequirement:
    dockerPull: local/hemnet:0.0.8

requirements:
  MultipleInputFeatureRequirement: {}

inputs:
  verbose_images:
    type: File[]
    linkMerge: merge_flattened

outputs:
  merged_images: File[]

expression: |
 ${ return {"merged_images": inputs.verbose_images.flat(Infinity)}; }