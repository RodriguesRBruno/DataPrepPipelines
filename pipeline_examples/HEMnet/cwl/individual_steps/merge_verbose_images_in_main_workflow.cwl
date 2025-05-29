#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: ExpressionTool

hints:
  DockerRequirement:
    dockerPull: local/hemnet:0.0.8

requirements:
  InlineJavascriptRequirement: {}

inputs:
  verbose_images: 
    type:
      type: array
      items:
        type: array
        items: [File]

outputs:
  merged_images: File[]

expression: |
 ${
    var flattened_array = [];
    console.log(inputs.verbose_images)
    for (var i = 0; i < inputs.verbose_images.length; i++) {
      for (var j = 0; j < inputs.verbose_images.length; j++) {
        if (inputs.verbose_images[i][j] != null) {
          flattened_array.push(inputs.verbose_images[i][j]);
        }
      }
    }
    console.log(flattened_array)
    return {"merged_images": flattened_array};
  }