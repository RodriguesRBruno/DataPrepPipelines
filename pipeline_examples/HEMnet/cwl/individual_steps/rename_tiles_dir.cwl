#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: ExpressionTool

inputs:
  tiles_dir: Directory
  image_prefix: string

requirements: 
 InlineJavascriptRequirement: {} 

outputs:
  renamed_tiles_dir: Directory
  

expression: |
 ${ inputs.tiles_dir.basename = inputs.tiles_dir.basename;
    return {"renamed_tiles_dir": inputs.tiles_dir}
 }