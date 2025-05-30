#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: ExpressionTool

hints:
  DockerRequirement:
    dockerPull: local/hemnet:0.0.8

requirements:
  InlineJavascriptRequirement: {}

inputs:
  tiles_files:
    type: 
      type: array
      items:
        type: array
        items: [File]

  final_dir_name: string
  tile_magnify:
    type: float
    default: 10
outputs:
  merged_tiles_dir: Directory

expression: |
 ${ 
  var final_dir = {"class": "Directory", "basename": "tiles_"+inputs.tile_magnify+"x/"+inputs.final_dir_name, "listing": inputs.tiles_files.flat(Infinity)}
  return {"merged_tiles_dir": final_dir}
  }