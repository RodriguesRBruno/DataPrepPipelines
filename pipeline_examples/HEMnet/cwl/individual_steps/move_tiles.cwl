#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: CommandLineTool

hints:
  DockerRequirement:
    dockerPull: local/hemnet:0.0.8

baseCommand: mkdir
arguments: ["-p", "$(runtime.outdir)/data/$(inputs.tiles_dir.basename)/$(inputs.image_prefix)", 
"&&", "cp", "$(inputs.tiles_dir.path)"," $(runtime.outdir)/data/$(inputs.tiles_dir.basename)/$(inputs.image_prefix)"]

inputs:
    tiles_dir: Directory
    image_prefix: string
  

outputs:
  moved_tiles_dir:
    type: Directory
    outputBinding:
      glob: "$(runtime.outdir)/data/$(inputs.tiles_dir.basename)/$(inputs.image_prefix)"