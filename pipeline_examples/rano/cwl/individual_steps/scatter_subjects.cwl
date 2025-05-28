cwlVersion: v1.2
class: CommandLineTool

hints:
  DockerRequirement:
    dockerPull: local/rano-data-prep-cwl:0.0.1

requirements:
  EnvVarRequirement:
    envDef:
      WORKSPACE_DIRECTORY: $(runtime.outdir)
      RAW_DIR: $(inputs.raw_data_dir.path)

inputs:
    raw_data_dir: Directory

baseCommand: get_subject_directories

stdout: output.txt
outputs:
    subject_subdirs: 
        type: stdout