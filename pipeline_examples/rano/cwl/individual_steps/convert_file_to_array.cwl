cwlVersion: v1.2
class: ExpressionTool

requirements: { InlineJavascriptRequirement: {} }

inputs:
  subject_subdirs_file:
    type: File
    inputBinding:
      loadContents: true

expression: |
  ${ return { "subjects_array": JSON.parse(inputs.subject_subdirs_file.contents.replaceAll("'",'"')) }; }

outputs:
  subjects_array: string[]