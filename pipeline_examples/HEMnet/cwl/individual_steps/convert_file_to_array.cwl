cwlVersion: v1.2
class: ExpressionTool

requirements: { InlineJavascriptRequirement: {} }

inputs:
  image_prefixes_file:
    type: File
    inputBinding:
      loadContents: true

expression: |
  ${ return { "image_prefixes_array": JSON.parse(inputs.image_prefixes_file.contents.replaceAll("'",'"')) }; }

outputs:
  image_prefixes_array: string[]