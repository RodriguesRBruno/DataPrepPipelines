version development

task prepare {
    input {
    Directory input_data_dir
    Directory labels_path
    File parameters_file
    String? output_dir_name='data'
  }

  Directory output_path = "./~{output_dir_name}"

  runtime {
      container: "mlcommons/chexpert-prep:0.1.0"
  }

  command <<<
    mkdir -p ~{output_path} && python3 /project/mlcube.py prepare --output_path ~{output_path} --data_path ~{input_data_dir} --labels_path ~{labels_path} --parameters_file ~{parameters_file}
  >>>

  output {
    Directory images_dir = "~{output_path}/images"
    File data_csv = "~{output_path}/data.csv"
  }
}
