version development
workflow chexpert {
  input {
    Directory input_data_dir
    Directory labels_path
    File parameters_file
  }
  call prepare {
    input:
      input_data_dir=input_data_dir,
      labels_path=labels_path,
      parameters_file=parameters_file
  }

  output {
    Directory images_dir=prepare.images_dir
    File data_csv=prepare.data_csv
  }
}

task prepare {
    input {
    Directory input_data_dir
    Directory labels_path
    File parameters_file
    Directory? output_path="./data"
  }

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
