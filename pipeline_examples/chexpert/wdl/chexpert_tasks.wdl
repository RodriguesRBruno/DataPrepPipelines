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
    Directory data_dir = "~{output_path}"
  }
}

task sanity_check {
  input {
    Directory data_dir
    File parameters_file
  }

  runtime {
      container: "mlcommons/chexpert-prep:0.1.0"
  }

  command <<<
      python3 /project/mlcube.py sanity_check --data_path ~{data_dir} --parameters_file ~{parameters_file}
  >>>

  output {
    Directory output_data_dir = "~{data_dir}"
  }
}

task statistics {
  input {
    Directory data_dir
    File parameters_file
    String? statistics_file_name='statistics.yaml'
  }

  runtime {
      container: "mlcommons/chexpert-prep:0.1.0"
  }
  
  File base_statistics_file="./~{statistics_file_name}"
  command <<<
      python3 /project/mlcube.py statistics --data_path ~{data_dir} --parameters_file ~{parameters_file} --output_path ~{base_statistics_file}
  >>>

  output {
    File statistics_file=base_statistics_file
  }
}


