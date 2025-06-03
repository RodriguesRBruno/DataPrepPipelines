version development
import "chexpert_tasks.wdl"

workflow chexpert {
  input {
    Directory input_data_dir
    Directory labels_path
    File parameters_file
    String? output_dir_name='data'
  }
  call chexpert_tasks.prepare {  # From prepare.wdl
    input:
      input_data_dir=input_data_dir,
      labels_path=labels_path,
      parameters_file=parameters_file,
      output_dir_name=output_dir_name
  }

  output {
    Directory images_dir=prepare.images_dir
    File data_csv=prepare.data_csv
  }
}