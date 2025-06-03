version development
import "chexpert_tasks.wdl"

workflow chexpert {
  input {
    Directory input_data_dir
    Directory labels_path
    File parameters_file
    String? output_dir_name='data'
  }
  call chexpert_tasks.prepare { 
    input:
      input_data_dir=input_data_dir,
      labels_path=labels_path,
      parameters_file=parameters_file,
      output_dir_name=output_dir_name
  }

  call chexpert_tasks.sanity_check{
    input:
      data_dir=prepare.data_dir,
      parameters_file=parameters_file
  }

  call chexpert_tasks.statistics{
    input:
      data_dir=sanity_check.output_data_dir,
      parameters_file=parameters_file
  }

  output {
    Directory images_dir=prepare.images_dir
    File data_csv=prepare.data_csv
    File statistics_file=statistics.statistics_file
  }
}