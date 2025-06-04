version development

task normaliser_step {
    input{
        Directory input_data_dir
        String normaliser="vahadane"
        Float alignment_magnify=10
        Boolean disable_luminosity_standardisation=false
    }
    env Directory WORKSPACE_PATH = "."
    env Directory OUTPUT_PATH = "~{WORKSPACE_PATH}/data"
    env Directory INPUT_PATH = input_data_dir
    env Directory TEMP_DATA_PATH = "~{WORKSPACE_PATH}/.tmp"
    
    runtime{
        container: "local/hemnet:0.0.9"
    }

    String command_str = "python3 /HEMnet/HEMnet/normaliser_step.py --normaliser ~{normaliser} --align_mag ~{alignment_magnify}"
    
    command <<<
      ~{command_str}~{if disable_luminosity_standardisation then " -std" else ""}
    >>>

    output{
        File base_normaliser_pkl="~{TEMP_DATA_PATH}/normaliser.pkl"
    }
}

task scatter_images {
    input{
        Directory input_data_dir
    }
    env Directory WORKSPACE_PATH = "."
    env Directory OUTPUT_PATH = "~{WORKSPACE_PATH}/data"
    env Directory INPUT_PATH = input_data_dir
    env Directory TEMP_DATA_PATH = "~{WORKSPACE_PATH}/.tmp"
    
    runtime{
        container: "local/hemnet:0.0.9"
    }

    String command_str = "python3 /HEMnet/HEMnet/slides_definition.py -l"
    
    command <<<
      ~{command_str}
    >>>

    output{
        Array[String] image_prefixes = read_lines(stdout())
    }
}

task consolidate_metrics {
    input {
        Array[File] performance_csv_files
    }
    env Directory WORKSPACE_PATH = "."
    env Directory OUTPUT_PATH = "~{WORKSPACE_PATH}/data"
    env Directory TEMP_DATA_PATH = "~{WORKSPACE_PATH}/.tmp"
    
    runtime {
        container: "local/hemnet:0.0.9"
    }
    command <<<
        python /HEMnet/HEMnet/consolidate_metrics.py --performance-csv-files ~{sep(" ",performance_csv_files)}
    >>>

    output {
        File performance_csv="~{OUTPUT_PATH}/performance_metrics.csv"
    }
}