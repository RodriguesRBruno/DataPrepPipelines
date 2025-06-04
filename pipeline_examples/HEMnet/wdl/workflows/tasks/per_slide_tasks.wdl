version development

task image_registration {
    input{
        Directory input_data_dir
        File base_normaliser_pkl
        String image_prefix
        Float alignment_magnify=10
        Boolean verbosity=true
    }
    env Directory WORKSPACE_PATH = "."
    env Directory OUTPUT_PATH = "~{WORKSPACE_PATH}/data"
    env Directory INPUT_PATH = input_data_dir
    env Directory TEMP_DATA_PATH = "~{WORKSPACE_PATH}/.tmp"
    
    runtime{
        container: "local/hemnet:0.0.9"
        memory: "3 GiB"
        cpu: 5
    }

    String command_str = <<< 
        python3 /HEMnet/HEMnet/image_registration.py \
        --subject-subdir ~{image_prefix} \
        --align_mag ~{alignment_magnify} \
        --normaliser-path ~{base_normaliser_pkl}
    >>>

    command <<<
      ~{command_str}~{if verbosity then " -v" else ""}
    >>>

    output{
        File performance_csv="~{TEMP_DATA_PATH}/~{image_prefix}/performance_metrics.csv"
        File specific_normaliser="~{TEMP_DATA_PATH}/~{image_prefix}/normaliser.pkl"
        File he_norm_img="~{TEMP_DATA_PATH}/~{image_prefix}/he_norm.npy"
        File he_gray_img="~{TEMP_DATA_PATH}/~{image_prefix}/he_gray.npy"
        File tp53_gray_img="~{TEMP_DATA_PATH}/~{image_prefix}/tp53_gray.npy"
        Array[File]? verbose_images=glob("~{OUTPUT_PATH}/*.jpeg")
    }
}

task affine_registration {
    input{
        Directory input_data_dir
        String image_prefix
        File performance_csv_in
        File he_gray_img
        File tp53_gray_img
        Float alignment_magnify=10
        Boolean verbosity=true
    }
    env Directory WORKSPACE_PATH = "."
    env Directory OUTPUT_PATH = "~{WORKSPACE_PATH}/data"
    env Directory INPUT_PATH = input_data_dir
    env Directory TEMP_DATA_PATH = "~{WORKSPACE_PATH}/.tmp"
    
    runtime{
        container: "local/hemnet:0.0.9"
        memory: "3 GiB"
        cpu: 5
    }

    String command_str = <<< 
            python3 /HEMnet/HEMnet/affine_registration.py \
            --subject-subdir ~{image_prefix} \
            --align_mag ~{alignment_magnify} \
            --performance-csv ~{performance_csv_in} \
            --he-gray ~{he_gray_img} \
            --tp53-gray ~{tp53_gray_img}
    >>>
    
    command <<<
      ~{command_str}~{if verbosity then " -v" else ""}
    >>>

    output{
        File performance_csv="~{TEMP_DATA_PATH}/~{image_prefix}/performance_metrics.csv"
        File moving_resampled_affine="~{TEMP_DATA_PATH}/~{image_prefix}/moving_resampled_affine.npy"
        File affine_transform="~{TEMP_DATA_PATH}/~{image_prefix}/affine_transform.hdf"
        Array[File]? verbose_images=glob("~{OUTPUT_PATH}/*.jpeg")
    }
}

task bspline_registration {
    input{
        Directory input_data_dir
        String image_prefix
        File performance_csv_in
        File he_gray_img
        File he_norm_img
        File tp53_gray_img
        File moving_resampled_affine
        File affine_transform
        Float alignment_magnify=10
        Boolean verbosity=true
    }
    env Directory WORKSPACE_PATH = "."
    env Directory OUTPUT_PATH = "~{WORKSPACE_PATH}/data"
    env Directory INPUT_PATH = input_data_dir
    env Directory TEMP_DATA_PATH = "~{WORKSPACE_PATH}/.tmp"
    
    runtime{
        container: "local/hemnet:0.0.9"
        memory: "3 GiB"
        cpu: 5
    }

    String command_str = <<< 
            python3 /HEMnet/HEMnet/bspline_registration.py \
            --subject-subdir ~{image_prefix} \
            --align_mag ~{alignment_magnify} \
            --performance-csv ~{performance_csv_in} \
            --he-gray ~{he_gray_img} \
            --he-norm ~{he_norm_img} \
            --tp53-gray ~{tp53_gray_img} \
            --moving-affine ~{moving_resampled_affine} \
            --affine-transform ~{affine_transform}
    >>>
    
    command <<<
      ~{command_str}~{if verbosity then " -v" else ""}
    >>>

    output{
        File performance_csv="~{TEMP_DATA_PATH}/~{image_prefix}/performance_metrics.csv"
        File he_filtered="~{TEMP_DATA_PATH}/~{image_prefix}/he_filtered.npy"
        File tp53_filtered="~{TEMP_DATA_PATH}/~{image_prefix}/tp53_filtered.npy"
        Array[File]? verbose_images=glob("~{OUTPUT_PATH}/*.jpeg")
    }
}

task generate_masks {
    input{
        Directory input_data_dir
        String image_prefix
        File he_filtered
        File tp53_filtered
        Float alignment_magnify=10
        Float tile_magnify=2
        Int tile_size=224
        Float cancer_thresh=0.39
        Float non_cancer_thresh=0.40
        Boolean verbosity=true
    }
    env Directory WORKSPACE_PATH = "."
    env Directory OUTPUT_PATH = "~{WORKSPACE_PATH}/data"
    env Directory INPUT_PATH = input_data_dir
    env Directory TEMP_DATA_PATH = "~{WORKSPACE_PATH}/.tmp"
    
    runtime{
        container: "local/hemnet:0.0.9"
        memory: "3 GiB"
        cpu: 5
    }

    String command_str = <<< 
            python3 /HEMnet/HEMnet/generate_masks.py \
            --subject-subdir ~{image_prefix} \
            --he-filtered ~{he_filtered} \
            --tp53-filtered ~{tp53_filtered} \
            --align_mag ~{alignment_magnify} \
            --tile_mag ~{tile_magnify} \
            --tile_size ~{tile_size} \
            --cancer_thresh ~{cancer_thresh} \
            --non_cancer_thresh ~{non_cancer_thresh}
    >>>
    
    command <<<
      ~{command_str}~{if verbosity then " -v" else ""}
    >>>

    output{
        File c_mask_filtered="~{TEMP_DATA_PATH}/~{image_prefix}/c_mask_filtered.npy"
        File non_c_mask_filtered="~{TEMP_DATA_PATH}/~{image_prefix}/non_c_mask_filtered.npy"
        File u_mask_filtered="~{TEMP_DATA_PATH}/~{image_prefix}/u_mask_filtered.npy"
        File t_mask_filtered="~{TEMP_DATA_PATH}/~{image_prefix}/t_mask_filtered.npy"
        Array[File]? verbose_images=glob("~{OUTPUT_PATH}/*.jpeg")
    }
}

task save_tiles {
    input{
        Directory input_data_dir
        String image_prefix
        File performance_csv_in
        File specific_normaliser
        File u_mask_filtered
        File c_mask_filtered
        File t_mask_filtered
        File non_c_mask_filtered
        Float tile_magnify=2
        Int tile_size=224
    }
    env Directory WORKSPACE_PATH = "."
    env Directory OUTPUT_PATH = "~{WORKSPACE_PATH}/data"
    env Directory INPUT_PATH = input_data_dir
    env Directory TEMP_DATA_PATH = "~{WORKSPACE_PATH}/.tmp"
    
    runtime{
        container: "local/hemnet:0.0.9"
        memory: "3 GiB"
        cpu: 5
    }

    String command_str = <<< 
            python3 /HEMnet/HEMnet/save_tiles.py \
            --subject-subdir ~{image_prefix} \
            --performance-csv ~{performance_csv_in} \
            --normaliser-path ~{specific_normaliser} \
            --u-mask ~{u_mask_filtered} \
            --c-mask ~{c_mask_filtered} \
            --non-c-mask ~{non_c_mask_filtered} \
            --t-mask ~{t_mask_filtered} \
            --tile_mag ~{tile_magnify} \
            --tile_size ~{tile_size} 
    >>>
    
    command <<<
      ~{command_str}
    >>>

    output{
        File performance_csv="~{TEMP_DATA_PATH}/~{image_prefix}/performance_metrics.csv"
        Array[File] cancer_tiles = glob("~{OUTPUT_PATH}/tiles_*/cancer/*.jpeg")
        Array[File] non_cancer_tiles = glob("~{OUTPUT_PATH}/tiles_*/non_cancer/*.jpeg")
        Array[File] uncertain_tiles = glob("~{OUTPUT_PATH}/tiles_*/uncertain/*.jpeg")
    }
}