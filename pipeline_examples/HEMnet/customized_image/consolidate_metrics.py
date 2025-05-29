import pandas as pd
import os
from mod_constants import OUTPUT_PATH, TEMP_DATA_PATH, PERFORMANCE_DF
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--performance-csv-files", default=None, nargs="+", type=str)
    args = parser.parse_args()

    performance_csv_files = args.performance_csv_files
    print(f"performance_csv_files={performance_csv_files}")
    if performance_csv_files is None:
        performance_csv_files = []
        slide_subidrs = [subdir for subdir in os.listdir(TEMP_DATA_PATH)]

        for slide_subdir in slide_subidrs:
            full_subdir = TEMP_DATA_PATH.joinpath(slide_subdir)
            if not os.path.isdir(full_subdir):
                continue
            elif PERFORMANCE_DF not in os.listdir(full_subdir):
                print(
                    f"Performance data not found for slide prefix {slide_subdir}. Will not be included in final metrics."
                )
                continue

            csv_path = os.path.join(full_subdir, PERFORMANCE_DF)
            performance_csv_files.append(csv_path)

    final_df = pd.DataFrame()
    for csv_file in performance_csv_files:
        tmp_df = pd.read_csv(csv_file, encoding="utf-8", index_col=0)
        final_df = pd.concat([final_df, tmp_df], axis=0)

    final_path = OUTPUT_PATH.joinpath(PERFORMANCE_DF)
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    final_df.to_csv(final_path)
