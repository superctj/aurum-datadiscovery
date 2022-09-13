import os
import shutil

import pandas as pd


delimiter_literal_map = {",": "colon", ";": "semicolon", "\\t": "tab"}


if __name__ == "__main__":
    testbed = "testbedL"
    root_dir = "/data/nextiaJD_datasets/"

    dataset_dir = os.path.join(root_dir, f"{testbed}/datasets/")
    output_parent_dir = os.path.join(root_dir, f"aurum_only/{testbed}/")

    metadata_path = os.path.join(root_dir, f"{testbed}/datasetInformation_{testbed}.csv")
    metadata = pd.read_csv(metadata_path)
    
    for _, row in metadata.iterrows():
        delimiter = row["delimiter"]
        delimiter_literal = delimiter_literal_map[delimiter]
        
        output_dir = os.path.join(output_parent_dir, delimiter_literal)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        filename = row["filename"]
        file_path = os.path.join(dataset_dir, filename)
        shutil.copy2(file_path, output_dir) 


    



