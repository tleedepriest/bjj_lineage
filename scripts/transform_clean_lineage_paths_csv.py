"""
This script will transform the clean_lineage_paths.csv into the format we
want to load into the database
"""
import sys
import pandas as pd
import numpy as np

def get_ent_id_mapping(df_with_lin):
    """
    Parameters
    --------------
    clean_lin_paths: pd.DataFrame()
        pandas DataFrame

    Returns
    --------------
    clean_lin_paths: Dict[str, int]
        Dict[entity, unique_id]
    """
    ent_id_mapping = {}
    unique_id = 0
    entity_ids = []
    parent_ids = []
    entities = []
    for file_path, lineage in zip(
            df_with_lin["file_path"].to_list(),
            df_with_lin["lineage"].astype(str).to_list()
            ):
        # these will go in another DB later on
        if lineage == "no path":
            pass
        else:
            # parent, child, grandchild, ..
            ent_list = lineage.split(",")
            for ind, ent in enumerate(ent_list):
                if ind == 0:
                    prev_ind = None #root
                    prev_ent = None
                else:
                    prev_ind = ind-1
                    prev_ent = ent_list[prev_ind]

                if ent not in ent_id_mapping:
                    ent_id_mapping[ent] = unique_id
                    unique_id+=1
                else:
                    pass

                if ent_id_mapping[ent] not in entity_ids:
                    entity_ids.append(ent_id_mapping[ent])

                if ent_id_mapping[prev_ind] not in parent_ids:
                    parent_ids.append(ent_id_mapping[prev_ind])

                if entity not in entities:
                    entities.append
    entity_df = pd.DataFrame({
        "entity_id": entity_ids,
        "parent_id": parent_ids,
        "entity":entities})

def main(in_path, out_path):
    """
    Parameters
    -------------
    in_path: str
        the path to clean_lineage_paths.csv

    out_path: str
        the path to processed/transformed csv
    """
    df_with_lin = pd.read_csv(in_path)
    #ent_id_mapping = get_ent_id_mapping(df_with_lin=clean_lin_paths)

    ent_id_mapping = {}
    unique_id = 0
    entity_ids = []
    parent_ids = []
    entities = []
    for file_path, lineage in zip(
            df_with_lin["file_path"].to_list(),
            df_with_lin["lineage"].astype(str).to_list()
            ):
        # these will go in another DB later on
        if lineage == "no path":
            pass
        else:
            # parent, child, grandchild, ..
            ent_list = lineage.split(",")
            for ind, ent in enumerate(ent_list):
                if ind == 0:
                    prev_ind = None # root
                    prev_ent = None
                else:
                    prev_ind = ind-1
                    prev_ent = ent_list[prev_ind] # this is the parent id

                if ent not in ent_id_mapping:
                    ent_id_mapping[ent] = unique_id
                    entity_ids.append(ent_id_mapping[ent])
                    entities.append(ent)
                    unique_id+=1
                    if prev_ind == None:
                        parent_ids.append(-1)
                    else:
                        # mapping allows me to convienently access parent_id
                        # here
                        parent_ids.append(ent_id_mapping[prev_ent])
                else:
                    pass

    entity_df = pd.DataFrame({
        "entity_id": entity_ids,
        "parent_id": parent_ids,
        "entity":entities})

    entity_df.to_csv(out_path, index=False)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

