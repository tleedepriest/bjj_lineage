"""
This script will find entities in the clean_lineage_paths that are possible duplicates
and can be added to the deduplication file in manual data.
"""
import sys
import pandas as pd
def removeDuplicates(lst):
    """
    remove duplicates from matches list in main
    """
    return [t for t in (set(tuple(i) for i in lst))]


def main(clean_lineage_paths_path):
    """
    Parameters
    ------------
    clean_lineage_paths_path: str
        path to csv containing all the clean lineage paths
    """
    possible_duplicates = []
    matches = []
    df = pd.read_csv(clean_lineage_paths_path)
    lineage = df['lineage'].astype(str).tolist()
    for lin in lineage:
        entities = lin.split(',')
        for entity in entities:
            if "." in entity and entity not in possible_duplicates:
                possible_duplicates.append(entity)
        
        for dup in possible_duplicates:
            print(dup)
            name_list = dup.split(' ')
            print(name_list)
            if len(name_list) == 3: #space in front of all names
                last_name = name_list[2]
                print(last_name)
                for entity in entities:
                    if entity not in possible_duplicates and last_name in entity:
                        matches.append((dup, entity))
        
    with open('possible_duplicates.txt', 'w') as fh:
        for possible_duplicate in possible_duplicates:
            fh.write(f'{possible_duplicate}\n')

    matches = removeDuplicates(matches)
    with open('possible_matches.txt', 'w') as fh:
        for dup, match in matches:
            fh.write(f'{match}\n')
            fh.write(f'{dup}\n\n')


if __name__ == "__main__":
    main(sys.argv[1])

