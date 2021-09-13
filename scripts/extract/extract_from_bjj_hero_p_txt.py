"""
This script is specifically written to extract the txt from the paragraph
sections of bjj heros txt.
"""
import re
import sys
import pandas as pd
import unicodedata
from pathlib import Path
utils_abs_dir_path = Path(
        Path(__file__).parent.resolve().parent) / Path('utils')
sys.path.insert(0, str(utils_abs_dir_path))

from file_utils import get_file_list, get_path_txt

def strip_accents(s):
       return ''.join(c for c in unicodedata.normalize('NFD', s)
                                 if unicodedata.category(c) != 'Mn')

def extract_lineage(txt):
    """
    Parameters
    -----------
    txt: str
        bjj heros p sections sep by new line

    Returns
    -----------
    match: str
    """
    # found 4 with spelling error missing e
    # If multiple lineages, then will have 1 after it
    # format of lineage below
    # root_name > next_name > next_name
    pattern = re.compile(r"Line?age:(.+)\s|Lineage 1:(.+)\s")
    match = re.search(pattern, txt)
    if match is not None:
        return match.group(1)
    return None

def main(txt_dir, outpath):
    """
    Parameters
    -------------
    txt_dir: str
        the directory containing all the txt files

    outpath: str
        the path to the output csv
    
    Returns
    ------------
    None

    Side Effects
    ---------------
    produces csv of extracted contents from txt files
    """
    txt_files = get_file_list(txt_dir)
    # make a list of unique entities so that we can analyze and perform some
    # manual deduplication
    entities = []
    for txt_file in txt_files:
        txt = get_path_txt(txt_file)
        lin = extract_lineage(txt)
        if lin is not None:
            # child of parent followed by > symbol
            lin_path = lin.split(">")
            for entity in lin_path:
                entity = entity.lstrip().rstrip()
                entity = strip_accents(entity)
                if not entity[0].isalpha():
                    entity = entity[1:]
                    entity = entity.lstrip()
                entities.append(entity)
    entities_deduped = list(set(entities))
    entities_deduped.sort()
    for ent in entities_deduped:
        print(ent)
    pd.DataFrame({"entity": entities_deduped}).to_csv(outpath, index=False)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
