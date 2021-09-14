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

from file_utils import get_file_list, get_path_txt, get_path_lines


def remove_xao(entity):
    return entity.replace(u'\xa0', u' ')

def clean_entity(entity):
    # remove ( and spaces at beginning
    if not entity[0].isalpha():
        entity = entity[1:]
        entity = entity.lstrip()
    return entity

def strip_accents(s):
       return ''.join(c for c in unicodedata.normalize('NFD', s)
                                 if unicodedata.category(c) != 'Mn')

def invert_mapping(mapping):
    inverted_mapping = {}
    for key, value in mapping.items():
        for ent in value:
            inverted_mapping[ent] = key
    return inverted_mapping


def get_dedupe_mapping(mapping_lines):
    """
    Parameters
    -------------
    mapping_string: str
        string containing contents to produce dedupe mapping dict
    """
    # split on newline, if two blank values occur
    # in a row, then this means that the next value
    # is a key
    mapping = {}
    key = mapping_lines[0]
    mapping[key] = []
    for num, line in enumerate(mapping_lines[1:]):
        # need to shift num over since shifting mapping lins over
        num+=1
        if line == "":
            key = mapping_lines[num+1]
            mapping[key] = []
        else:
            mapping[key].append(line)
    return mapping
        
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
    # Will treat multiple lineages in seperate DB adn file
    # format of lineage below
    # root_name > next_name > next_name
    pattern = re.compile(r"Line?age ?:(.+)\s")
    match = re.search(pattern, txt)
    if match is not None:
        return match.group(1)
    return None

def main(txt_dir, dedupe_mapping_path, outpath):
    """
    Parameters
    -------------
    txt_dir: str
        the directory containing all the txt files

    dedupe_mapping_path: str
        the path to the txt file containing the dedupe mapping

    outpath: str
        the path to the output csv
    
    Returns
    ------------
    None

    Side Effects
    ---------------
    produces csv of extracted contents from txt files
    """
    # each chunk of entities seperated by newline
    mapping_lines = get_path_lines(Path(dedupe_mapping_path))
    mapping_lines = [remove_xao(ent) for ent in mapping_lines]
    
    # Dict[str, List[str, str, ..]]
    mapping = get_dedupe_mapping(mapping_lines)
    # Needed each value in List[str] to be the key
    inverted_mapping = invert_mapping(mapping)
    
    txt_files = get_file_list(txt_dir)
    txt_file_strings = [str(txt_file) for txt_file in txt_files]
    
    # make a list of unique entities so that we can analyze and perform some
    # manual deduplication
    # entities = []
    clean_lin_paths = []
    for txt_file in txt_files:
        txt = get_path_txt(txt_file)
        lin = extract_lineage(txt)
        if lin is not None:
            # child of parent followed by > symbol
            lin_path = lin.split(">")
            clean_lin_path = []
            for entity in lin_path:
                entity = entity.lstrip().rstrip()
                entity = strip_accents(entity)
                entity = clean_entity(entity)
                entity = remove_xao(entity)
                if entity in inverted_mapping.keys():
                    entity = inverted_mapping[entity]
                clean_lin_path.append(entity)
            print(clean_lin_path)
            # for some reason they sometimes skip the root
            if clean_lin_path[0] == "Carlos Gracie Senior":
                clean_lin_path.insert(0, "Mitsuyo Maeda")
            clean_lin_paths.append(', '.join(clean_lin_path))
        else:
            clean_lin_paths.append("no path")
    pd.DataFrame(
            {"file_path": txt_file_strings,
             "lineage": clean_lin_paths}).to_csv(outpath, index=False)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
