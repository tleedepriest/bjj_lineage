"""
This script will perform a simple text search and create visualization to add
in the extraction of key fields from the text.
"""
import sys

from pathlib import Path
utils_abs_dir_path = Path(
        Path(__file__).parent.resolve().parent) / Path('utils')
sys.path.insert(0, str(utils_abs_dir_path))

from file_utils import get_file_list, get_path_txt

def main(txt_dir):
    """
    txt_dir: str
        the path to the directory you want to search

    out_csv_path: str
        the path of the csv file to produce
    """
    words = ["Lineage 1:", "Lineage:", "Lineage :", "Linage:"]
    txt_files = get_file_list(txt_dir)
    len(txt_files)
    count = 0
    for txt_file in txt_files:
        contents = get_path_txt(txt_file)
        #if words[0] not in contents and words[1] not in contents and words[2] not in contents:
        if words[3] in contents:
            count+=1
            print(txt_file)
    print(count)
    print(len(txt_files))

if __name__ == "__main__":
    main(sys.argv[1])
