"""
This script will perform a simple text search and create visualization to add
in the extraction of key fields from the text.
"""
import sys
import pandas as pd
from collections import defaultdict
from pathlib import Path
utils_abs_dir_path = Path(
        Path(__file__).parent.resolve().parent) / Path('utils')
sys.path.insert(0, str(utils_abs_dir_path))

from file_utils import get_file_list, get_path_txt

def main(txt_dir, out_csv_path):
    """
    txt_dir: str
        the path to the directory you want to search

    out_csv_path: str
        the path of the csv file to produce
    """
    # include common word for sanity check
    # and to make sure same as number of files
    words_to_count = [
            "Lineage 1:", 
            "Lineage:", 
            "Lineage :", 
            "Linage:",
            "Full Name:",
            "Nickname:",
            "Full Name :",
            "Nickname :",
            "and",
            "Lineage 2:",
            "Lineage 3:",
            "Lineage 4:"]

    txt_files = get_file_list(txt_dir)
    tally_occurence = defaultdict(int)
    print(len(txt_files))
    for txt_file in txt_files:
        print(txt_file)
        contents = get_path_txt(txt_file)
        for word in words_to_count:
            if word in contents:
                tally_occurence[word]+=1
            else:
                tally_occurence[word]+=0
    print(tally_occurence)
    df = pd.DataFrame(tally_occurence.items(), 
            columns=["word", "count_across_corpus"])
    df.to_csv(out_csv_path, index=False)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
