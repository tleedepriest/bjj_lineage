"""
This scripts reads html files and extracts data from the files
"""
import sys
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup, Tag, NavigableString
utils_abs_dir_path = Path(
                Path(__file__).parent.resolve().parent) / Path('utils')
sys.path.insert(0, str(utils_abs_dir_path))

from file_utils import get_file_list, get_soup

def get_descendants_of_tag_txt(html, tag='ul'):
    """
    Parameters
    --------------
    html: Path()
        the path to the html file
    """
    tag_txt = []
    soup = get_soup(html)
    # The ul element under Main Achievements has no class tag
    all_ul = soup.find_all(tag, attrs={'class': None})

    # The first two ul tags are ads
    for ul in all_ul[2:]:
        # we really want the <li> tags, as those are the acheivements
        for children in ul.children:
            
            # this will raise attribute error
            if isinstance(children, NavigableString):
                continue
            if isinstance(children, Tag):
                print(children.text)


    return '\n'.join(tag_txt)

def get_all_siblings_tag_txt(html, tag='p'):
    """
    Parameters
    ------------
    html: Path()
        Path object pointing to hrml file

    tag: str
        the name of the tag you want to find. Will find the first
        element and all of its siblings.

    Returns
    ------------
    ' '.join(tag_txt): str
        returns string representing all paragraph txt
        in the html file
    """
    tag_txt = []
    soup = get_soup(html)
    next_sib = soup.find(tag)

    while True:
        try:
            next_sib = next_sib.next_sibling
        except AttributeError: # no more siblings
            break

        # I think this could be improved modeling the
        # function above...I believe this is a Navigatable
        # string

        try:
            name = next_sib.name
        except AttributeError:
            name = ""
        
        if name == tag:
            text = next_sib.text
            tag_txt.append(text)

    return '\n'.join(tag_txt)

def main(html_dir, txt_dir):
    """
    Paramters
    -------------
    html_dir: str
        /generated_data/htmls

    Returns
    -------------
    None
    """     
    htmls = get_file_list(html_dir)
    for html in htmls:
        p_dir = Path(txt_dir) / 'p_tags'
        txt_path = p_dir / Path(Path(html).stem + '_p_tags.txt')
        p_dir.mkdir(parents=True, exist_ok=True)
        #ul = get_descendants_of_tag_txt(html)
        tag_txt = get_all_siblings_tag_txt(html)
        with open(txt_path, 'w') as fh:
            fh.write(tag_txt)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])



