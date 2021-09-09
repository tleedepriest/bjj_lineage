"""
This scripts reads html files and extracts data from the files
"""
import sys
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup

def get_p_txt(html):
    """
    Parameters
    ------------
    html: Path()
        Path object pointing to hrml file
    
    Returns
    ------------
    ' '.join(p_txt): str
        returns string representing all paragraph txt
        in the html file
    """
    p_txt = []
    with open(html, 'r') as fh:
        contents = fh.read()
        soup = BeautifulSoup(contents, features="lxml")
        next_sib = soup.find('p')

        while True:
            try:
                next_sib = next_sib.next_sibling
            except AttributeError: #no more siblings
                break

            try:
                name = next_sib.name
            except AttributeError:
                name = ""
            
            if name == "p":
                text = next_sib.text
                p_txt.append(text)

    return '\n'.join(p_txt)

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
    htmls = [x for x in Path(html_dir).glob('**/*') if x.is_file()]
    for html in htmls:
        txt_path = Path(txt_dir) / Path(Path(html).stem + '.txt')
        p_txt = get_p_txt(html)

if __name__ == "__main__":
    main(sys.argv[1])



