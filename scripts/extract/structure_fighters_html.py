"""
This scripts reads html files and extracts data from the files
"""
import sys
from collections import OrderedDict
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
import html2text

def get_full_name(soup_tag_name, soup_tag_element):
    """
    Parameters
    ---------------
    soup_tag_name: bs4.tag.name
        
    """


def main(html_dir):
    """
    Paramters
    -------------
    html_dir: str
        /generated_data/htmls

    Returns
    -------------
    None
    """
    fighters_df = pd.DataFrame(
            {"introduction:": [], 
             "full_name:": [],
             "nickname:": [],
             "lineage": []}
            )
            
    h = html2text.HTML2Text()
    #h.ignore_links = True

    htmls = [x for x in Path(html_dir).glob('**/*') if x.is_file()]
    for html in htmls:
        with open(html, 'r') as fh:
            contents = fh.read()
            #print(h.handle(contents))
            soup = BeautifulSoup(contents, features="lxml")
            intro = soup.find('p')
            next_sib = intro.next_sibling
            
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
                    # indicates bolded headers in webpage
                    text = next_sib.text
                    if "Full Name:" in text:
                        full_name = text.replace("Full Name: ", "")
                    elif "Nickname:" in text:
                        nickname = text.replace("Nickname: ", "")
                    elif "Lineage:" in text:
                        lineage = text.replace("Lineage: ", "")
                    elif "Main Achievements:" in text:
                        print(next_sib.next_sibling)
                        print(next_sib.next_sibling.next_sibling)

                
if __name__ == "__main__":
    main(sys.argv[1])



