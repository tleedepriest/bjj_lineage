"""
This scripts reads html files and extracts data from the files
"""
import sys
import logging
from pathlib import Path

# from collections import OrderedDict

import pandas as pd

# import html2text

import sqlite3
from bs4 import BeautifulSoup

#def create_db():
#    """
#    """
#    con = sqlite3.connect('generated_data/bjj_heros.db')
#    con.execute('IF personal_info EXISTS DROP TABLE personal_info;')
#    con.execute("""CREATE TABLE personal_info 
#            (file_name PRIMARY KEY, 
#             full_name TEXT, 
#             nickname TEXT, 
#             lineage TEXT);""")
#    return con


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
    # h = html2text.HTML2Text()
    # h.ignore_links = True
    
    #logging.basicConfig(
    #        format='%(message)s', 
    #        filename="logs/structure_fighter_html.log",
    #        filemode='w', 
    #        level=logging.DEBUG)

    #con = create_db()
    htmls = [x for x in Path(html_dir).glob('**/*') if x.is_file()]
    for html in htmls:
        values = {}
        values["file_name"] = Path(html).stem
        p_txt = get_p_txt(html)
        print(p_txt)

                    #if "Full Name:" in text:
                    #    full_name = text.replace("Full Name: ", "")
                    #    values["full_name"] = full_name
                    #elif "Nickname:" in text:
                    #    nickname = text.replace("Nickname: ", "")
                    #    values["nickname"] = nickname
                    
                    # need to use regex to fill in the number here
                    # and then add that to the dictionary.
                    #elif any(
                    #        lin in text for lin in [
                    #            "Lineage:", 
                    #            "Lineage :", 
                    #            "Linage:", 
                    #            "Lineage 1:"]
                    #        ):

                        # this contains all chars in all words above
                        # wont fail if doesn't contain char
                    #    for char in "Lineage 1: ":
                    #        lineage = text.replace(char, "")
                    #    values["lineage"] = lineage

                    #elif "Main Achievements:" in text:
                    #   pass
                        #print(next_sib.next_sibling)
                        #print(next_sib.next_sibling.next_sibling)
        #f not values:
        #   logging.info(f"""There is no nickname, full_name, or lineage\n
        #           in this file: {html}""")

        #elif len(values.keys())!=3:
        #    logging.info(f"""This file: {html} \n
        #                     contains dict without one or more of 
        #                     nickname, full_name, or lineage...\n
        #                     The keys are: \n
        #                     {values.keys()}\n
        #                     The values are:\n 
        #                     {values.values()}""")
        #else:
        #    con.execute("""INSERT INTO personal_info 
        #                   (file_name, full_name, nickname, lineage)
        #                   VALUES 
        #                   (:file_name, :full_name, :nickname, :lineage);""", 
        #                   values)

if __name__ == "__main__":
    main(sys.argv[1])



