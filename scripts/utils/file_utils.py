"""
convienence functions for retrieving files
"""
from pathlib import Path
from bs4 import BeautifulSoup

def get_path_lines(path_obj):
    """
    return lines of text file from path obj
    """
    with path_obj.open() as fh:
        lines = fh.readlines()
        return [line.rstrip() for line in lines]
            


def get_file_list(path_to_dir):
        """
        return all files in a given directory
        """
        return [x for x in Path(path_to_dir).glob("**/*") if x.is_file()]

def get_path_txt(path_obj):
    """
    returns contents of text file from path obj
    """
    with path_obj.open() as fh:
        contents = fh.read()
    return contents

def get_soup(html):
    """
    Parameters
    --------------
    html: Path()
    path object to html file
    open html and convert to soup
    """
    with open(html, 'r') as fh:
        contents = fh.read()
        soup = BeautifulSoup(contents, features="lxml")
    return soup

def get_soup_from_static_html(html):
    """
    Parameters
    ----------------
    html: str/path obj
        the path to the html file

    Returns
    -----------------
    soup: bs4 Soup obj
    """
    with open(html, 'r') as fh:
        soup = BeautifulSoup(fh, 'html.parser')
        #contents = fh.read()
        #soup = BeautifulSoup(contents, features='lxml')
    return soup



