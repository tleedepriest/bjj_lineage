"""
convienence functions for retrieving files and connection
and cursor objects for database
"""
import requests
from pathlib import Path
from bs4 import BeautifulSoup

import pymysql
from configparser import ConfigParser

def get_conn_cursor():
    """
    Creates connection using pymysql and mysql.cfg file
    returns connection and cursor objects
    """
    config = ConfigParser()
    config.read('mysql.cfg')
    # To connect MySQL database
    conn = pymysql.connect(
        host=config['mysqldb']['host'],
        user=config['mysqldb']['user'],
        password =config['mysqldb']['password'],
        db=config['mysqldb']['db'],
        )

    cur = conn.cursor()
    return cur, conn
    #cur.execute("select @@version")
    #output = cur.fetchall()

    # To close the connection
    #conn.close()

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

def download_html(link, output_path):
    """
    link: str
        the url that you want to download

    output_path: str
        the path where the html is written
    """
    response = requests.get(link)
    with open(output_path, 'wb') as fh:
        fh.write(response.content)

