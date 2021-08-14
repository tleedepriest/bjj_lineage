"""
This script will read the output generated from 
get_fighters_links_from_bjj_heros.py and make
requests and save the html locally for later
analysis.
"""
import pandas as pd
import requests

def main(fighter_links_csv, html_dir):
    """
    figher_links_csv: str
        path to csv containing figher links from bjj heros

    html_dir: str
        the directory for saving the html files of each fighter
    """
    fighter_df = pd.read_csv(fighter_links_csv)

