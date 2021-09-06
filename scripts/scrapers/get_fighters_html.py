"""
This script will read the output generated from 
get_fighters_links_from_bjj_heros.py and make
requests and save the html locally for later
analysis.
"""
from pathlib import Path
import pandas as pd
import requests

def main(fighter_links_csv, html_dir):
    """
    figher_links_csv: str
        path to csv containing figher links from bjj heros

    html_dir: str
        the directory for saving the html files of each fighter
    """
    if not Path(html_dir):
        mkdir(exist_ok=True)
    
    fighter_df = pd.read_csv(fighter_links_csv)
    print(fighter_df)
    for (first_name, 
         last_name, 
         link) in zip(
                fighter_df['first_name'].tolist(),
                fighter_df['last_name'].tolist(),
                fighter_df['link'].tolist()):
            
            # clean names of path characters
            first_name = first_name.replace("/", "").strip()
            last_name = last_name.replace("/", "").strip()

            fighter_html_path =  Path(html_dir) / \
                    Path(f"{first_name}_{last_name}.html")
            
            if not Path(fighter_html_path).is_file():
                response = requests.get(link)
                with open(fighter_html_path, 'wb') as fh:
                    fh.write(response.content)
    
    # write empty file to signify that task has finished.
    with open('markers/get_fighers_html', 'w') as fh:
        fh.write('')

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

