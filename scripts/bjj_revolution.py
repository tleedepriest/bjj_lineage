"""
This script extracts data from
https://bjjrevolutionteam.com/our-instructors
to collect instructors/black belts under fabio clemente.
"""
import luigi
from luigi.util import requires

import requests
from pathlib import Path
import pandas as pd
from utils.file_utils import get_soup
from bs4 import BeautifulSoup, Tag, NavigableString

class DownloadHTML(luigi.Task):
    
    url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
                'generated_data/revolution_bjj/bjj_rev_team.html')

    def run(self):
        Path(self.output().path).parent.mkdir(exist_ok=True, parents=True)
        response = requests.get(self.url)
        with open(self.output().path, 'wb') as fh:
            fh.write(response.content)

@requires(DownloadHTML)
class ExtractBlackBelts(luigi.Task):

    def output(self):
        return luigi.LocalTarget(
                'generated_data/revolution_bjj/bjj_rev_black_belts.csv')

    def run(self):
        data = {"entity": [],
                "degree": [],
                "year_awarded": [], 
                "parent": []}

        soup = get_soup(self.input().path)
        
        black_belts = soup.find_all("span", {"style": "font-size: 18px;"})
        for black_belt in black_belts:
            txt = black_belt.get_text()
            # elements with Degree in txt are black belts.
            # elemtents without Degree are certified instructors.
            if "Degree" in txt:
                txt_list = txt.split(',') 
                entity = txt_list[0]
                degree = txt_list[1]
                year_awarded = txt_list[2]
                print(txt_list) 
                if len(txt_list) == 4:
                    parent = txt_list[3]
                else:
                    parent = "Julio Cesar “Foca” Fernandez Nunes"
            
                data["entity"].append(entity)
                data["degree"].append(degree)
                data["year_awarded"].append(year_awarded)
                data["parent"].append(parent)
        pd.DataFrame().from_dict(data=data).to_csv(
                self.output().path, index=False)
if __name__ == "__main__":
    luigi.run()
