"""
scrapes results from ibjjf
"""
import sys
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
from utils.file_utils import download_html, get_soup_from_static_html, get_file_list

import luigi
from luigi.util import requires

class DownloadResults(luigi.Task):
    """
    Downloads HTML file for IBJJF Results
    """
    def requires(self):
        return None

    def output(self):
        ibjjf_dir = Path("generated_data/ibjjf")
        ibjjf_dir.mkdir(exist_ok=True, parents=True)
        download_path = ibjjf_dir / "ibjjf_results.html"
        return luigi.LocalTarget(download_path)

    def run(self):
        download_html("https://ibjjf.com/events/results",
                      self.output().path)

@requires(DownloadResults)
class ExtractResultsLinks(luigi.Task):

    def output(self):
        return luigi.LocalTarget("generated_data/ibjjf/result_links.csv")

    def run(self):
        data_dict = {"year": [],
                     "event": [],
                     "link": []}

        soup = get_soup_from_static_html(self.input().path)
        for a_el in soup.find_all("a",
                                  attrs={"class": "event-year-result"}):
            year = a_el["data-y"]
            event = a_el["data-n"]
            link = a_el["href"]
            data_dict["year"].append(year)
            data_dict["event"].append(event)
            data_dict["link"].append(link)

        df = pd.DataFrame().from_dict(data_dict)
        df.to_csv(self.output().path, index=False)


class DownloadResultFromResultLink(luigi.Task):

    link = luigi.Parameter()
    year = luigi.IntParameter()
    event = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            f"generated_data/ibjjf/events/{self.year}_{self.event}.html")

    def run(self):
        download_html(self.link, self.output().path)



@requires(ExtractResultsLinks)
class ExtractAllResultLink(luigi.Task):

    def output(self):
        return luigi.LocalTarget("markers/ibbjjf_events_downloaded.marker")

    def run(self):
        df = pd.read_csv(self.input().path)

        for year, event, link in zip(df["year"].tolist(),
                                     df["event"].tolist(),
                                     df["link"].tolist()):
            yield DownloadResultFromResultLink(link, year, event)

        with open(self.output().path, 'w') as fh:
            fh.write("")

@requires(ExtractAllResultLink)
class CompileResults(luigi.Task):
    def output(self):
        return luigi.LocalTarget("generated_data/ibjjf/compiled_results.csv")

    def run(self):
        with_div = 0
        without_div = 0
        htmls = get_file_list("generated_data/ibjjf/events")
        for html in htmls:
            soup = get_soup_from_static_html(html)
            div_elements = soup.find_all('div',
                                         attrs={'class': 'category mt-4 mb-3'})
            category = soup.find_all('h4', attrs={"class": "subtitle"})
            if div_elements:
                with_div+=1
                for div in div_elements:
                    #print(div)
                    pass
            elif category:
                for cat in category:
                    print(cat.text)
                    cat_list = cat.findNext('div')
                    athletes = cat_list.find_all('div', attrs={"class": "athlete-item"})
                    for athlete in athletes:
                        position_athlete = athlete.find('div', attrs={"class":
                                                                    "position-athlete"})
                        name = athlete.find('div', attrs={"class":"name"})
                        name_p = name.find('p').text
                        team = name.find('span').text
                        #if position_athlete:
                        print(position_athlete.text)
                        print(name_p)
                        #print(team)
                with_div+=1

            else:
                without_div+=1
                print(html)

            print(f"with div: {with_div}")
            print(f"without div {without_div}")
        #<a target="_blank" class="event-year-result" data-n="Curitiba Spring International Open IBJJF Jiu-Jitsu No-Gi Championship" data-y="2018" href="https://www.ibjjfdb.com/ChampionshipResults/1011/PublicResults">2018</a>


if __name__ == "__main__":
    luigi.run()

