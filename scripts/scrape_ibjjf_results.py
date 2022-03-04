"""
Scrapes HTML files from the links found on
this page

https://ibjjf.com/events/results

and loads the results into a SQL database for subsequent analysis.
"""
import sys
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
from utils.file_utils import download_html, get_soup_from_static_html, get_file_list, get_conn_cursor, get_sqlalchemy_conn
import luigi
from luigi.util import requires

class DownloadResults(luigi.Task):
    """
    Downloads HTML file for IBJJF Results.

    Save file locally for archiving and to be
    kind about making too many requests to server.
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
    """
    Extract all links from the results page and saves links to local
    file.
    """
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
    """
    Downloads the previously scraped links.
    """
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
        return luigi.LocalTarget("generated_data/ibjjf/ibjjf_results.csv")

    def run(self):
        successful_dfs = []
        with_div = 0
        without_div = 0
        htmls = get_file_list("generated_data/ibjjf/events")

        # if division has less than 4 participants, need to pad results
        pad_length = 4

        for num, html in enumerate(htmls):
            division_results = {
                "file_path": [],
                "division": [],
                "place": [],
                "athlete": [],
                "academy": []}
            soup = get_soup_from_static_html(html)
            athletes_section = soup.find(
                'div', attrs={'class': 'col-sm-12 athletes'})
            athletes_section_type_two = soup.find(
                'div', attrs={"class": "col-xs-12 col-md-6 col-athlete"})

            if athletes_section:
                division = athletes_section.find_all(
                    'div', attrs={'class': 'category mt-4 mb-3'})

                if division:
                    with_div+=1
                    for div in division:
                        division_results["file_path"].extend(
                            [str(html)]*pad_length)
                        division_results["division"].extend(
                            [div.text]*pad_length)

                        table = div.findNext('table')

                        place = table.find_all(
                            'td', attrs={'class': 'place'})
                        athlete_name = table.find_all(
                            'div', attrs={'class': 'athlete-name'})
                        academy_name = table.find_all(
                            'div', attrs={'class': 'academy-name'})

                        place = [
                            place.text for place
                            in place]
                        athlete_name = [
                            athlete_name.text for athlete_name
                            in athlete_name]
                        academy_name = [
                            academy_name.text for academy_name
                            in academy_name]

                        place+=[None]*(pad_length-len(place))
                        athlete_name+=[None]*(pad_length-len(athlete_name))
                        academy_name+=[None]*(pad_length-len(academy_name))

                        division_results["place"].extend(place)
                        division_results["athlete"].extend(athlete)
                        division_results["academy"].extend(academy_name)

            elif athletes_section_type_two:
                category = athletes_section_type_two.find_all(
                    'h4', attrs={"class": "subtitle"})
                for cat in category:
                    division_results["division"].extend(
                        [cat.text.strip()]*pad_length)
                    division_results["file_path"].extend(
                        [str(html)]*pad_length)

                    cat_list = cat.findNext('div')
                    athletes = cat_list.find_all(
                        'div', attrs={"class": "athlete-item"})

                    athlete_places = []
                    athlete_academies = []
                    athlete_names = []

                    for athlete in athletes:
                        place = athlete.find(
                            'div', attrs={"class": "position-athlete"})
                        athlete_places.append(place.text.strip())
                        name = athlete.find(
                            'div', attrs={"class":"name"})
                        name_team = name.find('p').text
                        name_team_split = name_team.strip().split('\n')
                        athlete_name = name_team_split[0]
                        team_name = name_team_split[-1].lstrip()
                        athlete_academies.append(team_name)
                        athlete_names.append(athlete_name)
                        #print(team_name)


                    athlete_places+=[None]*(pad_length-len(athlete_places))
                    athlete_names+=[None]*(pad_length-len(athlete_names))
                    athlete_academies+=[None]*(pad_length-len(athlete_academies))

                    division_results["place"].extend(athlete_places)
                    division_results["athlete"].extend(athlete_names)
                    division_results["academy"].extend(athlete_academies)
                with_div+=1

            else:
                without_div+=1
                print(html)

            print(f"with div: {with_div}")
            print(f"without div {without_div}")
            #print(html)

            try:
                df = pd.DataFrame.from_dict(division_results)
                successful_dfs.append(df)
            except ValueError:

                with open("debug.txt", 'a') as fh:
                    fh.write(str(html))
                    fh.write('\n')
        final = pd.concat(successful_dfs)
        final.to_csv(self.output().path, index=False)

@requires(CompileResults)
class LoadResultsIntoDB(luigi.Task):
    """
    This loads the results of the previous file into a database.
    """
    def output(self):
        luigi.LocalTarget('markers/load_ibjjf_results_into_db.marker')
    def run(self):

        # PREPARE THE DATAFRAME TO BE LOADED
        ibjjf_results = pd.read_csv(self.input().path)
        division_cols = ["age_group", "gender", "belt", "weight_group"]
        event_cols = ['year', 'event_name']
        ibjjf_results[division_cols] = ibjjf_results.division.str.split(
            " / ",expand=True)
        ibjjf_results['year'] = ibjjf_results['file_path'].apply(
            lambda x: x.split('/')[-1].split('_')[0])
        ibjjf_results['event_name'] = ibjjf_results['file_path'].apply(
            lambda x: x.split('/')[-1].split('_')[1].split('.')[0])
        print(ibjjf_results['year'])
        print(ibjjf_results['event_name'])
        ibjjf_results.to_csv("ibjjf_results_transformed.csv", index=False)
        # CREATE TEMPORARY TABLE
        conn, cur = get_conn_cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS
                        ibjjf_results
                        (year INT,
                         event_name VARCHAR(200),
                         age_group VARCHAR(100),
                         gender VARCHAR(100),
                         belt VARCHAR(100),
                         weight_group VARCHAR(100),
                         place INT,
                         athlete VARCHAR(200),
                         academy VARCHAR(200));'''
                    )
        conn.close()
        conn = get_sqlalchemy_conn()
        ibjjf_results = ibjjf_results.drop(columns=['file_path', 'division'])
        ibjjf_results.to_sql("ibjjf_results", con=conn, if_exists='append', chunksize=1000, index=False)


if __name__ == "__main__":
    luigi.build(tasks=[CompileResults(), LoadResultsIntoDB()])

