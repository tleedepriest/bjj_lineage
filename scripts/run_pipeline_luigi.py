"""
Writing the script from the Luigi Documentation to build some
muscle memory and test
"""
import os
import sys

from pathlib import Path
import pandas as pd
import requests

import luigi
from luigi.contrib.external_program import ExternalProgramTask

import scrapers.get_fighters_html

import transform.transform_htmls_to_txt_p
import extract.extract_from_bjj_hero_p_txt
import transform.transform_clean_lineage_paths_csv

#https://stackoverflow.com/questions/59842469/luigi-is-there-a-way-to-pass-false-to-a-bool-parameter-from-the-command-line
#https://github.com/spotify/luigi/issues/595
#https://stackoverflow.com/questions/34613296/how-to-reset-luigi-task-status
class ForceableTask(luigi.Task):
    force = luigi.BoolParameter(
            significant=False, 
            default=False,
            parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    force_upstream = luigi.BoolParameter(
            significant=False, 
            default=False,
            parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.force_upstream is True:
            self.force = True
        if self.force is True:
            done = False
            tasks = [self]
            while not done:
                outputs = luigi.task.flatten(tasks[0].output())
                #os.remove(out.path) for out in outputs if out.exists()]
                for out in outputs:
                    if out.exists():
                        print(f"Removing {out.path}!")
                        os.remove(out.path)
                if self.force_upstream is True: 
                    tasks+=luigi.task.flatten(tasks[0].requires())
                tasks.pop(0)
                if len(tasks) == 0:
                    done = True

class GenerateFighterLinksCSV(ExternalProgramTask):
    """
    visits bjjheroes and generates a csv of fighters.
    """
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget('generated_data/bjj_fighter_links.csv')

    def validate_scraped_table(self, data):
        """
        Parameters
        --------------
        data: pd.DataFrame()
            dataframe before validation

        Returns
        --------------
        None

        Side Effects
        -----------------
        enforces simple schema rules to validate data
        """
        scraped = data.to_dict(orient='index')
        
        pattern = r'https://www\.bjjheroes\.com/a-z-bjj-fighters-list/\?p=[\d]+'
        schema = Schema(
                {"link": lambda link: re.match(pattern, link),
                "first_name": lambda fn: len(fn) < 50,
                "last_name": lambda ln: len(ln) < 50,
                "nick_name": object,
                "team_link": object,
                "team": object
                })
        for value in scraped.values():
            schema.validate(value)

    def format_links(self, df, url):
        """
        link on page is relative to the url of the page
        we visited. Need to reformat to get full link
        """
        df["link"] = df["link"].apply(lambda x: url+x)

    def drop_redundant_cols(self, df):
        """
        multiple values in datafame contain same link
        """
        df.drop(columns=["link_", "link__"], axis=1, inplace=True)

    def rename_df_cols(self, df):
        """
        rename the columns of the dictionary    
        """
        df.columns=["first_name", "link", "last_name",
                "link_", "nick_name", "link__", "team", "team_link"]

    def get_df_from_dict(self, dictionary_of_rows):
        """
        Creates pandas DF and then writed DF to csv
        """
        df = pd.DataFrame.from_dict(dictionary_of_rows, orient='index')
        return df

    def get_webpage_contents(self, page_url):
        """opens browser, visits url, waits for javascript to load"""
        driver = webdriver.Firefox()
        driver.get(page_url)
        # wait so the page can load
        driver.implicitly_wait(5)

        # want to scroll on page so the cookies window goes away
        # and doesn't end up in csv
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        contents = driver.page_source
        driver.quit()
        return contents

    def get_link_from_td_element(self, td_element):
        "extracts link from td link, if available"
        a_tag = td_element.a
        if a_tag is not None:
            link = a_tag['href']
        else:
            link = 'None'
        return link
    def run(self):

        data = {}
        driver = webdriver.Firefox()
        contents = self.get_webpage_contents("https://www.bjjheroes.com/a-z-bjj-fighters-list")
        soup = BeautifulSoup(contents, 'lxml')
        tds = soup.find_all('td')
        
        row = []
        num_plus_one = 1
        for num, td in enumerate(tds):
            
            text = td.text
            link = self.get_link_from_td_element(td)
            
            row.append(text)
            row.append(link)
            
            # four columns in the table, 
            # so want to create new key, 
            # i.e. subsequent row in df, 
            # per four td elements
            row_mod = num_plus_one%4
            # only correct every 4th number
            possible_row_num = int(num/4)
            if row_mod==0:
                
                row_num = possible_row_num
                # if you dont make a copy, 
                # then get all same values in dictionary!
                data[f"row_{row_num}"] = row.copy()
                
                row.clear()
            num_plus_one+=1

        df = self.get_df_from_dict(data)
        self.rename_df_cols(df)
        self.drop_redundant_cols(df)
        self.format_links(df, url)
        self.validate_scraped_table(df)
        df.to_csv(self.output().path)

class CleanFighterLinksCSV(luigi.Task):
    """
    Cleans the first and last names inside the csv
    """
    def requires(self):
        return GenerateFighterLinksCSV()

    def output(self):
        return luigi.LocalTarget('generated_data/bjj_fighter_links_clean.csv')

    def run(self):
        df = pd.read_csv(self.input().path)
        df['first_name'] = df['first_name'].astype(str).apply(
                lambda name: name.replace("/", "").strip().replace(" ", "_"))
        df['last_name'] = df['last_name'].astype(str).apply(
                lambda name: name.replace("/", "").strip().replace(" ", "_"))
        df.to_csv(self.output().path)

# https://stackoverflow.com/questions/54701697/how-to-check-output-dynamically-with-luigi
class DownloadHTML(luigi.Task):
    """
    Downloads a single HTML file for a given fighter
    """
    df_index = luigi.IntParameter()
    
    def requires(self):
        return CleanFighterLinksCSV()

    def output(self):
        return luigi.LocalTarget(
                f"generated_data/htmls/{self.df_index}.html")
        
    def run(self):
        fighter_df = pd.read_csv(self.input().path)
        link = fighter_df.iloc[self.df_index]["link"]
        response = requests.get(link)
        with open(self.output().path, 'wb') as fh:
            fh.write(response.content)

class TransformHTMLToTxtPTag(luigi.Task):
    
    df_index = luigi.IntParameter()

    def requires(self):
        return DownloadHTML(self.df_index)
    
    def output(self):
        return luigi.LocalTarget(
                f'transformed_data/txt_section/p_tags/{self.df_index}.txt')

    def run(self):
        with self.output().open('w') as fh:
            fh.write('')

class ExtractLineageFromPTags(luigi.Task): 
    def requires(self):
        return TransformHTMLToTxtPTags()
    
    def output(self):
        return luigi.LocalTarget('transformed_data/clean_lineage_paths.csv')

    def run(self):
        extract.extract_from_bjj_hero_p_txt.main(
                Path('transformed_data/txt_section/p_tags'),
                Path('manual_data/deduplication.txt'),
                self.output().path)

class TransformLineagePathsToParentChild(ForceableTask):
    def requires(self):
        return ExtractLineageFromPTags()
    
    def output(self):
        return luigi.LocalTarget('transformed_data/entity_parent_lineage_paths.csv')

    def run(self):
        transform.transform_clean_lineage_paths_csv.main(
                self.input().path,
                self.output().path)


class RunPipeline(luigi.WrapperTask):
    def requires(self):
        yield GenerateFighterLinksCSV()
        yield RequestSaveHTML()
        yield TransformHTMLToTxtPTags()
        yield ExtractLineageFromPTags()
        yield TransformLineagePathsToParentChild()

# load lineage into database using clean_lineage_paths.csv
# create entity-relation db Here
class LoadLineageIntoDataBase():
    pass


if __name__ == "__main__":
    luigi.run()

