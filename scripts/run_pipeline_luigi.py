"""
Writing the script from the Luigi Documentation to build some
muscle memory and test
"""
import os
import sys
import re
import unicodedata
from pathlib import Path
import pandas as pd
import requests
import lxml
from selenium import webdriver
from bs4 import BeautifulSoup, Tag, NavigableString

from schema import Schema, Regex
import luigi

from luigi.contrib.external_program import ExternalProgramTask
import scrapers.get_fighters_html
import transform.transform_htmls_to_txt_p
import extract.extract_from_bjj_hero_p_txt
import transform.transform_clean_lineage_paths_csv


from utils.file_utils import get_file_list, get_soup, \
        get_path_lines, get_path_txt

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
        url = "https://www.bjjheroes.com/a-z-bjj-fighters-list"
        contents = self.get_webpage_contents(url)
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

# could use this task to perform more cleaning functions on the individual
# columns
class CleanFighterLinksCSV(luigi.Task):
    """
    Cleans the first and last names inside the csv
    """
    
    def requires(self):
        return GenerateFighterLinksCSV()

    def output(self):
        return luigi.LocalTarget('generated_data/bjj_fighter_links_clean.csv')

    def clean_name(self, name):
        return name.replace("/", "").strip().replace(" ", "_")
    
    def run(self):
        
        df = pd.read_csv(self.input().path)
        
        df['first_name'] = df['first_name'].astype(str).map(
                self.clean_name)
        df['last_name'] = df['last_name'].astype(str).map(
                self.clean_name)
        
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
        Path(self.output().path).parent.mkdir(exist_ok=True, parents=True)
        fighter_df = pd.read_csv(self.input().path)
        link = fighter_df.iloc[self.df_index]["link"]
        response = requests.get(link)
        with open(self.output().path, 'wb') as fh:
            fh.write(response.content)

class TransformHTMLToPTagTxt(luigi.Task):
    """
    This extracts all the ptags from the html files, which contains
    information we want like lineage and more.
    """
    df_index = luigi.IntParameter()

    def requires(self):
        return DownloadHTML(self.df_index)
    
    def output(self):
        return luigi.LocalTarget(
                f'transformed_data/txt_section/p_tags/{self.df_index}.txt')

    def get_all_siblings_tag_txt(self, html, tag='p'):
        """
        Parameters
        ------------
        html: Path()
            Path object pointing to hrml file

        tag: str
            the name of the tag you want to find. Will find the first
            element and all of its siblings.

        Returns
        ------------
        ' '.join(tag_txt): str
            returns string representing all paragraph txt
            in the html file
        """
        tag_txt = []
        soup = get_soup(html)
        next_sib = soup.find(tag)

        while True:
            try:
                next_sib = next_sib.next_sibling
            except AttributeError: # no more siblings
                break

            # I think this could be improved modeling the
            # function above...I believe this is a Navigatable
            # string

            try:
                name = next_sib.name
            except AttributeError:
                name = ""
            
            if name == tag:
                text = next_sib.text
                tag_txt.append(text)

        return '\n'.join(tag_txt)
    
    def run(self):
        Path(self.output().path).parent.mkdir(exist_ok=True, parents=True)
        tag_text = self.get_all_siblings_tag_txt(self.input().path)
        with self.output().open('w') as fh:
            fh.write(tag_text)

class ExtractLineageFromPTags(luigi.Task):
    """
    this extracts the lineage from the PTags AND deduplicates names
    AND cleans names...should probably break this task up into a few more.
    """
    clean_fighters_path = luigi.Parameter()

    def requires(self): 
        df = pd.read_csv(self.clean_fighters_path)
        return [TransformHTMLToPTagTxt(df_index) for df_index in
                df.index.to_numpy()]

    
    def output(self):
        return luigi.LocalTarget('transformed_data/clean_lineage_paths.csv')

    def remove_xao(self, entity):
        return entity.replace(u'\xa0', u' ')

    def clean_entity(self, entity):
        # remove ( and spaces at beginning
        if not entity[0].isalpha():
            entity = entity[1:]
            entity = entity.lstrip()
        return entity

    def strip_accents(self, s):
           return ''.join(c for c in unicodedata.normalize('NFD', s)
                                     if unicodedata.category(c) != 'Mn')

    def invert_mapping(self, mapping):
        inverted_mapping = {}
        for key, value in mapping.items():
            for ent in value:
                inverted_mapping[ent] = key
        return inverted_mapping


    def get_dedupe_mapping(self, mapping_lines):
        """
        Parameters
        -------------
        mapping_string: str
            string containing contents to produce dedupe mapping dict
        """
        # split on newline, if two blank values occur
        # in a row, then this means that the next value
        # is a key
        mapping = {}
        key = mapping_lines[0]
        mapping[key] = []
        for num, line in enumerate(mapping_lines[1:]):
            # need to shift num over since shifting mapping lins over
            num+=1
            if line == "":
                key = mapping_lines[num+1]
                mapping[key] = []
            else:
                mapping[key].append(line)
        return mapping
    
    def extract_lineage(self, txt):
        """
        Parameters
        -----------
        txt: str
            bjj heros p sections sep by new line

        Returns
        -----------
        match: str
        """
        # found 4 with spelling error missing e
        # Will treat multiple lineages in seperate DB adn file
        # format of lineage below
        # root_name > next_name > next_name
        pattern = re.compile(r"Line?age ?:(.+)\s")
        match = re.search(pattern, txt)
        if match is not None:
            return match.group(1)
        return None
    
    def run(self): 
        Path(self.output().path).parent.mkdir(exist_ok=True)
        
        # each chunk of entities seperated by newline
        mapping_lines = get_path_lines(Path('manual_data/deduplication.txt'))
        mapping_lines = [self.remove_xao(ent) for ent in mapping_lines]
        
        # Dict[str, List[str, str, ..]]
        mapping = self.get_dedupe_mapping(mapping_lines)
        # Needed each value in List[str] to be the key
        inverted_mapping = self.invert_mapping(mapping)
        
        txt_files = get_file_list('transformed_data/txt_section/p_tags')
        txt_file_strings = [str(txt_file) for txt_file in txt_files]
        
        # make a list of unique entities so that we can analyze and perform some
        # manual deduplication
        # entities = []
        clean_lin_paths = []
        for txt_file in txt_files:
        
            txt = get_path_txt(txt_file)
            lin = self.extract_lineage(txt)
            if lin is not None:
                # child of parent followed by > symbol
                lin_path = lin.split(">")
                clean_lin_path = []
                for entity in lin_path:
                    entity = entity.lstrip().rstrip()
                    entity = self.strip_accents(entity)
                    entity = self.clean_entity(entity)
                    entity = self.remove_xao(entity)
                    if entity in inverted_mapping.keys():
                        entity = inverted_mapping[entity]
                    clean_lin_path.append(entity)
                print(clean_lin_path)
                # for some reason they sometimes skip the root
                if clean_lin_path[0] == "Carlos Gracie Senior":
                    clean_lin_path.insert(0, "Mitsuyo Maeda")
                clean_lin_paths.append(', '.join(clean_lin_path))
            else:
                clean_lin_paths.append("no path")
        pd.DataFrame(
                {"file_path": txt_file_strings,
                 "lineage": clean_lin_paths}).to_csv(
                         self.output().path, index=False)

class TransformLineagePathsToParentChild(ForceableTask):
    def requires(self):
        return ExtractLineageFromPTags()
    
    def output(self):
        return luigi.LocalTarget('transformed_data/entity_parent_lineage_paths.csv')

    def run(self):
        transform.transform_clean_lineage_paths_csv.main(
                self.input().path,
                self.output().path)

#https://stackoverflow.com/questions/48418169/run-taska-and-run-next-tasks-with-parameters-that-returned-taska-in-luigi
class RunAll(luigi.WrapperTask):
    
    def complete(self):
        return self.output().exists()
    
    def output(self):
        return luigi.LocalTarget('RunAll.marker')

    def requires(self):
        return CleanFighterLinksCSV()

    def run(self):
        df = pd.read_csv(self.input().path)
        for index in df.index.to_numpy():
            yield DownloadHTML(index)
            yield TransformHTMLToPTagTxt(index)
        
        yield ExtractLineageFromPTags(self.input().path)
        # use this to signify that task is complete.
        with open('RunAll.marker', 'w') as fh:
            fh.write('')
# load lineage into database using clean_lineage_paths.csv
# create entity-relation db Here
class LoadLineageIntoDataBase():
    pass

if __name__ == "__main__":
    luigi.run()

