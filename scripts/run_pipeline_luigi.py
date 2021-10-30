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
    def output(self):
        return luigi.LocalTarget('generated_data/bjj_fighter_links.csv')

    def program_args(self):
        return [
                "python", 
                "scripts/scrapers/get_fighter_links_from_bjj_heros.py", 
                "https://www.bjjheroes.com/a-z-bjj-fighters-list",
                self.output().path
                ]

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
    first_name = luigi.Parameter()
    last_name = luigi.Parameter()
    
    def requires(self):
        return CleanFighterLinksCSV()

    def output(self):
        return luigi.LocalTarget(
                f"generated_data/htmls/{self.first_name}_{self.last_name}.html")
        
    def run(self):
        fighter_df = pd.read_csv(self.input().path)
        fighter_df_filtered = fighter_df[(
            df['first_name'] == self.first_name) & (df['last_name'] ==self.last_name )]
        
        # this will fail with first and last name are identical.
        link = fighter_df_filtered['link'].iloc[0]
        response = requests.get(link)
        with open(self.output().path, 'wb') as fh:
            fh.write(response.content)

class TransformHTMLToTxtPTags(luigi.Task):
    
    def requires(self):
        first_names = []
        last_names = []
        for filename in os.listdir("generated_data/htmls"):
            split_name = Path(filename).stem.split("_")
            first_name = split_name[0]
            last_name = split_name[1]
            first_names.append(first_name)
            last_names.append(last_name)

        return [DownloadHTML(first_name, last_name) for 
                first_name, last_name in zip(first_names, last_names)]
    
    def output(self):
        return luigi.LocalTarget('markers/transform_htmlsto_txt_p')

    def run(self):
        transform.transform_htmls_to_txt_p.main(
                Path('generated_data/htmls'),
                Path('transformed_data/txt_section/p_tags'))

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

