"""
Writing the script from the Luigi Documentation to build some
muscle memory and test
"""
import luigi
import sys
from pathlib import Path

import scrapers.get_fighter_links_from_bjj_heros
import scrapers.get_fighters_html

import transform.transform_htmls_to_txt_p
import extract.extract_from_bjj_hero_p_txt
import transform.transform_clean_lineage_paths_csv

class GenerateFighterLinksCSV(luigi.Task):
    """
    visits bjjheroes and generates a csv of fighters.
    """
    def output(self):
        return luigi.LocalTarget('generated_data/bjj_fighter_links.csv')

    def run(self):
        scrapers.get_fighter_links_from_bjj_heros.main(
                "https://www.bjjheroes.com/a-z-bjj-fighters-list",
                self.output().path)

class RequestSaveHTML(luigi.Task):
    """
    reads csv of fighters and writes out html files of each fighter.
    writes out marker file to indicate it is finished with task
    """
    def requires(self):
        return GenerateFighterLinksCSV()

    def output(self):
        return luigi.LocalTarget('markers/get_fighters_html')

    def run(self):
        scrapers.get_fighters_html.main(
                            self.input().path,
                            Path('generated_data/htmls'))

        # write empty file to signify that task has finished.
        with self.output().open('w') as fh:
            fh.write('')

class TransformHTMLToTxtPTags(luigi.Task):
    def requires(self):
        return RequestSaveHTML()
    
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

class TransformLineagePathsToParentChild(luigi.Task):
    def requires(self):
        return ExtractLineageFromPTags()
    
    def output(self):
        return luigi.LocalTarget('transformed_data/entity_parent_lineage_paths.csv')

    def run(self):
        transform.transform_clean_lineage_paths_csv.main(
                self.input().path,
                self.output().path)

# load lineage into database using clean_lineage_paths.csv
# create entity-relation db Here
class LoadLineageIntoDataBase():
    pass

#https://github.com/spotify/luigi/issues/595
class ForceableTask(luigi.Task):
    force = luigi.BoolParameter(significant=False, default=True)
    force_upstream = luigi.BoolParameter(significant=False, default=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.force_upstream is True:
            self.force = True
        if self.force is True:
            done = False
            tasks = [self]
            while not done:
                outputs = luigi.task.flatten(tasks[0].output())
                print(outputs)
                if self.force_upstream is True: tasks+=luigi.task.flatten(tasks[0].requires())
                tasks.pop(0)
                if len(tasks) == 0:
                    done = True

if __name__ == "__main__":
    luigi.run()

