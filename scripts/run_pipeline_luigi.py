"""
Writing the script from the Luigi Documentation to build some
muscle memory and test
"""
import luigi
import sys
from pathlib import Path
import scrapers.get_fighter_links_from_bjj_heros
import scrapers.get_fighters_html

# takes in Task class
class GenerateFighterLinksCSV(luigi.Task):
    """
    visits bjjheroes and generates a csv of fighters.
    """
    # define what is being made
    def output(self):
        return luigi.LocalTarget('generated_data/bjj_fighter_links.csv')

    def run(self):
        scrapers.get_fighter_links_from_bjj_heros.main(
                "https://www.bjjheroes.com/a-z-bjj-fighters-list",
                self.output().path)

# different than above, because this will have a
# dependency unlike Generate Words
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
                            Path('generated_data/htmls')
                            )

# extract all the ptags from the html and write them to a seperate txt file
class TransformHTMLToPTags():

#extract the lineage from the text file, perform preprocessing on tags
class ExtractLineageFromPTags():

# load lineage into database using clean_lineage_paths.csv
# create entity-relation db Here
class LoadLineageIntoDataBase():



 
if __name__ == "__main__":
    luigi.run()

