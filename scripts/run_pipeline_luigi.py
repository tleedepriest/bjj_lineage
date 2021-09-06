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

    def requires(self):
        return GenerateFighterLinksCSV()

    def output(self):
        return luigi.LocalTarget('markers/get_fighters_html')

    def run(self):
        scrapers.get_fighters_html.main(
                            self.input().path,
                            Path('generated_data/htmls')
                            )


if __name__ == "__main__":
    luigi.run()

