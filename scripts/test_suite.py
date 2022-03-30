"""
Test suite to make sure we are scraping the results we want
"""
from datetime import date
import unittest
import pandas as pd
import pandera as pa
import luigi
from scrape_ibjjf_results import ExtractResultsLinks
from utils.file_utils import get_soup_from_static_html

class TestExtractAllLinks(unittest.TestCase):
    """
    Testing functionality of extracting all links
    from ibjjf results page.
    """
    bad_input = ("generated_data/ibjjf/events/"
                 "1996_Pan Jiu-Jitsu IBJJF Championship.html")

    schema = pa.DataFrameSchema({
        "year": pa.Column(int, checks=pa.Check.in_range(1994, 2022)),
        "link": pa.Column(str, checks=[pa.Check.str_startswith("http"),
                                       pa.Check.str_contains("ibjjf")])
    })

    def test_it_creates_empty_df_for_irrelevant_html_file(self):
        self.extract_results_links = ExtractResultsLinks(date(2999, 12, 31))
        #worker.add(self.extract_results_links)
        #worker.run()
        soup = get_soup_from_static_html(self.bad_input)
        df = self.extract_results_links.get_results_links(soup)
        #df = pd.read_csv(output)
        df_cols = ['year', 'event', 'link']
        self.assertEqual(list(df), df_cols)
        self.assertEqual(len(df), 0)

    def test_run_validate_data(self):
        """
        Run the download and validate the results
        """
        worker = luigi.worker.Worker()
        self.extract_results_links = ExtractResultsLinks()
        worker.add(self.extract_results_links)
        worker.run()
        output = self.extract_results_links.output().path
        df = pd.read_csv(output)
        df_cols = ['year', 'event', 'link']
        self.assertEqual(list(df), df_cols)
        # only new links should be added as time progresses
        self.assertGreaterEqual(len(df), 1054)
        validated_df = self.schema(df)
        Path(self.extract_results_links).unlink()


if __name__ == "__main__":
    unittest.main()
