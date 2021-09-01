import sys
from pathlib import Path
import scrapers.get_fighter_links_from_bjj_heros
import scrapers.get_fighters_html
from configparser import ConfigParser

def main(config_parser_path):
    
    config = ConfigParser()
    config.read(config_parser_path)
    
    bjj_heros_url = config.get(
            'urls', 
            'bjj_heros')

    bjj_heros_outpath = config.get(
            'output_csvs',
            'bjj_figher_links')
    
    if not Path(bjj_heros_outpath).is_file():
        scrapers.get_fighter_links_from_bjj_heros.main(
                bjj_heros_url,
                bjj_heros_outpath)
    else:
        print(f"{bjj_heros_outpath} has already been made!")

    html_dir = config.get(
            'output_dirs',
            'htmls')

    if not Path(html_dir).is_dir():
        Path(html_dir).mkdir()

    scrapers.get_fighters_html.main(
            bjj_heros_outpath,
            html_dir)

if __name__ == "__main__":
    main(sys.argv[1])
