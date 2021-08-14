import sys
import scrapers.get_fighter_links_from_bjj_heros
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
    
    scrapers.get_fighter_links_from_bjj_heros.main(
            bjj_heros_url,
            bjj_heros_outpath)

if __name__ == "__main__":
    main(sys.argv[1])
