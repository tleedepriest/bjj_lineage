import sys
import pandas as pd
import lxml
from selenium import webdriver
from bs4 import BeautifulSoup

from schema import Schema, Regex
import re

def validate_scraped_table(data):
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

def format_links(df, url):
    """
    link on page is relative to the url of the page
    we visited. Need to reformat to get full link
    """
    df["link"] = df["link"].apply(lambda x: url+x)

def drop_redundant_cols(df):
    """
    multiple values in datafame contain same link
    """
    df.drop(columns=["link_", "link__"], axis=1, inplace=True)

def rename_df_cols(df):
    """
    rename the columns of the dictionary    
    """
    df.columns=["first_name", "link", "last_name",
            "link_", "nick_name", "link__", "team", "team_link"]

def get_df_from_dict(dictionary_of_rows):
    """
    Creates pandas DF and then writed DF to csv
    """
    df = pd.DataFrame.from_dict(dictionary_of_rows, orient='index')
    return df

def get_webpage_contents(page_url):
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

def get_link_from_td_element(td_element):
    "extracts link from td link, if available"
    a_tag = td_element.a
    if a_tag is not None:
        link = a_tag['href']
    else:
        link = 'None'
    return link

def main(url, output_path):
    """
    Parameters
    --------------
    url: str
        "https://www.bjjheroes.com/a-z-bjj-fighters-list"
    
    Notes
    -----------
    
    """
    # The following code below will only return javascript, 
    # not html..this is because the page 
    # calls javascript and hasn't finished loading?
    # response = requests.get("https://www.bjjheroes.com/a-z-bjj-fighters-list")
    # print(response.text)
    data = {}
    driver = webdriver.Firefox()
    contents = get_webpage_contents(url)
    soup = BeautifulSoup(contents, 'lxml')
    tds = soup.find_all('td')
    
    row = []
    num_plus_one = 1
    for num, td in enumerate(tds):
        
        text = td.text
        link = get_link_from_td_element(td)
        
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

    df = get_df_from_dict(data)
    rename_df_cols(df)
    drop_redundant_cols(df)
    format_links(df, url)
    validate_scraped_table(df)
    df.to_csv(output_path)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
