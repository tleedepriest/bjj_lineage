import sys
import pandas as pd
import lxml
from selenium import webdriver
from bs4 import BeautifulSoup

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
    df.columns=["last_name", "link", "nick_name",
            "link_", "team", "team_link", "first_name", "link__"]

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
    driver.implicitly_wait(5)
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

def main(url):
    """
    Parameters
    --------------
    url: str
        "https://www.bjjheroes.com/a-z-bjj-fighters-list"
    
    Notes
    -----------
    
    """
    # The following code below will only return javascript, not html..this is because the page 
    # calls javascript and hasn't finished loading?
    # response = requests.get("https://www.bjjheroes.com/a-z-bjj-fighters-list")
    # print(response.text)
    data = {}
    driver = webdriver.Firefox()
    contents = get_webpage_contents(url)
    soup = BeautifulSoup(contents, 'lxml')
    tds = soup.find_all('td')
    
    row = []
    for num, td in enumerate(tds):
        
        text = td.text
        link = get_link_from_td_element(td)
        
        row.append(text)
        row.append(link)
        
        # four columns in the table, 
        # so want to create new key, 
        # i.e. subsequent row in df, 
        # per four td elements
        
        row_mod = num%4
        # only correct every 4th number
        possible_row_num = int(num/4)
        if row_mod==0:
            
            row_num = possible_row_num
            # if you dont make a copy, 
            # then get all same values in dictionary!
            data[f"row_{row_num}"] = row.copy()
            
            row.clear()

    df = get_df_from_dict(data)
    rename_df_cols(df)
    drop_redundant_cols(df)
    format_links(df, url)
    
    df.to_csv(sys.stdout)

if __name__ == "__main__":
    main(sys.argv[1])
