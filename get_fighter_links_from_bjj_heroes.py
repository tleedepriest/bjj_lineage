import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup

def main():
    # The following code below will only return javascript, not html..this is because the page 
    # calls javascript and hasn't finished loading?
    # response = requests.get("https://www.bjjheroes.com/a-z-bjj-fighters-list")
    # print(response.text)
    driver = webdriver.Firefox()
    url = "https://www.bjjheroes.com/a-z-bjj-fighters-list"
    driver.get(url)
    driver.implicitly_wait(0)
    driver.
    print(driver.page_source)

if __name__ == "__main__":
    main()
