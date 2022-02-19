"""
Convert html2text so I can run some text analytics to understand the data
and structure of each webpage. Running into edge cases while trying to populate
database.
"""
import sys
from pathlib import Path
from bs4 import BeautifulSoup


def main(html_dir, txt_dir, marker_file):
    """
    Parameters
    --------------
    html_dir: str
        contains htmls extracted from bjj_heros -> generated_data/htmls

    txt_dir: str
        script transform htmls to txt files -> transformed_data/html_to_txt

    Returns
    ----------------
    None

    Side Effects
    ---------------
    Writes text file for each html file to txt_dir
    """
    htmls = [x for x in Path(html_dir).glob('**/*') if x.is_file()]
    for html in htmls:
            txt_path = Path(txt_dir) / Path(Path(html).stem + '.txt')
            soup = get_soup_from_static_html(html)
            text = soup.get_text()
            if not txt_path.is_file():
                with open(txt_path, 'w') as fh:
                    fh.write(text)

    # write marker file as dependency for Luigi Pipeline
    with open(marker_file, "w") as fh:
        fh.write("")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
