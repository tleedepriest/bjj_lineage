"""
This script will create schema for a lineage_path db, 
read in the clean_lineage_paths, and finally load all of these in a DB.
"""
import sys
from pathlib import Path
import pandas as pd
import sqlite3

def main(csv_to_load, db_path):
    """
    Parameters
    ---------------
    csv_to_load: str
        csv containing individual paths, each entity
        seperated by a comma.

    db_path: str
        the path to the local database.
    """
    con = sqlite3.connect(Path(db_path))
    con.execute("DROP TABLE IF EXISTS lineage")
    con.execute(
            """
            CREATE TABLE lineage(
                entity_id INT,
                parent_id INT,
                path TEXT,
            UNIQUE (entity_id)
            );
            """)
    lienage_paths = pd.read_csv(csv_to_load)
    
if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
