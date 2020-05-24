import sqlite3
import pandas as pd
import argparse
import os

#1   1   \N  2   rs57067126
#2   1   \N  2   rs58656435
#3   1   \N  2   rs117086615
#4   3   \N  2   rs60836110
#5   14  \N  2   rs386484535
#6   20  \N  2   rs4646963
#7   20  \N  2   rs56030631
#8   20  \N  2   rs62627090
#9   20  \N  2   rs562091107


SCHEMA = """
    CREATE TABLE IF NOT EXISTS variation_synonym (
    variation_id int(10)  NOT NULL,
    name varchar(255) DEFAULT NULL
    );
    """

class sqlClient():
    def __init__(self, database):
        self.database = database
        self.conn = self.create_conn()
        self.cur = self.conn.cursor()
        if self.conn:
            self.create_table()

    def create_conn(self):
        try:
            conn = sqlite3.connect(self.database)
            return conn
        except NameError as e:
            print(e)
        return None

    def create_table(self):
        self.cur.executescript(SCHEMA)

    def commit(self):
        self.cur.execute("COMMIT")

    def drop_indices(self):
        self.cur.execute("DROP INDEX IF EXISTS rsid_idx")
        self.cur.execute("DROP INDEX IF EXISTS syn_idx")

    def create_indices(self):
        self.cur.execute("CREATE INDEX rsid_idx on variation_synonym (name)")
        self.cur.execute("CREATE INDEX syn_idx on variation_synonym (variation_id)")


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of synonyms file to be processed', required=False)
    argparser.add_argument('-id_col', help='column number of id column (zero-based)', required=False)
    argparser.add_argument('-name_col', help='column number of name column (zero-based)', required=False)
    argparser.add_argument('-index', help='index the database (do this after loading all the data)', action='store_true', default='store_false', required=False)
    argparser.add_argument('-db', help='The name of the database to load to', required=True)
    args = argparser.parse_args()
    db = args.db
    id_col = int(args.id_col) if args.id_col else None
    name_col = int(args.name_col) if args.name_col else None

    if args.f:
        syns = args.f 
        synsdf = pd.read_csv(syns, sep='\t', 
                            header=None, 
                            dtype={id_col: int, name_col: str},
                            usecols=[id_col, name_col],
                            chunksize=1000000
                            )
            
        sql = sqlClient(db)
        sql.drop_indices()
        sql.cur.execute('BEGIN TRANSACTION')
        count = 1
        for chunk in synsdf:
            print(count)
            list_of_tuples = list(chunk.itertuples(index=False, name=None))
            sql.cur.executemany("insert into variation_synonym (variation_id, name) values (?, ?)", list_of_tuples)
            count += 1
        sql.cur.execute('COMMIT')
    if args.index:
        sql = sqlClient(db)
        sql.drop_indices()
        sql.create_indices()
    else:
        print("nothing to do")


if __name__ == '__main__':
    main()
