import csv
import os
import pandas as pd
import re
import sqlite3
import string
import sys

import config

def create_db(drive_path):
    create_table_sql = """CREATE TABLE IF NOT EXISTS files (\
        id INTEGER PRIMARY KEY, \
        path NOT NULL, \
        umoid NOT NULL, \
        resource_type TEXT, \
        migrated_to_rs BOOLEAN, \
        ingest_problem BOOLEAN
        )"""
    insert_sql = "INSERT INTO files (path, umoid) VALUES (?,?)"
    db_path = "files.sqlite"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()

    for root, dirs, files in os.walk(drive_path):
        for file in files:
                umoid = None
                path = os.path.join(root,file)
                try:
                    umoid = re.match(r".+\/u(\d+)",root).group(1)
                except:
                    pass

                if umoid and path:
                    print(path)
                    cursor.execute(insert_sql,(path,umoid))
    conn.commit()

def intake_metadata(metadata_csv,resource_type,conn,cursor):
    # this sql expression gets rid of piction-creatd derivs
    # delete from files where path like '%_o2.jpg' or path LIKE '%_o3.jpg' or path like '%_o4.jpg'
    db_columns = [x[1] for x in cursor.execute("PRAGMA table_info('files');").fetchall()]
    add_column_sql = "ALTER TABLE files ADD COLUMN {} TEXT;"
    select_umoid_sql = "SELECT id FROM files WHERE files.umoid=?"
    update_columns_sql = """
        UPDATE files
        SET {}
        WHERE id=?;
        """

    # read the metadata csv and put it in the db
    with open(metadata_csv,'r') as f:
        df = pd.read_csv(f)
    if not "UMO ID" in df.columns:
        print("Please name the column w/ the  Piction UMO ID 'UMO ID' and try again.")
        sys.exit()
    df.columns = df.columns.str.replace(
        r"["+string.punctuation+","+string.whitespace+"]",
        '_',
        regex=True)

    for column in df.columns:
        if column not in db_columns:
            try:
                cursor.execute(add_column_sql.format(column))
                conn.commit()
            except:
                print(cursor.execute(add_column_sql,(column,)).fetchall())
    for row in df.itertuples():
        item_umo = str(df.loc[row.Index,"UMO_ID"])
        item_id = cursor.execute(select_umoid_sql,(item_umo,)).fetchone()[0]
        values = [("resource_type",resource_type)]
        # skip the first entry which is the Index
        for field in row._fields[1:]:
            value = df.loc[row.Index,field]
            if not pd.isnull(value):
                values.append((field,value))

        # you can't use parameters to insert column names :/
        # so this just updates the column names and values as a formatted str
        values = ", ".join([f"{x}='{y}'" for (x,y) in values])
        update_columns_sql = update_columns_sql.format(values)

        cursor.execute(update_columns_sql,(item_id,))

    conn.commit()

def main():
    '''
    mode:
        new = make a new database reflecting the files on the piction drive; drag
                and drop the root directory of the drive into the command
        intake = read the input metadata csv (exported from piction) and
                add metadata for each row into the database; make columns as
                needed based on csv column headers
    '''
    mode = sys.argv[1]
    print(mode)

    if mode == 'new':
        try:
            drive_path = sys.argv[2]
            if not os.path.isdir(drive_path):
                sys.exit()
        except:
            sys.exit()
        create_db(drive_path)
    elif mode == "intake":
        metadata_csv = sys.argv[2]
        category = sys.argv[3]
        db_path = "files.sqlite"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        resource_type = config.instance_config['mappings'][category]["RESOURCE_TYPE"]
        intake_metadata(metadata_csv,resource_type,conn,cursor)
    else:
        print("what did you do")
        sys.exit()

if __name__=="__main__":
    main()
