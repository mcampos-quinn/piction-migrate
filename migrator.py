import concurrent.futures
import csv
import os
import re
import string
import sys
import threading
import urllib

import pandas as pd
import requests
import sqlite3

import config
import rs_utils

def create_db(drive_path):
    create_table_sql = """CREATE TABLE IF NOT EXISTS files (\
        id INTEGER PRIMARY KEY, \
        path NOT NULL, \
        umoid NOT NULL, \
        category TEXT, \
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

def intake_metadata(metadata_csv,category,resource_type,conn,cursor):
    # this sql expression gets rid of piction-creatd derivs
    # delete from files where path like '%_o2.jpg' or path LIKE '%_o3.jpg' or path like '%_o4.jpg'
    files_not_found = []
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
        print(item_umo)
        try:
            item_id = cursor.execute(select_umoid_sql,(item_umo,)).fetchone()[0]
            print(item_id)
        except:
            files_not_found.append(item_umo)
            continue

        values = [("resource_type",resource_type),("category",category)]
        # skip the first entry which is the Index
        for field in row._fields[1:]:
            value = df.loc[row.Index,field]
            if not pd.isnull(value):
                values.append((field,value))

        # you can't use parameters to insert column names :/
        # so this just updates the column names and values as a formatted str
        values = [(x,str(y).replace("'","''").replace('"','""')) for (x,y) in values]
        print(values)
        values = ", ".join([f'{x}="{y}"' for (x,y) in values])
        update_columns_sql_row = update_columns_sql.format(values)
        print(update_columns_sql_row)
        cursor.execute(update_columns_sql_row,(item_id,))

    conn.commit()

    with open('orphan_files.txt','a') as f:
        f.write("The files with these Piction UMO IDs weren't found on the drive listing:\n")
        for item in files_not_found:
            f.write(item+"\n")

def run_cleaner(limit=0):
    conn,cursor = db_connect()
    # only select rows that were processed during the csv parsing process
    select_rows_sql = "SELECT * FROM FILES "\
        "WHERE ingest_problem LIKE '%debug%' "\
        "AND migrated_to_rs = 'False' "\
        f"LIMIT {limit}"
    rows = cursor.execute(select_rows_sql).fetchall()
    headers = list(map(lambda attr : attr[0], cursor.description))
    results = [{header:row[i] for i, header in enumerate(headers)} for row in rows]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for item in results:
            futures.append(executor.submit(migrate, item))
        for future in concurrent.futures.as_completed(futures):
            try:
                print(future.result())
            except:
                print("Exception")

def run_migrator(limit=0):
    conn,cursor = db_connect()
    # only select rows that were processed during the csv parsing process
    select_rows_sql = "SELECT * FROM FILES "\
        "WHERE category IS NOT NULL "\
        "AND migrated_to_rs IS NULL "\
        "AND ingest_problem IS NULL "\
        f"LIMIT {limit}"
    rows = cursor.execute(select_rows_sql).fetchall()
    headers = list(map(lambda attr : attr[0], cursor.description))
    results = [{header:row[i] for i, header in enumerate(headers)} for row in rows]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for item in results:
            futures.append(executor.submit(migrate, item))
        for future in concurrent.futures.as_completed(futures):
            try:
                print(future.result())
            except:
                print("Exception")
        # for item in results:
        #     worker = threading.Thread(target=migrate,(item,conn,cursor))

def migrate(item):#,conn,cursor):
    conn,cursor = db_connect()
    rs_requester = rs_utils.RSpaceRequest()
    system_fields = ['id','path', 'category', 'umoid', 'resource_type', 'migrated_to_rs', 'ingest_problem']
    item_system_data = {field:value for field,value in item.items() if field in system_fields}
    if ' ' in item_system_data['path']:
        bad_name_sql = """
            UPDATE files
            SET ingest_problem = ?
            WHERE id=?;
            """
        cursor.execute(bad_name_sql,('There is a space in the filename. TBD.',item_system_data['id']))
        conn.commit()
        return
    item_metadata = {
        field:value for field,value in item.items()
        if field not in system_fields
        and value not in ["",None,"None"]
        }
    item_metadata = map_fields(item_metadata,item_system_data['category'])
    mapped_escaped_metadata = rs_utils.prep_resourcespace_JSON(item_metadata)
    path = item_system_data['path']
    path = path.replace('/Volumes',config.instance_config['FILE_SERVER_URL'])
    escaped_filepath = urllib.parse.quote(item_system_data['path'], safe='')
    # print(item_system_data)
    # print(item_metadata)
    # print(mapped_escaped_metadata)
    # print(escaped_filepath)
    rs_requester.rs_api_function="create_resource"
    rs_requester.parameters = f"resource_type={item_system_data['resource_type']}"\
        f"&archive=0"\
        f"&url={path}"\
        f"&autorotate=1"\
        f"&metadata={mapped_escaped_metadata}"
    rs_requester.make_query()
    response = rs_requester.post_query()
    print(response)
    if isinstance(response, (dict,str,int)):
        if not any([x in str(response) for x in ['FAILED','error','Error']]):
            values = [('migrated_to_rs',True),('ingest_problem',None)]
        else:
            values = [('migrated_to_rs',False),('ingest_problem',response)]
    else:
        values = [('migrated_to_rs',False),('ingest_problem',response.text)]

    values = ", ".join([f'{x}="{y}"' for (x,y) in values])
    result_sql = """
        UPDATE files
        SET {}
        WHERE id=?;
        """.format(values)
    print(result_sql)
    cursor.execute(result_sql,(item_system_data['id'],))
    conn.commit()



def map_fields(item_metadata,category):
    metadata = {}
    for k,v in config.instance_config['mappings'][category].items():
        k = re.sub(r"["+string.punctuation+string.whitespace+"]",'_',k)
        if v:
            if k in item_metadata:
                metadata[v] = item_metadata[k]

    return metadata

def db_connect():
    db_path = "files.sqlite"
    conn = sqlite3.connect(db_path)#, check_same_thread=False)
    cursor = conn.cursor()

    return conn,cursor

def quit(message="what did you do, read the instructions"):
    print(message)
    sys.exit()

def main():
    '''
    mode:
        new = make a new database reflecting the files on the piction drive; drag
                and drop the root directory of the drive into the command
        intake = read the input metadata csv (exported from piction) and
                add metadata for each row into the database; make columns as
                needed based on csv column headers
        migrate = migrate the files and metadata to ResourceSpace (set a
                reasonable limit, or use '-1' to run everything)
    '''
    mode = sys.argv[1]
    print(mode)

    if mode == 'new':
        try:
            drive_path = sys.argv[2]
            if not os.path.isdir(drive_path):
                quit(message="the drive path you entered doesn't look valid")
        except:
            quit(message="did you forget to enter the drive path?")
        create_db(drive_path)
    elif mode == "intake":
        metadata_csv = sys.argv[2]
        category = sys.argv[3]
        try:
            conn,cursor = db_connect()
            resource_type = config.instance_config['mappings'][category]["RESOURCE_TYPE"]
        except:
            quit(message="there's a problem w the database, the config settings or both")
        intake_metadata(metadata_csv,category,resource_type,conn,cursor)
    elif mode == "migrate":
        limit = sys.argv[2]
        run_migrator(limit)
    elif mode == "clean":
        limit = sys.argv[2]
        run_cleaner(limit)
    else:
        quit()

if __name__=="__main__":
    main()
