import csv
import os
import re
import sqlite3
import sys

def parse_file_row(row):
    try:
        umoid = re.match("(\.\/[g|h]_drive.*u)([\d]+)(.*)",row).group(2)
        path = row[1:]
    except AttributeError:
        umoid = path = None
    return path,umoid

def parse_metadata_row(row):
    umoid = row[0]
    return umoid

def create_db(drive_path):
    create_table_sql = """create table if not exists files (\
        id integer primary key, \
        path not null, \
        umoid not null)"""
    insert_sql = """insert into files (path, umoid) values (?,?)
    """
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

    # with open(file_csv,'r') as f:
    #     reader = csv.reader(f)
    #     for row in reader:
    #         print(row)
    #         path,umoid = parse_file_row(row[0])
    #         if path and umoid:
    #             cursor.execute(insert_sql,(path,umoid))
    conn.commit()

def marry_files(metadata_csv,conn,cursor):
    out_rows = []
    # this sql expression gets rid of dupe derivs
    # select path from files where path not like '%_o2.jpg' and path not LIKE '%_o3.jpg' and path not like '%_o4.jpg'
    select_umoid_sql = "select path from files where umoid=?;"
    with open(metadata_csv,'r') as f:
        h_reader = csv.DictReader(f)
        headers = h_reader.fieldnames
        headers.insert(0,'FILEPATH')
        out_rows.append(headers)

        reader = csv.reader(f)
        for row in reader:
            umoid = parse_metadata_row(row)
            print(umoid)
            path = cursor.execute(select_umoid_sql,(umoid,)).fetchone()
            if path:
                row.insert(0,path[0])
                out_rows.append(row)
            else:
                row.insert(0,path)
                out_rows.append("NO MATCH")
    with open('out.csv','w') as f:
        writer = csv.writer(f)
        for row in out_rows:
            writer.writerow(row)

def main():
    '''
    mode:
        new = make a new database from the input file listing csv
        marry = marry the input metadata csv (exported from piction) to the relevant file paths
                on the piction file export drive & output a new csv
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
    elif mode == "marry":
        metadata_csv = sys.argv[2]
        db_path = "files.sqlite"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        marry_files(metadata_csv,conn,cursor)
    else:
        print("what did you do")
        sys.exit()

if __name__=="__main__":
    main()
