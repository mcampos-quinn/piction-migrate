'''
this little piece of insanity was supposed to catch data entered in folder names
in piction but not in the metadata. it just grabs YEAR and TITLE from the
categories (i.e. folder names from piction) and adds them to the md if it had
been blank
'''
import csv
import re
import sys

def parse_categories(categories):
    parsed = re.match("([^,]+),([\d]+),?([^,]*),?(.*|$)",categories)
    if parsed:
        print(parsed)
        try:
            year = parsed.group(2)
            # print(len(year))
            if len(year) < 4:
                print(categories)
                print(parsed.groups())
                sys.exit
        except:
            year = None
        try:
            title = parsed.group(3)
        except:
            title = None
    else:
        year = title = None

    return year, title

def parse_rows(reader):
    out_rows = []
    for row in reader:
        row["metadata_note"] = ""
        year = row["YEAR"]
        title = row["TITLE"]
        categories = row["CATEGORIES"]
        if not categories.startswith("QA"):
            if any([x in (year,title) for x in ("",None)]):
                year, title = parse_categories(categories)
                row["YEAR"] = year
                row["TITLE"] = title
                row["metadata_note"] = "Note: metadata entered by automated process. Some errors may exist!"
            else:
                pass
        out_rows.append(row)
        # print(row)
    return out_rows

def main():
    path = sys.argv[1]
    with open(path,'r') as f:
        # print(f)
        reader = csv.DictReader(f)
        print(reader.fieldnames)
        out_fieldnames = list(reader.fieldnames)#
        out_fieldnames.append('metadata_note')
        out_rows = parse_rows(reader)
    enhanced_path = path.replace('.csv','_enhanced.csv')
    print(out_fieldnames)
    with open(enhanced_path,'w') as f:
        writer = csv.DictWriter(f,fieldnames=out_fieldnames,extrasaction='ignore')
        writer.writeheader()
        for row in out_rows:
            # print(row)
            writer.writerow(row)

    # write

if __name__ == '__main__':
    main()
