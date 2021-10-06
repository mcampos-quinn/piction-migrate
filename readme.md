# piction migrate

testing out migration from piction to resourcespace

## how

* set up `importer_config.json`
  * includes these items:
  * `rs_base_url`
  * `username` (for the authorized RS user)
  * `api_key` (the user's api key to rs)
  * `mappings`: a dict containing per-category subdicts of csv column headers (piction metadata fields) to RS field IDs. e.g.
  ```
  { "art collection" :
    { "PICTION UMO ID":120 }
  }
  ```
* export/prepare csv per piction collection (headers have to match *at least* what's in the config json dict for that collection)
* run `piction_fixer.py my_category.csv` to add metadata from category names (e.g. "summer fest 2007") in case there's minimal/no user-entered metadata (this will only work with certain collections, i.e. not the art collection)
* run `migrator.py new my_category_enhanced.csv` on a single collection csv. This creates a sqlite database for that collection
* run `migrator.py marry my_category_enhanced.csv` on a single collection csv. This creates a new csv based on `files.sqlite` that also includes filepaths from the piction backup hard drive.
* run `importer.py my_category my_category_married.csv` to post the assets to resourcespace.

## dependencies

`pip3 install requests`
