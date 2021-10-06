#!/usr/bin/env python3
# standard library modules
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.parse
# nonstandard libraries
import requests

def format_RS_POST(rs_base_url,query,api_key):
	'''
	Take in the base query,
	add the RS URL,
	sign it w API key,
	return completePOST
	'''
	print(api_key,query)
	sign = hashlib.sha256(api_key.encode()+query.encode())
	# print(dir(sign))
	sign_digest = sign.hexdigest()
	print(sign_digest)
	complete_POST = "{}/api/?{}&sign={}".format(
		rs_base_url,
		query,
		sign_digest
		)
	return complete_POST

def make_RS_API_call(completePOST):
	'''
	This actually does the POST.
	Made via requests.post()
	'''
	try:
		resp = requests.post(completePOST)
		# print(resp.text)
	except ConnectionError as err:
		print("BAD RS POST")
		raise err

	httpStatus = resp.status_code
	print(resp.text)
	if httpStatus == 200:
		return httpStatus,resp.text
	else:
		return httpStatus,None

def resourcespace_API_call(
	quoted_metadata_JSON,
	quoted_path,
	rs_base_url,
	user,
	api_key
	):
	'''
	make a call to the RS create_resource() function
	'''
	query = (
		"user={}"
		"&function=create_resource"
		"&resource_type=1"
		"&archive=0"
		"&url={}"
		"&metadata={}".format(
			user,
			quoted_path,
			quoted_metadata_JSON
			)
		)
	# print(query)
	complete_POST = format_RS_POST(rs_base_url,query,api_key)
	print(complete_POST)
	httpStatus,RSrecordID = make_RS_API_call(complete_POST)
	print(httpStatus)
	print("hey")
	print(RSrecordID)
	# if httpStatus in ('200',200) and not any([x in RSrecordID for x in ("Invalid signature","false")]):
	# 	print("SUCCESS! POSTED THE THING TO RS")
	# 	utils.delete_it(filePath)
	# return RSrecordID

def prep_resourcespace_JSON(row):
	'''
	Prepare URL-escaped JSON for posting to ResourceSpace
	'''

	row_JSON = json.dumps(row,ensure_ascii=False)
	# print(row_JSON)
	quoted_metadata_JSON = urllib.parse.quote_plus(row_JSON.encode())
	if "%5Cn" in quoted_metadata_JSON:
		print("REPLACING NEWLINES")
		quoted_metadata_JSON = quoted_metadata_JSON.replace('%5Cn','%3Cbr%2F%3E')

	return quoted_metadata_JSON

def post_rows(rows,config):
	rs_base_url = config["rs_base_url"]
	user = config["username"]
	api_key = config["api_key"]

	for row in rows:
		quoted_path = urllib.parse.quote(row['FILEPATH'], safe='')
		del row['FILEPATH']
		quoted_metadata_JSON = prep_resourcespace_JSON(row)
		response = resourcespace_API_call(
			quoted_metadata_JSON,
			quoted_path,
			rs_base_url,
			user,
			api_key
			)

def parse_row(row,mapping):
	row_dict = {"FILEPATH":row["FILEPATH"]}
	for k,v in row.items():
		if not v in ("",None):
			try:
				rs_field = mapping[k]
				row_dict[rs_field] = v
			except KeyError:
				pass
	return row_dict

def parse_input_csv(input_csv,config,category):
	mapping = config['mappings'][category]
	rows = []
	with open(input_csv,'r') as f:
		reader = csv.DictReader(f)
		for row in reader:
			row_dict = parse_row(row,mapping)
			# print(row_dict)
			rows.append(row_dict)

	return rows

def main():
	# category should be one of the mappings in `importer_config.json`: art, film, events, etc
	category = sys.argv[1]
	input_csv = sys.argv[2]
	with open('importer_config.json','r') as f:
		config = json.load(f)

	rows = parse_input_csv(input_csv,config,category)
	post_rows(rows,config)

if __name__ == "__main__":
	main()
