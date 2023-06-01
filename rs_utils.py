# standard library imports
import hashlib
import json
import re
import urllib
# third party imports
import requests
# from requests.utils import requote_uri
# local imports
import config

class RSpaceObject:
	"""defines an object (resource) in resourcespace"""
	def __init__(self,
		rsid=None,
		csid=None,
		metadata=None,
		local_filepath=None,
		filename=None,
		alternative_files=[(None,None)],
		derivative_url=None):
		self.rsid = rsid
		# the corresponding uuid in cspace for the object record related to this resource
		self.csid = csid
		self.metadata = metadata
		self.local_filepath = local_filepath
		self.filename = filename
		# a list of tuples (rsid,local_filepath) for alt files
		self.alternative_files = alternative_files
		self.derivative_url = derivative_url


class RSpaceRequest:
	"""builds a request to rs"""
	def __init__(self,
		rs_api_function=None,
		parameters=None):

		self.status = None
		self.rs_api_function = rs_api_function
		self.parameters = parameters
		self.rs_user = config.instance_config['RS_USER']
		self.rs_userkey = config.instance_config['RS_USERKEY']
		self.rs_url = config.instance_config['RS_URL']
		self.query_url = None

		self.check_status()

	def check_status(self):
		self.rs_api_function = "get_system_status"
		self.make_query()
		response = self.post_query()
		if not response:
			self.status = None
		else:
			self.status = True

	def format_params(self,parameters):
		params = "&".join(["{}={}".format(k,v) for k,v in parameters.items()])
		return params

	def update_field(self,resource_id=None,field_id=None,value=None):
		self.rs_api_function = "update_field"
		self.parameters = self.format_params({
			"resource":resource_id,
			"field":field_id,
			"value":urllib.parse.quote_plus(value)
		})
		self.make_query()
		response = self.post_query()

		return response

	def make_query(self):
		query = "user={}&function={}&{}".format(
			self.rs_user,
			self.rs_api_function,
			self.parameters
			)
		sign = hashlib.sha256(self.rs_userkey.encode()+query.encode())
		sign = sign.hexdigest()
		self.query_url = f"{self.rs_url}/?{query}&sign={sign}"
		print(self.query_url)

	def post_query(self):
		if not self.query_url:
			sys.exit(1) # lol this needs to be less extreme
		response = requests.post(self.query_url)
		# print(response)
		if str(response.status_code).startswith('20'):
			try:
				response = response.json()
			except:
				pass

		return response

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
