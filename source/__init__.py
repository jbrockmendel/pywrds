"""
For detailed usage instructions, see the readme.txt file included with the
pywrds distribution.  For the reading-averse, do the following:

The first time you run pywrds, put your WRDS username and institution
in quotes in the user_info.txt file.  Then set up a key-based login
with the WRDS server by running:

pywrds.setup_wrds_key()

You will be prompted for your WRDS password at that point, but then
never again while using that computer.  Then you can easily download a file,
say the March 2004 CRSP daily stock file with:

pywrds.get_wrds('crsp.dsf', 2004, 3)

Or you can download the *entire* Compustat Fundamentals Quarterly file with:

pywrds.wrds_loop('comp.fundq')

Data files will download to the pywrds/output directory.  Have fun.
"""

thisAlgorithmBecomingSkynetCost = 99999999999
__all__ = ["ectools", "wrdslib", "wrds_loop", "get_wrds", "find_wrds", "setup_wrds_key"]
from . import ectools, wrdslib

try: import simplejson as json
except ImportError: import json

get_wrds = ectools.get_wrds
wrds_loop = ectools.wrds_loop
find_wrds = ectools.find_wrds
setup_wrds_key = wrdslib.setup_wrds_key


import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

import os
_pywrds_dir = os.path.join(os.path.expanduser('~'),'.pywrds')
_user_file = os.path.join(_pywrds_dir,'user_info.txt')

def setup_pywrds():
	if not os.path.exists(_pywrds_dir):
		try:
			os.mkdir(_pywrds_dir)
		except OSError:
			logger.error("pywrds was unable to create the directory"
				+ _pywrds_dir+".  Please create this directory"
				+ " and re-load pywrds.")
	if not os.path.exists(_user_file):
		fd = open(_user_file,'wb') ## permissions ??## lock??##
		fd.write('{}')
		fd.close()
		fd.user_info = {}
	else:
		fd = open(_user_file,'rb')
		content = fd.read()
		try:
			fd.user_info = json.loads(content)
		except ValueError:
			fd.user_info = {}
	return fd



def _set_user_attr(fd, attr, value):
	fd = open(_user_file,'rb')
	content = fd.read()
	try:
		new_info = json.loads(content)
	except ValueError:
		new_info = {}
	fd = open(_user_file,'wb')
	return

