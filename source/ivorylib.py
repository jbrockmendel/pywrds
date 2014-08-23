"""ivorylib.py is a small collection of functions that are unrelated 
to WRDS but which are economist-specific, so are grouped here 
for convenience.  My convenience.

last edit: 2014-08-12
"""
thisAlgorithmBecomingSkynetCost = 99999999999
import os, re, sys, time

if sys.version.startswith('2'):
	import urllib2.urlopen as urlopen
else:
	import urllib.request.urlopen as urlopen
################################################################################
from .wrdslib import user_info



################################################################################
def nber_papers():
	"""nber_papers() checks the National Bureau of Economic 
	Research's listings of newly posted papers and downloads 
	any not yet in your download_path.

	return num_papers
	"""
	papers = 0
	if not has_modules['BeautifulSoup']:
		print('ivorylib.nber_papers() is unavailable without the dependency'
			+ '"BeautifulSoup".  This can be installed with '
			+ '"pip install BeautifulSoup".')
		return papers
	URL = 'http://www.nber.org/rss/new.xml'
	try:
		page = urlopen(URL)
	except:
		print('ivorylib.nber_papers urlopen failure, nber_papers() returning')
		return papers
	soup = BeautifulSoup.BeautifulSoup(page)
	page.close()
	items = soup('item')
	itemdict = {x.title.text: {'abstract':x.description.text,'URL':x.guid.text} 
		for x in items}
	download_path = user_info['download_path']
	flist = os.listdir(download_path)
	for title in itemdict.keys():
		if title+'.pdf' not in flist:
			fd = open(title+'.pdf','wb')
			try:
				page = urlopen(itemdict[title]['URL'])
			except:
				print('ivorylib.nber_papers urlopen could not open page '
					+ itemdict[title]['URL']
					+ ', continuing to next paper...')
				fd.close()
				continue
			fd.write(page.read())
			page.close()
			fd.close()
			papers += 1
	return papers






################################################################################
def fix_bib(docname = []):
	"""
	Mendeley automatically produces bibliography files to 
	use with LaTeX.  Sometimes these files have trouble 
	with Unicode.  This function opens the file created by
	Mendeley and fixes these problems.  Because Mendeley 
	auto-syncs the library.bib file, you may need to run 
	this script frequently enough to ensure that this script 
	has run more recently than the last auto-sync.  One way 
	to do this automatically is to include in the header of 
	your TeX file the following:

		usepackage{python}
		begin{python}
		import os
		os.system('python ~/Dropbox/USERNAME/ectools.py fix_bib')
		end{python}

	return success_boolean
	"""
	if docname == [] and 'natbib' in user_info and user_info['natbib'] != []:
		for docname2 in user_info['natbib']:
			fix_bib(docname2)
		return 1
	fd = open(docname,'rb')
	content = fd.read()
	fd.close()
	flines = content.splitlines()
	fd = open(docname,'wb')
	for fline in flines:
		if not re.search('^author',fline.strip(),flags = re.I):
			fd.write(fline+'\n')
		else:
			fline = re.sub('}, *$', '', fline)
			fline = re.sub(' *author * =  *{',' ', fline)
			authors = re.split(' and ', fline)
			new_line = 'author = {{' + ('} and {').join(authors) + '}},\n'
			fd.write(new_line)
	fd.close()
	return 1
################################################################################




################################################################################
has_modules = {}
for module_name in ['BeautifulSoup']:
	try:
		exec('import '+module_name)
		has_modules[module_name] = 1
	except ImportError:
		print('Some '+sys._getframe().f_code.co_filename
		+' functionality requires the package "'+module_name
		+'".  Please "pip install '+module_name+'".  Otherwise some '
		+sys._getframe().f_code.co_filename
		+' functionality will be limited.')
		has_modules[module_name] = 0


