thisAlgorithmBecomingSkynetCost = 99999999999
import os, sys
################################################################################

#def up2date(ssh=[], sftp=[]):

_crsp_dsets = ['crsp.msf', 'crsp.dsf']
def get_crsp():
	get_wrds(dataset='crsp.stocknames', Y='all')
	get_wrds(dataset='crsp.dsedelist', Y='all')
	get_wrds(dataset='crsp.msedelist', Y='all')
	get_wrds(dataset='crsp.dsedist', Y='all')
	get_wrds(dataset='crsp.msedist', Y='all')
	for dset in _crsp_dsets:
		wrds_loop(dataset=dset, min_date=0, recombine=1)
	return

_comp_dsets = ['comp.fundq', 'comp.g_fundq', 'comp.idx_daily', 'comp.exrt_dly']
def get_compustat():
	get_wrds(dataset='comp.company', Y='all')
	get_wrds(dataset='comp.g_company', Y='all')
	get_wrds(dataset='comp.names', Y='all')
	for dset in _comp_dsets:
		wrds_loop(dataset=dset, min_date=0, recombine=1)
	return

_optionm_dsets = ['optionm.opprcd']
def get_optionm():
	get_wrds(dataset='optionm.optionmnames', Y='all')
	for dset in _optionm_dsets:
		wrds_loop(dataset=dset, min_date=0, recombine=1)
	return

_ibes_dsets = [
	'ibes.det_epsus', 
	'ibes.det_xepsus', 
	'ibes.det_epsint', 
	'ibes.det_xepsint', 
	'ibes.recddet'
	]
def get_ibes():
	get_wrds(dataset='ibes.id', Y='all')
	for dset in _ibes_dsets:
		wrds_loop(dataset=dset, min_date=0, recombine=1)
	return

_tfn_dsets = ['tfn.s12', 'tfn.s34']
def get_tfn():
	get_wrds(dataset='tfn.company', Y='all')
	get_wrds(dataset='tfn.s12names', Y='all')
	get_wrds(dataset='tfn.s34names', Y='all')
	for dset in _tfn_dsets:
		wrds_loop(dataset=dset, min_date=0, recombine=1)
	return

_taq_dsets = ['taq.ct', 'taq.cq']
def get_taq():
	for dset in _taq_dsets:
		wrds_loop(dataset=dset, min_date=0, recombine=1)
	return








_other_datasets = {
	'markit': ['cds','red'],
	'bank': ['all','ama_v','ama_l','ama_m','ama_s','bs','is','os','ama','bankscope'],
	'risk': ['directors','governance'],
	'sprat': ['all'],
	'doe': ['all'],
	'cboe': ['all'],
	'djones': ['all'],
	'frb': ['all'],
	'fisd': ['fisd', 'naic'],
	'phlx': ['all'],
	'trace': ['all'],
	'tfn': ['insiderdata'],
	'taq': ['div'],
	'crsp': ['indices','cti','fund_summary2','sp500'],
	'crspa': ['cti','dreit','tfz_dly','sp500'],
	'comp': ['execcomp', 'g_idx_daily','inxcst_hist', 'secd', 'g_secd', 'currency', 'g_currency', 'index', 'g_idx_index', 'comp.spind_dly', 'it_mbuysell', 'iO_qbuysell'],
	'compa': 'Unknown',
	'compm': 'Unknown',
	'compq': 'Unknown'
}
#




################################################################################
_wrds_URL = 'https://wrds-web.wharton.upenn.edu/wrds/'
def wrds_register():
	"""wrds_register()
	walks the user through the process of setting up a WRDS account.
	Because it uses Selenium to navigate the WRDS website interface, 
	using this function is probably more difficult for the user 
	than just going to the website and setting up an account themselves.

	** wrds_register has not been tested ##

	return success_boolean
	"""
	if not has_modules['selenium.webdriver']:
		print ('wrdslib.wrds_register is unavailable without '
			+'the dependency "selenium.webdriver".')
		return 0
	browser = webdriver.Firefox()
	browser.get(_wrds_URL)
	button = browser.find_element_by_link_text('Register')
	button.click()
	#
	scroll = browser.find_element_by_id('schoolid')
	institutions = scroll.text.splitlines()
	if wrds_institution == []:
		wrds_institution = raw_input('Please enter your institutional '
			+ 'affiliation to register a wrds account:')
	matching_institutions = []
	if isinstance(wrds_institution,(str,unicode)):
		if wrds_institution in institutions:
			wrds_institution2 = wrds_institution
			matching_institutions = [wrds_institution]
		else:
			matching_institutions = [x for x in institutions 
				if re.search(wrds_institution,x,flags = re.I)]
	if matching_institutions == []:
		print ('Could not find an available institution matching '
			+ 'your input.  Please select your institution '
			+ 'from the following list:')
		print '\n'.join(institutions)
		wrds_institution2 = raw_input("institution: ")
	elif len(matching_institutions)>1:
		print ('Multiple institutions were found that match your '
			+ 'specified wrds_institution.  Please choose your exact '
			+' institution from the following list:')
		print '\n'.join(matching_institutions)
		wrds_institution2 = raw_input("institution: ")
	if len(matching_institutions) == 1:
		wrds_institution2 = matching_institutions[0]
	if wrds_institution2 not in institutions:
		print ('user input was not in the specified list.  '
			+'wrds_register is exiting uncompleted.')
		return 0
	#
	scroll_choices = scroll.find_elements_by_tag_name('option')
	user_choice = [x for x in scroll_choices if x.text == wrds_institution2]
	if len(user_choice) != 1:
		print ('An unrecognized problem has occurred in wrds_register.  '
			+'Please forward the following to Brock for debugging:')
		print wrds_institution
		print wrds_institution2
		print institutions
		print [x.text for x in scroll_choices]
		return 0
	user_choice = user_choice[0]
	user_choice.click()
	#
	affil_scroll = browser.find_element_by_id('usertype')
	user_types = affil_scroll.text.splitlines()
	possible_types = [
		'Faculty', 
		'Staff (IT / Librarian)', 
		'Visiting Faculty', 
		'Ph.D.', 
		'Masters Student', 
		'Research Assistant (Supervisted students only)', 
		'Class (Faculty requestor only)'
		]
	print ('Which of the following best describes '
		+' your affiliation with your institution: \n'
		+'(1) Faculty\n'
		+'(2) Staff (IT/Librarian)\n'
		+'(3) Visiting Faculty\n'
		+'(4) Ph.D.\n'
		+'(5) Masters Student\n'
		+'(6) Research Assistant (Supervised students only)\n'
		+'(7) Class (Faculty requestor only)\n'
		)
	prompt_str = ('Enter the integer corresponding to your affiliation: ')
	user_type = raw_input(prompt_str)
	user_type = str(user_type)
	if user_type not in ['1','2','3','4','5','6','7']:
		print ('Invalid affiliation type.  '
			+ 'wrds_register is exiting uncompleted.')
		return 0

	affil_scroll_choices = affil_scroll.find_elements_by_tag_name('option')
	affiliation_choice = [x for x in affil_scroll_choices 
		if x.text == possible_types[int(float(user_type))-1]]
	
	if len(affiliation_choice) != 1:
		print ('An unrecognized problem has occurred '
			+ 'in wrds_register.  Please forward the '
			+ 'following to Brock for debugging:')
		print user_type
		print affiliation_choice
		print [x.text for x in affil_scroll_choices]
		return 0

	affiliation_choice = affiliation_choice[0]
	affiliation_choice.click()

	remaining_inputs = [
		['fname_dd', 'First Name'], 
		['lname_dd', 'Last Name'], 
		['user_email_dd', 'Email (institution-affiliated)'], 
		['user_username_dd', 'Desired username']
		]
	for remaining_input in remaining_inputs:
		field = browser.find_element_by_id(remaining_input[0])
		user_value = raw_input('Please enter your '+remaining_input[1])
		field.send_keys(user_value)

	submit_button = browser.find_element_by_id('submit_dd')
	submit_button.click()
	print ('wrds_register appears to have completed '
		+ 'successfully.  You should receive an email '
		+ 'from wrds or your institution confirming '
		+ 'your registration.')
	return 1
################################################################################






################################################################################
has_modules = {}
try:
	import selenium.webdriver
	has_modules['selenium.webdriver'] = 1
except ImportError:
	print ('Some wrdslib functionality requires the package '
		+'selenium.webdriver.  Please "pip install selenium".  '
		+' Until then some functionality will be limited.')
	has_modules['selenium.webdriver'] = 0




