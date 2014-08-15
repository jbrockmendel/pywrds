"""
pywrds.wrdslib is a collection of functions used by ectools 
which use or provide information specific to the WRDS SAS 
system.
"""
# pywrds.wrdlib
# last edit: 2014-08-10
thisAlgorithmBecomingSkynetCost = 99999999999
import datetime, json, os, re, sys, time
################################################################################
import sshlib

wrds_domain = 'wrds.wharton.upenn.edu'

################################################################################

user_info = {}
this_file = os.path.abspath(__file__)
user_path = this_file.split('source')[0]
user_info_filename = os.path.join(user_path,'user_info.txt')
if os.path.exists(user_info_filename):
	fd = open(user_info_filename,'rb')
	content = fd.read()
	content = content.replace('\xe2\x80\x9c','"')
	content = content.replace('\xe2\x80\x9d','"')
	try:
		user_info = json.loads(content)
	except ValueError:
		print ('pywrds.wrdslib warning: user_info.txt file does not '
			+ 'conform to json format.  Please address this '
			+ 'and reload ectools.')
	fd.close()
else:
	print ('pywrds.wrdslib warning: Please create a user_info.txt '
		+ 'file conforming to the format given in the '
		+ 'user_info_example.txt file.')







################################################################################
download_path = os.path.join(user_path,'output')
if 'download_path' in user_info:
	download_path = user_info['download_path']

wrds_institution = []
if 'wrds_institution' in user_info.keys():
	wrds_institution = user_info['wrds_institution']

wrds_username =[]
if 'wrds_username' in user_info.keys():
	wrds_username = user_info['wrds_username']

if 'last_wrds_download' not in user_info.keys():
	user_info['last_wrds_download'] = {}
last_wrds_download = user_info['last_wrds_download']

################################################################################
# first_dates is an estimate of the first date YYYYMMDD on        #
# which data is available for a given WRDS dataset.               #
first_dates = {
	"taq.div": 19930000,
	"taq.mast": 19930000,
	"taq.RGSH": 20050100,
	"taq.cq": 19930104,
	"taq.ct": 19930104,
	"ff.liq_ps": 19620000,
	"ff.factors_daily": 19620000,
	"ff.factors_monthly": 19620000,
	"ff.liq_sadka": 19830000,
	"crsp.msf": 19250000,
	"crsp.dsf": 19250000,
	"crspa.sp500": 19250000,
	"crsp.sp500": 19250000,
	"crspq.portnomap": 20000000,
	"crsp.portnomap": 20000000,
	"crspq.holdings": 20010000,
	"crsp.holdings": 20010000,
	"crspq.front_load": 19610000,
	"crspq.rear_load": 19610000,
	"crsp1.daily_nav_ret": 19980000,
	"crsp.fund_summary2": 19610000,
	"crspa.sp500": 19250000, 
	"crspa.cti": 19250000, 
	"crspa.bxcalind": 19610000,
	"crspa.tfz_ft": 19610000,
	"crspa.tfz_dly": 19610000,
	"crspa.dreit": 19800000,
	"crspq.dividends": 19600000,
	"crspa.tfz_mth_rf": 19250000,
	"crsp.tfz_mth_rf": 19250000,
	"crspa.mbmdat": 19250000,
	"crsp.cti": 19250000,
	"ibes.det_xepsus": 19890000,
	"ibes.det_xepsint": 19930000,
	"ibes.recddet": 19920000,
	"ibes.det_epsus": 19800000,
	"ibes.det_epsint": 19800000,
	"optionm.opprcd": 19960000,
	"tfn.s12": 19800000,
	"tfn.s34": 19800000,
	"comp.fundq": 19610000,
	"comp.g_fundq": 19870000,
	"comp.secd": 19830000,
	"comp.g_secd": 19850000,
	"comp.idx_daily": 19000000,
	"comp.g_idx_daily": 19700000,
	"comp.inxcst_hist": 19000000,
	"compm.fundq": 19610101
}

# first_guesses is a courser estimate for the dataset provider       #
# crowdsourced improvements to these estimates are welcome.          #
first_date_guesses = {
	"tfn": 19790000,
	"ibes": 19750000,
	"optionm": 19960000,
	"comp": 19600000,
	"comp.exrt_dly": -1,
	"comp.execcomp": -2,
	"compm": 19610000
}

# date_vars gives the label that each dataset uses for its date variables   #
date_vars = {
	"optionm.opprcd": "date"
}
date_var_guesses = {
	"ibes": "anndats",
	"crsp": "date",
	"tfn": "fdate",
	"comp": "DATADATE",
	"optionm": "effect_date"
}

# dataset_list is a partial list of dataset            #
# labels that can be fetched with get_wrds             #
dataset_list = [
	"bank.all",
	"bvd.ama",
	"bvd.bankscope",
	"risk.directors",
	"risk.governance",
	"sprat.all",
	"doe.all",
	"cboe.all",
	"comp.execucomp",
	"crsp.indices",
	"djones.all",
	"frb.all",
	"fisd.fisd",
	"fisd.naic",
	"phlx.all",
	"trace.all",
	"tfn.insiderdata",
	"taq.div",
	"comp.g_idx_daily",
	"crsp.cti",
	"crspa.cti",
	"crspa.dreit",
	"crspa.tfz_dly",
	"crsp.fund_summary2",
	"comp.inxcst_hist",
	"crsp.sp500",
	"crspa.sp500",
	"comp.secd",
	"comp.g_secd"
]



################################################################################
def rows_per_file_adjusted(dataset):
	"""rows_per_file_adjusted(dataset)

	_rows_per_file chooses a number of rows to query in each 
	_get_wrds_chunk request to ensure that the files produced 
	do not approach the 1 GB server limit.  For most datasets, 
	10^7 rows in a file is not a problem.  For optionm.opprcd, 
	this number is dropped to 10^6.

	To date optionm.opprcd is the only dataset for which this has 
	consistently been necessary.  This is subject to change 
	with further use cases.

	return rows_per_file
	"""
	rows_per_file = 10**7
	if dataset.replace('.','_') == 'optionm_opprcd':
		rows_per_file = 10**6
	elif dataset.replace('.','_') == 'optionm_optionmnames':
		rows_per_file = 10**6
	return rows_per_file



################################################################################
now = time.localtime()
[this_year, this_month, today] = [now.tm_year, now.tm_mon, now.tm_mday]
def get_ymd_range(min_date, dataset, weekdays=1):
	"""get_ymd_range(min_date, dataset, weekdays=1) gets a list of 
	tuples [year, month, date] over which to iterate in wrds_loop.  Some 
	datasets include very large files and need to be queried 
	at a monthly or daily frequency to prevent giant files from 
	causing problems on the server.

	return ymdrange
	"""
	[min_year, min_month, min_day] = min_YMD(min_date, dataset)

	ymdrange = []
	years = xrange(min_year,now.tm_year+1)
	for year in years:
		frequency = get_loop_frequency(dataset, year)
		if frequency == 'Y':
			new_ymd = [year, 0, 0]
			ymdrange.append(new_ymd)
		elif frequency =='M':
			new_ymd = [[year, x, 0] for x in range(1,13)]
			ymdrange = ymdrange + new_ymd
		elif frequency == 'D':
			new_ymd = [[year, x, y] for x in range(1,13) for y in range(1,32)]
			new_ymd = fix_weekdays(new_ymd, weekdays)
			ymdrange = ymdrange + new_ymd

	ymdrange = [x for x in ymdrange if x <= [this_year, this_month, today]]
	ymdrange = [x for x in ymdrange if x >= [min_year, min_month, min_day]]
	return ymdrange


################################################################################
def get_loop_frequency(dataset, year):
	"""get_loop_frequency(dataset, year) finds the best frequency at which 
	to query the server for the given dataset so as to avoid producing 
	problematically large files.

	return frequency
	"""
	frequency = 'Y'
	if dataset.startswith('optionm'):
		if year < 2008:
			frequency = 'M'
		else:
			frequency = 'D'
	elif re.search('det_xepsus',dataset,flags=re.I):
		if year > 2005:
			frequency = 'M'
	elif re.search('det_xepsint',dataset,flags=re.I):
		if year > 2003:
			frequency = 'M'
	elif re.search('taq',dataset,flags=re.I):
		frequency = 'D'
	return frequency


################################################################################
def fix_weekdays(ymds, weekdays=1):
	"""fix_weekdays(ymds, weekdays=1) takes a set of [year,month,date] 
	tuples "ymds" and removes those which are not valid days, 
	e.g. June 31, February 30.

	If weekdays is set to its default value of 1, it also removes 
	Saturday and Sundays.

	return ymds
	"""
	ymds2 = []
	for [y, m, d] in ymds:
		try:
			wday = datetime.date(y,m,d).weekday()
		except ValueError:
			wday = -1
		if weekdays == 1 and wday in range(5):
			# weekdays==1 --> only keep weekdays     #
			ymds2.append([y,m,d])
		elif weekdays == 0 and wday!=-1:
			# weekdays==0 --> keey any valid day     #
			ymds2.append([y,m,d])
	return ymds2
################################################################################










################################################################################
def fix_input_name(dataset, year, month, day, rows=[]):
	"""fix_input_name(dataset, year, month, day, rows=[]) 
	adjusts the user-supplied dataset name to use the same 
	upper/lower case conventions as WRDS does.

	return [dataset, output_file]
	"""
	[Y, M, D, R] = [year, month, day, rows]
	if year != 'all':
		ystr = '_'*(dataset[-1].isdigit())+str(Y)
		mstr = '' + (M != 0)*('0'*(month<10)+str(M))
		dstr = (D != 0)*('0'*(D<10)+str(D))
		ymdstr = ystr + mstr + dstr +'.tsv'
		output_file = re.sub('\.','_',dataset) + ymdstr 
	else:
		output_file = re.sub('\.','_',dataset)+'.tsv'

	if dataset.lower() == 'optionm.opprcd':
		dataset = dataset+str(year)

	elif dataset.lower() in ['taq.cq', 'taq.ct']:
		dataset = re.sub('cq','CQ',dataset)
		dataset = re.sub('ct','CT',dataset)
		ystr = '_' + str(Y)
		mstr = ''+(M != 0)*('0'*(M<10)+str(M))
		dstr = ''+(D != 0)*('0'*(D<10)+str(D))
		ymdstr = ystr + mstr + dstr
		dataset = dataset + ymdstr
	
	elif dataset.lower() in ['taq.mast', 'taq.div']:
		ymdstr = '_'+str(Y)+(M != 0)*('0'*(M<10) + str(M))
		dataset = dataset + ymdstr
		
	elif dataset.lower() == 'taq.rgsh':
		ymdstr = str(100*Y+M)[2:]
		dataset = 'taq.RGSH'+ymdstr
	
	if R != []:
		rowstr = 'rows'+str(R[0])+'to'+str(R[1])+'.tsv'
		output_file = re.sub('.tsv$','',output_file) + rowstr

	return [dataset, output_file]



################################################################################
def wrds_sas_script(dataset, year, month=0, day=0, rows=[]):
	"""wrds_sas_script(dataset, year, month=0, day=0, rows=[]) 
	generates a .sas file which is executed on the WRDS server 
	to produce the desired dataset.

	return [sas_file, output_file, dataset]
	"""
	tic = time.time()
	[Y, M, D, R] = [year, month, day, rows]
	ystr = '' + ('_' + str(Y))*(Y != 'all')
	mstr = '' + (M != 0)*('0'*(M<10)+str(M))
	dstr = '' + (D != 0)*('0'*(D<10)+str(D))
	ymdstr = ystr + mstr + dstr
	sas_file = 'wrds_export_'+re.sub('\.','_',dataset)

	if R != []: 
		rowstr = 'rows'+str(R[0])+'to'+str(R[1])
		sas_file =  sas_file + ymdstr + rowstr
	else:
		sas_file = sas_file + ymdstr
	sas_file = sas_file + '.sas'

	[dataset, output_file] = fix_input_name(dataset, Y, M, D, R)
	fd = open(os.path.join(download_path,sas_file),'wb')
	fd.write('DATA new_data;\n')
	fd.write('\tSET '+dataset)
	if Y != 'all':
		where_query = ' (where = ('
		year_query = ('(year('+wrds_datevar(dataset)+')'
			+' between '+str(Y)+' and '+str(Y)+')')
		where_query = where_query + year_query

		if M != 0:
			month_query = (' and (month('+wrds_datevar(dataset)
				+') between '+str(M)+' and '+str(M)+')')
			where_query = where_query+month_query

		if D != 0:
			day_query = (' and (day('+wrds_datevar(dataset)
				+') between '+str(D)+' and '+str(D)+')')
			where_query = where_query+day_query

		where_query = where_query+'));\n'
		fd.write(where_query)
	else:
		fd.write(';\n')

	if R != []:
		rowquery = ('\tIF ('+str(R[0])+'<= _N_<= '+str(R[1])+');\n')
		fd.write(rowquery)
	
	fd.write('\n')
	fd.write('proc export data = new_data\n')
	fd.write(('\toutfile = "~/'+output_file+'" \n'
	+'\tdbms = tab \n'
	+'\treplace; \n'
	+'\tputnames = yes; \n'
	+'run; \n'))
	fd.close()
	return [sas_file, output_file, dataset]
################################################################################






################################################################################
def update_user_info(numfiles, new_files, fname, dataset, year, month=0, day=0):
	"""update_user_info(numfiles, new_files, fname, dataset, year, month=0, day=0) 
	amends the user_info file to reflect the most recent download dates 
	for wrds files.

	return
	"""
	if new_files > 0:
		numfiles = numfiles + new_files
		if 'last_wrds_download' not in user_info.keys():
			user_info['last_wrds_download'] = {}
		user_info['last_wrds_download'][dataset] = year*10000 + month*100 + day
		fd = open(user_info_filename,'wb')
		fd.write(json.dumps(user_info, indent=4))
		fd.close()
	else:
		print ('Could not retrieve: ' + fname)
	return
################################################################################




################################################################################
_get_all = ['crsp.stocknames', 'comp.company', 'comp.g_company']
# datasets for which the default is to download the entire   #
# dataset at once                                            #

def min_YMD(min_date, dataset):
	"""min_YMD(min_date, dataset) finds (year,month,day) at which 
	to start wrds_loop when downloading the entirety of a 
	dataset.  It checks user_info to find what files have 
	already been downloaded.

	return [min_year, min_month, min_day]
	"""
	if dataset in _get_all:
		return [-1, -1, -1]

	if 'last_wrds_download' not in user_info:
		user_info['last_wrds_download'] = {}
	if dataset not in user_info['last_wrds_download']:
		if dataset in first_dates:
			user_info['last_wrds_download'][dataset] = first_dates[dataset]
		else:
			user_info['last_wrds_download'][dataset] = 18000000

	if not isinstance(min_date,(int,float)):
		min_date = 0

	if min_date == 0:
		min_date = user_info['last_wrds_download'][dataset]
		min_date = str(min_date)
		if not min_date.isdigit() or len(min_date) != 8:
			min_date = 0
			print ('user_info["last_wrds_download"]["'+dataset+'"]='
				+min_date+' error, should be an eight digit integer.')
		min_year = int(float(min_date[:4]))
		min_month = int(float(min_date[4:6]))
		min_day = int(float(min_date[6:]))
		if min_month == min_day == 0:
			min_year += 1
		elif min_day == 0:
			min_month += 1
			if min_month == 13:
				min_month = 1
				min_year += 1
		else:
			min_day += 1
			try:
				wday = datetime.date(min_month,min_month,min_day).weekday()
			except:
				min_day = 1
				min_month += 1
				if min_month == 13:
					min_month = 1
					min_year += 1

	if min_date != 0:
		if min_date < 1880:
			min_day = 0
			min_month = 0
			min_year = 1880
			print ('Setting min_year = 1880.  This will result in '
				+'many empty data files and unnecessary looping.  '
				+'This can be prevented by a) inputting a higher '
				+'min_date or b) finding the first date at which '
				+'this dataset is available on WRDS and letting '
				+'Brock know so he can update the code appropriately.')
		elif min_date < 2050:
			min_day = 0
			min_month = 0
			min_year = int(min_date)
		elif 188000 < min_date < 1880000:
			min_month = min_date%100
			min_year = (min_date-(min_date%100))/100
		elif min_date < 20500000:
			min_day = min_date%100
			min_month = (min_date%10000-min_day)/100
			min_year = (min_date-(min_date%10000))/10000

	if min_date == 0:
		if dataset in first_dates.keys():
			min_day = first_dates[dataset]%100
			min_month = ((first_dates[dataset]-min_day)%10000)/100
			min_year = (first_dates[dataset]-100*min_month-min_day)/10000
		elif any(re.search(x,dataset) for x in first_date_guesses.keys()):
			key = [x for x in first_date_guesses.keys() 
				if re.search(x,dataset)][0]
			if dataset in first_date_guesses.keys():
				key = dataset
				if first_date_guesses[key] == -1:
					return [-1, -1, -1]
			min_day = first_date_guesses[key]%100
			min_month = ((first_date_guesses[key]-min_day)%10000)/100
			min_year = (first_date_guesses[key]-100*min_month-min_day)/10000
		else:
			min_day = 0
			min_month = 0
			min_year = 1880

	return [min_year, min_month, min_day]
################################################################################


################################################################################
def wrds_datevar(filename):
	"""wrds_datevar(filename) 
	Different datasets in WRDS use different names for 
	their date-variables.  wrds_datevar gives the right date 
	variable for each dataset.  This may need periodic updating.
	Crowdsourcing is welcome.

	return date_var
	"""
	if filename in ['tfn.s12','tfn.s34']:
		return 'fdate'
	if re.search('^crsp',filename):
		return 'date'
	if re.search('^comp',filename):
		return 'DATADATE'
	if re.search('^optionm\.opprcd',filename):
		return 'date'
	if re.search('^optionm',filename):
		return 'effect_date'
	if re.search('^ibes',filename):
		return 'anndats'
	return 'date'
################################################################################





################################################################################
def setup_wrds_key():
	"""setup_wrds_key() sets up a key-based authentication on 
	the wrds server, so that the user can log in without a 
	password going forward.

	return [ssh, sftp]
	"""
	[ssh, sftp] = sshlib.put_ssh_key(domain, username)
	institution = get_wrds_institution(ssh, sftp)
	return [ssh, sftp]



################################################################################
def get_wrds_institution(ssh, sftp):
	"""get_wrds_institution(ssh, sftp) gets the institution associated 
	with the user's account on the wrds server.

	return institution_path
	"""
	[ssh, sftp] = sshlib.getSSH(ssh, sftp, domain=wrds_domain, username=wrds_username, task={})
	if not sftp:
		return None
	try:
		wrds_path = sftp.normalize(path='.')
	except IOError:
		print ('sftp cannot resolve a path on the wrds server')
		return None
	institution_path = re.sub('/home/','',wrds_path).split('/')[0]
	if wrds_institution != institution_path:
		if wrds_institution == []:
			wrds_institution = institution_path
			user_info['wrds_institution'] = wrds_institution
			fd = open(user_info_filename,'wb')
			fd.write(json.dumps(user_info, indent=4))
			fd.close()
		else:
			print ('user_info["wrds_institution"] does not '
				+ 'match the directory "'+institution_path+'" '
				+ 'found on the wrds server.  '
				+ 'This mismatch may cause errors '
				+ 'in the download process.')
	return institution_path




################################################################################
autoexec_text = ("*  The library name definitions below are used by SAS;\n"
	+"*  Assign default libref for WRDS (Wharton Research Data Services);"
	+"\n\n   %include '!SASROOT/wrdslib.sas' ;\n\n\n"
	)


