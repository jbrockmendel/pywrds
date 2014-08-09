"""
pywrds.ectools is the main user interface for pywrds.  The primary 
functions are get_wrds, wrds_loop, and find_wrds.

I recommend that before running get_wrds or wrds_loop 
to download data, you run wrdslib.setup_wrds_key() 
to set up key-based authentication.  This will prevent 
you from needing to enter your password every time 
the program tries to connect to the WRDS server.
"""
# pywrds.ectools
# last edit: 2014-08-04
thisAlgorithmBecomingSkynetCost = 99999999999 # http://xkcd.com/534/
import datetime, math, os, re, shutil, sys, time
################################################################################
import wrdslib

getSSH = wrdslib.getSSH

_try_get = wrdslib._try_get
_try_put = wrdslib._try_put
_try_exec = wrdslib._try_exec
_try_listdir = wrdslib._try_listdir

_dlpath = wrdslib.download_path
_domain = wrdslib.wrds_domain
_uname = wrdslib.wrds_username
_institution = wrdslib.wrds_institution




################################################################################
def get_numlines_from_log(outfile, dname=_dlpath):
	"""get_numlines_from_log(outfile, dname=_dlpath) reads the 
	SAS log file created during get_wrds to find the number of 
	lines which the wrds server says should be in a downloaded 
	file "outfile".  This number can then be checked against 
	the number actually found in the file.

	return logfile_lines
	"""
	log_lines = -1
	sasfile = 'wrds_export_'+re.sub('\.tsv$','.log',outfile)
	if not os.path.exists(os.path.join(dname,sasfile)):
		partial_fname = re.sub('[0-9]*rows.*','',sasfile)
		sasfile2 = partial_fname+'_'+re.sub(partial_fname,'',sasfile)
		if os.path.exists(os.path.join(dname,sasfile2)):
			sasfile = sasfile2
		all_fname = re.sub('rows', '_allrows', sasfile)
		if os.path.exists(os.path.join(dname, all_fname)):
			sasfile = all_fname

	if os.path.exists(os.path.join(dname,sasfile)):
		fd = open(os.path.join(dname,sasfile))
		fsize = os.stat(fd.name).st_size
		while fd.tell() < fsize:
			fline = fd.readline()
			if re.search('^[0-9]* records created in ',fline):
				log_lines = re.split(' records created in ',fline)[0]
				log_lines = int(float(log_lines))
				break

			pattern0 = ('NOTE: The data set WORK\.NEW_DATA '
				+'has [0-9]* observations')
			if re.search(pattern0,fline):
				pattern01 = 'NOTE: The data set WORK\.NEW_DATA has '
				pattern02 = ' observations'
				split_log = re.split(pattern02,fline)[0]
				log_lines = re.split(pattern01, split_log)[-1]
				log_lines = int(float(log_lines))
				break

			pattern1 = 'NOTE: [0-9]* records were written to the file'
			if re.search(pattern1,fline):
				split_log = re.split('NOTE: ',fline)[-1]
				log_lines = re.split('records',split_log)[0]
				log_lines = int(float(log_lines))
				break

			# The numbers given by the pattern below are often     #
			# one row lower than the numbers given by the above    #
			# patterns, the latter being the desired answer.       #
			# This code is kept as an option to re-implement       #
			# should their arise cases where none of the other     #
			# patterns are found.                                  #
			#pattern2 = 'NOTE: There were [0-9]* observations read'
			#if re.search(pattern2,fline):
			#	split_log = re.split(' observations read',fline)[0]
			#	log_lines = re.split('NOTE: There were ',split_log)[-1]
			#	log_lines = int(float(log_lines))
			#	break
		fd.close()

	return log_lines


def _rename_after_download():
	return


################################################################################
def get_wrds(dataset, Y, M=0, D=0, ssh=[], sftp=[], recombine=1):
	"""get_wrds(dataset, Y=0, M=0, D=0, ssh=[], sftp=[], recombine=1)

	Remotely download a file from the WRDS server. For example, 
	the command 
	
	x = get_wrds('crsp.msf', 2010, 6) 

	will log in to the WRDS server, issue a query to generate 
	a tab-separated(*) file containing the entire CRSP Monthly 
	Stock File dataset for June 2010, then download that file 
	to your download_path (which you can edit in the user 
	information section above).  The output x is a pair 
	[indicator,elapsed_time] where indicator is a one if the 
	download was successful, zero otherwise.

	The arguments Y, M, D stand for Year, Month, Day, respectively.
	Ommitting the month argument 

	get_wrds(dataset_name, year) 

	will retrieve a single file for the entire year.

	(*) Tab-separated files (tsv) tend to work slightly 
	better than comma-separated files (csv) because sometimes 
	company names have commas e.g. Company Name, Inc.

	return [numfiles, total_rows, ssh, sftp, time_elapsed]
	"""
	keep_going = 1
	[startrow, numfiles, total_rows, tic] = [1, 0, 0, time.time()]
	rows_per_file = wrdslib.rows_per_file_adjusted(dataset)
	[dset2, outfile] = wrdslib.fix_input_name(dataset, Y, M, D, [])
	if os.path.exists(os.path.join(_dlpath,outfile)):
		keep_going = 0
	while keep_going:
		R = [startrow, startrow-1+rows_per_file]
		[dset2, outfile] = wrdslib.fix_input_name(dataset, Y, M, D, R)
		if not os.path.exists(os.path.join(_dlpath,outfile)):
			[keep_going, ssh, sftp, dt] = _get_wrds_chunk(dataset, Y, M, D, R, ssh, sftp)
		if keep_going > 0:
			numfiles += 1
			if os.path.exists(os.path.join(_dlpath,outfile)):				
				log_lines = get_numlines_from_log(outfile, dname=_dlpath)
				numlines = get_numlines(os.path.join(_dlpath,outfile))
				if log_lines > numlines:
					print ('get_wrds error: file '
						+outfile+' has '+ str(numlines)
						+' lines, but '+ str(log_lines)
						+' were expected.')
					keep_going = 0
				#elif len(fline.split('\t')) != len(first_line):
				## log files sometimes have lines like 
				# "The minimum record length was 34." 
				# "The maximum record length was 58."
				#	print ('get_wrds error: file '+outfile
				#		+' appears to have been truncated mid-writing.  '
				#		+' The final line has fewer '
				#		+'columns than the first line.')
				#	keep_going = 0

				total_rows += numlines
				if numlines < rows_per_file:
					keep_going = 0

				if log_lines == numlines < rows_per_file:
					keep_going = 0
					if not (log_lines == -1 or log_lines == numlines):
						print ('get_wrds warning: '
							+'log_lines = '+str(log_lines))
					if startrow == 1:
						subfrom = 'rows1to'+str(rows_per_file)
						newname = re.sub(subfrom,'',outfile)
						newp2f = os.path.join(_dlpath,newname)
						oldp2f = os.path.join(_dlpath,outfile)
						os.rename(oldp2f,newp2f)
					else:
						subfrom = 'to'+str(R[-1])
						subto = 'to'+str(R[0]-1+numlines)
						newname = re.sub(subfrom,subto,outfile)
						oldp2f = os.path.join(_dlpath,outfile)
						newp2f = os.path.join(_dlpath,newname)
						os.rename(oldp2f,newp2f)
					if recombine == 1:
						subfrom = 'rows[0-9]*to[0-9]*\.tsv'
						recombine_name = re.sub(subfrom,'',outfile)
						recombine_files(recombine_name, dname=_dlpath)
				else:
					startrow += rows_per_file
					newname = outfile

			else:
				keep_going = 0

	return [numfiles, total_rows, ssh, sftp, time.time()-tic]


################################################################################
def _get_wrds_chunk(dataset, Y, M=0, D=0, R=[], ssh=[], sftp=[]):
	"""_get_wrds_chunk(dataset, Y, M=0, D=0, rows=[], ssh=[], sftp=[])

	Some files requested by get_wrds are too large to fit 
	in a user's allotted space on the wrds server.  For these 
	files, get_wrds will split the request into multiple 
	smaller requests to retrieve multiple files and run each
	of them through _get_wrds_chunk.  If the argument
	"recombine" is set to its default value of 1, these files 
	will be recombined once the loop completes.

	return [success, ssh, sftp, time_elapsed]
	"""
	tic = time.time()
	[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)
	if not sftp:
		return [0, ssh, sftp, time.time()-tic]
	
	[sas_file, outfile, dataset] = wrdslib.wrds_sas_script(dataset, Y, M, D, R)
	log_file = re.sub('\.sas$','.log',sas_file)

	put_success = _put_sas_file(ssh, sftp, outfile, sas_file)
	exit_status = _sas_step(ssh, sftp, sas_file, outfile) 
	exit_status = _handle_sas_failure(ssh, sftp, exit_status, outfile, log_file)
		
	if exit_status in [0, 1]:
		[ssh, sftp, fdict] = _try_listdir('.', ssh, sftp, _domain, _uname)
		file_list = fdict.keys()
		#file_list = sftp.listdir()
		if outfile not in file_list:
			print ('exit_status in [0, 1] suggests SAS succeeded, '
				+'but the desired output_file "'
				+outfile+'"" is not present in the file list:')
			print file_list

		else:
			remote_size = _wait_for_sas_file_completion(ssh, sftp, outfile)
			[get_success, dt] = _retrieve_file(ssh, sftp, outfile, remote_size)
			local_size = _wait_for_retrieve_completion(outfile, get_success)
			compare_success = _compare_local_to_remote(ssh, sftp, outfile, remote_size, local_size)

	[got_log, ssh, sftp] = _get_log_file(ssh, sftp, log_file, sas_file)
	checkfile = os.path.join(_dlpath,outfile)
	if os.path.exists(checkfile) or exit_status == 0:
		return [1, ssh, sftp, time.time()-tic]
	return [0, ssh, sftp, time.time()-tic]





################################################################################
now = time.localtime()
[this_year, this_month, today] = [now.tm_year, now.tm_mon, now.tm_mday]
def wrds_loop(dataset, min_date=0, recombine=1, ssh=None, sftp=None):
	"""wrds_loop(dataset, min_date=0, recombine=1, ssh=None, sftp=None)
	executes get_wrds(database_name,...) over all years and 
	months for which data is available for the specified 
	data set.  File separated into chunks for downloading 
	will be recombined into their original forms if 
	recombine is set to its default value 1.

	return [numfiles, time_elapsed]
	"""
	tic = time.time()
	[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)
	[numfiles, numlines, numlines0] = [0, 0, 0]
	[min_year, min_month, min_day] = wrdslib.min_YMD(min_date, dataset)
	flist = os.listdir(_dlpath)
		
	if [min_year, min_month, min_day] == [-1, -1, -1]:
		Y = 'all'
		get_output = get_wrds(dataset, Y, M=0, D=0, ssh=ssh, sftp=sftp, recombine=recombine)
		[new_files, total_lines, ssh, sftp, dt] = get_output
		if new_files > 0:
			numfiles = numfiles + 1
		ssh.close()
		sftp.close()
		[ssh, sftp] = [[], []]
		return [numfiles, time.time()-tic]
	

	for ymd in wrdslib.get_ymd_range(min_date, dataset, 1):
		[Y, M, D] = ymd
		[dset2, outfile] = wrdslib.fix_input_name(dataset, Y, M, D, [])
		if outfile in flist:
			continue
		get_output = get_wrds(dataset, Y, M=M, D=D, ssh=ssh, sftp=sftp, recombine=recombine)
		[new_files, total_lines, ssh, sftp, dt] = get_output

		numfiles = numfiles + new_files
		wrdslib.update_user_info(numfiles, new_files, fname=outfile, dataset=dataset, year=Y, month=M, day=D)

	return [numfiles, time.time()-tic]


















################################################################################
def _put_sas_file(ssh, sftp, outfile, sas_file):
	"""_put_sas_file(ssh, sftp, outfile, sas_file) puts the sas_file 
	in the appropriate directory on the wrds server, handling 
	several common errors that occur during this process.  

	It removes old files which may interfere with the new 
	files and checks that there is enough space in the user 
	account on the wrds server to run the sas command.
	
	Finally it checks that the necessary autoexec.sas files 
	are present in the directory.

	return put_success_boolean
	"""
	[ssh, sftp, fdict] = _try_listdir('.', ssh, sftp, _domain, _uname)
	initial_files = fdict.values()

	old_export_files = [x for x in initial_files 
		if re.search('wrds_export.*sas$', x.filename) 
		or re.search('wrds_export.*log$', x.filename) 
		or x.filename == sas_file]
	for old_file in old_export_files:
		try:
			sftp.remove(old_file.filename)
		except (IOError,EOFError,paramiko.SSHException):
			pass
		initial_files.remove(old_file)

	pattern = '[0-9]*rows[0-9]+to[0-9]+\.tsv$'
	old_outfiles = [x for x in initial_files 
		if re.sub(pattern,'',x.filename) == re.sub(pattern,'',outfile)]

	for old_file in old_outfiles:
		try:
			sftp.remove(old_file.filename)
		except (IOError,EOFError,paramiko.SSHException):
			pass
		initial_files.remove(old_file)
		## see if the file is something you                ##
		## actually want before deleting it out of hand    ##

	file_sizes = [initial_file.st_size for initial_file in initial_files]
	total_file_size = sum(file_sizes)
	if total_file_size > 5*10**8:
		MBs = int(float(total_file_size)/1000000)
		print ('You are using approximately '+str(MBs)
			+' megabytes of your 1 GB'
			+' quota on the WRDS server.  This may cause '
			+'ectools.get_wrds to operate'
			+' incorrectly.  The files present are: ')
		print [x.filename for x in initial_files]

	auto_names = ['autoexec.sas', '.autoexecsas']
	autoexecs = [x.filename for x in initial_files if x.filename in auto_names]
	if autoexecs == ['.autoexecsas']: 
		# if 'autoexec.sas' is not present, the sas program will fail   #
		# a backup copy is stored by default in .autoexecsas            #
		ssh_command = 'cp .autoexecsas autoexec.sas'
		[exec_succes, stdin, stdout, stderr, ssh, sftp] = _try_exec(ssh_command, ssh, sftp, _domain, _uname)
		#ssh.exec_command('cp .autoexecsas autoexec.sas')
	elif autoexecs == ['autoexec.sas']:
		ssh_command = 'cp autoexec.sas .autoexecsas'
		[exec_succes, stdin, stdout, stderr, ssh, sftp] = _try_exec(ssh_command, ssh, sftp, _domain, _uname)
		#ssh.exec_command('cp autoexec.sas .autoexecsas')
	elif autoexecs == []:
		fd = open('autoexec.sas','wb')
		fd.write(wrdslib.autoexec_text)
		fd.close()
		local_path = 'autoexec.sas'
		remote_path = 'autoexec.sas'
		[put_success, ssh, sftp] = _try_put(local_path, remote_path, ssh, sftp, _domain, _uname)
		#sftp.put('autoexec.sas','autoexec.sas')## try ##
		ssh_command = 'cp autoexec.sas .autoexecsas'
		[exec_succes, stdin, stdout, stderr, ssh, sftp] = _try_exec(ssh_command, ssh, sftp, _domain, _uname)
		#ssh.exec_command('cp autoexec.sas .autoexecsas')
		os.remove('autoexec.sas')

	local_path = os.path.join(_dlpath,sas_file)
	remote_path = sas_file
	[put_success, ssh, sftp] = _try_put(local_path, remote_path, ssh, sftp, _domain, _uname)
	#[put_success, numtrys, max_trys] = [0, 0, 3]
	#while put_success == 0 and numtrys < max_trys:
	#	try:
	#		sftp.put(local_path, sas_file)
	#		put_success = 1
	#	except (paramiko.SSHException,IOError,EOFError):
	#		[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)
	#		numtrys += 1

	return put_success




################################################################################
def _sas_step(ssh, sftp, sas_file, outfile):
	"""_sas_step(ssh, sftp, sas_file, outfile) wraps the running of 
	the sas command (_run_sas_command) with retrying and 
	re-initializing the network connection if necessary.

	return exit_status
	"""
	[sas_completion, num_sas_trys, max_sas_trys] = [0, 0, 3]
	while sas_completion == 0 and num_sas_trys < max_sas_trys:
		exit_status = _run_sas_command(ssh, sftp, sas_file, outfile)
		num_sas_trys += 1
		sas_completion = 1

		if exit_status in [42, 104]: 
			# 42 = network read failed                 #
			# 104 = connection reset by peer           #
			sas_completion = 0
			[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)
			if not sftp:
				return exit_status

			[ssh, sftp, fdict] = _try_listdir('.', ssh, sftp, _domain, _uname)
			#file_list = sftp.listdir()
			if outfile in fdict.keys():
				exit_status = 0
				sas_completion = 1

			elif log_file in fdict.keys():
				exit_status = -1
				sas_completion = 1
				
	return exit_status





################################################################################
def _run_sas_command(ssh, sftp, sas_file, outfile):
	"""_run_sas_command(ssh, sftp, sas_file, outfile) executes 
	the sas script sas_file on the wrds server and waits for 
	an exit status to be returned.

	return exit_status
	"""
	[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)
	if not sftp:
		return -1

	sas_command = ('sas -noterminal '+ sas_file)

	#[success, stdin, stdout, stderr, ssh, sftp] = _try_exec(sas_command, ssh, sftp, _domain, _uname)
	[stdin,stdout,stderr] = ssh.exec_command(sas_command) 
	[exit_status, exit_status2, waited, maxwait] = [-1, -1, 0, 1200]
	while exit_status == -1 and waited < maxwait:
		time.sleep(10)
		waited += 10
		exit_status = stdout.channel.recv_exit_status()

	if waited >= maxwait:
		print ('get_wrds stopped waiting for SAS '
			+'completion at step 1: '+outfile)
	return exit_status




################################################################################
def _handle_sas_failure(ssh, sftp, exit_status, outfile, log_file):
	"""_handle_sas_failure(ssh, sftp, exit_status, outfile, log_file) 
	checks the sas exit status returned by the wrds server and 
	responds appropriately to any statuses other than success.

	return exit_status
	"""
	[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)
	if not sftp: 
		return exit_status 
	## turn the last three lines into a wrapper? ##

	real_failure = 1

	[ssh, sftp, fdict] = _try_listdir('.', ssh, sftp, _domain, _uname)
	#file_list = sftp.listdir()
	if exit_status == 2 and log_file in fdict.keys():
		fd = sftp.file(log_file)
		logcontent = fd.read()
		fd.close()
		if re.search('error: file .* does not exist.',logcontent,flags=re.I):
			real_failure = 0

	if exit_status not in [0, 1] and real_failure == 1:
		# 1 is "SAS system issued warnings", non-fatal    #

		if outfile in fdict.keys():
			print ('SAS is apparently returning an incorrect '
				+ 'exit status: '+str(exit_status)+', '+outfile+'.  '
				+'ectools is downloading the file for user inspection.')
			remote_path = outfile
			local_path = os.path.join(_dlpath,outfile)
			[get_success, ssh, sftp, dt] = _try_get(ssh, sftp, domain=_domain, username=_uname, remote_path=remote_path, local_path=local_path)
			if get_success == 0:
				print ('File download failure.')
			#try:
			#	sftp.get(outfile, localpath=os.path.join(_dlpath,outfile))
			#except KeyboardInterrupt:
			#	raise KeyboardInterrupt
			#except: ### hacked together ###
			#	print ('File download failure.')
			#	pass
			#try:
			#	sftp.remove(outfile)
			#except KeyboardInterrupt:
			#	raise KeyboardInterrupt
			#except:
			#	print ('File removal failure.')
			#	pass

		else:
			print ('get_wrds failed on file "'+outfile+'"\n'
				+'exit_status = '+str(exit_status)+'\n'
				+'For details, see log file "'+log_file+'"')

	return exit_status




################################################################################
def _wait_for_sas_file_completion(ssh, sftp, outfile):
	"""_wait_for_sas_file_completion(ssh, sftp, outfile) checks the size 
	of the file outfile produced on the wrds server within get_wrds. 
	Until it observes two successive measurements with the same file 
	size, it infers that the sas script is still writing the file.

	return remote_size
	"""
	## add getSSH for the sftp.stat?           ##
	## i think this may be perfunctory in      ##
	## the case where exit_status = 0          ## 
	## indicates the process is done, not sure ##
	[measure1, measure2, mtime, waited2, maxwait2] = [0, 1, time.time(), 0, 1200]
	while sftp and ((waited2 < maxwait2) 
		and (measure1 != measure2 or (time.time() - mtime <= 10))):
		measure1 = measure2
		time.sleep(10)
		waited2 += 10
		try:
			output_stat = sftp.stat(outfile)
			measure2 = output_stat.st_size
			mtime = output_stat.st_mtime
		except (IOError,EOFError,paramiko.SSHException):
			[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)


	if waited2 >= maxwait2:
		print ['get_wrds stopped waiting for SAS completion at step 2',
			measure1, measure2, mtime]
		measure1 = 0
		## should i remove the file in this case?  ##
	remote_size = measure1

	return remote_size





################################################################################
def _retrieve_file(ssh, sftp, outfile, remote_size):
	"""_retrieve_file(ssh, sftp, outfile, remote_size) retrieves the 
	file outfile produced on the wrds server in get_wrds, including 
	correct handling of several common network errors.

	return retrieve_success_boolean
	"""
	tic = time.time()
	if remote_size == 0:
		return [0, time.time()-tic]
	if remote_size >= 10**7:
		# skip messages for small files    #
		print ('starting retrieve_file: '+outfile
			+' ('+repr(remote_size)+') bytes')

	vfs = os.statvfs(_dlpath)
	free_local_space = vfs.f_bavail*vfs.f_frsize
	if remote_size > free_local_space:
		print ('get_wrds cannot download file '+outfile+', only '
		+str(free_local_space)+' bytes available on drive for '
		+str(remote_size)+'-byte file.')
		return [0, time.time()-tic]

	[get_success, numtrys, maxtrys] = [0, 0, 3]
	remote_path = ('/home/'+_institution+'/'+_uname+'/'+outfile)
	write_file = '.'+outfile+'--writing'
	local_path = os.path.join(os.path.expanduser('~'),write_file)
	[get_success, ssh, sftp, dt] = _try_get(ssh, sftp, domain=_domain, username=_uname, remote_path=remote_path, local_path=local_path)
	#while get_success == 0 and numtrys < maxtrys:
	#	try:
	#		sftp.get(remotepath=remote_path, localpath=local_path)
	#		get_success = 1
	#	except (paramiko.SSHException,paramiko.SFTPError,IOError,EOFError):
	#		if os.path.exists(local_path):
	#			os.remove(local_path)
	#		[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)
	#		numtrys += 1
	#	except KeyboardInterrupt:
	#		if os.path.exists(local_path):
	#			os.remove(local_path)
	#		raise KeyboardInterrupt

	print ('retrieve_file: '+repr(outfile)
		+' ('+repr(remote_size)+' bytes) '
		+' time elapsed='+repr(time.time()-tic))

	return [get_success, time.time()-tic]








################################################################################
def _wait_for_retrieve_completion(outfile, get_success, maxwait=1200):
	"""_wait_for_retrieve_completion(outfile, get_success) checks the 
	size of the downloaded file outfile multiple times and waits for 
	two successive measurements giving the same file size.  Until 
	this point, it infers that the download is still in progress.

	return local_size
	"""
	if get_success == 0:
		return 0
	waited3 = 0
	[locmeasure1, locmeasure2, mtime2] = [0, 1, time.time()]
	write_file = '.'+outfile+'--writing'
	local_path = os.path.join(os.path.expanduser('~'),write_file)
	while ((waited3 < maxwait) 
		and (locmeasure1 != locmeasure2 or (time.time() - mtime2) <= 10)):
		locmeasure1 = locmeasure2
		time.sleep(5)
		waited3 += 5
		local_stat = os.stat(local_path)
		locmeasure2 = local_stat.st_size
		mtime2 = local_stat.st_mtime

	if waited3 >= maxwait:
		print ('get_wrds stopped waiting for SAS completion at step 3',
			locmeasure1, locmeasure2, mtime2)
		locmeasure1 = 0
	local_size = locmeasure1
	return local_size







################################################################################
def _compare_local_to_remote(ssh, sftp, outfile, remote_size, local_size):
	"""_compare_local_to_remote(ssh, sftp, outfile, remote_size, local_size) 
	compares the size of the file "outfile" downloaded (local_size) to 
	the size of the file as listed on the server (remote_size) to 
	check that the download completed properly.

	return compare_success_boolean
	"""
	compare_success = 0
	write_file = '.'+outfile+'--writing'
	local_path = os.path.join(os.path.expanduser('~'),write_file)
	if remote_size == local_size != 0:
		[exec_succes, stdin, stdout, stderr, ssh, sftp] = _try_exec('rm ' + outfile, ssh, sftp, _domain, _uname)
		#[stdin, stdout, stderr] = ssh.exec_command('rm ' + outfile)
		to_path = os.path.join(_dlpath,outfile)
		shutil.move(local_path, to_path)
		comare_success = 1

	elif local_size != 0:
		print ['remote_size != local_size', outfile, remote_size, local_size]
		log_size = math.log(local_size,2)
		if log_size == int(log_size):
			print ('The error appears to involve '
				+'the download stopping at 2^'+repr(log_size)+' bytes.')
		error_file = '.'+outfile+'--size_error'
		from_file = os.path.join(os.path.expanduser('~'),error_file)
		to_file = os.path.join(_dlpath,outfile)
		shutil.move(from_file, to_file)
		compare_success = 0

	return compare_success








################################################################################
def _get_log_file(ssh, sftp, log_file, sas_file):
	"""_get_log_file(log_file, sas_file)

	_get_log_file(log_file, sas_file) attempts to retrieve the SAS 
	log file generated by _get_wrds_chunk from the WRDS server.

	_get_log_file also removes the sas_file from the local directory, 
	though strictly speaking this belongs in a separate function.

	return success_boolean
	"""
	success = 1
	remote_path = ('/home/'+_institution+'/'+_uname+'/'+log_file)
	local_path = os.path.join(_dlpath,log_file)
	[success, ssh, sftp, dt] = _try_get(ssh, sftp, domain=_domain, username=_uname, remote_path=remote_path, local_path=local_path)
	#try:
	#	sftp.get(remotepath=remote_path, localpath=local_path)
	#except IOError,EOFError:
	#	success = 0
	[exec_succes, stdin, stdout, stderr, ssh, sftp] = _try_exec('rm ' + sas_file, ssh, sftp, _domain, _uname)
	#[stdin, stdout, stderr] = ssh.exec_command('rm ' + sas_file)
	[exec_succes, stdin, stdout, stderr, ssh, sftp] = _try_exec('rm wrds_export*', ssh, sftp, _domain, _uname)
	#[stdin, stdout, stderr] = ssh.exec_command('rm wrds_export*')

	saspath = os.path.join(_dlpath,sas_file)
	if os.path.exists(saspath):
		os.remove(saspath)
	return [success, ssh, sftp]




















################################################################################
def find_wrds(filename, ssh=None, sftp=None):
	"""find_wrds(dataset_name, ssh=None, sftp=None) will query 
	WRDS for a list of tables available from dataset_name.  For 
	example, setting dataset_name = 'crsp' returns a file with 
	a list of names including "dsf" (daily stock file) and 
	"msf" (monthly stock file).

	return [file_list, ssh, sftp]
	"""
	tic = time.time()
	local_sas_file = os.path.join(_dlpath,'wrds_dicts.sas')
	fd = open(local_sas_file,'wb')
	fd.write('\tproc sql;\n')
	fd.write('\tselect memname\n') 
	# optional: "select distinct memname"   #
	fd.write('\tfrom dictionary.tables\n')
	fd.write('\twhere libname = "' + filename.upper() +'";\n')
	fd.write('\tquit;\n')
	fd.close()
	[ssh, sftp] = getSSH(ssh, sftp, domain=_domain, username=_uname)
	for fname in ['wrds_dicts.sas', 'wrds_dicts.lst', 'wrds_dicts.log']:
		try:
			sftp.remove(fname)
		except KeyboardInterrupt:
			raise KeyboardInterrupt
		except:
			pass

	[put_success, ssh, sftp] = _try_put(local_sas_file, 'wrds_dicts.sas', ssh, sftp, _domain, _uname)
	#sftp.put(local_sas_file,'wrds_dicts.sas')
	sas_command = 'sas -noterminal wrds_dicts.sas'
	#[exec_succes, stdin, stdout, stderr, ssh, sftp] = _try_exec(sas_command, ssh, sftp, _domain, _uname)
	[stdin, stdout, stderr] = ssh.exec_command(sas_command)
	exit_status = -1
	while exit_status == -1:
		time.sleep(10)
		exit_status = stdout.channel.recv_exit_status()
	
	local_path = os.path.join(_dlpath,filename+'_dicts.lst')
	remote_path = ('/home/'+_institution+'/'+_uname+'/wrds_dicts.lst')
	[ssh, sftp, fdict] = _try_listdir('.', ssh, sftp, _domain, _uname)
	remote_list = fdict.keys()
	#remote_list = sftp.listdir()
	if exit_status in [0, 1] and 'wrds_dicts.lst' in remote_list:
		#sftp.get(remotepath=remote_path, localpath=local_path)
		[get_success, ssh, sftp, dt] = _try_get(ssh, sftp, domain=_domain, username=_uname, remote_path=remote_path, local_path=local_path)
	else:
		print ('find_wrds did not generate a wrds_dicts.lst '
			+'file for input: '+repr(filename))
	try:
		sftp.remove('wrds_dicts.sas')
	except (IOError,EOFError,paramiko.SSHException):
		pass
	os.remove(local_sas_file)

	flist = []
	if os.path.exists(local_path):
		fd = open(local_path, 'rb')
		flist = fd.read().splitlines()
		fd.close()
		flist = [x.strip() for x in flist]
		flist = [x for x in flist if x != '']
		dash_line = [x for x in range(len(flist)) if flist[x].strip('- ') == '']
		if dash_line:
			dnum = dash_line[0]
			flist = flist[dnum:]


	return [flist, ssh, sftp]
################################################################################











################################################################################
def _recombine_ready(fname, dname=None, suppress=0):
	"""_recombine_ready(fname, dname=None, suppress=0) 
	checks files downloaded by get_wrds to see if the loop 
	has completed successfully and the files are ready 
	to be be recombined.

	If dname==None, the directory defaults to os.getcwd().

	return is_ready_boolean
	"""
	if not dname:
		dname = os.getcwd()
	isready = 1
	fname0 = re.sub('rows[0-9][0-9]*to[0-9][0-9]*\.tsv','',fname)

	if os.path.exists(os.path.join(dname,fname+'.tsv')):
		isready = 0

	rows_per_file = wrdslib.rows_per_file_adjusted(fname0)
	flist0 = os.listdir(dname)
	flist0 = [x for x in flist0 if x.endswith('.tsv')]
	flist0 = [x for x in flist0 if re.search(fname0,x)]
	fdict = {x: x.split('rows')[-1] for x in flist0}
	fdict = {x: re.split('_?to_?',fdict[x])[0] for x in fdict}
	fdict= {x: float(fdict[x]) for x in fdict if fdict[x].isdigit()}
	flist = [[fdict[x],x] for x in fdict]

	if isready and flist == []:
		isready = 0
		if suppress == 0:
			print ('recombine_ready: No such files found: '+fname)

	numlist = [x[0] for x in sorted(flist)]
	missing_nums = [x for x in numlist if x != 1]
	missing_nums = [x for x in missing_nums if x-rows_per_file not in numlist]

	if isready and missing_nums != []:
		isready = 0
		if suppress == 0:
			print ('recombine_ready: '+fname
				+' missing_nums '+repr(missing_nums+numlist))

	end_nums = [re.sub('\.tsv$','',x[1]) for x in flist]
	end_nums = [re.split('to',x)[-1] for x in end_nums]
	end_nums = [float(x) for x in end_nums]

	if isready and end_nums != [] and max(end_nums)%rows_per_file == 0:
		max_num = int(max(end_nums))
		flist2 = [x[1] for x in flist if x[1].endswith(repr(max_num)+'.tsv')]
		if len(flist2) == 1:
			outfile = flist2[0]
			numlines = get_numlines(os.path.join(dname,outfile))
			log_numlines = get_numlines_from_log(outfile, dname)
			if numlines != log_numlines:
				isready = 0
				print ('recombine_ready: '+outfile
					+' numlines!=log_numlines: '
					+repr([numlines,log_numlines]))
		else:
			isready = 0
			if suppress == 0:
				print ('recombine_ready: '+fname
					+' appears incomplete: '+repr(max(end_nums)))
	return isready





################################################################################
def recombine_files(fname, dname=None, suppress=0):
	"""recombine_files(fname, dname=None, suppress=0) 
	reads the files downloaded by get_wrds and combines them 
	back into the single file of interest.

	If dname==None, the directory defaults to os.getcwd().

	return num_combined_files
	"""
	if not dname:
		dname = os.getcwd()
	combined_files = 0
	if not _recombine_ready(fname, dname, suppress):
		return combined_files

	fname0 = re.sub('rows[0-9][0-9]*to[0-9][0-9]*\.tsv','',fname)
	rows_per_file = wrdslib.rows_per_file_adjusted(fname0)

	flist0 = [x for x in os.listdir(dname) if re.search(fname0,x)]
	flist0 = [x for x in flist0 if x.endswith('.tsv')]
	fdict = {x: x.split('rows')[-1] for x in flist0}
	fdict = {x: re.split('_?to_?',fdict[x])[0] for x in fdict}
	fdict = {x: float(fdict[x]) for x in fdict if fdict[x].isdigit()}
	flist = [[fdict[x], x] for x in fdict]

	flist = [x[1] for x in sorted(flist)]
	fd = open(os.path.join(dname, flist[-1]),'rb')
	fsize = os.stat(fd.name).st_size
	nlines = 0
	while fd.tell() > fsize:
		fd.readline()
		nlines += 1
	fd.close()
	if nlines >= rows_per_file:
		print [fname,flist[-1],
		'len(flines)='+repr(nlines),
		'should_be='+repr(rows_per_file)]
		return combined_files

	fd = open(os.path.join(dname,fname0+'.tsv'),'wb')
	headers = []
	found_problem = 0
	for fname1 in flist:
		fd1 = open(os.path.join(dname,fname1),'rb')
		fsize1 = os.stat(fd1.name).st_size
		headers1 = fd1.readline().strip('\r\n')
		if headers == []:
			headers = headers1
			fd.write(headers1+'\n')
		if headers1 != headers:
			print ('Problem with header matching:' +fname1)
			found_problem = 1
		if found_problem == 0:
			try:
				while fd1.tell() < fsize1:
					fd.write(fd1.readline().strip('\r\n')+'\n')
				fd1.close()
			except KeyboardInterrupt:
				fd1.close()
				fd.close()
				os.remove(fd.name)
				raise KeyboardInterrupt
			combined_files += 1
	fd.close()

	if found_problem == 0:
		for fname1 in flist:
			os.remove(os.path.join(dname,fname1))
	return combined_files

################################################################################
def get_numlines(path2file):
	"""get_numlines(path2file) reads a textfile located at 
	path2file and returns the number of lines found.

	return numlines
	"""
	fd = open(path2file,'rb')
	fsize = os.stat(fd.name).st_size
	numlines = 0
	first_line = fd.readline().split('\t')
	while fd.tell()<fsize:
		fline = fd.readline()
		numlines += 1
	fd.close()
	return numlines


################################################################################
has_modules = {}
for module_name in ['paramiko']:
	try:
		exec('import '+module_name)
		has_modules[module_name] = 1
	except ImportError:
		print ('Some '+sys._getframe().f_code.co_filename
		+' functionality requires the package "'+module_name
		+'".  Please "pip install '+module_name+'".  Otherwise some '
		+sys._getframe().f_code.co_filename
		+' functionality will be limited.')
		has_modules[module_name] = 0


################################################################################
def main():
	args = sys.argv
	if len(args) >= 2:
		dset = args[1]
		year = 0
		month = 0
		day = 0
		if len(args) >= 3:
			year = args[2]
			if len(args) >= 4:
				month = args[3]
				if len(args) >= 5:
					day = args[4]
		return get_wrds(dset, year, M=month, D=day)

	return

if __name__ == "__main__":
	main()







