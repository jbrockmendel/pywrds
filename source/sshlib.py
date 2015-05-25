#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pywrds.sshlib is essentially a wrapper around paramiko for interacting with
remote servers via SSH and SFTP.  Nothing in sshlib is specific to WRDS.

last edit: 2015-05-18
"""
thisAlgorithmBecomingSkynetCost = 99999999999
import getpass, os, re, signal, socket, string, sys, time
import logging
logger = logging.getLogger(__name__)
################################################################################
# @TODO: Handle BadHostKeyException


################################################################################
def getSSH(ssh, sftp, domain, username, ports=[22]):
	"""
	getSSH(ssh, sftp, domain, username, ports=[22])

	Checks to see if the ssh and sftp objects are active paramiko connections
	to the server at "domain".  If not, the function attempts to initiate a new
	connection.

	The function first trys key-based authentication and then falls back to
	password authentication.  If no password is entered within 10 seconds, the
	function assumes it is being run as part of a script and skips the password
	step.

	return (ssh, sftp)
	"""
	if not has_modules['paramiko']:
		logger.error('sshlib.getSSH is unavailable without dependency '
			+ '"paramiko".  Returning (None, None).'
			)
		return (None, None)
	if sftp:
		try:
			pwd = sftp.getcwd()
		except KeyboardInterrupt:
			raise KeyboardInterrupt
		except (IOError,EOFError,paramiko.SSHException):
			sftp = None
	if ssh and not sftp:
		try:
			sftp = ssh.open_sftp()
			pwd = sftp.getcwd()
		except KeyboardInterrupt:
			raise KeyboardInterrupt
		except (IOError,EOFError,paramiko.SSHException):
			ssh  = None
			sftp = None
	if not ssh:
		ssh = paramiko.SSHClient()
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		key_filename = find_ssh_key(0)
		for port in ports:
			try:
				ssh.connect(domain,
					username=username,
					port=port,
					key_filename=key_filename
					)

				transport = ssh.get_transport()
				tlogger = transport.logger
				tlogger.addHandler(default_handler)

				sftp = ssh.open_sftp()
				break
			except paramiko.AuthenticationException:
				# @TODO: Why is there both default_logger and logger?
				default_logger.info('key-based authentication to '
					+'server '+str(domain) + ' failed, attempting '
					+'password-based authentication')
				try:
					prompt = str(domain)+' password: '
					ssh.connect(domain,
						username=username,
						password=quick_password(prompt=prompt)
						)

					transport = ssh.get_transport()
					tlogger = transport.logger
					tlogger.addHandler(default_handler)

					sftp = ssh.open_sftp()
					break
				except paramiko.AuthenticationException:
					ssh = None
					default_logger.warning(print_func()
						+' could not connect to the server '
						+str(domain)+' with username '
						+str(username)
						)
					break
			except (paramiko.SSHException,socket.error):
				(error_type, error_value, error_traceback) = sys.exc_info()
				if port == ports[-1]:
					ssh = None
					default_logger.error(print_func()+' '
						+ error_type.__module__+'.'+error_type.__name__
						+ ': paramiko could not connect to '
						+ 'the server '+str(domain))
	if sftp:
		pwd = sftp.getcwd()
	return (ssh, sftp)









################################################################################
def find_ssh_key(make=1):
	"""find_ssh_key(make=1)

	Looks for the user's "~/.ssh" directory, or Windows equivalent.

	If it cannot find such a directory and make==1 (the default), it
	will make one.  It returns the directory path.  If it finds no
	such path and make==0, it returns None.

	return key_path
	"""
	home_dir = os.path.expanduser('~')
	ssh_dir = os.path.join(home_dir,'.ssh')
	if not os.path.exists(ssh_dir):
		if make == 1:
			os.makedirs(ssh_dir)
		else:
			logger.warning('find_ssh_key() no SSH keys found, none created.')
			return None
	ssh_dirlist = os.listdir(ssh_dir)
	key_path = os.path.join(ssh_dir,'id_rsa.pub')
	return key_path








################################################################################
def ssh_keygen():
	"""ssh_keygen()

	Looks for the user's RSA keys in the appropriate
	path.  If it finds neither a public key nor a private key,
	it will produce and return a new pair ("id_rsa", "id_rsa.pub").
	If it finds an existing pair, it returns that pair.  If it
	finds only one of the pair existing, it will print a debugging
	message and exit without changing anything.

	return key_path
	"""
	## Bug 2014-08-21: open(...,'wb') appears to cause a OSError 13
	## on Windows systems.
	if not has_modules['Crypto.PublicKey.RSA']:
		logger.warning('sshlib.ssh_keygen is unavailable without '
			+'dependency "Crypto.PublicKey.RSA".  Returning None.'
			)
		return None

	key_path = find_ssh_key(make=1)
	ssh_dir = os.path.split(key_path)[0]
	ssh_dirlist = os.listdir(key_path)

	pub_path = os.path.join(ssh_dir, 'id_rsa.pub')
	pub_read = os.path.exists(pub_path) and os.access(pub_path, os.R_OK)
	pub_write = pub_read and os.access(pub_path, os.W_OK)

	priv_path = os.path.join(ssh_dir, 'id_rsa')
	priv_read = os.path.exists(priv_path) and os.access(priv_path, os.R_OK)
	priv_write = pub_read and os.access(priv_path, os.W_OK)

	# @TODO: warn and return in cases of permission problems.
	if 'id_rsa' not in ssh_dirlist and 'id_rsa.pub' not in ssh_dirlist:
		key = Crypto.PublicKey.RSA.generate(2048)
		with open(priv_path, 'wb') as fd:
			fd.write(key.exportKey())

		with open(pub_path, 'wb') as fd:
			fd.write(key.publickey().exportKey('OpenSSH'))

		os.chmod(priv_path, 600)
		os.chmod(pub_path, 600)
		os.chmod(ssh_dir, 700)
		home_dir = os.path.split(ssh_dir)[0] # alt: os.path.expanduser('~')
		os.chmod(home_dir,700)
	elif 'id_rsa' in ssh_dirlist:
		with open(priv_path, 'rb') as fd:
			private_key = fd.read()
		# begin unmerged lines from cpt branch
        ## BUG: PyCrypto not working with password encrypted keys, see
        ## http://stackoverflow.com/questions/20613946/how-can-i-read-a-standard-openssl-rsa-private-key-with-pycrypto-and-decrypt-with
        ## Plus, not sure why you need all this when you just pass back the
        ## key_path.
        # end unmgerged lines from cpt branch, everything below
        # here in this "elif" block is commented out in that branch.
		pk = Crypto.PublicKey.RSA.importKey(private_key)
		public_key = pk.publickey().exportKey('OpenSSH')
		with open(pub_path, 'wb') as fd:
			fd.write(public_key)

		os.chmod(priv_path, 600)
		os.chmod(pub_path, 600)
		os.chmod(ssh_dir, 600)
		home_dir = os.path.split(ssh_dir)[0] # alt: os.path.expanduser('~')
		os.chmod(home_dir, 700)
	elif 'id_rsa.pub' in ssh_dirlist:
		logger.warning('ssh_keygen() expected the directory '+ssh_dir
			+ ' to contain either zero or both of "id_rsa", "id_rsa.pub",'
			+ ' but only "id_rsa.pub" was found.  Aborting ssh_keygen on '
			+ 'the assumption that this situation is intentional on the '
			+ 'part of the user.'
			)
		key_path = None
	return key_path








################################################################################
def put_ssh_key(domain, username):
	"""put_ssh_key(domain, username)

	Attempts to log in to the given server using the given username.
	If key-based authentication is not yet set up, the user will be
	prompted for an account password.

	If login is successful, the function will find the user's
	public key (generating one if needbe) and put it in
	the appropriate location on the server.  It will set
	the proper permissions before logging out and testing
	the new connection.

	return (ssh, sftp)
	"""
	if not has_modules['paramiko']:
		logger.warning('sshlib.put_ssh_key is unavailable without dependency "paramiko"'
			+'  Returning [None, None].')
		return (None, None)

	key_path = ssh_keygen()
	if not key_path:
		logger.warning('put_wrds_key() cannot run until the error '
			'produced by ssh_keygen() is resolved.')
		return (None, None)

	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	try:
		ssh.connect(domain, username=username, key_filename=key_path)
		sftp = ssh.open_sftp()
		logger.info('key-based authentication is already set up '
			+'on the server ' + domain)
		return (ssh, sftp)
	except paramiko.AuthenticationException:
		[ssh, sftp] = [None, None]

	try:
		strprompt = str(domain)+' password: '
		ssh.connect(domain,username=username,
			password=quick_password(prompt=strprompt))
		sftp = ssh.open_sftp()
	except KeyboardInterrupt:
		raise KeyboardInterrupt
	except paramiko.AuthenticationException:
		(ssh, sftp) = (None, None)
		logger.error('paramiko.AuthenticationException, '
			+ 'could not connect to the server '+str(domain)
			+ ' with username '+str(username)
			)
	except (IOError,EOFError,paramiko.SSHException):
		(ssh, sftp) = [None, None]
		logger.error('paramiko could not connect to the server '+str(domain)
			+ ' with username '+str(username)
			)

	if not sftp:
		logger.error('Connection to domain '+domain+' failed, '
			+'put_ssh_key() returning unsuccessfully')
		return (ssh, sftp)

	remote_list = sftp.listdir()
	if '.ssh' not in remote_list:
		sftp.mkdir('.ssh')
	remote_list2 = sftp.listdir('.ssh')
	if 'authorized_keys' not in remote_list2:
		sftp.put(key_path, '.ssh/authorized_keys')
	else:
		sftp.put(key_path, 'authorized_keys-temp')
		cat_cmd = 'cat authorized_keys-temp >> .ssh/authorized_keys'
		(stdin, stdout, stderr) = ssh.exec_command(cat_cmd)

	sftp.chmod('.ssh/authorized_keys',600)
	sftp.chmod('.ssh',700)
	sftp.close()
	ssh.close()

	logger.info('SSH key successfully deposited on the '
		+domain+' server.  Checking that passwordless '
		+'login works correctly...')
	try:
		ssh.connect(domain, username=username, key_filename=key_path)
		logger.info('Passwordless login was successful.')
		sftp = ssh.open_sftp()
		success = 1
	except paramiko.AuthenticationException:
		[error_type, error_value, error_traceback] = sys.exc_info()
		logger.error('Passwordless login was unsuccessful.  '
			+ 'Debugging information follows...\n'
			+ 'error_type='+str(error_type)+'\n'
			+ 'error_value='+str(error_value)+'\n'
			+ 'error_traceback='+str(error_traceback))
		[ssh, sftp] = [None, None]
	return (ssh, sftp)





























################################################################################
################################################################################
def _put_carefully(local_path, remote_path, ssh, sftp, domain, username, ports, lag=60):
	"""_put_carefully(local_path, remote_path, ssh, sftp, domain, username, ports)

	Trys three times to sftp the file at local_path on the server at
	remote_path, creating intermediate directories if necessary,
	reinitiating the ssh connection if needbe.

	The function returns successful if it finds that the file
	already exists at remote_path and it has the same size as
	the file at local_path.

	Alternately if the file exists at remote_path and has both
	a larger file size and a more recent modification time, the
	function returns successfully.

	return (ssh, sftp, success, time_elapsed)
	"""
	tic = time.time()
	[success, num_trys, max_trys] = [0, 0, 3]
	(dname, fname) = os.path.split(local_path)

	(ssh, sftp, go_on, success) = _check_stats(local_path, remote_path, ssh, sftp, domain, username, ports, lag)
	if not go_on:
		return (ssh, sftp, success, time.time()-tic)

	(success, ssh, sftp) = _try_put(local_path, remote_path, ssh, sftp, domain, username, ports)
	return (ssh, sftp, success, time.time()-tic)





################################################################################
def _try_put(local_path, remote_path, ssh, sftp, domain, username, ports=[22]):
	"""_try_put(local_path, remote_path, ssh, sftp, domain, username, ports)

	Trys to sftp the file at local_path on the server at
	remote_path, reinitiating the ssh connection if needbe.

	return (ssh, sftp, success)
	"""
	[success, numtrys, maxtrys] = [0, 0 ,3]
	local_stat = os.stat(local_path)
	while success == 0 and numtrys < maxtrys:
		try:
			remote_attrs = sftp.put(local_path, remote_path)
			if remote_attrs.st_size == local_stat.st_size:
				success = 1
		except KeyboardInterrupt:
			try:
				sftp.remove(remote_path)
			except (IOError,EOFError,paramiko.SSHException):
				pass
			raise KeyboardInterrupt
		except (IOError,EOFError,paramiko.SSHException):
			(ssh, sftp) = getSSH(ssh, sftp, domain, username, ports)
			try:
				sftp.remove(remote_path)
			except (IOError,EOFError,paramiko.SSHException):
				pass
		numtrys += 1
	return (success, ssh, sftp)






################################################################################
def _check_stats(local_path, remote_path, ssh, sftp, domain, username, ports, lag):
	"""_check_stats(local_path, remote_path, ssh, sftp, domain, username, ports, lag)

	Checks whether the file exists at local_path and has remained unchanged
	for at least "lag" seconds.  If not,it returns a code go_on=0 indicating that
	this file should be skipped by any downloading script.

	Otherwise the function checks for the existence of the file at remote_path.
	If the file does not exist, it returns go_on=1.

	If the remote file exists, the function checks whether that file has been
	modified in the last lag seconds, and if so returns go_on=0.

	In each of these last three cases, along with go_on the function returns
	success=0 indicating that the transfer has not yet been accomplished.

	If both files exist and neither file has been modified in the last lag
	seconds, the function checks if the two files are the same size.  If so,
	it returns go_on=0 and success=1 indicating that the file can be skipped
	because the transfer has already occurred successfully.

	If the remote file is both bigger and more recently modified than the local
	file, the function returns go_on=0, success=1, assuming that the newer
	bigger file is the correct version.

	In all other cases, the function returns go_on=1, success=0.

	return (ssh, sftp, go_on, success)
	"""
	if not os.path.exists(local_path):
		return (ssh, sftp, 0, 0)

	local_stat = os.stat(local_path)
	time_diff = time.time() - local_stat.st_mtime
	[waits, max_waits] = [0, 3]
	while time_diff < lag and waits < max_waits:
		time.sleep(lag - time_diff)
		local_stat = os.stat(local_path)
		time_diff = time.time() - local_stat.st_mtime
		waits += 1

	if waits == max_waits:
		return (ssh, sftp, 0, 0)

	try:
		remote_stat = sftp.stat(remote_path)
	except IOError:
		# IOError: [Errno 2] No such file #
		remote_stat = None

	if remote_stat:
		time_diff = time.time() - remote_stat.st_mtime
		[waits, max_waits] = [0, 3]
		while  time_diff < lag and waits < max_waits:
			time.sleep(lag - time_diff)
			remote_stat = sftp.stat(remote_path)
			time_diff = time.time() - remote_stat.st_mtime
			waits += 1

		if waits == max_waits:
			return (ssh, sftp, 0, 0)

		if remote_stat.st_size == local_stat.st_size:
			return (ssh, sftp, 0, 1)

		if (remote_stat.st_mtime > local_stat.st_mtime
			and remote_stat.st_size > local_stat.st_size):
			return (ssh, sftp, 0, 1)

	return (ssh, sftp, 1, 0)







################################################################################
def _try_get(ssh, sftp, domain, username, remote_path, local_path, ports=[22]):
	"""_try_get(ssh, sftp, domain, username, remote_path, local_path, ports=[22])

	Trys three times to download a file from the remote ssh server
	from remote_path to local_path.  If a connection error occurs, it
	is re-established.

	_try_get does *not* check that the remote file exists, that
	the local_path is not already in use, or that there is enough
	space free on the local disk to complete the download.

	return (success_boolean, time_elapsed)
	"""
	tic = time.time()
	[success, numtrys, maxtrys] = [0, 0, 3]
	while success == 0 and numtrys < maxtrys:
		try:
			sftp.get(remotepath=remote_path, localpath=local_path)
			success = 1
		except (paramiko.SSHException,paramiko.SFTPError,IOError,EOFError):
			if os.path.exists(local_path):
				os.remove(local_path)
			(ssh, sftp) = getSSH(ssh, sftp, domain=domain, username=username)
			numtrys += 1
		except KeyboardInterrupt:
			if os.path.exists(local_path):
				os.remove(local_path)
			raise KeyboardInterrupt

	return (success, ssh, sftp, time.time()-tic)









################################################################################
def _try_listdir(remote_dir, ssh, sftp, domain, username, ports=[22]):
	"""_try_listdir(remote_dir, ssh, sftp, domain, username, ports=[22])

	Trys three times to get a a list of files and their attributes from the
	directory remote_dir on the remote server, reinitiating the ssh connection
	if necessary.

	Creates a dictionary fdict = {filename: [attributes]} across
	the files in the remote directory.

	returns (ssh, sftp, fdict)
	"""
	fdict = {}
	remote_list = []
	[success, numtrys, maxtrys] = [0, 0, 3]
	while success == 0 and numtrys < maxtrys:
		try:
			remote_list = sftp.listdir_attr(remote_dir)
			success=1
		except (IOError,EOFError,paramiko.SSHException):
			(ssh, sftp) = getSSH(ssh, sftp, domain, username, ports)
			numtrys += 1

	fdict = {x.filename: x for x in remote_list}
	return (ssh, sftp, fdict)







################################################################################
def _try_get_remote_stats(remote_path, ssh, sftp, domain, username, ports):
	"""_try_get_remote_stats(remote_path, ssh, sftp, domain, username, ports)

	Trys to find the stats (like os.stat) of the file at remote_path,
	reinitiating the connection if necessary.  The output includes
	an indicator which is True if the stats are found, and False,
	otherwise (usually indicating the file does not exist).

	returns (ssh, sftp, remote_size, exists_boolean)
	"""
	stats = None
	[success, numtrys, maxtrys] = [0, 0, 3]
	while success == 0 and numtrys < maxtrys:
		try:
			stats = sftp.stat(remote_path)
			success = 1
		except (IOError,EOFError,paramiko.SSHException):
			(ssh, sftp) = getSSH(ssh, sftp, domain, username, ports)
			numtrys += 1

	return (ssh, sftp, stats, success)







################################################################################
def _try_exec(command, ssh, sftp, domain, username, ports=[22]):
	# @ TODO: wait for success, capture stdout, stderr messages
	[success, numtrys, maxtrys] = [0, 0 ,3]
	[stdin, stdout, stderr] = [None, None, None]
	while not success and numtrys < maxtrys:
		try:
			(stdin, stdout, stderr) = ssh.exec_command(command)
			success = 1
		except (IOError,EOFError,paramiko.SSHException):
			(ssh, sftp) = getSSH(ssh, sftp, domain=domain, username=username)
			numtrys += 1

	return (success, stdin, stdout, stderr, ssh, sftp)







################################################################################
def print_func(level=1):
	"""print_func(level=1)

	When called from within function "myfunction" defined in module
	"mymodule", print_func(1) returns the string "mymodule.myfunction".
	setting level=2 returns a string corresponding to the function
	that called myfunction, and so on.

	This is designed for identifying the sources of errors in logging.

	return module.function
	"""
	module_name   = sys._getframe(level).f_code.co_filename.split('/')[-1][:-3]
	function_name = sys._getframe(level).f_code_co_name
	return module_name +'.'+ function_name




################################################################################
def timeout_decorator(timeout_time, default):
	"""timeout_decorator(timeout_time, default)

	A function decorated with timeout_decorator(timeout_time,default)
	either finished within "timeout_time" seconds or will exit and
	return "default".

	Copied with minor alterations from
	http://pguides.net/python-tutorial/python-timeout-a-function/

	return timeout_function
	"""
	# @TODO: Note that signal is *NIX only
	def timeout_function(f):
		def f2(*args):
			def timeout_handler(signum, frame):
				raise TimeoutException()

			old_handler = signal.signal(signal.SIGALRM, timeout_handler)
			signal.alarm(timeout_time)
			try:
				retval = f()
			except TimeoutException:
				return default
			finally:
				signal.signal(signal.SIGALRM, old_handler)
			signal.alarm(0)
			return retval
		return f2
	return timeout_function

class TimeoutException(Exception):
	pass

@timeout_decorator(10, '')
def quick_password(prompt="Please enter your password: "):
	"""quick_password(prompt="Please enter your password: ")

	Prompt the user for a password.  The decorated function
	gives an upper bound of 10 seconds on how long it will wait
	for input before assuming an empty string.

	return password
	"""
	password = getpass.getpass(prompt)
	return password



################################################################################
default_logger = logging.getLogger('log')
default_logger.setLevel(logging.INFO)
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
default_formatter = logging.Formatter(log_format)
default_handler = logging.StreamHandler()
default_handler.setLevel(logging.INFO)
default_handler.setFormatter(default_formatter)
default_logger.addHandler(default_handler)




################################################################################
has_modules = {}
try:
	import paramiko
	has_modules['paramiko'] = 1
except ImportError:
	logger.warning('Some pywrds.sshlib'
		+ ' functionality requires the package "paramiko".'
		+ '  Please "pip install paramiko".  Otherwise some '
		+ ' functionality will be limited.'
		)
	has_modules['paramiko'] = 0

try:
	import Crypto.PublicKey.RSA
	has_modules['Crypto.PublicKey.RSA'] = 1
except ImportError:
	logger.warning('Some pywrds.sshlib'
		+ ' functionality requires the package "Crypto.PublicKey.RSA".'
		+ '  Please "pip install pycrypto".  Otherwise some '
		+ ' functionality will be limited.\n'

		+ "If this erorr occurs again after installing pycrypto, "
		+ ", there may be an issue with "
		+ 'Python recognizing "crypto" instead of "Crypto".  This '
		+ 'can be duct-taped by changing the directory name where '
		+ ' "crypto" is installed to "Crypto".'
		+ "  This is purely based on the author's experience.  "
		+ "Your mileage may vary"
	)
	has_modules['Crypto.PublicKey.RSA'] = 0


