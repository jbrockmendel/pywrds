# Setup instructions for “pywrds”
# Brock Mendel
# jbrockmendel@gmail.com
# Last Update: 2014-08-02


I) How To Use This Document
	A) 

	B) The installation instructions are broken down
	into subsections for Mac, Windows, and Linux 
	platforms, respectively.

	C) Unless specifically mentioned in this file, you can
	ignore all of the programs in the Mac/Windows/Linux 
	folders.  Anything not mentioned here is optional and 
	only needed for advanced usage.

	D) pywrds contains several functions for downloading 
	data from WRDS (Wharton Research Data Services).  See 
	Section V) for instructions on how to configure your 
	WRDS account for optimal performance.





II) Installation
	A) Mac
		1) Python comes built-in on most modern Macs.  Open 
		a Terminal from your Applications/Utilities folder 
		(see Figures 1 and 2).


		2) Enter Python by running the command “python”.
		(See Figure 3)


		3) Depending on your version of Mac OS, you may or 
		may not have “pip” installed by default.  Either way, 
		to make sure you have the most up-to-date version 
		run the command:

		sudo easy_install pip

		(See Figure 4)
		

		Note that when running a command with “sudo”, you will 
		be prompted for your computer password, and must have
		owner-privileges (i.e. be using your own computer).
		Once you have entered your password once, subsequent 
		“sudo” commands do not prompt you again for about 15 
		minutes.

		
		4) The next step requires a working C compiler.  One 
		is provided by Apple but you need to get it from the 
		App Store.  Get Xcode from the App Store.  If you 
		have an old version, you may need to update.  
			i) On Mac OSX 10.9 Mavericks, open a Terminal
			window and run the command:

			xcode-select --install

			and follow the installation instructions that
			pop up.


			ii) On Mac OSX 10.8 or earlier, open Xcode
			from your Applications folder, open the 
			Xcode drop down menu (upper-left corner) 
			and choose go to Preferences.  Hit the 
			Downloads icon.  Download the 
			“Command Line Tools” component.



		5) Once pip is installed, there are thousands of freely 
		available packages listed at https://pypi.python.org/pypi
		which can be installed easily with the command

		sudo pip install antigravity

		The ones you will need are:

		pip install BeautifulSoup
		pip install paramiko

		(See Figure 5)




	B) Windows
		1) Python does not come pre-installed on Windows.  I 
		put into the install/Windows folder two of the many 
		options available.  A bare-bones Python implementation 
		can be installed with:

		python-2.7.5.msi

		while a more feature-rich version can be installed with:

		Anaconda-2.0.1-Windows-x86_64.exe

		Note that there may exist more recent versions of either 
		of these at their respective websites.  I recommend 
		Anaconda for beginners, since it automatically installs 
		everything else described in this section.


		2) Whichever you choose, open it and check that you have 
		the package manager “pip” installed by running:

		easy_install pip

		Your output should resemble that in Figure 4, though your 
		mileage may vary.


		3)  Once pip is installed, there are thousands of freely 
		available packages listed at https://pypi.python.org/pypi
		which can be installed easily with the command

		pip install antigravity

		The ones you will need are:

		pip install BeautifulSoup
		pip install paramiko

		(See Figure 5)

		Note: In my limited experience with Windows, pip only works 
		if executed from the directory "C://Python27/Scripts".  This 
		may be different with Anaconda, I don’t know.


		


	C) Linux
		1) If you use Linux, I Baysume (Bayesian-assume) 
		that you can handle most of these steps without my help.


		2) You should already have python and pip installed, 
		but just in case:

		sudo apt-get install python
		sudo apt-get install python-pip
		sudo pip install BeautifulSoup
		sudo pip install paramiko
		




III) Your “user_info.txt” File
	Copy the file "user_info example.txt" from the "install" folder to 
	your folder, changing the name to "user_info.txt".

	6.1) In that file, set the wrds_username and wrds_institution to their 
	appropriate values.

	6.2) Choose a folder name for where you want the program to store data.  
	In the example, my "reserved_folder" is "Magic".  When the program 
	downloads data, it puts it into the folder "/Users/brock/Documents/Magic" 
	on my hard drive (or sub-folders of that folder).  The program will never 
	read or write anything on your hard drive that is not in A) "Magic" or 
	B) the Dropbox.

	6.3) If you do not want anything saved to your hard drive and only want 
	to operate in the dropbox, set this variable to "".

	6.4) If you have one or more external hard drives on which you prefer 
	to store data, list them in "database_paths".  You will need to create 
	a "Magic" folder on each hard drive you include.



IV) WRDS Configuration
	A) You will need a WRDS account.  If you do not have one, you can
	sign up for one at http://wrds-web.wharton.upenn.edu/

	B) Please respect the WRDS Terms of Use, which can be found here:
	https://wrds-web.wharton.upenn.edu/wrds/about/terms.cfm



V) Usage
	pywrds has the following basic functions for getting data 
	from WRDS.

	

VI) License
	pywrds is distributed freely under a BSD 3-Clause License.  
	See the included “LICENSE.txt” file.

VII) For More Information

VIII) Notes
	A) These tools are a work-in-progress.  If you encounter 
	an error at any point in the installation or usage, please 
	let me know and I will try to fix it promptly.  By doing so
	you will help prevent other people from encountering the same 
	problems.

	B) The most efficient way to notify me of an error is through 
	the Github Issue Tracker: https://github.com/jbrockmendel/pywrds
	
	Email is a fine second-best.
	














5) In order to log in to WRDS remotely without being prompted for a password each 
time, you need to set up an SSH key:
	5.1) Open a command line (Terminal) window and move to your home directory,
	 e.g. "/Users/brock".  In many cases, your home directory will be the default.
	5.2) You need to choose a filename and password for your SSH key.  For reasons 
	that I do not yet understand, it appears that these work best when the file 
	name is set to its default value "id_rsa" and the password is left empty.
	This is not mandatory, but other names may entail trading emails with me 
	until we figure out what's going on.
	5.2) Run the command "ssh-keygen". You should get output that looks 
	something like:
	
	> Generating public/private rsa key pair.
	> Enter file in which to save the key (/Users/brock/.ssh/id_rsa): [HIT ENTER]
	> Enter passphrase (empty for no passphrase): [HIT ENTER]
	> Enter same passphrase again: [HIT ENTER]
	> Your identification has been saved in filename
	> Your public key has been saved in filename.pub
	> The key fingerprint is:
	> RANDOM_LOOKING_STRING_WITH_LOTS_OF_COLONS
	> The key's random art image is:
	> BOX_OF_TEXT_ART
	
	5.3) Notes: 
		a) If you leave filename blank, it will be set to the default "id_rsa".
		b) I copy/pasted the RANDOM_LOOKING_STRING_WITH_LOTS_OF_COLONS and 
		BODY_OF_TEXT_ART to somewhere safe and have never needed them since then.
	
	5.4) Run the command "ls".  This will produce a list of files in your home 
	directory.  Depending on your system, this list may include the files "id_rsa" 
	and "id_rsa.pub".  If so, move these using the commands...
	
	mv id_rsa .ssh/id_rsa
	mv id_rsa.pub .ssh/id_rsa.pub
	
	... In some cases you may get an error to the effect of "Directory .ssh does not exist".
	If this occurs, run...
	
	mkdir .ssh
	mv id_rsa .ssh/id_rsa
	mv id_rsa.pub .ssh/id_rsa.pub
	
	5.5) Upload your public key to the WRDS server with the command...
	
	scp .ssh/id_rsa.pub MY_WRDS_USERNAME@wrds.wharton.upenn.edu:
	
	... Enter your WRDS password when prompted.
	
	5.6) Log in to WRDS with the command...

	ssh -2 -l MY_WRDS_USERNAME wrds.wharton.upenn.edu
	
	... Enter your password when prompted.  In some cases this may be followed by a 
	Yes/No prompt, type "yes" and Enter.

	5.7) Check that the folder "/.ssh" exists in your WRDS directory with the command...
	
	cd .ssh

	... If the folder does not exist, this will produce an error.  In this case,
	create the folder with the command...
	
	mkdir ~/.ssh
	
	5.8) To activate your key on the server, run...
	
	mv id_rsa.pub .ssh/authorized_keys
	chmod 600 .ssh/authorized_keys
	chmod 700 .ssh
	exit
	
	5.9) You're done.  You should be able to log in to the server with...

	ssh MY_USERNAME@wrds.wharton.upenn.edu

	... without being prompted for a password.







7) To test the setup, move to the folder containing ectools.py and enter 
Python by running the command:...

python

... Then import ectools with the command...

import ectools

... Finally, get a WRDS dataset (e.g. crsp monthly stock file for May 1986) with the command...

ectools.get_wrds('crsp.msf',1986,5)



8) ectools is a work in progress, and I have a number of other programs which I 
intend to slowly incorporate.  Thank you for your help in this process.  Suggestions 
on how to make the tools more useful are welcome.
	8.1) I expect to make frequent edits to the ectools.py program, and am 
	will automatically sync my "main" copy with the one in your folder.  Any 
	changes you make to the file are likely to be lost in this event.


