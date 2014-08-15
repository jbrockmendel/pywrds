# Setup instructions for “pywrds”
# Brock Mendel
# jbrockmendel@gmail.com
# Last Update: 2014-08-13


I) How To Use This Document
	A) If you are new to Python, see Section VI) for setup 
	intructions.  These are broken down into subsections for 
	Mac, Windows, and Linux platforms.

	B) See Section III)B) for important instructions on how to 
	configure your WRDS account for optimal performance.

	C) For specifics on the Open Source license of pywrds, 
	see the "LICENSE.txt" document distributed with this 
	document.



II) Your “user_info.txt” File
	A) Open the "user_info.txt" file in the pywrds directory.

	B) In that file, set the "wrds_username" and "wrds_institution" 
	to their appropriate values.

	C) By default, any files you download from WRDS using pywrds 
	will be put in the pywrds/output directory.  If you want to 
	specify some other directory for these downloads, add to your 
	user_info.txt file an entry (above "wrds_institution"):

	"download_path": "/Some/Other/Download/Path",

	Make sure to include the comma at the end.  If you encounter 
	problems, see:
	https://en.wikipedia.org/wiki/JSON



III) WRDS Configuration
	A) You will need a WRDS account.  If you do not have one, you can
	sign up for one at http://wrds-web.wharton.upenn.edu/
	(See Figure 0 in the "install" directory)


	B) *IMPORTANT* The first time you run a command with pywrds, 
	the program will try to log in to the WRDS server.  At this 
	point you will be prompted for your WRDS password.  If you 
	don't want to be prompted for this password over and over 
	again, you should set up key-based authentication.

	To set up key-based authentication with the WRDS 
	server, run the command:

	pywrds.setup_wrds_key()

	You will be prompted for your WRDS password this one time, 
	but from then on pywrds will be able to log in to the server 
	without a password prompt.  Note that pywrds does not 
	store your password in any form.  See 
	https://en.wikipedia.org/wiki/Key_authentication


	C) Please respect the WRDS Terms of Use, which can be found here:
	https://wrds-web.wharton.upenn.edu/wrds/about/terms.cfm



IV) Usage
	For basic pywrds usage instructions, import pywrds and enter:

	pywrds.__doc__

	For more in-depth instructions, enter:

	pywrds.ectools.get_wrds.__doc__
	pywrds.ectools.wrds_loop.__doc__
	pywrds.ectools.find_wrds.__doc__




V) Other Notes
	A) These tools are a work-in-progress.  If you encounter 
	an error at any point in the installation or usage, please 
	let me know and I will try to fix it promptly.  By doing so
	you will help prevent other people from encountering the same 
	problems.

	B) The most efficient way to notify me of an error is through 
	the Github Issue Tracker: https://github.com/jbrockmendel/pywrds
	
	Email is a fine second-best.
	



VI) Installing Python and pywrds Dependencies
	A) Mac
		1) Python comes built-in on most modern Macs.  Open 
		a Terminal from your Applications/Utilities folder 
		(see Figures 1 and 2 in the "install" directory).


		2) Enter Python by running the command “python”.
		(See Figure 3 in the "install" directory)


		3) Depending on your version of Mac OS, you may or 
		may not have “pip” installed by default.  Either way, 
		to make sure you have the most up-to-date version 
		run the command:

		sudo easy_install pip

		(See Figure 4 in the "install" directory)
		
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

		(See Figure 5 in the "install" directory)




	B) Windows
		0) I have very little experience working with Python on
		Windows.  If you run into issues not described here, or 
		handled incorrectly here, please let me know so I can 
		update these instructions.


		1) Python does not come pre-installed on Windows.  There 
		are several available distributions.  Two of the most 
		basic ones are:

		https://www.python.org/downloads/
		http://continuum.io/downloads

		The latter is called Anaconda, and is my recommendation 
		for users who want something that "just works", since it
		comes with most of your basic needs pre-configured.  
		This includes everything else described in the remainder 
		of this section.


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

		(See Figure 5 in the "install" directory)

		Note 1: In my limited experience with Windows, pip only works 
		if executed from the directory "C://Python27/Scripts".  This 
		may be different with Anaconda, I don’t know.

		Note 2: If you use the Anaconda distribution of Python, 
		it comes with BeautifulSoup installed, but it is labelled 
		"bs4".


	C) Linux
		1) If you use Linux, I Baysume (Bayesian-assume) 
		that you can handle most of these steps without my help.


		2) You should already have python and pip installed, 
		but just in case:

		sudo apt-get install python
		sudo apt-get install python-pip
		sudo pip install BeautifulSoup
		sudo pip install paramiko
		



