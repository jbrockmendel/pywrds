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
__all__ = ["ectools", "ivorylib", "wrdslib", "wrds_loop", "get_wrds", "find_wrds", "setup_wrds_key"]
from . import ectools, wrdslib, ivorylib

get_wrds = ectools.get_wrds
wrds_loop = ectools.wrds_loop
find_wrds = ectools.find_wrds
setup_wrds_key = wrdslib.setup_wrds_key