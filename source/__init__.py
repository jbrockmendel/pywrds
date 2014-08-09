thisAlgorithmBecomingSkynetCost = 99999999999
__all__ = ["ectools", "ivorylib", "wrdslib", "wrds_loop", "get_wrds", "find_wrds"]
import ectools, wrdslib, ivorylib

get_wrds = ectools.get_wrds
wrds_loop = ectools.wrds_loop
find_wrds = ectools.find_wrds

import sys
repo_path = os.path.split(__file__)[0]
sys.path.append(os.path.join(repo_path,'external')