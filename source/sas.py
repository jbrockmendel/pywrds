#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re

from . import static


################################################################################
def compress_sasfile_script(username, outfile, sasfile_path):
	archive_name = "../../../sastemp/"+username+"/"+outfile[:-3]+"--writing.zip"
	path_arg = "../../../sastemp/"+username+"/"+outfile[:-3]+"--writing.zip"
	script_content = (
		"ods package open nopf;" + "\n"
		+ "ods package add file='"+sasfile_path+"';" + "\n"
		+ "ods package publish archive properties (archive_name='"+archive_name+"' PATH='"+path_arg+"');" + "\n"
		+ "ods package close;" + "\n"
		)

	script_filename = 'sas_script'+outfile[:-3]+'.sas'
	return (script_filename, script_content)









################################################################################
def order_data_script(dataset, year, month=0, day=0, rows=[]):
	"""order_data_script(dataset, year, month=0, day=0, rows=[])

	Generate a .sas file which is executed on the WRDS server to produce the
	desired dataset.

	e.g. sample request ...

		DATA new_data;
			SET crsp.dsf (where = ((year(date) between 2008 and 2008) and
			(month(date) between 2 and 2) and (day(date) between 2 and 2)));
			IF (1<= _N_<= 10000000);

		proc export data = new_data
			outfile = "~/crsp_dsf20080202rows1to10000000.tsv"
			dbms = tab
			replace;
			putnames = yes;
		run;

	return (sas_file, output_file, dataset)
	"""
	(Y, M, D, R) = (year, month, day, rows)
	ystr = '' + ('_' + str(Y))*(Y != 'all')
	mstr = '' + (M != 0)*('0'*(M<10)+str(M))
	dstr = '' + (D != 0)*('0'*(D<10)+str(D))
	ymdstr = ystr + mstr + dstr
	sas_file = 'wrds_export_'+re.sub('\.','_',dataset)

	output_dir = static.user_info['server_output_dir']

	if R != []:
		rowstr = 'rows'+str(R[0])+'to'+str(R[1])
		sas_file =  sas_file + ymdstr + rowstr
	else:
		sas_file = sas_file + ymdstr
	script_filename = sas_file + '.sas'

	(dataset, output_file) = static.fix_input_name(dataset, Y, M, D, R)

	where_query = ';\n'
	if Y != 'all':
		dvar = static.wrds_datevar(dataset)
		yquery = '(year('+dvar+') between '+str(Y)+' and '+str(Y)+')'

		mquery = ''
		if M != 0:
			mquery = ' and (month('+dvar+') between '+str(M)+' and '+str(M)+')'

		dquery = ''
		if D != 0:
			dquery = ' and (day('+dvar+') between '+str(D)+' and '+str(D)+')'

		where_query = ' (where = ('
		where_query = where_query + yquery
		where_query = where_query + mquery
		where_query = where_query + dquery
		where_query = where_query + '));\n'

	rowquery = ''
	if R != []:
		rowquery = '\tIF ('+str(R[0])+'<= _N_<= '+str(R[1])+');\n'

	output_path = output_dir + '/' + output_file
	# Not os.path.join because the server is Linux while the client may be Windows
	output_path = re.sub('//+', '/', output_path)

	script_content = (
		'DATA new_data;\n'
		+ '\tSET ' + dataset
		+ where_query
		+ rowquery + '\n'
		+ 'proc export data = new_data\n'
		+ '\toutfile = "'+output_path+'" \n'
		+ '\tdbms = tab \n'
		+ '\treplace; \n'
		+ '\tputnames = yes; \n'
		+ 'run; \n'
		)
	return (script_filename, output_file, script_content)







################################################################################
def describe_columns(libname='OPTIONM', memname=None):
	"""describe_columns(libname='OPTIONM', memname=None)

	Output is written to describe_columns.lst (assuming the file is saved on
	the server as "describe_columns.sas"), everything after a line of -------
	can be split with re.split('  +', line) to get
	(library_name, member_name, column_name)

	return script_content
	"""
	memname_query = ''
	if memname:
		memname_query = ' AND memname="'+memname+'"'

	script_content = (
		'\t' + 'proc sql;' + '\n'
		+'\t' + 'select libname, memname, name from dictionary.columns' + '\n'
		+'\t WHERE libname="'+libname+'"'
		+ memname_query + ';' + '\n'
		+'quit;'
		)

	return script_content








################################################################################
def describe_tables(libname=None):
	"""describe_tables(libname=None)

	Query the SAS system for a list of all tables in the database.  If libname
	is given, the query is limited to that dataset, e.g. libname=COMP
	will limit the results to Compustat tables.

	Output is written to describe_tables.lst (assuming the file is saved on
	the server as "describe_tables.sas"), everything after a line of -------
	can be split with re.split('  +', line) to get
	(library_name, member_name, data_type, num_observations)

	return script_content
	"""
	libname_query = ''
	if libname:
		libname_query = 'WHERE libname="'+libname+'"'

	script_content = (
		'\t' + 'proc sql;' + '\n'
		+'\t' + 'select libname, memname, memtype, nobs from dictionary.tables' + '\n'
		+ libname_query + ';' + '\n'
		+ 'quit;'
		)

	return script_content




################################################################################
def get_dataset_list():
	# This version gives (library_name, pathname)
	script_content = (
		'\t' + 'proc sql;' + '\n'
		+'\t' + 'select distinct libname, path FROM dictionary.libnames;' + '\n'
		+'quit;'
		)

	alternative_script_content = (
		'\t' + 'proc sql;' + '\n'
		'\t' + 'select distinct libname FROM dictionary.tables;' + '\n'
		+'quit;'
		)

	return script_content




################################################################################
def get_columns(libname='crsp', memname='dsf'):
	"""get_columns(libname='crsp', memname='dsf')

	https://wrds-web.wharton.upenn.edu/wrds/research/notes/index.cfm

	Columns are returned one-per-line in get_columns.lst after the ---- line.

	return script_content
	"""
	script_content = (
		'\t' + 'proc sql;' + '\n'
		+ '\t' + 'select distinct quote(trim(name)) into :varlist separated by ","' + '\n'
		+ '\t' + 'from dictionary.columns' + '\n'
		+ '\t' + 'where libname="%upcase('+libname+')" and' + '\n'
		+'\t\t' + 'memname="%upcase('+memname+')";' + '\n'
		+'quit;'
		)

	#%put &varlist # Adding this at the end of the script causes the columns to be
	# listed at the end of get_columns.log, comma-separated, wrapped where
	# applicable.
	return






################################################################################
def get_total_observations(libname='OPTIONM', memname='OPPRCD1997'):
	"""get_num_observations(libname='OPTIONM', memname='OPPRCD1997')

	As far as I can tell this queries the total size from a cache, and therefore
	cannot be subsetted.


	https://stackoverflow.com/questions/19612039/how-to-count-the-number-of-observations-in-a-sas-table

	return script_content
	"""
	script_content = (
		'\t' + 'proc sql;' + '\n'
		+ '\t' + 'select nlobs' + '\n'
		+ '\t' + 'from dictionary.tables' + '\n'
		+ '\t' + 'where libname="'+libname+'"' + '\n'
		+ '\t' + 'and memname="'+memname+'";' + '\n'
		+ 'quit;' + '\n'
		)
	return script_content





################################################################################
def get_num_observations(libname, memname, year_bounds=None, month_bounds=None, day_bounds=None):
	"""get_num_observations(libname, memname, year_bounds=None, month_bounds=None, day_bounds=None)

	Puts the number of observations into a file e.g. count_obs.lst with a
	single integer below a -----

	# Note this is not fast

	return script_content
	"""
	# note memname needs to be e.g. OPPRCD1997, so you can't put just OPPRCD
	bound_query = []
	if year_bounds:
		year_query = '(year(date) between '+str(year_bounds[0])+' and '+str(year_bounds[1])+')'
		bound_query.append(year_query)

	if month_bounds:
		month_query = '(month(date) between '+str(month_bounds[0])+' and '+str(month_bounds[1])+')'
		bound_query.append(month_query)

	if day_bounds:
		day_query = '(day(date) between '+str(day_bounds[0])+' and '+str(day_bounds[1])+')'
		bound_query.append(day_query)

	if bound_query:
		bound_query = 'where (' + ' and '.join(bound_query)+')'
	else:
		bound_query = ''


	script_content = (
		'\t' + 'proc sql;' + '\n'
		+ '\t' + 'select count(*) into : nobs' + '\n'
		+ '\t' + 'from '+libname+'.'+memname + bound_query + ';' + '\n'
		+ 'quit;'
		)
	return script_content


