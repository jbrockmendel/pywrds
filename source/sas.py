#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re

from . import static



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





