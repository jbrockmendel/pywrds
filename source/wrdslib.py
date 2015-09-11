#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pywrds.wrdslib is a collection of functions used by ectools which use or
provide information specific to the WRDS SAS system.

last edit: 2015-08-28
"""
thisAlgorithmBecomingSkynetCost = 99999999999
import datetime, os, re, sys, time
import logging
logger = logging.getLogger(__name__)

try: import simplejson as json
except ImportError: import json

################################################################################
from . import sshlib, static

from .wrds_parameters import wrds_domain, _get_all, first_dates, \
    first_date_guesses, date_vars, date_var_guesses, autoexec_text, \
    bytes_per_line


from .static import user_info, wrds_datevar, download_path, \
    wrds_institution, wrds_username



now = time.localtime()
(this_year, this_month, today) = (now.tm_year, now.tm_mon, now.tm_mday)




################################################################################
def check_quota(ssh):
    """check_quota(ssh)

    Check how much of your account's storage quota on the WRDS server
    is full.

    Output variables (usage, quota, limit) are in kilobytes.  If the "quota -v"
    command is unsuccessful, "usage" and "limit" defualt to None, and "quota"
    defaults to 2^30 (1 GB).

    return (usage, quota, limit)
    """
    (usage, quota, limit) = (None, 2**30, None)
    (stdin, stdout, stderr) = ssh.exec_command('quota -v')
    (slept, exit_status, max_sleep) = (0, None, 3)
    while exit_status == None and slept < max_sleep:
        time.sleep(1)
        slept += 1
        exit_status = stdout.channel.recv_exit_status()

    if exit_status == 0:
        output = stdout.read()
        flines = output.splitlines()
        split_lines = [re.split(' +', x.strip()) for x in flines]
        assert len(split_lines) >= 3 ## don't actually do it like this

        #assert len(split_lines[1]) == len(split_lines[2])
        #fdict = {split_lines[1][n]: split_lines[2][n] for n in range(len(split_lines[1]))}

        assert split_lines[1][1:4] == ['usage', 'quota', 'limit'], split_lines
        (usage, quota, limit) = split_lines[2][1:4]
        try:
            usage = int(usage)
        except ValueError:
            pass
        try:
            quota = int(quota)
        except ValueError:
            pass
        try:
            limit = int(limit)
        except ValueError:
            pass
        if all(isinstance(var,int) for var in [usage, quota, limit]):
            user_info['usage'] = usage
            user_info['quota'] = quota
            user_info['limit'] = limit

    else:
        # @TODO: Fall back to manually checking the sum size of files in the
        # user directory.
        pass
    return (usage, quota, limit)











################################################################################
def estimate_bytes_per_line(dataset):
    # Until something smarter is implemented, use 1024 as a conservative bound
    # This bound seems to work for, say, tfn.s34 which has lines of ~140 chars
    # but may fail for, say, compustat.fundq which has lines of ~2100 chars
    return bytes_per_line.get(dataset, 1024)




default_max_usage = .5
def adjust_rows_using_quota(dataset, ssh):
    (usage, quota, limit) = check_quota(ssh)
    rows_per_file = static.rows_per_file_adjusted(dataset)

    if not isinstance(usage,int) or not isinstance(quota, int):
        # No useful info from the quota check.
        return rows_per_file

    free_bytes = (quota - usage)*1024

    if free_bytes <= 0:
        # @TODO: warn, stop download
        pass

    bytes_per_line = estimate_bytes_per_line(dataset)

    new_num_lines = int(free_bytes*default_max_usage/bytes_per_line)

    if user_info['server_output_dir'] != "~/":
        new_num_lines = new_num_lines*10
    return new_num_lines







################################################################################
def fix_input_name(dataset, year, month, day, rows=[]):
    """fix_input_name(dataset, year, month, day, rows=[])

    Adjust the user-supplied dataset name to use the same upper/lower case
    conventions used on the WRDS server.

    return (dataset, output_file)
    """
    # @TODO use dictionary.tables to get this right automatically
    (Y, M, D, R) = (year, month, day, rows)
    if year != 'all':
        ystr = '_'*(dataset[-1].isdigit())+str(Y)
        mstr = '' + (M != 0)*('0'*(month<10)+str(M))
        dstr = (D != 0)*('0'*(D<10)+str(D))
        ymdstr = ystr + mstr + dstr +'.tsv'
        output_file = re.sub('\.', '_', dataset) + ymdstr
    else:
        output_file = re.sub('\.', '_', dataset)+'.tsv'

    if dataset.lower() == 'optionm.opprcd':
        dataset = dataset+str(year)

    elif dataset.lower() in ['taq.cq', 'taq.ct']:
        dataset = re.sub('cq', 'CQ', dataset)
        dataset = re.sub('ct', 'CT', dataset)
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
        output_file = re.sub('.tsv$', '', output_file) + rowstr

    return (dataset, output_file)



################################################################################
def wrds_sas_script(dataset, year, month=0, day=0, rows=[]):
    """wrds_sas_script(dataset, year, month=0, day=0, rows=[])

    Generate a .sas file which is executed on the WRDS server to produce the
    desired dataset.

     e.g. sample request.

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

    _out_dir = user_info['server_output_dir']

    if R != []:
        rowstr = 'rows'+str(R[0])+'to'+str(R[1])
        sas_file =  sas_file + ymdstr + rowstr
    else:
        sas_file = sas_file + ymdstr
    sas_file = sas_file + '.sas'

    (dataset, output_file) = fix_input_name(dataset, Y, M, D, R)

    where_query = ';\n'
    if Y != 'all':
        dvar = wrds_datevar(dataset)
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

    sas_content = (
        'DATA new_data;\n'
        + '\tSET ' + dataset
        + where_query
        + rowquery + '\n'
        + 'proc export data = new_data\n'
        + '\toutfile = "'+_out_dir+output_file+'" \n'
        + '\tdbms = tab \n'
        + '\treplace; \n'
        + '\tputnames = yes; \n'
        + 'run; \n'
        )

    if _use_zip:
        sas_content = sas_content.replace('"'+_out_dir+output_file+'"', 'writer')
        output_file = os.path.splitext(output_file)[0]+'.zip'
        front_content = "filename writer pipe 'compress > " + _out_dir+output_file+ "';\n"
        sas_content = front_content + sas_content

    with open(os.path.join(download_path, sas_file), 'wb') as fd:
        ## @BUG: IOErro --> Permission denied
        fd.write(sas_content)

#   fd = open(os.path.join(download_path, sas_file), 'wb')
#   fd.write('DATA new_data;\n')
#   fd.write('\tSET '+dataset)
#   if Y != 'all':
#       where_query = ' (where = ('
#       year_query = ('(year('+wrds_datevar(dataset)+')'
#           +' between '+str(Y)+' and '+str(Y)+')')
#       where_query = where_query + year_query
#
#       if M != 0:
#           month_query = (' and (month('+wrds_datevar(dataset)
#               +') between '+str(M)+' and '+str(M)+')')
#           where_query = where_query+month_query
#
#       if D != 0:
#           day_query = (' and (day('+wrds_datevar(dataset)
#               +') between '+str(D)+' and '+str(D)+')')
#           where_query = where_query+day_query
#
#       where_query = where_query+'));\n'
#       fd.write(where_query)
#   else:
#       fd.write(';\n')
#
#   if R != []:
#       rowquery = ('\tIF ('+str(R[0])+'<= _N_<= '+str(R[1])+');\n')
#       fd.write(rowquery)
#
#   fd.write('\n')
#   fd.write('proc export data = new_data\n')
#   fd.write(('\toutfile = "~/'+output_file+'" \n'
#       +'\tdbms = tab \n'
#       +'\treplace; \n'
#       +'\tputnames = yes; \n'
#       +'run; \n')
#   )
#   fd.close()

    os.chmod(os.path.join(download_path, sas_file), 777)
    return (sas_file, output_file, dataset)
################################################################################






################################################################################
def update_user_info(numfiles, new_files, fname, dataset, year, month=0, day=0):
    """update_user_info(numfiles, new_files, fname, dataset, year, month=0, day=0)

    Amend the user_info file to reflect the most recent download dates
    for wrds files.

    return
    """
    if new_files > 0:
        numfiles = numfiles + new_files
        last_wrds_download = user_info.setdefault('last_wrds_download', {})
        last_wrds_download[dataset] = year*10000 + month*100 + day
        content = json.dumps(user_info, indent=4)
        with open(static.user_info_filename, 'wb') as fd:
            fd.write(content)
    else:
        logger.error('Could not retrieve: ' + fname)
    return
################################################################################




################################################################################
def setup_wrds_key():
    """setup_wrds_key()

    Set up a key-based authentication on the wrds server, so that the user can
    log in without a password going forward.

    return (ssh, sftp)
    """
    if not wrds_username:
        logger.warning('setup_wrds_key() cannot run until wrds_username is '
            +'specified in the user_info.txt file.'
            )
        return (None, None)
    (ssh, sftp) = sshlib.put_ssh_key(domain=wrds_domain, username=wrds_username)
    institution = get_wrds_institution(ssh, sftp)
    return (ssh, sftp)



################################################################################
def get_wrds_institution(ssh, sftp):
    """get_wrds_institution(ssh, sftp)

    Get the institution associated with the user's account on the wrds server.

    return institution_path
    """
    (ssh, sftp) = sshlib.getSSH(ssh, sftp, domain=wrds_domain, username=wrds_username)
    if not sftp:
        return None
    try:
        wrds_path = sftp.normalize(path='.')
    except IOError:
        logger.error('sftp cannot resolve a path on the wrds server')
        return None

    institution_path = re.sub('/home/', '', wrds_path).split('/')[0]
    if wrds_institution != institution_path:
        if wrds_institution == []:
            wrds_institution = institution_path
            user_info['wrds_institution'] = wrds_institution
            content = json.dumps(user_info, indent=4)
            with open(static.user_info_filename, 'wb') as fd:
                fd.write(content)
        else:
            logger.warning('user_info["wrds_institution"] does not '
                + 'match the directory "'+institution_path+'" '
                + 'found on the wrds server.  '
                + 'This mismatch may cause errors '
                + 'in the download process.'
                )
    return institution_path



