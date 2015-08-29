#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pywrds.wrdslib is a collection of functions used by ectools which use or
provide information specific to the WRDS SAS system.

last edit: 2015-07-17
"""
thisAlgorithmBecomingSkynetCost = 99999999999
import datetime, os, re, sys, time
import logging
logger = logging.getLogger(__name__)

try: import simplejson as json
except ImportError: import json

################################################################################
from . import sshlib

from .wrds_parameters import wrds_domain, _get_all, first_dates, \
    first_date_guesses, date_vars, date_var_guesses, autoexec_text, \
    bytes_per_line

now = time.localtime()
(this_year, this_month, today) = (now.tm_year, now.tm_mon, now.tm_mday)

################################################################################
user_info = {}
this_file = os.path.abspath(__file__)
user_path = this_file.split('source')[0]
user_info_filename = os.path.join(user_path,'user_info.txt')
if os.path.exists(user_info_filename):
    with open(user_info_filename, 'r') as fd:
        # r instead of rb for Python3 compatibility
        content = fd.read()
    content = content.replace(u'\xe2\x80\x9c', u'"')
    content = content.replace(u'\xe2\x80\x9d', u'"')
    # Fix "smart" quotes sometimes inserted by text editors.
    try:
        new_user_info = json.loads(content)
        user_info.update(new_user_info)
    except ValueError:
        logger.warning('pywrds.wrdslib warning: user_info.txt file does not '
            + 'conform to json format.  Please address this '
            + 'and reload ectools.'
            )
else:
    logger.warning('pywrds.wrdslib warning: Please create a user_info.txt '
        + 'file conforming to the format given in the '
        + 'user_info_example.txt file.'
        )


################################################################################
download_path = os.path.join(user_path, 'output')
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
    rows_per_file = rows_per_file_adjusted(dataset)

    if not isinstance(usage,int) or not isinstance(quota, int):
        # No useful info from the quota check.
        return rows_per_file

    free_bytes = (quota - usage)*1024

    if free_bytes <= 0:
        # @TODO: warn, stop download
        pass

    bytes_per_line = estimate_bytes_per_line(dataset)

    new_num_lines = int(free_bytes*default_max_usage/bytes_per_line)

    if _out_dir != "~/":
        new_num_lines = new_num_lines*10
    return new_num_lines


################################################################################
def rows_per_file_adjusted(dataset):
    """rows_per_file_adjusted(dataset)

    Choose a number of rows to query in each _get_wrds_chunk request to ensure
    that the files produced do not approach the 1 GB server limit.  For most
    datasets, 10^7 rows in a file is not a problem.  For optionm.opprcd,
    this number is dropped to 10^6.

    To date optionm.opprcd is the only dataset for which this has
    consistently been necessary.  This is subject to change
    with further use cases.

    return rows_per_file
    """
    # @TODO: query the server for file sizes and number of lines, use
    # this to determine the rows_per_file
    rows_per_file = 10**7
    if dataset.replace('.', '_') == 'optionm_opprcd':
        rows_per_file = 10**6
    elif dataset.replace('.', '_') == 'optionm_optionmnames':
        rows_per_file = 10**6
    return rows_per_file



################################################################################
def get_ymd_range(min_date, dataset, weekdays=1):
    """get_ymd_range(min_date, dataset, weekdays=1)

    Get a list of tuples (year, month, date) over which to iterate in wrds_loop.
    Some datasets include very large files and need to be queried
    at a monthly or daily frequency to prevent giant files from
    causing problems on the server.

    return ymdrange
    """
    (min_year, min_month, min_day) = min_YMD(min_date, dataset)

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
    """get_loop_frequency(dataset, year)

    Find the best frequency at which to query the server for the given dataset
    so as to avoid producing problematically large files.

    return frequency
    """
    # @TODO: Check the file size on the server and use a set rule instead
    # of making case-by-case decisions.
    frequency = 'Y'
    if dataset.startswith('optionm'):
        if year < 2008:
            frequency = 'M'
        else:
            frequency = 'D'
    elif re.search('det_xepsus', dataset, flags=re.I):
        if year > 2005:
            frequency = 'M'
    elif re.search('det_xepsint', dataset, flags=re.I):
        if year > 2003:
            frequency = 'M'
    elif re.search('taq', dataset, flags=re.I):
        frequency = 'D'
    return frequency


################################################################################
def fix_weekdays(ymds, weekdays=1):
    """fix_weekdays(ymds, weekdays=1)

    Take a set of (year, month, date) tuples "ymds" and removes those which
    are not valid days, e.g. June 31, February 30.

    If weekdays is set to its default value of 1, also remove
    Saturday and Sundays.

    return ymds
    """
    # @TODO: holidays, see pypi's "holidays"
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

    return (sas_file, output_file, dataset)
    """
    (Y, M, D, R) = (year, month, day, rows)
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

    with open(os.path.join(download_path, sas_file), 'wb') as fd: ## @BUG: IOErro --> Permission denied
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
        if 'last_wrds_download' not in user_info.keys():
            user_info['last_wrds_download'] = {}
        user_info['last_wrds_download'][dataset] = year*10000 + month*100 + day
        content = json.dumps(user_info, indent=4)
        with open(user_info_filename, 'wb') as fd:
            fd.write(content)
    else:
        logger.error('Could not retrieve: ' + fname)
    return
################################################################################




################################################################################
def min_YMD(min_date, dataset):
    """min_YMD(min_date, dataset)

    Find (year, month, day) at which to start wrds_loop when downloading the
    entirety of a dataset.  Check user_info to find what files have
    already been downloaded.

    return (min_year, min_month, min_day)
    """
    if dataset in _get_all:
        return (-1, -1, -1)

    last_download = user_info.setdefault('last_wrds_download', {})
    dataset_1st_date = first_dates.get(dataset, 18000000)
    dataset_last_download = last_download.setdefault(dataset, dataset_1st_date)

    if not isinstance(min_date,(int,float)):
        min_date = 0

    if min_date == 0:
        min_date = user_info['last_wrds_download'][dataset]
        if not str(min_date.isdigit()) or len(str(min_date)) != 8:
            logger.warning('user_info["last_wrds_download"]["'+dataset+'"]='
                +str(min_date)+' error, should be an eight digit integer.'
                )
             min_date = 0
        min_date = int(min_date)
        (min_year, leftover) = divmod(min_date, 10000)
        (min_month, min_day) = divmod(leftover, 100)
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
                wday = datetime.date(min_month, min_month, min_day).weekday()
            except ValueError:
                min_day = 1
                min_month += 1
                if min_month == 13:
                    min_month = 1
                    min_year += 1

    if min_date != 0:
        if min_date < 1880:
            # Likely invalid min_date
            min_day = 0
            min_month = 0
            min_year = 1880
            logger.warning('Setting min_year = 1880.  This will result in '
                + 'many empty data files and unnecessary looping.  '
                + 'This can be prevented by a) inputting a higher '
                + 'min_date or b) finding the first date at which '
                + 'this dataset is available on WRDS and letting '
                + 'Brock know so he can update the code appropriately.'
            )
        elif min_date < 2050:
            # 4-digit min_date
            min_day = 0
            min_month = 0
            min_year = int(min_date)
        elif 188000 < min_date < 1880000:
            # 6-digit min_date
            (min_year, min_month) = divmod(min_date, 100)
        elif min_date < 20500000:
            (min_year, leftover) = divmod(min_date, 10000)
            (min_month, min_day) = divmod(leftover, 100)

    if min_date == 0:
        if dataset in first_dates:
            min_date = first_dates[dataset]
        elif dataset in first_date_guesses:
            min_date = first_date_guesses[dataset]
        elif any(re.search(x,dataset) for x in first_date_guesses):
            key = [x for x in first_date_guesses if re.search(x,dataset)][0]
            min_date = first_date_guesses[key]
        else:
            min_date = 18000000

        if min_date == -1:
            return (-1,-1,-1)

        (min_year, leftover) = divmod(min_date, 10000)
        (min_month, min_day) = divmod(leftover, 100)


    return (min_year, min_month, min_day)



################################################################################
def wrds_datevar(filename):
    """wrds_datevar(filename)

    Different datasets in WRDS use different names for
    their date-variables.  wrds_datevar gives the right date
    variable for each dataset.  This may need periodic updating.
    Crowdsourcing is welcome.

    return date_var
    """
    # @TODO: Find these programatically.
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
            with open(user_info_filename, 'wb') as fd:
                fd.write(content)
        else:
            logger.warning('user_info["wrds_institution"] does not '
                + 'match the directory "'+institution_path+'" '
                + 'found on the wrds server.  '
                + 'This mismatch may cause errors '
                + 'in the download process.'
                )
    return institution_path



