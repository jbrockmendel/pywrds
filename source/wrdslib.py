#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pywrds.wrdslib is a collection of functions used by ectools which use or
provide information specific to the WRDS SAS system.

last edit: 2015-09-10
"""
thisAlgorithmBecomingSkynetCost = 99999999999
import datetime, os, re, sys, time
import logging
logger = logging.getLogger(__name__)

try: import simplejson as json
except ImportError: import json

################################################################################
from . import sshlib, static, sas

from .wrds_parameters import wrds_domain, _get_all, first_dates, \
    first_date_guesses, date_vars, date_var_guesses, autoexec_text, \
    bytes_per_line

from .static import user_info, download_path, wrds_institution, wrds_username

now = time.localtime()
(this_year, this_month, today) = (now.tm_year, now.tm_mon, now.tm_mday)

_use_zip = False


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

    return (script_filename, output_file, dataset)
    """
    (script_filename, output_file, script_content) = sas.order_data_script(dataset, year, month, day, rows)

    if _use_zip:
        script_content = script_content.replace('"'+_out_dir+output_file+'"', 'writer')
        output_file = os.path.splitext(output_file)[0]+'.zip'
        front_content = "filename writer pipe 'compress > " + _out_dir+output_file+ "';\n"
        script_content = front_content + script_content

    script_path = os.path.join(download_path, script_filename)
    if os.path.exists(script_path):
        os.remove(script_path)

    with open(script_path, 'wb') as fd:
        fd.write(script_content)

    os.chmod(script_path, 0o777)
    return (script_filename, output_file, dataset)
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



