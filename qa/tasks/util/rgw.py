import logging
import json
import time
import xml.etree.ElementTree as ET

from io import StringIO

import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def rgwadmin(ctx, client, cmd, stdin=StringIO(), check_status=False,
             omit_sudo=False, omit_tdir=False, format='json', decode=True,
             log_level=logging.DEBUG):
    log.info('rgwadmin: {client} : {cmd}'.format(client=client,cmd=cmd))
    testdir = teuthology.get_testdir(ctx)
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    client_with_id = daemon_type + '.' + client_id
    pre = [
        'adjust-ulimits',
        'ceph-coverage']
    if not omit_tdir:
        pre.append(
            '{tdir}/archive/coverage'.format(tdir=testdir))
    pre.extend([
        'radosgw-admin',
        '--log-to-stderr',
        '--format', format,
        '-n',  client_with_id,
        '--cluster', cluster_name,
        ])
    pre.extend(cmd)
    log.log(log_level, 'rgwadmin: cmd=%s' % pre)
    (remote,) = ctx.cluster.only(client).remotes.keys()
    proc = remote.run(
        args=pre,
        check_status=check_status,
        omit_sudo=omit_sudo,
        stdout=StringIO(),
        stderr=StringIO(),
        stdin=stdin,
        )
    r = proc.exitstatus
    out = proc.stdout.getvalue()
    if not decode:
        return (r, out)
    j = None
    if not r and out != '':
        try:
            j = json.loads(out)
            log.log(log_level, ' json result: %s' % j)
        except ValueError:
            j = out
            log.log(log_level, ' raw result: %s' % j)
    return (r, j)

def get_user_summary(out, user):
    """Extract the summary for a given user"""
    user_summary = None
    for summary in out['summary']:
        if summary.get('user') == user:
            user_summary = summary

    if not user_summary:
        raise AssertionError('No summary info found for user: %s' % user)

    return user_summary

def get_user_successful_ops(out, user):
    summary = out['summary']
    if len(summary) == 0:
        return 0
    return get_user_summary(out, user)['total']['successful_ops']

def s3_get_usage(endpoint_url, access_key, secret_key, region='us-east-1'):
    """Issue a signed S3 GET /?usage request (Ceph RGW extension)."""
    creds = Credentials(access_key, secret_key)
    url = endpoint_url.rstrip('/') + '/'
    request = AWSRequest(
        method='GET',
        url=url,
        params={'usage': ''},
        headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'},
    )
    SigV4Auth(creds, 's3', region).add_auth(request)
    prepared = request.prepare()
    log.debug('s3_get_usage: url=%s', prepared.url)
    result = requests.get(prepared.url, headers=prepared.headers)
    return result.status_code, result.text

def parse_s3_usage_xml(text):
    """Parse the XML body of GET /?usage into an ElementTree root."""
    return ET.fromstring(text)

def s3_usage_capacity_entries(root):
    """Return per-bucket storage entries from CapacityUsed."""
    return root.findall('.//CapacityUsed//Entry')

def s3_usage_log_users(root):
    """Return usage log user sections from Entries."""
    return root.findall('.//Entries//User')

def s3_usage_log_owners(root):
    """Return Owner ids from usage log Entries."""
    owners = []
    for owner in root.findall('.//Entries//Owner'):
        if owner.text:
            owners.append(owner.text)
    return owners

def s3_usage_total_ops(root):
    """Sum Ops values from usage log categories in Entries."""
    total = 0
    for ops in root.findall('.//Entries//Ops'):
        if ops.text:
            total += int(ops.text)
    return total

def s3_usage_summary_successful_ops(root):
    """Sum SuccessfulOps from Summary totals."""
    total = 0
    for ops in root.findall('.//Summary//Total//SuccessfulOps'):
        if ops.text:
            total += int(ops.text)
    return total

def s3_usage_total_bytes(root):
    """Return TotalBytes from Summary (user storage)."""
    el = root.find('.//Summary//TotalBytes')
    if el is not None and el.text:
        return int(el.text)
    return 0

def wait_for_radosgw(url, remote):
    """ poll the given url until it starts accepting connections

    add_daemon() doesn't wait until radosgw finishes startup, so this is used
    to avoid racing with later tasks that expect radosgw to be up and listening
    """
    # TODO: use '--retry-connrefused --retry 8' when teuthology is running on
    # Centos 8 and other OS's with an updated version of curl
    curl_cmd = ['curl',
                url]
    exit_status = 0
    num_retries = 8
    for seconds in range(num_retries):
        proc = remote.run(
            args=curl_cmd,
            check_status=False,
            stdout=StringIO(),
            stderr=StringIO(),
            stdin=StringIO(),
            )
        exit_status = proc.exitstatus
        if exit_status == 0:
            break
        time.sleep(2**seconds)

    assert exit_status == 0
