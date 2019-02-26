#! /usr/bin/env python

import requests
import time
import sys
import json

# Do not show the stupid message about verify=False.  ignore exceptions bc
# this doesn't work on some distros.
try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
except:
    pass

if len(sys.argv) < 3:
    print("Usage: %s <url> <admin_key>" % sys.argv[0])
    sys.exit(1)

addr = sys.argv[1]
auth = ('admin', sys.argv[2])
headers = {'Content-type': 'application/json'}

request = None

# Create a pool and get its id
request = requests.post(
    addr + '/pool?wait=yes',
    data=json.dumps({'name': 'supertestfriends', 'pg_num': 128}),
    headers=headers,
    verify=False,
    auth=auth)
print(request.text)
request = requests.get(addr + '/pool', verify=False, auth=auth)
assert(request.json()[-1]['pool_name'] == 'supertestfriends')
pool_id = request.json()[-1]['pool']

# get a mon name
request = requests.get(addr + '/mon', verify=False, auth=auth)
firstmon = request.json()[0]['name']
print('first mon is %s' % firstmon)

# get a server name
request = requests.get(addr + '/osd', verify=False, auth=auth)
aserver = request.json()[0]['server']
print('a server is %s' % aserver)


screenplay = [
    ('get',    '/', {}),
    ('get',    '/config/cluster', {}),
    ('get',    '/crush/rule', {}),
    ('get',    '/doc', {}),
    ('get',    '/mon', {}),
    ('get',    '/mon/' + firstmon, {}),
    ('get',    '/osd', {}),
    ('get',    '/osd/0', {}),
    ('get',    '/osd/0/command', {}),
    ('get',    '/pool/1', {}),
    ('get',    '/server', {}),
    ('get',    '/server/' + aserver, {}),
    ('post',   '/osd/0/command', {'command': 'scrub'}),
    ('post',   '/pool?wait=1', {'name': 'supertestfriends', 'pg_num': 128}),
    ('patch',  '/osd/0', {'in': False}),
    ('patch',  '/config/osd', {'pause': True}),
    ('get',    '/config/osd', {}),
    ('patch',  '/pool/' + str(pool_id), {'size': 2}),
    ('patch',  '/config/osd', {'pause': False}),
    ('patch',  '/osd/0', {'in': True}),
    ('get',    '/pool', {}),
    ('delete', '/pool/' + str(pool_id) + '?wait=1', {}),
    ('get',    '/request?page=0', {}),
    ('delete', '/request', {}),
    ('get',    '/request', {}),
    ('patch', '/pool/1', {'pg_num': 128}),
    ('patch', '/pool/1', {'pgp_num': 128}),
]

for method, endpoint, args in screenplay:
    if method == 'sleep':
        time.sleep(endpoint)
        continue
    url = addr + endpoint
    print("URL = " + url)
    request = getattr(requests, method)(
        url,
        data=json.dumps(args),
        headers=headers,
        verify=False,
        auth=auth)
    print(request.text)
    if request.status_code != 200 or 'error' in request.json():
        print('ERROR: %s request for URL "%s" failed' % (method, url))
        sys.exit(1)

print('OK')
