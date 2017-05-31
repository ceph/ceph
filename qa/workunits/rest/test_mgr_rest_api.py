#! /usr/bin/env python

import requests
import time
import sys

# Do not show the stupid message about verify=False
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

if len(sys.argv) < 3:
    print("Usage: %s <node> <admin_key>" % sys.argv[0])
    sys.exit(1)

host = sys.argv[1]
addr = 'https://' + host + ':8003'

auth = ('admin', sys.argv[2])

request = None

repeat = 1
if len(sys.argv) > 2:
    repeat = int(sys.argv[2])

# Create a pool and get its id
request = requests.post(addr + '/pool?wait=yes', json={'name': 'supertestfriends', 'pg_num': 128}, verify=False, auth=auth)
print(request.text)
request = requests.get(addr + '/pool', verify=False, auth=auth)
assert(request.json()[-1]['pool_name'] == 'supertestfriends')
pool_id = request.json()[-1]['pool']

screenplay = [
    ('get',    '/', {}),
    ('get',    '/config/cluster', {}),
    ('get',    '/crush/rule', {}),
    ('get',    '/crush/ruleset', {}),
    ('get',    '/doc', {}),
    ('get',    '/mon', {}),
    ('get',    '/mon/' + host, {}),
    ('get',    '/osd', {}),
    ('get',    '/osd/0', {}),
    ('get',    '/osd/0/command', {}),
    ('get',    '/pool/0', {}),
    ('get',    '/server', {}),
    ('get',    '/server/' + host, {}),
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
]

for method, endpoint, args in screenplay:
    if method == 'sleep':
        time.sleep(endpoint)
        continue
    url = addr + endpoint
    print("URL = " + url)
    request = getattr(requests, method)(url, json=args, verify=False, auth=auth)
    print(request.text)
    if request.status_code != 200 or 'error' in request.json():
        print('ERROR: %s request for URL "%s" failed' % (method, url))
        sys.exit(1)
