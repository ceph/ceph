#!/usr/bin/python
'''
A backend for PowerDNS to direct RADOS Gateway bucket traffic to the correct regions.

For example, two regions exist, US and EU.

EU: o.myobjects.eu
US: o.myobjects.us

A global domain o.myobjects.com exists.

Bucket 'foo' exists in the region EU and 'bar' in US.

foo.o.myobjects.com will return a CNAME to foo.o.myobjects.eu
bar.o.myobjects.com will return a CNAME to foo.o.myobjects.us

The HTTP Remote Backend from PowerDNS is used in this case: http://doc.powerdns.com/html/remotebackend.html

PowerDNS must be compiled with Remote HTTP backend support enabled, this is not default.

Configuration for PowerDNS:

launch=remote
remote-connection-string=http:url=http://localhost:6780/dns

Usage for this backend is showed by invoking with --help. See rgw-pdns.conf.in for a configuration example

The ACCESS and SECRET key pair requires the caps "metadata=read"

To test:

$ curl -X GET http://localhost:6780/dns/lookup/foo.o.myobjects.com/ANY

Should return something like:

{
 "result": [
  {
   "content": "foo.o.myobjects.eu",
   "qtype": "CNAME",
   "qname": "foo.o.myobjects.com",
   "ttl": 60
  }
 ]
}

'''

# Copyright: Wido den Hollander <wido@42on.com> 2014
# License:   LGPL2.1

from ConfigParser import SafeConfigParser, NoSectionError
from flask import abort, Flask, request, Response
from hashlib import sha1 as sha
from time import gmtime, strftime
from urlparse import urlparse
import argparse
import base64
import hmac
import json
import pycurl
import StringIO
import urllib
import os
import sys

config_locations = ['rgw-pdns.conf', '~/rgw-pdns.conf', '/etc/ceph/rgw-pdns.conf']

# PowerDNS expects a 200 what ever happends and always wants
# 'result' to 'true' if the query fails
def abort_early():
    return json.dumps({'result': 'true'}) + "\n"

# Generate the Signature string for S3 Authorization with the RGW Admin API
def generate_signature(method, date, uri, headers=None):
    sign = "%s\n\n" % method

    if 'Content-Type' in headers:
        sign += "%s\n" % headers['Content-Type']
    else:
        sign += "\n"

    sign += "%s\n/%s/%s" % (date, config['rgw']['admin_entry'], uri)
    h = hmac.new(config['rgw']['secret_key'].encode('utf-8'), sign.encode('utf-8'), digestmod=sha)
    return base64.encodestring(h.digest()).strip()

def generate_auth_header(signature):
    return str("AWS %s:%s" % (config['rgw']['access_key'], signature.decode('utf-8')))

# Do a HTTP request to the RGW Admin API
def do_rgw_request(uri, params=None, data=None, headers=None):
    if headers == None:
        headers = {}

    headers['Date'] = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
    signature = generate_signature("GET", headers['Date'], uri, headers)
    headers['Authorization'] = generate_auth_header(signature)

    query = None
    if params != None:
        query = '&'.join("%s=%s" % (key,val) for (key,val) in params.iteritems())

    c = pycurl.Curl()
    b = StringIO.StringIO()
    url = "http://" + config['rgw']['endpoint'] + "/" + config['rgw']['admin_entry'] + "/" + uri + "?format=json"
    if query != None:
        url += "&" + urllib.quote_plus(query)

    http_headers = []
    for header in headers.keys():
        http_headers.append(header + ": " + headers[header])

    c.setopt(pycurl.URL, str(url))
    c.setopt(pycurl.HTTPHEADER, http_headers)
    c.setopt(pycurl.WRITEFUNCTION, b.write)
    c.setopt(pycurl.FOLLOWLOCATION, 0)
    c.setopt(pycurl.CONNECTTIMEOUT, 5)
    c.perform()

    response = b.getvalue()
    if len(response) > 0:
        return json.loads(response)

    return None

def get_radosgw_metadata(key):
    return do_rgw_request('metadata', {'key': key})

# Returns a string of the region where the bucket is in
def get_bucket_region(bucket):
    meta = get_radosgw_metadata("bucket:%s" % bucket)
    bucket_id = meta['data']['bucket']['bucket_id']
    meta_instance = get_radosgw_metadata("bucket.instance:%s:%s" % (bucket, bucket_id))
    region = meta_instance['data']['bucket_info']['region']
    return region

# Returns the correct host for the bucket based on the regionmap
def get_bucket_host(bucket, region_map):
    region = get_bucket_region(bucket)
    return bucket + "." + region_map[region]

# This should support multiple endpoints per region!
def parse_region_map(map):
    regions = {}
    for region in map['regions']:
        url = urlparse(region['val']['endpoints'][0])
        regions.update({region['key']: url.netloc})

    return regions

def str2bool(s):
    return s.lower() in ("yes", "true", "1")

def init_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="The configuration file to use.", action="store")

    args = parser.parse_args()

    defaults = {
                   'listen_addr': '127.0.0.1',
                   'listen_port': '6780',
                   'dns_zone': 'rgw.local.lan',
                   'dns_soa_record': 'dns1.icann.org. hostmaster.icann.org. 2012080849 7200 3600 1209600 3600',
                   'dns_soa_ttl': '3600',
                   'dns_default_ttl': '60',
                   'rgw_endpoint': 'localhost:8080',
                   'rgw_admin_entry': 'admin',
                   'rgw_access_key': 'access',
                   'rgw_secret_key': 'secret',
                   'debug': False
               }

    cfg = SafeConfigParser(defaults)
    if args.config == None:
        cfg.read(config_locations)
    else:
        if not os.path.isfile(args.config):
            print "Could not open configuration file %s" % args.config
            sys.exit(1)

        cfg.read(args.config)

    config_section = 'powerdns'

    try:
        return {
            'listen': {
                'port': cfg.getint(config_section, 'listen_port'),
                'addr': cfg.get(config_section, 'listen_addr')
                },
            'dns': {
                'zone': cfg.get(config_section, 'dns_zone'),
                'soa_record': cfg.get(config_section, 'dns_soa_record'),
                'soa_ttl': cfg.get(config_section, 'dns_soa_ttl'),
                'default_ttl': cfg.get(config_section, 'dns_default_ttl')
            },
            'rgw': {
                'endpoint': cfg.get(config_section, 'rgw_endpoint'),
                'admin_entry': cfg.get(config_section, 'rgw_admin_entry'),
                'access_key': cfg.get(config_section, 'rgw_access_key'),
                'secret_key': cfg.get(config_section, 'rgw_secret_key')
            },
            'debug': str2bool(cfg.get(config_section, 'debug'))
        }

    except NoSectionError:
         return None

def generate_app(config):
    # The Flask App
    app = Flask(__name__)

    # Get the RGW Region Map
    region_map = parse_region_map(do_rgw_request('config'))

    @app.route('/')
    def index():
        abort(404)

    @app.route("/dns/lookup/<qname>/<qtype>")
    def bucket_location(qname, qtype):
        if len(qname) == 0:
            return abort_early()

        split = qname.split(".", 1)
        if len(split) != 2:
            return abort_early()

        bucket = split[0]
        zone = split[1]

        # If the received qname doesn't match our zone we abort
        if zone != config['dns']['zone']:
            return abort_early()

        # We do not serve MX records
        if qtype == "MX":
            return abort_early()

        # The basic result we always return, this is what PowerDNS expects.
        response = {'result': 'true'}
        result = {}

        # A hardcoded SOA response (FIXME!)
        if qtype == "SOA":
            result.update({'qtype': qtype})
            result.update({'qname': qname})
            result.update({'content': config['dns']['soa_record']})
            result.update({'ttl': config['dns']['soa_ttl']})
        else:
            region_hostname = get_bucket_host(bucket, region_map)
            result.update({'qtype': 'CNAME'})
            result.update({'qname': qname})
            result.update({'content': region_hostname})
            result.update({'ttl': config['dns']['default_ttl']})

        if len(result) > 0:
            res = []
            res.append(result)
            response['result'] = res

        return json.dumps(response, indent=1) + "\n"

    return app


# Initialize the configuration and generate the Application
config = init_config()
if config == None:
    print "Could not parse configuration file. Tried to parse %s" % config_locations
    sys.exit(1)

app = generate_app(config)
app.debug = config['debug']

# Only run the App if this script is invoked from a Shell
if __name__ == '__main__':
    app.run(host=config['listen']['addr'], port=config['listen']['port'])

# Otherwise provide a variable called 'application' for mod_wsgi
else:
    application = app
