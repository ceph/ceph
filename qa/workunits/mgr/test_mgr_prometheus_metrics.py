#! /usr/bin/env python3

import errno
import requests
import time
import sys
import json
from typing import Dict

def get_mon_metadata(metadata_string: str) -> Dict[str, str]:
    result = dict((item.strip(), str(value.strip()))
                     for item, value in (element.split('=')
                                  for element in metadata_string.split(',')))
    return result

def main():
    if len(sys.argv) < 3:
        print("Usage: %s <url> <mon-metadata>" % sys.argv[0])
        sys.exit(1)

    addr = sys.argv[1]
    json_file = sys.argv[2]
    headers = {'Content-type': 'application/json'}

    request = None

    try:
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    except:
        pass

    try:
        temp_file = open(json_file, 'r+')
    except IOError as err:
        if err.errno == errno.ENOENT:
            assert False, \
                "error opening '{fp}': {e}".format(fp=json_file, e=err)
        else:
            assert False, \
                'some error occurred: {e}'.format(e=err)

    try:
        metadata_dump = json.load(temp_file)
    except ValueError:
        assert False, \
            "{jf} content is not a valid JSON object".format(jf=json_file)

    mon_metadata = {}
    for metadata in metadata_dump:
         mon_metadata[metadata.get('name')] = metadata.get('ceph_version')

    request = requests.get(addr + '/metrics', verify=False)
    assert(request.status_code == 200)
    metrics_data = request.text
    for line in str(metrics_data).split("\n"):
        if ("ceph_mon_metadata" in line) and ("#" not in line):
            metrics_metadata_string = line.replace('ceph_mon_metadata{','').split('}', 1)[0]
            print(f"metrics_metadata_string : {metrics_metadata_string}")
            metrics_mon_metadata = get_mon_metadata(metrics_metadata_string)
            assert('ceph_version' in metrics_mon_metadata)
            mon_name = eval(metrics_mon_metadata.get('ceph_daemon').replace('mon.',''))
            mon_version = eval(metrics_mon_metadata.get('ceph_version'))
            assert(mon_version != '')
            assert(mon_version == mon_metadata[mon_name])
            print(f"mon_metadata[{mon_name}] : {mon_version}")

if __name__ == '__main__':
    main()
