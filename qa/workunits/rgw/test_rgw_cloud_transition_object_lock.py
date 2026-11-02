#!/usr/bin/env python3
"""Exercise cloud transition with retention, legal holds, and unlocked controls.

Requires an existing cloud-s3 storage class, AWS CLI credentials for RGW, and
radosgw-admin access to the same cluster. Run against a disposable test cluster.
The supplied bucket must not exist. Objects remain for inspection; the lifecycle
policy is removed even on failure. Protected objects retain their one-hour lock.

Example (with CEPH_CONF and LD_LIBRARY_PATH set for a vstart cluster):
  python3 test_rgw_cloud_transition_object_lock.py --bucket lc-lock-test \
      --endpoint-url http://localhost:8000 --storage-class CLOUDTIER \
      --radosgw-admin ./bin/radosgw-admin
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def run(command, check=True):
    result = subprocess.run(command, capture_output=True, text=True, timeout=180)
    if check and result.returncode:
        raise RuntimeError(result.stderr)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--endpoint-url', required=True)
    parser.add_argument('--storage-class', required=True)
    parser.add_argument('--radosgw-admin', default='radosgw-admin')
    args = parser.parse_args()
    os.environ['AWS_PAGER'] = ''
    os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'
    admin = [args.radosgw_admin]
    aws = ['aws', '--endpoint-url', args.endpoint_url, '--region', 'us-east-1',
           '--output', 'json', 's3api']

    def s3(operation, **parameters):
        command = aws + [operation, '--bucket=' + args.bucket]
        for name, value in parameters.items():
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            command.append('--' + name.replace('_', '-') + '=' + str(value))
        output = run(command).stdout
        return json.loads(output) if output.strip() else {}

    existing = run(admin + ['bucket', 'stats', '--bucket', args.bucket], check=False)
    if existing.returncode != 2:
        raise RuntimeError('The test requires a new bucket; existence check: ' + existing.stderr)

    records = []
    with tempfile.TemporaryDirectory(prefix='rgw-cloud-lock-') as directory:
        body = Path(directory) / 'body'

        def upload(key, sequence, protection):
            payload = (f'{key}/{sequence}\n'.encode() * 4096)[:4096]
            body.write_bytes(payload)
            response = s3('put-object', key=key, body=body)
            records.append({'key': key, 'version': response.get('VersionId', 'null'),
                            'protection': protection, 'payload': payload})

        s3('create-bucket')
        # Cover an object uploaded before the bucket became versioned.
        upload('null-governance', 1, 'GOVERNANCE')
        s3('put-bucket-versioning', versioning_configuration={'Status': 'Enabled'})
        for protection in ['GOVERNANCE', 'COMPLIANCE', 'LEGAL_HOLD', 'UNLOCKED']:
            for sequence in [1, 2]:
                upload(protection.lower(), sequence, protection)
        for mode in ['GOVERNANCE', 'COMPLIANCE']:
            upload('expired-' + mode.lower(), 1, 'EXPIRED_' + mode)
        s3('put-object-lock-configuration',
           object_lock_configuration={'ObjectLockEnabled': 'Enabled'})

        def future(seconds):
            return (datetime.datetime.now(datetime.timezone.utc) +
                    datetime.timedelta(seconds=seconds)).isoformat()

        for record in records:
            address = {'key': record['key'], 'version_id': record['version']}
            protection = record['protection']
            if protection == 'LEGAL_HOLD':
                s3('put-object-legal-hold', **address, legal_hold={'Status': 'ON'})
            elif protection != 'UNLOCKED':
                mode = protection.removeprefix('EXPIRED_')
                seconds = 5 if protection.startswith('EXPIRED_') else 3600
                s3('put-object-retention', **address,
                   retention={'Mode': mode, 'RetainUntilDate': future(seconds)})

        before = {(v['Key'], v['VersionId']): v for v in
                  s3('list-object-versions').get('Versions', [])}
        if len(before) != len(records):
            raise AssertionError('Unexpected initial version count')
        locked = {r['key'] for r in records
                  if r['protection'] in ['GOVERNANCE', 'COMPLIANCE', 'LEGAL_HOLD']}
        policy = {'Rules': [{'ID': 'cloud-object-lock-regression', 'Status': 'Enabled',
                             'Filter': {'Prefix': ''},
                             'Transitions': [{'Date': '2000-01-01T00:00:00Z',
                                              'StorageClass': args.storage_class}],
                             'NoncurrentVersionTransitions': [
                                 {'NoncurrentDays': 1, 'StorageClass': args.storage_class}]}]}
        try:
            s3('put-bucket-lifecycle-configuration', lifecycle_configuration=policy)
            # A five-second test day applies only to these admin invocations.
            time.sleep(7)
            for attempt in range(3):
                run(admin + ['--rgw-lc-debug-interval=5', 'lc', 'process',
                             '--bucket', args.bucket])
                time.sleep(7)
            listing = s3('list-object-versions')
            versions = {(v['Key'], v['VersionId']): v for v in listing.get('Versions', [])}
            if set(versions) != set(before):
                raise AssertionError('An original version was lost or an unexpected one appeared')
            for record in records:
                address = {'key': record['key'], 'version_id': record['version']}
                identity = (record['key'], record['version'])
                version = versions[identity]
                if record['key'] in locked:
                    if version.get('StorageClass') != 'STANDARD':
                        raise AssertionError(f'Protected object transitioned: {identity}')
                    if version['IsLatest'] != before[identity]['IsLatest']:
                        raise AssertionError(
                            f'Protected version changed current status: {identity}')
                    destination = Path(directory) / 'download'
                    run(aws + ['get-object', '--bucket=' + args.bucket,
                               '--key=' + record['key'], '--version-id=' + record['version'],
                               str(destination)])
                    if destination.read_bytes() != record['payload']:
                        raise AssertionError(f'Protected payload changed: {identity}')
                    if record['protection'] == 'LEGAL_HOLD':
                        if s3('get-object-legal-hold', **address)['LegalHold']['Status'] != 'ON':
                            raise AssertionError('Legal hold was removed')
                    else:
                        value = s3('get-object-retention', **address)['Retention']
                        if value['Mode'] != record['protection']:
                            raise AssertionError('Retention mode changed')
                elif version.get('StorageClass') != args.storage_class:
                    raise AssertionError(
                        f'Unlocked/expired control failed to transition: {identity}')
            if any(v['Key'] in locked for v in listing.get('DeleteMarkers', [])):
                raise AssertionError('Lifecycle created a delete marker for a protected object')
            stats = run(admin + ['bucket', 'stats', '--bucket', args.bucket])
            usage = json.loads(stats.stdout)['usage']
            if usage.get('rgw.main', {}).get('num_objects') != 7:
                raise AssertionError(f'Expected seven protected local versions: {usage}')
            if usage.get('rgw.cloudtiered', {}).get('num_objects') != 4:
                raise AssertionError(f'Expected four transitioned control versions: {usage}')
            print('PASS: 7 protected versions skipped; 4 unlocked/expired versions transitioned')
        finally:
            s3('delete-bucket-lifecycle')
            check = run(admin + ['lc', 'get', '--bucket', args.bucket], check=False)
            if check.returncode != 2:
                raise AssertionError('Lifecycle removal could not be verified')


if __name__ == '__main__':
    main()
