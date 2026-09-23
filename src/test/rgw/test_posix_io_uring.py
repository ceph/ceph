#!/usr/bin/env python3
"""Exercise POSIX/NSFS multipart io_uring GETs against a test gateway.

Requires the AWS CLI and credentials in its usual environment/configuration.
Start RGW with debug_rgw >= 1, rgw_<backend>_io_engine=io_uring, and both
rgw_<backend>_{get,put}_iodepth=128. Run with direct_io both true and false.
Example: test_posix_io_uring.py --endpoint http://localhost:8000 \
    --rgw-log build/out/radosgw.8000.log
"""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--rgw-log', required=True, type=Path)
    parser.add_argument('--backend', choices=('posix', 'nsfs'), default='posix')
    parser.add_argument('--keep-bucket', action='store_true',
                        help='Keep the empty bucket after deleting test objects')
    args = parser.parse_args()
    log_start = args.rgw_log.stat().st_size
    bucket = args.backend + '-uring-' + uuid.uuid4().hex

    def s3(operation, *options):
        command = ['aws', '--endpoint-url', args.endpoint, '--region', 'us-east-1',
                   '--output', 'json', 's3api', operation, '--bucket', bucket]
        result = subprocess.run(command + list(options),
                                capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(f'aws {operation}: {result.stderr}')
        return json.loads(result.stdout) if result.stdout.strip() else {}

    def check_log():
        with args.rgw_log.open('rb') as log:
            log.seek(log_start)
            new_log = log.read().decode(errors='replace')
        assert 'op=get_obj' in new_log, 'GET logging is required to verify engine selection'
        clamp = f'clamping rgw_{args.backend}_get_iodepth'
        assert clamp not in new_log, 'GET fell back to sync'
        assert 'URING: ERROR:' not in new_log, 'io_uring error in gateway log'

    versions = []
    with tempfile.TemporaryDirectory(prefix=args.backend + '-uring-') as tmp:
        tmp = Path(tmp)
        # Odd sizes and distinct contents expose part-offset/alignment mistakes.
        parts = []
        for n, size in enumerate((5 * 1024 * 1024 + 123,
                                  5 * 1024 * 1024 + 321, 65553), 1):
            pattern = bytes((value + n) % 256 for value in range(251))
            parts.append((pattern * ((size + 250) // 251))[:size])
        expected = b''.join(parts)
        manifest = tmp / 'complete.json'
        output = tmp / 'get'

        def check_get(key, data, *options):
            response = s3('get-object', '--key', key, *options, str(output))
            assert response['ContentLength'] == len(data), response
            assert output.read_bytes() == data, (key, options)

        def upload(key):
            upload_id = s3('create-multipart-upload', '--key', key)['UploadId']
            completed = []
            try:
                for number, data in enumerate(parts, 1):
                    path = tmp / f'part-{number}'
                    path.write_bytes(data)
                    result = s3('upload-part', '--key', key,
                                '--upload-id', upload_id,
                                '--part-number', str(number), '--body', str(path))
                    completed.append({'PartNumber': number, 'ETag': result['ETag']})
                manifest.write_text(json.dumps({'Parts': completed}))
                result = s3('complete-multipart-upload', '--key', key,
                            '--upload-id', upload_id,
                            '--multipart-upload', 'file://' + str(manifest))
            except BaseException:
                s3('abort-multipart-upload', '--key', key, '--upload-id', upload_id)
                raise
            version = result.get('VersionId')
            versions.append((key, version))
            return version

        def check_ranges(key, *options):
            check_get(key, expected, *options)
            boundary = len(parts[0])
            for start, end in ((7, 8194), (boundary - 13, boundary + 29),
                               (boundary, boundary + 4096),
                               (len(expected) - 37, len(expected) - 1)):
                check_get(key, expected[start:end + 1], *options,
                          '--range', f'bytes={start}-{end}')

        s3('create-bucket')
        try:
            upload('multipart')
            check_ranges('multipart')
            check_log()
            for number, data in enumerate(parts, 1):
                check_get('multipart', data, '--part-number', str(number))

            # Existing single-file io_uring behavior must also keep working.
            path = tmp / 'single'
            path.write_bytes(expected)
            s3('put-object', '--key', 'single', '--body', str(path))
            versions.append(('single', None))
            check_ranges('single')

            for key, _ in versions:
                s3('delete-object', '--key', key)
            versions.clear()

            s3('put-bucket-versioning', '--versioning-configuration',
               '{"Status":"Enabled"}')
            version = upload('versioned-multipart')
            assert version, 'multipart completion did not return a version'
            check_ranges('versioned-multipart')
            check_ranges('versioned-multipart', '--version-id', version)
            if args.backend == 'nsfs':
                # Exercise the .versions/ file after the multipart version
                # has been superseded by a regular PUT.
                path.write_bytes(b'replacement')
                s3('put-object', '--key', 'versioned-multipart', '--body', str(path))
                result = s3('head-object', '--key', 'versioned-multipart')
                versions.append(('versioned-multipart', result['VersionId']))
                assert result['VersionId'] != version
                check_ranges('versioned-multipart', '--version-id', version)
                for number, data in enumerate(parts, 1):
                    check_get('versioned-multipart', data, '--version-id', version,
                              '--part-number', str(number))
        finally:
            for key, version in reversed(versions):
                options = ['--version-id', version] if version else []
                s3('delete-object', '--key', key, *options)
            if args.keep_bucket:
                print(f'Keeping test bucket: {bucket}')
            else:
                s3('delete-bucket')

    check_log()
    print('PASS: multipart, ranges, partNumber, single-file, and versioned GETs')


if __name__ == '__main__':
    main()
