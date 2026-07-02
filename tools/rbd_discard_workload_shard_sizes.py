#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from datetime import datetime

sys.path.insert(0, '/work/ceph/build/lib/cython_modules/lib.3')

import rados
import rbd


def mib(value: int) -> int:
    return value * 1024 * 1024


def prime_regions(image: rbd.Image, region_size: int, regions: int,
                  payload_byte: int, initial_offset: int,
                  initial_length: int, progress) -> None:
    if initial_length == 0:
        progress('skipping initial write because initial-length is 0')
        return

    payload = bytes([payload_byte]) * initial_length
    for region in range(regions):
        region_base = region * region_size
        write_offset = region_base + initial_offset
        progress(f'prime region {region + 1}/{regions}: write offset={write_offset} len={initial_length}')
        image.write(payload, write_offset)


def mutate_clone_regions(image: rbd.Image, region_size: int, regions: int,
                         payload_byte: int, progress) -> None:
    discard_start = 64 * 1024
    discard_len = region_size - discard_start
    write_len = 4 * 1024
    write_offset_in_region = region_size - write_len
    write_payload = bytes([payload_byte]) * write_len
    for region in range(regions):
        region_base = region * region_size
        discard_offset = region_base + discard_start
        write_offset = region_base + write_offset_in_region
        progress(f'region {region + 1}/{regions}: async discard+write '
                 f'discard_off={discard_offset} discard_len={discard_len} '
                 f'write_off={write_offset} write_len={write_len}')
        comps = []
        comp_d = image.aio_discard(discard_offset, discard_len, lambda c: None)
        comps.append(comp_d)
        comp_w = image.aio_write(write_payload, write_offset, lambda c: None)
        comps.append(comp_w)
        for c in comps:
            c.wait_for_complete_and_cb()
            ret = c.get_return_value()
            if ret < 0:
                raise RuntimeError(f'aio op failed: {ret}')
        progress(f'region {region + 1}/{regions}: both ops completed')


def parse_size(value: str) -> int:
    value = value.strip()
    if value.isdigit():
        return int(value)
    suffix = value[-1].lower()
    if suffix == 'k' and value[:-1].isdigit():
        return int(value[:-1]) * 1024
    raise ValueError(f'unsupported size value: {value}')



def get_pool_details(args) -> list[dict]:
    result = subprocess.run([
        '/work/ceph/build/bin/ceph', 'osd', 'pool', 'ls', 'detail', '-f', 'json',
        '-c', args.conf, '-n', 'client.admin', '--keyring', args.keyring
    ], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)



def verify_pools(args) -> None:
    pools = {pool['pool_name']: pool for pool in get_pool_details(args)}
    if args.pool not in pools:
        raise RuntimeError(f'pool {args.pool} does not exist')
    if args.data_pool not in pools:
        raise RuntimeError(f'pool {args.data_pool} does not exist')

    metadata_pool = pools[args.pool]
    data_pool = pools[args.data_pool]

    if metadata_pool['type'] != 1:
        raise RuntimeError(f'pool {args.pool} must be replicated')
    if 'rbd' not in metadata_pool.get('application_metadata', {}):
        raise RuntimeError(f'pool {args.pool} is not initialized for rbd')

    if data_pool['type'] != 3:
        raise RuntimeError(f'pool {args.data_pool} must be erasure-coded')
    if 'ec_overwrites' not in data_pool.get('flags_names', ''):
        raise RuntimeError(f'pool {args.data_pool} does not have allow_ec_overwrites enabled')
    if 'rbd' not in data_pool.get('application_metadata', {}):
        raise RuntimeError(f'pool {args.data_pool} is not initialized for rbd')



def main() -> int:
    parser = argparse.ArgumentParser(description='Create RBD clones and issue writes + discards')
    parser.add_argument('--conf', default='/work/ceph/build/ceph.conf')
    parser.add_argument('--keyring', default='/work/ceph/build/keyring')
    parser.add_argument('--pool', default='rbd_replicated')
    parser.add_argument('--data-pool', default='rbd_erasure')
    parser.add_argument('--image', default='rbd-discard-base', help='Base image in the metadata pool')
    parser.add_argument('--create-image-size-mib', type=int, default=512)
    parser.add_argument('--snap', default=None)
    parser.add_argument('--initial-offset', type=int, default=0)
    parser.add_argument('--initial-length', type=int, default=0)
    parser.add_argument('--clone-suffix', default=None)
    parser.add_argument('--regions', type=int, default=4)
    parser.add_argument('--region-size-mib', type=int, default=4)
    args = parser.parse_args()

    def progress(message: str) -> None:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        print(f'[{timestamp}] {message}', flush=True)

    verify_pools(args)

    cluster = rados.Rados(rados_id='admin', conffile=args.conf)
    cluster.conf_set('keyring', args.keyring)
    progress(f'connecting to cluster using {args.conf}')
    cluster.connect()
    progress(f'opening pool {args.pool}')
    ioctx = cluster.open_ioctx(args.pool)
    rbdi = rbd.RBD()

    run_suffix = args.clone_suffix or datetime.now().strftime('%Y%m%d%H%M%S%f')
    snap_prefix = args.snap or 'txbase'
    image_name = f'{args.image}-{run_suffix}'

    try:
        rbdi.create(ioctx, image_name, mib(args.create_image_size_mib),
                    features=rbd.RBD_FEATURE_LAYERING,
                    data_pool=args.data_pool)
        progress(f'created base image {args.pool}/{image_name} size={args.create_image_size_mib} MiB data_pool={args.data_pool}')
    except rbd.ImageExists:
        progress(f'using existing base image {args.pool}/{image_name}')

    base = rbd.Image(ioctx, image_name)
    try:
        image_data_pool = base.data_pool_id()
    finally:
        base.close()
    if image_data_pool is None:
        raise RuntimeError(f'image {args.pool}/{image_name} is not EC-backed; recreate it with --data-pool {args.data_pool}')

    region_size = mib(args.region_size_mib)

    progress('step 1: optional initial write in every region')
    base = rbd.Image(ioctx, image_name)
    try:
        prime_regions(base, region_size, args.regions, 0x31,
                      args.initial_offset, args.initial_length, progress)
        snap_name = f'{snap_prefix}-{run_suffix}'
        progress(f'creating snapshot {image_name}@{snap_name}')
        try:
            base.create_snap(snap_name)
        except rbd.ImageExists:
            pass
        progress(f'protecting snapshot {image_name}@{snap_name}')
        try:
            base.protect_snap(snap_name)
        except (rbd.ImageExists, rbd.ImageBusy):
            pass
    finally:
        base.close()

    clone_name = f'{image_name}-txclone'
    progress(f'step 2: create clone {clone_name} from {image_name}@{snap_name}')
    try:
        rbdi.remove(ioctx, clone_name)
    except rbd.ImageNotFound:
        pass
    rbdi.clone(ioctx, image_name, snap_name, ioctx, clone_name,
               features=rbd.RBD_FEATURE_LAYERING,
               data_pool=args.data_pool)

    image = rbd.Image(ioctx, clone_name)
    try:
        if image.data_pool_id() is None:
            raise RuntimeError(f'clone {clone_name} is not EC-backed; expected data pool {args.data_pool}')
        progress(f'clone info: id={image.id()} block_name_prefix={image.block_name_prefix()}')

        clone_snap_name = f'deep-{run_suffix}'
        progress(f'step 3: create clone snapshot {clone_name}@{clone_snap_name} (triggers deep_copyup)')
        image.create_snap(clone_snap_name)

        progress('step 4: async discard tail 64K + write head 4K of every region')
        mutate_clone_regions(image, region_size, args.regions, 0x61, progress)

        progress('step 5: skipping readback (would crash OSDs due to short shards)')
    finally:
        image.close()

    progress('step 5 complete — run deep-scrub on the PG to detect shard size mismatch')

    progress('workload complete, closing cluster handles')
    ioctx.close()
    cluster.shutdown()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
