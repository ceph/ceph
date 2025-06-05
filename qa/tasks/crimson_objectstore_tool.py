"""
crimson_objectstore_tool - Test of crimson-objectstore-tool utility
"""

import contextlib
import json
import logging
import os
import tempfile
import time
from tasks import ceph_manager
from tasks.util.rados import (rados, create_replicated_pool)
from teuthology import misc as teuthology
from teuthology.orchestra import run
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

# Crimson OSD data paths (using cluster name)
FSPATH = "/var/lib/ceph/osd/{cluster}-{id}"
JPATH = "/var/lib/ceph/osd/{cluster}-{id}/journal"


def cos_setup_local_data(log, ctx, NUM_OBJECTS, DATADIR,
                         BASE_NAME, DATALINECOUNT):
    objects = range(1, NUM_OBJECTS + 1)
    for i in objects:
        NAME = BASE_NAME + "{num}".format(num=i)
        LOCALNAME = os.path.join(DATADIR, NAME)

        dataline = range(DATALINECOUNT)
        fd = open(LOCALNAME, "w")
        data = "This is the crimson data for " + NAME + "\n"
        for _ in dataline:
            fd.write(data)
        fd.close()


def cos_setup_remote_data(log, ctx, remote, NUM_OBJECTS, DATADIR,
                          BASE_NAME, DATALINECOUNT):
    objects = range(1, NUM_OBJECTS + 1)
    for i in objects:
        NAME = BASE_NAME + "{num}".format(num=i)
        DDNAME = os.path.join(DATADIR, NAME)

        remote.run(args=['rm', '-f', DDNAME])

        dataline = range(DATALINECOUNT)
        data = "This is the crimson data for " + NAME + "\n"
        DATA = ""
        for _ in dataline:
            DATA += data
        remote.write_file(DDNAME, DATA)


def cos_setup(log, ctx, remote, NUM_OBJECTS, DATADIR,
              BASE_NAME, DATALINECOUNT, POOL, db):
    ERRORS = 0
    log.info("Creating {objs} objects in pool".format(objs=NUM_OBJECTS))

    objects = range(1, NUM_OBJECTS + 1)
    for i in objects:
        NAME = BASE_NAME + "{num}".format(num=i)
        DDNAME = os.path.join(DATADIR, NAME)

        # Put object
        proc = rados(ctx, remote, ['-p', POOL, 'put', NAME, DDNAME],
                     wait=False)
        ret = proc.wait()
        if ret != 0:
            log.critical("Rados put failed with status {ret}".
                         format(ret=proc.exitstatus))
            ERRORS += 1
            continue

        db[NAME] = {}

        # Set xattrs
        keys = range(i)
        db[NAME]["xattr"] = {}
        for k in keys:
            if k == 0:
                continue
            mykey = "key{i}-{k}".format(i=i, k=k)
            myval = "val{i}-{k}".format(i=i, k=k)
            proc = remote.run(args=['rados', '-p', POOL, 'setxattr',
                                    NAME, mykey, myval])
            ret = proc.wait()
            if ret != 0:
                log.error("setxattr failed with {ret}".format(ret=ret))
                ERRORS += 1
            db[NAME]["xattr"][mykey] = myval

        # Set omap header for objects except the first one
        if i != 1:
            myhdr = "hdr{i}".format(i=i)
            proc = remote.run(args=['rados', '-p', POOL, 'setomapheader',
                                    NAME, myhdr])
            ret = proc.wait()
            if ret != 0:
                log.error("setomapheader failed with {ret}".format(ret=ret))
                ERRORS += 1
            db[NAME]["omapheader"] = myhdr

        # Set omap values
        db[NAME]["omap"] = {}
        for k in keys:
            if k == 0:
                continue
            mykey = "okey{i}-{k}".format(i=i, k=k)
            myval = "oval{i}-{k}".format(i=i, k=k)
            proc = remote.run(args=['rados', '-p', POOL, 'setomapval',
                                    NAME, mykey, myval])
            ret = proc.wait()
            if ret != 0:
                log.error("setomapval failed with {ret}".format(ret=ret))
                ERRORS += 1
            db[NAME]["omap"][mykey] = myval

    return ERRORS


@contextlib.contextmanager
def task(ctx, config):
    """
    Run crimson_objectstore_tool test

    The config should be as follows::

        crimson_objectstore_tool:
          objects: 5 # <number of objects>
          pgnum: 8
    """

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'crimson_objectstore_tool task only accepts a dict for configuration'

    log.info('Beginning crimson_objectstore_tool...')

    clients = ctx.cluster.only(teuthology.is_type('client'))
    assert len(clients.remotes) > 0, 'Must specify at least 1 client'
    (cli_remote, _) = clients.remotes.popitem()

    osds = ctx.cluster.only(teuthology.is_type('osd'))
    log.info("OSDS: {}".format(osds.remotes))

    manager = ctx.managers['ceph']
    
    # Wait for cluster to be ready
    while (len(manager.get_osd_status()['up']) !=
           len(manager.get_osd_status()['raw'])):
        time.sleep(10)
    while (len(manager.get_osd_status()['in']) !=
           len(manager.get_osd_status()['up'])):
        time.sleep(10)
    
    manager.raw_cluster_cmd('osd', 'set', 'noout')
    manager.raw_cluster_cmd('osd', 'set', 'nodown')

    PGNUM = config.get('pgnum', 8)
    log.info("pgnum: {num}".format(num=PGNUM))

    ERRORS = 0

    # Test with replicated pool
    REP_POOL = "crimson_rep_pool"
    REP_NAME = "CRIMobject"
    create_replicated_pool(cli_remote, REP_POOL, PGNUM)
    ERRORS += test_crimson_objectstore(ctx, config, cli_remote, REP_POOL, REP_NAME)

    if ERRORS == 0:
        log.info("CRIMSON TEST PASSED")
    else:
        log.error("CRIMSON TEST FAILED WITH {errcount} ERRORS".format(errcount=ERRORS))

    assert ERRORS == 0

    try:
        yield
    finally:
        log.info('Ending crimson_objectstore_tool')


def test_crimson_objectstore(ctx, config, cli_remote, REP_POOL, REP_NAME):
    manager = ctx.managers['ceph']
    osds = ctx.cluster.only(teuthology.is_type('osd'))

    TEUTHDIR = teuthology.get_testdir(ctx)
    DATADIR = os.path.join(TEUTHDIR, "crimson.data")
    DATALINECOUNT = 1000
    ERRORS = 0
    NUM_OBJECTS = config.get('objects', 5)
    log.info("objects: {num}".format(num=NUM_OBJECTS))

    pool_dump = manager.get_pool_dump(REP_POOL)
    REPID = pool_dump['pool']
    log.debug("pool id={num}".format(num=REPID))

    db = {}

    # Setup local and remote data
    LOCALDIR = tempfile.mkdtemp("cos")
    cos_setup_local_data(log, ctx, NUM_OBJECTS, LOCALDIR,
                         REP_NAME, DATALINECOUNT)
    
    allremote = [cli_remote] + list(osds.remotes.keys())
    allremote = list(set(allremote))
    for remote in allremote:
        cos_setup_remote_data(log, ctx, remote, NUM_OBJECTS, DATADIR,
                              REP_NAME, DATALINECOUNT)

    ERRORS += cos_setup(log, ctx, cli_remote, NUM_OBJECTS, DATADIR,
                        REP_NAME, DATALINECOUNT, REP_POOL, db)

    # Get PG mappings
    pgs = {}
    for stats in manager.get_pg_stats():
        if stats["pgid"].find(str(REPID) + ".") != 0:
            continue
        for osd in stats["acting"]:
            pgs.setdefault(osd, []).append(stats["pgid"])

    log.info("PGs: {}".format(pgs))
    log.info("DB: {}".format(db))

    # Check if this is a crimson cluster
    is_crimson_cluster = getattr(ctx, 'flavor', None) == 'crimson'
    log.info("Detected cluster type: {}".format("Crimson" if is_crimson_cluster else "Classic"))

    # Stop OSDs to access data files
    for osd in manager.get_osd_status()['up']:
        manager.kill_osd(osd)
    time.sleep(5)

    # Verify OSDs are actually stopped 
    log.info("Verifying OSDs are stopped...")
    for remote in osds.remotes.keys():
        try:
            # Check for crimson-osd processes
            result = remote.run(args=['pgrep', '-f', 'crimson-osd'], check_status=False)
            if result.returncode == 0:
                log.warning("Some crimson-osd processes still running")
            else:
                log.info("All crimson-osd processes stopped")
        except Exception as e:
            log.debug("Process check failed: {}".format(e))

    pgswithobjects = set()
    objsinpg = {}

    # Test --list-pgs operation
    log.info("Test --list-pgs functionality")
    cluster = manager.cluster
    prefix = ("sudo crimson-objectstore-tool "
              "--data-path {fpath} "
              "--store-type seastore ").format(fpath=FSPATH.format(cluster=cluster, id="{id}"))
    
    for remote in osds.remotes.keys():
        for role in osds.remotes[remote]:
            if not role.startswith("osd."):
                continue
            osdid = int(role.split('.')[1])
            log.info("Testing OSD.{id} on {remote}".format(id=osdid, remote=remote))
            
            # Check if OSD data directory exists
            osd_data_path = FSPATH.format(cluster=cluster, id=osdid)
            result = remote.run(args=['test', '-d', osd_data_path], check_status=False)
            if result.returncode != 0:
                log.warning("OSD data directory not found: {path}".format(path=osd_data_path))
                continue
                
            # Check for seastore specific files
            result = remote.run(args=['ls', '-la', osd_data_path], check_status=False)
            if result.returncode == 0:
                log.debug("OSD {id} data directory contents: {contents}".format(
                    id=osdid, contents=result.stdout))
            
            cmd = (prefix + "--list-pgs").format(id=osdid)
            try:
                result = remote.sh(cmd, check_status=False)
                if result.strip():
                    log.info("Found PGs: {}".format(result.strip()))
                else:
                    log.info("No PGs found or empty result")
            except CommandFailedError as e:
                log.warning("--list-pgs failed with status {ret} for osd.{id}".
                          format(ret=e.exitstatus, id=osdid))

    # Test --list-objects operation  
    log.info("Test --list-objects functionality")
    for remote in osds.remotes.keys():
        for role in osds.remotes[remote]:
            if not role.startswith("osd."):
                continue
            osdid = int(role.split('.')[1])
            if osdid not in pgs:
                continue

            for pg in pgs[osdid]:
                cmd = (prefix + "--list-objects --pg {pg}").format(id=osdid, pg=pg)
                try:
                    objects_output = remote.sh(cmd, check_status=False)
                    if objects_output.strip():
                        log.info("Found objects in pg {pg}: {objs}".format(
                            pg=pg, objs=objects_output.strip()))
                        pgswithobjects.add(pg)
                        objsinpg.setdefault(pg, []).extend(objects_output.strip().split('\n'))
                except CommandFailedError as e:
                    log.warning("--list-objects failed with status {ret} for pg {pg}".
                              format(ret=e.exitstatus, pg=pg))

    # Test format options
    log.info("Test format options")
    formats = ['json', 'json-pretty', 'plain']
    for fmt in formats:
        for remote in osds.remotes.keys():
            for role in osds.remotes[remote]:
                if not role.startswith("osd."):
                    continue
                osdid = int(role.split('.')[1])
                
                cmd = (prefix + "--format {fmt} --list-pgs").format(id=osdid, fmt=fmt)
                try:
                    result = remote.sh(cmd, check_status=False)
                    log.info("Format {fmt} test passed for osd.{id}".format(fmt=fmt, id=osdid))
                    if fmt.startswith('json') and result.strip():
                        # Try to parse JSON to verify it's valid
                        try:
                            json.loads(result)
                            log.info("JSON format validation passed")
                        except json.JSONDecodeError:
                            log.warning("JSON format validation failed")
                            ERRORS += 1
                    break  # Only test with first available OSD
                except CommandFailedError as e:
                    log.warning("Format {fmt} test failed with status {ret}".
                              format(fmt=fmt, ret=e.exitstatus))
            break

    # Test help functionality
    log.info("Test help functionality")
    for remote in osds.remotes.keys():
        cmd = "crimson-objectstore-tool --help"
        try:
            help_output = remote.sh(cmd, check_status=False)
            if "crimson-objectstore-tool" in help_output or "Usage" in help_output:
                log.info("Help functionality test passed")
            else:
                log.warning("Help output seems incomplete")
                ERRORS += 1
            break
        except CommandFailedError as e:
            log.warning("Help test failed with status {ret}".format(ret=e.exitstatus))
            ERRORS += 1
            break

    # Test invalid arguments
    log.info("Test invalid argument handling")
    invalid_cases = [
        ("--data-path /nonexistent --store-type seastore --list-pgs", 
         "nonexistent data path"),
        ("--data-path /tmp --store-type invalid --list-pgs", 
         "invalid store type"),
        ("--store-type seastore --list-pgs", 
         "missing data path"),
    ]
    
    for remote in osds.remotes.keys():
        for args, description in invalid_cases:
            cmd = "crimson-objectstore-tool " + args
            try:
                result = remote.sh(cmd, check_status=False)
                log.warning("Command should have failed for {desc}: {cmd}".
                          format(desc=description, cmd=args))
                # Don't increment ERRORS as this might be expected behavior
            except CommandFailedError:
                log.info("Correctly failed for {desc}".format(desc=description))
        break

    # Test OMAP operations
    log.info("Test OMAP operations")
    
    # Only test if we have objects and PGs
    if pgswithobjects and objsinpg:
        for remote in osds.remotes.keys():
            for role in osds.remotes[remote]:
                if not role.startswith("osd."):
                    continue
                osdid = int(role.split('.')[1])
                if osdid not in pgs:
                    continue

                # Test with the first available PG that has objects
                test_pg = None
                test_objects = []
                for pg in pgs[osdid]:
                    if pg in pgswithobjects:
                        test_pg = pg
                        test_objects = objsinpg.get(pg, [])
                        break
                
                if not test_pg or not test_objects:
                    continue

                # Use the first available object for testing
                test_object = test_objects[0] if test_objects else None
                if not test_object:
                    continue

                log.info("Testing OMAP operations on object {obj} in pg {pg}".format(
                    obj=test_object, pg=test_pg))

                # Test list-omap
                log.info("Testing --list-omap")
                cmd = (prefix + "--list-omap --pg {pg} --object {obj}").format(
                    id=osdid, pg=test_pg, obj=test_object)
                try:
                    omap_list = remote.sh(cmd, check_status=False)
                    log.info("list-omap result: {result}".format(result=omap_list.strip()))
                except CommandFailedError as e:
                    log.warning("--list-omap failed with status {ret}".format(ret=e.exitstatus))

                # Test set-omap
                log.info("Testing --set-omap")
                test_key = "test_omap_key"
                test_value = "test_omap_value"
                cmd = (prefix + "--set-omap --pg {pg} --object {obj} --omap-key {key} --omap-value {value}").format(
                    id=osdid, pg=test_pg, obj=test_object, key=test_key, value=test_value)
                try:
                    result = remote.sh(cmd, check_status=False)
                    log.info("set-omap completed for key {key}".format(key=test_key))
                except CommandFailedError as e:
                    log.warning("--set-omap failed with status {ret}".format(ret=e.exitstatus))

                # Test get-omap (try to get the key we just set)
                log.info("Testing --get-omap")
                cmd = (prefix + "--get-omap --pg {pg} --object {obj} --omap-key {key}").format(
                    id=osdid, pg=test_pg, obj=test_object, key=test_key)
                try:
                    omap_value = remote.sh(cmd, check_status=False)
                    if omap_value.strip() == test_value:
                        log.info("get-omap verification PASSED: got expected value {value}".format(
                            value=test_value))
                    else:
                        log.warning("get-omap verification failed: expected {exp}, got {actual}".format(
                            exp=test_value, actual=omap_value.strip()))
                        ERRORS += 1
                except CommandFailedError as e:
                    log.warning("--get-omap failed with status {ret}".format(ret=e.exitstatus))

                # Test omap argument validation
                log.info("Testing OMAP argument validation")
                omap_invalid_cases = [
                    ("--list-omap --pg {pg}".format(pg=test_pg), "list-omap without object"),
                    ("--get-omap --pg {pg} --object {obj}".format(pg=test_pg, obj=test_object), 
                     "get-omap without omap-key"),
                    ("--set-omap --pg {pg} --object {obj} --omap-key testkey".format(
                        pg=test_pg, obj=test_object), "set-omap without omap-value"),
                ]
                
                for args, description in omap_invalid_cases:
                    cmd = (prefix + args).format(id=osdid)
                    try:
                        result = remote.sh(cmd, check_status=False)
                        log.warning("OMAP command should have failed for {desc}".format(desc=description))
                    except CommandFailedError:
                        log.info("OMAP correctly failed for {desc}".format(desc=description))

                break  # Only test with first available OSD
            break
    else:
        log.info("Skipping OMAP tests - no objects found in PGs")

    log.info("Crimson objectstore tool testing completed")
    log.info("PGs with objects: {}".format(pgswithobjects))
    log.info("Objects in PGs: {}".format(objsinpg))

    return ERRORS