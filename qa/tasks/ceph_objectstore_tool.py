"""
ceph_objectstore_tool - Simple test of ceph-objectstore-tool utility
"""
from io import BytesIO

import contextlib
import json
import logging
import os
import sys
import tempfile
import time
from tasks import ceph_manager
from tasks.util.rados import (rados, create_replicated_pool, create_ec_pool)
from teuthology import misc as teuthology
from teuthology.orchestra import run

from teuthology.exceptions import CommandFailedError

# from util.rados import (rados, create_ec_pool,
#                               create_replicated_pool,
#                               create_cache_pool)

log = logging.getLogger(__name__)

# Global variable to track whether we're using crimson-objectstore-tool
CRIMSON = False

def SKIP_IF_CRIMSON(reason="Not supported in crimson-objectstore-tool"):
    """
    Decorator to skip tests that are not supported in crimson-objectstore-tool
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            if CRIMSON:
                log.info("SKIPPING: {} - {}".format(func.__name__, reason))
                return 0  # Return no errors for skipped tests
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Should get cluster name "ceph" from somewhere
# and normal path from osd_data and osd_journal in conf
FSPATH = "/var/lib/ceph/osd/ceph-{id}"
JPATH = "/var/lib/ceph/osd/ceph-{id}/journal"


def cod_setup_local_data(log, ctx, NUM_OBJECTS, DATADIR,
                         BASE_NAME, DATALINECOUNT):
    objects = range(1, NUM_OBJECTS + 1)
    for i in objects:
        NAME = BASE_NAME + "{num}".format(num=i)
        LOCALNAME = os.path.join(DATADIR, NAME)

        dataline = range(DATALINECOUNT)
        fd = open(LOCALNAME, "w")
        data = "This is the data for " + NAME + "\n"
        for _ in dataline:
            fd.write(data)
        fd.close()


def cod_setup_remote_data(log, ctx, remote, NUM_OBJECTS, DATADIR,
                          BASE_NAME, DATALINECOUNT):

    objects = range(1, NUM_OBJECTS + 1)
    for i in objects:
        NAME = BASE_NAME + "{num}".format(num=i)
        DDNAME = os.path.join(DATADIR, NAME)

        remote.run(args=['rm', '-f', DDNAME])

        dataline = range(DATALINECOUNT)
        data = "This is the data for " + NAME + "\n"
        DATA = ""
        for _ in dataline:
            DATA += data
        remote.write_file(DDNAME, DATA)


def cod_setup(log, ctx, remote, NUM_OBJECTS, DATADIR,
              BASE_NAME, DATALINECOUNT, POOL, db, ec):
    ERRORS = 0
    log.info("Creating {objs} objects in pool".format(objs=NUM_OBJECTS))

    objects = range(1, NUM_OBJECTS + 1)
    for i in objects:
        NAME = BASE_NAME + "{num}".format(num=i)
        DDNAME = os.path.join(DATADIR, NAME)

        proc = rados(ctx, remote, ['-p', POOL, 'put', NAME, DDNAME],
                     wait=False)
        # proc = remote.run(args=['rados', '-p', POOL, 'put', NAME, DDNAME])
        ret = proc.wait()
        if ret != 0:
            log.critical("Rados put failed with status {ret}".
                         format(ret=proc.exitstatus))
            sys.exit(1)

        db[NAME] = {}

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

        # Erasure coded pools don't support omap
        if ec:
            continue

        # Create omap header in all objects but REPobject1
        if i != 1:
            myhdr = "hdr{i}".format(i=i)
            proc = remote.run(args=['rados', '-p', POOL, 'setomapheader',
                                    NAME, myhdr])
            ret = proc.wait()
            if ret != 0:
                log.critical("setomapheader failed with {ret}".format(ret=ret))
                ERRORS += 1
            db[NAME]["omapheader"] = myhdr

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
                log.critical("setomapval failed with {ret}".format(ret=ret))
            db[NAME]["omap"][mykey] = myval

    return ERRORS


def get_lines(filename):
    tmpfd = open(filename, "r")
    line = True
    lines = []
    while line:
        line = tmpfd.readline().rstrip('\n')
        if line:
            lines += [line]
    tmpfd.close()
    os.unlink(filename)
    return lines


@contextlib.contextmanager
def task(ctx, config):
    """
    Run ceph_objectstore_tool test

    The config should be as follows::

        ceph_objectstore_tool:
          objects: 20 # <number of objects>
          pgnum: 12
          crimson_objectstore_tool: true # use crimson-objectstore-tool instead of ceph-objectstore-tool
    """

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'ceph_objectstore_tool task only accepts a dict for configuration'

    # Set global CRIMSON flags based on configuration
    global CRIMSON
    global CRIMSON_DEVICE_TYPE
    CRIMSON = config.get('crimson_objectstore_tool', False)
    log.info('crimson_objectstore_tool is {}...'.format(CRIMSON))

    log.debug(config)
    log.debug(ctx)
    clients = ctx.cluster.only(teuthology.is_type('client'))
    assert len(clients.remotes) > 0, 'Must specify at least 1 client'
    (cli_remote, _) = clients.remotes.popitem()
    log.debug(cli_remote)

    # clients = dict(teuthology.get_clients(ctx=ctx, roles=config.keys()))
    # client = clients.popitem()
    # log.info(client)
    osds = ctx.cluster.only(teuthology.is_type('osd'))
    log.info("OSDS")
    log.info(osds)
    log.info(osds.remotes)

    manager = ctx.managers['ceph']
    while (len(manager.get_osd_status()['up']) !=
           len(manager.get_osd_status()['raw'])):
        time.sleep(10)
    while (len(manager.get_osd_status()['in']) !=
           len(manager.get_osd_status()['up'])):
        time.sleep(10)
    manager.raw_cluster_cmd('osd', 'set', 'noout')
    manager.raw_cluster_cmd('osd', 'set', 'nodown')

    if CRIMSON:
        CRIMSON_DEVICE_TYPE = manager.get_config('osd', 0, 'seastore_main_device_type')
        log.info('seastore_main_device_type is {}...'.format(CRIMSON_DEVICE_TYPE))

    PGNUM = config.get('pgnum', 12)
    log.info("pgnum: {num}".format(num=PGNUM))

    ERRORS = 0

    REP_POOL = "rep_pool"
    REP_NAME = "REPobject"
    create_replicated_pool(cli_remote, REP_POOL, PGNUM)
    ERRORS += test_objectstore(ctx, config, cli_remote, REP_POOL, REP_NAME)

    EC_POOL = "ec_pool"
    EC_NAME = "ECobject"
    if not CRIMSON:
        # EC pools may not be supported in crimson-objectstore-tool yet
        create_ec_pool(cli_remote, EC_POOL, 'default', PGNUM)
        ERRORS += test_objectstore(ctx, config, cli_remote,
                                   EC_POOL, EC_NAME, ec=True)
    else:
        log.info("SKIPPING EC pool tests for crimson-objectstore-tool")

    if ERRORS == 0:
        log.info("TEST PASSED")
    else:
        log.error("TEST FAILED WITH {errcount} ERRORS".format(errcount=ERRORS))

    assert ERRORS == 0

    try:
        yield
    finally:
        log.info('Ending ceph_objectstore_tool')


def test_objectstore(ctx, config, cli_remote, REP_POOL, REP_NAME, ec=False):
    manager = ctx.managers['ceph']

    osds = ctx.cluster.only(teuthology.is_type('osd'))

    TEUTHDIR = teuthology.get_testdir(ctx)
    DATADIR = os.path.join(TEUTHDIR, "ceph.data")
    DATALINECOUNT = 10000
    ERRORS = 0
    NUM_OBJECTS = config.get('objects', 10)
    log.info("objects: {num}".format(num=NUM_OBJECTS))

    pool_dump = manager.get_pool_dump(REP_POOL)
    REPID = pool_dump['pool']

    log.debug("repid={num}".format(num=REPID))

    db = {}

    LOCALDIR = tempfile.mkdtemp("cod")

    cod_setup_local_data(log, ctx, NUM_OBJECTS, LOCALDIR,
                         REP_NAME, DATALINECOUNT)
    allremote = []
    allremote.append(cli_remote)
    allremote += list(osds.remotes.keys())
    allremote = list(set(allremote))
    for remote in allremote:
        cod_setup_remote_data(log, ctx, remote, NUM_OBJECTS, DATADIR,
                              REP_NAME, DATALINECOUNT)

    ERRORS += cod_setup(log, ctx, cli_remote, NUM_OBJECTS, DATADIR,
                        REP_NAME, DATALINECOUNT, REP_POOL, db, ec)

    pgs = {}
    for stats in manager.get_pg_stats():
        if stats["pgid"].find(str(REPID) + ".") != 0:
            continue
        if pool_dump["type"] == ceph_manager.PoolType.REPLICATED:
            for osd in stats["acting"]:
                pgs.setdefault(osd, []).append(stats["pgid"])
        elif pool_dump["type"] == ceph_manager.PoolType.ERASURE_CODED:
            shard = 0
            for osd in stats["acting"]:
                pgs.setdefault(osd, []).append("{pgid}s{shard}".
                                               format(pgid=stats["pgid"],
                                                      shard=shard))
                shard += 1
        else:
            raise Exception("{pool} has an unexpected type {type}".
                            format(pool=REP_POOL, type=pool_dump["type"]))

    log.info("Expected pgs: {_pgs}".format(_pgs=pgs))
    log.info("Expected : {_db}".format(_db=db))

    for osd in manager.get_osd_status()['up']:
        manager.kill_osd(osd)
    time.sleep(5)

    pgswithobjects = set()
    objsinpg = {}

    # Test --op list and generate json for all objects
    log.info("Test --op list by generating json for all objects")
    
    if CRIMSON:
        prefix = ("sudo crimson-objectstore-tool "
                  "--data-path {fpath} "
                  "--device-type {device_t} ").format(fpath=FSPATH, device_t=CRIMSON_DEVICE_TYPE)
    else:
        prefix = ("sudo ceph-objectstore-tool "
                  "--data-path {fpath} "
                  "--journal-path {jpath} ").format(fpath=FSPATH, jpath=JPATH)
    for remote in osds.remotes.keys():
        log.debug(remote)
        log.debug(osds.remotes[remote])
        for role in osds.remotes[remote]:
            if not role.startswith("osd."):
                continue
            osdid = int(role.split('.')[1])
            log.info("process osd.{id} on {remote}".
                     format(id=osdid, remote=remote))
            cmd = (prefix + "--op list").format(id=osdid)
            try:
                lines = remote.sh(cmd, check_status=False).splitlines()
                for pgline in lines:
                    if not pgline or not pgline.startswith("["):
                        continue
                    log.info("parsing {_line}".format(_line=pgline))
                    (pg, obj) = json.loads(pgline)
                    name = obj['oid']
                    if name in db:
                        log.info("found {_name}".format(_name=name))
                        pgswithobjects.add(pg)
                        objsinpg.setdefault(pg, []).append(name)
                        db[name].setdefault("pg2json",
                                            {})[pg] = json.dumps(obj)
            except CommandFailedError as e:
                log.error("Bad exit status {ret} from --op list request".
                          format(ret=e.exitstatus))
                ERRORS += 1

    log.info("Finished --op list")
    log.info("Found: {_db}".format(_db=db))
    log.info("Found pgs: {_pgswithobjects}".format(_pgswithobjects=pgswithobjects))
    log.info("Found objects by pgs: {_objsinpg}".format(_objsinpg=objsinpg))

    if pool_dump["type"] == ceph_manager.PoolType.REPLICATED:
        # Test get-bytes
        log.info("Test get-bytes and set-bytes")
        for basename in db.keys():
            file = os.path.join(DATADIR, basename)
            GETNAME = os.path.join(DATADIR, "get")
            SETNAME = os.path.join(DATADIR, "set")

            for remote in osds.remotes.keys():
                for role in osds.remotes[remote]:
                    if not role.startswith("osd."):
                        continue
                    osdid = int(role.split('.')[1])
                    if osdid not in pgs:
                        continue

                    for pg, JSON in db[basename]["pg2json"].items():
                        if pg in pgs[osdid]:
                            cmd = ((prefix + "--pgid {pg}").
                                   format(id=osdid, pg=pg).split())
                            cmd.append(run.Raw("'{json}'".format(json=JSON)))
                            cmd += ("get-bytes {fname}".
                                    format(fname=GETNAME).split())
                            proc = remote.run(args=cmd, check_status=False)
                            if proc.exitstatus != 0:
                                remote.run(args="rm -f {getfile}".
                                           format(getfile=GETNAME).split())
                                log.error("Bad exit status {ret}".
                                          format(ret=proc.exitstatus))
                                ERRORS += 1
                                continue
                            cmd = ("diff -q {file} {getfile}".
                                   format(file=file, getfile=GETNAME))
                            proc = remote.run(args=cmd.split())
                            if proc.exitstatus != 0:
                                log.error("Data from get-bytes differ")
                                # log.debug("Got:")
                                # cat_file(logging.DEBUG, GETNAME)
                                # log.debug("Expected:")
                                # cat_file(logging.DEBUG, file)
                                ERRORS += 1
                            else:
                                log.debug("Got original data from get-bytes {_basename} "
                                          "successfully!".format(_basename=basename))

                            remote.run(args="rm -f {getfile}".
                                       format(getfile=GETNAME).split())

                            log.debug("Setting new data to {_basename}".format(_basename=basename))

                            data = ("set-bytes going into {file}\n".
                                    format(file=file))
                            remote.write_file(SETNAME, data)
                            cmd = ((prefix + "--pgid {pg}").
                                   format(id=osdid, pg=pg).split())
                            cmd.append(run.Raw("'{json}'".format(json=JSON)))
                            cmd += ("set-bytes {fname}".
                                    format(fname=SETNAME).split())
                            proc = remote.run(args=cmd, check_status=False)
                            proc.wait()
                            if proc.exitstatus != 0:
                                log.info("set-bytes failed for object {obj} "
                                         "in pg {pg} osd.{id} ret={ret}".
                                         format(obj=basename, pg=pg,
                                                id=osdid, ret=proc.exitstatus))
                                ERRORS += 1

                            cmd = ((prefix + "--pgid {pg}").
                                   format(id=osdid, pg=pg).split())
                            cmd.append(run.Raw("'{json}'".format(json=JSON)))
                            cmd += "get-bytes -".split()
                            try:
                                output = remote.sh(cmd, wait=True)
                                if data != output:
                                    log.error("Data inconsistent after "
                                              "set-bytes, got:")
                                    log.error(output)
                                    ERRORS += 1
                                else:
                                    log.debug("Got data after set-bytes to {_basename} "
                                              "successfully!".format(_basename=basename))

                            except CommandFailedError as e:
                                log.error("get-bytes after "
                                          "set-bytes ret={ret}".
                                          format(ret=e.exitstatus))
                                ERRORS += 1


                            log.debug("Retrning {_basename} to orginal data".format(_basename=basename))

                            cmd = ((prefix + "--pgid {pg}").
                                   format(id=osdid, pg=pg).split())
                            cmd.append(run.Raw("'{json}'".format(json=JSON)))
                            cmd += ("set-bytes {fname}".
                                    format(fname=file).split())
                            proc = remote.run(args=cmd, check_status=False)
                            proc.wait()
                            if proc.exitstatus != 0:
                                log.info("set-bytes failed for object {obj} "
                                         "in pg {pg} osd.{id} ret={ret}".
                                         format(obj=basename, pg=pg,
                                                id=osdid, ret=proc.exitstatus))
                                ERRORS += 1

    log.info("Test list-attrs get-attr")
    for basename in db.keys():
        file = os.path.join(DATADIR, basename)
        GETNAME = os.path.join(DATADIR, "get")
        SETNAME = os.path.join(DATADIR, "set")

        for remote in osds.remotes.keys():
            for role in osds.remotes[remote]:
                if not role.startswith("osd."):
                    continue
                osdid = int(role.split('.')[1])
                if osdid not in pgs:
                    continue

                for pg, JSON in db[basename]["pg2json"].items():
                    if pg in pgs[osdid]:
                        cmd = ((prefix + "--pgid {pg}").
                               format(id=osdid, pg=pg).split())
                        cmd.append(run.Raw("'{json}'".format(json=JSON)))
                        cmd += ["list-attrs"]
                        try:
                            keys = remote.sh(cmd, wait=True, stderr=BytesIO()).split()
                        except CommandFailedError as e:
                            log.error("Bad exit status {ret}".
                                      format(ret=e.exitstatus))
                            ERRORS += 1
                            continue
                        values = dict(db[basename]["xattr"])

                        for key in keys:
                            if (key == "_" or
                                    key == "snapset" or
                                    key == "hinfo_key" or
                                    key == "omap_header"):
                                continue
                            key = key.strip("_")
                            if key not in values:
                                log.error("The key {key} should be present".
                                          format(key=key))
                                ERRORS += 1
                                continue
                            exp = values.pop(key)
                            cmd = ((prefix + "--pgid {pg}").
                                   format(id=osdid, pg=pg).split())
                            cmd.append(run.Raw("'{json}'".format(json=JSON)))
                            cmd += ("get-attr {key}".
                                    format(key="_" + key).split())
                            try:
                                val = remote.sh(cmd, wait=True)
                            except CommandFailedError as e:
                                log.error("get-attr failed with {ret}".
                                          format(ret=e.exitstatus))
                                ERRORS += 1
                                continue
                            if exp != val:
                                log.error("For key {key} got value {got} "
                                          "instead of {expected}".
                                          format(key=key, got=val,
                                                 expected=exp))
                                ERRORS += 1
                        if "hinfo_key" in keys:
                            cmd_prefix = prefix.format(id=osdid)
                            cmd = """
      expected=$({prefix} --pgid {pg} '{json}' get-attr {key} | base64)
      echo placeholder | {prefix} --pgid {pg} '{json}' set-attr {key} -
      test $({prefix} --pgid {pg} '{json}' get-attr {key}) = placeholder
      echo $expected | base64 --decode | \
         {prefix} --pgid {pg} '{json}' set-attr {key} -
      test $({prefix} --pgid {pg} '{json}' get-attr {key} | base64) = $expected
                            """.format(prefix=cmd_prefix, pg=pg, json=JSON,
                                       key="hinfo_key")
                            log.debug(cmd)
                            proc = remote.run(args=['bash', '-e', '-x',
                                                    '-c', cmd],
                                              check_status=False,
                                              stdout=BytesIO(),
                                              stderr=BytesIO())
                            proc.wait()
                            if proc.exitstatus != 0:
                                log.error("failed with " +
                                          str(proc.exitstatus))
                                log.error(" ".join([
                                    proc.stdout.getvalue().decode(),
                                    proc.stderr.getvalue().decode(),
                                    ]))
                                ERRORS += 1

                        if len(values) != 0:
                            log.error("Not all keys found, remaining keys:")
                            log.error(values)

    def test_pg_info():
        local_errors = 0
        log.info("Test pg info")
        for remote in osds.remotes.keys():
            for role in osds.remotes[remote]:
                if not role.startswith("osd."):
                    continue
                osdid = int(role.split('.')[1])
                if osdid not in pgs:
                    continue

                for pg in pgs[osdid]:
                    cmd = ((prefix + "--op info --pgid {pg}").
                           format(id=osdid, pg=pg).split())
                    try:
                        info = remote.sh(cmd, wait=True)
                    except CommandFailedError as e:
                        log.error("Failure of --op info command with %s",
                                  e.exitstatus)
                        local_errors += 1
                        continue
                    if not str(pg) in info:
                        log.error("Bad data from info: %s", info)
                        local_errors += 1
        return local_errors

    ERRORS += test_pg_info()

    @SKIP_IF_CRIMSON("PG logging not implemented")
    def test_pg_logging():
        local_errors = 0
        log.info("Test pg logging")
        for remote in osds.remotes.keys():
            for role in osds.remotes[remote]:
                if not role.startswith("osd."):
                    continue
                osdid = int(role.split('.')[1])
                if osdid not in pgs:
                    continue

                for pg in pgs[osdid]:
                    cmd = ((prefix + "--op log --pgid {pg}").
                           format(id=osdid, pg=pg).split())
                    try:
                        output = remote.sh(cmd, wait=True)
                    except CommandFailedError as e:
                        log.error("Getting log failed for pg {pg} "
                                  "from osd.{id} with {ret}".
                                  format(pg=pg, id=osdid, ret=e.exitstatus))
                        local_errors += 1
                        continue
                    HASOBJ = pg in pgswithobjects
                    MODOBJ = "modify" in output
                    if HASOBJ != MODOBJ:
                        log.error("Bad log for pg {pg} from osd.{id}".
                                  format(pg=pg, id=osdid))
                        MSG = (HASOBJ and [""] or ["NOT "])[0]
                        log.error("Log should {msg}have a modify entry".
                                  format(msg=MSG))
                        local_errors += 1
        return local_errors
    
    ERRORS += test_pg_logging()

    @SKIP_IF_CRIMSON("PG export not implemented")
    def test_pg_export():
        exp_errors = 0
        log.info("Test pg export")
        for remote in osds.remotes.keys():
            for role in osds.remotes[remote]:
                if not role.startswith("osd."):
                    continue
                osdid = int(role.split('.')[1])
                if osdid not in pgs:
                    continue

                for pg in pgs[osdid]:
                    fpath = os.path.join(DATADIR, "osd{id}.{pg}".
                                         format(id=osdid, pg=pg))

                    cmd = ((prefix + "--op export --pgid {pg} --file {file}").
                           format(id=osdid, pg=pg, file=fpath))
                    try:
                        remote.sh(cmd, wait=True)
                    except CommandFailedError as e:
                        log.error("Exporting failed for pg {pg} "
                                  "on osd.{id} with {ret}".
                                  format(pg=pg, id=osdid, ret=e.exitstatus))
                        exp_errors += 1
        return exp_errors

    EXP_ERRORS = test_pg_export()
    ERRORS += EXP_ERRORS

    @SKIP_IF_CRIMSON("PG removal not implemented")
    def test_pg_removal():
        rm_errors = 0
        log.info("Test pg removal")
        for remote in osds.remotes.keys():
            for role in osds.remotes[remote]:
                if not role.startswith("osd."):
                    continue
                osdid = int(role.split('.')[1])
                if osdid not in pgs:
                    continue

                for pg in pgs[osdid]:
                    cmd = ((prefix + "--force --op remove --pgid {pg}").
                           format(pg=pg, id=osdid))
                    try:
                        remote.sh(cmd, wait=True)
                    except CommandFailedError as e:
                        log.error("Removing failed for pg {pg} "
                                  "on osd.{id} with {ret}".
                                  format(pg=pg, id=osdid, ret=e.exitstatus))
                        rm_errors += 1
        return rm_errors

    RM_ERRORS = test_pg_removal()
    ERRORS += RM_ERRORS

    @SKIP_IF_CRIMSON("PG import not implemented")
    def test_pg_import():
        imp_errors = 0
        if EXP_ERRORS == 0 and RM_ERRORS == 0:
            log.info("Test pg import")

            for remote in osds.remotes.keys():
                for role in osds.remotes[remote]:
                    if not role.startswith("osd."):
                        continue
                    osdid = int(role.split('.')[1])
                    if osdid not in pgs:
                        continue

                    for pg in pgs[osdid]:
                        fpath = os.path.join(DATADIR, "osd{id}.{pg}".
                                             format(id=osdid, pg=pg))

                        cmd = ((prefix + "--op import --file {file}").
                               format(id=osdid, file=fpath))
                        try:
                            remote.sh(cmd, wait=True)
                        except CommandFailedError as e:
                            log.error("Import failed from {file} with {ret}".
                                      format(file=fpath, ret=e.exitstatus))
                            imp_errors += 1
        else:
            log.warning("SKIPPING IMPORT TESTS DUE TO PREVIOUS FAILURES")
        return imp_errors

    IMP_ERRORS = test_pg_import()
    ERRORS += IMP_ERRORS

    if EXP_ERRORS == 0 and RM_ERRORS == 0 and IMP_ERRORS == 0:
        log.info("Restarting OSDs....")
        # They are still look to be up because of setting nodown
        for osd in manager.get_osd_status()['up']:
            manager.revive_osd(osd)
        # Wait for health?
        time.sleep(5)
        # Let scrub after test runs verify consistency of all copies
        log.info("Verify replicated import data")
        objects = range(1, NUM_OBJECTS + 1)
        for i in objects:
            NAME = REP_NAME + "{num}".format(num=i)
            TESTNAME = os.path.join(DATADIR, "gettest")
            REFNAME = os.path.join(DATADIR, NAME)

            proc = rados(ctx, cli_remote,
                         ['-p', REP_POOL, 'get', NAME, TESTNAME], wait=False)

            ret = proc.wait()
            if ret != 0:
                log.error("After import, rados get failed with {ret}".
                          format(ret=proc.exitstatus))
                ERRORS += 1
                continue

            cmd = "diff -q {gettest} {ref}".format(gettest=TESTNAME,
                                                   ref=REFNAME)
            proc = cli_remote.run(args=cmd, check_status=False)
            proc.wait()
            if proc.exitstatus != 0:
                log.error("Data comparison failed for {obj}".format(obj=NAME))
                ERRORS += 1

    return ERRORS
