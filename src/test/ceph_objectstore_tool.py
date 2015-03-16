#!/usr/bin/env python

from subprocess import call
try:
    from subprocess import check_output
except ImportError:
    def check_output(*popenargs, **kwargs):
        import subprocess
        # backported from python 2.7 stdlib
        process = subprocess.Popen(
           stdout=subprocess.PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            error = subprocess.CalledProcessError(retcode, cmd)
            error.output = output
            raise error
        return output

import subprocess
import os
import time
import sys
import re
import string
import logging
import json

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.WARNING)


def wait_for_health():
    print "Wait for health_ok...",
    while call("./ceph health 2> /dev/null | grep -v 'HEALTH_OK\|HEALTH_WARN' > /dev/null", shell=True) == 0:
        time.sleep(5)
    print "DONE"


def get_pool_id(name, nullfd):
    cmd = "./ceph osd pool stats {pool}".format(pool=name).split()
    # pool {pool} id # .... grab the 4 field
    return check_output(cmd, stderr=nullfd).split()[3]


# return a list of unique PGS given an osd subdirectory
def get_osd_pgs(SUBDIR, ID):
    PGS = []
    if ID:
        endhead = re.compile("{id}.*_head$".format(id=ID))
    DIR = os.path.join(SUBDIR, "current")
    PGS += [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and (ID == None or endhead.match(f))]
    PGS = [re.sub("_head", "", p) for p in PGS if "_head" in p]
    return PGS


# return a sorted list of unique PGs given a directory
def get_pgs(DIR, ID):
    OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and string.find(f, "osd") == 0]
    PGS = []
    for d in OSDS:
        SUBDIR = os.path.join(DIR, d)
        PGS += get_osd_pgs(SUBDIR, ID)
    return sorted(set(PGS))


# return a sorted list of PGS a subset of ALLPGS that contain objects with prefix specified
def get_objs(ALLPGS, prefix, DIR, ID):
    OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and string.find(f, "osd") == 0]
    PGS = []
    for d in OSDS:
        DIRL2 = os.path.join(DIR, d)
        SUBDIR = os.path.join(DIRL2, "current")
        for p in ALLPGS:
            PGDIR = p + "_head"
            if not os.path.isdir(os.path.join(SUBDIR, PGDIR)):
                continue
            FINALDIR = os.path.join(SUBDIR, PGDIR)
            # See if there are any objects there
            if [ f for f in [ val for  _, _, fl in os.walk(FINALDIR) for val in fl ] if string.find(f, prefix) == 0 ]:
                PGS += [p]
    return sorted(set(PGS))


# return a sorted list of OSDS which have data from a given PG
def get_osds(PG, DIR):
    ALLOSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and string.find(f, "osd") == 0]
    OSDS = []
    for d in ALLOSDS:
        DIRL2 = os.path.join(DIR, d)
        SUBDIR = os.path.join(DIRL2, "current")
        PGDIR = PG + "_head"
        if not os.path.isdir(os.path.join(SUBDIR, PGDIR)):
            continue
        OSDS += [d]
    return sorted(OSDS)


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


def cat_file(level, filename):
    if level < logging.getLogger().getEffectiveLevel():
        return
    print "File: " + filename
    with open(filename, "r") as f:
        while True:
            line = f.readline().rstrip('\n')
            if not line:
                break
            print line
    print "<EOF>"


def vstart(new):
    print "vstarting....",
    OPT = new and "-n" or ""
    call("MON=1 OSD=4 CEPH_PORT=7400 ./vstart.sh -l {opt} -d mon osd > /dev/null 2>&1".format(opt=OPT), shell=True)
    print "DONE"

def test_failure_tty(cmd, errmsg):
    try:
        ttyfd = open("/dev/tty", "rw")
    except Exception, e:
        logging.info(str(e))
        logging.info("SKIP " + cmd)
        return 0
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=os.getpid())
    tmpfd = open(TMPFILE, "w")

    logging.debug(cmd)
    ret = call(cmd, shell=True, stdin=ttyfd, stdout=ttyfd, stderr=tmpfd)
    ttyfd.close()
    tmpfd.close()
    if ret == 0:
        logging.error("Should have failed, but got exit 0")
        return 1
    lines = get_lines(TMPFILE)
    line = lines[0]
    if line == errmsg:
        logging.info("Correctly failed with message \"" + line + "\"")
        return 0
    else:
        logging.error("Bad message to stderr \"" + line + "\"")
        return 1

def test_failure(cmd, errmsg):
    logging.debug(cmd)
    try:
        out = check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        logging.error("Should have failed, but got exit 0")
        return 1
    except subprocess.CalledProcessError, e:
        if errmsg in e.output:
            logging.info("Correctly failed with message \"" + errmsg + "\"")
            return 0
        else:
            logging.error("Bad message to stderr \"" + e.output + "\"")
            return 1

def get_nspace(num):
    if num == 0:
        return ""
    return "ns{num}".format(num=num)


def verify(DATADIR, POOL, NAME_PREFIX):
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=os.getpid())
    nullfd = open(os.devnull, "w")
    ERRORS = 0
    for nsfile in [f for f in os.listdir(DATADIR) if f.split('-')[1].find(NAME_PREFIX) == 0]:
        nspace = nsfile.split("-")[0]
        file = nsfile.split("-")[1]
        path = os.path.join(DATADIR, nsfile)
        try:
            os.unlink(TMPFILE)
        except:
            pass
        cmd = "./rados -p {pool} -N '{nspace}' get {file} {out}".format(pool=POOL, file=file, out=TMPFILE, nspace=nspace)
        logging.debug(cmd)
        call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
        cmd = "diff -q {src} {result}".format(src=path, result=TMPFILE)
        logging.debug(cmd)
        ret = call(cmd, shell=True)
        if ret != 0:
            logging.error("{file} data not imported properly".format(file=file))
            ERRORS += 1
        try:
            os.unlink(TMPFILE)
        except:
            pass
    return ERRORS

CEPH_DIR = "ceph_objectstore_tool_dir"
CEPH_CONF = os.path.join(CEPH_DIR, 'ceph.conf')

def kill_daemons():
    call("./init-ceph -c {conf} stop osd mon > /dev/null 2>&1".format(conf=CEPH_CONF), shell=True)

def main(argv):
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    nullfd = open(os.devnull, "w")

    call("rm -fr ceph_objectstore_tool_dir ; mkdir ceph_objectstore_tool_dir", shell=True)
    os.environ["CEPH_DIR"] = CEPH_DIR;
    OSDDIR = os.path.join(CEPH_DIR, "dev")
    REP_POOL = "rep_pool"
    REP_NAME = "REPobject"
    EC_POOL = "ec_pool"
    EC_NAME = "ECobject"
    if len(argv) > 0 and argv[0] == 'large':
        PG_COUNT = 12
        NUM_REP_OBJECTS = 800
        NUM_EC_OBJECTS = 12
        NUM_NSPACES = 4
        # Larger data sets for first object per namespace
        DATALINECOUNT = 50000
        # Number of objects to do xattr/omap testing on
        ATTR_OBJS = 10
    else:
        PG_COUNT = 4
        NUM_REP_OBJECTS = 2
        NUM_EC_OBJECTS = 2
        NUM_NSPACES = 2
        # Larger data sets for first object per namespace
        DATALINECOUNT = 10
        # Number of objects to do xattr/omap testing on
        ATTR_OBJS = 2
    ERRORS = 0
    pid = os.getpid()
    TESTDIR = "/tmp/test.{pid}".format(pid=pid)
    DATADIR = "/tmp/data.{pid}".format(pid=pid)
    CFSD_PREFIX = "./ceph-objectstore-tool --data-path " + OSDDIR + "/{osd} --journal-path " + OSDDIR + "/{osd}.journal "
    PROFNAME = "testecprofile"

    os.environ['CEPH_CONF'] = CEPH_CONF
    vstart(new=True)
    wait_for_health()

    cmd = "./ceph osd pool create {pool} {pg} {pg} replicated".format(pool=REP_POOL, pg=PG_COUNT)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    REPID = get_pool_id(REP_POOL, nullfd)

    print "Created Replicated pool #{repid}".format(repid=REPID)

    cmd = "./ceph osd erasure-code-profile set {prof} ruleset-failure-domain=osd".format(prof=PROFNAME)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    cmd = "./ceph osd erasure-code-profile get {prof}".format(prof=PROFNAME)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    cmd = "./ceph osd pool create {pool} {pg} {pg} erasure {prof}".format(pool=EC_POOL, prof=PROFNAME, pg=PG_COUNT)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    ECID = get_pool_id(EC_POOL, nullfd)

    print "Created Erasure coded pool #{ecid}".format(ecid=ECID)

    print "Creating {objs} objects in replicated pool".format(objs=(NUM_REP_OBJECTS*NUM_NSPACES))
    cmd = "mkdir -p {datadir}".format(datadir=DATADIR)
    logging.debug(cmd)
    call(cmd, shell=True)

    db = {}

    objects = range(1, NUM_REP_OBJECTS + 1)
    nspaces = range(NUM_NSPACES)
    for n in nspaces:
        nspace = get_nspace(n)

        db[nspace] = {}

        for i in objects:
            NAME = REP_NAME + "{num}".format(num=i)
            LNAME = nspace + "-" + NAME
            DDNAME = os.path.join(DATADIR, LNAME)

            cmd = "rm -f " + DDNAME
            logging.debug(cmd)
            call(cmd, shell=True)

            if i == 1:
                dataline = range(DATALINECOUNT)
            else:
                dataline = range(1)
            fd = open(DDNAME, "w")
            data = "This is the replicated data for " + LNAME + "\n"
            for _ in dataline:
                fd.write(data)
            fd.close()

            cmd = "./rados -p {pool} -N '{nspace}' put {name} {ddname}".format(pool=REP_POOL, name=NAME, ddname=DDNAME, nspace=nspace)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stderr=nullfd)
            if ret != 0:
                logging.critical("Replicated pool object creation failed with {ret}".format(ret=ret))
                return 1

            db[nspace][NAME] = {}

            if i < ATTR_OBJS + 1:
                keys = range(i)
            else:
                keys = range(0)
            db[nspace][NAME]["xattr"] = {}
            for k in keys:
                if k == 0:
                    continue
                mykey = "key{i}-{k}".format(i=i, k=k)
                myval = "val{i}-{k}".format(i=i, k=k)
                cmd = "./rados -p {pool} -N '{nspace}' setxattr {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval, nspace=nspace)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.error("setxattr failed with {ret}".format(ret=ret))
                    ERRORS += 1
                db[nspace][NAME]["xattr"][mykey] = myval

            # Create omap header in all objects but REPobject1
            if i < ATTR_OBJS + 1 and i != 1:
                myhdr = "hdr{i}".format(i=i)
                cmd = "./rados -p {pool} -N '{nspace}' setomapheader {name} {hdr}".format(pool=REP_POOL, name=NAME, hdr=myhdr, nspace=nspace)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.critical("setomapheader failed with {ret}".format(ret=ret))
                    ERRORS += 1
                db[nspace][NAME]["omapheader"] = myhdr

            db[nspace][NAME]["omap"] = {}
            for k in keys:
                if k == 0:
                    continue
                mykey = "okey{i}-{k}".format(i=i, k=k)
                myval = "oval{i}-{k}".format(i=i, k=k)
                cmd = "./rados -p {pool} -N '{nspace}' setomapval {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval, nspace=nspace)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.critical("setomapval failed with {ret}".format(ret=ret))
                db[nspace][NAME]["omap"][mykey] = myval

    print "Creating {objs} objects in erasure coded pool".format(objs=(NUM_EC_OBJECTS*NUM_NSPACES))

    objects = range(1, NUM_EC_OBJECTS + 1)
    nspaces = range(NUM_NSPACES)
    for n in nspaces:
        nspace = get_nspace(n)

        for i in objects:
            NAME = EC_NAME + "{num}".format(num=i)
            LNAME = nspace + "-" + NAME
            DDNAME = os.path.join(DATADIR, LNAME)

            cmd = "rm -f " + DDNAME
            logging.debug(cmd)
            call(cmd, shell=True)

            if i == 1:
                dataline = range(DATALINECOUNT)
            else:
                dataline = range(1)
            fd = open(DDNAME, "w")
            data = "This is the erasure coded data for " + LNAME + "\n"
            for j in dataline:
                fd.write(data)
            fd.close()

            cmd = "./rados -p {pool} -N '{nspace}' put {name} {ddname}".format(pool=EC_POOL, name=NAME, ddname=DDNAME, nspace=nspace)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stderr=nullfd)
            if ret != 0:
                logging.critical("Erasure coded pool creation failed with {ret}".format(ret=ret))
                return 1

            db[nspace][NAME] = {}

            db[nspace][NAME]["xattr"] = {}
            if i < ATTR_OBJS + 1:
                keys = range(i)
            else:
                keys = range(0)
            for k in keys:
                if k == 0:
                    continue
                mykey = "key{i}-{k}".format(i=i, k=k)
                myval = "val{i}-{k}".format(i=i, k=k)
                cmd = "./rados -p {pool} -N '{nspace}' setxattr {name} {key} {val}".format(pool=EC_POOL, name=NAME, key=mykey, val=myval, nspace=nspace)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.error("setxattr failed with {ret}".format(ret=ret))
                    ERRORS += 1
                db[nspace][NAME]["xattr"][mykey] = myval

            # Omap isn't supported in EC pools
            db[nspace][NAME]["omap"] = {}

    logging.debug(db)

    kill_daemons()

    if ERRORS:
        logging.critical("Unable to set up test")
        return 1

    ALLREPPGS = get_pgs(OSDDIR, REPID)
    logging.debug(ALLREPPGS)
    ALLECPGS = get_pgs(OSDDIR, ECID)
    logging.debug(ALLECPGS)

    OBJREPPGS = get_objs(ALLREPPGS, REP_NAME, OSDDIR, REPID)
    logging.debug(OBJREPPGS)
    OBJECPGS = get_objs(ALLECPGS, EC_NAME, OSDDIR, ECID)
    logging.debug(OBJECPGS)

    ONEPG = ALLREPPGS[0]
    logging.debug(ONEPG)
    osds = get_osds(ONEPG, OSDDIR)
    ONEOSD = osds[0]
    logging.debug(ONEOSD)

    print "Test invalid parameters"
    # On export can't use stdout to a terminal
    cmd = (CFSD_PREFIX + "--op export --pgid {pg}").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure_tty(cmd, "stdout is a tty and no --file filename specified")

    # On export can't use stdout to a terminal
    cmd = (CFSD_PREFIX + "--op export --pgid {pg} --file -").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure_tty(cmd, "stdout is a tty and no --file filename specified")

    OTHERFILE = "/tmp/foo.{pid}".format(pid=pid)
    foofd = open(OTHERFILE, "w")
    foofd.close()

    # On import can't specify a PG
    cmd = (CFSD_PREFIX + "--op import --pgid {pg} --file {FOO}").format(osd=ONEOSD, pg=ONEPG, FOO=OTHERFILE)
    ERRORS += test_failure(cmd, "--pgid option invalid with import")

    os.unlink(OTHERFILE)
    cmd = (CFSD_PREFIX + "--op import --file {FOO}").format(osd=ONEOSD, FOO=OTHERFILE)
    ERRORS += test_failure(cmd, "open: No such file or directory")

    # On import can't use stdin from a terminal
    cmd = (CFSD_PREFIX + "--op import --pgid {pg}").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure_tty(cmd, "stdin is a tty and no --file filename specified")

    # On import can't use stdin from a terminal
    cmd = (CFSD_PREFIX + "--op import --pgid {pg} --file -").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure_tty(cmd, "stdin is a tty and no --file filename specified")

    # Specify a bad --type
    cmd = (CFSD_PREFIX + "--type foobar --op list --pgid {pg}").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Must provide --type (filestore, memstore, keyvaluestore-dev)")

    # Don't specify a data-path
    cmd = "./ceph-objectstore-tool --journal-path {dir}/{osd}.journal --type memstore --op list --pgid {pg}".format(dir=OSDDIR, osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Must provide --data-path")

    # Don't specify a journal-path for filestore
    cmd = "./ceph-objectstore-tool --type filestore --data-path {dir}/{osd} --op list --pgid {pg}".format(dir=OSDDIR, osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Must provide --journal-path")

    # Test --op list and generate json for all objects
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=pid)
    ALLPGS = OBJREPPGS + OBJECPGS

    print "Test --op list variants"
    OSDS = get_osds(ALLPGS[0], OSDDIR)
    osd = OSDS[0]

    # retrieve all objects from all PGs
    cmd = (CFSD_PREFIX + "--op list --format json").format(osd=osd)
    logging.debug(cmd);
    tmpfd = open(TMPFILE, "a")
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=tmpfd)
    if ret != 0:
        logging.error("Bad exit status {ret} from {cmd}".format(ret=ret, cmd=cmd))
        ERRORS += 1
    tmpfd.close()
    lines = get_lines(TMPFILE)
    JSONOBJ = sorted(set(lines))
    (pgid, jsondict) = json.loads(JSONOBJ[0])[0]

    # retrieve all objects in a given PG
    cmd = (CFSD_PREFIX + "--op list --pgid {pg} --format json").format(osd=osd, pg=pgid)
    logging.debug(cmd);
    tmpfd = open(OTHERFILE, "a")
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=tmpfd)
    if ret != 0:
        logging.error("Bad exit status {ret} from {cmd}".format(ret=ret, cmd=cmd))
        ERRORS += 1
    tmpfd.close()
    lines = get_lines(OTHERFILE)
    JSONOBJ = sorted(set(lines))
    (other_pgid, other_jsondict) = json.loads(JSONOBJ[0])[0]

    if pgid != other_pgid or jsondict != other_jsondict:
        logging.error("the first line of --op list is different "
                      "from the first line of --op list --pgid {pg}".format(pg=pgid))
        ERRORS += 1

    # retrieve all objects with a given name in a given PG
    cmd = (CFSD_PREFIX + "--op list --pgid {pg} {object} --format json").format(osd=osd, pg=pgid, object=jsondict['oid'])
    logging.debug(cmd);
    tmpfd = open(OTHERFILE, "a")
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=tmpfd)
    if ret != 0:
        logging.error("Bad exit status {ret} from {cmd}".format(ret=ret, cmd=cmd))
        ERRORS += 1
    tmpfd.close()
    lines = get_lines(OTHERFILE)
    JSONOBJ = sorted(set(lines))
    (other_pgid, other_jsondict) in json.loads(JSONOBJ[0])[0]

    if pgid != other_pgid or jsondict != other_jsondict:
        logging.error("the first line of --op list is different "
                      "from the first line of --op list --pgid {pg} {object}".format(pg=pgid, object=jsondict['oid']))
        ERRORS += 1

    print "Test --op list by generating json for all objects using default format"
    for pg in ALLPGS:
        OSDS = get_osds(pg, OSDDIR)
        for osd in OSDS:
            cmd = (CFSD_PREFIX + "--op list --pgid {pg}").format(osd=osd, pg=pg)
            tmpfd = open(TMPFILE, "a")
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=tmpfd)
            if ret != 0:
                logging.error("Bad exit status {ret} from --op list request".format(ret=ret))
                ERRORS += 1

    tmpfd.close()
    lines = get_lines(TMPFILE)
    JSONOBJ = sorted(set(lines))
    for JSON in JSONOBJ:
        (pgid, jsondict) = json.loads(JSON)
        db[jsondict['namespace']][jsondict['oid']]['json'] = json.dumps((pgid, jsondict))
        # print db[jsondict['namespace']][jsondict['oid']]['json']
        if string.find(jsondict['oid'], EC_NAME) == 0 and 'shard_id' not in jsondict:
            logging.error("Malformed JSON {json}".format(json=JSON))
            ERRORS += 1

    # Test get-bytes
    print "Test get-bytes and set-bytes"
    for nspace in db.keys():
        for basename in db[nspace].keys():
            file = os.path.join(DATADIR, nspace + "-" + basename)
            JSON = db[nspace][basename]['json']
            GETNAME = "/tmp/getbytes.{pid}".format(pid=pid)
            TESTNAME = "/tmp/testbytes.{pid}".format(pid=pid)
            SETNAME = "/tmp/setbytes.{pid}".format(pid=pid)
            for pg in OBJREPPGS:
                OSDS = get_osds(pg, OSDDIR)
                for osd in OSDS:
                    DIR = os.path.join(OSDDIR, os.path.join(osd, os.path.join("current", "{pg}_head".format(pg=pg))))
                    fnames = [f for f in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, f))
                              and f.split("_")[0] == basename and f.split("_")[4] == nspace]
                    if not fnames:
                        continue
                    try:
                        os.unlink(GETNAME)
                    except:
                        pass
                    cmd = (CFSD_PREFIX + " --pgid {pg} '{json}' get-bytes {fname}").format(osd=osd, pg=pg, json=JSON, fname=GETNAME)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True)
                    if ret != 0:
                        logging.error("Bad exit status {ret}".format(ret=ret))
                        ERRORS += 1
                        continue
                    cmd = "diff -q {file} {getfile}".format(file=file, getfile=GETNAME)
                    ret = call(cmd, shell=True)
                    if ret != 0:
                        logging.error("Data from get-bytes differ")
                        logging.debug("Got:")
                        cat_file(logging.DEBUG, GETNAME)
                        logging.debug("Expected:")
                        cat_file(logging.DEBUG, file)
                        ERRORS += 1
                    fd = open(SETNAME, "w")
                    data = "put-bytes going into {file}\n".format(file=file)
                    fd.write(data)
                    fd.close()
                    cmd = (CFSD_PREFIX + "--pgid {pg} '{json}' set-bytes {sname}").format(osd=osd, pg=pg, json=JSON, sname=SETNAME)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True)
                    if ret != 0:
                        logging.error("Bad exit status {ret} from set-bytes".format(ret=ret))
                        ERRORS += 1
                    fd = open(TESTNAME, "w")
                    cmd = (CFSD_PREFIX + "--pgid {pg} '{json}' get-bytes -").format(osd=osd, pg=pg, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdout=fd)
                    fd.close()
                    if ret != 0:
                        logging.error("Bad exit status {ret} from get-bytes".format(ret=ret))
                        ERRORS += 1
                    cmd = "diff -q {setfile} {testfile}".format(setfile=SETNAME, testfile=TESTNAME)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True)
                    if ret != 0:
                        logging.error("Data after set-bytes differ")
                        logging.debug("Got:")
                        cat_file(logging.DEBUG, TESTNAME)
                        logging.debug("Expected:")
                        cat_file(logging.DEBUG, SETNAME)
                        ERRORS += 1
                    fd = open(file, "r")
                    cmd = (CFSD_PREFIX + "--pgid {pg} '{json}' set-bytes").format(osd=osd, pg=pg, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdin=fd)
                    if ret != 0:
                        logging.error("Bad exit status {ret} from set-bytes to restore object".format(ret=ret))
                        ERRORS += 1

    try:
        os.unlink(GETNAME)
    except:
        pass
    try:
        os.unlink(TESTNAME)
    except:
        pass
    try:
        os.unlink(SETNAME)
    except:
        pass

    print "Test list-attrs get-attr"
    ATTRFILE = r"/tmp/attrs.{pid}".format(pid=pid)
    VALFILE = r"/tmp/val.{pid}".format(pid=pid)
    for nspace in db.keys():
        for basename in db[nspace].keys():
            file = os.path.join(DATADIR, nspace + "-" + basename)
            JSON = db[nspace][basename]['json']
            jsondict = json.loads(JSON)

            if 'shard_id' in jsondict:
                logging.debug("ECobject " + JSON)
                found = 0
                for pg in OBJECPGS:
                    OSDS = get_osds(pg, OSDDIR)
                    # Fix shard_id since we only have one json instance for each object
                    jsondict['shard_id'] = int(string.split(pg, 's')[1])
                    JSON = json.dumps(jsondict)
                    for osd in OSDS:
                        cmd = (CFSD_PREFIX + "--pgid {pg} '{json}' get-attr hinfo_key").format(osd=osd, pg=pg, json=JSON)
                        logging.debug("TRY: " + cmd)
                        try:
                            out = check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                            logging.debug("FOUND: {json} in {osd} has value '{val}'".format(osd=osd, json=JSON, val=out))
                            found += 1
                        except subprocess.CalledProcessError, e:
                            if "No such file or directory" not in e.output and "No data available" not in e.output:
                                raise
                # Assuming k=2 m=1 for the default ec pool
                if found != 3:
                    logging.error("{json} hinfo_key found {found} times instead of 3".format(json=JSON, found=found))
                    ERRORS += 1

            for pg in ALLPGS:
                # Make sure rep obj with rep pg or ec obj with ec pg
                if ('shard_id' in jsondict) != (pg.find('s') > 0):
                    continue
                if 'shard_id' in jsondict:
                    # Fix shard_id since we only have one json instance for each object
                    jsondict['shard_id'] = int(string.split(pg, 's')[1])
                    JSON = json.dumps(jsondict)
                OSDS = get_osds(pg, OSDDIR)
                for osd in OSDS:
                    DIR = os.path.join(OSDDIR, os.path.join(osd, os.path.join("current", "{pg}_head".format(pg=pg))))
                    fnames = [f for f in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, f))
                              and f.split("_")[0] == basename and f.split("_")[4] == nspace]
                    if not fnames:
                        continue
                    afd = open(ATTRFILE, "w")
                    cmd = (CFSD_PREFIX + "--pgid {pg} '{json}' list-attrs").format(osd=osd, pg=pg, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdout=afd)
                    afd.close()
                    if ret != 0:
                        logging.error("list-attrs failed with {ret}".format(ret=ret))
                        ERRORS += 1
                        continue
                    keys = get_lines(ATTRFILE)
                    values = dict(db[nspace][basename]["xattr"])
                    for key in keys:
                        if key == "_" or key == "snapset" or key == "hinfo_key":
                            continue
                        key = key.strip("_")
                        if key not in values:
                            logging.error("Unexpected key {key} present".format(key=key))
                            ERRORS += 1
                            continue
                        exp = values.pop(key)
                        vfd = open(VALFILE, "w")
                        cmd = (CFSD_PREFIX + "--pgid {pg} '{json}' get-attr {key}").format(osd=osd, pg=pg, json=JSON, key="_" + key)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True, stdout=vfd)
                        vfd.close()
                        if ret != 0:
                            logging.error("get-attr failed with {ret}".format(ret=ret))
                            ERRORS += 1
                            continue
                        lines = get_lines(VALFILE)
                        val = lines[0]
                        if exp != val:
                            logging.error("For key {key} got value {got} instead of {expected}".format(key=key, got=val, expected=exp))
                            ERRORS += 1
                    if len(values) != 0:
                        logging.error("Not all keys found, remaining keys:")
                        print values

    print "Test pg info"
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            cmd = (CFSD_PREFIX + "--op info --pgid {pg} | grep '\"pgid\": \"{pg}\"'").format(osd=osd, pg=pg)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=nullfd)
            if ret != 0:
                logging.error("Getting info failed for pg {pg} from {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
                ERRORS += 1

    print "Test pg logging"
    if len(ALLREPPGS + ALLECPGS) == len(OBJREPPGS + OBJECPGS):
        logging.warning("All PGs have objects, so no log without modify entries")
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            tmpfd = open(TMPFILE, "w")
            cmd = (CFSD_PREFIX + "--op log --pgid {pg}").format(osd=osd, pg=pg)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=tmpfd)
            if ret != 0:
                logging.error("Getting log failed for pg {pg} from {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
                ERRORS += 1
            HASOBJ = pg in OBJREPPGS + OBJECPGS
            MODOBJ = False
            for line in get_lines(TMPFILE):
                if line.find("modify") != -1:
                    MODOBJ = True
                    break
            if HASOBJ != MODOBJ:
                logging.error("Bad log for pg {pg} from {osd}".format(pg=pg, osd=osd))
                MSG = (HASOBJ and [""] or ["NOT "])[0]
                print "Log should {msg}have a modify entry".format(msg=MSG)
                ERRORS += 1

    try:
        os.unlink(TMPFILE)
    except:
        pass

    print "Test list-pgs"
    for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and string.find(f, "osd") == 0]:

        CHECK_PGS = get_osd_pgs(os.path.join(OSDDIR, osd), None)
        CHECK_PGS = sorted(CHECK_PGS)

        cmd = (CFSD_PREFIX + "--op list-pgs").format(osd=osd)
        logging.debug(cmd)
        TEST_PGS = check_output(cmd, shell=True).split("\n")
        TEST_PGS = sorted(TEST_PGS)[1:] # Skip extra blank line

        if TEST_PGS != CHECK_PGS:
            logging.error("list-pgs got wrong result for osd.{osd}".format(osd=osd))
            logging.error("Expected {pgs}".format(pgs=CHECK_PGS))
            logging.error("Got {pgs}".format(pgs=TEST_PGS))
            ERRORS += 1

    print "Test pg export"
    EXP_ERRORS = 0
    os.mkdir(TESTDIR)
    for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and string.find(f, "osd") == 0]:
        os.mkdir(os.path.join(TESTDIR, osd))
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            mydir = os.path.join(TESTDIR, osd)
            fname = os.path.join(mydir, pg)
            if pg == ALLREPPGS[0]:
                cmd = (CFSD_PREFIX + "--op export --pgid {pg} > {file}").format(osd=osd, pg=pg, file=fname)
            elif pg == ALLREPPGS[1]:
                cmd = (CFSD_PREFIX + "--op export --pgid {pg} --file - > {file}").format(osd=osd, pg=pg, file=fname)
            else:
              cmd = (CFSD_PREFIX + "--op export --pgid {pg} --file {file}").format(osd=osd, pg=pg, file=fname)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
            if ret != 0:
                logging.error("Exporting failed for pg {pg} on {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
                EXP_ERRORS += 1

    ERRORS += EXP_ERRORS

    print "Test pg removal"
    RM_ERRORS = 0
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            cmd = (CFSD_PREFIX + "--op remove --pgid {pg}").format(pg=pg, osd=osd)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=nullfd)
            if ret != 0:
                logging.error("Removing failed for pg {pg} on {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
                RM_ERRORS += 1

    ERRORS += RM_ERRORS

    IMP_ERRORS = 0
    if EXP_ERRORS == 0 and RM_ERRORS == 0:
        print "Test pg import"
        for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and string.find(f, "osd") == 0]:
            dir = os.path.join(TESTDIR, osd)
            PGS = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]
            for pg in PGS:
                file = os.path.join(dir, pg)
                if pg == PGS[0]:
                    cmd = ("cat {file} |".format(file=file) + CFSD_PREFIX + "--op import").format(osd=osd)
                elif pg == PGS[1]:
                    cmd = (CFSD_PREFIX + "--op import --file - < {file}").format(osd=osd, file=file)
                else:
                    cmd = (CFSD_PREFIX + "--op import --file {file}").format(osd=osd, file=file)
                logging.debug(cmd)
                ret = call(cmd, shell=True, stdout=nullfd)
                if ret != 0:
                    logging.error("Import failed from {file} with {ret}".format(file=file, ret=ret))
                    IMP_ERRORS += 1
    else:
        logging.warning("SKIPPING IMPORT TESTS DUE TO PREVIOUS FAILURES")

    ERRORS += IMP_ERRORS
    logging.debug(cmd)

    if EXP_ERRORS == 0 and RM_ERRORS == 0 and IMP_ERRORS == 0:
        print "Verify replicated import data"
        for nsfile in [f for f in os.listdir(DATADIR) if f.split('-')[1].find(REP_NAME) == 0]:
            nspace = nsfile.split("-")[0]
            file = nsfile.split("-")[1]
            path = os.path.join(DATADIR, nsfile)
            tmpfd = open(TMPFILE, "w")
            cmd = "find {dir} -name '{file}_*_{nspace}_*'".format(dir=OSDDIR, file=file, nspace=nspace)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=tmpfd)
            if ret:
                logging.critical("INTERNAL ERROR")
                return 1
            tmpfd.close()
            obj_locs = get_lines(TMPFILE)
            if len(obj_locs) == 0:
                logging.error("Can't find imported object {name}".format(name=file))
                ERRORS += 1
            for obj_loc in obj_locs:
                cmd = "diff -q {src} {obj_loc}".format(src=path, obj_loc=obj_loc)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.error("{file} data not imported properly into {obj}".format(file=file, obj=obj_loc))
                    ERRORS += 1
    else:
        logging.warning("SKIPPING CHECKING IMPORT DATA DUE TO PREVIOUS FAILURES")

    vstart(new=False)
    wait_for_health()

    if EXP_ERRORS == 0 and RM_ERRORS == 0 and IMP_ERRORS == 0:
        print "Verify erasure coded import data"
        ERRORS += verify(DATADIR, EC_POOL, EC_NAME)

    if EXP_ERRORS == 0:
        NEWPOOL = "import-rados-pool"
        cmd = "./rados mkpool {pool}".format(pool=NEWPOOL)
        logging.debug(cmd)
        ret = call(cmd, shell=True, stdout=nullfd)

        print "Test import-rados"
        for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and string.find(f, "osd") == 0]:
            dir = os.path.join(TESTDIR, osd)
            for pg in [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]:
                if string.find(pg, "{id}.".format(id=REPID)) != 0:
                    continue
                file = os.path.join(dir, pg)
                cmd = "./ceph-objectstore-tool import-rados {pool} {file}".format(pool=NEWPOOL, file=file)
                logging.debug(cmd)
                ret = call(cmd, shell=True, stdout=nullfd)
                if ret != 0:
                    logging.error("Import-rados failed from {file} with {ret}".format(file=file, ret=ret))
                    ERRORS += 1

        ERRORS += verify(DATADIR, NEWPOOL, REP_NAME)
    else:
        logging.warning("SKIPPING IMPORT-RADOS TESTS DUE TO PREVIOUS FAILURES")

    call("/bin/rm -rf {dir}".format(dir=TESTDIR), shell=True)
    call("/bin/rm -rf {dir}".format(dir=DATADIR), shell=True)

    if ERRORS == 0:
        print "TEST PASSED"
        return 0
    else:
        print "TEST FAILED WITH {errcount} ERRORS".format(errcount=ERRORS)
        return 1

if __name__ == "__main__":
    status = 1
    try:
        status = main(sys.argv[1:])
    finally:
        kill_daemons()
        call("/bin/rm -fr ceph_objectstore_tool_dir", shell=True)
    sys.exit(status)
