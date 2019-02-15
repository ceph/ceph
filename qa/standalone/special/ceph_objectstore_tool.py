#!/usr/bin/env python

from __future__ import print_function
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

import filecmp
import os
import subprocess
import math
import time
import sys
import re
import logging
import json
import tempfile
import platform

try:
    from subprocess import DEVNULL
except ImportError:
    DEVNULL = open(os.devnull, "wb")

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.WARNING)


if sys.version_info[0] >= 3:
    def decode(s):
        return s.decode('utf-8')

    def check_output(*args, **kwargs):
        return decode(subprocess.check_output(*args, **kwargs))
else:
    def decode(s):
        return s



def wait_for_health():
    print("Wait for health_ok...", end="")
    tries = 0
    while call("{path}/ceph health 2> /dev/null | grep -v 'HEALTH_OK\|HEALTH_WARN' > /dev/null".format(path=CEPH_BIN), shell=True) == 0:
        tries += 1
        if tries == 150:
            raise Exception("Time exceeded to go to health")
        time.sleep(1)
    print("DONE")


def get_pool_id(name, nullfd):
    cmd = "{path}/ceph osd pool stats {pool}".format(pool=name, path=CEPH_BIN).split()
    # pool {pool} id # .... grab the 4 field
    return check_output(cmd, stderr=nullfd).split()[3]


# return a list of unique PGS given an osd subdirectory
def get_osd_pgs(SUBDIR, ID):
    PGS = []
    if ID:
        endhead = re.compile("{id}.*_head$".format(id=ID))
    DIR = os.path.join(SUBDIR, "current")
    PGS += [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and (ID is None or endhead.match(f))]
    PGS = [re.sub("_head", "", p) for p in PGS if "_head" in p]
    return PGS


# return a sorted list of unique PGs given a directory
def get_pgs(DIR, ID):
    OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and f.find("osd") == 0]
    PGS = []
    for d in OSDS:
        SUBDIR = os.path.join(DIR, d)
        PGS += get_osd_pgs(SUBDIR, ID)
    return sorted(set(PGS))


# return a sorted list of PGS a subset of ALLPGS that contain objects with prefix specified
def get_objs(ALLPGS, prefix, DIR, ID):
    OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and f.find("osd") == 0]
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
            if any(f for f in [val for _, _, fl in os.walk(FINALDIR) for val in fl] if f.startswith(prefix)):
                PGS += [p]
    return sorted(set(PGS))


# return a sorted list of OSDS which have data from a given PG
def get_osds(PG, DIR):
    ALLOSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and f.find("osd") == 0]
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
    print("File: " + filename)
    with open(filename, "r") as f:
        while True:
            line = f.readline().rstrip('\n')
            if not line:
                break
            print(line)
    print("<EOF>")


def vstart(new, opt=""):
    print("vstarting....", end="")
    NEW = new and "-n" or "-N"
    call("MON=1 OSD=4 MDS=0 MGR=1 CEPH_PORT=7400 MGR_PYTHON_PATH={path}/src/pybind/mgr {path}/src/vstart.sh --filestore --short -l {new} -d {opt} > /dev/null 2>&1".format(new=NEW, opt=opt, path=CEPH_ROOT), shell=True)
    print("DONE")


def test_failure(cmd, errmsg, tty=False):
    if tty:
        try:
            ttyfd = open("/dev/tty", "rwb")
        except Exception as e:
            logging.info(str(e))
            logging.info("SKIP " + cmd)
            return 0
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=os.getpid())
    tmpfd = open(TMPFILE, "wb")

    logging.debug(cmd)
    if tty:
        ret = call(cmd, shell=True, stdin=ttyfd, stdout=ttyfd, stderr=tmpfd)
        ttyfd.close()
    else:
        ret = call(cmd, shell=True, stderr=tmpfd)
    tmpfd.close()
    if ret == 0:
        logging.error(cmd)
        logging.error("Should have failed, but got exit 0")
        return 1
    lines = get_lines(TMPFILE)
    matched = [ l for l in lines if errmsg in l ]
    if any(matched):
        logging.info("Correctly failed with message \"" + matched[0] + "\"")
        return 0
    else:
        logging.error("Command: " + cmd )
        logging.error("Bad messages to stderr \"" + str(lines) + "\"")
        logging.error("Expected \"" + errmsg + "\"")
        return 1


def get_nspace(num):
    if num == 0:
        return ""
    return "ns{num}".format(num=num)


def verify(DATADIR, POOL, NAME_PREFIX, db):
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=os.getpid())
    ERRORS = 0
    for rawnsfile in [f for f in os.listdir(DATADIR) if f.split('-')[1].find(NAME_PREFIX) == 0]:
        nsfile = rawnsfile.split("__")[0]
        clone = rawnsfile.split("__")[1]
        nspace = nsfile.split("-")[0]
        file = nsfile.split("-")[1]
        # Skip clones
        if clone != "head":
            continue
        path = os.path.join(DATADIR, rawnsfile)
        try:
            os.unlink(TMPFILE)
        except:
            pass
        cmd = "{path}/rados -p {pool} -N '{nspace}' get {file} {out}".format(pool=POOL, file=file, out=TMPFILE, nspace=nspace, path=CEPH_BIN)
        logging.debug(cmd)
        call(cmd, shell=True, stdout=DEVNULL, stderr=DEVNULL)
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
        for key, val in db[nspace][file]["xattr"].items():
            cmd = "{path}/rados -p {pool} -N '{nspace}' getxattr {name} {key}".format(pool=POOL, name=file, key=key, nspace=nspace, path=CEPH_BIN)
            logging.debug(cmd)
            getval = check_output(cmd, shell=True, stderr=DEVNULL)
            logging.debug("getxattr {key} {val}".format(key=key, val=getval))
            if getval != val:
                logging.error("getxattr of key {key} returned wrong val: {get} instead of {orig}".format(key=key, get=getval, orig=val))
                ERRORS += 1
                continue
        hdr = db[nspace][file].get("omapheader", "")
        cmd = "{path}/rados -p {pool} -N '{nspace}' getomapheader {name} {file}".format(pool=POOL, name=file, nspace=nspace, file=TMPFILE, path=CEPH_BIN)
        logging.debug(cmd)
        ret = call(cmd, shell=True, stderr=DEVNULL)
        if ret != 0:
            logging.error("rados getomapheader returned {ret}".format(ret=ret))
            ERRORS += 1
        else:
            getlines = get_lines(TMPFILE)
            assert(len(getlines) == 0 or len(getlines) == 1)
            if len(getlines) == 0:
                gethdr = ""
            else:
                gethdr = getlines[0]
            logging.debug("header: {hdr}".format(hdr=gethdr))
            if gethdr != hdr:
                logging.error("getomapheader returned wrong val: {get} instead of {orig}".format(get=gethdr, orig=hdr))
                ERRORS += 1
        for key, val in db[nspace][file]["omap"].items():
            cmd = "{path}/rados -p {pool} -N '{nspace}' getomapval {name} {key} {file}".format(pool=POOL, name=file, key=key, nspace=nspace, file=TMPFILE, path=CEPH_BIN)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stderr=DEVNULL)
            if ret != 0:
                logging.error("getomapval returned {ret}".format(ret=ret))
                ERRORS += 1
                continue
            getlines = get_lines(TMPFILE)
            if len(getlines) != 1:
                logging.error("Bad data from getomapval {lines}".format(lines=getlines))
                ERRORS += 1
                continue
            getval = getlines[0]
            logging.debug("getomapval {key} {val}".format(key=key, val=getval))
            if getval != val:
                logging.error("getomapval returned wrong val: {get} instead of {orig}".format(get=getval, orig=val))
                ERRORS += 1
        try:
            os.unlink(TMPFILE)
        except:
            pass
    return ERRORS


def check_journal(jsondict):
    errors = 0
    if 'header' not in jsondict:
        logging.error("Key 'header' not in dump-journal")
        errors += 1
    elif 'max_size' not in jsondict['header']:
        logging.error("Key 'max_size' not in dump-journal header")
        errors += 1
    else:
        print("\tJournal max_size = {size}".format(size=jsondict['header']['max_size']))
    if 'entries' not in jsondict:
        logging.error("Key 'entries' not in dump-journal output")
        errors += 1
    elif len(jsondict['entries']) == 0:
        logging.info("No entries in journal found")
    else:
        errors += check_journal_entries(jsondict['entries'])
    return errors


def check_journal_entries(entries):
    errors = 0
    for enum in range(len(entries)):
        if 'offset' not in entries[enum]:
            logging.error("No 'offset' key in entry {e}".format(e=enum))
            errors += 1
        if 'seq' not in entries[enum]:
            logging.error("No 'seq' key in entry {e}".format(e=enum))
            errors += 1
        if 'transactions' not in entries[enum]:
            logging.error("No 'transactions' key in entry {e}".format(e=enum))
            errors += 1
        elif len(entries[enum]['transactions']) == 0:
            logging.error("No transactions found in entry {e}".format(e=enum))
            errors += 1
        else:
            errors += check_entry_transactions(entries[enum], enum)
    return errors


def check_entry_transactions(entry, enum):
    errors = 0
    for tnum in range(len(entry['transactions'])):
        if 'trans_num' not in entry['transactions'][tnum]:
            logging.error("Key 'trans_num' missing from entry {e} trans {t}".format(e=enum, t=tnum))
            errors += 1
        elif entry['transactions'][tnum]['trans_num'] != tnum:
            ft = entry['transactions'][tnum]['trans_num']
            logging.error("Bad trans_num ({ft}) entry {e} trans {t}".format(ft=ft, e=enum, t=tnum))
            errors += 1
        if 'ops' not in entry['transactions'][tnum]:
            logging.error("Key 'ops' missing from entry {e} trans {t}".format(e=enum, t=tnum))
            errors += 1
        else:
            errors += check_transaction_ops(entry['transactions'][tnum]['ops'], enum, tnum)
    return errors


def check_transaction_ops(ops, enum, tnum):
    if len(ops) is 0:
        logging.warning("No ops found in entry {e} trans {t}".format(e=enum, t=tnum))
    errors = 0
    for onum in range(len(ops)):
        if 'op_num' not in ops[onum]:
            logging.error("Key 'op_num' missing from entry {e} trans {t} op {o}".format(e=enum, t=tnum, o=onum))
            errors += 1
        elif ops[onum]['op_num'] != onum:
            fo = ops[onum]['op_num']
            logging.error("Bad op_num ({fo}) from entry {e} trans {t} op {o}".format(fo=fo, e=enum, t=tnum, o=onum))
            errors += 1
        if 'op_name' not in ops[onum]:
            logging.error("Key 'op_name' missing from entry {e} trans {t} op {o}".format(e=enum, t=tnum, o=onum))
            errors += 1
    return errors


def test_dump_journal(CFSD_PREFIX, osds):
    ERRORS = 0
    pid = os.getpid()
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=pid)

    for osd in osds:
        # Test --op dump-journal by loading json
        cmd = (CFSD_PREFIX + "--op dump-journal --format json").format(osd=osd)
        logging.debug(cmd)
        tmpfd = open(TMPFILE, "wb")
        ret = call(cmd, shell=True, stdout=tmpfd)
        if ret != 0:
            logging.error("Bad exit status {ret} from {cmd}".format(ret=ret, cmd=cmd))
            ERRORS += 1
            continue
        tmpfd.close()
        tmpfd = open(TMPFILE, "r")
        jsondict = json.load(tmpfd)
        tmpfd.close()
        os.unlink(TMPFILE)

        journal_errors = check_journal(jsondict)
        if journal_errors is not 0:
            logging.error(jsondict)
        ERRORS += journal_errors

    return ERRORS

CEPH_BUILD_DIR = os.environ.get('CEPH_BUILD_DIR')
CEPH_BIN = os.environ.get('CEPH_BIN')
CEPH_ROOT = os.environ.get('CEPH_ROOT')

if not CEPH_BUILD_DIR:
    CEPH_BUILD_DIR=os.getcwd()
    os.putenv('CEPH_BUILD_DIR', CEPH_BUILD_DIR)
    CEPH_BIN=os.path.join(CEPH_BUILD_DIR, 'bin')
    os.putenv('CEPH_BIN', CEPH_BIN)
    CEPH_ROOT=os.path.dirname(CEPH_BUILD_DIR)
    os.putenv('CEPH_ROOT', CEPH_ROOT)
    CEPH_LIB=os.path.join(CEPH_BUILD_DIR, 'lib')
    os.putenv('CEPH_LIB', CEPH_LIB)

try:
    os.mkdir("td")
except:
    pass # ok if this is already there
CEPH_DIR = os.path.join(CEPH_BUILD_DIR, os.path.join("td", "cot_dir"))
CEPH_CONF = os.path.join(CEPH_DIR, 'ceph.conf')

def kill_daemons():
    call("{path}/init-ceph -c {conf} stop > /dev/null 2>&1".format(conf=CEPH_CONF, path=CEPH_BIN), shell=True)


def check_data(DATADIR, TMPFILE, OSDDIR, SPLIT_NAME):
    repcount = 0
    ERRORS = 0
    for rawnsfile in [f for f in os.listdir(DATADIR) if f.split('-')[1].find(SPLIT_NAME) == 0]:
        nsfile = rawnsfile.split("__")[0]
        clone = rawnsfile.split("__")[1]
        nspace = nsfile.split("-")[0]
        file = nsfile.split("-")[1] + "__" + clone
        # Skip clones
        if clone != "head":
            continue
        path = os.path.join(DATADIR, rawnsfile)
        tmpfd = open(TMPFILE, "wb")
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
            # For btrfs skip snap_* dirs
            if re.search("/snap_[0-9]*/", obj_loc) is not None:
                continue
            repcount += 1
            cmd = "diff -q {src} {obj_loc}".format(src=path, obj_loc=obj_loc)
            logging.debug(cmd)
            ret = call(cmd, shell=True)
            if ret != 0:
                logging.error("{file} data not imported properly into {obj}".format(file=file, obj=obj_loc))
                ERRORS += 1
    return ERRORS, repcount


def set_osd_weight(CFSD_PREFIX, osd_ids, osd_path, weight):
    # change the weight of osd.0 to math.pi in the newest osdmap of given osd
    osdmap_file = tempfile.NamedTemporaryFile(delete=True)
    cmd = (CFSD_PREFIX + "--op get-osdmap --file {osdmap_file}").format(osd=osd_path,
                                                                        osdmap_file=osdmap_file.name)
    output = check_output(cmd, shell=True)
    epoch = int(re.findall('#(\d+)', output)[0])

    new_crush_file = tempfile.NamedTemporaryFile(delete=True)
    old_crush_file = tempfile.NamedTemporaryFile(delete=True)
    ret = call("{path}/osdmaptool --export-crush {crush_file} {osdmap_file}".format(osdmap_file=osdmap_file.name,
                                                                          crush_file=old_crush_file.name, path=CEPH_BIN),
               stdout=DEVNULL,
               stderr=DEVNULL,
               shell=True)
    assert(ret == 0)

    for osd_id in osd_ids:
        cmd = "{path}/crushtool -i {crush_file} --reweight-item osd.{osd} {weight} -o {new_crush_file}".format(osd=osd_id,
                                                                                                          crush_file=old_crush_file.name,
                                                                                                          weight=weight,
                                                                                                          new_crush_file=new_crush_file.name, path=CEPH_BIN)
        ret = call(cmd, stdout=DEVNULL, shell=True)
        assert(ret == 0)
        old_crush_file, new_crush_file = new_crush_file, old_crush_file

    # change them back, since we don't need to preapre for another round
    old_crush_file, new_crush_file = new_crush_file, old_crush_file
    old_crush_file.close()

    ret = call("{path}/osdmaptool --import-crush {crush_file} {osdmap_file}".format(osdmap_file=osdmap_file.name,
                                                                               crush_file=new_crush_file.name, path=CEPH_BIN),
               stdout=DEVNULL,
               stderr=DEVNULL,
               shell=True)
    assert(ret == 0)

    # Minimum test of --dry-run by using it, but not checking anything
    cmd = CFSD_PREFIX + "--op set-osdmap --file {osdmap_file} --epoch {epoch} --force --dry-run"
    cmd = cmd.format(osd=osd_path, osdmap_file=osdmap_file.name, epoch=epoch)
    ret = call(cmd, stdout=DEVNULL, shell=True)
    assert(ret == 0)

    # osdmaptool increases the epoch of the changed osdmap, so we need to force the tool
    # to use use a different epoch than the one in osdmap
    cmd = CFSD_PREFIX + "--op set-osdmap --file {osdmap_file} --epoch {epoch} --force"
    cmd = cmd.format(osd=osd_path, osdmap_file=osdmap_file.name, epoch=epoch)
    ret = call(cmd, stdout=DEVNULL, shell=True)

    return ret == 0

def get_osd_weights(CFSD_PREFIX, osd_ids, osd_path):
    osdmap_file = tempfile.NamedTemporaryFile(delete=True)
    cmd = (CFSD_PREFIX + "--op get-osdmap --file {osdmap_file}").format(osd=osd_path,
                                                                        osdmap_file=osdmap_file.name)
    ret = call(cmd, stdout=DEVNULL, shell=True)
    if ret != 0:
        return None
    # we have to read the weights from the crush map, even we can query the weights using
    # osdmaptool, but please keep in mind, they are different:
    #    item weights in crush map versus weight associated with each osd in osdmap
    crush_file = tempfile.NamedTemporaryFile(delete=True)
    ret = call("{path}/osdmaptool --export-crush {crush_file} {osdmap_file}".format(osdmap_file=osdmap_file.name,
                                                                               crush_file=crush_file.name, path=CEPH_BIN),
               stdout=DEVNULL,
               shell=True)
    assert(ret == 0)
    output = check_output("{path}/crushtool --tree -i {crush_file} | tail -n {num_osd}".format(crush_file=crush_file.name,
                                                                                          num_osd=len(osd_ids), path=CEPH_BIN),
                          stderr=DEVNULL,
                          shell=True)
    weights = []
    for line in output.strip().split('\n'):
        print(line)
        linev = re.split('\s+', line)
        if linev[0] is '':
            linev.pop(0)
        print('linev %s' % linev)
        weights.append(float(linev[2]))

    return weights


def test_get_set_osdmap(CFSD_PREFIX, osd_ids, osd_paths):
    print("Testing get-osdmap and set-osdmap")
    errors = 0
    kill_daemons()
    weight = 1 / math.e           # just some magic number in [0, 1]
    changed = []
    for osd_path in osd_paths:
        if set_osd_weight(CFSD_PREFIX, osd_ids, osd_path, weight):
            changed.append(osd_path)
        else:
            logging.warning("Failed to change the weights: {0}".format(osd_path))
    # i am pissed off if none of the store gets changed
    if not changed:
        errors += 1

    for osd_path in changed:
        weights = get_osd_weights(CFSD_PREFIX, osd_ids, osd_path)
        if not weights:
            errors += 1
            continue
        if any(abs(w - weight) > 1e-5 for w in weights):
            logging.warning("Weight is not changed: {0} != {1}".format(weights, weight))
            errors += 1
    return errors

def test_get_set_inc_osdmap(CFSD_PREFIX, osd_path):
    # incrementals are not used unless we need to build an MOSDMap to update
    # OSD's peers, so an obvious way to test it is simply overwrite an epoch
    # with a different copy, and read it back to see if it matches.
    kill_daemons()
    file_e2 = tempfile.NamedTemporaryFile(delete=True)
    cmd = (CFSD_PREFIX + "--op get-inc-osdmap --file {file}").format(osd=osd_path,
                                                                     file=file_e2.name)
    output = check_output(cmd, shell=True)
    epoch = int(re.findall('#(\d+)', output)[0])
    # backup e1 incremental before overwriting it
    epoch -= 1
    file_e1_backup = tempfile.NamedTemporaryFile(delete=True)
    cmd = CFSD_PREFIX + "--op get-inc-osdmap --epoch {epoch} --file {file}"
    ret = call(cmd.format(osd=osd_path, epoch=epoch, file=file_e1_backup.name), shell=True)
    if ret: return 1
    # overwrite e1 with e2
    cmd = CFSD_PREFIX + "--op set-inc-osdmap --force --epoch {epoch} --file {file}"
    ret = call(cmd.format(osd=osd_path, epoch=epoch, file=file_e2.name), shell=True)
    if ret: return 1
    # Use dry-run to set back to e1 which shouldn't happen
    cmd = CFSD_PREFIX + "--op set-inc-osdmap --dry-run --epoch {epoch} --file {file}"
    ret = call(cmd.format(osd=osd_path, epoch=epoch, file=file_e1_backup.name), shell=True)
    if ret: return 1
    # read from e1
    file_e1_read = tempfile.NamedTemporaryFile(delete=True)
    cmd = CFSD_PREFIX + "--op get-inc-osdmap --epoch {epoch} --file {file}"
    ret = call(cmd.format(osd=osd_path, epoch=epoch, file=file_e1_read.name), shell=True)
    if ret: return 1
    errors = 0
    try:
        if not filecmp.cmp(file_e2.name, file_e1_read.name, shallow=False):
            logging.error("{{get,set}}-inc-osdmap mismatch {0} != {1}".format(file_e2.name, file_e1_read.name))
            errors += 1
    finally:
        # revert the change with file_e1_backup
        cmd = CFSD_PREFIX + "--op set-inc-osdmap --epoch {epoch} --file {file}"
        ret = call(cmd.format(osd=osd_path, epoch=epoch, file=file_e1_backup.name), shell=True)
        if ret:
            logging.error("Failed to revert the changed inc-osdmap")
            errors += 1

    return errors


def test_removeall(CFSD_PREFIX, db, OBJREPPGS, REP_POOL, CEPH_BIN, OSDDIR, REP_NAME, NUM_CLONED_REP_OBJECTS):
    # Test removeall
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=os.getpid())
    nullfd = open(os.devnull, "w")
    errors=0
    print("Test removeall")
    kill_daemons()
    for nspace in db.keys():
        for basename in db[nspace].keys():
            JSON = db[nspace][basename]['json']
            for pg in OBJREPPGS:
                OSDS = get_osds(pg, OSDDIR)
                for osd in OSDS:
                    DIR = os.path.join(OSDDIR, os.path.join(osd, os.path.join("current", "{pg}_head".format(pg=pg))))
                    fnames = [f for f in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, f))
                              and f.split("_")[0] == basename and f.split("_")[4] == nspace]
                    if not fnames:
                        continue

                    if int(basename.split(REP_NAME)[1]) <= int(NUM_CLONED_REP_OBJECTS):
                        cmd = (CFSD_PREFIX + "'{json}' remove").format(osd=osd, json=JSON)
                        errors += test_failure(cmd, "Snapshots are present, use removeall to delete everything")

                    cmd = (CFSD_PREFIX + " --force --dry-run '{json}' remove").format(osd=osd, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
                    if ret != 0:
                        logging.error("remove with --force failed for {json}".format(json=JSON))
                        errors += 1

                    cmd = (CFSD_PREFIX + " --dry-run '{json}' removeall").format(osd=osd, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
                    if ret != 0:
                        logging.error("removeall failed for {json}".format(json=JSON))
                        errors += 1

                    cmd = (CFSD_PREFIX + " '{json}' removeall").format(osd=osd, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
                    if ret != 0:
                        logging.error("removeall failed for {json}".format(json=JSON))
                        errors += 1

                    tmpfd = open(TMPFILE, "w")
                    cmd = (CFSD_PREFIX + "--op list --pgid {pg} --namespace {ns} {name}").format(osd=osd, pg=pg, ns=nspace, name=basename)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdout=tmpfd)
                    if ret != 0:
                        logging.error("Bad exit status {ret} from {cmd}".format(ret=ret, cmd=cmd))
                        errors += 1
                    tmpfd.close()
                    lines = get_lines(TMPFILE)
                    if len(lines) != 0:
                        logging.error("Removeall didn't remove all objects {ns}/{name} : {lines}".format(ns=nspace, name=basename, lines=lines))
                        errors += 1
    vstart(new=False)
    wait_for_health()
    cmd = "{path}/rados -p {pool} rmsnap snap1".format(pool=REP_POOL, path=CEPH_BIN)
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    if ret != 0:
        logging.error("rados rmsnap failed")
        errors += 1
    time.sleep(2)
    wait_for_health()
    return errors


def main(argv):
    if sys.version_info[0] < 3:
        sys.stdout = stdout = os.fdopen(sys.stdout.fileno(), 'wb', 0)
    else:
        stdout = sys.stdout.buffer
    if len(argv) > 1 and argv[1] == "debug":
        nullfd = stdout
    else:
        nullfd = DEVNULL

    call("rm -fr {dir}; mkdir -p {dir}".format(dir=CEPH_DIR), shell=True)
    os.chdir(CEPH_DIR)
    os.environ["CEPH_DIR"] = CEPH_DIR
    OSDDIR = "dev"
    REP_POOL = "rep_pool"
    REP_NAME = "REPobject"
    EC_POOL = "ec_pool"
    EC_NAME = "ECobject"
    if len(argv) > 0 and argv[0] == 'large':
        PG_COUNT = 12
        NUM_REP_OBJECTS = 200
        NUM_CLONED_REP_OBJECTS = 50
        NUM_EC_OBJECTS = 12
        NUM_NSPACES = 4
        # Larger data sets for first object per namespace
        DATALINECOUNT = 50000
        # Number of objects to do xattr/omap testing on
        ATTR_OBJS = 10
    else:
        PG_COUNT = 4
        NUM_REP_OBJECTS = 2
        NUM_CLONED_REP_OBJECTS = 2
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
    CFSD_PREFIX = CEPH_BIN + "/ceph-objectstore-tool --no-mon-config --data-path " + OSDDIR + "/{osd} "
    PROFNAME = "testecprofile"

    os.environ['CEPH_CONF'] = CEPH_CONF
    vstart(new=True)
    wait_for_health()

    cmd = "{path}/ceph osd pool create {pool} {pg} {pg} replicated".format(pool=REP_POOL, pg=PG_COUNT, path=CEPH_BIN)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    time.sleep(2)
    REPID = get_pool_id(REP_POOL, nullfd)

    print("Created Replicated pool #{repid}".format(repid=REPID))

    cmd = "{path}/ceph osd erasure-code-profile set {prof} crush-failure-domain=osd".format(prof=PROFNAME, path=CEPH_BIN)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    cmd = "{path}/ceph osd erasure-code-profile get {prof}".format(prof=PROFNAME, path=CEPH_BIN)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    cmd = "{path}/ceph osd pool create {pool} {pg} {pg} erasure {prof}".format(pool=EC_POOL, prof=PROFNAME, pg=PG_COUNT, path=CEPH_BIN)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    ECID = get_pool_id(EC_POOL, nullfd)

    print("Created Erasure coded pool #{ecid}".format(ecid=ECID))

    print("Creating {objs} objects in replicated pool".format(objs=(NUM_REP_OBJECTS*NUM_NSPACES)))
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
            DDNAME += "__head"

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

            cmd = "{path}/rados -p {pool} -N '{nspace}' put {name} {ddname}".format(pool=REP_POOL, name=NAME, ddname=DDNAME, nspace=nspace, path=CEPH_BIN)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stderr=nullfd)
            if ret != 0:
                logging.critical("Rados put command failed with {ret}".format(ret=ret))
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
                cmd = "{path}/rados -p {pool} -N '{nspace}' setxattr {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval, nspace=nspace, path=CEPH_BIN)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.error("setxattr failed with {ret}".format(ret=ret))
                    ERRORS += 1
                db[nspace][NAME]["xattr"][mykey] = myval

            # Create omap header in all objects but REPobject1
            if i < ATTR_OBJS + 1 and i != 1:
                myhdr = "hdr{i}".format(i=i)
                cmd = "{path}/rados -p {pool} -N '{nspace}' setomapheader {name} {hdr}".format(pool=REP_POOL, name=NAME, hdr=myhdr, nspace=nspace, path=CEPH_BIN)
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
                cmd = "{path}/rados -p {pool} -N '{nspace}' setomapval {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval, nspace=nspace, path=CEPH_BIN)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.critical("setomapval failed with {ret}".format(ret=ret))
                db[nspace][NAME]["omap"][mykey] = myval

    # Create some clones
    cmd = "{path}/rados -p {pool} mksnap snap1".format(pool=REP_POOL, path=CEPH_BIN)
    logging.debug(cmd)
    call(cmd, shell=True)

    objects = range(1, NUM_CLONED_REP_OBJECTS + 1)
    nspaces = range(NUM_NSPACES)
    for n in nspaces:
        nspace = get_nspace(n)

        for i in objects:
            NAME = REP_NAME + "{num}".format(num=i)
            LNAME = nspace + "-" + NAME
            DDNAME = os.path.join(DATADIR, LNAME)
            # First clone
            CLONENAME = DDNAME + "__1"
            DDNAME += "__head"

            cmd = "mv -f " + DDNAME + " " + CLONENAME
            logging.debug(cmd)
            call(cmd, shell=True)

            if i == 1:
                dataline = range(DATALINECOUNT)
            else:
                dataline = range(1)
            fd = open(DDNAME, "w")
            data = "This is the replicated data after a snapshot for " + LNAME + "\n"
            for _ in dataline:
                fd.write(data)
            fd.close()

            cmd = "{path}/rados -p {pool} -N '{nspace}' put {name} {ddname}".format(pool=REP_POOL, name=NAME, ddname=DDNAME, nspace=nspace, path=CEPH_BIN)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stderr=nullfd)
            if ret != 0:
                logging.critical("Rados put command failed with {ret}".format(ret=ret))
                return 1

    print("Creating {objs} objects in erasure coded pool".format(objs=(NUM_EC_OBJECTS*NUM_NSPACES)))

    objects = range(1, NUM_EC_OBJECTS + 1)
    nspaces = range(NUM_NSPACES)
    for n in nspaces:
        nspace = get_nspace(n)

        for i in objects:
            NAME = EC_NAME + "{num}".format(num=i)
            LNAME = nspace + "-" + NAME
            DDNAME = os.path.join(DATADIR, LNAME)
            DDNAME += "__head"

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

            cmd = "{path}/rados -p {pool} -N '{nspace}' put {name} {ddname}".format(pool=EC_POOL, name=NAME, ddname=DDNAME, nspace=nspace, path=CEPH_BIN)
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
                cmd = "{path}/rados -p {pool} -N '{nspace}' setxattr {name} {key} {val}".format(pool=EC_POOL, name=NAME, key=mykey, val=myval, nspace=nspace, path=CEPH_BIN)
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

    print("Test invalid parameters")
    # On export can't use stdout to a terminal
    cmd = (CFSD_PREFIX + "--op export --pgid {pg}").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "stdout is a tty and no --file filename specified", tty=True)

    # On export can't use stdout to a terminal
    cmd = (CFSD_PREFIX + "--op export --pgid {pg} --file -").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "stdout is a tty and no --file filename specified", tty=True)

    # Prep a valid ec export file for import failure tests
    ONEECPG = ALLECPGS[0]
    osds = get_osds(ONEECPG, OSDDIR)
    ONEECOSD = osds[0]
    OTHERFILE = "/tmp/foo.{pid}".format(pid=pid)
    cmd = (CFSD_PREFIX + "--op export --pgid {pg} --file {file}").format(osd=ONEECOSD, pg=ONEECPG, file=OTHERFILE)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)

    os.unlink(OTHERFILE)

    # Prep a valid export file for import failure tests
    OTHERFILE = "/tmp/foo.{pid}".format(pid=pid)
    cmd = (CFSD_PREFIX + "--op export --pgid {pg} --file {file}").format(osd=ONEOSD, pg=ONEPG, file=OTHERFILE)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)

    # On import can't specify a different pgid than the file
    TMPPG="{pool}.80".format(pool=REPID)
    cmd = (CFSD_PREFIX + "--op import --pgid 12.dd --file {file}").format(osd=ONEOSD, pg=TMPPG, file=OTHERFILE)
    ERRORS += test_failure(cmd, "specified pgid 12.dd does not match actual pgid")

    os.unlink(OTHERFILE)
    cmd = (CFSD_PREFIX + "--op import --file {FOO}").format(osd=ONEOSD, FOO=OTHERFILE)
    ERRORS += test_failure(cmd, "file: {FOO}: No such file or directory".format(FOO=OTHERFILE))

    cmd = "{path}/ceph-objectstore-tool --no-mon-config --data-path BAD_DATA_PATH --op list".format(osd=ONEOSD, path=CEPH_BIN)
    ERRORS += test_failure(cmd, "data-path: BAD_DATA_PATH: No such file or directory")

    cmd = (CFSD_PREFIX + "--journal-path BAD_JOURNAL_PATH --op list").format(osd=ONEOSD)
    ERRORS += test_failure(cmd, "journal-path: BAD_JOURNAL_PATH: No such file or directory")

    cmd = (CFSD_PREFIX + "--journal-path /bin --op list").format(osd=ONEOSD)
    ERRORS += test_failure(cmd, "journal-path: /bin: (21) Is a directory")

    # On import can't use stdin from a terminal
    cmd = (CFSD_PREFIX + "--op import --pgid {pg}").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "stdin is a tty and no --file filename specified", tty=True)

    # On import can't use stdin from a terminal
    cmd = (CFSD_PREFIX + "--op import --pgid {pg} --file -").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "stdin is a tty and no --file filename specified", tty=True)

    # Specify a bad --type
    os.mkdir(OSDDIR + "/fakeosd")
    cmd = ("{path}/ceph-objectstore-tool --no-mon-config --data-path " + OSDDIR + "/{osd} --type foobar --op list --pgid {pg}").format(osd="fakeosd", pg=ONEPG, path=CEPH_BIN)
    ERRORS += test_failure(cmd, "Unable to create store of type foobar")

    # Don't specify a data-path
    cmd = "{path}/ceph-objectstore-tool --no-mon-config --type memstore --op list --pgid {pg}".format(dir=OSDDIR, osd=ONEOSD, pg=ONEPG, path=CEPH_BIN)
    ERRORS += test_failure(cmd, "Must provide --data-path")

    cmd = (CFSD_PREFIX + "--op remove --pgid 2.0").format(osd=ONEOSD)
    ERRORS += test_failure(cmd, "Please use export-remove or you must use --force option")

    cmd = (CFSD_PREFIX + "--force --op remove").format(osd=ONEOSD)
    ERRORS += test_failure(cmd, "Must provide pgid")

    # Don't secify a --op nor object command
    cmd = CFSD_PREFIX.format(osd=ONEOSD)
    ERRORS += test_failure(cmd, "Must provide --op or object command...")

    # Specify a bad --op command
    cmd = (CFSD_PREFIX + "--op oops").format(osd=ONEOSD)
    ERRORS += test_failure(cmd, "Must provide --op (info, log, remove, mkfs, fsck, repair, export, export-remove, import, list, fix-lost, list-pgs, dump-journal, dump-super, meta-list, get-osdmap, set-osdmap, get-inc-osdmap, set-inc-osdmap, mark-complete, reset-last-complete, dump-import, trim-pg-log)")

    # Provide just the object param not a command
    cmd = (CFSD_PREFIX + "object").format(osd=ONEOSD)
    ERRORS += test_failure(cmd, "Invalid syntax, missing command")

    # Provide an object name that doesn't exist
    cmd = (CFSD_PREFIX + "NON_OBJECT get-bytes").format(osd=ONEOSD)
    ERRORS += test_failure(cmd, "No object id 'NON_OBJECT' found")

    # Provide an invalid object command
    cmd = (CFSD_PREFIX + "--pgid {pg} '' notacommand").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Unknown object command 'notacommand'")

    cmd = (CFSD_PREFIX + "foo list-omap").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "No object id 'foo' found or invalid JSON specified")

    cmd = (CFSD_PREFIX + "'{{\"oid\":\"obj4\",\"key\":\"\",\"snapid\":-1,\"hash\":2826278768,\"max\":0,\"pool\":1,\"namespace\":\"\"}}' list-omap").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Without --pgid the object '{\"oid\":\"obj4\",\"key\":\"\",\"snapid\":-1,\"hash\":2826278768,\"max\":0,\"pool\":1,\"namespace\":\"\"}' must be a JSON array")

    cmd = (CFSD_PREFIX + "'[]' list-omap").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Object '[]' must be a JSON array with 2 elements")

    cmd = (CFSD_PREFIX + "'[\"1.0\"]' list-omap").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Object '[\"1.0\"]' must be a JSON array with 2 elements")

    cmd = (CFSD_PREFIX + "'[\"1.0\", 5, 8, 9]' list-omap").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Object '[\"1.0\", 5, 8, 9]' must be a JSON array with 2 elements")

    cmd = (CFSD_PREFIX + "'[1, 2]' list-omap").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Object '[1, 2]' must be a JSON array with the first element a string")

    cmd = (CFSD_PREFIX + "'[\"1.3\",{{\"snapid\":\"not an int\"}}]' list-omap").format(osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "Decode object JSON error: value type is 2 not 4")

    TMPFILE = r"/tmp/tmp.{pid}".format(pid=pid)
    ALLPGS = OBJREPPGS + OBJECPGS
    OSDS = get_osds(ALLPGS[0], OSDDIR)
    osd = OSDS[0]

    print("Test all --op dump-journal")
    ALLOSDS = [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and f.find("osd") == 0]
    ERRORS += test_dump_journal(CFSD_PREFIX, ALLOSDS)

    # Test --op list and generate json for all objects
    print("Test --op list variants")

    # retrieve all objects from all PGs
    tmpfd = open(TMPFILE, "wb")
    cmd = (CFSD_PREFIX + "--op list --format json").format(osd=osd)
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=tmpfd)
    if ret != 0:
        logging.error("Bad exit status {ret} from {cmd}".format(ret=ret, cmd=cmd))
        ERRORS += 1
    tmpfd.close()
    lines = get_lines(TMPFILE)
    JSONOBJ = sorted(set(lines))
    (pgid, coll, jsondict) = json.loads(JSONOBJ[0])[0]

    # retrieve all objects in a given PG
    tmpfd = open(OTHERFILE, "ab")
    cmd = (CFSD_PREFIX + "--op list --pgid {pg} --format json").format(osd=osd, pg=pgid)
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=tmpfd)
    if ret != 0:
        logging.error("Bad exit status {ret} from {cmd}".format(ret=ret, cmd=cmd))
        ERRORS += 1
    tmpfd.close()
    lines = get_lines(OTHERFILE)
    JSONOBJ = sorted(set(lines))
    (other_pgid, other_coll, other_jsondict) = json.loads(JSONOBJ[0])[0]

    if pgid != other_pgid or jsondict != other_jsondict or coll != other_coll:
        logging.error("the first line of --op list is different "
                      "from the first line of --op list --pgid {pg}".format(pg=pgid))
        ERRORS += 1

    # retrieve all objects with a given name in a given PG
    tmpfd = open(OTHERFILE, "wb")
    cmd = (CFSD_PREFIX + "--op list --pgid {pg} {object} --format json").format(osd=osd, pg=pgid, object=jsondict['oid'])
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=tmpfd)
    if ret != 0:
        logging.error("Bad exit status {ret} from {cmd}".format(ret=ret, cmd=cmd))
        ERRORS += 1
    tmpfd.close()
    lines = get_lines(OTHERFILE)
    JSONOBJ = sorted(set(lines))
    (other_pgid, other_coll, other_jsondict) in json.loads(JSONOBJ[0])[0]

    if pgid != other_pgid or jsondict != other_jsondict or coll != other_coll:
        logging.error("the first line of --op list is different "
                      "from the first line of --op list --pgid {pg} {object}".format(pg=pgid, object=jsondict['oid']))
        ERRORS += 1

    print("Test --op list by generating json for all objects using default format")
    for pg in ALLPGS:
        OSDS = get_osds(pg, OSDDIR)
        for osd in OSDS:
            tmpfd = open(TMPFILE, "ab")
            cmd = (CFSD_PREFIX + "--op list --pgid {pg}").format(osd=osd, pg=pg)
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
        # Skip clones for now
        if jsondict['snapid'] != -2:
            continue
        db[jsondict['namespace']][jsondict['oid']]['json'] = json.dumps((pgid, jsondict))
        # print db[jsondict['namespace']][jsondict['oid']]['json']
        if jsondict['oid'].find(EC_NAME) == 0 and 'shard_id' not in jsondict:
            logging.error("Malformed JSON {json}".format(json=JSON))
            ERRORS += 1

    # Test get-bytes
    print("Test get-bytes and set-bytes")
    for nspace in db.keys():
        for basename in db[nspace].keys():
            file = os.path.join(DATADIR, nspace + "-" + basename + "__head")
            JSON = db[nspace][basename]['json']
            GETNAME = "/tmp/getbytes.{pid}".format(pid=pid)
            TESTNAME = "/tmp/testbytes.{pid}".format(pid=pid)
            SETNAME = "/tmp/setbytes.{pid}".format(pid=pid)
            BADNAME = "/tmp/badbytes.{pid}".format(pid=pid)
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
                    fd = open(TESTNAME, "wb")
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

                    # Use set-bytes with --dry-run and make sure contents haven't changed
                    fd = open(BADNAME, "w")
                    data = "Bad data for --dry-run in {file}\n".format(file=file)
                    fd.write(data)
                    fd.close()
                    cmd = (CFSD_PREFIX + "--dry-run --pgid {pg} '{json}' set-bytes {sname}").format(osd=osd, pg=pg, json=JSON, sname=BADNAME)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
                    if ret != 0:
                        logging.error("Bad exit status {ret} from set-bytes --dry-run".format(ret=ret))
                        ERRORS += 1
                    fd = open(TESTNAME, "wb")
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
                        logging.error("Data after set-bytes --dry-run changed!")
                        logging.debug("Got:")
                        cat_file(logging.DEBUG, TESTNAME)
                        logging.debug("Expected:")
                        cat_file(logging.DEBUG, SETNAME)
                        ERRORS += 1

                    fd = open(file, "rb")
                    cmd = (CFSD_PREFIX + "--pgid {pg} '{json}' set-bytes").format(osd=osd, pg=pg, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdin=fd)
                    if ret != 0:
                        logging.error("Bad exit status {ret} from set-bytes to restore object".format(ret=ret))
                        ERRORS += 1
                    fd.close()

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
    try:
        os.unlink(BADNAME)
    except:
        pass

    # Test get-attr, set-attr, rm-attr, get-omaphdr, set-omaphdr, get-omap, set-omap, rm-omap
    print("Test get-attr, set-attr, rm-attr, get-omaphdr, set-omaphdr, get-omap, set-omap, rm-omap")
    for nspace in db.keys():
        for basename in db[nspace].keys():
            file = os.path.join(DATADIR, nspace + "-" + basename + "__head")
            JSON = db[nspace][basename]['json']
            for pg in OBJREPPGS:
                OSDS = get_osds(pg, OSDDIR)
                for osd in OSDS:
                    DIR = os.path.join(OSDDIR, os.path.join(osd, os.path.join("current", "{pg}_head".format(pg=pg))))
                    fnames = [f for f in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, f))
                              and f.split("_")[0] == basename and f.split("_")[4] == nspace]
                    if not fnames:
                        continue
                    for key, val in db[nspace][basename]["xattr"].items():
                        attrkey = "_" + key
                        cmd = (CFSD_PREFIX + " '{json}' get-attr {key}").format(osd=osd, json=JSON, key=attrkey)
                        logging.debug(cmd)
                        getval = check_output(cmd, shell=True)
                        if getval != val:
                            logging.error("get-attr of key {key} returned wrong val: {get} instead of {orig}".format(key=attrkey, get=getval, orig=val))
                            ERRORS += 1
                            continue
                        # set-attr to bogus value "foobar"
                        cmd = ("echo -n foobar | " + CFSD_PREFIX + " --pgid {pg} '{json}' set-attr {key}").format(osd=osd, pg=pg, json=JSON, key=attrkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from set-attr".format(ret=ret))
                            ERRORS += 1
                            continue
                        # Test set-attr with dry-run
                        cmd = ("echo -n dryrunbroken | " + CFSD_PREFIX + "--dry-run '{json}' set-attr {key}").format(osd=osd, pg=pg, json=JSON, key=attrkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True, stdout=nullfd)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from set-attr".format(ret=ret))
                            ERRORS += 1
                            continue
                        # Check the set-attr
                        cmd = (CFSD_PREFIX + " --pgid {pg} '{json}' get-attr {key}").format(osd=osd, pg=pg, json=JSON, key=attrkey)
                        logging.debug(cmd)
                        getval = check_output(cmd, shell=True)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from get-attr".format(ret=ret))
                            ERRORS += 1
                            continue
                        if getval != "foobar":
                            logging.error("Check of set-attr failed because we got {val}".format(val=getval))
                            ERRORS += 1
                            continue
                        # Test rm-attr
                        cmd = (CFSD_PREFIX + "'{json}' rm-attr {key}").format(osd=osd, pg=pg, json=JSON, key=attrkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from rm-attr".format(ret=ret))
                            ERRORS += 1
                            continue
                        # Check rm-attr with dry-run
                        cmd = (CFSD_PREFIX + "--dry-run '{json}' rm-attr {key}").format(osd=osd, pg=pg, json=JSON, key=attrkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True, stdout=nullfd)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from rm-attr".format(ret=ret))
                            ERRORS += 1
                            continue
                        cmd = (CFSD_PREFIX + "'{json}' get-attr {key}").format(osd=osd, pg=pg, json=JSON, key=attrkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True, stderr=nullfd, stdout=nullfd)
                        if ret == 0:
                            logging.error("For rm-attr expect get-attr to fail, but it succeeded")
                            ERRORS += 1
                        # Put back value
                        cmd = ("echo -n {val} | " + CFSD_PREFIX + " --pgid {pg} '{json}' set-attr {key}").format(osd=osd, pg=pg, json=JSON, key=attrkey, val=val)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from set-attr".format(ret=ret))
                            ERRORS += 1
                            continue

                    hdr = db[nspace][basename].get("omapheader", "")
                    cmd = (CFSD_PREFIX + "'{json}' get-omaphdr").format(osd=osd, json=JSON)
                    logging.debug(cmd)
                    gethdr = check_output(cmd, shell=True)
                    if gethdr != hdr:
                        logging.error("get-omaphdr was wrong: {get} instead of {orig}".format(get=gethdr, orig=hdr))
                        ERRORS += 1
                        continue
                    # set-omaphdr to bogus value "foobar"
                    cmd = ("echo -n foobar | " + CFSD_PREFIX + "'{json}' set-omaphdr").format(osd=osd, pg=pg, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True)
                    if ret != 0:
                        logging.error("Bad exit status {ret} from set-omaphdr".format(ret=ret))
                        ERRORS += 1
                        continue
                    # Check the set-omaphdr
                    cmd = (CFSD_PREFIX + "'{json}' get-omaphdr").format(osd=osd, pg=pg, json=JSON)
                    logging.debug(cmd)
                    gethdr = check_output(cmd, shell=True)
                    if ret != 0:
                        logging.error("Bad exit status {ret} from get-omaphdr".format(ret=ret))
                        ERRORS += 1
                        continue
                    if gethdr != "foobar":
                        logging.error("Check of set-omaphdr failed because we got {val}".format(val=getval))
                        ERRORS += 1
                        continue
                    # Test dry-run with set-omaphdr
                    cmd = ("echo -n dryrunbroken | " + CFSD_PREFIX + "--dry-run '{json}' set-omaphdr").format(osd=osd, pg=pg, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdout=nullfd)
                    if ret != 0:
                        logging.error("Bad exit status {ret} from set-omaphdr".format(ret=ret))
                        ERRORS += 1
                        continue
                    # Put back value
                    cmd = ("echo -n {val} | " + CFSD_PREFIX + "'{json}' set-omaphdr").format(osd=osd, pg=pg, json=JSON, val=hdr)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True)
                    if ret != 0:
                        logging.error("Bad exit status {ret} from set-omaphdr".format(ret=ret))
                        ERRORS += 1
                        continue

                    for omapkey, val in db[nspace][basename]["omap"].items():
                        cmd = (CFSD_PREFIX + " '{json}' get-omap {key}").format(osd=osd, json=JSON, key=omapkey)
                        logging.debug(cmd)
                        getval = check_output(cmd, shell=True)
                        if getval != val:
                            logging.error("get-omap of key {key} returned wrong val: {get} instead of {orig}".format(key=omapkey, get=getval, orig=val))
                            ERRORS += 1
                            continue
                        # set-omap to bogus value "foobar"
                        cmd = ("echo -n foobar | " + CFSD_PREFIX + " --pgid {pg} '{json}' set-omap {key}").format(osd=osd, pg=pg, json=JSON, key=omapkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from set-omap".format(ret=ret))
                            ERRORS += 1
                            continue
                        # Check set-omap with dry-run
                        cmd = ("echo -n dryrunbroken | " + CFSD_PREFIX + "--dry-run --pgid {pg} '{json}' set-omap {key}").format(osd=osd, pg=pg, json=JSON, key=omapkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True, stdout=nullfd)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from set-omap".format(ret=ret))
                            ERRORS += 1
                            continue
                        # Check the set-omap
                        cmd = (CFSD_PREFIX + " --pgid {pg} '{json}' get-omap {key}").format(osd=osd, pg=pg, json=JSON, key=omapkey)
                        logging.debug(cmd)
                        getval = check_output(cmd, shell=True)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from get-omap".format(ret=ret))
                            ERRORS += 1
                            continue
                        if getval != "foobar":
                            logging.error("Check of set-omap failed because we got {val}".format(val=getval))
                            ERRORS += 1
                            continue
                        # Test rm-omap
                        cmd = (CFSD_PREFIX + "'{json}' rm-omap {key}").format(osd=osd, pg=pg, json=JSON, key=omapkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from rm-omap".format(ret=ret))
                            ERRORS += 1
                        # Check rm-omap with dry-run
                        cmd = (CFSD_PREFIX + "--dry-run '{json}' rm-omap {key}").format(osd=osd, pg=pg, json=JSON, key=omapkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True, stdout=nullfd)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from rm-omap".format(ret=ret))
                            ERRORS += 1
                        cmd = (CFSD_PREFIX + "'{json}' get-omap {key}").format(osd=osd, pg=pg, json=JSON, key=omapkey)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True, stderr=nullfd, stdout=nullfd)
                        if ret == 0:
                            logging.error("For rm-omap expect get-omap to fail, but it succeeded")
                            ERRORS += 1
                        # Put back value
                        cmd = ("echo -n {val} | " + CFSD_PREFIX + " --pgid {pg} '{json}' set-omap {key}").format(osd=osd, pg=pg, json=JSON, key=omapkey, val=val)
                        logging.debug(cmd)
                        ret = call(cmd, shell=True)
                        if ret != 0:
                            logging.error("Bad exit status {ret} from set-omap".format(ret=ret))
                            ERRORS += 1
                            continue

    # Test dump
    print("Test dump")
    for nspace in db.keys():
        for basename in db[nspace].keys():
            file = os.path.join(DATADIR, nspace + "-" + basename + "__head")
            JSON = db[nspace][basename]['json']
            jsondict = json.loads(JSON)
            for pg in OBJREPPGS:
                OSDS = get_osds(pg, OSDDIR)
                for osd in OSDS:
                    DIR = os.path.join(OSDDIR, os.path.join(osd, os.path.join("current", "{pg}_head".format(pg=pg))))
                    fnames = [f for f in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, f))
                              and f.split("_")[0] == basename and f.split("_")[4] == nspace]
                    if not fnames:
                        continue
                    if int(basename.split(REP_NAME)[1]) > int(NUM_CLONED_REP_OBJECTS):
                        continue
                    logging.debug("REPobject " + JSON)
                    cmd = (CFSD_PREFIX + " '{json}' dump | grep '\"snap\": 1,' > /dev/null").format(osd=osd, json=JSON)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True)
                    if ret != 0:
                        logging.error("Invalid dump for {json}".format(json=JSON))
                        ERRORS += 1
            if 'shard_id' in jsondict[1]:
                logging.debug("ECobject " + JSON)
                for pg in OBJECPGS:
                    OSDS = get_osds(pg, OSDDIR)
                    jsondict = json.loads(JSON)
                    for osd in OSDS:
                        DIR = os.path.join(OSDDIR, os.path.join(osd, os.path.join("current", "{pg}_head".format(pg=pg))))
                        fnames = [f for f in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, f))
                                  and f.split("_")[0] == basename and f.split("_")[4] == nspace]
                        if not fnames:
                            continue
                        if int(basename.split(EC_NAME)[1]) > int(NUM_EC_OBJECTS):
                            continue
                        # Fix shard_id since we only have one json instance for each object
                        jsondict[1]['shard_id'] = int(pg.split('s')[1])
                        cmd = (CFSD_PREFIX + " '{json}' dump | grep '\"hinfo\": [{{]' > /dev/null").format(osd=osd, json=json.dumps((pg, jsondict[1])))
                        logging.debug(cmd)
                        ret = call(cmd, shell=True)
                        if ret != 0:
                            logging.error("Invalid dump for {json}".format(json=JSON))

    print("Test list-attrs get-attr")
    ATTRFILE = r"/tmp/attrs.{pid}".format(pid=pid)
    VALFILE = r"/tmp/val.{pid}".format(pid=pid)
    for nspace in db.keys():
        for basename in db[nspace].keys():
            file = os.path.join(DATADIR, nspace + "-" + basename)
            JSON = db[nspace][basename]['json']
            jsondict = json.loads(JSON)

            if 'shard_id' in jsondict[1]:
                logging.debug("ECobject " + JSON)
                found = 0
                for pg in OBJECPGS:
                    OSDS = get_osds(pg, OSDDIR)
                    # Fix shard_id since we only have one json instance for each object
                    jsondict[1]['shard_id'] = int(pg.split('s')[1])
                    JSON = json.dumps((pg, jsondict[1]))
                    for osd in OSDS:
                        cmd = (CFSD_PREFIX + " '{json}' get-attr hinfo_key").format(osd=osd, json=JSON)
                        logging.debug("TRY: " + cmd)
                        try:
                            out = check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                            logging.debug("FOUND: {json} in {osd} has value '{val}'".format(osd=osd, json=JSON, val=out))
                            found += 1
                        except subprocess.CalledProcessError as e:
                            if "No such file or directory" not in e.output and "No data available" not in e.output:
                                raise
                # Assuming k=2 m=1 for the default ec pool
                if found != 3:
                    logging.error("{json} hinfo_key found {found} times instead of 3".format(json=JSON, found=found))
                    ERRORS += 1

            for pg in ALLPGS:
                # Make sure rep obj with rep pg or ec obj with ec pg
                if ('shard_id' in jsondict[1]) != (pg.find('s') > 0):
                    continue
                if 'shard_id' in jsondict[1]:
                    # Fix shard_id since we only have one json instance for each object
                    jsondict[1]['shard_id'] = int(pg.split('s')[1])
                    JSON = json.dumps((pg, jsondict[1]))
                OSDS = get_osds(pg, OSDDIR)
                for osd in OSDS:
                    DIR = os.path.join(OSDDIR, os.path.join(osd, os.path.join("current", "{pg}_head".format(pg=pg))))
                    fnames = [f for f in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, f))
                              and f.split("_")[0] == basename and f.split("_")[4] == nspace]
                    if not fnames:
                        continue
                    afd = open(ATTRFILE, "wb")
                    cmd = (CFSD_PREFIX + " '{json}' list-attrs").format(osd=osd, json=JSON)
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
                        vfd = open(VALFILE, "wb")
                        cmd = (CFSD_PREFIX + " '{json}' get-attr {key}").format(osd=osd, json=JSON, key="_" + key)
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
                        print(values)

    print("Test --op meta-list")
    tmpfd = open(TMPFILE, "wb")
    cmd = (CFSD_PREFIX + "--op meta-list").format(osd=ONEOSD)
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=tmpfd)
    if ret != 0:
        logging.error("Bad exit status {ret} from --op meta-list request".format(ret=ret))
        ERRORS += 1

    print("Test get-bytes on meta")
    tmpfd.close()
    lines = get_lines(TMPFILE)
    JSONOBJ = sorted(set(lines))
    for JSON in JSONOBJ:
        (pgid, jsondict) = json.loads(JSON)
        if pgid != "meta":
            logging.error("pgid incorrect for --op meta-list {pgid}".format(pgid=pgid))
            ERRORS += 1
        if jsondict['namespace'] != "":
            logging.error("namespace non null --op meta-list {ns}".format(ns=jsondict['namespace']))
            ERRORS += 1
        logging.info(JSON)
        try:
            os.unlink(GETNAME)
        except:
            pass
        cmd = (CFSD_PREFIX + "'{json}' get-bytes {fname}").format(osd=ONEOSD, json=JSON, fname=GETNAME)
        logging.debug(cmd)
        ret = call(cmd, shell=True)
        if ret != 0:
            logging.error("Bad exit status {ret}".format(ret=ret))
            ERRORS += 1

    try:
        os.unlink(GETNAME)
    except:
        pass
    try:
        os.unlink(TESTNAME)
    except:
        pass

    print("Test pg info")
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            cmd = (CFSD_PREFIX + "--op info --pgid {pg} | grep '\"pgid\": \"{pg}\"'").format(osd=osd, pg=pg)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=nullfd)
            if ret != 0:
                logging.error("Getting info failed for pg {pg} from {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
                ERRORS += 1

    print("Test pg logging")
    if len(ALLREPPGS + ALLECPGS) == len(OBJREPPGS + OBJECPGS):
        logging.warning("All PGs have objects, so no log without modify entries")
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            tmpfd = open(TMPFILE, "wb")
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
                print("Log should {msg}have a modify entry".format(msg=MSG))
                ERRORS += 1

    try:
        os.unlink(TMPFILE)
    except:
        pass

    print("Test list-pgs")
    for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and f.find("osd") == 0]:

        CHECK_PGS = get_osd_pgs(os.path.join(OSDDIR, osd), None)
        CHECK_PGS = sorted(CHECK_PGS)

        cmd = (CFSD_PREFIX + "--op list-pgs").format(osd=osd)
        logging.debug(cmd)
        TEST_PGS = check_output(cmd, shell=True).split("\n")
        TEST_PGS = sorted(TEST_PGS)[1:]  # Skip extra blank line

        if TEST_PGS != CHECK_PGS:
            logging.error("list-pgs got wrong result for osd.{osd}".format(osd=osd))
            logging.error("Expected {pgs}".format(pgs=CHECK_PGS))
            logging.error("Got {pgs}".format(pgs=TEST_PGS))
            ERRORS += 1

    EXP_ERRORS = 0
    print("Test pg export --dry-run")
    pg = ALLREPPGS[0]
    osd = get_osds(pg, OSDDIR)[0]
    fname = "/tmp/fname.{pid}".format(pid=pid)
    cmd = (CFSD_PREFIX + "--dry-run --op export --pgid {pg} --file {file}").format(osd=osd, pg=pg, file=fname)
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    if ret != 0:
        logging.error("Exporting --dry-run failed for pg {pg} on {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
        EXP_ERRORS += 1
    elif os.path.exists(fname):
        logging.error("Exporting --dry-run created file")
        EXP_ERRORS += 1

    cmd = (CFSD_PREFIX + "--dry-run --op export --pgid {pg} > {file}").format(osd=osd, pg=pg, file=fname)
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    if ret != 0:
        logging.error("Exporting --dry-run failed for pg {pg} on {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
        EXP_ERRORS += 1
    else:
        outdata = get_lines(fname)
        if len(outdata) > 0:
            logging.error("Exporting --dry-run to stdout not empty")
            logging.error("Data: " + outdata)
            EXP_ERRORS += 1

    os.mkdir(TESTDIR)
    for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and f.find("osd") == 0]:
        os.mkdir(os.path.join(TESTDIR, osd))
    print("Test pg export")
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

    print("Test clear-data-digest")
    for nspace in db.keys():
        for basename in db[nspace].keys():
          JSON = db[nspace][basename]['json']
          cmd = (CFSD_PREFIX + "'{json}' clear-data-digest").format(osd='osd0', json=JSON)
          logging.debug(cmd)
          ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
          if ret != 0:
              logging.error("Clearing data digest failed for {json}".format(json=JSON))
              ERRORS += 1
              break
          cmd = (CFSD_PREFIX + "'{json}' dump | grep '\"data_digest\": \"0xff'").format(osd='osd0', json=JSON)
          logging.debug(cmd)
          ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
          if ret != 0:
              logging.error("Data digest not cleared for {json}".format(json=JSON))
              ERRORS += 1
              break
          break
        break

    print("Test pg removal")
    RM_ERRORS = 0
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            # This should do nothing
            cmd = (CFSD_PREFIX + "--op remove --pgid {pg} --dry-run").format(pg=pg, osd=osd)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=nullfd)
            if ret != 0:
                logging.error("Removing --dry-run failed for pg {pg} on {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
                RM_ERRORS += 1
            cmd = (CFSD_PREFIX + "--force --op remove --pgid {pg}").format(pg=pg, osd=osd)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=nullfd)
            if ret != 0:
                logging.error("Removing failed for pg {pg} on {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
                RM_ERRORS += 1

    ERRORS += RM_ERRORS

    IMP_ERRORS = 0
    if EXP_ERRORS == 0 and RM_ERRORS == 0:
        print("Test pg import")
        for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and f.find("osd") == 0]:
            dir = os.path.join(TESTDIR, osd)
            PGS = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]
            for pg in PGS:
                file = os.path.join(dir, pg)
                # Make sure this doesn't crash
                cmd = (CFSD_PREFIX + "--op dump-import --file {file}").format(osd=osd, file=file)
                logging.debug(cmd)
                ret = call(cmd, shell=True, stdout=nullfd)
                if ret != 0:
                    logging.error("Dump-import failed from {file} with {ret}".format(file=file, ret=ret))
                    IMP_ERRORS += 1
                # This should do nothing
                cmd = (CFSD_PREFIX + "--op import --file {file} --dry-run").format(osd=osd, file=file)
                logging.debug(cmd)
                ret = call(cmd, shell=True, stdout=nullfd)
                if ret != 0:
                    logging.error("Import failed from {file} with {ret}".format(file=file, ret=ret))
                    IMP_ERRORS += 1
                if pg == PGS[0]:
                    cmd = ("cat {file} |".format(file=file) + CFSD_PREFIX + "--op import").format(osd=osd)
                elif pg == PGS[1]:
                    cmd = (CFSD_PREFIX + "--op import --file - --pgid {pg} < {file}").format(osd=osd, file=file, pg=pg)
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
        print("Verify replicated import data")
        data_errors, _ = check_data(DATADIR, TMPFILE, OSDDIR, REP_NAME)
        ERRORS += data_errors
    else:
        logging.warning("SKIPPING CHECKING IMPORT DATA DUE TO PREVIOUS FAILURES")

    print("Test all --op dump-journal again")
    ALLOSDS = [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and f.find("osd") == 0]
    ERRORS += test_dump_journal(CFSD_PREFIX, ALLOSDS)

    vstart(new=False)
    wait_for_health()

    if EXP_ERRORS == 0 and RM_ERRORS == 0 and IMP_ERRORS == 0:
        print("Verify erasure coded import data")
        ERRORS += verify(DATADIR, EC_POOL, EC_NAME, db)
        # Check replicated data/xattr/omap using rados
        print("Verify replicated import data using rados")
        ERRORS += verify(DATADIR, REP_POOL, REP_NAME, db)

    if EXP_ERRORS == 0:
        NEWPOOL = "rados-import-pool"
        cmd = "{path}/ceph osd pool create {pool} 8".format(pool=NEWPOOL, path=CEPH_BIN)
        logging.debug(cmd)
        ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)

        print("Test rados import")
        first = True
        for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and f.find("osd") == 0]:
            dir = os.path.join(TESTDIR, osd)
            for pg in [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]:
                if pg.find("{id}.".format(id=REPID)) != 0:
                    continue
                file = os.path.join(dir, pg)
                if first:
                    first = False
                    # This should do nothing
                    cmd = "{path}/rados import -p {pool} --dry-run {file}".format(pool=NEWPOOL, file=file, path=CEPH_BIN)
                    logging.debug(cmd)
                    ret = call(cmd, shell=True, stdout=nullfd)
                    if ret != 0:
                        logging.error("Rados import --dry-run failed from {file} with {ret}".format(file=file, ret=ret))
                        ERRORS += 1
                    cmd = "{path}/rados -p {pool} ls".format(pool=NEWPOOL, path=CEPH_BIN)
                    logging.debug(cmd)
                    data = check_output(cmd, shell=True)
                    if data:
                        logging.error("'{data}'".format(data=data))
                        logging.error("Found objects after dry-run")
                        ERRORS += 1
                cmd = "{path}/rados import -p {pool} {file}".format(pool=NEWPOOL, file=file, path=CEPH_BIN)
                logging.debug(cmd)
                ret = call(cmd, shell=True, stdout=nullfd)
                if ret != 0:
                    logging.error("Rados import failed from {file} with {ret}".format(file=file, ret=ret))
                    ERRORS += 1
                cmd = "{path}/rados import -p {pool} --no-overwrite {file}".format(pool=NEWPOOL, file=file, path=CEPH_BIN)
                logging.debug(cmd)
                ret = call(cmd, shell=True, stdout=nullfd)
                if ret != 0:
                    logging.error("Rados import --no-overwrite failed from {file} with {ret}".format(file=file, ret=ret))
                    ERRORS += 1

        ERRORS += verify(DATADIR, NEWPOOL, REP_NAME, db)
    else:
        logging.warning("SKIPPING IMPORT-RADOS TESTS DUE TO PREVIOUS FAILURES")

    # Clear directories of previous portion
    call("/bin/rm -rf {dir}".format(dir=TESTDIR), shell=True)
    call("/bin/rm -rf {dir}".format(dir=DATADIR), shell=True)
    os.mkdir(TESTDIR)
    os.mkdir(DATADIR)

    # Cause SPLIT_POOL to split and test import with object/log filtering
    print("Testing import all objects after a split")
    SPLIT_POOL = "split_pool"
    PG_COUNT = 1
    SPLIT_OBJ_COUNT = 5
    SPLIT_NSPACE_COUNT = 2
    SPLIT_NAME = "split"
    cmd = "{path}/ceph osd pool create {pool} {pg} {pg} replicated".format(pool=SPLIT_POOL, pg=PG_COUNT, path=CEPH_BIN)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    SPLITID = get_pool_id(SPLIT_POOL, nullfd)
    pool_size = int(check_output("{path}/ceph osd pool get {pool} size".format(pool=SPLIT_POOL, path=CEPH_BIN), shell=True, stderr=nullfd).split(" ")[1])
    EXP_ERRORS = 0
    RM_ERRORS = 0
    IMP_ERRORS = 0

    objects = range(1, SPLIT_OBJ_COUNT + 1)
    nspaces = range(SPLIT_NSPACE_COUNT)
    for n in nspaces:
        nspace = get_nspace(n)

        for i in objects:
            NAME = SPLIT_NAME + "{num}".format(num=i)
            LNAME = nspace + "-" + NAME
            DDNAME = os.path.join(DATADIR, LNAME)
            DDNAME += "__head"

            cmd = "rm -f " + DDNAME
            logging.debug(cmd)
            call(cmd, shell=True)

            if i == 1:
                dataline = range(DATALINECOUNT)
            else:
                dataline = range(1)
            fd = open(DDNAME, "w")
            data = "This is the split data for " + LNAME + "\n"
            for _ in dataline:
                fd.write(data)
            fd.close()

            cmd = "{path}/rados -p {pool} -N '{nspace}' put {name} {ddname}".format(pool=SPLIT_POOL, name=NAME, ddname=DDNAME, nspace=nspace, path=CEPH_BIN)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stderr=nullfd)
            if ret != 0:
                logging.critical("Rados put command failed with {ret}".format(ret=ret))
                return 1

    wait_for_health()
    kill_daemons()

    for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and f.find("osd") == 0]:
        os.mkdir(os.path.join(TESTDIR, osd))

    pg = "{pool}.0".format(pool=SPLITID)
    EXPORT_PG = pg

    export_osds = get_osds(pg, OSDDIR)
    for osd in export_osds:
        mydir = os.path.join(TESTDIR, osd)
        fname = os.path.join(mydir, pg)
        cmd = (CFSD_PREFIX + "--op export --pgid {pg} --file {file}").format(osd=osd, pg=pg, file=fname)
        logging.debug(cmd)
        ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
        if ret != 0:
            logging.error("Exporting failed for pg {pg} on {osd} with {ret}".format(pg=pg, osd=osd, ret=ret))
            EXP_ERRORS += 1

    ERRORS += EXP_ERRORS

    if EXP_ERRORS == 0:
        vstart(new=False)
        wait_for_health()

        cmd = "{path}/ceph osd pool set {pool} pg_num 2".format(pool=SPLIT_POOL, path=CEPH_BIN)
        logging.debug(cmd)
        ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
        time.sleep(5)
        wait_for_health()

        kill_daemons()

        # Now 2 PGs, poolid.0 and poolid.1
        # make note of pgs before we remove the pgs...
        osds = get_osds("{pool}.0".format(pool=SPLITID), OSDDIR);
        for seed in range(2):
            pg = "{pool}.{seed}".format(pool=SPLITID, seed=seed)

            for osd in osds:
                cmd = (CFSD_PREFIX + "--force --op remove --pgid {pg}").format(pg=pg, osd=osd)
                logging.debug(cmd)
                ret = call(cmd, shell=True, stdout=nullfd)

        which = 0
        for osd in osds:
            # This is weird.  The export files are based on only the EXPORT_PG
            # and where that pg was before the split.  Use 'which' to use all
            # export copies in import.
            mydir = os.path.join(TESTDIR, export_osds[which])
            fname = os.path.join(mydir, EXPORT_PG)
            which += 1
            cmd = (CFSD_PREFIX + "--op import --pgid {pg} --file {file}").format(osd=osd, pg=EXPORT_PG, file=fname)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=nullfd)
            if ret != 0:
                logging.error("Import failed from {file} with {ret}".format(file=file, ret=ret))
                IMP_ERRORS += 1

        ERRORS += IMP_ERRORS

        # Start up again to make sure imports didn't corrupt anything
        if IMP_ERRORS == 0:
            print("Verify split import data")
            data_errors, count = check_data(DATADIR, TMPFILE, OSDDIR, SPLIT_NAME)
            ERRORS += data_errors
            if count != (SPLIT_OBJ_COUNT * SPLIT_NSPACE_COUNT * pool_size):
                logging.error("Incorrect number of replicas seen {count}".format(count=count))
                ERRORS += 1
            vstart(new=False)
            wait_for_health()

    call("/bin/rm -rf {dir}".format(dir=TESTDIR), shell=True)
    call("/bin/rm -rf {dir}".format(dir=DATADIR), shell=True)

    ERRORS += test_removeall(CFSD_PREFIX, db, OBJREPPGS, REP_POOL, CEPH_BIN, OSDDIR, REP_NAME, NUM_CLONED_REP_OBJECTS)

    # vstart() starts 4 OSDs
    ERRORS += test_get_set_osdmap(CFSD_PREFIX, list(range(4)), ALLOSDS)
    ERRORS += test_get_set_inc_osdmap(CFSD_PREFIX, ALLOSDS[0])

    kill_daemons()
    CORES = [f for f in os.listdir(CEPH_DIR) if f.startswith("core.")]
    if CORES:
        CORE_DIR = os.path.join("/tmp", "cores.{pid}".format(pid=os.getpid()))
        os.mkdir(CORE_DIR)
        call("/bin/mv {ceph_dir}/core.* {core_dir}".format(ceph_dir=CEPH_DIR, core_dir=CORE_DIR), shell=True)
        logging.error("Failure due to cores found")
        logging.error("See {core_dir} for cores".format(core_dir=CORE_DIR))
        ERRORS += len(CORES)

    if ERRORS == 0:
        print("TEST PASSED")
        return 0
    else:
        print("TEST FAILED WITH {errcount} ERRORS".format(errcount=ERRORS))
        return 1


def remove_btrfs_subvolumes(path):
    if platform.system() == "FreeBSD":
        return
    result = subprocess.Popen("stat -f -c '%%T' %s" % path, shell=True, stdout=subprocess.PIPE)
    for line in result.stdout:
        filesystem = decode(line).rstrip('\n')
    if filesystem == "btrfs":
        result = subprocess.Popen("sudo btrfs subvolume list %s" % path, shell=True, stdout=subprocess.PIPE)
        for line in result.stdout:
            subvolume = decode(line).split()[8]
            # extracting the relative volume name
            m = re.search(".*(%s.*)" % path, subvolume)
            if m:
                found = m.group(1)
                call("sudo btrfs subvolume delete %s" % found, shell=True)


if __name__ == "__main__":
    status = 1
    try:
        status = main(sys.argv[1:])
    finally:
        kill_daemons()
        os.chdir(CEPH_BUILD_DIR)
        remove_btrfs_subvolumes(CEPH_DIR)
        call("/bin/rm -fr {dir}".format(dir=CEPH_DIR), shell=True)
    sys.exit(status)
