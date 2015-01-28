"""
MDS admin socket scrubbing-related tests.
"""
from cStringIO import StringIO
import json
import logging

from teuthology.orchestra import run
from teuthology import misc as teuthology

from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)


def run_test(ctx, config, filesystem):
    """
    Run flush and scrub commands on the specified files in the filesystem. This
    task will run through a sequence of operations, but it is not comprehensive
    on its own -- it doesn't manipulate the mds cache state to test on both
    in- and out-of-memory parts of the hierarchy. So it's designed to be run
    multiple times within a single test run, so that the test can manipulate
    memory state.

    Usage:
    mds_scrub_checks:
      mds_rank: 0
      path: path/to/test/dir
      client: 0
      run_seq: [0-9]+

    Increment the run_seq on subsequent invocations within a single test run;
    it uses that value to generate unique folder and file names.
    """

    mds_rank = config.get("mds_rank")
    test_path = config.get("path")
    run_seq = config.get("run_seq")
    client_id = config.get("client")

    if mds_rank is None or test_path is None or run_seq is None:
        raise ValueError("Must specify each of mds_rank, test_path, run_seq,"
                         "client_id in config!")

    teuthdir = teuthology.get_testdir(ctx)
    client_path = "{teuthdir}/mnt.{id_}/{test_path}".\
                  format(teuthdir=teuthdir,
                         id_=client_id,
                         test_path=test_path)

    log.info("Cloning repo into place (if not present)")
    repo_path = clone_repo(ctx, client_id, client_path)

    log.info("Initiating mds_scrub_checks on mds.{id_}, "
             "test_path {path}, run_seq {seq}".format(
                 id_=mds_rank, path=test_path, seq=run_seq))

    def json_validator(json, rc, element, expected_value):
        if (rc != 0):
            return False, "asok command returned error {rc}".format(rc=str(rc))
        element_value = json.get(element)
        if element_value != expected_value:
            return False, "unexpectedly got {jv} instead of {ev}!".format(
                jv=element_value, ev=expected_value)
        return True, "Succeeded"

    success_validator = lambda j, r: json_validator(j, r, "return_code", 0)

    nep = "{test_path}/i/dont/exist".format(test_path=test_path)
    command = "flush_path {nep}".format(nep=nep)
    asok_command(ctx, mds_rank, command,
                 lambda j, r: json_validator(j, r, "return_code", -2),
                 filesystem)

    command = "scrub_path {nep}".format(nep=nep)
    asok_command(ctx, mds_rank, command,
                 lambda j, r: json_validator(j, r, "return_code", -2),
                 filesystem)

    test_repo_path = "{test_path}/ceph-qa-suite".format(test_path=test_path)
    dirpath = "{repo_path}/suites".format(repo_path=test_repo_path)

    if (run_seq == 0):
        log.info("First run: flushing {dirpath}".format(dirpath=dirpath))
        command = "flush_path {dirpath}".format(dirpath=dirpath)
        asok_command(ctx, mds_rank, command, success_validator, filesystem)
    command = "scrub_path {dirpath}".format(dirpath=dirpath)
    asok_command(ctx, mds_rank, command, success_validator, filesystem)
    
    filepath = "{repo_path}/suites/fs/verify/validater/valgrind.yaml".format(
        repo_path=test_repo_path)
    if (run_seq == 0):
        log.info("First run: flushing {filepath}".format(filepath=filepath))
        command = "flush_path {filepath}".format(filepath=filepath)
        asok_command(ctx, mds_rank, command, success_validator, filesystem)
    command = "scrub_path {filepath}".format(filepath=filepath)
    asok_command(ctx, mds_rank, command, success_validator, filesystem)

    filepath = "{repo_path}/suites/fs/basic/clusters/fixed-3-cephfs.yaml".\
               format(repo_path=test_repo_path)
    command = "scrub_path {filepath}".format(filepath=filepath)
    asok_command(ctx, mds_rank, command,
                 lambda j, r: json_validator(j, r, "performed_validation",
                                             False),
                 filesystem)

    if (run_seq == 0):
        log.info("First run: flushing base dir /")
        command = "flush_path /"
        asok_command(ctx, mds_rank, command, success_validator, filesystem)
    command = "scrub_path /"
    asok_command(ctx, mds_rank, command, success_validator, filesystem)

    client = ctx.manager.find_remote("client", client_id)
    new_dir = "{repo_path}/new_dir_{i}".format(repo_path=repo_path, i=run_seq)
    test_new_dir = "{repo_path}/new_dir_{i}".format(repo_path=test_repo_path,
                                                    i=run_seq)
    client.run(args=[
        "mkdir", new_dir])
    command = "flush_path {dir}".format(dir=test_new_dir)
    asok_command(ctx, mds_rank, command, success_validator, filesystem)

    new_file = "{repo_path}/new_file_{i}".format(repo_path=repo_path,
                                                 i=run_seq)
    test_new_file = "{repo_path}/new_file_{i}".format(repo_path=test_repo_path,
                                                      i=run_seq)
    client.run(args=[
        "echo", "hello", run.Raw('>'), new_file])
    command = "flush_path {file}".format(file=test_new_file)
    asok_command(ctx, mds_rank, command, success_validator, filesystem)

    # check that scrub fails on errors. First, get ino
    client = ctx.manager.find_remote("client", 0)
    proc = client.run(
        args=[
            "ls", "-li", new_file, run.Raw('|'),
            "grep", "-o", run.Raw('"^[0-9]*"')
        ],
        wait=False,
        stdout=StringIO()
    )
    proc.wait()
    ino = int(proc.stdout.getvalue().strip())
    rados_obj_name = "{ino}.00000000".format(ino=hex(ino).split('x')[1])
    client.run(
        args=[
            "rados", "-p", "data", "rmxattr",
            rados_obj_name, "parent"
        ]
    )
    command = "scrub_path {file}".format(file=test_new_file)
    asok_command(ctx, mds_rank, command,
                 lambda j, r: json_validator(j, r, "return_code", -61), filesystem)
    client.run(
        args=[
            "rados", "-p", "data", "rm", rados_obj_name
        ]
    )
    asok_command(ctx, mds_rank, command,
                 lambda j, r: json_validator(j, r, "return_code", -2), filesystem)

    command = "flush_path /"
    asok_command(ctx, mds_rank, command, success_validator, filesystem)


class AsokCommandFailedError(Exception):
    """
    Exception thrown when we get an unexpected response
    on an admin socket command
    """
    def __init__(self, command, rc, json, errstring):
        self.command = command
        self.rc = rc
        self.json = json
        self.errstring = errstring

    def __str__(self):
        return "Admin socket: {command} failed with rc={rc},"
        "json output={json}, because '{es}'".format(
            command=self.command, rc=self.rc,
            json=self.json, es=self.errstring)


def asok_command(ctx, mds_rank, command, validator, filesystem):
    log.info("Running command '{command}'".format(command=command))

    command_list = command.split()

    # we just assume there's an active mds for every rank
    mds_id = filesystem.get_active_names()[mds_rank]

    proc = ctx.manager.admin_socket('mds', mds_id,
                                    command_list, check_status=False)
    rout = proc.exitstatus
    sout = proc.stdout.getvalue()

    if sout.strip():
        jout = json.loads(sout)
    else:
        jout = None

    log.info("command '{command}' got response code "
             "'{rout}' and stdout '{sout}'".format(
                 command=command, rout=rout, sout=sout))

    success, errstring = validator(jout, rout)

    if not success:
        raise AsokCommandFailedError(command, rout, jout, errstring)

    return jout


def clone_repo(ctx, client_id, path):
    repo = "ceph-qa-suite"
    repo_path = "{path}/{repo}".format(path=path, repo=repo)

    client = ctx.manager.find_remote("client", client_id)
    client.run(
        args=[
            "mkdir", "-p", path
            ]
        )
    client.run(
        args=[
            "ls", repo_path, run.Raw('||'),
            "git", "clone", '--branch', 'giant',
            "http://github.com/ceph/{repo}".format(repo=repo),
            "{path}/{repo}".format(path=path, repo=repo)
            ]
        )

    return repo_path


def task(ctx, config):
    fs = Filesystem(ctx)

    run_test(ctx, config, fs)
