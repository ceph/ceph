"""
MDS admin socket scrubbing-related tests.
"""
import json
import logging
import errno
import time
from teuthology.exceptions import CommandFailedError
import os
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestScrubChecks(CephFSTestCase):
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

    MDSS_REQUIRED = 1
    CLIENTS_REQUIRED = 1

    def test_scrub_checks(self):
        self._checks(0)
        self._checks(1)

    def _checks(self, run_seq):
        mds_rank = 0
        test_dir = "scrub_test_path"

        abs_test_path = "/{0}".format(test_dir)

        log.info("mountpoint: {0}".format(self.mount_a.mountpoint))
        client_path = os.path.join(self.mount_a.mountpoint, test_dir)
        log.info("client_path: {0}".format(client_path))

        log.info("Cloning repo into place")
        repo_path = self.clone_repo(self.mount_a, client_path)

        log.info("Initiating mds_scrub_checks on mds.{id_}, " +
                 "test_path {path}, run_seq {seq}".format(
                     id_=mds_rank, path=abs_test_path, seq=run_seq)
                 )


        success_validator = lambda j, r: self.json_validator(j, r, "return_code", 0)

        nep = "{test_path}/i/dont/exist".format(test_path=abs_test_path)
        self.asok_command(mds_rank, "flush_path {nep}".format(nep=nep),
                          lambda j, r: self.json_validator(j, r, "return_code", -errno.ENOENT))
        self.asok_command(mds_rank, "scrub_path {nep}".format(nep=nep),
                          lambda j, r: self.json_validator(j, r, "return_code", -errno.ENOENT))

        test_repo_path = "{test_path}/ceph-qa-suite".format(test_path=abs_test_path)
        dirpath = "{repo_path}/suites".format(repo_path=test_repo_path)

        if run_seq == 0:
            log.info("First run: flushing {dirpath}".format(dirpath=dirpath))
            command = "flush_path {dirpath}".format(dirpath=dirpath)
            self.asok_command(mds_rank, command, success_validator)
        command = "scrub_path {dirpath}".format(dirpath=dirpath)
        self.asok_command(mds_rank, command, success_validator)

        filepath = "{repo_path}/suites/fs/verify/validater/valgrind.yaml".format(
            repo_path=test_repo_path)
        if run_seq == 0:
            log.info("First run: flushing {filepath}".format(filepath=filepath))
            command = "flush_path {filepath}".format(filepath=filepath)
            self.asok_command(mds_rank, command, success_validator)
        command = "scrub_path {filepath}".format(filepath=filepath)
        self.asok_command(mds_rank, command, success_validator)

        filepath = "{repo_path}/suites/fs/basic/clusters/fixed-3-cephfs.yaml". \
            format(repo_path=test_repo_path)
        command = "scrub_path {filepath}".format(filepath=filepath)
        self.asok_command(mds_rank, command,
                          lambda j, r: self.json_validator(j, r, "performed_validation",
                                                           False))

        if run_seq == 0:
            log.info("First run: flushing base dir /")
            command = "flush_path /"
            self.asok_command(mds_rank, command, success_validator)
        command = "scrub_path /"
        self.asok_command(mds_rank, command, success_validator)

        new_dir = "{repo_path}/new_dir_{i}".format(repo_path=repo_path, i=run_seq)
        test_new_dir = "{repo_path}/new_dir_{i}".format(repo_path=test_repo_path,
                                                        i=run_seq)
        self.mount_a.run_shell(["mkdir", new_dir])
        command = "flush_path {dir}".format(dir=test_new_dir)
        self.asok_command(mds_rank, command, success_validator)

        new_file = "{repo_path}/new_file_{i}".format(repo_path=repo_path,
                                                     i=run_seq)
        test_new_file = "{repo_path}/new_file_{i}".format(repo_path=test_repo_path,
                                                          i=run_seq)
        self.mount_a.write_n_mb(new_file, 1)

        command = "flush_path {file}".format(file=test_new_file)
        self.asok_command(mds_rank, command, success_validator)

        # check that scrub fails on errors
        ino = self.mount_a.path_to_ino(new_file)
        rados_obj_name = "{ino:x}.00000000".format(ino=ino)
        command = "scrub_path {file}".format(file=test_new_file)

        # Missing parent xattr -> ENODATA
        self.fs.rados(["rmxattr", rados_obj_name, "parent"], pool=self.fs.get_data_pool_name())
        self.asok_command(mds_rank, command,
                          lambda j, r: self.json_validator(j, r, "return_code", -errno.ENODATA))

        # Missing object -> ENOENT
        self.fs.rados(["rm", rados_obj_name], pool=self.fs.get_data_pool_name())
        self.asok_command(mds_rank, command,
                          lambda j, r: self.json_validator(j, r, "return_code", -errno.ENOENT))

        command = "flush_path /"
        self.asok_command(mds_rank, command, success_validator)

    def test_scrub_repair(self):
        mds_rank = 0
        test_dir = "scrub_repair_path"

        self.mount_a.run_shell(["sudo", "mkdir", test_dir])
        self.mount_a.run_shell(["sudo", "touch", "{0}/file".format(test_dir)])
        dir_objname = "{:x}.00000000".format(self.mount_a.path_to_ino(test_dir))

        self.mount_a.umount_wait()

        # flush journal entries to dirfrag objects, and expire journal
        self.fs.mds_asok(['flush', 'journal'])
        self.fs.mds_stop()

        # remove the dentry from dirfrag, cause incorrect fragstat/rstat
        self.fs.rados(["rmomapkey", dir_objname, "file_head"],
                      pool=self.fs.get_metadata_pool_name())

        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        # fragstat indicates the directory is not empty, rmdir should fail
        with self.assertRaises(CommandFailedError) as ar:
            self.mount_a.run_shell(["sudo", "rmdir", test_dir])
        self.assertEqual(ar.exception.exitstatus, 1)

        self.asok_command(mds_rank, "scrub_path /{0} repair".format(test_dir),
                          lambda j, r: self.json_validator(j, r, "return_code", 0))

	# wait a few second for background repair
	time.sleep(10)

	# fragstat should be fixed
	self.mount_a.run_shell(["sudo", "rmdir", test_dir])

    @staticmethod
    def json_validator(json_out, rc, element, expected_value):
        if rc != 0:
            return False, "asok command returned error {rc}".format(rc=rc)
        element_value = json_out.get(element)
        if element_value != expected_value:
            return False, "unexpectedly got {jv} instead of {ev}!".format(
                jv=element_value, ev=expected_value)
        return True, "Succeeded"

    def asok_command(self, mds_rank, command, validator):
        log.info("Running command '{command}'".format(command=command))

        command_list = command.split()

        # we just assume there's an active mds for every rank
        mds_id = self.fs.get_active_names()[mds_rank]
        proc = self.fs.mon_manager.admin_socket('mds', mds_id,
                                                command_list, check_status=False)
        rout = proc.exitstatus
        sout = proc.stdout.getvalue()

        if sout.strip():
            jout = json.loads(sout)
        else:
            jout = None

        log.info("command '{command}' got response code " +
                 "'{rout}' and stdout '{sout}'".format(
                     command=command, rout=rout, sout=sout))

        success, errstring = validator(jout, rout)

        if not success:
            raise AsokCommandFailedError(command, rout, jout, errstring)

        return jout

    def clone_repo(self, client_mount, path):
        repo = "ceph-qa-suite"
        repo_path = os.path.join(path, repo)
        client_mount.run_shell(["mkdir", "-p", path])

        try:
            client_mount.stat(repo_path)
        except CommandFailedError:
            client_mount.run_shell([
                "git", "clone", '--branch', 'giant',
                "http://github.com/ceph/{repo}".format(repo=repo),
                "{path}/{repo}".format(path=path, repo=repo)
            ])

        return repo_path


class AsokCommandFailedError(Exception):
    """
    Exception thrown when we get an unexpected response
    on an admin socket command
    """

    def __init__(self, command, rc, json_out, errstring):
        self.command = command
        self.rc = rc
        self.json = json_out
        self.errstring = errstring

    def __str__(self):
        return "Admin socket: {command} failed with rc={rc}," + \
               "json output={json}, because '{es}'".format(
                   command=self.command, rc=self.rc,
                   json=self.json, es=self.errstring)
