"""
Use this task to check that parity shards in an EC pool
match the output produced by the Ceph Erasure Code Tool.
"""

import logging
import json
import os
import atexit
import tempfile
import shutil
import time
from io import StringIO
from io import BytesIO
from typing import Dict, List, Any
from tasks import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)
DATA_SHARD_FILENAME = 'ec-obj'


class ErasureCodeObject:
    """
    Store data relating to an RBD erasure code object,
    including the object's erasure code profile as well as
    the data for k + m shards.
    """

    def __init__(self, oid: str, snapid: int, ec_profile: Dict[str, Any]):
        self.oid = oid
        self.snapid = snapid
        self.uid = oid + '_' + str(snapid)
        self.ec_profile = ec_profile
        self.k = int(ec_profile["k"])
        self.m = int(ec_profile["m"])
        self.shards = [None] * (self.k + self.m)
        self.jsons = [None] * (self.k + self.m)
        self.osd_map = [None] * (self.k + self.m)
        self.object_size = None

    def get_ec_tool_profile(self) -> str:
        """
        Return the erasure code profile associated with the object
        in string format suitable to be fed into the erasure code tool
        """
        profile_str = ''
        for key, value in self.ec_profile.items():
            profile_str += str(key) + '=' + str(value) + ','
        return profile_str[:-1]

    def get_want_to_encode_str(self) -> str:
        """
        Return a comma seperated string of the shards
        to be produced by the EC tool encode
        This includes k + m shards as tool also produces the data shards
        """
        nums = "0,"
        for i in range(1, self.k + self.m):
            nums += (str(i) + ",")
        return nums[:-1]

    def update_shard(self, index: int, data: bytearray):
        """
        Update a shard at the specified index
        """
        self.shards[index] = data

    def get_data_shards(self) -> List[bytearray]:
        """
        Return an ordered list of data shards.
        """
        return self.shards[:self.k]

    def get_parity_shards(self) -> List[bytearray]:
        """
        Return an ordered list of parity shards.
        """
        return self.shards[self.k:self.k + self.m]

    def write_data_shards_to_file(self, filepath: str):
        """
        Write the data shards to files for
        consumption by Erasure Code tool.
        """
        shards = self.get_data_shards()
        assert None not in shards, "Object is missing data shards"
        data_out = bytearray()
        for shard in shards:
            data_out += shard
        with open(filepath, "wb") as binary_file:
            binary_file.write(data_out)
            binary_file.close()

    def delete_shards(self):
        """
        Free up memory used by the shards for this object
        """
        self.shards = [None] * (self.k + self.m)

    def does_shard_match_file(self, index: int, file_in: str) -> bool:
        """
        Compare shard at specified index with contents of the supplied file
        Return True if they match, False otherwise
        """
        shard_data = self.shards[index]
        file_content = bytearray()
        with open(file_in, "rb") as binary_file:
            b = binary_file.read()
            file_content.extend(b)
        return shard_data == file_content

    def compare_parity_shards_to_files(self, filepath: str) -> bool:
        """
        Check the object's parity shards match the files generated
        by the erasure code tool. Return True if they match, False otherwise.
        """
        do_all_shards_match = True
        for i in range(self.k, self.k + self.m):
            shard_filename = filepath + '.' + str(i)
            match = self.does_shard_match_file(i, shard_filename)
            if match:
                log.debug("Shard %i in object %s matches file content",
                          i,
                          self.uid)
            else:
                log.debug("MISMATCH: Shard %i in object "
                          "%s does not match file content",
                          i,
                          self.uid)
                do_all_shards_match = False
        return do_all_shards_match


class ErasureCodeObjects:
    """
    Class for managing objects of type ErasureCodeObject
    Constuctor takes an optional list of oids to check,
    if specified, any objects not on the list will not be checked
    """
    def __init__(self, manager: ceph_manager.CephManager,
                 config: Dict[str, Any] = None):
        self.manager = manager
        self.os_tool = ObjectStoreTool(manager)
        self.pools_json = self.manager.get_osd_dump_json()["pools"]
        self.objects_to_include = config.get('object_list', None)
        self.pools_to_check = config.get('pools_to_check', None)
        self.ec_profiles = {}
        self.objects = []

    def get_object_by_uid(self, object_id: str) -> ErasureCodeObject:
        """
        Return the ErasureCodeObject corresponding to the supplied
        UID if it exists
        """
        for obj in self.objects:
            if obj.uid == object_id:
                return obj
        return None

    def create_ec_object(self, oid: str, snapid: int,
                         ec_profile: Dict[str, Any]):
        """
        Create a new ErasureCodeObject and add it to the list
        """
        ec_object = ErasureCodeObject(oid, snapid, ec_profile)
        self.objects.append(ec_object)
        return ec_object

    def update_object_shard(self, object_id: str,
                            shard_id: int, data: bytearray):
        """
        Update a shard of an existing ErasureCodeObject
        """
        ec_object = self.get_object_by_uid(object_id)
        ec_object.update_shard(shard_id, data)

    def get_object_uid(self, info_dump: Dict[str, Any]):
        """
        Returns a unique ID for an object, a combination of the oid and snapid
        """
        return info_dump["oid"] + "_" + str(info_dump["snapid"])

    def is_object_in_pool_to_be_checked(self, object_json: Dict[str, Any]):
        """
        Check if an object is a member of pool
        that is to be checked for consistency.
        """
        if not self.pools_to_check:
            return True  # All pools to be checked
        object_json = object_json[1]
        shard_pool_id = object_json["pool"]
        for pool_json in self.pools_json:
            if shard_pool_id == pool_json["pool"]:
                if pool_json["pool_name"] in self.pools_to_check:
                    return True
        return False

    def is_object_in_ec_pool(self, object_json: Dict[str, Any]):
        """
        Check if an object is a member of an EC pool or not.
        """
        is_object_in_ec_pool = False
        object_json = object_json[1]
        shard_pool_id = object_json["pool"]
        for pool_json in self.pools_json:
            if shard_pool_id == pool_json["pool"]:
                pool_type = pool_json['type']  # 1 for rep, 3 for ec
                if pool_type == 3:
                    is_object_in_ec_pool = True
                    break
        return is_object_in_ec_pool

    def get_ec_profile_for_pool(self,
                                pool_id: int) -> Dict[str, Any]:
        """
        Find and return the EC profile for a given pool.
        Cache it locally if not already stored.
        """
        if pool_id in self.ec_profiles:
            return self.ec_profiles[pool_id]
        for pool_json in self.pools_json:
            if pool_id == pool_json["pool"]:
                ec_profile_name = self.manager.get_pool_property(
                    pool_json["pool_name"], "erasure_code_profile")
                ec_profile_json = self.manager.raw_cluster_cmd(
                    "osd",
                    "erasure-code-profile",
                    "get",
                    ec_profile_name,
                    "--format=json"
                )
                break
        try:
            ec_profile = json.loads(ec_profile_json)
            self.ec_profiles[pool_id] = ec_profile
        except ValueError as e:
            log.error("Failed to parse object dump to JSON: %s", e)
        return self.ec_profiles[pool_id]

    def process_object_shard_data(self, ec_object: ErasureCodeObject):
        """
        Use the Object Store tool to get object info and the bytes data
        for all shards in an object
        """
        for (json_str, osd_id) in zip(ec_object.jsons, ec_object.osd_map):
            shard_info = self.os_tool.get_shard_info_dump(osd_id, json_str)
            shard_data = self.os_tool.get_shard_bytes(osd_id, json_str)
            shard_index = shard_info["id"]["shard_id"]
            ec_object.object_size = shard_info["hinfo"]["total_chunk_size"]
            ec_object.update_shard(shard_index, shard_data)

    def process_object_json(self, osd_id: int, object_json: List[Any]):
        """
        Create an ErasureCodeObject from JSON list output
        Don't populate data and other info yet as that requires
        slow calls to the ObjectStore tool
        """
        json_str = json.dumps(object_json)
        object_json = object_json[1]
        object_oid = object_json["oid"]
        object_snapid = object_json["snapid"]
        object_uid = object_oid + '_' + str(object_snapid)
        ec_object = self.get_object_by_uid(object_uid)
        if (self.objects_to_include and
           object_oid not in self.objects_to_include):
            return
        if not ec_object:
            shard_pool_id = object_json["pool"]
            ec_profile = self.get_ec_profile_for_pool(shard_pool_id)
            ec_object = self.create_ec_object(object_oid,
                                              object_snapid, ec_profile)
        shard_id = object_json["shard_id"]
        ec_object.osd_map[shard_id] = osd_id
        ec_object.jsons[shard_id] = json_str


class ObjectStoreTool:
    """
    Interface for running the Object Store Tool, contains functions
    for retreiving information and data from OSDs
    """
    def __init__(self, manager: ceph_manager.CephManager):
        self.manager = manager
        self.fspath = self.manager.get_filepath()

    def run_objectstore_tool(self, osd_id: int, cmd: List[str],
                             string_out: bool = True):
        """
        Run the ceph objectstore tool.
        Execute the objectstore tool with the supplied arguments
        in cmd on the machine where the specified OSD lives.
        """
        remote = self.manager.find_remote("osd", osd_id)
        data_path = self.fspath.format(id=osd_id)
        if self.manager.cephadm:
            return shell(
                self.manager.ctx,
                self.manager.cluster,
                remote,
                args=[
                    "ceph-objectstore-tool",
                    "--err-to-stderr",
                    "--no-mon-config",
                    "--data-path",
                    data_path
                ]
                + cmd,
                name="osd" + str(osd_id),
                wait=True,
                check_status=False,
                stdout=StringIO() if string_out else BytesIO(),
                stderr=StringIO()
            )
        elif self.manager.rook:
            assert False, "not implemented"
        else:
            return remote.run(
                args=[
                    "sudo",
                    "adjust-ulimits",
                    "ceph-objectstore-tool",
                    "--err-to-stderr",
                    "--no-mon-config",
                    "--data-path",
                    data_path
                ]
                + cmd,
                wait=True,
                check_status=False,
                stdout=StringIO() if string_out else BytesIO(),
                stderr=StringIO()
            )

    def get_ec_data_objects(self, osd_id: int) -> List[Any]:
        """
        Return list of erasure code objects living on this OSD.
        """
        objects = []
        proc = self.run_objectstore_tool(osd_id, ["--op", "list"])
        stdout = proc.stdout.getvalue()
        if not stdout:
            log.error("Objectstore tool failed with error "
                      "when retreiving list of data objects")
        else:
            for line in stdout.split('\n'):
                if line:
                    try:
                        shard = json.loads(line)
                        if self.is_shard_part_of_ec_object(shard):
                            objects.append(shard)
                    except ValueError as e:
                        log.error("Failed to parse shard list to JSON: %s", e)
        return objects

    def get_shard_info_dump(self, osd_id: int,
                            json_str: str) -> Dict[str, Any]:
        """
        Return the JSON formatted shard information living on specified OSD.
        json_str is the line of the string produced by the OS tool 'list'
        command which corresponds to a given shard
        """
        shard_info = {}
        proc = self.run_objectstore_tool(osd_id, ["--json", json_str, "dump"])
        stdout = proc.stdout.getvalue()
        if not stdout:
            log.error("Objectstore tool failed with error "
                      "when dumping object info.")
        else:
            try:
                shard_info = json.loads(stdout)
            except ValueError as e:
                log.error("Failed to parse object dump to JSON: %s", e)
        return shard_info

    def get_shard_bytes(self, osd_id: int, object_id: str) -> bytearray:
        """
        Return the contents of the shard living on the specified OSD as bytes.
        """
        shard_bytes = None
        proc = self.run_objectstore_tool(osd_id,
                                         [object_id, "get-bytes"], False)
        stdout = proc.stdout.getvalue()
        if not stdout:
            log.error("Objectstore tool failed to get shard bytes.")
        else:
            shard_bytes = bytearray(stdout)
        return shard_bytes

    def is_shard_part_of_ec_object(self, shard: List[Any]):
        """
        Perform some checks on a shard to determine if it's actually a shard
        in a valid EC object that should be checked for consistency. Attempts
        to exclude scrub objects, trash and other various metadata objects."""
        pgid = shard[0]
        shard_info = shard[1]
        object_is_sharded = 's' in pgid
        shard_has_oid = shard_info["oid"] != ''
        shard_has_pool = shard_info["pool"] >= 0
        shard_is_not_trash = "trash" not in shard_info["oid"]
        shard_is_not_info = "info" not in shard_info["oid"]
        if (object_is_sharded and shard_has_oid and
           shard_has_pool and shard_is_not_trash and
           shard_is_not_info):
            return True
        else:
            return False


class ErasureCodeTool:
    """
    Interface for running the Ceph Erasure Code Tool
    """
    def __init__(self, manager: ceph_manager.CephManager, osd: int):
        self.manager = manager
        self.remote = self.manager.find_remote("osd", osd)

    def run_erasure_code_tool(self, cmd: List[str]):
        """
        Run the ceph erasure code tool with the arguments in the supplied list
        """
        args = ["sudo", "adjust-ulimits", "ceph-erasure-code-tool"] + cmd
        if self.manager.cephadm:
            return shell(
                self.manager.ctx,
                self.manager.cluster,
                self.remote,
                args=args,
                name=None,
                wait=True,
                check_status=False,
                stdout=StringIO(),
                stderr=StringIO(),
            )
        elif self.manager.rook:
            assert False, "not implemented"
        else:
            return self.remote.run(
                args=args,
                wait=True,
                check_status=False,
                stdout=StringIO(),
                stderr=StringIO(),
            )

    def calc_chunk_size(self, profile: str, object_size: str) -> int:
        """
        Returns the chunk size for the given profile and object size
        """
        cmd = ["calc-chunk-size", profile, object_size]
        proc = self.run_erasure_code_tool(cmd)
        stdout = proc.stdout.getvalue()
        if not stdout:
            log.error("Erasure Code tool failed to calculate chunk size.")
        return stdout

    def encode(self, profile: str, stripe_unit: int,
               file_nums: str, filepath: str):
        """
        Encode the specified file using the erasure code tool
        Output will be written to files in the same directory
        """
        cmd = ["encode", profile, str(stripe_unit), file_nums, filepath]
        proc = self.run_erasure_code_tool(cmd)
        if proc.exitstatus != 0:
            log.error("Erasure Code tool failed to encode.")

    def decode(self, profile: str, stripe_unit: int,
               file_nums: str, filepath: str):
        """
        Decode the specified file using the erasure code tool
        Output will be written to files in the same directory
        """
        cmd = ["decode", profile, str(stripe_unit), file_nums, filepath]
        proc = self.run_erasure_code_tool(cmd)
        if proc.exitstatus != 0:
            log.error("Erasure Code tool failed to decode.")


def shell(ctx: any, cluster_name: str, remote: any,
          args: List[str], name: str = None, **kwargs: any):
    """
    Interface for running commands on cephadm clusters
    """
    extra_args = []
    if name:
        extra_args = ['-n', name]
    return remote.run(
        args=[
            'sudo',
            ctx.cephadm,
            '--image', ctx.ceph[cluster_name].image,
            'shell',
        ] + extra_args + [
            '--fsid', ctx.ceph[cluster_name].fsid,
            '--',
        ] + args,
        **kwargs
    )


def get_tmp_directory():
    """
    Returns a temporary directory name that will be used to store shard data
    Includes the PID so different instances can be run in parallel
    """
    tmpdir = (tempfile.gettempdir() +
              '/consistency-check-' + str(os.getpid()) + '/')
    return tmpdir


def print_summary(consistent: List[str],
                  inconsistent: List[str]):
    """
    Print a summary including counts of objects checked
    and a JSON-formatted lists of consistent and inconsistent objects.
    """
    log.info("Consistent objects counted: %i", len(consistent))
    log.info("Inconsistent objects counted %i", len(inconsistent))
    log.info("Total objects checked: %i", len(consistent) + len(inconsistent))
    if consistent:
        out = '[' + ','.join("'" + str(o) + "'" for o in consistent) + ']'
        log.info("Consistent objects: %s", out)
    if inconsistent:
        out = '[' + ','.join("'" + str(o) + "'" for o in inconsistent) + ']'
        log.info("Objects with a mismatch: %s", out)


def handle_mismatch(assert_on_mismatch: bool):
    """
    Raise a RunTimeError if assert_on_mismatch is set,
    otherwise just log an error.
    """
    err = "Shard mismatch detected."
    if assert_on_mismatch:
        raise RuntimeError(err)
    log.error(err)


def load_objects_to_check(objects_to_check: str) -> List[str]:
    """
    Attempt to parse the object_list config option
    takes a stringified JSON list as an argument
    """
    object_list = None
    if objects_to_check is None:
        return object_list
    try:
        object_list = json.loads(objects_to_check)
    except ValueError:
        log.error("Failed to parse object_list config option,"
                  "input should be a JSON formatted list")
    return object_list


def exit_handler(manager: ceph_manager.CephManager,
                 osds: List[Dict[str, Any]], retain_files: bool = False):
    """
    Revive any OSDs that were killed during the task and
    clean up any temporary files. Optionally retain files for
    debug if specified by the config
    """
    for osd in osds:
        osd_id = osd["osd"]
        manager.revive_osd(osd_id, skip_admin_check=True)

    dir_exists = os.path.isdir(get_tmp_directory())
    if dir_exists and not retain_files:
        shutil.rmtree(get_tmp_directory())


def task(ctx, config: Dict[str, Any]):
    """
    Gathers information about EC objects living on the OSDs, then
    gathers the shards the shard data using the ObjectStore tool.
    Runs the data shards through the EC tool and verifies the encoded
    output matches the parity shards on the OSDs.

    Accepts the following optional config options:

    ec_parity_consistency:
        retain_files: <bool> - Keep files gathered during the test in /var/tmp/
        assert_on_mismatch: <bool> - Whether to count a mismatch as a failure
        max_run_time: <int> - Max amount of time to run the tool for in seconds
        object_list: <List[str]> - OID list of which objects to check
        pools_to_check: <List[str]> - List of pool names to check for objects
    """

    if config is None:
        config = {}

    log.info("Python Process ID: %i", os.getpid())
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild("ceph_manager")
    )

    cephadm_not_supported = "Tool not supported for use with cephadm clusters"
    assert not manager.cephadm, cephadm_not_supported

    retain_files = config.get('retain_files', False)
    max_time = int(config.get('max_run_time', 3600))
    assert_on_mismatch = config.get('assert_on_mismatch', True)

    osds = manager.get_osd_dump()
    os_tool = ObjectStoreTool(manager)
    ec_tool = ErasureCodeTool(manager, osds[0]["osd"])
    ec_objects = ErasureCodeObjects(manager, config)
    start_time = time.time()

    atexit.register(exit_handler, manager, osds, retain_files)

    # Loop through every OSD, storing each object shard in an EC object
    # Objects not in EC pools or the object_list will be ignored
    for osd in osds:
        osd_id = osd["osd"]
        manager.kill_osd(osd_id)
        data_objects = os_tool.get_ec_data_objects(osd_id)
        if data_objects:
            for obj in data_objects:
                is_in_ec_pool = ec_objects.is_object_in_ec_pool(obj)
                check_pool = ec_objects.is_object_in_pool_to_be_checked(obj)
                if is_in_ec_pool and check_pool:
                    ec_objects.process_object_json(osd_id, obj)
                else:
                    log.debug("Object not in pool to be checked, skipping.")

    # Now compute the parities for each object
    # and verify they match what the EC tool produces
    consistent, inconsistent = [], []
    for ec_object in ec_objects.objects:
        time_elapsed = time.time() - start_time
        if time_elapsed > max_time:
            log.info("%i seconds elapsed, stopping "
                     "due to time limit.", time_elapsed)
            break
        ec_objects.process_object_shard_data(ec_object)
        # Create dir and write out shards
        object_uid = ec_object.uid
        object_dir = get_tmp_directory() + object_uid + '/'
        object_filepath = object_dir + DATA_SHARD_FILENAME
        os.makedirs(object_dir)
        ec_object.write_data_shards_to_file(object_filepath)
        # Encode the shards and output to the object dir
        want_to_encode = ec_object.get_want_to_encode_str()
        ec_profile = ec_object.get_ec_tool_profile()
        object_size = ec_object.object_size
        ec_tool.encode(ec_profile,
                       object_size,
                       want_to_encode,
                       object_filepath)
        # Compare stored parities to EC tool output
        match = ec_object.compare_parity_shards_to_files(object_filepath)
        if match:
            consistent.append(object_uid)
        else:
            inconsistent.append(object_uid)
        # Free up memory consumed by shards
        ec_object.delete_shards()

    # Print a summary of matches, raise a RunTimeError if required
    print_summary(consistent, inconsistent)
    if len(inconsistent) > 0:
        handle_mismatch(assert_on_mismatch)
