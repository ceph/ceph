#!/usr/bin/python3

"""
Last Revision: 2026-11-05
Version: 5.5

## Major version change log
v5.3: Add client name/keyring support
v5.2: Added prometheus exporter
v5: Now multi-threaded ( separate submit / process threads )
v4: Now storing results in RADOS
v3: Now using async io
v2: Now in Python, performing RADOS stat on the backing objects
v1: Initial shell script, has performance limitations due to long running
    listing processes.

Performs a Rados Gateway Gap analysis

Over the years, there have been a couple of bugs which resulted in backing
user data being deleted for Ceph RGW S3 objects.  It's rare, but has happend.

There is a shell script tool available ( that I also wrote ) but it has a few
drawbacks:
- It must wait for a complete `radosgw-admin bucket radoslist` to complete
- It must wait for a complete `rados ls` on the bucket data pool to complete
- Then it compares the listings looking for gaps in `rados ls`
- It's prone to false positives which can be tedious for mere mortals to
  verify.

-- This can take a LONG time on large clusters, and it doesn't generate any
   usable output until both lists are complete and the comparison begins.

This python version attempts to address these shortcoming in the following way:
1. It runs on a per-bucket basis and generates usable output for each bucket
   along the way.
2. When ran without any bucket constraints ( either bucket list, or list file ),
   this script maintains state synchronization using dedicated objects in the
   bucket index pool in the 'rgw-gap-list' namespace (by default).
3. Since the state is synchronized via Ceph RADOS... multiple instances can be
   running in parallel, even across different hosts!
4. This script can also be ran with the '-r' option to generate a report of
   current running hosts, and state per bucket.
5. This script can verify its own results by passing the '-x' flag.

Usage can be had by passing '--help' to the script.

## Tips:
- I recommend using '-vv' the first time ( or any time ) to see what is going
  on.
- Get a report of current host activity and bucket scan states by passing '-r'
- You can force a rescan by passing '-a #' with a value in seconds to consider
  the prior scan stale ( after the # seconds value ) - use 1 to force rescan
  everything.
- You can wipe out the synchronized state data by passing '-d'
- Passing any bucket constraints ( -b or -l ) ignores the synchronized state!!
  -- NOTE -- Read the above line again.
- The bucket data pool(s) and the sync state pool can be overridden with '-p'
  and '-s', respectively.
  -- NOTE -- If you don't use the same pools on all instances of this script,
  the synchronized state will not work.
- You can verify the RADOS stored results with the '-x' parameter.
- You can limit the objects to only those matching a given prefix using the
  '-m' parameter.
- The '-g', '-r' and '-x' parameters support json output by adding '-j' flag.

## Known Issues:
- If two separate instances attempt to start processing the same bucket in
  a very narrow window ( < 50ms, but the exact value depends on a lot of
  variables ), they may both succeed in starting the process, instead of one
  winning the race to push the sync object omap update and blocking the other.
  This has no real impact aside from doubling any gap objects listed in the
  results and having two threads processing the same bucket.

Enjoy!
"""

import argparse
from datetime import datetime
import hashlib
import io
import json
import logging
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from types import FrameType
from typing import List, Dict, Optional, Tuple, Union
import rados

LOG_LEVELS = [ 50, 30, 20, 10 ]

def signal_handler(sig: int, _frame: Optional[FrameType]) -> None:
    """
    A handler for SIGINT and SIGTERM signals.
    """
    print(f'Received {sig}, Terminating')
    sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class CephClusterConnection:
    """
    A context manager to handle connecting to and disconnecting from a
    Ceph RADOS cluster, ensuring resources are cleaned up properly.
    """
    def __init__(self, ceph_conf: str = '/etc/ceph/ceph.conf', 
                 client_name: str = None, 
                 client_keyring: str = None, 
                 pool_names: List = None,
                 sync_pool: str = None,
                 namespace: str = None) -> None:
        self.ceph_conf = ceph_conf
        self.cluster = None
        self.client_name = client_name
        self.client_keyring = client_keyring
        self.pool_names = pool_names
        self.pool_ioctl = []
        self.sync_pool = sync_pool
        self.sync_ioctl = None
        self.namespace = namespace

    def __enter__(self):
        """
        Called when entering the 'with' block.

        Establishes connection to the Ceph cluster and opens IO contexts for
        the specified pools.

        Returns:
            CephClusterConnection: The instance itself for use in 'with' statements
        """
        self.cluster = rados.Rados(
            conffile=self.ceph_conf,
            name=self.client_name,
            conf={'keyring': self.client_keyring})
        try:
            self.cluster.connect()
            logger.info("Successfully connected to the Ceph cluster.")
        except rados.Error as e:
            logger.critical("Failed to connect to the Ceph cluster: %s", e)
            raise RuntimeError(f'Failed to connect to the Ceph cluster: {e}') from e

        logger.info("Opening ioctl for sync pool %s", self.sync_pool)
        try:
            self.sync_ioctl = self.cluster.open_ioctx(self.sync_pool)
        except rados.ObjectNotFound:
            logger.critical("Sync Pool %s not present.  Exiting.", self.sync_pool)
            sys.exit(1)
        else:
            if len(self.namespace) > 0:
                self.sync_ioctl.set_namespace(self.namespace)

        for pool_name in self.pool_names:
            logger.info("Opening ioctl for pool %s", pool_name)
            try:
                self.pool_ioctl.append(self.cluster.open_ioctx(pool_name))
            except rados.ObjectNotFound:
                logger.error("Pool %s not present, skipping.", pool_name)
            else:
                if re.search(r"\.non-ec$",pool_name):
                    logger.info("Pool %s, adding namespace 'multipart'", pool_name)
                    self.pool_ioctl.append(self.cluster.open_ioctx(pool_name))
                    self.pool_ioctl[len(self.pool_ioctl)-1].set_namespace('multipart')

        if len(self.pool_ioctl)==0:
            logger.critical("None of the listed pools exist!  Exiting!")
            sys.exit(1)

        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Called when exiting the 'with' block, ensuring safe shutdown.

        Closes all IO contexts and disconnects from the Ceph cluster.

        Args:
            exc_type: Exception type (if any)
            exc_val: Exception value (if any)
            exc_tb: Exception traceback (if any)
        """
        if self.cluster:
            for ioctx in self.pool_ioctl:
                try:
                    ioctx.close()
                except:
                    pass
            try:
                self.sync_ioctl.close()
            except:
                pass
            try:
                self.cluster.shutdown()
            except:
                pass
            logger.info("Connection to the Ceph cluster closed.")

    def null_cb(self, *extra_args) -> None:
        """
        A null callback for aio stat commands. The callback doesn't contain anything
        useful for our purposes.

        Args:
            *extra_args: Additional arguments (unused)
        """
        return None

    def async_stat_datapool_object(self, object_name: str = "", idx: Optional[int] = None) -> List:
        """
        Perform an asynchronous stat command to the RADOS data pool(s) to check if the object exists.

        Since there can be many data bearing pools (even by default, `.data` and `.non-ec`)
        Based on the value of the idx variable passed to the function, it will selectively check:
        - None: all data pools
        - 0: First data pool only
        - >=1: Stat all pools from this index and all remaining pools.

        Args:
            object_name (str): Name of the object to stat
            idx (Optional[int]): Index to determine which pools to check (None for all, 0 for first only, >=1 for starting from that index)

        Returns:
            List: List of asynchronous stat operations
        """
        if not self.cluster:
            logger.critical("Cluster is not connected.")
            raise RuntimeError("Cluster is not connected.")

        idxstart = 0
        idxend = len(self.pool_ioctl)

        # iterate over each pool attempting to stat the object.
        stat_ops = []
        if idx is not None:
            if idx == 0:
                idxend = 1
            elif idx >= 1 and len(self.pool_ioctl) > 1:
                idxstart = idx
            else:
                return stat_ops

        for myidx in range(idxstart,idxend):
            stat_ops.append(self.pool_ioctl[myidx].aio_stat(object_name,self.null_cb))

        return stat_ops

    def write_syncpool_object_data(self, object_name: str = "", contents: Union[str,bytes] = "") -> None:
        """
        Write data to an object in the sync pool.

        Args:
            object_name (str): Name of the object to write
            contents (Union[str, bytes]): Contents to write to the object
        """
        try:
            self.sync_ioctl.write_full(object_name, contents.encode("utf-8"))
        except AttributeError:
            self.sync_ioctl.write_full(object_name, contents)

    def read_syncpool_object_data(self, object_name: str = "") -> Union[ Dict, str ]:
        """
        Read data from an object in the sync pool.

        Args:
            object_name (str): Name of the object to read

        Returns:
            Union[Dict, str]: The decoded JSON data or raw string if not valid JSON
        """
        data = self.sync_ioctl.read(object_name).decode("utf-8")
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return data

    def stat_syncpool_object(self, object_name: str = "") ->  bool:
        """
        Synchronous stat of an object in the sync pool.

        Args:
            object_name (str): Name of the object to stat

        Returns:
            bool: True if object exists, False otherwise
        """
        try:
            self.sync_ioctl.stat(object_name)
            logger.debug("[STAT] Object exists: %s", object_name)
            return True
        except rados.ObjectNotFound:
            logger.debug("[STAT] Object does not exist: %s", object_name)
            return False

    def remove_syncpool_object(self, object_name: str = "") -> None:
        """
        Remove an object in the sync pool.

        Args:
            object_name (str): Name of the object to remove
        """
        try:
            self.sync_ioctl.remove_object(object_name)
            logger.debug("Removed %s", object_name)
        except rados.ObjectNotFound:
            logger.debug("Removal unnecessary, object %s not present.", object_name)

    def write_syncpool_omap(self, object_name: str = "", key_name: str = "", contents: str = "") -> None:
        """
        Write omap to an object in the sync pool.

        Args:
            object_name (str): Name of the object to write omap data to
            key_name (str): Name of the omap key
            contents (str): Contents to store with the key
        """
        with rados.WriteOpCtx() as write_op:
            self.sync_ioctl.set_omap(write_op,(key_name, ),( contents, ))
            self.sync_ioctl.operate_write_op(write_op, object_name)

    def read_syncpool_omap_vals(self, object_name: str) -> Dict:
        """
        Read all omap key/value pairs from an object in the sync pool

        Args:
            object_name (str): Name of the object to read omap data from

        Returns:
            Dict: Dictionary of key, value pairs
        """
        kvdata = {}
        last_omap_key = ""
        batch_size = 5000

        with rados.ReadOpCtx() as read_op:
            while True:
                omap_iterator, ret = self.sync_ioctl.get_omap_vals(
                    read_op,
                    start_after=last_omap_key,
                    filter_prefix="",
                    max_return=batch_size
                )

                if not ret==0:
                    logger.critical("Failed to setup omap data read.")
                    sys.exit(1)

                try:
                    self.sync_ioctl.operate_read_op(read_op, object_name)
                except rados.ObjectNotFound:
                    logger.error("Missing Object %s", object_name)
                    break

                omap_batch = list(omap_iterator)

                if not omap_batch:
                    break

                for k,v in omap_batch:
                    try:
                        kvdata[k] = json.loads(v)
                    except (json.JSONDecodeError, TypeError):
                        kvdata[k] = v

                last_omap_key = omap_batch[-1][0]

                # If we received fewer keys than max_return, we've reached the end
                if len(omap_batch) < batch_size:
                    break

        return kvdata

    def read_syncpool_omap_vals_by_keys(self, object_name: str = "", key_list: Tuple = () ) -> Dict:
        """
        Read the omap key, value pair(s) for a given object and key(s) in the sync pool

        Args:
            object_name (str): Name of the object to write omap data to
            key_list (tuple): Name(s) of the omap key(s) to return

        Returns:
            Dict: Dictionary of the key, value pair(s)
        """
        results = {}
        with rados.ReadOpCtx() as read_op:
            omap_iterator, _ret = self.sync_ioctl.get_omap_vals_by_keys(read_op, key_list)
            try:
                self.sync_ioctl.operate_read_op(read_op,object_name)
            except rados.ObjectNotFound:
                logger.debug("Object %s not found.", object_name)
            else:
                results = {key: json.loads(val.decode("utf-8")) for key, val in dict(omap_iterator).items()}

        return results

    def remove_syncpool_omap_keys(self, object_name: str = "", key_list: List = None) -> None:
        """
        Remove omap key, value pair(s) from a given object in the sync pool

        Args:
            object_name (str): Name of the object to write omap data to
            key_list (list): Name of the omap key(s) to remove
        """
        if not isinstance(key_list,List):
            return

        with rados.WriteOpCtx() as write_op:
            self.sync_ioctl.remove_omap_keys(write_op, tuple(key_list))
            try:
                self.sync_ioctl.operate_write_op(write_op, object_name)
            except rados.ObjectNotFound:
                logger.info("Primary results object not found: %s", object_name)

# End class CephClusterConnection

class CephGapScanner:
    """
    A class to handle scanning a validating data consistency between the RADOS Gateway bucket index
    and the backing data pools.  Specifically, it checks that all RADOS objects referenced in the
    bucket index exist in RADOS.
    """
    def __init__(self, localceph: CephClusterConnection) -> None:
        self.bucket_gap_count = 0
        self.bucket_gap_results_obj_count = 0
        self.bucket_object_count = 0
        self.bucket_objects_processed = 0
        self.bucket_objects_with_gaps = 0
        self.ceph = localceph
        self.gap_header_data = None
        self.json = False
        self.match = ""
        self.max_age = 7 * 86400
        self.max_inflight = 15000
        self.processed_bucket_count = 0
        self.report_every_x_object_count = 10000
        self.results = {}
        self.shard_count = 1
        self.skipped_bucket_count = 0
        self.total_bucket_count = 0 

        self.MYPID = os.getpid()
        self.MYHOST = os.uname().nodename

        self.FIELD_SEPARATOR = "\xfe"
        self.BUCKET_LIST_COMMAND = ["radosgw-admin", "bucket", "list"]
        self.BUCKET_RADOSLIST_COMMAND = ['radosgw-admin', 'bucket', 'radoslist', f'--rgw-obj-fs={self.FIELD_SEPARATOR}']
        self.BUCKET_STATS_COMMAND = ['radosgw-admin', 'bucket', 'stats']
        self.RESULTS_OBJECT_NAME = "results"
        self.SYNC_OBJECT_NAME = "sync"

    def __enter__(self):
        """
        Called when entering the 'with' block.

        Returns:
            CephGapScanner: The instance itself for use in 'with' statements
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Called when exiting the 'with' block.

        Calls function to remove the sync state for this process from the cluster.

        Args:
            exc_type: Exception type (if any)
            exc_val: Exception value (if any)
            exc_tb: Exception traceback (if any)
        """
        self.rm_sync_state()

    def hash_bucket_name(self,bucket_name: str) -> int:
        """
        Calculate the hash of a bucket name for shard assignment.

        Args:
            bucket_name (str): Name of the bucket

        Returns:
            int: Hash value modulo shard count
        """
        digest = hashlib.sha256(bucket_name.encode("utf-8")).digest()
        return int.from_bytes(digest,byteorder="big") % self.shard_count

    def populate_sync_objects(self, shard_count: int = 1, bucket_count: int = 0) -> None:
        """
        Initialize synchronization objects in the cluster.

        Args:
            shard_count (int): Number of shards to use for bucket distribution
            bucket_count (int): Total count of buckets to process
        """
        self.shard_count=shard_count
        if not self.ceph.stat_syncpool_object(self.RESULTS_OBJECT_NAME):
            self.ceph.write_syncpool_object_data(self.RESULTS_OBJECT_NAME,"")

        if self.ceph.stat_syncpool_object(self.SYNC_OBJECT_NAME):
            logger.debug("Found primary sync object: %s", self.SYNC_OBJECT_NAME)
            self.touch_sync_state('', 0)
            bucket_metadata_header = self.ceph.read_syncpool_object_data(self.SYNC_OBJECT_NAME)
            running_hosts = self.get_other_running_hosts()
            logger.debug("Request %i shards, existing %i", shard_count, bucket_metadata_header["shard_count"])
            if shard_count <= ( bucket_metadata_header["shard_count"] * 1.5 ) or running_hosts:
                shard_count = self.shard_count = bucket_metadata_header["shard_count"]
            else:
                logger.info("No running hosts, and shard count is too low, resetting sync objects.")
                self.delete_sync_objects()
                self.populate_sync_objects(shard_count, bucket_count)
                return
        else:
            logger.info("Populating sync objects...")
            logger.debug("Creating primary sync object: %s", self.SYNC_OBJECT_NAME)
            sync_data = { "bucket_count": bucket_count, "shard_count": shard_count, "epoch": round(time.time(),3) }
            self.ceph.write_syncpool_object_data(self.SYNC_OBJECT_NAME,json.dumps(sync_data).encode("utf-8"))
            self.touch_sync_state('', 0)

        for i in range(shard_count):
            if self.ceph.stat_syncpool_object(f"{self.SYNC_OBJECT_NAME}.{i}"):
                logger.debug("Found sync object: %s.%i", self.SYNC_OBJECT_NAME, i)
            else:
                logger.debug("Creating sync object: %s.%i", self.SYNC_OBJECT_NAME, i)
                self.ceph.write_syncpool_object_data(f"{self.SYNC_OBJECT_NAME}.{i}",'')

        logger.info("Finished populating sync objects...")

    def touch_sync_state(self, bucket_name: str = '', rados_obj_count: int = 0, qsize: int = 0) -> None:
        """
        Update the synchronization state for this process.

        Args:
            bucket_name (str): Name of the current bucket being processed
            rados_obj_count (int): Count of RADOS objects processed
            qsize (int): The number of RADOS stat calls in queue
        """
        sync_state = { "epoch": round(time.time(),3), "current_bucket": bucket_name, "bucket_object_count": self.bucket_object_count,
                        "bucket_objects_processed": self.bucket_objects_processed, "rados_obj_count": rados_obj_count, 
                        "bucket_objects_with_gaps": self.bucket_objects_with_gaps, "gap_count": self.bucket_gap_count, 
                        "bucket_counter": self.processed_bucket_count, "queue_size": qsize, 
                        "total_buckets": self.total_bucket_count, "bucket_gap_results_obj_count": self.bucket_gap_results_obj_count }
        self.ceph.write_syncpool_omap(self.SYNC_OBJECT_NAME, f"{self.MYHOST}.{self.MYPID}" , json.dumps(sync_state))

    def rm_sync_state(self) -> None:
        """
        Remove the synchronization state for this process from the cluster.
        """
        self.ceph.remove_syncpool_omap_keys(self.SYNC_OBJECT_NAME, [ f"{self.MYHOST}.{self.MYPID}" ])

    def delete_sync_objects(self) -> None:
        """
        Delete all synchronization objects from the cluster.

        This method removes both primary and shard-level sync objects.
        """
        running_hosts = self.get_other_running_hosts()
        if len(running_hosts) > 0:
            logger.critical("There are active running processes. Exiting!")
            sys.exit(1)

        logger.critical("Deleting sync objects...")
        try:
            self.ceph.stat_syncpool_object(self.SYNC_OBJECT_NAME)
        except rados.ObjectNotFound:
            pass
        else:
            self.shard_count = self.ceph.read_syncpool_object_data(self.SYNC_OBJECT_NAME)["shard_count"]
            logger.info("Deleting primary sync object: %s", self.SYNC_OBJECT_NAME)
            self.ceph.remove_syncpool_object(self.SYNC_OBJECT_NAME)

        for i in range(self.shard_count):
            if self.ceph.stat_syncpool_object(f"{self.SYNC_OBJECT_NAME}.{i}"):
                logger.info("Deleting sync object: %s.%i", self.SYNC_OBJECT_NAME, i)
                self.ceph.remove_syncpool_object(f"{self.SYNC_OBJECT_NAME}.{i}")

        logger.critical("Finished deleting sync objects.")

    def delete_gap_objects(self, bucket_list: list = None) -> None:
        """
        Delete gap result objects for specified buckets.

        Args:
            bucket_list (list, optional): List of bucket names to delete results for.
                                          If None or empty, deletes all gap results.
        """
        remove_all = False
        if not isinstance(bucket_list, List):
            bucket_list = []
            remove_all = True

        running_hosts = self.get_other_running_hosts()
        if len(running_hosts) and remove_all:
            logger.critical("There are active running processes. Exiting!")
            sys.exit(1)

        if len(bucket_list) > 0:
            logger.debug("Deleting prior gap results for bucket(s) %s", bucket_list)
        else:
            logger.critical("Deleting gap results object(s)...")
            bucket_list = list(self.read_gap_header(cache=True))

        for bucket_name in bucket_list:
            idx = 0
            while True:
                idx += 1
                results_object = f"{self.RESULTS_OBJECT_NAME}.{bucket_name}.{idx}"
                if self.ceph.stat_syncpool_object(results_object):
                    logger.info("Sync object found, deleting: %s", results_object)
                    self.ceph.remove_syncpool_object(results_object)
                else:
                    break

        if remove_all:
            if self.ceph.stat_syncpool_object(self.RESULTS_OBJECT_NAME):
                logger.info("Deleting primary results object: %s", self.RESULTS_OBJECT_NAME)
                self.ceph.remove_syncpool_object(self.RESULTS_OBJECT_NAME)
        else:
            logger.info("Deleting bucket keys from %s", self.RESULTS_OBJECT_NAME)
            self.ceph.remove_syncpool_omap_keys(self.RESULTS_OBJECT_NAME,bucket_list)

    def add_result_entry(self, bucket_name: str = '', object_name: str = '', rados_object: str = '') -> None:
        """
        Add a gap result entry to the results dictionary.

        Args:
            bucket_name (str): Name of the bucket being processed
            object_name (str): Name of the S3 object with gaps
            rados_object (str): Name of the missing RADOS object
        """
        logger.debug("Adding gap for s3://%s/%s :: %s", bucket_name, object_name, rados_object)
        if object_name not in self.results:
            self.bucket_objects_with_gaps += 1
            self.results[object_name]={ "epoch": round(time.time(), 3), "missing_rados_objects": [ ] }
        self.results[object_name]["missing_rados_objects"].append(rados_object)
        self.bucket_gap_count += 1

        if len(json.dumps(self.results).encode("utf-8")) >= 1<<22: # 4mb
            self.write_result_object(bucket_name, False)

    def write_result_object(self, bucket_name: str = '', final: bool = False) -> None:
        """
        Write gap results to a result object in the cluster.

        Args:
            bucket_name (str): Name of the bucket being processed
            final (bool): Whether this is the final write for the bucket
        """
        if len(self.results):
            self.bucket_gap_results_obj_count += 1  # Increment first, so 0 means no objects in the bucket status omap.
            results_stored_size = len(json.dumps(self.results).encode("utf-8"))
            results_object = f"{self.RESULTS_OBJECT_NAME}.{bucket_name}.{self.bucket_gap_results_obj_count}"
            logger.info("Writing result object %s of %i bytes", results_object, results_stored_size)
            if not self.ceph.stat_syncpool_object(results_object):
                self.ceph.write_syncpool_object_data(results_object,"")

            try:
                self.ceph.write_syncpool_object_data(results_object,json.dumps(self.results))
            except Exception as e:
                logger.error("Failed to write results to %s: %s", results_object, e)
                logger.critical("Dumping result here due to failure to write %s: %s", results_object, json.dumps(self.results))
            else:
                bucket_statistics = { "results_obj_count": self.bucket_gap_results_obj_count, "gap_count": self.bucket_gap_count, "latest_scan": round(time.time(),3) }
                self.ceph.write_syncpool_omap(self.RESULTS_OBJECT_NAME, bucket_name, json.dumps(bucket_statistics))

            self.results={}
            if final:
                self.bucket_gap_results_obj_count = 0
        else:
            self.bucket_gap_results_obj_count = 0

    def start_bucket(self, bucket_name, match: str = '') -> None:
        """
        Initialize processing for a bucket.

        Args:
            bucket_name (str): Name of the bucket to process
            match (str): Prefix match filter for objects
        """
        self.delete_gap_objects([ bucket_name ])
        shardid = self.hash_bucket_name(bucket_name)
        logger.debug("Setting bucket start metadata to sync shard %i", shardid)
        sync_metadata = { "hostname": self.MYHOST, "pid": self.MYPID, "bucket_object_count": self.bucket_object_count,
                         "bucket_objects_processed": self.bucket_objects_processed,
                         "bucket_objects_with_gaps": self.bucket_objects_with_gaps,
                         "rados_obj_count": 0, "gap_count": 0, "start_time": round(time.time(),3), 
                         "end_time": 0, "match": match }
        self.ceph.write_syncpool_omap(f"{self.SYNC_OBJECT_NAME}.{shardid}", bucket_name, json.dumps(sync_metadata))
        self.touch_sync_state(bucket_name,0)

    def get_bucket_meta(self, bucket_name: str) -> Union[Dict, bool]:
        """
        Retrieve metadata for a specific bucket from the sync pool.

        Args:
            bucket_name (str): Name of the bucket to retrieve metadata for

        Returns:
            Dict|bool: Bucket metadata if found, False otherwise
        """
        shardid = self.hash_bucket_name(bucket_name)
        logger.info("Getting bucket metadata from shard %i", shardid)
        omap_data = self.ceph.read_syncpool_omap_vals_by_keys(f"{self.SYNC_OBJECT_NAME}.{shardid}", ( bucket_name, ))
        results = list(omap_data)

        if results:
            rval = results[0]
            logger.debug("Found bucket metadata: %s", rval)
            return omap_data[rval]

        logger.debug("Bucket metadata not present.")
        return False

    def update_bucket(self, bucket_name: str, data: Dict = None) -> None:
        """
        Update the bucket metadata object with the provide K/V data provided.

        Args:
            bucket_name (str): Name of the bucket that was processed
            data (dict): KV data to add/update
        """
        shardid = self.hash_bucket_name(bucket_name)
        logger.info("Setting bucket end metadata for %s to sync shard %i", bucket_name, shardid)
        bucket_meta = self.get_bucket_meta(bucket_name)
        if bucket_meta:
            bucket_meta.update( data )
            logger.debug("Bucket meta: %s", bucket_meta)
            self.ceph.write_syncpool_omap(f"{self.SYNC_OBJECT_NAME}.{shardid}", bucket_name, json.dumps(bucket_meta))
        else:
            logger.error("Bucket start metadata for %s is missing from shard %i", bucket_name, shardid)

    def end_bucket(self, bucket_name: str, rados_obj_count: int) -> None:
        """
        Mark the completion of processing a specified bucket.

        Args:
            bucket_name (str): Name of the bucket that was processed
            rados_obj_count (int): Count of RADOS objects processed
        """
        update_data = { "end_time": round(time.time(),3), "gap_count": self.bucket_gap_count, 
                        "bucket_object_count": self.bucket_object_count, "rados_obj_count": rados_obj_count,
                        "bucket_objects_processed": self.bucket_objects_processed,
                        "bucket_objects_with_gaps": self.bucket_objects_with_gaps }
        self.update_bucket(bucket_name, update_data)
        self.touch_sync_state(bucket_name, rados_obj_count)
        self.bucket_gap_count = 0

    def is_bucket_scanning(self, bucket_name: str) -> Union[ Dict, bool ]:
        """
        Check if a bucket is currently being scanned by another process.

        Args:
            bucket_name (str): Name of the bucket to check

        Returns:
            Dict | bool : Running host data if bucket is being scanned, False otherwise

        """
        running_hosts = self.get_other_running_hosts(bucket_keyed=True)
        if bucket_name in running_hosts:
            return running_hosts[bucket_name]

        return False

    def get_other_running_hosts(self, bucket_keyed: bool = False) -> Dict:
        """
        Get information about other hosts currently running the gap scanner.

        Args:
            bucket_keyed (bool): If True, return a dictionary keyed by bucket name instead of host

        Returns:
            Dict: Dictionary containing information about running hosts
        """
        running_hosts = {}
        running_hosts_raw = self.ceph.read_syncpool_omap_vals(self.SYNC_OBJECT_NAME)
        stale_host_seconds = self.report_every_x_object_count * ( 120 / 10000 )
        # 2 minutes per 10k object count report interval

        for key, value in running_hosts_raw.items():
            if key == f"{self.MYHOST}.{self.MYPID}":
                continue
            if value.get('epoch',time.time()) < time.time() - stale_host_seconds:
                # If the host has not updated the sync state, consider it stale / dead.
                continue

            key_parts = key.strip().split(".")
            remote_host = key_parts[0]
            remote_pid = key_parts[-1]
            status = value
            if not remote_host in running_hosts and not bucket_keyed:
                running_hosts[remote_host] = {}
            if bucket_keyed:
                status.update( {'hostname': remote_host, 'pid': remote_pid } )
                running_hosts[status['current_bucket']] = status
            else:
                running_hosts[remote_host][remote_pid] = value

        return running_hosts

    def get_buckets_state(self) -> Dict:
        """
        Get the state of all buckets that have been or are being processed.

        Returns:
            Dict: Dictionary containing bucket states from all shards
        """
        buckets_state = {}
        for i in range(self.shard_count):
            buckets_state |= self.ceph.read_syncpool_omap_vals(f"{self.SYNC_OBJECT_NAME}.{i}")

        return buckets_state

    def read_gap_header(self, cache: bool = False) -> Dict:
        """
        Read the gap results header data.

        Args:
            cache (bool): Whether to use cached data

        Returns:
            Dict: Gap header data
        """
        if not cache:
            self.gap_header_data = {}
            return self.ceph.read_syncpool_omap_vals(self.RESULTS_OBJECT_NAME)

        if not self.gap_header_data:
            self.gap_header_data = self.ceph.read_syncpool_omap_vals(self.RESULTS_OBJECT_NAME)

        return self.gap_header_data

    def read_gap_results(self, bucket_name: str, cache: bool = False) -> Dict:
        """
        Read gap results for a specific bucket.

        Args:
            bucket_name (str): Name of the bucket to read results for
            cache (bool): Whether to use cached data

        Returns:
            Dict: Gap results for the specified bucket
        """
        bucket_gap_results = {}

        if not cache or not self.gap_header_data:
            self.gap_header_data = self.read_gap_header(cache)

        if bucket_name in self.gap_header_data:
            for i in range(1,self.gap_header_data[bucket_name]["results_obj_count"]+1):
                bucket_gap_results |= self.ceph.read_syncpool_object_data(f"{self.RESULTS_OBJECT_NAME}.{bucket_name}.{i}")

        if not cache:
            self.gap_header_data = None

        return bucket_gap_results

    def generate_gap_list(self, verify: bool = False, bucket_list: List = None, exclude_bucket_list: List = None) -> None:
        """
        Generate a list of gaps found in the RADOS objects.

        Args:
            verify (bool): Whether to verify the results against actual RADOS objects
            bucket_list (List): List of buckets to process (None means process all buckets)
            exclude_bucket_list (List): List of buckets to exclude from processing
        """
        if not isinstance(exclude_bucket_list, List):
            exclude_bucket_list = []

        if not isinstance(bucket_list,List):
            bucket_list = []

        logger.info("Generating gap list report")
        gap_results = {}
        found_count = 0
        missing_count = 0

        for obj in [self.SYNC_OBJECT_NAME, self.RESULTS_OBJECT_NAME]:
            if not self.ceph.stat_syncpool_object(obj):
                logger.critical("Sync / Results object found: %s.  Exiting", obj)
                sys.exit(1)
            logger.debug("Found primary sync / results object: %s", obj)

        running_hosts = self.get_other_running_hosts()

        if not bucket_list:
            bucket_list = list(self.read_gap_header(cache = True))

        if verify:
            op_queue = queue.Queue(maxsize=self.max_inflight)
            results_lock = threading.Lock()

            def verify_processor():
                nonlocal found_count
                while True:
                    op_obj = op_queue.get()
                    if op_obj is None:
                        op_queue.task_done()
                        break

                    results = []
                    for stat_op in op_obj['stat_op']:
                        stat_op.wait_for_complete()
                        results.append(stat_op.get_return_value())

                    # If all specified pools returned 0, the object was found
                    if len(results) > 0 and results.count(0) > 0:
                        logger.debug("Found %s", op_obj['rados_object'])

                        # Thread-safe deletion of resolved gaps
                        with results_lock:
                            found_count += 1
                            bucket = op_obj['bucket_name']
                            obj_name = op_obj['object_name']
                            rados_obj = op_obj['rados_object']

                            if bucket in gap_results and obj_name in gap_results[bucket]:
                                if rados_obj in gap_results[bucket][obj_name]['missing_rados_objects']:
                                    gap_results[bucket][obj_name]['missing_rados_objects'].remove(rados_obj)
                                if len(gap_results[bucket][obj_name]['missing_rados_objects']) == 0:
                                    del gap_results[bucket][obj_name]
                                if len(gap_results[bucket]) == 0:
                                    del gap_results[bucket]

                    op_queue.task_done()

            processor_thread = threading.Thread(target=verify_processor, daemon=True)
            processor_thread.start()

        for bucket_name in bucket_list:
            if bucket_name in exclude_bucket_list:
                self.skipped_bucket_count += 1
                continue

            gap_results[bucket_name] = self.read_gap_results(bucket_name, cache=True)

            if verify:
                # Wrap dictionaries/lists in list() to freeze iterables against asynchronous background changes
                for object_name, object_data in list(gap_results[bucket_name].items()):
                    for rados_object in list(object_data["missing_rados_objects"]):
                        logger.debug("Verifying %s", rados_object)
                        op_queue.put({
                            "stat_op": self.ceph.async_stat_datapool_object(rados_object),
                            "bucket_name": bucket_name,
                            "rados_object": rados_object,
                            "object_name": object_name
                        })

        if verify:
            op_queue.put(None)
            processor_thread.join()

        if self.json:
            dump_object = {"active_processes": bool(len(running_hosts) > 0), "verified": verify }
            if verify:
                dump_object['found_count'] = found_count
            print(json.dumps(dump_object | { "gap_results": gap_results }))
        else:
            missing_text = "STILL MISSING" if verify else "MISSING"

            for bucket_name, bucket_data in gap_results.items():
                for object_name, object_data in bucket_data.items():
                    for rados_object in object_data['missing_rados_objects']:
                        print(f"s3://{bucket_name}/{object_name} {missing_text} {rados_object}")
                        missing_count += 1

        if not self.json:
            verified="Verified " if verify else ""
            found=f", but found {found_count} rados objects" if found_count else ""
            print(f"{verified}Missing {missing_count} rados objects{found}")

        if len(running_hosts) and not self.json:
            print("WARNING: There are active gap list processes, results may be incomplete.")


    def generate_report(self) -> None:
        """
        Generate a report of bucket metadata and processing status.
        """
        logger.info("Generating bucket metadata report")
        if not self.ceph.stat_syncpool_object(self.SYNC_OBJECT_NAME):
            logger.critical("No primary sync object found.  Exiting")
            sys.exit(1)

        logger.debug("Found primary sync object: %s", self.SYNC_OBJECT_NAME)

        bucket_metadata_header = self.ceph.read_syncpool_object_data(self.SYNC_OBJECT_NAME)
        self.shard_count = bucket_metadata_header["shard_count"]
        self.total_bucket_count = bucket_metadata_header["bucket_count"]

        running_hosts = self.get_other_running_hosts()
        bucket_state = self.get_buckets_state()
        if self.json:
            print(json.dumps({"active_hosts": running_hosts,"bucket_state": bucket_state}))
        else:
            if len(running_hosts):
                print("\nRunning Hosts:")
                total_processed=0
                for host,data in running_hosts.items():
                    host_processed=0
                    print(f"  {host} ( {len(data.items())} processes )")
                    for pid,status in data.items():
                        dt = datetime.fromtimestamp(status['epoch']).strftime('%Y-%m-%d %H:%M:%S')
                        pct_complete = 0
                        if status.get('bucket_object_count',0)>0:
                            pct_complete = status.get('bucket_objects_processed',0)/status.get('bucket_object_count',99999999999999) * 100
                        print(f"    PID: {pid}, {status['current_bucket']}, User Objects: {status.get('bucket_objects_processed',0)}/{status.get('bucket_object_count',0)} ({pct_complete:0.1f}%), Rados Count: {status['rados_obj_count']}, QD: {status['queue_size']}, Gap Count: {status['gap_count']}, Bucket Counter: {status['bucket_counter']}, Last Updated: {dt}")
                        host_processed += status['bucket_counter']
                        total_processed += status['bucket_counter']
                    print(f"  Host processed: {host_processed}")
                print(f"Total processed: {total_processed} of {self.total_bucket_count}")
            else:
                print("No active hosts.")

            if len(bucket_state):
                print("\nBucket State:")
                for bucket_name,data in bucket_state.items():
                    output_line = f"  {bucket_name}:: Object Count: ({data.get('bucket_objects_processed',0)}/{data.get('bucket_object_count',0)}), Rados Count: {data['rados_obj_count']}, "
                    if data.get('end_time',0):
                        dt = datetime.fromtimestamp(data['end_time']).strftime('%Y-%m-%d %H:%M:%S')
                        hum = self.seconds_to_human(data.get('total_time_secs',0))
                        scope = f" (prefix: '{data['match']}')" if data.get('match') else ""
                        output_line += f"Last Scan Completed: {dt} in {hum}, found {data.get('bucket_objects_with_gaps',0)} S3 / {data.get('gap_count',0)} rados gaps{scope}."
                    elif data.get('start_time',0):
                        dt = datetime.fromtimestamp(data['start_time']).strftime('%Y-%m-%d %H:%M:%S')
                        state = "never completed, process not running"
                        if data['hostname'] in running_hosts and str(data['pid']) in running_hosts[data['hostname']]:
                            state = f"active on host {data['hostname']} (pid: {data['pid']})"
                        output_line += f"Scan Started: {dt} ({state})"
                    print(output_line)
            else:
                print("  No bucket state available.")

    def prometheus_exporter(self):
        from collections import defaultdict

        def esc(v):
            return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

        def classify(oid,object_name):
            MP   = re.compile(r'__multipart_|__shadow_')
            if MP.search(oid):
                return "multipart_tail"
            if oid.endswith("_" + object_name):
                return "head"
            return "other"

        for obj in [self.SYNC_OBJECT_NAME, self.RESULTS_OBJECT_NAME]:
            if not self.ceph.stat_syncpool_object(obj):
                logger.critical("Sync / Results object found: %s.  Exiting", obj)
                sys.exit(1)
            logger.debug("Found primary sync / results object: %s", obj)

        newest = 0
        gap_results = {}
        affected = defaultdict(set)
        missing = defaultdict(int)
        running_hosts = self.get_other_running_hosts()
        bucket_state = self.get_buckets_state()

        bucket_list = list(self.read_gap_header(cache = True))

        for bucket_name in bucket_list:
            gap_results[bucket_name] = self.read_gap_results(bucket_name, cache=True)

            for object_name, object_data in list(gap_results[bucket_name].items()):
                newest = max(newest, object_data['epoch'])
                for rados_object in list(object_data["missing_rados_objects"]):
                    kind = classify(rados_object,object_name)
                    affected[(bucket_name, kind)].add(object_name)
                    missing[(bucket_name, kind)] += 1

        out = []
        out.append("# HELP rgw_gap_s3_objects_affected Distinct S3 objects with at least one missing rados object")
        out.append("# TYPE rgw_gap_s3_objects_affected gauge")
        for (b, k), s in sorted(affected.items()):
            out.append(f'rgw_gap_s3_objects_affected{{bucket="{esc(b)}",kind="{k}"}} {len(s)}')
        out.append("# HELP rgw_gap_rados_objects_missing Missing rados objects referenced by RGW manifests")
        out.append("# TYPE rgw_gap_rados_objects_missing gauge")
        for (b, k), n in sorted(missing.items()):
            out.append(f'rgw_gap_rados_objects_missing{{bucket="{esc(b)}",kind="{k}"}} {n}')
        out.append("# TYPE rgw_gap_results_mtime_seconds gauge")
        out.append(f'rgw_gap_results_mtime_seconds{int(newest)}')

        states = defaultdict(int)
        last_done = 0
        now = time.time()
        for bname, st in bucket_state.items():
            if st.get("end_time"):
                last_done = max(last_done, st["end_time"])
                states["stale" if now - st["end_time"] > 7 * 86400 else "completed"] += 1
            else:
                states["running_or_dead"] += 1
        out.append("# TYPE rgw_gap_scan_buckets gauge")
        for s, n in states.items():
            out.append(f'rgw_gap_scan_buckets{{state="{s}"}} {n}')
        out.append("# TYPE rgw_gap_scan_last_completed_seconds gauge")
        out.append(f'rgw_gap_scan_last_completed_seconds {int(last_done)}')
        out.append("# TYPE rgw_gap_scan_active_hosts gauge")
        out.append(f'rgw_gap_scan_active_hosts {len(running_hosts)}')

        print("\n".join(out))

        return

    def seconds_to_human(self, secs: float) -> str:
        """
        Convert seconds to a human-readable time format.

        Args:
            secs (float): Number of seconds to convert

        Returns:
            str: Human-readable time string
        """
        secs = float(secs)
        days = int(secs // 86400)
        hours = int((secs % 86400) // 3600)
        minutes = int((secs % 3600) // 60)
        seconds = round(secs % 60,3)
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours or minutes or seconds:
            parts.append(f"{hours:02d}")
            parts.append(f"{minutes:02d}")
            parts.append(f"{seconds:02.3f}")
        return ":".join(parts)

    def check_aio_result(self, op_obj: Dict) -> Union[Dict, None]:
        """
        Check return of Async Object Stat.

        - If found, return None
        - If not found, but pool index is 0 (only checked first pool),
          re-submit to all remaining pools
        - If not found, and results entry, return None

        Args:
            op_obj (Dict): Operation object containing stat operation data

        Returns:
            Union[Dict, None]: Updated operation object, or None
        """
        results = []
        for stat_op in op_obj['stat_op']:
            stat_op.wait_for_complete()
            results.append(stat_op.get_return_value())

        if len(results) > 0 and results.count(0) == 0:
            if op_obj['poolidx'] == 0 and len(self.ceph.pool_ioctl) > 1:
                logger.info("%s not found in default pool, checking remaining pools.", op_obj['rados_object'])
                stat_ops = self.ceph.async_stat_datapool_object(op_obj['rados_object'],1)
                if len(stat_ops):
                    op_obj.update({'stat_op': stat_ops, 'poolidx': 1 })
                    return op_obj
                else:
                    logger.info("No remaining pool checks returned, %s not found.", op_obj['rados_object'])
                    self.add_result_entry(bucket_name=op_obj['bucket'], object_name=op_obj['user_object'], rados_object=op_obj['rados_object'])
                    logger.debug("[NOT FOUND] s3://%s/%s MISSING %s", op_obj['bucket'], op_obj['user_object'], op_obj['rados_object'])

            self.add_result_entry(bucket_name=op_obj['bucket'], object_name=op_obj['user_object'], rados_object=op_obj['rados_object'])
            logger.debug("[NOT FOUND] s3://%s/%s MISSING %s", op_obj['bucket'], op_obj['user_object'], op_obj['rados_object'])

        return None

    def output_status(self, bucket_name: str = '', rados_obj_count: int = 0, delta_start: int = 0, delta_last: int = 0, qsize: int = 0):
        """
        Output processing status information.

        Args:
            bucket_name (str): Name of the bucket being processed
            rados_obj_count (int): Count of RADOS objects processed
            delta_start (int): Time elapsed since start
            delta_last (int): Time elapsed since last status update
        """
        logger.info("[Status] Submitted %s rados objects in %.3f seconds ( last 10k in %.3f seconds ) for %s. Queue size: %i",
                    rados_obj_count, delta_start, delta_last, bucket_name, qsize)
        self.touch_sync_state(bucket_name, rados_obj_count, qsize)

    def process_bucket(self, bucket_name: str, force_scan = False) -> None:
        """
        Process a single bucket for gap detection.

        Args:
            bucket_name (str): Name of the bucket to process
            force_scan (bool): Whether to force scanning even if recently scanned
        """
        bucket_meta = None
        self.bucket_object_count = 0
        self.bucket_objects_processed = 0
        self.bucket_objects_with_gaps = 0
        last_user_object_processed = None

        if not force_scan:
            logger.info("Checking %s via sync state", bucket_name)
            is_scanning = self.is_bucket_scanning(bucket_name)
            if is_scanning:
                logger.info("Bucket %s is actively being scanned on %s (%i)", bucket_name, is_scanning['hostname'], int(is_scanning['pid']))
                return None

        bucket_meta = self.get_bucket_meta(bucket_name)

        if bucket_meta:
            dt = datetime.fromtimestamp(bucket_meta["end_time"]).strftime('%Y-%m-%d %H:%M:%S')
            hum = self.seconds_to_human(self.max_age)
            scanned_match = bucket_meta.get("match", "")

            if force_scan:
                logger.info("Force scan set, scanning.")
            elif time.time() - bucket_meta["end_time"] > int(self.max_age):
                logger.info("Bucket %s end time ( %s ) is more than %s old.  Processing again.", bucket_name, dt, hum)
            elif not self.match.startswith(scanned_match):
                logger.info("Bucket %s last scan ( %s ) only covered prefix '%s', not '%s'.  Processing again.",
                            bucket_name, dt, scanned_match, self.match)
            else:
                logger.info("Bucket %s end time ( %s ) is less than %s old.  Skipping.", bucket_name, dt, hum)
                return None

        logger.info("Processing %s", bucket_name)
        self.processed_bucket_count += 1

        bucket_rados_obj_count = 0
        start_time = last_status_time = round(time.time(),3)
        self.start_bucket(bucket_name,self.match)

        bucket_stats = {}
        with subprocess.Popen(self.BUCKET_STATS_COMMAND + [f"--bucket={bucket_name}"], bufsize=1048576,
                              shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as bsl:
            bsl_data, _ = bsl.communicate()
            if bsl.returncode == 0 and bsl_data:
                bucket_stats = json.loads(bsl_data.decode("utf-8"))

        try:
            if bucket_stats['usage']['rgw.main']['num_objects'] == 0:
                logger.debug("Bucket %s has zero objects.  Skipping.", bucket_name)
                now_time = round(time.time(),3)
                self.output_status(bucket_name, bucket_rados_obj_count, now_time - start_time, now_time - last_status_time)
                self.update_bucket(bucket_name, { "total_time_secs": round(now_time - start_time,3)})
                self.end_bucket(bucket_name,bucket_rados_obj_count)
                return None
        except KeyError:
            logger.debug("Bucket %s has zero objects. Skipping.", bucket_name)
            now_time = round(time.time(),3)
            self.output_status(bucket_name, bucket_rados_obj_count, now_time - start_time, now_time - last_status_time)
            self.update_bucket(bucket_name, { "total_time_secs": round(now_time - start_time,3)})
            self.end_bucket(bucket_name,bucket_rados_obj_count)
            return None

        self.bucket_object_count=bucket_stats['usage']['rgw.main']['num_objects']
        self.update_bucket(bucket_name, { "bucket_object_count": self.bucket_object_count } )
        self.touch_sync_state(bucket_name, 0, 0)

        # Initialize thread-safe queue to manage in-flight operations natively
        op_queue = queue.Queue(maxsize=self.max_inflight)

        def result_processor():
            while True:
                op_obj = op_queue.get()
                if op_obj is None:  # Sentinel value to exit
                    op_queue.task_done()
                    break

                res = self.check_aio_result(op_obj)

                if isinstance(res, dict):
                    # Operation required further checks on remaining pools, put it back
                    op_queue.put(res)

                op_queue.task_done()

        # Start consumer thread
        processor_thread = threading.Thread(target=result_processor, daemon=True)
        processor_thread.start()

        with subprocess.Popen(self.BUCKET_RADOSLIST_COMMAND + [f"--bucket={bucket_name}"], bufsize=1048576, shell=False, \
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as brl:
            for brl_line in io.TextIOWrapper(brl.stdout, encoding="utf-8"):
                object_data = brl_line.strip().split(self.FIELD_SEPARATOR)
                if self.match and not object_data[2].startswith(self.match):
                    continue

                if object_data[2] != last_user_object_processed:
                    self.bucket_objects_processed += 1
                    last_user_object_processed=object_data[2]

                bucket_rados_obj_count += 1
                if bucket_rados_obj_count % self.report_every_x_object_count == 0:
                    now_time = round(time.time(),3)
                    self.output_status(bucket_name, bucket_rados_obj_count, now_time - start_time, now_time - last_status_time, op_queue.qsize())
                    last_status_time = now_time

                # Add initial op to the queue. put() will naturally block if maxsize is reached
                op_obj = {
                    "stat_op": self.ceph.async_stat_datapool_object(object_data[0], 0),
                    "rados_object": object_data[0],
                    "bucket": bucket_name,
                    "user_object": object_data[2],
                    "poolidx": 0
                }
                op_queue.put(op_obj)

        # Signal completion to thread and wait
        op_queue.put(None)
        processor_thread.join()

        self.write_result_object(bucket_name, final = True)

        now_time = round(time.time(),3)
        self.output_status(bucket_name, bucket_rados_obj_count, now_time - start_time, now_time - last_status_time)

        self.update_bucket(bucket_name, { "total_time_secs": round(now_time - start_time,3)})
        self.end_bucket(bucket_name,bucket_rados_obj_count)
        return None

    def process_list(self, bucket_list: List = None, exclude_bucket_list: List = None) -> None:
        """
        Process a list of buckets for gap detection.

        Args:
            bucket_list (List): List of bucket names to process (None for all)
            exclude_bucket_list (List): List of bucket names to exclude from processing
        """
        if not isinstance(exclude_bucket_list, List):
            exclude_bucket_list = []

        if not isinstance(bucket_list, List):
            bucket_list = []

        if len(bucket_list) > 0:
            logger.info("Starting processing of %i bucket(s)", len(bucket_list))
            self.populate_sync_objects(1, len(bucket_list))

            for bucket in bucket_list:
                if bucket not in exclude_bucket_list:
                    self.process_bucket(bucket)
                else:
                    self.skipped_bucket_count += 1
                    logger.debug("Found %s in exclude_bucket_list, skipping.", bucket)
            return None

        # If we get here, we're processing -all- buckets
        # Get a count of the buckets to determine sync object count
        with subprocess.Popen(self.BUCKET_LIST_COMMAND, bufsize=1048576, shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as bl, \
            subprocess.Popen(["jq","-cr",".[]"],stdin=bl.stdout,stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as jql, \
            subprocess.Popen(["wc","-l"], stdin=jql.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as bc:

            bl.stdout.close()
            jql.stdout.close()

            bc_out, _ = bc.communicate()
            self.total_bucket_count = int(bc_out.decode("utf-8").strip())

        logger.info("Starting processing of %i bucket(s)", self.total_bucket_count)

        self.shard_count = int(self.total_bucket_count/400) + 1
        self.populate_sync_objects(self.shard_count, self.total_bucket_count)

        if args.norandom: # Do not randomize the bucket list, optional.
            with subprocess.Popen(self.BUCKET_LIST_COMMAND, bufsize=1048576, shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as bl:
                for bl_line in io.TextIOWrapper(bl.stdout, encoding="utf-8"):
                    bl_line = bl_line.strip()
                    if re.match(r'^"',bl_line):
                        # The raw output of bucket list is a json array.  We need to only process
                        # lines that start with double quotes, and then we need to remove the
                        # double quotes and ending comma (if present), but NOT remove any other
                        # characters in between.

                        bucket = re.sub(r'^"','',bl_line)
                        bucket = re.sub(r',$','',bucket)
                        bucket = re.sub(r'"$','',bucket)

                        if bucket not in exclude_bucket_list:
                            self.process_bucket(bucket)
                        else:
                            self.skipped_bucket_count += 1
                            logger.debug("Found %s in exclude_bucket_list, skipping.", bucket)
            return None

        # Randomize the bucket list, this is the default.
        with subprocess.Popen(self.BUCKET_LIST_COMMAND, bufsize=1048576, shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as bl, \
            subprocess.Popen(["jq","-cr",".[]"],stdin=bl.stdout,stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as jql, \
            subprocess.Popen(["sort","--random-sort"],stdin=jql.stdout,stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as sortl:

            bl.stdout.close()
            jql.stdout.close()

            for sortl_line in io.TextIOWrapper(sortl.stdout, encoding="utf-8"):
                bucket = sortl_line.strip()
                if bucket not in exclude_bucket_list:
                    self.process_bucket(bucket)
                else:
                    self.skipped_bucket_count += 1
                    logger.debug("Found %s in exclude_bucket_list, skipping.", bucket)

        return None

# End class CephGapScanner

if __name__ == "__main__":
    """
    Main entry point for the rgw-gap-list tool.

    This script performs gap analysis on Ceph RGW S3 buckets by by stat'ing
    all RADOS objects which are referenced by bucket index entries to
    identify missing objects.

    The script supports various modes of operation:
    - Gap detection and process state reporting
    - Result reporting and verification

    Command line arguments control the behavior of the gap scanner, including
    which buckets to process, how to filter results, and where to store synchronization
    data.
    """

    parser = argparse.ArgumentParser(description="Multi-run / Multi-host capable rgw-gap-list tool")
    parser.add_argument("-a", "--maxage",  default = 7*86400, type=int, help="Maximum age (in seconds) of last scan before rescan is forced.  Default 7 days.")
    parser.add_argument("-b", "--bucketlist",  default = '', help="Optional: Bucket(s) to operate on, default is all buckets, quoted space separated list is supported. Supercedes -l.")
    parser.add_argument("-e", "--excludelist",  default = '', help="Optional: Bucket(s) to skip, default is process all buckets, quoted space separated list is supported. Supercedes -f.")
    parser.add_argument("-f", "--excludefile", default = '', help="Optional: File with list of bucket(s) to skip, should be one bucket name per line.")
    parser.add_argument("-c", "--conf", default = '/etc/ceph/ceph.conf', help="Ceph conf file to use, default '/etc/ceph/ceph.conf'")
    parser.add_argument("--ceph_client_name", default = 'client.admin', help="Ceph client name to use, default 'client.admin'")
    parser.add_argument("--ceph_client_keyring", default = '/etc/ceph/ceph.client.admin.keyring', help="Ceph client keyring to use, default '/etc/ceph/ceph.client.admin.keyring'")
    parser.add_argument("-d", "--delete",  default = False, action="store_true", help="Remove all sync objects and Exit. Used to clear all syncronized bucket status data.")
    parser.add_argument("-g", "--gaps",  default = False, action="store_true", help="Dump the gap results from RADOS object contents.  All other options are ignore ( except -j )")
    parser.add_argument("-i", "--inflight",  default = 15000, type=int, help="Maximum number of in-flight ops to allow without a response.  Default: 15000")
    parser.add_argument("-l", "--listfile", default = '', help="Optional: Bucket list file, should be one bucket name per line.")
    parser.add_argument("-m", "--match", default = '', help="Specify a prefix match for the object names.  Only objects matching this prefix will be checked for gaps.")
    parser.add_argument("-n", "--norandom", default = False, action="store_true", help="By default, the script randomizes the list of buckets before processing.  On large bucket count environments, this may cause significant delay before start of processing due to the way the randomizing occurs.  Set '-n' to Not Randomize the list to remove this delay.")
    parser.add_argument("--namespace", default = 'rgw-gap-list', help="What namespace to use for sync / results objects. Default: rgw-gap-list")
    parser.add_argument("-p", "--pool", default = 'default.rgw.buckets.data default.rgw.buckets.non-ec', help="Bucket Data Pool(s), default 'default.rgw.buckets.data default.rgw.buckets.non-ec', quoted space separated list is supported.")
    parser.add_argument("--promexport",  default = False, action="store_true", help="Prometheus Export.  All other processing and output options are ignored if this flag is specified.")
    parser.add_argument("-s", "--syncpool", default = 'default.rgw.buckets.index', help="Synchronization / Queuing pool for the script to use, default 'default.rgw.buckets.index'.")
    parser.add_argument("-r", "--report",  default = False, action="store_true", help="Generate bucket scrub metadata report.")
    parser.add_argument("-j", "--json",  default = False, action="store_true", help="Use JSON format for bucket scrub metadata report. Only considered with -g, -r and -x")
    parser.add_argument("-v", "--verbosity", default = 0, action="count", help="Optional: Verbosity level, multiple -v's are supported for higher verbosity, example: -vvv")
    parser.add_argument("-x", "--verify", default = False, action="store_true", help="Used to verify the results from a prior run.")
    args = parser.parse_args()

    debug_level = min([len(LOG_LEVELS)-1,args.verbosity])

    logging.basicConfig(
        level=LOG_LEVELS[debug_level],
        format=f'%(asctime)s {os.getpid()}.{os.uname().nodename} %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger('rgw-gap-list')

    bucket_list = []
    exclude_bucket_list = []
    BUCKET_NAME_RE = re.compile(
        r"^([a-zA-Z0-9][a-zA-Z0-9._-]*/)?[a-z0-9][a-z0-9.-]{1,253}[a-z0-9]$"
    )

    if args.excludelist:
        exclude_bucket_list = [ bn for bn in args.excludelist.split(" ") if BUCKET_NAME_RE.match(bn) ]
        if len(exclude_bucket_list) == 0:
            logger.critical("The provided exclude bucket list did not contain any valid bucket names.  Please confirm proper s3 bucket names are present.")
            sys.exit(1)
    elif args.excludefile:
        with open(args.excludefile, encoding="utf-8") as elist:
            exclude_bucket_list = [ line.strip() for line in elist if BUCKET_NAME_RE.match(line.strip()) ]
        if len(exclude_bucket_list) == 0:
            logger.critical("The provided exclude bucket list file did not contain any valid bucket names.  Please confirm proper s3 bucket names are present.")
            sys.exit(1)

    if args.bucketlist:
        bucket_list = [ bn for bn in args.bucketlist.split(" ") if BUCKET_NAME_RE.match(bn) ]
        if len(bucket_list) == 0:
            logger.critical("The provided bucket list did not contain any valid bucket names.  Please confirm proper s3 bucket names are present.")
            sys.exit(1)
    elif args.listfile:
        with open(args.listfile, encoding="utf-8") as blist:
            bucket_list = [ line.strip() for line in blist if BUCKET_NAME_RE.match(line.strip()) ]
        if len(bucket_list) == 0:
            logger.critical("The provided bucket list file did not contain any valid bucket names.  Please confirm proper s3 bucket names are present.")
            sys.exit(1)

    with CephClusterConnection(
            ceph_conf=args.conf, 
            client_name=args.ceph_client_name, 
            client_keyring=args.ceph_client_keyring, 
            pool_names=args.pool.split(" "), 
            sync_pool=args.syncpool,
            namespace=args.namespace
            ) as ceph:
        with CephGapScanner(ceph) as scanner:
            scanner.max_inflight = max(args.inflight,1)
            scanner.match = args.match
            scanner.max_age = max(0,int(args.maxage))
            scanner.json = args.json
            try:
                if args.promexport:
                    scanner.prometheus_exporter()
                elif args.gaps:
                    scanner.generate_gap_list(bucket_list = bucket_list, exclude_bucket_list = exclude_bucket_list)
                elif args.report:
                    scanner.generate_report()
                elif args.delete:
                    scanner.delete_gap_objects()
                    scanner.delete_sync_objects()
                elif args.verify:
                    scanner.generate_gap_list(bucket_list = bucket_list, exclude_bucket_list = exclude_bucket_list, verify=True)
                else:
                    scanner.process_list(bucket_list = bucket_list, exclude_bucket_list = exclude_bucket_list)
            except BrokenPipeError:
                # Redirect stdout to /dev/null to avoid a second BrokenPipeError at exit
                devnull = os.open(os.devnull, os.O_WRONLY)
                os.dup2(devnull, sys.stdout.fileno())
