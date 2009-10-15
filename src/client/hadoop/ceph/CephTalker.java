// -*- mode:Java; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*- 
/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * 
 * Wraps a number of native function calls to communicate with the Ceph
 * filesystem.
 */
package org.apache.hadoop.fs.ceph;

import org.apache.hadoop.conf.Configuration;

class CephTalker extends CephFS {
	//we write a constructor so we can load the libraries
	public CephTalker(Configuration conf) {
		System.load(conf.get("fs.ceph.libDir")+"/libhadoopcephfs.so");
		System.load(conf.get("fs.ceph.libDir")+"/libceph.so");
	}
	protected native boolean ceph_initializeClient(String arguments, int block_size);
	protected native String ceph_getcwd();
	protected native boolean ceph_setcwd(String path);
  protected native boolean ceph_rmdir(String path);
  protected native boolean ceph_unlink(String path);
  protected native boolean ceph_rename(String old_path, String new_path);
  protected native boolean ceph_exists(String path);
  protected native long ceph_getblocksize(String path);
  protected native boolean ceph_isdirectory(String path);
  protected native boolean ceph_isfile(String path);
  protected native String[] ceph_getdir(String path);
  protected native int ceph_mkdirs(String path, int mode);
  protected native int ceph_open_for_append(String path);
  protected native int ceph_open_for_read(String path);
  protected native int ceph_open_for_overwrite(String path, int mode);
  protected native int ceph_close(int filehandle);
  protected native boolean ceph_setPermission(String path, int mode);
  protected native boolean ceph_kill_client();
  protected native boolean ceph_stat(String path, CephFileSystem.Stat fill);
  protected native int ceph_statfs(String Path, CephFileSystem.CephStat fill);
  protected native int ceph_replication(String path);
  protected native String ceph_hosts(int fh, long offset);
  protected native int ceph_setTimes(String path, long mtime, long atime);
}
