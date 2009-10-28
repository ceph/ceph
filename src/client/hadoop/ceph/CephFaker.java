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
 * This uses the local Filesystem but pretends to be communicating
 * with a Ceph deployment, for unit testing the CephFileSystem.
 */

package org.apache.hadoop.fs.ceph;

import java.util.Hashtable;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

class CephFaker extends CephFS {
	
	FileSystem localFS;
	int blockSize;
	Configuration conf;
	Hashtable<Integer, Object> files;
	Hashtable<Integer, String> filenames;
	int fileCount = 0;
	
	public CephFaker(Configuration con, Log log) {
		super(con, log);
		conf = con;
		files = new Hashtable<Integer, Object>();
		filenames = new Hashtable<Integer, String>();
	}
	
	protected boolean ceph_initializeClient(String args, int block_size) {
		//let's remember the default block_size
		blockSize = block_size;
		/* for a real Ceph deployment, this starts up the client, 
		 * sets debugging levels, etc. We just need to get the
		 * local FileSystem to use, and we'll ignore any
		 * command-line arguments. */
		try {
			localFS = FileSystem.getLocal(conf);
			localFS.setWorkingDirectory(new Path(conf.get("test.build.data", "/tmp")));
		}
		catch (IOException e) {
			return false;
		}
		return true;
	}

	protected String ceph_getcwd() {
		return localFS.getWorkingDirectory().toString();
	}

	protected boolean ceph_setcwd(String path) {
		localFS.setWorkingDirectory(new Path(path));
		return true;
	}

	//the caller is responsible for ensuring empty dirs
	protected boolean ceph_rmdir(String pth) {
		Path path = new Path(pth);
		boolean ret = false;
		try {
			if (localFS.listStatus(path).length <= 1) {
				ret = localFS.delete(path, true);
			}
		}
		catch (IOException e){ }
		return ret;
	}

	//this needs to work on (empty) directories too
	protected boolean ceph_unlink(String path) {
		boolean ret = false;
		if (ceph_isdirectory(path)) {
			ret = ceph_rmdir(path);
		}
		else {
			try {
				ret = localFS.delete(new Path(path), false);
			}
			catch (IOException e){ }
		}
		return ret;
	}

	protected boolean ceph_rename(String oldName, String newName) {
		boolean ret = false;
		try {
			ret = localFS.rename(new Path(oldName), new Path(newName));
		}
		catch (IOException e) { }
		return ret;
	}

	protected boolean ceph_exists(String path) {
		boolean ret = false;
		try {
			ret = localFS.exists(new Path(path));
		}
		catch (IOException e){ }
		return ret;
	}

	protected long ceph_getblocksize(String path) {
		try {
			FileStatus status = localFS.getFileStatus(new Path(path));
			return status.getBlockSize();
		}
		catch (FileNotFoundException e) {
			return -CephFS.ENOENT;
		}
		catch (IOException e) {
			return -1; //just fail generically
		}
	}

	protected boolean ceph_isdirectory(String path) {
		boolean ret = false;
		try {
			FileStatus status = localFS.getFileStatus(new Path(path));
			ret = status.isDir();
		}
		catch (IOException e) {}
		return ret;
	}

	protected boolean ceph_isfile(String path) {
		boolean ret = false;
		try {
			FileStatus status = localFS.getFileStatus(new Path(path));
			ret = !status.isDir();
		}
		catch (Exception e) {}
		return ret;
	}

	protected String[] ceph_getdir(String path) {
		if (!ceph_isdirectory(path)) {
			return null;
		}
		try {
			FileStatus[] stats = localFS.listStatus(new Path(path));
			String[] names = new String[stats.length];
			for (int i=0; i<stats.length; ++i) {
				names[i] = stats[i].getPath().toString();
			}
			return names;
		}
		catch (IOException e) {}
		return null;
	}

	protected int ceph_mkdirs(String path, int mode) {
		boolean success = false;
		try {
			success = localFS.mkdirs(new Path(path), new FsPermission((short)mode));
		}
		catch (IOException e) { }
		if (success) {
			return 0;
		}
		if (ceph_isdirectory(path)) {
			return -EEXIST; //apparently it already existed
		}
		return -1;
	}

	/* 
	 * Unlike a real Ceph deployment, you can't do opens on a directory.
	 * Since that has unpredictable behavior and you shouldn't do it anyway,
	 * it's okay.
	 */
	protected int ceph_open_for_append(String path) {
		FSDataOutputStream stream;
		try {
			stream = localFS.append(new Path(path));
			files.put(new Integer(fileCount), stream);
			filenames.put(new Integer(fileCount), path);
			return fileCount++;
		}
		catch (IOException e) { }
		return -1; // failure
	}

	protected int ceph_open_for_read(String path) {
		FSDataInputStream stream;
		try {
			stream = localFS.open(new Path(path));
			files.put(new Integer(fileCount), stream);
			filenames.put(new Integer(fileCount), path);
			return fileCount++;
		}
		catch (IOException e) { }
		return -1; //failure
	}

	protected int ceph_open_for_overwrite(String path, int mode) {
		FSDataOutputStream stream;
		try {
			stream = localFS.create(new Path(path));
			files.put(new Integer(fileCount), stream);
			filenames.put(new Integer(fileCount), path);
			return fileCount++;
		}
		catch (IOException e) { }
		return -1; //failure
	}

	protected int ceph_close(int filehandle) {
		try {
			if(files.remove(new Integer(filehandle)) == null) {
				return -ENOENT; //this isn't quite the right error code,
				// but the important part is it's negative
			}
			else {
				filenames.remove(new Integer(filehandle));
				return 0; //hurray, success
			}
		}
		catch (NullPointerException e) { } //err, how?
		return -1; //failure
	}

	protected boolean ceph_setPermission(String pth, int mode) {
		Path path = new Path(pth);
		boolean ret = false;
		try {
			localFS.setPermission(path, new FsPermission((short)mode));
			ret = true;
		}
		catch (IOException e) { }
		return ret;
	}

	//rather than try and match a Ceph deployment's behavior exactly,
	//just make bad things happen if they try and call methods after this
	protected boolean ceph_kill_client() {
		localFS = null;
		files = null;
		filenames = null;
		return true;
	}

	protected boolean ceph_stat(String pth, CephFileSystem.Stat fill) {
		Path path = new Path(pth);
		boolean ret = false;
		try {
			FileStatus status = localFS.getFileStatus(path);
			fill.size = status.getLen();
			fill.is_dir = status.isDir();
			fill.block_size = status.getBlockSize();
			fill.mod_time = status.getModificationTime();
			fill.access_time = status.getAccessTime();
			fill.mode = status.getPermission().toShort();
			ret = true;
		}
		catch (IOException e) {}
		return ret;
	}

	protected int ceph_statfs(String pth, CephFileSystem.CephStat fill) {
		Path path = new Path(pth);
		try {
			FsStatus stat = localFS.getStatus();
			fill.capacity = stat.getCapacity();
			fill.used = stat.getUsed();
			fill.remaining = stat.getRemaining();
			return 0;
		}
		catch (Exception e){}
		return -1; //failure;
	}

	protected int ceph_replication(String path) {
		int ret = -1; //-1 for failure
		try {
			ret = localFS.getFileStatus(new Path(path)).getReplication();
		}
		catch (IOException e) {}
		return ret;
	}

	protected String ceph_hosts(int fh, long offset) {
		String ret = null;
		try {
			BlockLocation[] locs = localFS.getFileBlockLocations(
						 localFS.getFileStatus(new Path(
											filenames.get(new Integer(fh)))), offset, 1);
			ret = locs[0].getNames()[0];
		}
		catch (IOException e) {}
		catch (NullPointerException f) { }
		return ret;
	}

	protected int ceph_setTimes(String pth, long mtime, long atime) {
		Path path = new Path(pth);
		int ret = -1; //generic fail
		try {
			localFS.setTimes(path, mtime, atime);
			ret = 0;
		}
		catch (IOException e) {}
		return ret;
	}

	protected long ceph_getpos(int fh) {
		long ret = -1; //generic fail
		try {
			Object stream = files.get(new Integer(fh));
			if (stream instanceof FSDataInputStream) {
				ret = ((FSDataInputStream)stream).getPos();
			}
			else if (stream instanceof FSDataOutputStream) {
				ret = ((FSDataOutputStream)stream).getPos();
			}
		}
		catch (IOException e) {}
		catch (NullPointerException f) { }
		return ret;
	}

	protected int ceph_write(int fh, byte[] buffer,
													 int buffer_offset, int length) {
		long ret = -1;//generic fail
		try {
			FSDataOutputStream os = (FSDataOutputStream) files.get(new Integer(fh));
			long startPos = os.getPos();
			os.write(buffer, buffer_offset, length);
			ret = os.getPos() - startPos;
		}
		catch (IOException e) {}
		catch (NullPointerException f) { }
		return (int)ret;
	}

	protected int ceph_read(int fh, byte[] buffer,
													int buffer_offset, int length) {
		long ret = -1;//generic fail
		try {
			FSDataInputStream is = (FSDataInputStream)files.get(new Integer(fh));
			long startPos = is.getPos();
			is.read(buffer, buffer_offset, length);
			ret = is.getPos() - startPos;
		}
		catch (IOException e) {}
		catch (NullPointerException f) {}
		return (int)ret;
	}

	protected long ceph_seek_from_start(int fh, long pos) {
		long ret = -1;//generic fail
		try {
			FSDataInputStream is = (FSDataInputStream) files.get(new Integer(fh));
			if(is.seekToNewSource(pos)) {
				ret = is.getPos();
			}
		}
		catch (IOException e) {}
		catch (NullPointerException f) {}
		return (int)ret;
	}
}
