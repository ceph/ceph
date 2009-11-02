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

import java.net.URI;
import java.util.Hashtable;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

class CephFaker extends CephFS {
	
	FileSystem localFS;
	String localPrefix;
	int blockSize;
	Configuration conf;
	Hashtable<Integer, Object> files;
	Hashtable<Integer, String> filenames;
	int fileCount = 0;
	boolean initialized = false;
	
	public CephFaker(Configuration con, Log log) {
		super(con, log);
		conf = con;
		files = new Hashtable<Integer, Object>();
		filenames = new Hashtable<Integer, String>();
	}
	
	protected boolean ceph_initializeClient(String args, int block_size) {
		if (!initialized) {
			//let's remember the default block_size
			blockSize = block_size;
			/* for a real Ceph deployment, this starts up the client, 
			 * sets debugging levels, etc. We just need to get the
			 * local FileSystem to use, and we'll ignore any
			 * command-line arguments. */
			try {
				localFS = FileSystem.getLocal(conf);
				localFS.initialize(URI.create("file://localhost"), conf);
				localFS.setVerifyChecksum(false);
				String testDir = conf.get("hadoop.tmp.dir");
				localPrefix = localFS.getWorkingDirectory().toString();
				int testDirLoc = localPrefix.indexOf(testDir) - 1;
				if (-2 == testDirLoc)
					testDirLoc = localPrefix.length();
				localPrefix = localPrefix.substring(0, testDirLoc)
					+ "/" + conf.get("hadoop.tmp.dir");

				localFS.setWorkingDirectory(new Path(localPrefix
																						 +"/user/"
																						 + System.getProperty("user.name")));
				//I don't know why, but the unit tests expect the default
				//working dir to be /user/username, so satisfy them!
				//debug("localPrefix is " + localPrefix, INFO);
			}
			catch (IOException e) {
				return false;
			}
			initialized = true;
		}
		return true;
	}

	protected String ceph_getcwd() {
		return sanitize_path(localFS.getWorkingDirectory().toString());
	}

	protected boolean ceph_setcwd(String path) {
		localFS.setWorkingDirectory(new Path(prepare_path(path)));
		return true;
	}

	//the caller is responsible for ensuring empty dirs
	protected boolean ceph_rmdir(String pth) {
		Path path = new Path(prepare_path(pth));
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
		path = prepare_path(path);
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
		oldName = prepare_path(oldName);
		newName = prepare_path(newName);
		try {
			Path parent = new Path(newName).getParent();
			Path newPath = new Path(newName);
			if (localFS.exists(parent) && !localFS.exists(newPath))
				return localFS.rename(new Path(oldName), newPath);
			return false;
		}
		catch (IOException e) { return false; }
	}

	protected boolean ceph_exists(String path) {
		path = prepare_path(path);
		boolean ret = false;
		try {
			ret = localFS.exists(new Path(path));
		}
		catch (IOException e){ }
		return ret;
	}

	protected long ceph_getblocksize(String path) {
		path = prepare_path(path);
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
		path = prepare_path(path);
		try {
			FileStatus status = localFS.getFileStatus(new Path(path));
			return status.isDir();
		}
		catch (IOException e) { return false; }
	}

	protected boolean ceph_isfile(String path) {
		path = prepare_path(path);
		boolean ret = false;
		try {
			FileStatus status = localFS.getFileStatus(new Path(path));
			ret = !status.isDir();
		}
		catch (Exception e) {}
		return ret;
	}

	protected String[] ceph_getdir(String path) {
		path = prepare_path(path);
		if (!ceph_isdirectory(path)) {
			return null;
		}
		try {
			FileStatus[] stats = localFS.listStatus(new Path(path));
			String[] names = new String[stats.length];
			String name;
			for (int i=0; i<stats.length; ++i) {
				name = stats[i].getPath().toString();
				names[i] = name.substring(name.lastIndexOf(Path.SEPARATOR)+1);
			}
			return names;
		}
		catch (IOException e) {}
		return null;
	}

	protected int ceph_mkdirs(String path, int mode) {
		path = prepare_path(path);
		//debug("ceph_mkdirs on " + path, INFO);
		try {
			if(localFS.mkdirs(new Path(path), new FsPermission((short)mode)))
				return 0;
		}
		catch (FileAlreadyExistsException fe) { return ENOTDIR; }
		catch (IOException e) {}
		if (ceph_isdirectory(path))
			return -EEXIST; //apparently it already existed
		return -1;
	}

	/* 
	 * Unlike a real Ceph deployment, you can't do opens on a directory.
	 * Since that has unpredictable behavior and you shouldn't do it anyway,
	 * it's okay.
	 */
	protected int ceph_open_for_append(String path) {
		path = prepare_path(path);
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
		path = prepare_path(path);
		FSDataInputStream stream;
		try {
			stream = localFS.open(new Path(path));
			files.put(new Integer(fileCount), stream);
			filenames.put(new Integer(fileCount), path);
			debug("ceph_open_for_read fh:" + fileCount
						+ ", pathname:" + path, INFO);
			return fileCount++;
		}
		catch (IOException e) { }
		return -1; //failure
	}

	protected int ceph_open_for_overwrite(String path, int mode) {
		path = prepare_path(path);
		FSDataOutputStream stream;
		try {
			stream = localFS.create(new Path(path));
			files.put(new Integer(fileCount), stream);
			filenames.put(new Integer(fileCount), path);
			debug("ceph_open_for_overwrite fh:" + fileCount
						+ ", pathname:" + path, INFO);
			return fileCount++;
		}
		catch (IOException e) { }
		return -1; //failure
	}

	protected int ceph_close(int filehandle) {
		debug("ceph_close(filehandle " + filehandle + ")", INFO);
		try {
			((Closeable)files.get(new Integer(filehandle))).close();
			if(null == files.get(new Integer(filehandle))) {
				return -ENOENT; //this isn't quite the right error code,
				// but the important part is it's negative
			}
			return 0; //hurray, success
		}
		catch (NullPointerException ne) {
			debug("ceph_close caught NullPointerException!" + ne, WARN);
		} //err, how?
		catch (IOException ie) {
			debug("ceph_close caught IOException!" + ie, WARN);
		}
		return -1; //failure
	}

	protected boolean ceph_setPermission(String pth, int mode) {
		pth = prepare_path(pth);
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
		//debug("ceph_kill_client", INFO);
		localFS.setWorkingDirectory(new Path(localPrefix));
		//debug("working dir is now " + localFS.getWorkingDirectory(), INFO);
		try{
			localFS.close(); }
		catch (Exception e) {}
		localFS = null;
		files = null;
		filenames = null;
		return true;
	}

	protected boolean ceph_stat(String pth, CephFileSystem.Stat fill) {
		pth = prepare_path(pth);
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
		pth = prepare_path(pth);
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
		path = prepare_path(path);
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
		pth = prepare_path(pth);
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
		debug("ceph_write fh:" + fh + ", buffer_offset:" + buffer_offset
					+ ", length:" + length, INFO);
		long ret = -1;//generic fail
		try {
			FSDataOutputStream os = (FSDataOutputStream) files.get(new Integer(fh));
			debug("ceph_write got outputstream", INFO);
			long startPos = os.getPos();
			os.write(buffer, buffer_offset, length);
			ret = os.getPos() - startPos;
		}
		catch (IOException e) { debug("ceph_write caught IOException!", WARN);}
		catch (NullPointerException f) {
			debug("ceph_write caught NullPointerException!", WARN); }
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
		debug("ceph_seek_from_start(fh " + fh + ", pos " + pos + ")", INFO);
		long ret = -1;//generic fail
		try {
			debug("ceph_seek_from_start filename is "
						+ filenames.get(new Integer(fh)), INFO);
			if (null == files.get(new Integer(fh))) {
				debug("ceph_seek_from_start: is is null!", WARN);
			}
			FSDataInputStream is = (FSDataInputStream) files.get(new Integer(fh));
			debug("ceph_seek_from_start retrieved is!", INFO);
			is.seek(pos);
			ret = is.getPos();
		}
		catch (IOException e) {debug("ceph_seek_from_start caught IOException!", WARN);}
		catch (NullPointerException f) {debug("ceph_seek_from_start caught NullPointerException!", WARN);}
		return (int)ret;
	}

	/*
	 * We need to remove the localFS file prefix before returning to Ceph
	 */
	private String sanitize_path(String path) {
		//debug("sanitize_path(" + path + ")", INFO);
		/*		if (path.startsWith("file:"))
					path = path.substring("file:".length()); */
		if (path.startsWith(localPrefix)) {
			path = path.substring(localPrefix.length());
			if (path.length() == 0) //it was a root path
				path = "/";
		}
		//debug("sanitize_path returning " + path, INFO);
		return path;
	}

	/*
	 * If it's an absolute path we need to shove the
	 * test dir onto the front as a prefix.
	 */
	private String prepare_path(String path) {
		//debug("prepare_path(" + path + ")", INFO);
		if (path.startsWith("/"))
			path = localPrefix + path;
		else if (path.equals("..")) {
			if (ceph_getcwd().equals("/"))
				path = "."; // you can't go up past root!
		}
		//debug("prepare_path returning" + path, INFO);
		return path;
	}
}
