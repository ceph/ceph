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
 * Abstract base class for communicating with a Ceph filesystem and its
 * C++ codebase from Java, or pretending to do so (for unit testing purposes).
 * As only the Ceph package should be using this directly, all methods
 * are protected.
 */
package org.apache.hadoop.fs.ceph;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;

abstract class CephFS {

	protected static final int FATAL = 0;
	protected static final int ERROR = 1;
	protected static final int WARN = 2;
	protected static final int INFO = 3;
	protected static final int DEBUG = 4;
	protected static final int TRACE = 5;
	protected static final int NOLOG = 6;

	private boolean debug = false;
	private Log LOG;

	public CephFS(Configuration conf, Log log) {
      debug = ("true".equals(conf.get("fs.ceph.debug", "false")));
			LOG = log;
	}

	/*
	 * Performs any necessary setup to allow general use of the filesystem.
	 * Inputs:
	 *  String argsuments -- a command-line style input of Ceph config params
	 *  int block_size -- the size in bytes to use for blocks
	 * Returns: true on success, false otherwise
	 */
	abstract protected boolean ceph_initializeClient(String arguments, int block_size);
	
	/*
	 * Returns the current working directory.(absolute) as a String
	 */
	abstract protected String ceph_getcwd();
	/*
	 * Changes the working directory.
	 * Inputs:
	 *  String path: The path (relative or absolute) to switch to
	 * Returns: true on success, false otherwise.
	 */
	abstract protected boolean ceph_setcwd(String path);
	/*
	 * Given a path to a directory, removes the directory.if empty.
	 * Inputs:
	 *  jstring j_path: The path (relative or absolute) to the directory
	 * Returns: true on successful delete; false otherwise
	 */
  abstract protected boolean ceph_rmdir(String path);
	/*
	 * Given a path, unlinks it.
	 * Inputs:
	 *  String path: The path (relative or absolute) to the file or empty dir
	 * Returns: true if the unlink occurred, false otherwise.
	 */
  abstract protected boolean ceph_unlink(String path);
	/*
	 * Changes a given path name to a new name.
	 * Inputs:
	 *  jstring j_from: The path whose name you want to change.
	 *  jstring j_to: The new name for the path.
	 * Returns: true if the rename occurred, false otherwise
	 */
  abstract protected boolean ceph_rename(String old_path, String new_path);
	/*
	 * Returns true if it the input path exists, false
	 * if it does not or there is an unexpected failure.
	 */
  abstract protected boolean ceph_exists(String path);
	/*
	 * Get the block size for a given path.
	 * Input:
	 *  String path: The path (relative or absolute) you want
	 *  the block size for.
	 * Returns: block size if the path exists, otherwise a negative number
	 *  corresponding to the standard C++ error codes (which are positive).
	 */
  abstract protected long ceph_getblocksize(String path);
	/*
	 * Returns true if the given path is a directory, false otherwise.
	 */
  abstract protected boolean ceph_isdirectory(String path);
	/*
	 * Returns true if the given path is a file; false otherwise.
	 */
  abstract protected boolean ceph_isfile(String path);
	/*
	 * Get the contents of a given directory.
	 * Inputs:
	 *  String path: The path (relative or absolute) to the directory.
	 * Returns: A Java String[] of the contents of the directory, or
	 *  NULL if there is an error (ie, path is not a dir). This listing
	 *  will not contain . or .. entries.
	 */
  abstract protected String[] ceph_getdir(String path);
	/*
	 * Create the specified directory and any required intermediate ones with the
	 * given mode.
	 */
  abstract protected int ceph_mkdirs(String path, int mode);
	/*
	 * Open a file to append. If the file does not exist, it will be created.
	 * Opening a dir is possible but may have bad results.
	 * Inputs:
	 *  String path: The path to open.
	 * Returns: an int filehandle, or a number<0 if an error occurs.
	 */
  abstract protected int ceph_open_for_append(String path);
	/*
	 * Open a file for reading.
	 * Opening a dir is possible but may have bad results.
	 * Inputs:
	 *  String path: The path to open.
	 * Returns: an int filehandle, or a number<0 if an error occurs.
	 */
  abstract protected int ceph_open_for_read(String path);
	/*
	 * Opens a file for overwriting; creates it if necessary.
	 * Opening a dir is possible but may have bad results.
	 * Inputs:
	 *  String path: The path to open.
	 *  int mode: The mode to open with.
	 * Returns: an int filehandle, or a number<0 if an error occurs.
	 */
  abstract protected int ceph_open_for_overwrite(String path, int mode);
	/*
	 * Closes the given file. Returns 0 on success, or a negative
	 * error code otherwise.
	 */
  abstract protected int ceph_close(int filehandle);
	/*
	 * Change the mode on a path.
	 * Inputs:
	 *  String path: The path to change mode on.
	 *  int mode: The mode to apply.
	 * Returns: true if the mode is properly applied, false if there
	 *  is any error.
	 */
  abstract protected boolean ceph_setPermission(String path, int mode);
	/*
	 * Closes the Ceph client. This should be called before shutting down
	 * (multiple times is okay but redundant).
	 */
  abstract protected boolean ceph_kill_client();
	/*
	 * Get the statistics on a path returned in a custom format defined below.
	 * Inputs:
	 *  String path: The path to stat.
	 *  Stat fill: The stat object to fill.
	 * Returns: true if the stat is successful, false otherwise.
	 */
  abstract protected boolean ceph_stat(String path, CephFileSystem.Stat fill);
	/*
	 * Statfs a filesystem in a custom format defined in CephFileSystem.
	 * Inputs:
	 *  String path: A path on the filesystem that you wish to stat.
	 *  CephStat fill: The CephStat object to fill.
	 * Returns: true if successful and the CephStat is filled; false otherwise.
	 */
  abstract protected int ceph_statfs(String Path, CephFileSystem.CephStat fill);
	/*
	 * Check how many times a path should be replicated (if it is
	 * degraded it may not actually be replicated this often).
	 * Inputs:
	 *  String path: The path to check.
	 * Returns: an int containing the number of times replicated.
	 */
  abstract protected int ceph_replication(String path);
	/*
	 * Find the IP:port addresses of the primary OSD for a given file and offset.
	 * Inputs:
	 *  int fh: The filehandle for the file.
	 *  long offset: The offset to get the location of.
	 * Returns: a String of the location as IP, or NULL if there is an error.
	 */
  abstract protected String ceph_hosts(int fh, long offset);
	/*
	 * Set the mtime and atime for a given path.
	 * Inputs:
	 *  String path: The path to set the times for.
	 *  long mtime: The mtime to set, in millis since epoch (-1 to not set).
	 *  long atime: The atime to set, in millis since epoch (-1 to not set)
	 * Returns: 0 if successful, an error code otherwise.
	 */
  abstract protected int ceph_setTimes(String path, long mtime, long atime);
	/*
	 * Get the current position in a file (as a long) of a given filehandle.
	 * Returns: (long) current file position on success, or a
	 *  negative error code on failure.
	 */
  abstract protected long ceph_getpos(int fh);
	/*
	 * Write the given buffer contents to the given filehandle.
	 * Inputs:
	 *  int fh: The filehandle to write to.
	 *  byte[] buffer: The buffer to write from
	 *  int buffer_offset: The position in the buffer to write from
	 *  int length: The number of (sequential) bytes to write.
	 * Returns: int, on success the number of bytes written, on failure
	 *  a negative error code.
	 */
  abstract protected int ceph_write(int fh, byte[] buffer, int buffer_offset, int length);

	/*
	 * Reads into the given byte array from the current position.
	 * Inputs:
	 *  int fh: the filehandle to read from
	 *  byte[] buffer: the byte array to read into
	 *  int buffer_offset: where in the buffer to start writing
	 *  int length: how much to read.
	 * There'd better be enough space in the buffer to write all
	 * the data from the given offset!
	 * Returns: the number of bytes read on success (as an int),
	 *  or an error code otherwise.	 */
  protected native int ceph_read(int fh, byte[] buffer, int buffer_offset, int length);
	/*
	 * Seeks to the given position in the given file.
	 * Inputs:
	 *  int fh: The filehandle to seek in.
	 *  long pos: The position to seek to.
	 * Returns: the new position (as a long) of the filehandle on success,
	 *  or a negative error code on failure.	 */
  protected native long ceph_seek_from_start(int fh, long pos);
    
	protected void debug(String statement, int priority) {
    if (debug) System.err.println(statement);
		switch(priority) {
		case FATAL: LOG.fatal(statement);
			break;
		case ERROR: LOG.error(statement);
			break;
		case WARN: LOG.warn(statement);
			break;
		case INFO: LOG.info(statement);
			break;
		case DEBUG: LOG.debug(statement);
			break;
		case TRACE: LOG.trace(statement);
			break;
		case NOLOG: break;
		default: break;
		}
  }
}
