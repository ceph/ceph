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
 * Implements the Hadoop FS interfaces to allow applications to store
 * files in Ceph.
 */
package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.URI;
import java.util.EnumSet;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.CreateFlag;

/**
 * <p>
 * A {@link FileSystem} backed by <a href="http://ceph.newdream.net">Ceph.</a>.
 * This will not start a Ceph instance; one must already be running.
 * </p>
 * Configuration of the CephFileSystem is handled via a few Hadoop
 * Configuration properties: <br>
 * fs.ceph.monAddr -- the ip address/port of the monitor to connect to. <br>
 * fs.ceph.libDir -- the directory that libceph and libhadoopceph are
 * located in. This assumes Hadoop is being run on a linux-style machine
 * with names like libceph.so.
 * fs.ceph.commandLine -- if you prefer you can fill in this property
 * just as you would when starting Ceph up from the command line. Specific
 * properties override any configuration specified here.
 * <p>
 * You can also enable debugging of the CephFileSystem and Ceph itself: <br>
 * fs.ceph.debug -- if 'true' will print out method enter/exit messages,
 * plus a little more.
 * fs.ceph.clientDebug/fs.ceph.messengerDebug -- will print out debugging
 * from the respective Ceph system of at least that importance.
 */
public class CephFileSystem extends FileSystem {

  private static final int EEXIST = 17;
  private static final int ENOENT = 2;
	private static final int FATAL = 0;
	private static final int ERROR = 1;
	private static final int WARN = 2;
	private static final int INFO = 3;
	private static final int DEBUG = 4;
	private static final int TRACE = 5;
	private static final int NOLOG = 6;


  private URI uri;

  private final Path root;
  private boolean initialized = false;
	private CephFS ceph = null;

  private boolean debug = false;
  private String fs_default_name;

  /**
   * Create a new CephFileSystem.
   */
  public CephFileSystem() {
    root = new Path("/");
    debug("CephFileSystem:exit", DEBUG);
  }

  /**
   * Lets you get the URI of this CephFileSystem.
   * @return the URI.
   */
  public URI getUri() {
    if (!initialized) return null;
    debug("getUri:exit with return " + uri, DEBUG);
    return uri;
  }

  /**
   * Should be called after constructing a CephFileSystem but before calling
   * any other methods.
   * Starts up the connection to Ceph, reads in configuraton options, etc.
   * @param uri The URI for this filesystem.
   * @param conf The Hadoop Configuration to retrieve properties from.
   * @throws IOException if the Ceph client initialization fails
   * or necessary properties are unset.
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    debug("initialize:enter", DEBUG);
    if (!initialized) {
      super.initialize(uri, conf);
      setConf(conf);
      this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());  
      statistics = getStatistics(uri.getScheme(), getClass());

			if (ceph == null) {
				ceph = new CephTalker(conf);
			}
      
      fs_default_name = conf.get("fs.default.name");
      debug = ("true".equals(conf.get("fs.ceph.debug", "false")));
      //build up the arguments for Ceph
      String arguments = "CephFSInterface";
      arguments += conf.get("fs.ceph.commandLine", "");
      if (conf.get("fs.ceph.clientDebug") != null) {
				arguments += " --debug_client ";
				arguments += conf.get("fs.ceph.clientDebug");
      }
      if (conf.get("fs.ceph.messengerDebug") != null) {
				arguments += " --debug_ms ";
				arguments += conf.get("fs.ceph.messengerDebug");
      }
      if (conf.get("fs.ceph.monAddr") != null) {
				arguments += " -m ";
				arguments += conf.get("fs.ceph.monAddr");
      }
      arguments += " --client-readahead-max-periods="
				+ conf.get("fs.ceph.readahead", "1");
      //make sure they gave us a ceph monitor address or conf file
			debug("initialize:Ceph intialization arguments: " + arguments, INFO);
      if ( (conf.get("fs.ceph.monAddr") == null) &&
					 (arguments.indexOf("-m") == -1) &&
					 (arguments.indexOf("-c") == -1) ) {
				debug("initialize:You need to specify a Ceph monitor address.", FATAL);
				throw new IOException("You must specify a Ceph monitor address or config file!");
      }
      //  Initialize the client
      if (!ceph.ceph_initializeClient(arguments,
																 conf.getInt("fs.ceph.blockSize", 1<<26))) {
				debug("initialize:Ceph initialization failed!", FATAL);
				throw new IOException("Ceph initialization failed!");
      }
      initialized = true;
      debug("initialize:Ceph initialized client. Setting cwd to /", INFO);
      ceph.ceph_setcwd("/");
    }
    debug("initialize:exit", DEBUG);
  }

	/**
	 * Used for testing purposes, this version of initialize
	 * sets the given CephFS instead of defaulting to a
	 * CephTalker (with its assumed real Ceph instance to talk to).
	 */
	protected void initialize(URI uri, Configuration conf, CephFS ceph_fs)
		throws IOException{
		ceph = ceph_fs;
		initialize(uri, conf);
	}

  /**
   * Close down the CephFileSystem. Runs the base-class close method
   * and then kills the Ceph client itself.
   * @throws IOException if initialize() hasn't been called.
   */
  @Override
  public void close() throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("close:enter", DEBUG);
    super.close();//this method does stuff, make sure it's run!
		debug("close: Calling ceph_kill_client from Java", TRACE);
    ceph.ceph_kill_client();
    debug("close:exit", DEBUG);
  }

  /**
   * Get an FSDataOutputStream to append onto a file.
   * @param file The File you want to append onto
   * @param bufferSize Ceph does internal buffering; this is ignored.
   * @param progress The Progressable to report progress to.
   * Reporting is limited but exists.
   * @return An FSDataOutputStream that connects to the file on Ceph.
   * @throws IOException If initialize() hasn't been called or the file cannot be found or appended to.
   */
  public FSDataOutputStream append (Path file, int bufferSize,
																		Progressable progress) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("append:enter with path " + file + " bufferSize " + bufferSize, DEBUG);
    Path abs_path = makeAbsolute(file);
    if (progress!=null) progress.progress();
		debug("append: Entering ceph_open_for_append from Java", TRACE);
    int fd = ceph.ceph_open_for_append(abs_path.toString());
		debug("append: Returned to Java", TRACE);
    if (progress!=null) progress.progress();
    if( fd < 0 ) { //error in open
      throw new IOException("append: Open for append failed on path \"" +
														abs_path.toString() + "\"");
    }
    CephOutputStream cephOStream = new CephOutputStream(getConf(), fd);
    debug("append:exit", DEBUG);
    return new FSDataOutputStream(cephOStream, statistics);
  }

  /**
   * Get the current working directory for the given file system
   * @return the directory Path
   */
  public Path getWorkingDirectory() {
    if (!initialized) return null;
    debug("getWorkingDirectory:enter", DEBUG);
		String cwd = ceph.ceph_getcwd();
    debug("getWorkingDirectory:exit with path " + cwd, DEBUG);
    return new Path(fs_default_name + ceph.ceph_getcwd());
  }

  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it. You need to have initialized the
   * filesystem prior to calling this method.
   *
   * @param dir The directory to change to.
   */
  @Override
	public void setWorkingDirectory(Path dir) {
    if (!initialized) return;
    debug("setWorkingDirecty:enter with new working dir " + dir, DEBUG);
    Path abs_path = makeAbsolute(dir);
    debug("setWorkingDirectory:calling ceph_setcwd from Java", TRACE);
    if (!ceph.ceph_setcwd(abs_path.toString()))
      debug("setWorkingDirectory: WARNING! ceph_setcwd failed for some reason on path " + abs_path, ERROR);
    debug("setWorkingDirectory:exit", DEBUG);
  }

  /**
   * Check if a path exists.
   * Overriden because it's moderately faster than the generic implementation.
   * @param path The file to check existence on.
   * @return true if the file exists, false otherwise.
   * @throws IOException if initialize() hasn't been called.
   */
  @Override
	public boolean exists(Path path) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("exists:enter with path " + path, DEBUG);
    boolean result;
    Path abs_path = makeAbsolute(path);
    if (abs_path.equals(root)) {
      result = true;
    }
    else {
      debug("exists:Calling ceph_exists from Java on path "
											+ abs_path.toString(), TRACE);
      result =  ceph.ceph_exists(abs_path.toString());
      debug("exists:Returned from ceph_exists to Java", TRACE);
    }
    debug("exists:exit with value " + result, DEBUG);
    return result;
  }

  /**
   * Create a directory and any nonexistent parents. Any portion
   * of the directory tree can exist without error.
   * @param path The directory path to create
   * @param perms The permissions to apply to the created directories.
   * @return true if successful, false otherwise
   * @throws IOException if initialize() hasn't been called.
   */
  public boolean mkdirs(Path path, FsPermission perms) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("mkdirs:enter with path " + path, DEBUG);
    Path abs_path = makeAbsolute(path);
    debug("mkdirs:calling ceph_mkdirs from Java", TRACE);
    int result = ceph.ceph_mkdirs(abs_path.toString(), (int)perms.toShort());
    debug("mkdirs:exit with result " + result, DEBUG);
    if (result != 0)
      return false;
    else return true;
  }

  /**
   * Check if a path is a file. This is moderately faster than the
   * generic implementation.
   * @param path The path to check.
   * @return true if the path is a file, false otherwise.
   * @throws IOException if initialize() hasn't been called.
   */
  @Override
	public boolean isFile(Path path) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("isFile:enter with path " + path, DEBUG);
    Path abs_path = makeAbsolute(path);
    boolean result;
    if (abs_path.equals(root)) {
      result =  false;
    }
    else {
			debug("isFile:entering ceph_isfile from Java", TRACE);
      result = ceph.ceph_isfile(abs_path.toString());
    }
    debug("isFile:exit with result " + result, DEBUG);
    return result;
  }

  /**
   * Check if a path is a directory. This is moderately faster than
   * the generic implementation.
   * @param path The path to check
   * @return true if the path is a directory, false otherwise.
   * @throws IOException if initialize() hasn't been called.
   */
  @Override
	public boolean isDirectory(Path path) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("isDirectory:enter with path " + path, DEBUG);
    Path abs_path = makeAbsolute(path);
    boolean result;
    if (abs_path.equals(root)) {
      result = true;
    }
    else {
      debug("calling ceph_isdirectory from Java", TRACE);
      result = ceph.ceph_isdirectory(abs_path.toString());
      debug("Returned from ceph_isdirectory to Java", TRACE);
    }
    debug("isDirectory:exit with result " + result, DEBUG);
    return result;
  }

  /**
   * Get stat information on a file. This does not fill owner or group, as
   * Ceph's support for these is a bit different than HDFS'.
   * @param path The path to stat.
   * @return FileStatus object containing the stat information.
   * @throws IOException if initialize() hasn't been called
   * @throws FileNotFoundException if the path could not be resolved.
   */
  public FileStatus getFileStatus(Path path) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("getFileStatus:enter with path " + path, DEBUG);
    Path abs_path = makeAbsolute(path);
    //sadly, Ceph doesn't really do uids/gids just yet, but
    //everything else is filled
    FileStatus status;
    Stat lstat = new Stat();
		debug("getFileStatus: calling ceph_stat from Java", TRACE);
    if(ceph.ceph_stat(abs_path.toString(), lstat)) {
      status = new FileStatus(lstat.size, lstat.is_dir,
															ceph.ceph_replication(abs_path.toString()),
															lstat.block_size,
															lstat.mod_time,
															lstat.access_time,
															new FsPermission((short)lstat.mode),
															null,
															null,
															new Path(fs_default_name+abs_path.toString()));
    }
    else { //fail out
			throw new FileNotFoundException("org.apache.hadoop.fs.ceph.CephFileSystem: File "
																			+ path + " does not exist or could not be accessed");
    }

    debug("getFileStatus:exit", DEBUG);
    return status;
  }

  /**
   * Get the FileStatus for each listing in a directory.
   * @param path The directory to get listings from.
   * @return FileStatus[] containing one FileStatus for each directory listing;
   * null if path is not a directory.
   * @throws IOException if initialize() hasn't been called.
   * @throws FileNotFoundException if the input path can't be found.
   */
  public FileStatus[] listStatus(Path path) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("listStatus:enter with path " + path, DEBUG);
    Path abs_path = makeAbsolute(path);
    Path[] paths = listPaths(abs_path);
    if (paths != null) {
      FileStatus[] statuses = new FileStatus[paths.length];
      for (int i = 0; i < paths.length; ++i) {
				statuses[i] = getFileStatus(paths[i]);
      }
      debug("listStatus:exit", DEBUG);
      return statuses;
    }
    if (!isFile(path)) throw new FileNotFoundException(); //if we get here, listPaths returned null
    //which means that the input wasn't a directory, so throw an Exception if it's not a file
    return null; //or return null if it's a file
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
		debug("setPermission:enter with path " + p
					+ " and permissions " + permission, DEBUG);
    Path abs_path = makeAbsolute(p);
		debug("setPermission:calling ceph_setpermission from Java", TRACE);
    ceph.ceph_setPermission(abs_path.toString(), permission.toShort());
		debug("setPermission:exit", DEBUG);
  }

  /**
   * Set access/modification times of a file.
   * @param p The path
   * @param mtime Set modification time in number of millis since Jan 1, 1970.
   * @param atime Set access time in number of millis since Jan 1, 1970.
   */
	@Override
	public void setTimes(Path p, long mtime, long atime) throws IOException {
		if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
		debug("setTimes:enter with path " + p + " mtime:" + mtime +
					" atime:" + atime, DEBUG);
		Path abs_path = makeAbsolute(p);
		debug("setTimes:calling ceph_setTimes from Java", TRACE);
		int r = ceph.ceph_setTimes(abs_path.toString(), mtime, atime);
		if (r<0) throw new IOException ("Failed to set times on path "
																		+ abs_path.toString() + " Error code: " + r);
		debug("setTimes:exit", DEBUG);
	}

  /**
   * Create a new file and open an FSDataOutputStream that's connected to it.
   * @param path The file to create.
   * @param permission The permissions to apply to the file.
   * @param flag If CreateFlag.OVERWRITE, overwrite any existing
   * file with this name; otherwise don't.
   * @param bufferSize Ceph does internal buffering; this is ignored.
   * @param replication Ignored by Ceph. This can be
   * configured via Ceph configuration.
   * @param blockSize Ignored by Ceph. You can set client-wide block sizes
   * via the fs.ceph.blockSize param if you like.
   * @param progress A Progressable to report back to.
   * Reporting is limited but exists.
   * @return An FSDataOutputStream pointing to the created file.
   * @throws IOException if initialize() hasn't been called, or the path is an
   * existing directory, or the path exists but overwrite is false, or there is a
   * failure in attempting to open for append with Ceph.
   */
  public FSDataOutputStream create(Path path,
																	 FsPermission permission,
																	 EnumSet<CreateFlag> flag,
																	 //boolean overwrite,
																	 int bufferSize,
																	 short replication,
																	 long blockSize,
																	 Progressable progress
																	 ) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("create:enter with path " + path, DEBUG);
    Path abs_path = makeAbsolute(path);
    if (progress!=null) progress.progress();
    // We ignore replication since that's not configurable here, and
    // progress reporting is quite limited.
    // Required semantics: if the file exists, overwrite if CreateFlag.OVERWRITE;
    // throw an exception if !CreateFlag.OVERWRITE.

    // Step 1: existence test
    boolean exists = exists(abs_path);
    if (exists) {
      if(isDirectory(abs_path))
				throw new IOException("create: Cannot overwrite existing directory \""
															+ path.toString() + "\" with a file");
      //if (!overwrite)
      if (!flag.contains(CreateFlag.OVERWRITE))
				throw new IOException("createRaw: Cannot open existing file \"" 
															+ abs_path.toString() 
															+ "\" for writing without overwrite flag");
    }

    if (progress!=null) progress.progress();

    // Step 2: create any nonexistent directories in the path
    if (!exists) {
      Path parent = abs_path.getParent();
      if (parent != null) { // if parent is root, we're done
				int r = ceph.ceph_mkdirs(parent.toString(), permission.toShort());
				if (!(r==0 || r==-EEXIST))
					throw new IOException ("Error creating parent directory; code: " + r);
      }
      if (progress!=null) progress.progress();
    }
    // Step 3: open the file
    debug("calling ceph_open_for_overwrite from Java", TRACE);
    int fh = ceph.ceph_open_for_overwrite(abs_path.toString(), (int)permission.toShort());
    if (progress!=null) progress.progress();
    debug("Returned from ceph_open_for_overwrite to Java with fh " + fh, TRACE);
    if (fh < 0) {
      throw new IOException("create: Open for overwrite failed on path \"" + 
														path.toString() + "\"");
    }
      
    // Step 4: create the stream
    OutputStream cephOStream = new CephOutputStream(getConf(), fh);
    debug("create:exit", DEBUG);
    return new FSDataOutputStream(cephOStream, statistics);
	}

  /**
   * Open a Ceph file and attach the file handle to an FSDataInputStream.
   * @param path The file to open
   * @param bufferSize Ceph does internal buffering; this is ignored.
   * @return FSDataInputStream reading from the given path.
   * @throws IOException if initialize() hasn't been called, the path DNE or is a
   * directory, or there is an error getting data to set up the FSDataInputStream.
   */
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("open:enter with path " + path, DEBUG);
    Path abs_path = makeAbsolute(path);
    
    int fh = ceph.ceph_open_for_read(abs_path.toString());
    if (fh < 0) { //uh-oh, something's bad!
      if (fh == -ENOENT) //well that was a stupid open
				throw new IOException("open:  absolute path \""  + abs_path.toString()
															+ "\" does not exist");
      else //hrm...the file exists but we can't open it :(
				throw new IOException("open: Failed to open file " + abs_path.toString());
    }

    if(isDirectory(abs_path)) { //yes, it is possible to open Ceph directories
      //but that doesn't mean you should in Hadoop!
      ceph.ceph_close(fh);
      throw new IOException("open:  absolute path \""  + abs_path.toString()
														+ "\" is a directory!");
    }
    Stat lstat = new Stat();
		debug("open:calling ceph_stat from Java", TRACE);
    ceph.ceph_stat(abs_path.toString(), lstat);
		debug("open:returned to Java", TRACE);
    long size = lstat.size;
    if (size < 0) {
      throw new IOException("Failed to get file size for file " + abs_path.toString() + 
														" but succeeded in opening file. Something bizarre is going on.");
    }
    FSInputStream cephIStream = new CephInputStream(getConf(), fh, size);
    debug("open:exit", DEBUG);
    return new FSDataInputStream(cephIStream);
	}

  /**
   * Rename a file or directory.
   * @param src The current path of the file/directory
   * @param dst The new name for the path.
   * @return true if the rename succeeded, false otherwise.
   * @throws IOException if initialize() hasn't been called.
   */
  @Override
	public boolean rename(Path src, Path dst) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("rename:enter with src:" + src + " and dest:" + dst, DEBUG);
    Path abs_src = makeAbsolute(src);
    Path abs_dst = makeAbsolute(dst);
    debug("calling ceph_rename from Java", TRACE);
    boolean result = ceph.ceph_rename(abs_src.toString(), abs_dst.toString());
    debug("rename:exit with result: " + result, DEBUG);
    return result;
  }

  /**
   * Get a BlockLocation object for each block in a file.
   *
   * Note that this doesn't include port numbers in the name field as
   * Ceph handles slow/down servers internally. This data should be used
   * only for selecting which servers to run which jobs on.
   *
   * @param file A FileStatus object corresponding to the file you want locations for.
   * @param start The offset of the first part of the file you are interested in.
   * @param len The amount of the file past the offset you are interested in.
   * @return A BlockLocation[] where each object corresponds to a block within
   * the given range.
   * @throws IOException if initialize() hasn't been called.
   */
  @Override
	public BlockLocation[] getFileBlockLocations(FileStatus file,
																								 long start, long len) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
		debug("getFileBlockLocations:enter with path " + file.getPath() +
					", start pos " + start + ", length " + len, DEBUG);
    //sanitize and get the filehandle
    Path abs_path = makeAbsolute(file.getPath());
		debug("getFileBlockLocations:call ceph_open_for_read from Java", TRACE);
    int fh = ceph.ceph_open_for_read(abs_path.toString());
		debug("getFileBlockLocations:return from ceph_open_for_read to Java with fh "
					+ fh, TRACE);
    if (fh < 0) {
			debug("getFileBlockLocations:got error " + fh +
						", exiting and returning null!", ERROR);
      return null;
    }
    //get the block size
		debug("getFileBlockLocations:call ceph_getblocksize from Java", TRACE);
    long blockSize = ceph.ceph_getblocksize(abs_path.toString());
		debug("getFileBlockLocations:return from ceph_getblocksize", TRACE);
    BlockLocation[] locations =
      new BlockLocation[(int)Math.ceil(len/(float)blockSize)];
		long offset;
    for (int i = 0; i < locations.length; ++i) {
			offset = start + i*blockSize;
			debug("getFileBlockLocations:call ceph_hosts from Java on fh "
						+ fh + " and offset " + offset, TRACE);
      String host = ceph.ceph_hosts(fh, offset);
			debug("getFileBlockLocations:return from ceph_hosts to Java with host "
						+ host, TRACE);
      String[] hostArray = new String[1];
      hostArray[0] = host;
      locations[i] = new BlockLocation(hostArray, hostArray,
																			 start+i*blockSize-(start % blockSize),
																			 blockSize);
    }
		debug("getFileBlockLocations:call ceph_close from Java on fh "
					+ fh, TRACE);
    ceph.ceph_close(fh);
		debug("getFileBlockLocations:return with " + locations.length
					+ " locations", DEBUG);
    return locations;
  }
  
  /**
   * Get usage statistics on the Ceph filesystem.
   * @param path A path to the partition you're interested in.
   * Ceph doesn't partition, so this is ignored.
   * @return FsStatus reporting capacity, usage, and remaining spac.
   * @throws IOException if initialize() hasn't been called, or the
   * stat somehow fails.
   */
  @Override
	public FsStatus getStatus (Path path) throws IOException {
    if (!initialized) throw new IOException("You have to initialize the "
																						+ " CephFileSystem before calling other methods.");
    debug("getStatus:enter with path " + path, DEBUG);
    Path abs_path = makeAbsolute(path);
    
    //currently(Ceph .16) Ceph actually ignores the path
    //but we still pass it in; if Ceph stops ignoring we may need more
    //error-checking code.
    CephStat ceph_stat = new CephStat();
		debug("getStatus:calling ceph_statfs from Java", TRACE);
    int result = ceph.ceph_statfs(abs_path.toString(), ceph_stat);
    if (result!=0) throw new IOException("Somehow failed to statfs the Ceph filesystem. Error code: " + result);
    debug("getStatus:exit successfully", DEBUG);
    return new FsStatus(ceph_stat.capacity,
												ceph_stat.used, ceph_stat.remaining);
  }

  /**
   * Delete the given path, and optionally its children.
   * @param path the path to delete.
   * @param recursive If the path is a directory and this is false,
   * delete will throw an IOException. If path is a file this is ignored.
   * @return true if the delete succeeded, false otherwise (including if
   * path doesn't exist).
   * @throws IOException if initialize() hasn't been called,
   * or you attempt to non-recursively delete a directory,
   * or you attempt to delete the root directory.
   */
  public boolean delete(Path path, boolean recursive) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the "
																						 +"CephFileSystem before calling other methods.");
    debug("delete:enter with path " + path + " and recursive=" + recursive,
					DEBUG);
    Path abs_path = makeAbsolute(path);
    
    // sanity check
    if (abs_path.equals(root))
      throw new IOException("Error: deleting the root directory is a Bad Idea.");
    if (!exists(abs_path)) return false;

    // if the path is a file, try to delete it.
    if (isFile(abs_path)) {
			debug("delete:calling ceph_unlink from Java with path " + abs_path,
						TRACE);
      boolean result = ceph.ceph_unlink(abs_path.toString());
      if(!result)
				debug("delete: failed to delete file \"" +
												abs_path.toString() + "\".", ERROR);
      debug("delete:exit with success=" + result, DEBUG);
      return result;
    }

    /* The path is a directory, so recursively try to delete its contents,
       and then delete the directory. */
    if (!recursive) {
      throw new IOException("Directories must be deleted recursively!");
    }
    //get the entries; listPaths will remove . and .. for us
    Path[] contents = listPaths(abs_path);
    if (contents == null) {
      debug("delete: Failed to read contents of directory \"" +
						abs_path.toString() +
						"\" while trying to delete it, BAILING", ERROR);
      return false;
    }
    // delete the entries
		debug("delete: recursively calling delete on contents of "
					+ abs_path, DEBUG);
    for (Path p : contents) {
      if (!delete(p, true)) {
				debug("delete: Failed to delete file \"" + 
												p.toString() + "\" while recursively deleting \""
												+ abs_path.toString() + "\", BAILING", ERROR );
				return false;
      }
    }
    //if we've come this far it's a now-empty directory, so delete it!
    boolean result = ceph.ceph_rmdir(abs_path.toString());
    if (!result)
      debug("delete: failed to delete \"" + abs_path.toString()
						+ "\", BAILING", ERROR);
    debug("delete:exit", DEBUG);
    return result;
  }

  /**
   * Returns the default replication value of 1. This may
   * NOT be the actual value, as replication is controlled
   * by a separate Ceph configuration.
   */
  @Override
	public short getDefaultReplication() {
    return 1;
  }

  /**
   * Get the default block size.
   * @return the default block size, in bytes, as a long.
   */
  @Override
	public long getDefaultBlockSize() {
    return getConf().getInt("fs.ceph.blockSize", 1<<26);
  }

  // Makes a Path absolute. In a cheap, dirty hack, we're
  // also going to strip off any fs_default_name prefix we see. 
  private Path makeAbsolute(Path path) {
    debug("makeAbsolute:enter with path " + path, NOLOG);
    if (path == null) return new Path("/");
    // first, check for the prefix
    if (path.toString().startsWith(fs_default_name)) {	  
      Path stripped_path = new Path(path.toString().substring(fs_default_name.length()));
      debug("makeAbsolute:exit with path " + stripped_path, NOLOG);
      return stripped_path;
    }

    if (path.isAbsolute()) {
      debug("makeAbsolute:exit with path " + path, NOLOG);
      return path;
    }
    Path new_path = new Path(ceph.ceph_getcwd(), path);
    debug("makeAbsolute:exit with path " + new_path, NOLOG);
    return new_path;
  }
 
  private Path[] listPaths(Path path) throws IOException {
    debug("listPaths:enter with path " + path, NOLOG);
    String dirlist[];

    Path abs_path = makeAbsolute(path);

    // If it's a directory, get the listing. Otherwise, complain and give up.
    debug("calling ceph_getdir from Java with path " + abs_path, NOLOG);
    dirlist = ceph.ceph_getdir(abs_path.toString());
    debug("returning from ceph_getdir to Java", NOLOG);

    if (dirlist == null) {
      throw new IOException("listPaths: path " + path.toString() + " is not a directory.");
    }
    
    // convert the strings to Paths
    Path[] paths = new Path[dirlist.length];
    for (int i = 0; i < dirlist.length; ++i) {
      debug("Raw enumeration of paths in \"" + abs_path.toString() + "\": \"" +
											dirlist[i] + "\"", NOLOG);
      // convert each listing to an absolute path
      Path raw_path = new Path(dirlist[i]);
      if (raw_path.isAbsolute())
				paths[i] = raw_path;
      else
				paths[i] = new Path(abs_path, raw_path);
    }
    debug("listPaths:exit", NOLOG);
    return paths;
  }

  private void debug(String statement, int priority) {
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

  static class Stat {
    public long size;
    public boolean is_dir;
    public long block_size;
    public long mod_time;
    public long access_time;
    public int mode;

    public Stat(){}
  }

  static class CephStat {
    public long capacity;
    public long used;
    public long remaining;

    public CephStat() {}
  }
}
