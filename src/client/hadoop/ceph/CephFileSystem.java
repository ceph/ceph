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
  }

  /**
   * Lets you get the URI of this CephFileSystem.
   * @return the URI.
   */
  public URI getUri() {
    if (!initialized) return null;
    ceph.debug("getUri:exit with return " + uri, ceph.DEBUG);
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
    if (!initialized) {
      super.initialize(uri, conf);
      setConf(conf);
      this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());  
      statistics = getStatistics(uri.getScheme(), getClass());

			if (ceph == null) {
				ceph = new CephTalker(conf, LOG);
			}
      
      fs_default_name = conf.get("fs.default.name");
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
			ceph.debug("initialize:Ceph intialization arguments: " + arguments, ceph.INFO);
      if ( (conf.get("fs.ceph.monAddr") == null) &&
					 (arguments.indexOf("-m") == -1) &&
					 (arguments.indexOf("-c") == -1) ) {
				ceph.debug("initialize:You need to specify a Ceph monitor address.", ceph.FATAL);
				throw new IOException("You must specify a Ceph monitor address or config file!");
      }
      //  Initialize the client
      if (!ceph.ceph_initializeClient(arguments,
																 conf.getInt("fs.ceph.blockSize", 1<<26))) {
				ceph.debug("initialize:Ceph initialization failed!", ceph.FATAL);
				throw new IOException("Ceph initialization failed!");
      }
      initialized = true;
      ceph.debug("initialize:Ceph initialized client. Setting cwd to /", ceph.INFO);
      ceph.ceph_setcwd("/");
    }
    ceph.debug("initialize:exit", ceph.DEBUG);
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
    ceph.debug("close:enter", ceph.DEBUG);
    super.close();//this method does stuff, make sure it's run!
		ceph.debug("close: Calling ceph_kill_client from Java", ceph.TRACE);
    ceph.ceph_kill_client();
    ceph.debug("close:exit", ceph.DEBUG);
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
    ceph.debug("append:enter with path " + file + " bufferSize " + bufferSize, ceph.DEBUG);
    Path abs_path = makeAbsolute(file);
    if (progress!=null) progress.progress();
		ceph.debug("append: Entering ceph_open_for_append from Java", ceph.TRACE);
    int fd = ceph.ceph_open_for_append(abs_path.toString());
		ceph.debug("append: Returned to Java", ceph.TRACE);
    if (progress!=null) progress.progress();
    if( fd < 0 ) { //error in open
      throw new IOException("append: Open for append failed on path \"" +
														abs_path.toString() + "\"");
    }
    CephOutputStream cephOStream = new CephOutputStream(getConf(), ceph, fd);
    ceph.debug("append:exit", ceph.DEBUG);
    return new FSDataOutputStream(cephOStream, statistics);
  }

  /**
   * Get the current working directory for the given file system
   * @return the directory Path
   */
  public Path getWorkingDirectory() {
    if (!initialized) return null;
    ceph.debug("getWorkingDirectory:enter", ceph.DEBUG);
		String cwd = ceph.ceph_getcwd();
    ceph.debug("getWorkingDirectory:exit with path " + cwd, ceph.DEBUG);
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
    ceph.debug("setWorkingDirecty:enter with new working dir " + dir, ceph.DEBUG);
    Path abs_path = makeAbsolute(dir);
    ceph.debug("setWorkingDirectory:calling ceph_setcwd from Java", ceph.TRACE);
    if (!ceph.ceph_setcwd(abs_path.toString()))
      ceph.debug("setWorkingDirectory: WARNING! ceph_setcwd failed for some reason on path " + abs_path, ceph.ERROR);
    ceph.debug("setWorkingDirectory:exit", ceph.DEBUG);
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
    ceph.debug("exists:enter with path " + path, ceph.DEBUG);
    boolean result;
    Path abs_path = makeAbsolute(path);
    if (abs_path.equals(root)) {
      result = true;
    }
    else {
      ceph.debug("exists:Calling ceph_exists from Java on path "
											+ abs_path.toString(), ceph.TRACE);
      result =  ceph.ceph_exists(abs_path.toString());
      ceph.debug("exists:Returned from ceph_exists to Java", ceph.TRACE);
    }
    ceph.debug("exists:exit with value " + result, ceph.DEBUG);
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
    ceph.debug("mkdirs:enter with path " + path, ceph.DEBUG);
    Path abs_path = makeAbsolute(path);
    ceph.debug("mkdirs:calling ceph_mkdirs from Java", ceph.TRACE);
    int result = ceph.ceph_mkdirs(abs_path.toString(), (int)perms.toShort());
    ceph.debug("mkdirs:exit with result " + result, ceph.DEBUG);
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
    ceph.debug("isFile:enter with path " + path, ceph.DEBUG);
    Path abs_path = makeAbsolute(path);
    boolean result;
    if (abs_path.equals(root)) {
      result =  false;
    }
    else {
			ceph.debug("isFile:entering ceph_isfile from Java", ceph.TRACE);
      result = ceph.ceph_isfile(abs_path.toString());
    }
    ceph.debug("isFile:exit with result " + result, ceph.DEBUG);
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
    ceph.debug("isDirectory:enter with path " + path, ceph.DEBUG);
    Path abs_path = makeAbsolute(path);
    boolean result;
    if (abs_path.equals(root)) {
      result = true;
    }
    else {
      ceph.debug("calling ceph_isdirectory from Java", ceph.TRACE);
      result = ceph.ceph_isdirectory(abs_path.toString());
      ceph.debug("Returned from ceph_isdirectory to Java", ceph.TRACE);
    }
    ceph.debug("isDirectory:exit with result " + result, ceph.DEBUG);
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
    ceph.debug("getFileStatus:enter with path " + path, ceph.DEBUG);
    Path abs_path = makeAbsolute(path);
    //sadly, Ceph doesn't really do uids/gids just yet, but
    //everything else is filled
    FileStatus status;
    Stat lstat = new Stat();
		ceph.debug("getFileStatus: calling ceph_stat from Java", ceph.TRACE);
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

    ceph.debug("getFileStatus:exit", ceph.DEBUG);
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
    ceph.debug("listStatus:enter with path " + path, ceph.DEBUG);
    Path abs_path = makeAbsolute(path);
    Path[] paths = listPaths(abs_path);
    if (paths != null) {
      FileStatus[] statuses = new FileStatus[paths.length];
      for (int i = 0; i < paths.length; ++i) {
				statuses[i] = getFileStatus(paths[i]);
      }
      ceph.debug("listStatus:exit", ceph.DEBUG);
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
		ceph.debug("setPermission:enter with path " + p
					+ " and permissions " + permission, ceph.DEBUG);
    Path abs_path = makeAbsolute(p);
		ceph.debug("setPermission:calling ceph_setpermission from Java", ceph.TRACE);
    ceph.ceph_setPermission(abs_path.toString(), permission.toShort());
		ceph.debug("setPermission:exit", ceph.DEBUG);
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
		ceph.debug("setTimes:enter with path " + p + " mtime:" + mtime +
					" atime:" + atime, ceph.DEBUG);
		Path abs_path = makeAbsolute(p);
		ceph.debug("setTimes:calling ceph_setTimes from Java", ceph.TRACE);
		int r = ceph.ceph_setTimes(abs_path.toString(), mtime, atime);
		if (r<0) throw new IOException ("Failed to set times on path "
																		+ abs_path.toString() + " Error code: " + r);
		ceph.debug("setTimes:exit", ceph.DEBUG);
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
    ceph.debug("create:enter with path " + path, ceph.DEBUG);
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
				if (!(r==0 || r==-ceph.EEXIST))
					throw new IOException ("Error creating parent directory; code: " + r);
      }
      if (progress!=null) progress.progress();
    }
    // Step 3: open the file
    ceph.debug("calling ceph_open_for_overwrite from Java", ceph.TRACE);
    int fh = ceph.ceph_open_for_overwrite(abs_path.toString(), (int)permission.toShort());
    if (progress!=null) progress.progress();
    ceph.debug("Returned from ceph_open_for_overwrite to Java with fh " + fh, ceph.TRACE);
    if (fh < 0) {
      throw new IOException("create: Open for overwrite failed on path \"" + 
														path.toString() + "\"");
    }
      
    // Step 4: create the stream
    OutputStream cephOStream = new CephOutputStream(getConf(), ceph, fh);
    ceph.debug("create:exit", ceph.DEBUG);
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
    ceph.debug("open:enter with path " + path, ceph.DEBUG);
    Path abs_path = makeAbsolute(path);
    
    int fh = ceph.ceph_open_for_read(abs_path.toString());
    if (fh < 0) { //uh-oh, something's bad!
      if (fh == -ceph.ENOENT) //well that was a stupid open
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
		ceph.debug("open:calling ceph_stat from Java", ceph.TRACE);
    ceph.ceph_stat(abs_path.toString(), lstat);
		ceph.debug("open:returned to Java", ceph.TRACE);
    long size = lstat.size;
    if (size < 0) {
      throw new IOException("Failed to get file size for file " + abs_path.toString() + 
														" but succeeded in opening file. Something bizarre is going on.");
    }
    FSInputStream cephIStream = new CephInputStream(getConf(), ceph, fh, size);
    ceph.debug("open:exit", ceph.DEBUG);
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
    ceph.debug("rename:enter with src:" + src + " and dest:" + dst, ceph.DEBUG);
    Path abs_src = makeAbsolute(src);
    Path abs_dst = makeAbsolute(dst);
    ceph.debug("calling ceph_rename from Java", ceph.TRACE);
    boolean result = ceph.ceph_rename(abs_src.toString(), abs_dst.toString());
    ceph.debug("rename:exit with result: " + result, ceph.DEBUG);
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
		ceph.debug("getFileBlockLocations:enter with path " + file.getPath() +
					", start pos " + start + ", length " + len, ceph.DEBUG);
    //sanitize and get the filehandle
    Path abs_path = makeAbsolute(file.getPath());
		ceph.debug("getFileBlockLocations:call ceph_open_for_read from Java", ceph.TRACE);
    int fh = ceph.ceph_open_for_read(abs_path.toString());
		ceph.debug("getFileBlockLocations:return from ceph_open_for_read to Java with fh "
					+ fh, ceph.TRACE);
    if (fh < 0) {
			ceph.debug("getFileBlockLocations:got error " + fh +
						", exiting and returning null!", ceph.ERROR);
      return null;
    }
    //get the block size
		ceph.debug("getFileBlockLocations:call ceph_getblocksize from Java", ceph.TRACE);
    long blockSize = ceph.ceph_getblocksize(abs_path.toString());
		ceph.debug("getFileBlockLocations:return from ceph_getblocksize", ceph.TRACE);
    BlockLocation[] locations =
      new BlockLocation[(int)Math.ceil(len/(float)blockSize)];
		long offset;
    for (int i = 0; i < locations.length; ++i) {
			offset = start + i*blockSize;
			ceph.debug("getFileBlockLocations:call ceph_hosts from Java on fh "
						+ fh + " and offset " + offset, ceph.TRACE);
      String host = ceph.ceph_hosts(fh, offset);
			ceph.debug("getFileBlockLocations:return from ceph_hosts to Java with host "
						+ host, ceph.TRACE);
      String[] hostArray = new String[1];
      hostArray[0] = host;
      locations[i] = new BlockLocation(hostArray, hostArray,
																			 start+i*blockSize-(start % blockSize),
																			 blockSize);
    }
		ceph.debug("getFileBlockLocations:call ceph_close from Java on fh "
					+ fh, ceph.TRACE);
    ceph.ceph_close(fh);
		ceph.debug("getFileBlockLocations:return with " + locations.length
					+ " locations", ceph.DEBUG);
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
    ceph.debug("getStatus:enter with path " + path, ceph.DEBUG);
    Path abs_path = makeAbsolute(path);
    
    //currently(Ceph .16) Ceph actually ignores the path
    //but we still pass it in; if Ceph stops ignoring we may need more
    //error-checking code.
    CephStat ceph_stat = new CephStat();
		ceph.debug("getStatus:calling ceph_statfs from Java", ceph.TRACE);
    int result = ceph.ceph_statfs(abs_path.toString(), ceph_stat);
    if (result!=0) throw new IOException("Somehow failed to statfs the Ceph filesystem. Error code: " + result);
    ceph.debug("getStatus:exit successfully", ceph.DEBUG);
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
    ceph.debug("delete:enter with path " + path + " and recursive=" + recursive,
					ceph.DEBUG);
    Path abs_path = makeAbsolute(path);
    
    // sanity check
    if (abs_path.equals(root))
      throw new IOException("Error: deleting the root directory is a Bad Idea.");
    if (!exists(abs_path)) return false;

    // if the path is a file, try to delete it.
    if (isFile(abs_path)) {
			ceph.debug("delete:calling ceph_unlink from Java with path " + abs_path,
						ceph.TRACE);
      boolean result = ceph.ceph_unlink(abs_path.toString());
      if(!result)
				ceph.debug("delete: failed to delete file \"" +
												abs_path.toString() + "\".", ceph.ERROR);
      ceph.debug("delete:exit with success=" + result, ceph.DEBUG);
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
      ceph.debug("delete: Failed to read contents of directory \"" +
						abs_path.toString() +
						"\" while trying to delete it, BAILING", ceph.ERROR);
      return false;
    }
    // delete the entries
		ceph.debug("delete: recursively calling delete on contents of "
					+ abs_path, ceph.DEBUG);
    for (Path p : contents) {
      if (!delete(p, true)) {
				ceph.debug("delete: Failed to delete file \"" + 
												p.toString() + "\" while recursively deleting \""
												+ abs_path.toString() + "\", BAILING", ceph.ERROR );
				return false;
      }
    }
    //if we've come this far it's a now-empty directory, so delete it!
    boolean result = ceph.ceph_rmdir(abs_path.toString());
    if (!result)
      ceph.debug("delete: failed to delete \"" + abs_path.toString()
						+ "\", BAILING", ceph.ERROR);
    ceph.debug("delete:exit", ceph.DEBUG);
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
    ceph.debug("makeAbsolute:enter with path " + path, ceph.NOLOG);
    if (path == null) return new Path("/");
    // first, check for the prefix
    if (path.toString().startsWith(fs_default_name)) {	  
      Path stripped_path = new Path(path.toString().substring(fs_default_name.length()));
      ceph.debug("makeAbsolute:exit with path " + stripped_path, ceph.NOLOG);
      return stripped_path;
    }

    if (path.isAbsolute()) {
      ceph.debug("makeAbsolute:exit with path " + path, ceph.NOLOG);
      return path;
    }
    Path new_path = new Path(ceph.ceph_getcwd(), path);
    ceph.debug("makeAbsolute:exit with path " + new_path, ceph.NOLOG);
    return new_path;
  }
 
  private Path[] listPaths(Path path) throws IOException {
    ceph.debug("listPaths:enter with path " + path, ceph.NOLOG);
    String dirlist[];

    Path abs_path = makeAbsolute(path);

    // If it's a directory, get the listing. Otherwise, complain and give up.
    ceph.debug("calling ceph_getdir from Java with path " + abs_path, ceph.NOLOG);
    dirlist = ceph.ceph_getdir(abs_path.toString());
    ceph.debug("returning from ceph_getdir to Java", ceph.NOLOG);

    if (dirlist == null) {
      throw new IOException("listPaths: path " + path.toString() + " is not a directory.");
    }
    
    // convert the strings to Paths
    Path[] paths = new Path[dirlist.length];
    for (int i = 0; i < dirlist.length; ++i) {
      ceph.debug("Raw enumeration of paths in \"" + abs_path.toString() + "\": \"" +
											dirlist[i] + "\"", ceph.NOLOG);
      // convert each listing to an absolute path
      Path raw_path = new Path(dirlist[i]);
      if (raw_path.isAbsolute())
				paths[i] = raw_path;
      else
				paths[i] = new Path(abs_path, raw_path);
    }
    ceph.debug("listPaths:exit", ceph.NOLOG);
    return paths;
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
