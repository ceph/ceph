// -*- mode:Java; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.OutputStream;
import java.net.URI;
import java.util.Set;
import java.util.EnumSet;
import java.util.Vector;
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
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FsStatus;
//import org.apache.hadoop.fs.CreateFlag;

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

  private URI uri;

  private Path root;
  private boolean initialized = false;

  private static boolean debug = false;
  private static String cephDebugLevel;
  private static String monAddr;
  private static String fs_default_name;
  
  private native boolean ceph_initializeClient(String arguments, int block_size);
  private native String  ceph_getcwd();
  private native boolean ceph_setcwd(String path);
  private native boolean ceph_rmdir(String path);
  private native boolean ceph_mkdir(String path);
  private native boolean ceph_unlink(String path);
  private native boolean ceph_rename(String old_path, String new_path);
  private native boolean ceph_exists(String path);
  private native long    ceph_getblocksize(String path);
  private native boolean ceph_isdirectory(String path);
  private native boolean ceph_isfile(String path);
  private native String[] ceph_getdir(String path);
  private native int ceph_mkdirs(String path, int mode);
  private native int ceph_open_for_append(String path);
  private native int ceph_open_for_read(String path);
  private native int ceph_open_for_overwrite(String path, int mode);
  private native int ceph_close(int filehandle);
  private native boolean ceph_setPermission(String path, int mode);
  private native boolean ceph_kill_client();
  private native boolean ceph_stat(String path, Stat fill);
  private native int ceph_statfs(String Path, CephStat fill);
  private native int ceph_replication(String path);
  private native String ceph_hosts(int fh, long offset);
  private native int ceph_setTimes(String path, long mtime, long atime);

  /**
   * Create a new CephFileSystem.
   */
  public CephFileSystem() {
    debug("CephFileSystem:enter");
    root = new Path("/");
    debug("CephFileSystem:exit");
  }

  /**
   * Lets you get the URI of this CephFileSystem.
   * @return the URI.
   */
  public URI getUri() {
    if (!initialized) return null;
    debug("getUri:enter");
    debug("getUri:exit with return " + uri);
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
    debug("initialize:enter");
    if (!initialized) {
      System.load(conf.get("fs.ceph.libDir")+"/libhadoopcephfs.so");
      System.load(conf.get("fs.ceph.libDir")+"/libceph.so");
      super.initialize(uri, conf);
      setConf(conf);
      this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());    
      
      fs_default_name = conf.get("fs.default.name");
      debug = ("true".equals(conf.get("fs.ceph.debug", "false")));
      //build up the arguments for Ceph
      String arguments = new String("CephFSInterface");
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
      //make sure they gave us a ceph monitor address or conf file
      if ( (conf.get("fs.ceph.monAddr") == null) &&
	   (arguments.indexOf("-m") == -1) &&
	   (arguments.indexOf("-c") == -1) ) {
	debug("You need to specify a Ceph monitor address.");
	throw new IOException("You must specify a Ceph monitor address or config file!");
      }
      //  Initialize the client
      if (!ceph_initializeClient(arguments,
				 conf.getInt("fs.ceph.blockSize", 1<<26))) {
	debug("Ceph initialization failed!");
	throw new IOException("Ceph initialization failed!");
      }
      initialized = true;
      debug("Initialized client. Setting cwd to /");
      ceph_setcwd("/");
    }
    debug("initialize:exit");
  }

  /**
   * Close down the CephFileSystem. Runs the base-class close method
   * and then kills the Ceph client itself.
   * @throws IOException if initialize() hasn't been called.
   */
  @Override
  public void close() throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the"
			 +"CephFileSystem before calling other methods.");
    debug("close:enter");
    super.close();//this method does stuff, make sure it's run!
    ceph_kill_client();
    debug("close:exit");
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
    if (!initialized) throw new IOException ("You have to initialize the"
			 +"CephFileSystem before calling other methods.");
    debug("append:enter with path " + file + " bufferSize " + bufferSize);
    Path abs_path = makeAbsolute(file);
    if (progress!=null) progress.progress();
    int fd = ceph_open_for_append(abs_path.toString());
    if (progress!=null) progress.progress();
    if( fd < 0 ) { //error in open
      throw new IOException("append: Open for append failed on path \"" +
			    abs_path.toString() + "\"");
    }
    CephOutputStream cephOStream = new CephOutputStream(getConf(), fd);
    debug("append:exit");
    return new FSDataOutputStream(cephOStream);
  }

  /**
   * Get the name of this CephFileSystem.
   * @return The name of the CephFileSystem as a string.
   * @deprecated Use getUri() instead.
   */
  @Deprecated
  public String getName() {
    if (!initialized) return null;
    debug("getName:enter");
    debug("getName:exit with value " + getUri().toString());
    return getUri().toString();
  }

  /**
   * Get the current working directory for the given file system
   * @return the directory Path
   */
  public Path getWorkingDirectory() {
    if (!initialized) return null;
    debug("getWorkingDirectory:enter");
    debug("Working directory is " + ceph_getcwd());
    debug("getWorkingDirectory:exit");
    return new Path(fs_default_name + ceph_getcwd());
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
    debug("setWorkingDirecty:enter with new working dir " + dir);
    Path abs_path = makeAbsolute(dir);
    debug("calling ceph_setcwd from Java");
    if (!ceph_setcwd(abs_path.toString()))
      debug("Warning:ceph_setcwd failed for some reason on path " + abs_path);
    debug("returned from ceph_setcwd to Java" );
    debug("setWorkingDirectory:exit");
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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("exists:enter with path " + path);
    boolean result;
    Path abs_path = makeAbsolute(path);
    if (abs_path.toString().equals("/")) {
      result = true;
    }
    else {
      debug("Calling ceph_exists from Java on path "
	    + abs_path.toString() + ":");
      result =  ceph_exists(abs_path.toString());
      debug("Returned from ceph_exists to Java");
    }
    debug("exists:exit with value " + result);
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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("mkdirs:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    debug("calling ceph_mkdirs from Java");
    int result = ceph_mkdirs(abs_path.toString(), (int)perms.toShort());
    debug("Returned from ceph_mkdirs to Java with result " + result);
    debug("mkdirs:exit with result " + result);
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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("isFile:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    boolean result;
    if (abs_path.toString().equals("/")) {
      result =  false;
    }
    else {
      result = ceph_isfile(abs_path.toString());
    }
    debug("isFile:exit with result " + result);
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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("isDirectory:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    boolean result;
    if (abs_path.toString().equals(root)) {
      result = true;
    }
    else {
      debug("calling ceph_isdirectory from Java");
      result = ceph_isdirectory(abs_path.toString());
      debug("Returned from ceph_isdirectory to Java");
    }
    debug("isDirectory:exit with result " + result);
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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("getFileStatus:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    //sadly, Ceph doesn't really do uids/gids just yet, but
    //everything else is filled
    FileStatus status;
    Stat lstat = new Stat();
    if(ceph_stat(abs_path.toString(), lstat)) {
      status = new FileStatus(lstat.size, lstat.is_dir,
			      ceph_replication(abs_path.toString()),
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

    debug("getFileStatus:exit");
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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("listStatus:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    Path[] paths = listPaths(abs_path);
    if (paths != null) {
      FileStatus[] statuses = new FileStatus[paths.length];
      for (int i = 0; i < paths.length; ++i) {
	statuses[i] = getFileStatus(paths[i]);
      }
      debug("listStatus:exit");
      return statuses;
    }
    if (!isFile(path)) throw new FileNotFoundException(); //if we get here, listPaths returned null
    //which means that the input wasn't a directory, so throw an Exception if it's not a file
    return null; //or return null if it's a file
  }

  @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    Path abs_path = makeAbsolute(p);
    ceph_setPermission(abs_path.toString(), permission.toShort());
  }

  /**
   * Set access/modification times of a file.
   * @param p The path
   * @param mtime Set modification time in number of millis since Jan 1, 1970.
   * @param atime Set access time in number of millis since Jan 1, 1970.
   */
@Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
  if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
  Path abs_path = makeAbsolute(p);
  int r = ceph_setTimes(abs_path.toString(), mtime, atime);
  if (r<0) throw new IOException ("Failed to set times on path "
				  + abs_path.toString() + " Error code: " + r);
}

  /**
   * Create a new file and open an FSDataOutputStream that's connected to it.
   * @param path The file to create.
   * @param permission The permissions to apply to the file.
   * @param overwrite If true, overwrite any existing file with this name.
   * @param bufferSize Ceph does internal buffering; this is ignored.
   * @param replication Ignored by Ceph. This can be configured via Ceph configuration.
   * @param blockSize Ignored by Ceph.
   * @param progress A Progressable to report back to. Reporting is limited but exists.
   * @return An FSDataOutputStream pointing to the created file.
   * @throws IOException if initialize() hasn't been called, or the path is an
   * existing directory, or the path exists but overwrite is false, or there is a
   * failure in attempting to open for append with Ceph.
   */
  public FSDataOutputStream create(Path path,
				   FsPermission permission,
				   //EnumSet<CreateFlag> flag,
				   boolean overwrite,
				   int bufferSize,
				   short replication,
				   long blockSize,
				   Progressable progress
				   ) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("create:enter with path " + path);
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
      if (!overwrite)
      //if (!flag.contains(CreateFlag.OVERWRITE)) {
	throw new IOException("createRaw: Cannot open existing file \"" 
			      + abs_path.toString() 
			      + "\" for writing without overwrite flag");
    }

    if (progress!=null) progress.progress();

    // Step 2: create any nonexistent directories in the path
    if (!exists) {
      Path parent = abs_path.getParent();
      if (parent != null) { // if parent is root, we're done
	int r = ceph_mkdirs(parent.toString(), permission.toShort());
	if (!(r==0 || r==-EEXIST))
	  throw new IOException ("Error creating parent directory; code: " + r);
      }
      if (progress!=null) progress.progress();
    }
    // Step 3: open the file
    debug("calling ceph_open_for_overwrite from Java");
    int fh = ceph_open_for_overwrite(abs_path.toString(), (int)permission.toShort());
    if (progress!=null) progress.progress();
    debug("Returned from ceph_open_for_overwrite to Java with fh " + fh);
    if (fh < 0) {
      throw new IOException("create: Open for overwrite failed on path \"" + 
			    path.toString() + "\"");
    }
      
    // Step 4: create the stream
    OutputStream cephOStream = new CephOutputStream(getConf(), fh);
    debug("create:exit");
    return new FSDataOutputStream(cephOStream);
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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("open:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    
    int fh = ceph_open_for_read(abs_path.toString());
    if (fh < 0) { //uh-oh, something's bad!
      if (fh == -ENOENT) //well that was a stupid open
	throw new IOException("open:  absolute path \""  + abs_path.toString()
			      + "\" does not exist");
      else //hrm...the file exists but we can't open it :(
	throw new IOException("open: Failed to open file " + abs_path.toString());
    }

    if(isDirectory(abs_path)) { //yes, it is possible to open Ceph directories
      //but that doesn't mean you should in Hadoop!
      ceph_close(fh);
      throw new IOException("open:  absolute path \""  + abs_path.toString()
			    + "\" is a directory!");
    }
    Stat lstat = new Stat();
    ceph_stat(abs_path.toString(), lstat);
    long size = lstat.size;
    if (size < 0) {
      throw new IOException("Failed to get file size for file " + abs_path.toString() + 
			    " but succeeded in opening file. Something bizarre is going on.");
    }
    FSInputStream cephIStream = new CephInputStream(getConf(), fh, size);
    debug("open:exit");
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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("rename:enter");
    debug("calling ceph_rename from Java");
    Path abs_src = makeAbsolute(src);
    Path abs_dst = makeAbsolute(dst);
    boolean result = ceph_rename(abs_src.toString(), abs_dst.toString());
    debug("return from ceph_rename to Java with result " + result);
    debug("rename:exit");
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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    //sanitize and get the filehandle
    Path abs_path = makeAbsolute(file.getPath());
    int fh = ceph_open_for_read(abs_path.toString());
    if (fh < 0) {
      return null;
    }
    //get the block size
    long blockSize = ceph_getblocksize(abs_path.toString());
    BlockLocation[] locations =
      new BlockLocation[(int)Math.ceil((len-start)/(float)blockSize)];
    for (int i = 0; i < locations.length; ++i) {
      String host = ceph_hosts(fh, start + i*blockSize);
      String[] hostArray = new String[1];
      hostArray[0] = host;
      locations[i] = new BlockLocation(hostArray, hostArray,
				       start+i*blockSize, blockSize);
    }
    ceph_close(fh);
    return locations;
  }
  
  /**
   * Get usage statistics on the Ceph filesystem.
   * @param path A path to the partition you're interested in.
   * Ceph doesn't partition, so this is ignored.
   * @return FsStatus reportin capacity, usage, and remaining spac.
   * @throws IOException if initialize() hasn't been called, or the
   * stat somehow fails.
   *
  public FsStatus getStatus (Path path) throws IOException {
    if (!initialized) throw new IOException("You have to initialize the"
		      + " CephFileSystem before calling other methods.");
    debug("getStatus:enter");
    Path abs_path = makeAbsolute(path);
    
    //currently(Ceph .12) Ceph actually ignores the path
    //but we still pass it in; if Ceph stops ignoring we may need more
    //error-checking code.
    CephStat ceph_stat = new CephStat();
    int result = ceph_statfs(abs_path.toString(), ceph_stat);
    if (result!=0) throw new IOException("Somehow failed to statfs the Ceph filesystem. Error code: " + result);
    debug("getStatus:exit");
    return new FsStatus(ceph_stat.capacity,
			ceph_stat.used, ceph_stat.remaining);
  } */

  /**
   * Delete the given path, and any children if it's a directory.
   * @param path The path to delete.
   * @return true if the delete succeeded, false otherwise.
   * @throws IOException If initialize() hasn't been called,
   * or you try to delete the root directory.
   * @deprecated Use delete(Path path, boolean recursive) instead.
   */
  public boolean delete(Path path) throws IOException { return delete(path, true); };

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
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("delete:enter");
    Path abs_path = makeAbsolute(path);
    
    //debug("delete: Deleting path " + abs_path.toString());
    // sanity check
    if (abs_path.toString().equals(root))
      throw new IOException("Error: deleting the root directory is a Bad Idea.");

    if (!exists(abs_path)) return false;

    // if the path is a file, try to delete it.
    if (isFile(abs_path)) {
      boolean result = ceph_unlink(abs_path.toString());
      /*      if(!result) {
	debug("delete: failed to delete file \"" +
			   abs_path.toString() + "\".");
			   } */
      debug("delete:exit");
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
      // debug("delete: Failed to read contents of directory \"" +
      //	     abs_path.toString() + "\" while trying to delete it");
      debug("delete:exit");
      return false;
    }
    // delete the entries
    Path parent = abs_path.getParent();
    for (Path p : contents) {
      if (!delete(p, true)) {
	// debug("delete: Failed to delete file \"" + 
	//		 p.toString() + "\" while recursively deleting \""
	//		 + abs_path.toString() + "\"" );
	debug("delete:exit");
	return false;
      }
    }
    //if we've come this far it's a now-empty directory, so delete it!
    boolean result = ceph_rmdir(abs_path.toString());
    if (!result)
      debug("delete: failed to delete \"" + abs_path.toString() + "\"");
    debug("delete:exit");
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
   * Get the block size for a given file. This will return if path is a
   * directory, but the value is meaningless.
   * @param path The Path you want to get the block size for.
   * @return the block size of path (in bytes) as a long.
   * @throws IOException if initialize() hasn't been called or
   * the path doesn't exist.
   * @deprecated use getFileStatus instead.
   */
  @Deprecated
  public long getBlockSize(Path path) throws IOException {
    if (!initialized) throw new IOException ("You have to initialize the"
		       +"CephFileSystem before calling other methods.");
    debug("getBlockSize:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    long result = ceph_getblocksize(abs_path.toString());
  
    if (result < 0)
      throw new IOException("org.apache.hadoop.fs.ceph.CephFileSystem.getBlockSize: File or directory "
			    + path.toString() + " does not exist.");

    debug("getBlockSize:exit with result " + result);
    return result;
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
    debug("makeAbsolute:enter with path " + path);
    if (path == null) return new Path("/");
    // first, check for the prefix
    if (path.toString().startsWith(fs_default_name)) {	  
      Path stripped_path = new Path(path.toString().substring(fs_default_name.length()));
      debug("makeAbsolute:exit with path " + stripped_path);
      return stripped_path;
    }

    if (path.isAbsolute()) {
      debug("makeAbsolute:exit with path " + path);
      return path;
    }
    Path new_path = new Path(ceph_getcwd(), path);
    debug("makeAbsolute:exit with path " + new_path);
    return new_path;
  }
 
  private Path[] listPaths(Path path) throws IOException {
    debug("listPaths:enter with path " + path);
    String dirlist[];

    Path abs_path = makeAbsolute(path);

    // If it's a directory, get the listing. Otherwise, complain and give up.
    debug("calling ceph_getdir from Java with path " + abs_path);
    dirlist = ceph_getdir(abs_path.toString());
    debug("returning from ceph_getdir to Java");

    if (dirlist == null) {
      throw new IOException("listPaths: path " + path.toString() + " is not a directory.");
    }
    
    // convert the strings to Paths
    Path[] paths = new Path[dirlist.length];
    for (int i = 0; i < dirlist.length; ++i) {
      debug("Raw enumeration of paths in \"" + abs_path.toString() + "\": \"" +
			 dirlist[i] + "\"");
      // convert each listing to an absolute path
      Path raw_path = new Path(dirlist[i]);
      if (raw_path.isAbsolute())
	paths[i] = raw_path;
      else
	paths[i] = new Path(abs_path, raw_path);
    }
    debug("listPaths:exit");
    return paths;
  }

  private void debug(String statement) {
    if (debug) System.err.println(statement);
  }

  private class Stat {
    public long size;
    public boolean is_dir;
    public long block_size;
    public long mod_time;
    public long access_time;
    public int mode;

    public Stat(){}
  }

  private class CephStat {
    public long capacity;
    public long used;
    public long remaining;

    public CephStat() {}
  }
}
