// -*- mode:Java; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.OutputStream;
import java.net.URI;
import java.util.Set;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
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
//import org.apache.hadoop.fs.CreateFlag;

/**
 * <p>
 * A {@link FileSystem} backed by <a href="http://ceph.sourceforge.net">Ceph.</a>.
 * This will not start a Ceph instance; one must already be running.
 * </p>
  */
public class CephFileSystem extends FileSystem {

  private static final long DEFAULT_BLOCK_SIZE = 8 * 1024 * 1024;
  
  static {
    System.loadLibrary("hadoopcephfs");
  }
  
  private URI uri;

  private FileSystem localFs;
  private Path root;
  private Path parent;

  private static boolean debug;
  private static String cephDebugLevel;
  private static String monAddr;
  private static String fs_default_name;
  
  private native boolean ceph_initializeClient(String debugLevel, String mon);
  private native boolean ceph_copyFromLocalFile(String localPath, String cephPath);
  private native boolean ceph_copyToLocalFile(String cephPath, String localPath);
  private native String  ceph_getcwd();
  private native boolean ceph_setcwd(String path);
  private native boolean ceph_rmdir(String path);
  private native boolean ceph_mkdir(String path);
  private native boolean ceph_unlink(String path);
  private native boolean ceph_rename(String old_path, String new_path);
  private native boolean ceph_exists(String path);
  private native long    ceph_getblocksize(String path);
  private native long    ceph_getfilesize(String path);
  private native boolean ceph_isdirectory(String path);
  private native boolean ceph_isfile(String path);
  private native String[] ceph_getdir(String path);
  private native int ceph_mkdirs(String path, int mode);
  private native int ceph_open_for_append(String path);
  private native int ceph_open_for_read(String path);
  private native int ceph_open_for_overwrite(String path, int mode);
  private native boolean ceph_setPermission(String path, int mode);
  private native boolean ceph_kill_client();
  private native boolean ceph_stat(String path, Stat fill);
  public CephFileSystem() {
    debug("CephFileSystem:enter");
    root = new Path("/");
    parent = new Path("..");
    debug("CephFileSystem:exit");
  }

  /*
    public S3FileSystem(FileSystemStore store) {
    this.store = store;
    } */

  public URI getUri() {
    debug("getUri:enter");
    debug("getUri:exit with return " + uri);
    return uri;
  }

  @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
    debug("initialize:enter");
    super.initialize(uri, conf);
    //store.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());    

    // TODO: local filesystem? we really need to figure out this conf thingy
    this.localFs = get(URI.create("file:///"), conf);

    fs_default_name = conf.get("fs.default.name");
    monAddr = conf.get("fs.ceph.monAddr");
    cephDebugLevel = conf.get("fs.ceph.debugLevel");
    debug = ("true".equals(conf.get("fs.ceph.debug")));
    //  Initializes the client
    if (!ceph_initializeClient(cephDebugLevel, monAddr)) {
      throw new IOException("Ceph initialization failed!");
    }
    debug("Initialized client. Setting cwd to /");
    ceph_setcwd("/");
    debug("initialize:exit");
  }

  @Override
    public void close() throws IOException {
    debug("close:enter");
    super.close();//this method does stuff, make sure it's run!
    System.gc(); //to run the finalizers on CephInput/OutputStreams
    //this is kinda a hack and we may need to adjust it
    ceph_kill_client();
    debug("close:exit");
  }

  public FSDataOutputStream append (Path file, int bufferSize,
				    Progressable progress) throws IOException {
    debug("append:enter with path " + file + " bufferSize " + bufferSize);
    Path abs_path = makeAbsolute(file);
    int fd = ceph_open_for_append(abs_path.toString());
    if( fd < 0 ) { //error in open
      throw new IOException("append: Open for append failed on path \"" +
			    abs_path.toString() + "\"");
    }
    CephOutputStream cephOStream = new CephOutputStream(getConf(), fd);
    debug("append:exit");
    return new FSDataOutputStream(cephOStream);
  }

  @Deprecated
    public String getName() {
    debug("getName:enter");
    debug("getName:exit with value " + getUri().toString());
    return getUri().toString();
  }

  public Path getWorkingDirectory() {
    debug("getWorkingDirectory:enter");
    debug("Working directory is " + ceph_getcwd());
    debug("getWorkingDirectory:exit");
    return new Path(fs_default_name + makeAbsolute(new Path(ceph_getcwd())));
  }

  @Override
    public void setWorkingDirectory(Path dir) {
    debug("setWorkingDirecty:enter with new working dir " + dir);
    Path abs_path = makeAbsolute(dir);

    // error conditions if path's not a directory
    boolean isDir = false;
    boolean path_exists = false;
    try {
      isDir = __isDirectory(abs_path);
      path_exists = exists(abs_path);
    }

    catch (IOException e) {
      debug("Warning: isDirectory threw an exception");
    }

    if (!isDir) {
      if (path_exists)
	debug("Warning: SetWorkingDirectory(" + dir.toString() + 
			   "): path is not a directory");
      else
	debug("Warning: SetWorkingDirectory(" + dir.toString() + 
			   "): path does not exist");
    }
    else {
      debug("calling ceph_setcwd from Java");
      ceph_setcwd(abs_path.toString());
      debug("returned from ceph_setcwd to Java" );
    }
    debug("setWorkingDirectory:exit");
  }



  @Override
    /**
     * There's not much point to using a full getFileStatus for this since
     * that makes more calls than we need, so override.
     */
    public boolean exists(Path path) throws IOException {
    debug("exists:enter with path " + path);
    boolean result;
    Path abs_path = makeAbsolute(path);
    if (abs_path.toString().equals("/")) {
      result = true;
    }
    else {
      debug("Calling ceph_exists from Java on path " + abs_path.toString() + ":");
      result =  ceph_exists(abs_path.toString());
      debug("Returned from ceph_exists to Java");
    }
    debug("exists:exit with value " + result);
    return result;
  }

  /* Creates the directory and all nonexistent parents.   */
  public boolean mkdirs(Path path, FsPermission perms) throws IOException {
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
   * As with exists, this is faster than their method
   */
  @Override
    public boolean isFile(Path path) throws IOException {
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

  public FileStatus getFileStatus(Path p) throws IOException {
    debug("getFileStatus:enter with path " + p);
    Path abs_p = makeAbsolute(p);
    //sadly, Ceph doesn't really do uids/gids just yet, but
    //everything else is filled
    // For the moment, hardwired replication
    int replication = 2;
    FileStatus status;
    Stat lstat = new Stat();
    if(ceph_stat(abs_p.toString(), lstat)) {
      debug("getFileStatus: mod_time is " + lstat.mod_time);
      status = new FileStatus(lstat.size, lstat.is_dir, replication,
			      lstat.block_size,
			      //these times in seconds get converted to millis
			      lstat.mod_time*1000,
			      lstat.access_time*1000,
			      new FsPermission((short)lstat.mode),
			      null,
			      null,
			      new Path(fs_default_name+abs_p.toString()));
    }
    else { //fail out
	throw new FileNotFoundException("org.apache.hadoop.fs.ceph.CephFileSystem: File "
					+ p + " does not exist or could not be accessed");
    }
    debug("getFileStatus:exit");
    return status;
  }

  // array of statuses for the directory's contents
  public FileStatus[] listStatus(Path p) throws IOException {
    debug("listStatus:enter with path " + p);
    Path abs_p = makeAbsolute(p);
    Path[] paths = listPaths(abs_p);
    FileStatus[] statuses = new FileStatus[paths.length];
    for (int i = 0; i < paths.length; ++i) {
      statuses[i] = getFileStatus(paths[i]);
    }
    debug("listStatus:exit");
    return statuses;
  }

  @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
    Path abs_path = makeAbsolute(p);
    ceph_setPermission(abs_path.toString(), permission.toShort());
  }

  /**
   * In order to run this with Hadoop .20, I had to revert back to using
   * a boolean instead of the CreateFlag.
   */
  public FSDataOutputStream create(Path f,
				   FsPermission permission,
				   //EnumSet<CreateFlag> flag,
				   boolean overwrite,
				   int bufferSize,
				   short replication,
				   long blockSize,
				   Progressable progress
				   ) throws IOException {
	
    debug("create:enter with path " + f);
    Path abs_path = makeAbsolute(f);
      
    // We ignore progress reporting and replication.
    // Required semantics: if the file exists, overwrite if CreateFlag.OVERWRITE;
    // throw an exception if !CreateFlag.OVERWRITE.

    // Step 1: existence test
    if(__isDirectory(abs_path))
      throw new IOException("create: Cannot overwrite existing directory \""
			    + abs_path.toString() + "\" with a file");      
    //if (!flag.contains(CreateFlag.OVERWRITE)) {
    if (!overwrite) {
      if (exists(abs_path)) {
	throw new IOException("createRaw: Cannot open existing file \"" 
			      + abs_path.toString() 
			      + "\" for writing without overwrite flag");
      }
    }

    // Step 2: create any nonexistent directories in the path
    Path parent =  abs_path.getParent();
    if (parent != null) { // if parent is root, we're done
      if(!exists(parent)) {
	mkdirs(parent);
      }
    }

    // Step 3: open the file
    debug("calling ceph_open_for_overwrite from Java");
    int fh = ceph_open_for_overwrite(abs_path.toString(), (int)permission.toShort());
    debug("Returned from ceph_open_for_overwrite to Java with fh " + fh);
    if (fh < 0) {
      throw new IOException("create: Open for overwrite failed on path \"" + 
			    abs_path.toString() + "\"");
    }
      
    // Step 4: create the stream
    OutputStream cephOStream = new CephOutputStream(getConf(), fh);
    //debug("createRaw: opened absolute path \""  + absfilepath.toString() 
    //		 + "\" for writing with fh " + fh);

    debug("create:exit");
    return new FSDataOutputStream(cephOStream);
  }

  // Opens a Ceph file and attaches the file handle to an FSDataInputStream.
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    debug("open:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    
    if(!isFile(abs_path)) {
      if (!exists(abs_path))
	throw new IOException("open:  absolute path \""  + abs_path.toString()
			      + "\" does not exist");
      else
	throw new IOException("open:  absolute path \""  + abs_path.toString()
			      + "\" is not a file");
    }
    
    int fh = ceph_open_for_read(abs_path.toString());
    if (fh < 0) {
      throw new IOException("open: Failed to open file " + abs_path.toString());
    }
    long size = ceph_getfilesize(abs_path.toString());
    if (size < 0) {
      throw new IOException("Failed to get file size for file " + abs_path.toString() + 
			    " but succeeded in opening file. Something bizarre is going on.");
    }
    FSInputStream cephIStream = new CephInputStream(getConf(), fh, size);
    debug("open:exit");
    return new FSDataInputStream(cephIStream);
  }

  @Override
    public boolean rename(Path src, Path dst) throws IOException {
    // TODO: Check corner cases: dst already exists,
    // or path is directory with children
    debug("rename:enter");
    debug("calling ceph_rename from Java");
    Path abs_src = makeAbsolute(src);
    Path abs_dst = makeAbsolute(dst);
    boolean result = ceph_rename(abs_src.toString(), abs_dst.toString());
    debug("return from ceph_rename to Java with result " + result);
    debug("rename:exit");
    return result;
  }
  
  /* Added in for .20, not required in trunk */
  public boolean delete(Path path) throws IOException { return delete(path, true); };

  public boolean delete(Path path, boolean recursive) throws IOException {
    debug("delete:enter");
    Path abs_path = makeAbsolute(path);
    
    //debug("delete: Deleting path " + abs_path.toString());
    // sanity check
    if (abs_path.toString().equals("/"))
      throw new IOException("Error: deleting the root directory is a Bad Idea.");
    
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
    
    /* If the path is a directory, recursively try to delete its contents,
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
   * User-defined replication is not supported for Ceph file systems at the moment.
   */
  @Deprecated
    public short getReplication(Path path) throws IOException {
    return 1;
  }

  @Override
    public short getDefaultReplication() {
    return 1;
  }
  
  /**
   * User-defined replication is not supported for Ceph file systems at the moment.
   */
  public boolean setReplicationRaw(Path path, short replication) throws IOException {
    return true;
  }

  /**
   * You need to guarantee the path exists before calling this method, for now.
   */
  @Deprecated
    public long getBlockSize(Path path) throws IOException {
    debug("getBlockSize:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    if (!exists(abs_path)) {
      throw new IOException("org.apache.hadoop.fs.ceph.CephFileSystem.getBlockSize: File or directory " + path.toString() + " does not exist.");
    }
    long result = ceph_getblocksize(abs_path.toString());
    
    if (result < 4096) {
      debug("org.apache.hadoop.fs.ceph.CephFileSystem.getBlockSize: " + 
			 "path exists; strange block size of " + result + " defaulting to 8192");
      return 8192;
    }
    debug("getBlockSize:exit with result " + result);
    return result;
  }

  @Override
    public long getDefaultBlockSize() {
    return DEFAULT_BLOCK_SIZE;
    //return getConf().getLong("fs.ceph.block.size", DEFAULT_BLOCK_SIZE);
  }

  // Makes a Path absolute. In a cheap, dirty hack, we're
  // also going to strip off any fs_default_name prefix we see. 
  private Path makeAbsolute(Path path) {
    debug("makeAbsolute:enter with path " + path);
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
    Path wd = getWorkingDirectory();
    if (wd.toString().equals("")){
      Path new_path = new Path(root, path);
      debug("makeAbsolute:exit with path " + new_path);
      return new_path;
    }
    else {
      Path new_path = new Path(root, path);
      debug("makeAbsolute:exit with path " + new_path);
      return new_path;
    }
  }

  private boolean __isDirectory(Path path) throws IOException {
    debug("__isDirectory:enter with path " + path);
    Path abs_path = makeAbsolute(path);
    boolean result;
    if (abs_path.toString().equals("/")) {
      result = true;
    }
    else {
      debug("calling ceph_isdirectory from Java");
      result = ceph_isdirectory(abs_path.toString());
      debug("Returned from ceph_isdirectory to Java");
    }
    debug("__isDirectory:exit with result " + result);
    return result;
  }

  private long __getLength(Path path) throws IOException {
    debug("__getLength:enter with path " + path);
    Path abs_path = makeAbsolute(path);

    if (!exists(abs_path)) {
      throw new FileNotFoundException("org.apache.hadoop.fs.ceph.CephFileSystem.__getLength: File or directory " + abs_path.toString() + " does not exist.");
    }	  
    
    long filesize = ceph_getfilesize(abs_path.toString());
    if (filesize < 0) {
      throw new IOException("org.apache.hadoop.fs.ceph.CephFileSystem.getLength: Size of file or directory " + abs_path.toString() + " could not be retrieved.");
    }
    debug("__getLength:exit with size " + filesize);
    return filesize;
  }

  private Path[] listPaths(Path path) throws IOException {
    debug("listPaths:enter with path " + path);
    String dirlist[];

    Path abs_path = makeAbsolute(path);

    // If it's a directory, get the listing. Otherwise, complain and give up.
    if (__isDirectory(abs_path)) {
      debug("calling ceph_getdir from Java with path " + abs_path);
      dirlist = ceph_getdir(abs_path.toString());
      debug("returning from ceph_getdir to Java");
    }
    else {
      debug("listPaths:exit failed on isDirectory");
      return null;
    }
    
    // convert the strings to Paths
    Path paths[] = new Path[dirlist.length];
    for(int i = 0; i < dirlist.length; ++i) {
      //we don't want . or .. entries, which Ceph includes
      if (dirlist[i].equals(".") || dirlist[i].equals("..")) continue;
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
    public int user_id;
    public int group_id; 

    public Stat(){}
  }
}
