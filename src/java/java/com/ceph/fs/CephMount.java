/*
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package com.ceph.fs;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.lang.String;

import com.ceph.crush.Bucket;

public class CephMount {

  /*
   * Set via JNI callback in native_ceph_create
   *
   * Do not touch!
   */
  private long instance_ptr;

  /*
   * Flags for open().
   *
   * Must be synchronized with JNI if changed.
   */
  public static final int O_RDONLY    = 1;
  public static final int O_RDWR      = 2;
  public static final int O_APPEND    = 4;
  public static final int O_CREAT     = 8;
  public static final int O_TRUNC     = 16;
  public static final int O_EXCL      = 32;
  public static final int O_WRONLY    = 64;
  public static final int O_DIRECTORY = 128;

  /*
   * Whence flags for seek().
   *
   * Must be synchronized with JNI if changed.
   */
  public static final int SEEK_SET = 1;
  public static final int SEEK_CUR = 2;
  public static final int SEEK_END = 3;

  /*
   * Attribute flags for setattr().
   *
   * Must be synchronized with JNI if changed.
   */
  public static final int SETATTR_MODE  = 1;
  public static final int SETATTR_UID   = 2;
  public static final int SETATTR_GID   = 4;
  public static final int SETATTR_MTIME = 8;
  public static final int SETATTR_ATIME = 16;

  /*
   * Flags for setxattr();
   *
   * Must be synchronized with JNI if changed.
   */
  public static final int XATTR_CREATE  = 1;
  public static final int XATTR_REPLACE = 2;
  public static final int XATTR_NONE    = 3;

  /*
   * Flags for flock();
   *
   * Must be synchronized with JNI if changed.
   */
  public static final int LOCK_SH       = 1;
  public static final int LOCK_EX       = 2;
  public static final int LOCK_NB       = 4;
  public static final int LOCK_UN       = 8;

  /*
   * This is run by the class loader and will report early any problems
   * finding or linking in the shared JNI library.
   */
  static {
    loadLibrary();
  }

  static synchronized void loadLibrary() {
    CephNativeLoader.getInstance().loadLibrary();
  }

  /*
   * Package-private: called from CephNativeLoader
   */
  static native void native_initialize();

  /*
   * RW lock used for fine grained synchronization to native
   */
  private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
  private final Lock rlock = rwlock.readLock();
  private final Lock wlock = rwlock.writeLock();

  /*
   * Controls clean-up synchronization between the constructor and finalize().
   * If native_ceph_create fails, then we want a call to finalize() to not
   * attempt to clean-up native context, because there is none.
   */
  private boolean initialized = false;

  /*
   * Try to clean-up. First, unmount() will catch users who forget to do the
   * unmount manually. Second, release() will destroy the entire context. It
   * is safe to call release after a failure in unmount.
   */
  protected void finalize() throws Throwable {
    if (initialized) {
      try {
        unmount();
      } catch (Exception e) {}
      try {
        native_ceph_release(instance_ptr);
      } catch (Exception e) {}
    }
    super.finalize();
  }

  /**
   * Create a new CephMount with specific client id.
   *
   * @param id client id.
   */
  public CephMount(String id) {
    native_ceph_create(this, id);
    initialized = true;
  }

  private static native int native_ceph_create(CephMount mount, String id);

  /**
   * Create a new CephMount with default client id.
   */
  public CephMount() {
    this(null);
  }

  /**
   * Activate the mount with a given root path.
   *
   * @param root The path to use as the root (pass null for "/").
   */
  public void mount(String root) {
    wlock.lock();
    try {
      native_ceph_mount(instance_ptr, root);
    } finally {
      wlock.unlock();
    }
  }

  private static native int native_ceph_mount(long mountp, String root);

  /**
   * Deactivate the mount.
   *
   * The mount can be reactivated using mount(). Configuration parameters
   * previously set are not reset.
   */
  public void unmount() {
    wlock.lock();
    try {
      native_ceph_unmount(instance_ptr);
    } finally {
      wlock.unlock();
    }
  }

  private static native int native_ceph_unmount(long mountp);

  /*
   * Private access to low-level ceph_release.
   */
  private static native int native_ceph_release(long mountp);

  /**
   * Load configuration from a file.
   *
   * @param path The path to the configuration file.
   */
  public void conf_read_file(String path) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_conf_read_file(instance_ptr, path);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_conf_read_file(long mountp, String path);

  /**
   * Set the value of a configuration option.
   *
   * @param option The configuration option to modify.
   * @param value The new value of the option.
   */
  public void conf_set(String option, String value) {
    rlock.lock();
    try {
      native_ceph_conf_set(instance_ptr, option, value);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_conf_set(long mountp, String option, String value);

  /**
   * Get the value of a configuration option.
   *
   * @param option The name of the configuration option.
   * @return The value of the option or null if option not found
   */
  public String conf_get(String option) {
    rlock.lock();
    try {
      return native_ceph_conf_get(instance_ptr, option);
    } finally {
      rlock.unlock();
    }
  }

  private static native String native_ceph_conf_get(long mountp, String option);

  /**
   * Get file system status.
   *
   * @param path Path to file in file system.
   * @param statvfs CephStatVFS structure to hold status.
   */
  public void statfs(String path, CephStatVFS statvfs) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_statfs(instance_ptr, path, statvfs);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_statfs(long mountp, String path, CephStatVFS statvfs);

  /**
   * Get the current working directory.
   *
   * @return The current working directory in Ceph.
   */
  public String getcwd() {
    rlock.lock();
    try {
      return native_ceph_getcwd(instance_ptr);
    } finally {
      rlock.unlock();
    }
  }

  private static native String native_ceph_getcwd(long mountp);

  /**
   * Set the current working directory.
   *
   * @param path The directory set as the cwd.
   */
  public void chdir(String path) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_chdir(instance_ptr, path);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_chdir(long mountp, String cwd);

  /**
   * List the contents of a directory.
   *
   * @param dir The directory.
   * @return List of files and directories excluding "." and "..".
   */
  public String[] listdir(String dir) throws FileNotFoundException {
    rlock.lock();
    try {
      return native_ceph_listdir(instance_ptr, dir);
    } finally {
      rlock.unlock();
    }
  }

  private static native String[] native_ceph_listdir(long mountp, String path);

  /**
   * Create a hard link to an existing file.
   *
   * @param oldpath The target path of the link.
   * @param newpath The name of the link.
   */
  public void link(String oldpath, String newpath) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_link(instance_ptr, oldpath, newpath);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_link(long mountp, String existing, String newname);

  /**
   * Unlink/delete a name from the file system.
   *
   * @param path The name to unlink/delete.
   */
  public void unlink(String path) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_unlink(instance_ptr, path);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_unlink(long mountp, String path);

  /**
   * Rename a file or directory.
   *
   * @param from The current path.
   * @param to The new path.
   */
  public void rename(String from, String to) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_rename(instance_ptr, from, to);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_rename(long mountp, String from, String to);

  /**
   * Create a directory.
   *
   * @param path The directory to create.
   * @param mode The mode of the new directory.
   */
  public void mkdir(String path, int mode) {
    rlock.lock();
    try {
      native_ceph_mkdir(instance_ptr, path, mode);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_mkdir(long mountp, String path, int mode);

  /**
   * Create a directory and all parents.
   *
   * @param path The directory to create.
   * @param mode The mode of the new directory.
   */
  public void mkdirs(String path, int mode) throws IOException {
    rlock.lock();
    try {
      native_ceph_mkdirs(instance_ptr, path, mode);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_mkdirs(long mountp, String path, int mode);

  /**
   * Delete a directory.
   *
   * @param path The directory to delete.
   */
  public void rmdir(String path) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_rmdir(instance_ptr, path);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_rmdir(long mountp, String path);

  /**
   * Read the value of a symbolic link.
   */
  public String readlink(String path) throws FileNotFoundException {
    rlock.lock();
    try {
      return native_ceph_readlink(instance_ptr, path);
    } finally {
      rlock.unlock();
    }
  }

  private static native String native_ceph_readlink(long mountp, String path);

  /**
   * Create a symbolic link.
   *
   * @param oldpath Target of the symbolic link.
   * @param newpath Name of the link.
   */
  public void symlink(String oldpath, String newpath) {
    rlock.lock();
    try {
      native_ceph_symlink(instance_ptr, oldpath, newpath);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_symlink(long mountp, String existing, String newname);

  /**
   * Get file status.
   *
   * @param path Path of file to stat.
   * @param stat CephStat structure to hold file status.
   */
  public void stat(String path, CephStat stat) throws FileNotFoundException, CephNotDirectoryException {
    rlock.lock();
    try {
      native_ceph_stat(instance_ptr, path, stat);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_stat(long mountp, String path, CephStat stat);

  /**
   * Get file status, without following symlinks.
   *
   * @param path Path of file to stat.
   * @param stat CephStat structure to hold file status.
   */
  public void lstat(String path, CephStat stat) throws FileNotFoundException, CephNotDirectoryException {
    rlock.lock();
    try {
      native_ceph_lstat(instance_ptr, path, stat);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_lstat(long mountp, String path, CephStat stat);

  /**
   * Set file attributes.
   *
   * @param path Path to file.
   * @param stat CephStat structure holding attributes.
   * @param mask Mask specifying which attributes to set.
   */
  public void setattr(String path, CephStat stat, int mask) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_setattr(instance_ptr, path, stat, mask);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_setattr(long mountp, String relpath, CephStat stat, int mask);

  /**
   * Change file mode.
   * 
   * @param path Path to file.
   * @param mode New mode bits.
   */
  public void chmod(String path, int mode) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_chmod(instance_ptr, path, mode);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_chmod(long mountp, String path, int mode);

  /**
   * Change file mode of an open file.
   *
   * @param fd The open file descriptor to change the mode bits on.
   * @param mode New mode bits.
   */
  public void fchmod(int fd, int mode) {
    rlock.lock();
    try {
      native_ceph_fchmod(instance_ptr, fd, mode);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_fchmod(long mountp, int fd, int mode);

  /**
   * Truncate a file to a specified length.
   *
   * @param path Path of the file.
   * @param size New file length.
   */
  public void truncate(String path, long size) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_truncate(instance_ptr, path, size);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_truncate(long mountp, String path, long size);

  /**
   * Open a file.
   *
   * @param path Path of file to open or create.
   * @param flags Open flags.
   * @param mode Permission mode.
   * @return File descriptor.
   */
  public int open(String path, int flags, int mode) throws FileNotFoundException {
    rlock.lock();
    try {
      return native_ceph_open(instance_ptr, path, flags, mode);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_open(long mountp, String path, int flags, int mode);

  /**
   * Open a file with a specific file layout.
   *
   * @param path Path of file to open or create.
   * @param flags Open flags.
   * @param mode Permission mode.
   * @param stripe_unit File layout stripe unit size.
   * @param stripe_count File layout stripe count.
   * @param object_size Size of each object.
   * @param data_pool The target data pool.
   * @return File descriptor.
   */
  public int open(String path, int flags, int mode, int stripe_unit, int stripe_count,
      int object_size, String data_pool) throws FileNotFoundException {
    rlock.lock();
    try {
      return native_ceph_open_layout(instance_ptr, path, flags, mode, stripe_unit,
          stripe_count, object_size, data_pool);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_open_layout(long mountp, String path,
      int flags, int mode, int stripe_unit, int stripe_count, int object_size, String data_pool);

  /**
   * Close an open file.
   *
   * @param fd The file descriptor.
   */
  public void close(int fd) {
    rlock.lock();
    try {
      native_ceph_close(instance_ptr, fd);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_close(long mountp, int fd);

  /**
   * Seek to a position in a file.
   *
   * @param fd File descriptor.
   * @param offset New offset.
   * @param whence Whence value.
   * @return The new offset.
   */
  public long lseek(int fd, long offset, int whence) {
    rlock.lock();
    try {
      return native_ceph_lseek(instance_ptr, fd, offset, whence);
    } finally {
      rlock.unlock();
    }
  }

  private static native long native_ceph_lseek(long mountp, int fd, long offset, int whence);

  /**
   * Read from a file.
   *
   * @param fd The file descriptor.
   * @param buf Buffer to for data read.
   * @param size Amount of data to read into the buffer.
   * @param offset Offset to read from (-1 for current position).
   * @return The number of bytes read.
   */
  public long read(int fd, byte[] buf, long size, long offset) {
    rlock.lock();
    try {
      return native_ceph_read(instance_ptr, fd, buf, size, offset);
    } finally {
      rlock.unlock();
    }
  }

  private static native long native_ceph_read(long mountp, int fd, byte[] buf, long size, long offset);

  /**
   * Write to a file at a specific offset.
   *
   * @param fd The file descriptor.
   * @param buf Buffer to write.
   * @param size Amount of data to write.
   * @param offset Offset to write from (-1 for current position).
   * @return The number of bytes written.
   */
  public long write(int fd, byte[] buf, long size, long offset) {
    rlock.lock();
    try {
      return native_ceph_write(instance_ptr, fd, buf, size, offset);
    } finally {
      rlock.unlock();
    }
  }

  private static native long native_ceph_write(long mountp, int fd, byte[] buf, long size, long offset);

  /**
   * Truncate a file.
   *
   * @param fd File descriptor of the file to truncate.
   * @param size New file size.
   */
  public void ftruncate(int fd, long size) {
    rlock.lock();
    try {
      native_ceph_ftruncate(instance_ptr, fd, size);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_ftruncate(long mountp, int fd, long size);

  /**
   * Synchronize a file with the file system.
   *
   * @param fd File descriptor to synchronize.
   * @param dataonly Synchronize only data.
   */
  public void fsync(int fd, boolean dataonly) {
    rlock.lock();
    try {
      native_ceph_fsync(instance_ptr, fd, dataonly);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_fsync(long mountp, int fd, boolean dataonly);

  /**
   * Apply or remove an advisory lock.
   *
   * @param fd File descriptor to lock or unlock.
   * @param operation the advisory lock operation to be performed on the file
   * descriptor among LOCK_SH (shared lock), LOCK_EX (exclusive lock),
   * or LOCK_UN (remove lock). The LOCK_NB value can be ORed to perform a
   * non-blocking operation.
   * @param owner the user-supplied owner identifier (an arbitrary integer)
   */
  public void flock(int fd, int operation, long owner) throws IOException {
    rlock.lock();
    try {
      native_ceph_flock(instance_ptr, fd, operation, owner);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_flock(long mountp, int fd, int operation, long owner);

  /**
   * Get file status.
   *
   * @param fd The file descriptor.
   * @param stat The object in which to store the status.
   */
  public void fstat(int fd, CephStat stat) {
    rlock.lock();
    try {
      native_ceph_fstat(instance_ptr, fd, stat);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_fstat(long mountp, int fd, CephStat stat);

  /**
   * Synchronize the client with the file system.
   */
  public void sync_fs() {
    rlock.lock();
    try {
      native_ceph_sync_fs(instance_ptr);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_sync_fs(long mountp);

  /**
   * Get an extended attribute value.
   *
   * If the buffer is large enough to hold the entire attribute value, or
   * buf is null, the size of the value is returned.
   *
   * @param path File path.
   * @param name Name of the attribute.
   * @param buf Buffer to store attribute value.
   * @return The length of the attribute value. See description for more
   * details.
   */
  public long getxattr(String path, String name, byte[] buf) throws FileNotFoundException {
    rlock.lock();
    try {
      return native_ceph_getxattr(instance_ptr, path, name, buf);
    } finally {
      rlock.unlock();
    }
  }

  private static native long native_ceph_getxattr(long mountp, String path, String name, byte[] buf);

  /**
   * Get an extended attribute value of a symbolic link.
   *
   * If the buffer is large enough to hold the entire attribute value, or
   * buf is null, the size of the value is returned.
   *
   * @param path File path.
   * @param name Name of attribute.
   * @param buf Buffer to store attribute value.
   * @return The length of the attribute value. See description for more
   * details.
   */
  public long lgetxattr(String path, String name, byte[] buf) throws FileNotFoundException {
    rlock.lock();
    try {
      return native_ceph_lgetxattr(instance_ptr, path, name, buf);
    } finally {
      rlock.unlock();
    }
  }

  private static native long native_ceph_lgetxattr(long mountp, String path, String name, byte[] buf);

  /**
   * List extended attributes.
   *
   * @param path File path.
   * @return List of attribute names.
   */
  public String[] listxattr(String path) throws FileNotFoundException {
    rlock.lock();
    try {
      return native_ceph_listxattr(instance_ptr, path);
    } finally {
      rlock.unlock();
    }
  }

  private static native String[] native_ceph_listxattr(long mountp, String path);

  /**
   * List extended attributes of a symbolic link.
   *
   * @param path File path.
   * @return List of attribute names.
   */
  public String[] llistxattr(String path) throws FileNotFoundException {
    rlock.lock();
    try {
      return native_ceph_llistxattr(instance_ptr, path);
    } finally {
      rlock.unlock();
    }
  }

  private static native String[] native_ceph_llistxattr(long mountp, String path);

  /**
   * Remove an extended attribute.
   *
   * @param path File path.
   * @param name Name of attribute.
   */
  public void removexattr(String path, String name) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_removexattr(instance_ptr, path, name);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_removexattr(long mountp, String path, String name);

  /**
   * Remove an extended attribute from a symbolic link.
   *
   * @param path File path.
   * @param name Name of attribute.
   */
  public void lremovexattr(String path, String name) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_lremovexattr(instance_ptr, path, name);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_lremovexattr(long mountp, String path, String name);

  /**
   * Set the value of an extended attribute.
   *
   * @param path The file path.
   * @param name The attribute name.
   * @param buf The attribute value.
   * @param size The size of the attribute value.
   * @param flags Flag controlling behavior (XATTR_CREATE/REPLACE/NONE).
   */
  public void setxattr(String path, String name, byte[] buf, long size, int flags) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_setxattr(instance_ptr, path, name, buf, size, flags);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_setxattr(long mountp, String path, String name, byte[] buf, long size, int flags);

  /**
   * Set the value of an extended attribute on a symbolic link.
   *
   * @param path The file path.
   * @param name The attribute name.
   * @param buf The attribute value.
   * @param size The size of the attribute value.
   * @param flags Flag controlling behavior (XATTR_CREATE/REPLACE/NONE).
   */
  public void lsetxattr(String path, String name, byte[] buf, long size, int flags) throws FileNotFoundException {
    rlock.lock();
    try {
      native_ceph_lsetxattr(instance_ptr, path, name, buf, size, flags);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_lsetxattr(long mountp, String path, String name, byte[] buf, long size, int flags);

  /**
   * Get the stripe unit of a file.
   *
   * @param fd The file descriptor.
   * @return The stripe unit.
   */
  public int get_file_stripe_unit(int fd) {
    rlock.lock();
    try {
      return native_ceph_get_file_stripe_unit(instance_ptr, fd);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_get_file_stripe_unit(long mountp, int fd);

  /**
   * Get the name of the pool a file is stored in.
   *
   * @param fd An open file descriptor.
   * @return The pool name.
   */
  public String get_file_pool_name(int fd) {
    rlock.lock();
    try {
      return native_ceph_get_file_pool_name(instance_ptr, fd);
    } finally {
      rlock.unlock();
    }
  }

  private static native String native_ceph_get_file_pool_name(long mountp, int fd);

  /**
   * Get the replication of a file.
   *
   * @param fd The file descriptor.
   * @return The file replication.
   */
  public int get_file_replication(int fd) {
    rlock.lock();
    try {
      return native_ceph_get_file_replication(instance_ptr, fd);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_get_file_replication(long mountp, int fd);

  /**
   * Favor reading from local replicas when possible.
   *
   * @param state Enable or disable localized reads.
   */
  public void localize_reads(boolean state) {
    rlock.lock();
    try {
      native_ceph_localize_reads(instance_ptr, state);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_localize_reads(long mountp, boolean on);

  /**
   * Get file layout stripe unit granularity.
   *
   * @return Stripe unit granularity.
   */
  public int get_stripe_unit_granularity() {
    rlock.lock();
    try {
      return native_ceph_get_stripe_unit_granularity(instance_ptr);
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_get_stripe_unit_granularity(long mountp);

  /**
   * Get the pool id for the named pool.
   *
   * @param name The pool name.
   * @return The pool id.
   */
  public int get_pool_id(String name) throws CephPoolException {
    rlock.lock();
    try {
      return native_ceph_get_pool_id(instance_ptr, name);
    } catch (FileNotFoundException e) {
      throw new CephPoolException("pool name " + name + " not found");
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_get_pool_id(long mountp, String name) throws FileNotFoundException;

  /**
   * Get the pool replication factor.
   *
   * @param pool_id The pool id.
   * @return Number of replicas stored in the pool.
   */
  public int get_pool_replication(int pool_id) throws CephPoolException {
    rlock.lock();
    try {
      return native_ceph_get_pool_replication(instance_ptr, pool_id);
    } catch (FileNotFoundException e) {
      throw new CephPoolException("pool id " + pool_id + " not found");
    } finally {
      rlock.unlock();
    }
  }

  private static native int native_ceph_get_pool_replication(long mountp, int pool_id) throws FileNotFoundException;

  /**
   * Get file extent containing a given offset.
   *
   * @param fd The file descriptor.
   * @param offset Offset in file.
   * @return A CephFileExtent object.
   */
  public CephFileExtent get_file_extent(int fd, long offset) {
    rlock.lock();
    try {
      return native_ceph_get_file_extent_osds(instance_ptr, fd, offset);
    } finally {
      rlock.unlock();
    }
  }

  private static native CephFileExtent native_ceph_get_file_extent_osds(long mountp, int fd, long offset);

  /**
   * Get the fully qualified CRUSH location of an OSD.
   *
   * Returns (type, name) string pairs for each device in the CRUSH bucket
   * hierarchy starting from the given OSD to the root.
   *
   * @param osd The OSD device id.
   * @return List of pairs.
   */
  public Bucket[] get_osd_crush_location(int osd) {
    rlock.lock();
    try {
      String[] parts = native_ceph_get_osd_crush_location(instance_ptr, osd);
      Bucket[] path = new Bucket[parts.length / 2];
      for (int i = 0; i < path.length; i++)
        path[i] = new Bucket(parts[i*2], parts[i*2+1]);
      return path;
    } finally {
      rlock.unlock();
    }
  }

  private static native String[] native_ceph_get_osd_crush_location(long mountp, int osd);

  /**
   * Get the network address of an OSD.
   *
   * @param osd The OSD device id.
   * @return The network address.
   */
  public InetAddress get_osd_address(int osd) {
    rlock.lock();
    try {
      return native_ceph_get_osd_addr(instance_ptr, osd);
    } finally {
      rlock.unlock();
    }
  }

  private static native InetAddress native_ceph_get_osd_addr(long mountp, int osd);
}
