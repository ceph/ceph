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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import org.junit.*;
import static org.junit.Assert.*;

import com.ceph.crush.Bucket;

/*
 * Coverage
 *  - Everything is covered in at least success cases.
 *  - l[set,get,remove]xattr are not working
 */

public class CephMountTest {

  private static CephMount mount;
  private static String basedir = null;

  @BeforeClass
  public static void setup() throws Exception {
    mount = new CephMount("admin");

    String conf_file = System.getProperty("CEPH_CONF_FILE");
    if (conf_file != null)
      mount.conf_read_file(conf_file);
    mount.conf_set("client_permissions", "0");

    mount.mount(null);

    basedir = "/libcephfs_junit_" + UUID.randomUUID();
    mount.mkdir(basedir, 0777);
  }

  @AfterClass
  public static void destroy() throws Exception {
    String[] list = mount.listdir(basedir);
    for (String l : list)
      System.out.println(l);
    mount.rmdir(basedir);
    mount.unmount();
  }

  /*
   * Helper function to construct a unique path.
   */
  public String makePath() {
    String path = basedir + "/" + UUID.randomUUID();
    return path;
  }

  /*
   * Helper to learn the data pool name, by reading it
   * from the '/' dir inode.
   */
  public String getRootPoolName() throws Exception
  {
    int fd = mount.open("/", CephMount.O_DIRECTORY, 0600);
    String pool = mount.get_file_pool_name(fd);
    mount.close(fd);
    return pool;
  }

  /*
   * Helper function to create a file with the given path and size. The file
   * is filled with size bytes and the file descriptor is returned.
   */
  public int createFile(String path, int size) throws Exception {
    int fd = mount.open(path, CephMount.O_RDWR|CephMount.O_CREAT, 0600);
    byte[] buf = new byte[4096];
    int left = size;
    while (left > 0) {
      size = Math.min(buf.length, left);
      long ret = mount.write(fd, buf, size, -1);
      left -= ret;
    }
    return fd;
  }

  /*
   * Helper function to create a unique file and fill it with size bytes. The
   * file descriptor is returned.
   */
  public int createFile(int size) throws Exception {
    return createFile(makePath(), size);
  }

  @Test(expected=FileNotFoundException.class)
  public void test_mount_dne() throws Exception {
    CephMount mount2 = new CephMount("admin");
    String conf_file = System.getProperty("CEPH_CONF_FILE");
    if (conf_file != null)
      mount2.conf_read_file(conf_file);
    mount2.mount("/wlfkjwlekfjwlejfwe");
    mount2.unmount();
  }

  /*
   * Test loading of conf file that doesn't exist.
   *
   * FIXME:
   * Ceph returns -ENOSYS rather than -ENOENT. Correct?
   */
  //@Test(expected=FileNotFoundException.class)
  @Test
  public void test_conf_read_file_dne() throws Exception {
    //mount.conf_read_file("/this_file_does_not_exist");
  }

  /*
   * Test loading of conf file that isn't valid
   *
   * FIXME: implement
   */
  @Test
  public void test_conf_read_file_invalid() throws Exception {
  }

  @Test(expected=NullPointerException.class)
  public void test_conf_read_file_null() throws Exception {
    mount.conf_read_file(null);
  }

  /*
   * conf_set/conf_get
   */

  @Test(expected=NullPointerException.class)
  public void test_conf_set_null_opt() throws Exception {
    mount.conf_set(null, "value");
  }

  @Test(expected=NullPointerException.class)
  public void test_conf_set_null_val() throws Exception {
    mount.conf_set("option", null);
  }

  @Test(expected=NullPointerException.class)
  public void test_conf_get_null_opt() throws Exception {
    mount.conf_get(null);
  }

  @Test
  public void test_conf() throws Exception {
    String opt = "log to stderr";
    String val1, val2, val3;

    /* get the current value */
    val1 = mount.conf_get(opt);

    /*
     * flip the value. this may make some debug information be dumped to the
     * console when the value becomes true. TODO: find a better config option
     * to toggle.
     */
    if (val1.compareTo("true") == 0)
      val2 = "false";
    else
      val2 = "true";
    mount.conf_set(opt, val2);

    /* verify the change */
    val3 = mount.conf_get(opt);
    assertTrue(val3.compareTo(val2) == 0);

    /* reset to original value */
    mount.conf_set(opt, val1);
    val3 = mount.conf_get(opt);
    assertTrue(val3.compareTo(val1) == 0);
  }

  /*
   * statfs
   */

  @Test
  public void test_statfs() throws Exception {
    CephStatVFS st1 = new CephStatVFS();
    mount.statfs("/", st1);

    /*
     * FIXME: a better test here is to see if changes to the file system are
     * reflected through statfs (e.g. increasing number of files). However, it
     * appears that the updates aren't immediately visible.
     */
    assertTrue(st1.bsize > 0);
    assertTrue(st1.frsize > 0);
    assertTrue(st1.blocks > 0);
    assertTrue(st1.bavail > 0);
    assertTrue(st1.namemax > 0);
  }

  /*
   * getcwd/chdir
   */

  @Test
  public void test_getcwd() throws Exception {
    mount.chdir(basedir);
    String cwd = mount.getcwd();
    assertTrue(cwd.compareTo(basedir) == 0);

    /* Make sure to reset cwd to root */
    mount.chdir("/");
    cwd = mount.getcwd();
    assertTrue(cwd.compareTo("/") == 0);
  }

  @Test(expected=NullPointerException.class)
  public void test_chdir_null() throws Exception {
    mount.chdir(null);
  }

  @Test(expected=FileNotFoundException.class)
  public void test_chdir_dne() throws Exception {
    mount.chdir("/this/path/does/not/exist/");
  }

  /*
   * FIXME: this test should throw an error (but does not)?
   */
  //@Test(expected=IOException.class)
  @Test
  public void test_chdir_not_dir() throws Exception {
    String path = makePath();
    int fd = createFile(path, 1);
    mount.close(fd);
    //mount.chdir(path); shouldn't be able to do this?
    mount.unlink(path);

    /*
     * Switch back. Other tests seem to be sensitive to the current directory
     * being something other than "/". This shouldn't happen once this tests
     * passes and the call to chdir fails anyway.
     */
    mount.chdir("/");
  }

  /*
   * listdir
   */

  @Test(expected=NullPointerException.class)
  public void test_listdir_null() throws Exception {
    mount.listdir(null);
  }

  @Test(expected=FileNotFoundException.class)
  public void test_listdir_dne() throws Exception {
    mount.listdir("/this/path/does/not/exist/");
  }

  @Test(expected=IOException.class)
  public void test_listdir_not_dir() throws Exception {
    String path = makePath();
    int fd = createFile(path, 1);
    mount.close(fd);
    try {
      mount.listdir(path);
    } finally {
      mount.unlink(path);
    }
  }

  @Test
  public void test_listdir() throws Exception {
    String dir = makePath();
    mount.mkdir(dir, 0777);
    /* test that new directory is empty */
    String[] list = mount.listdir(dir);
    assertTrue(list.length == 0);
    /* test that new directories are seen */
    for (int i = 0; i < 3; i++)
      mount.mkdir(dir + "/" + i, 777);
    list = mount.listdir(dir);
    assertTrue(list.length == 3);
    /* test that more new directories are seen */
    for (int i = 0; i < 30; i++)
      mount.mkdir(dir + "/x" + i, 777);
    list = mount.listdir(dir);
    assertTrue(list.length == 33);

    /* remove */
    for (int i = 0; i < 30; i++)
      mount.rmdir(dir + "/x" + i);
    for (int i = 0; i < 3; i++)
      mount.rmdir(dir + "/" + i);
    mount.rmdir(dir);
  }

  /*
   * Missing
   *
   * ceph_link
   * ceph_unlink
   */

  /*
   * rename
   */

  @Test(expected=NullPointerException.class)
  public void test_rename_null_from() throws Exception {
    mount.rename(null, "to");
  }

  @Test(expected=NullPointerException.class)
  public void test_rename_null_to() throws Exception {
    mount.rename("from", null);
  }

  @Test(expected=FileNotFoundException.class)
  public void test_rename_dne() throws Exception {
    mount.rename("/this/doesnt/exist", "/this/neither");
  }

  @Test
  public void test_rename() throws Exception {
    /* create a file */
    String path = makePath();
    int fd = createFile(path, 1);
    mount.close(fd);

    /* move it to a new name */
    String newpath = makePath();
    mount.rename(path, newpath);

    /* verfiy the sizes are the same */
    CephStat st = new CephStat();
    mount.lstat(newpath, st);
    assertTrue(st.size == 1);

    /* remove the file */
    mount.unlink(newpath);
  }

  /*
   * mkdir/mkdirs/rmdir
   */

  @Test(expected=IOException.class)
  public void test_mkdir_exists() throws Exception {
    String path = makePath();
    mount.mkdir(path, 0777);
    try {
      mount.mkdir(path, 0777);
    } finally {
      mount.rmdir(path);
    }
  }

  @Test(expected=IOException.class)
  public void test_mkdirs_exists() throws Exception {
    String path = makePath();
    mount.mkdirs(path, 0777);
    try {
      mount.mkdirs(path, 0777);
    } finally {
      mount.rmdir(path);
    }
  }

  @Test
  public void test_mkdir() throws Exception {
    String path = makePath();
    mount.mkdir(path, 0777);
    CephStat st = new CephStat();
    mount.lstat(path, st);
    assertTrue(st.isDir());
    mount.rmdir(path);
  }

  @Test
  public void test_mkdirs() throws Exception {
    String path = makePath();
    mount.mkdirs(path + "/x/y", 0777);

    CephStat st = new CephStat();
    mount.lstat(path, st);
    assertTrue(st.isDir());

    mount.lstat(path + "/x", st);
    assertTrue(st.isDir());

    mount.lstat(path + "/x/y", st);
    assertTrue(st.isDir());

    mount.rmdir(path + "/x/y");
    mount.rmdir(path + "/x");
    mount.rmdir(path);
  }

  @Test(expected=FileNotFoundException.class)
  public void test_rmdir() throws Exception {
    /* make a new directory */
    String path = makePath();
    mount.mkdir(path, 0777);
    CephStat st = new CephStat();
    mount.lstat(path, st);
    assertTrue(st.isDir());
    /* remove it */
    mount.rmdir(path);
    /* should not exist now */
    mount.lstat(path, st);
  }

  /*
   * readlink
   * symlink
   */
  @Test
  public void test_symlink() throws Exception {
    String oldpath = makePath();
    String newpath = makePath();

    mount.symlink(oldpath, newpath);
    CephStat stat = new CephStat();
    mount.lstat(newpath, stat);
    assertTrue(stat.isSymlink());

    String symlink = mount.readlink(newpath);
    assertTrue(symlink.compareTo(oldpath) == 0);

    mount.unlink(newpath);
  }

  /*
   * lstat
   */

  @Test(expected=NullPointerException.class)
  public void test_lstat_null_path() throws Exception {
    mount.lstat(null, new CephStat());
  }

  @Test(expected=NullPointerException.class)
  public void test_lstat_null_stat() throws Exception {
    mount.lstat("/path", null);
  }

  @Test(expected=FileNotFoundException.class)
  public void test_lstat_null_dne() throws Exception {
    mount.lstat("/path/does/not/exist", new CephStat());
  }

  /*
   * test_stat covers lstat and fstat and stat.
   *
   * TODO: create test that for lstat vs stat with symlink follow/nofollow.
   */

  @Test
  public void test_stat() throws Exception {
    /* create a new file */
    String path = makePath();
    int size = 12345;
    int fd = createFile(path, size);
    mount.close(fd);

    /* test some basic info about the new file */
    CephStat orig_st = new CephStat();
    mount.lstat(path, orig_st);
    assertTrue(orig_st.size == size);
    assertTrue(orig_st.blksize > 0);
    assertTrue(orig_st.blocks > 0);

    /* now try stat */
    CephStat stat_st = new CephStat();
    mount.stat(path, stat_st);

    /* now try fstat */
    CephStat other_st = new CephStat();
    fd = mount.open(path, CephMount.O_RDWR, 0);
    mount.fstat(fd, other_st);
    mount.close(fd);

    mount.unlink(path);

    /* compare to fstat results */
    assertTrue(orig_st.mode == other_st.mode);
    assertTrue(orig_st.uid == other_st.uid);
    assertTrue(orig_st.gid == other_st.gid);
    assertTrue(orig_st.size == other_st.size);
    assertTrue(orig_st.blksize == other_st.blksize);
    assertTrue(orig_st.blocks == other_st.blocks);

    /* compare to stat results */
    assertTrue(orig_st.mode == stat_st.mode);
    assertTrue(orig_st.uid == stat_st.uid);
    assertTrue(orig_st.gid == stat_st.gid);
    assertTrue(orig_st.size == stat_st.size);
    assertTrue(orig_st.blksize == stat_st.blksize);
    assertTrue(orig_st.blocks == stat_st.blocks);
  }

  /*
   * stat
   */

  @Test(expected=NullPointerException.class)
  public void test_stat_null_path() throws Exception {
    mount.stat(null, new CephStat());
  }

  @Test(expected=NullPointerException.class)
  public void test_stat_null_stat() throws Exception {
    mount.stat("/path", null);
  }

  @Test(expected=FileNotFoundException.class)
  public void test_stat_null_dne() throws Exception {
    mount.stat("/path/does/not/exist", new CephStat());
  }

  @Test(expected=CephNotDirectoryException.class)
  public void test_enotdir() throws Exception {
    String path = makePath();
    int fd = createFile(path, 1);
    mount.close(fd);

    try {
      CephStat stat = new CephStat();
      mount.lstat(path + "/blah", stat);
    } finally {
      mount.unlink(path);
    }
  }

  /*
   * setattr
   */

  @Test(expected=NullPointerException.class)
  public void test_setattr_null_path() throws Exception {
    mount.setattr(null, new CephStat(), 0);
  }

  @Test(expected=NullPointerException.class)
  public void test_setattr_null_stat() throws Exception {
    mount.setattr("/path", null, 0);
  }

  @Test(expected=FileNotFoundException.class)
  public void test_setattr_dne() throws Exception {
    mount.setattr("/path/does/not/exist", new CephStat(), 0);
  }

  @Test
  public void test_setattr() throws Exception {
    /* create a file */
    String path = makePath();
    int fd = createFile(path, 1);
    mount.close(fd);

    CephStat st1 = new CephStat();
    mount.lstat(path, st1);

    st1.uid += 1;
    st1.gid += 1;
    mount.setattr(path, st1, mount.SETATTR_UID|mount.SETATTR_GID);

    CephStat st2 = new CephStat();
    mount.lstat(path, st2);

    assertTrue(st2.uid == st1.uid);
    assertTrue(st2.gid == st1.gid);

    /* remove the file */
    mount.unlink(path);
  }

  /*
   * chmod
   */

  @Test(expected=NullPointerException.class)
  public void test_chmod_null_path() throws Exception {
    mount.chmod(null, 0);
  }

  @Test(expected=FileNotFoundException.class)
  public void test_chmod_dne() throws Exception {
    mount.chmod("/path/does/not/exist", 0);
  }

  @Test
  public void test_chmod() throws Exception {
    /* create a file */
    String path = makePath();
    int fd = createFile(path, 1);
    mount.close(fd);

    CephStat st = new CephStat();
    mount.lstat(path, st);

    /* flip a bit */
    int mode = st.mode;
    if ((mode & 1) != 0)
      mode -= 1;
    else
      mode += 1;

    mount.chmod(path, mode);
    CephStat st2 = new CephStat();
    mount.lstat(path, st2);
    assertTrue(st2.mode == mode);

    mount.unlink(path);
  }

  /*
   * fchmod
   */

  @Test
  public void test_fchmod() throws Exception {
    /* create a file */
    String path = makePath();
    int fd = createFile(path, 1);

    CephStat st = new CephStat();
    mount.lstat(path, st);

    /* flip a bit */
    int mode = st.mode;
    if ((mode & 1) != 0)
      mode -= 1;
    else
      mode += 1;

    mount.fchmod(fd, mode);
    mount.close(fd);

    CephStat st2 = new CephStat();
    mount.lstat(path, st2);
    assertTrue(st2.mode == mode);

    mount.unlink(path);
  }

  /*
   * truncate
   */

  @Test(expected=FileNotFoundException.class)
  public void test_truncate_dne() throws Exception {
    mount.truncate("/path/does/not/exist", 0);
  }

  @Test(expected=NullPointerException.class)
  public void test_truncate_null_path() throws Exception {
    mount.truncate(null, 0);
  }

  @Test
  public void test_truncate() throws Exception {
    // make file
    String path = makePath();
    int orig_size = 1398331;
    int fd = createFile(path, orig_size);
    mount.close(fd);

    // check file size
    CephStat st = new CephStat();
    mount.lstat(path, st);
    assertTrue(st.size == orig_size);

    // truncate and check
    int crop_size = 333333;
    mount.truncate(path, crop_size);
    mount.lstat(path, st);
    assertTrue(st.size == crop_size);

    // check after re-open
    fd = mount.open(path, CephMount.O_RDWR, 0);
    mount.fstat(fd, st);
    assertTrue(st.size == crop_size);
    mount.close(fd);

    mount.unlink(path);
  }

  @Test
  public void test_open_layout() throws Exception {
    String path = makePath();
    int fd = mount.open(path, CephMount.O_WRONLY|CephMount.O_CREAT, 0,
        (1<<20), 1, (1<<20), null);
    mount.close(fd);
    mount.unlink(path);
  }

  /*
   * open/close
   */

  @Test(expected=FileNotFoundException.class)
  public void test_open_dne() throws Exception {
    mount.open("/path/doesnt/exist", 0, 0);
  }

  /*
   * lseek
   */

  @Test
  public void test_lseek() throws Exception {
    /* create a new file */
    String path = makePath();
    int size = 12345;
    int fd = createFile(path, size);
    mount.close(fd);

    /* open and check size */
    fd = mount.open(path, CephMount.O_RDWR, 0);
    long end = mount.lseek(fd, 0, CephMount.SEEK_END);
    mount.close(fd);

    mount.unlink(path);

    assertTrue(size == (int)end);
  }

  /*
   * read/write
   */

  @Test
  public void test_read() throws Exception {
    String path = makePath();
    int fd = createFile(path, 1500);
    byte[] buf = new byte[1500];
    long ret = mount.read(fd, buf, 1500, 0);
    assertTrue(ret == 1500);
    mount.unlink(path);
  }

  /*
   * ftruncate
   */

  @Test
  public void test_ftruncate() throws Exception {
    // make file
    String path = makePath();
    int orig_size = 1398331;
    int fd = createFile(path, orig_size);

    // check file size
    CephStat st = new CephStat();
    mount.fstat(fd, st);
    assertTrue(st.size == orig_size);

    // truncate and check
    int crop_size = 333333;
    mount.ftruncate(fd, crop_size);
    mount.fstat(fd, st);
    if (st.size != crop_size) {
      System.err.println("ftruncate error: st.size=" + st.size + " crop_size=" + crop_size);
      assertTrue(false);
    }
    assertTrue(st.size == crop_size);
    mount.close(fd);

    // check after re-open
    fd = mount.open(path, CephMount.O_RDWR, 0);
    mount.fstat(fd, st);
    assertTrue(st.size == crop_size);
    mount.close(fd);

    mount.unlink(path);
  }

  /*
   * fsync
   */

  @Test
  public void test_fsync() throws Exception {
    String path = makePath();
    int fd = createFile(path, 123);
    mount.fsync(fd, false);
    mount.fsync(fd, true);
    mount.close(fd);
    mount.unlink(path);
  }

  /*
   * flock
   */

  @Test
  public void test_flock() throws Exception {
    String path = makePath();
    int fd = createFile(path, 123);
    mount.flock(fd, CephMount.LOCK_SH | CephMount.LOCK_NB, 42);
    mount.flock(fd, CephMount.LOCK_SH | CephMount.LOCK_NB, 43);
    mount.flock(fd, CephMount.LOCK_UN, 42);
    mount.flock(fd, CephMount.LOCK_UN, 43);
    mount.flock(fd, CephMount.LOCK_EX | CephMount.LOCK_NB, 42);
    try {
      mount.flock(fd, CephMount.LOCK_SH | CephMount.LOCK_NB, 43);
      assertTrue(false);
    } catch(IOException io) {}
    try {
      mount.flock(fd, CephMount.LOCK_EX | CephMount.LOCK_NB, 43);
      assertTrue(false);
    } catch(IOException io) {}
    mount.flock(fd, CephMount.LOCK_SH, 42);  // downgrade
    mount.flock(fd, CephMount.LOCK_SH, 43);
    mount.flock(fd, CephMount.LOCK_UN, 42);
    mount.flock(fd, CephMount.LOCK_UN, 43);
    mount.close(fd);
    mount.unlink(path);
  }

  /*
   * fstat
   *
   * success case is handled in test_stat along with lstat.
   */

  /*
   * sync_fs
   */

  @Test
  public void test_sync_fs() throws Exception {
    mount.sync_fs();
  }

  /*
   * get/set/list/remove xattr
   */

  @Test
  public void test_xattr() throws Exception {
    /* make file */
    String path = makePath();
    int fd = createFile(path, 123);
    mount.close(fd);

    /* make xattrs */
    String val1 = "This is a new xattr";
    String val2 = "This is a different xattr";
    byte[] buf1 = val1.getBytes();
    byte[] buf2 = val2.getBytes();
    mount.setxattr(path, "user.attr1", buf1, buf1.length, mount.XATTR_CREATE);
    mount.setxattr(path, "user.attr2", buf2, buf2.length, mount.XATTR_CREATE);

    /* list xattrs */
    String[] xattrs = mount.listxattr(path);
    assertTrue(xattrs.length == 2);
    int found = 0;
    for (String xattr : xattrs) {
      if (xattr.compareTo("user.attr1") == 0) {
        found++;
        continue;
      }
      if (xattr.compareTo("user.attr2") == 0) {
        found++;
        continue;
      }
      System.out.println("found unwanted xattr: " + xattr);
    }
    assertTrue(found == 2);

    /* get first xattr by looking up length */
    long attr1_len = mount.getxattr(path, "user.attr1", null);
    byte[] out = new byte[(int)attr1_len];
    mount.getxattr(path, "user.attr1", out);
    String outStr = new String(out);
    assertTrue(outStr.compareTo(val1) == 0);

    /* get second xattr assuming original length */
    out = new byte[buf2.length];
    mount.getxattr(path, "user.attr2", out);
    outStr = new String(out);
    assertTrue(outStr.compareTo(val2) == 0);

    /* remove the attributes */
    /* FIXME: the MDS returns ENODATA for removexattr */
    /*
    mount.removexattr(path, "attr1");
    xattrs = mount.listxattr(path);
    assertTrue(xattrs.length == 1);
    mount.removexattr(path, "attr2");
    xattrs = mount.listxattr(path);
    assertTrue(xattrs.length == 0);
    */

    mount.unlink(path);
  }

  /*
   * get/set/list/remove symlink xattr
   *
   * Currently not working. Code is the same as for regular xattrs, so there
   * might be a deeper issue.
   */

  @Test
  public void test_get_stripe_unit() throws Exception {
    String path = makePath();
    int fd = createFile(path, 1);
    assertTrue(mount.get_file_stripe_unit(fd) > 0);
    mount.close(fd);
    mount.unlink(path);
  }

  @Test
  public void test_get_repl() throws Exception {
    String path = makePath();
    int fd = createFile(path, 1);
    assertTrue(mount.get_file_replication(fd) > 0);
    mount.close(fd);
    mount.unlink(path);
  }

  /*
   * stripe unit granularity
   */

  @Test
  public void test_get_stripe_unit_gran() throws Exception {
    assertTrue(mount.get_stripe_unit_granularity() > 0);
  }

  @Test
  public void test_get_pool_id() throws Exception {
    String data_pool_name = getRootPoolName();
    /* returns valid pool id */
    assertTrue(mount.get_pool_id(data_pool_name) >= 0);

    /* test non-existent pool name */
    try {
      mount.get_pool_id("asdlfkjlsejflkjef");
      assertTrue(false);
    } catch (CephPoolException e) {}
  }

  @Test
  public void test_get_pool_replication() throws Exception {
    /* test invalid pool id */
    try {
      mount.get_pool_replication(-1);
      assertTrue(false);
    } catch (CephPoolException e) {}

    /* test valid pool id */
    String data_pool_name = getRootPoolName();
    int poolid = mount.get_pool_id(data_pool_name);
    assertTrue(poolid >= 0);
    assertTrue(mount.get_pool_replication(poolid) > 0);
  }

  @Test
  public void test_get_file_pool_name() throws Exception {
    String data_pool_name = getRootPoolName();
    String path = makePath();
    int fd = createFile(path, 1);
    String pool = mount.get_file_pool_name(fd);
    mount.close(fd);
    assertTrue(pool != null);
    /* assumes using default data pool */
    assertTrue(pool.compareTo(data_pool_name) == 0);
    mount.unlink(path);
  }

  @Test(expected=IOException.class)
  public void test_get_file_pool_name_ebadf() throws Exception {
    String pool = mount.get_file_pool_name(-40);
  }

  @Test
  public void test_get_file_extent() throws Exception {
    int stripe_unit = 1<<18;
    String path = makePath();
    int fd = mount.open(path, CephMount.O_WRONLY|CephMount.O_CREAT, 0,
        stripe_unit, 2, stripe_unit*2, null);

    CephFileExtent e = mount.get_file_extent(fd, 0);
    assertTrue(e.getOSDs().length > 0);

    assertTrue(e.getOffset() == 0);
    assertTrue(e.getLength() == stripe_unit);

    e = mount.get_file_extent(fd, stripe_unit/2);
    assertTrue(e.getOffset() == stripe_unit/2);
    assertTrue(e.getLength() == stripe_unit/2);

    e = mount.get_file_extent(fd, 3*stripe_unit/2-1);
    assertTrue(e.getOffset() == 3*stripe_unit/2-1);
    assertTrue(e.getLength() == stripe_unit/2+1);

    e = mount.get_file_extent(fd, 3*stripe_unit/2+1);
    assertTrue(e.getLength() == stripe_unit/2-1);

    mount.close(fd);
    mount.unlink(path);
  }

  @Test
  public void test_get_osd_crush_location() throws Exception {
    Bucket[] path = mount.get_osd_crush_location(0);
    assertTrue(path.length > 0);
    for (Bucket b : path) {
      assertTrue(b.getType().length() > 0);
      assertTrue(b.getName().length() > 0);
    }
  }

  @Test
  public void test_get_osd_address() throws Exception {
    InetAddress addr = mount.get_osd_address(0);
    assertTrue(addr.getHostAddress().length() > 0);
  }
}
