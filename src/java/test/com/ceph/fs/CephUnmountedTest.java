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

import org.junit.*;
import static org.junit.Assert.*;

public class CephUnmountedTest {

  private CephMount mount;

  @Before
  public void setup() throws Exception {
    mount = new CephMount("admin");
  }

  @Test(expected=CephNotMountedException.class)
  public void test_unmount() throws Exception {
    mount.unmount();
  }

  @Test(expected=CephNotMountedException.class)
  public void test_statfs() throws Exception {
    CephStatVFS stat = new CephStatVFS();
    mount.statfs("/a/path", stat);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_getcwd() throws Exception {
    mount.getcwd();
  }

  @Test(expected=CephNotMountedException.class)
  public void test_chdir() throws Exception {
    mount.chdir("/a/path");
  }

  @Test(expected=CephNotMountedException.class)
  public void test_listdir() throws Exception {
    mount.listdir("/a/path");
  }

  @Test(expected=CephNotMountedException.class)
  public void test_unlink() throws Exception {
    mount.unlink("/a/path");
  }

  @Test(expected=CephNotMountedException.class)
  public void test_rename() throws Exception {
    mount.rename("/a/path", "/another/path");
  }

  @Test(expected=CephNotMountedException.class)
  public void test_mkdirs() throws Exception {
    mount.mkdirs("/a/path", 0);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_rmdir() throws Exception {
    mount.rmdir("/a/path");
  }

  @Test(expected=CephNotMountedException.class)
  public void test_stat() throws Exception {
    CephStat stat = new CephStat();
    mount.stat("/a/path", stat);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_lstat() throws Exception {
    CephStat stat = new CephStat();
    mount.lstat("/a/path", stat);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_setattr() throws Exception {
    CephStat stat = new CephStat();
    mount.setattr("/a/path", stat, 0);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_open() throws Exception {
    mount.open("/a/path", 0, 0);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_open_layout() throws Exception {
    mount.open("/a/path", 0, 0, 0, 0, 0, null);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_close() throws Exception {
    mount.close(0);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_lseek() throws Exception {
    mount.lseek(0, 0, CephMount.SEEK_CUR);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_read() throws Exception {
    byte[] buf = new byte[1];
    mount.read(0, buf, 1, 0);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_write() throws Exception {
    byte[] buf = new byte[1];
    mount.write(0, buf, 1, 0);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_get_stripe_unit() throws Exception {
    mount.get_file_stripe_unit(0);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_get_repl() throws Exception {
    mount.get_file_replication(0);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_get_stripe_unit_gran() throws Exception {
    mount.get_stripe_unit_granularity();
  }

  @Test(expected=CephNotMountedException.class)
  public void test_get_pool_id() throws Exception {
    mount.get_pool_id("data");
  }

  @Test(expected=CephNotMountedException.class)
  public void test_get_pool_replication() throws Exception {
    mount.get_pool_replication(1);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_fchmod() throws Exception {
    mount.fchmod(1, 0);
  }

  @Test(expected=CephNotMountedException.class)
  public void test_chmod() throws Exception {
    mount.chmod("/foo", 0);
  }
}
