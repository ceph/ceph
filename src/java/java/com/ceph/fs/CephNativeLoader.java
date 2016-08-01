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

class CephNativeLoader {
  private static final CephNativeLoader instance = new CephNativeLoader();
  private static boolean initialized = false;

  private static final String JNI_PATH_ENV_VAR = "CEPH_JNI_PATH";
  private static final String LIBRARY_NAME = "cephfs_jni";
  private static final String LIBRARY_FILE = "libcephfs_jni.so";

  private CephNativeLoader() {}

  public static CephNativeLoader getInstance() {
    return instance;
  }

  public synchronized void loadLibrary() {
    if (initialized)
      return;

    boolean success = false;

    /*
     * Allow a Ceph specific environment variable to force
     * the loading path.
     */
    String path = System.getenv(JNI_PATH_ENV_VAR);
    try {
      if (path != null) {
        System.out.println("Loading libcephfs-jni: " + path);
        System.load(path);
        success = true;
      } else {
        try {
          /*
           * Try default Java loading path(s)
           */
          System.out.println("Loading libcephfs-jni from default path: " +
              System.getProperty("java.library.path"));
          System.loadLibrary(LIBRARY_NAME);
          success = true;
        } catch (final UnsatisfiedLinkError ule1) {
          try {
            /*
             * Try RHEL/CentOS default path
             */
            path = "/usr/lib64/" + LIBRARY_FILE;
            System.out.println("Loading libcephfs-jni: " + path);
            System.load(path);
            success = true;
          } catch (final UnsatisfiedLinkError ule2) {
            /*
             * Try Ubuntu default path
             */
            path = "/usr/lib/jni/" + LIBRARY_FILE;
            System.out.println("Loading libcephfs-jni: " + path);
            System.load(path);
            success = true;
          }
        }
      }
    } finally {
      System.out.println("Loading libcephfs-jni: " +
          (success ? "Success!" : "Failure!"));
    }

    /*
     * Finish initialization
     */
    CephMount.native_initialize();
    initialized = true;
  }

}
