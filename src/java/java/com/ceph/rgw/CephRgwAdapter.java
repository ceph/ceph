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
package com.ceph.rgw;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.LinkedList;
import java.lang.String;

public class CephRgwAdapter {
    private long rgwFS;
    private long rootFH;
    private long bucketFH;
    /*
     * Flags for open().
     *
     * Must be synchronized with JNI if changed.
     */
    public static final int O_RDONLY = 1;
    public static final int O_RDWR = 2;
    public static final int O_APPEND = 4;
    public static final int O_CREAT = 8;
    public static final int O_TRUNC = 16;
    public static final int O_EXCL = 32;
    public static final int O_WRONLY = 64;
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
    public static final int SETATTR_MODE = 1;
    public static final int SETATTR_UID = 2;
    public static final int SETATTR_GID = 4;
    public static final int SETATTR_MTIME = 8;
    public static final int SETATTR_ATIME = 16;

    /*
     * Flags for setxattr();
     *
     * Must be synchronized with JNI if changed.
     */
    public static final int XATTR_CREATE = 1;
    public static final int XATTR_REPLACE = 2;
    public static final int XATTR_NONE = 3;

    /*
     * Flags for flock();
     *
     * Must be synchronized with JNI if changed.
     */
    public static final int LOCK_SH = 1;
    public static final int LOCK_EX = 2;
    public static final int LOCK_NB = 4;
    public static final int LOCK_UN = 8;

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
            } catch (Exception e) {
            }
            try {
                native_ceph_release(rgwFS);
            } catch (Exception e) {
            }
        }
        super.finalize();
    }

    /**
     * Create a new CephRgwAdapter with specific client id.
     *
     * @param id client id.
     */
    public CephRgwAdapter(String cephArgs) {
        native_ceph_create(cephArgs);
        initialized = true;
    }

    private static native int native_ceph_create(String cephArgs);

    /**
     * Create a new CephRgwAdapter with default client id.
     */
    public CephRgwAdapter() {
        this(null);
    }

    private long getLookupFH(boolean isRoot) {
        if (isRoot == false)
            return bucketFH;
        return rootFH;
    }

    /**
     * Activate the mount with a given root path.
     *
     * @param root The path to use as the root (pass null for "/").
     */
    public void mount(String uid, String access, String secret, String root) {
        wlock.lock();
        try {
            rgwFS = native_ceph_mount(this, uid, access, secret, root);
        } finally {
            wlock.unlock();
        }
    }

    private static native long native_ceph_mount(CephRgwAdapter obj, String uid, String access, String secret, String root);

    /**
     * Deactivate the mount.
     * <p>
     * The mount can be reactivated using mount(). Configuration parameters
     * previously set are not reset.
     */
    public void unmount() {
        wlock.lock();
        try {
            native_ceph_unmount(rgwFS);
        } finally {
            wlock.unlock();
        }
    }

    private static native int native_ceph_unmount(long rgwFS);

    /*
     * Private access to low-level ceph_release.
     */
    private static native int native_ceph_release(long rgwFS);

    public void statfs(String path, boolean isRoot, CephStatVFS statvfs) throws FileNotFoundException {
        rlock.lock();
        try {
            native_ceph_statfs(rgwFS, getLookupFH(isRoot), path, statvfs);
        } finally {
            rlock.unlock();
        }
    }

    private static native int native_ceph_statfs(long rgwFS, long fh, String path, CephStatVFS statvfs);

    abstract class ListDirHandler {
        void listDirHandler(String name, int mode, int uid, int gid, long size, long blksize, long blocks, long mTime, long aTime) throws IOException {
            CephStat stat = new CephStat(mode, uid, gid, size, blksize, blocks, mTime, aTime);
            listDirCallback(name, stat);
        }

        abstract void listDirCallback(String name, CephStat stat) throws IOException;
    }

    public int listdir(String dir, boolean isRoot, LinkedList<String> nameList, LinkedList<CephStat> statList) throws FileNotFoundException {
        rlock.lock();
        try {
            int ret = native_ceph_listdir(rgwFS, getLookupFH(isRoot), dir, new ListDirHandler() {
                @Override
                void listDirCallback(String name, CephStat stat) {
                    nameList.add(name);
                    statList.add(stat);
                }
            });
            if (ret != 0) {
                nameList.clear();
                statList.clear();
                return 0;
            }
            return nameList.size();
        } finally {
            rlock.unlock();
        }
    }

    private static native int native_ceph_listdir(long rgwFS, long fh, String path, ListDirHandler handler);

    public void unlink(String path, boolean isRoot) throws FileNotFoundException {
        rlock.lock();
        try {
            native_ceph_unlink(rgwFS, getLookupFH(isRoot), path);
        } finally {
            rlock.unlock();
        }
    }

    private static native int native_ceph_unlink(long rgwFS, long fh, String path);

    public int rename(String src_path, boolean srcIsRoot, String src_name, String dst_path, boolean dstIsRoot, String dst_name) throws FileNotFoundException {
        rlock.lock();
        try {
            return native_ceph_rename(rgwFS, getLookupFH(srcIsRoot), src_path, src_name, getLookupFH(dstIsRoot), dst_path, dst_name);
        } finally {
            rlock.unlock();
        }
    }

    private static native int native_ceph_rename(long rgwFS, long src_fd, String src_path, String src_name, long dst_fd, String dst_path, String dst_name);

    public boolean mkdirs(String path, boolean isRoot, String name, int mode) throws IOException {
        rlock.lock();
        try {
            return native_ceph_mkdirs(rgwFS, getLookupFH(isRoot), path, name, mode);
        } finally {
            rlock.unlock();
        }
    }

    private static native boolean native_ceph_mkdirs(long rgwFS, long fd, String path, String name, int mode);

    public void lstat(String path, boolean isRoot, CephStat stat) throws FileNotFoundException {
        rlock.lock();
        try {
            native_ceph_lstat(rgwFS, getLookupFH(isRoot), path, stat);
        } finally {
            rlock.unlock();
        }
    }

    private static native int native_ceph_lstat(long rgwFS, long fh, String path, CephStat stat);

    public void setattr(String path, boolean isRoot, CephStat stat, int mask) throws FileNotFoundException {
        rlock.lock();
        try {
            native_ceph_setattr(rgwFS, getLookupFH(isRoot), path, stat, mask);
        } finally {
            rlock.unlock();
        }
    }

    private static native int native_ceph_setattr(long rgwFS, long fh, String path, CephStat stat, int mask);

    public long open(String path, boolean isRoot, int flags, int mode) throws FileNotFoundException {
        rlock.lock();
        try {
            return native_ceph_open(rgwFS, getLookupFH(isRoot), path, flags, mode);
        } finally {
            rlock.unlock();
        }
    }

    private static native long native_ceph_open(long rgwFS, long fh, String path, int flags, int mode);

    public void close(long fd) {
        rlock.lock();
        try {
            native_ceph_close(rgwFS, fd);
        } finally {
            rlock.unlock();
        }
    }

    private static native int native_ceph_close(long rgwFS, long fd);

    public long read(long fd, long offset, byte[] buf, long size) {
        rlock.lock();
        try {
            return native_ceph_read(rgwFS, fd, offset, buf, size);
        } finally {
            rlock.unlock();
        }
    }

    private static native long native_ceph_read(long rgwFS, long fd, long offset, byte[] buf, long size);

    public long write(long fd, long offset, byte[] buf, long size) {
        rlock.lock();
        try {
            return native_ceph_write(rgwFS, fd, offset, buf, size);
        } finally {
            rlock.unlock();
        }
    }

    private static native long native_ceph_write(long rgwFS, long fd, long offset, byte[] buf, long size);

    public void fsync(long fd, boolean dataonly) {
        rlock.lock();
        try {
            native_ceph_fsync(rgwFS, fd, dataonly);
        } finally {
            rlock.unlock();
        }
    }

    private static native int native_ceph_fsync(long rgwFS, long fd, boolean dataonly);

}
