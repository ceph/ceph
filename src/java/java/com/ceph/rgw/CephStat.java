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

/**
 * Holds struct stat fields.
 */
public class CephStat {
    private static final int FLAG_IFLNK = 0120000;
    private static final int FLAG_IFREG = 0100000;
    private static final int FLAG_IFDIR = 0040000;

    /* Set from native */
    public int mode;
    public int uid;
    public int gid;
    public long size;
    public long blksize;
    public long blocks;
    public long a_time;
    public long m_time;

    public CephStat() {
    }

    public CephStat(int mode) {
        this.mode = mode;
    }

    public CephStat(long m_time, long a_time) {
        this.m_time = m_time;
        this.a_time = a_time;
    }

    public CephStat(int mode, int uid, int gid, long size, long blksize, long blocks, long m_time, long a_time) {
        this.mode = mode;
        this.uid = uid;
        this.gid = gid;
        this.size = size;
        this.blksize = blksize;
        this.blocks = blocks;
        this.a_time = a_time;
        this.m_time = m_time;
    }

    public boolean isFile() {
        return (mode & FLAG_IFREG) != 0;
    }

    public boolean isDir() {
        return (mode & FLAG_IFDIR) != 0;
    }

    public boolean isSymlink() {
        return (mode & FLAG_IFLNK) != 0;
    }
}
