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

import java.util.Arrays;

/**
 * Holds information about a file extent in CephFS.
 */
public class CephFileExtent {
  private long offset;
  private long length;
  private int[] osds;

  CephFileExtent(long offset, long length, int[] osds) {
    this.offset = offset;
    this.length = length;
    this.osds = osds;
  }

  /**
   * Get starting offset of extent.
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Get length of extent.
   */
  public long getLength() {
    return length;
  }

  /**
   * Get list of OSDs with this extent.
   */
  public int[] getOSDs() {
    return osds;
  }

  /**
   * Pretty print.
   */
  public String toString() {
    return "extent[" + offset + "," + length + ","
      + Arrays.toString(osds) + "]";
  }
}
