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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
//import java.lang.IndexOutOfBoundsException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;


/**
 * <p>
 * An {@link FSInputStream} for a CephFileSystem and corresponding
 * Ceph instance.
 */
public class CephInputStream extends FSInputStream {

  private int bufferSize;

  private boolean closed;

  private int fileHandle;

  private long fileLength;

  private boolean debug;

  private native int ceph_read(int fh, byte[] buffer, int buffer_offset, int length);
  private native long ceph_seek_from_start(int fh, long pos);
  private native long ceph_getpos(int fh);
  private native int ceph_close(int fh);
    
  /**
   * Create a new CephInputStream.
   * @param conf The system configuration. Unused.
   * @param fh The filehandle provided by Ceph to reference.
   * @param flength The current length of the file. If the length changes
   * you will need to close and re-open it to access the new data.
   */
  public CephInputStream(Configuration conf, int fh, long flength) {
    System.load(conf.get("fs.ceph.libDir")+"/libhadoopcephfs.so");
    System.load(conf.get("fs.ceph.libDir")+"/libceph.so");
    // Whoever's calling the constructor is responsible for doing the actual ceph_open
    // call and providing the file handle.
    fileLength = flength;
    fileHandle = fh;
    closed = false;
    debug = ("true".equals(conf.get("fs.ceph.debug", "false")));
    if(debug) debug("CephInputStream constructor: initializing stream with fh "
										+ fh + " and file length " + flength);
      
  }
  /** Ceph likes things to be closed before it shuts down,
   * so closing the IOStream stuff voluntarily in a finalizer is good
   */
  protected void finalize () throws Throwable {
    try {
      if (!closed) close();
    }
    finally { super.finalize(); }
  }

  public synchronized long getPos() throws IOException {
    return ceph_getpos(fileHandle);
  }
  /**
   * Find the number of bytes remaining in the file.
   */
  @Override
	public synchronized int available() throws IOException {
      return (int) (fileLength - getPos());
    }

  public synchronized void seek(long targetPos) throws IOException {
    if(debug) debug("CephInputStream.seek: Seeking to position " + targetPos +
										" on fd " + fileHandle);
    if (targetPos > fileLength) {
      throw new IOException("CephInputStream.seek: failed seeking to position " + targetPos +
														" on fd " + fileHandle + ": Cannot seek after EOF " + fileLength);
    }
    ceph_seek_from_start(fileHandle, targetPos);
  }

  /**
   * Failovers are handled by the Ceph code at a very low level;
   * if there are issues that can be solved by changing sources
   * they'll be dealt with before anybody even tries to call this method!
   * @return false.
   */
  public synchronized boolean seekToNewSource(long targetPos) {
    return false;
  }
    
    
  /**
   * Read a byte from the file.
   * @return the next byte.
   */
  @Override
	public synchronized int read() throws IOException {
      if(debug) debug("CephInputStream.read: Reading a single byte from fd " + fileHandle
											+ " by calling general read function");

      byte result[] = new byte[1];
      if (getPos() >= fileLength) return -1;
      if (-1 == read(result, 0, 1)) return -1;
      if (result[0]<0) return 256+(int)result[0];
      else return result[0];
    }

  /**
   * Read a specified number of bytes into a byte[] from the file.
   * @param buf the byte array to read into.
   * @param off the offset to start at in the file
   * @param len the number of bytes to read
   * @return 0 if successful, otherwise an error code.
   */
  @Override
	public synchronized int read(byte buf[], int off, int len) throws IOException {
      if(debug) debug("CephInputStream.read: Reading " + len  + " bytes from fd " + fileHandle);
      
      if (closed) {
				throw new IOException("CephInputStream.read: cannot read " + len  + 
															" bytes from fd " + fileHandle + ": stream closed");
      }
      if (null == buf) {
				throw new NullPointerException("Read buffer is null");
      }
      
      // check for proper index bounds
      if((off < 0) || (len < 0) || (off + len > buf.length)) {
				throw new IndexOutOfBoundsException("CephInputStream.read: Indices out of bounds for read: "
																						+ "read length is " + len + ", buffer offset is " 
																						+ off +", and buffer size is " + buf.length);
      }
      
      // ensure we're not past the end of the file
      if (getPos() >= fileLength) 
				{
					if(debug) debug("CephInputStream.read: cannot read " + len  + 
													" bytes from fd " + fileHandle + ": current position is " +
													getPos() + " and file length is " + fileLength);
	  
					return -1;
				}
      // actually do the read
      int result = ceph_read(fileHandle, buf, off, len);
      if (result < 0)
				if(debug) debug("CephInputStream.read: Reading " + len
												+ " bytes from fd " + fileHandle + " failed.");

      if(debug) debug("CephInputStream.read: Reading " + len  + " bytes from fd " 
											+ fileHandle + ": succeeded in reading " + result + " bytes");   
      return result;
		}

  /**
   * Close the CephInputStream and release the associated filehandle.
   */
  @Override
	public void close() throws IOException {
    if(debug) debug("CephOutputStream.close:enter");
    if (closed) {
      throw new IOException("Stream closed");
    }

    int result = ceph_close(fileHandle);
    if (result != 0) {
      throw new IOException("Close failed!");
    }
    closed = true;
    if(debug) debug("CephOutputStream.close:exit");
  }

  private void debug(String out) {
    System.err.println(out);
  }
}
