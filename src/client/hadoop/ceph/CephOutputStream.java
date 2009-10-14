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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * An {@link OutputStream} for a CephFileSystem and corresponding
 * Ceph instance.
 */
public class CephOutputStream extends OutputStream {

  private boolean closed;

  private int fileHandle;

  private boolean debug;

	/*
	 * Get the current position in a file (as a long) of a given filehandle.
	 * Returns: (long) current file position on success, or a
	 *  negative error code on failure.
	 */
  private native long ceph_getpos(int fh);
	/*
	 * Closes the given file. Returns 0 on success, or a negative
	 * error code otherwise.
	 */
  private native int ceph_close(int fh);
	/*
	 * Write the given buffer contents to the given filehandle.
	 * Inputs:
	 *  int fh: The filehandle to write to.
	 *  byte[] buffer: The buffer to write from
	 *  int buffer_offset: The position in the buffer to write from
	 *  int length: The number of (sequential) bytes to write.
	 * Returns: int, on success the number of bytes written, on failure
	 *  a negative error code.
	 */
  private native int ceph_write(int fh, byte[] buffer, int buffer_offset, int length);


  /**
   * Construct the CephOutputStream.
   * @param conf The FileSystem configuration.
   * @param fh The Ceph filehandle to connect to.
   */
  public CephOutputStream(Configuration conf,  int fh) {
    System.load(conf.get("fs.ceph.libDir")+"/libhadoopcephfs.so");
    System.load(conf.get("fs.ceph.libDir")+"/libceph.so");
    fileHandle = fh;
    closed = false;
    debug = ("true".equals(conf.get("fs.ceph.debug", "false")));
  }

  /**Ceph likes things to be closed before it shuts down,
   *so closing the IOStream stuff voluntarily is good
   */
  protected void finalize () throws Throwable {
    try {
      if (!closed) close();
    }
    finally { super.finalize();}
  }

  /**
   * Get the current position in the file.
   * @return The file offset in bytes.
   */
  public long getPos() throws IOException {
    return ceph_getpos(fileHandle);
  }

  /**
   * Write a byte.
   * @param b The byte to write.
   * @throws IOException If you have closed the CephOutputStream or the
   * write fails.
   */
  @Override
	public synchronized void write(int b) throws IOException {
      debug("CephOutputStream.write: writing a single byte to fd " + fileHandle);

      if (closed) {
				throw new IOException("CephOutputStream.write: cannot write " + 
															"a byte to fd " + fileHandle + ": stream closed");
      }
      // Stick the byte in a buffer and write it
      byte buf[] = new byte[1];
      buf[0] = (byte) b;    
      int result = ceph_write(fileHandle, buf, 0, 1);
      if (1 != result)
				debug("CephOutputStream.write: failed writing a single byte to fd "
												+ fileHandle + ": Ceph write() result = " + result);
      return;
    }

  /**
   * Write a byte buffer into the Ceph file.
   * @param buf the byte array to write from
   * @param off the position in the file to start writing at.
   * @param len The number of bytes to actually write.
   * @throws IOException if you have closed the CephOutputStream, or
	 * if buf is null or len > buf.length, or
	 * if the write fails due to a Ceph error.
   */
  @Override
	public synchronized void write(byte buf[], int off, int len) throws IOException {
			debug("CephOutputStream.write: writing " + len + 
											" bytes to fd " + fileHandle);
      // make sure stream is open
      if (closed) {
				throw new IOException("CephOutputStream.write: cannot write " + len + 
															"bytes to fd " + fileHandle + ": stream closed");
      }

      // sanity check
      if (null == buf) {
				throw new IOException("CephOutputStream.write: cannot write " + len + 
																			 "bytes to fd " + fileHandle + ": write buffer is null");
      }

      // check for proper index bounds
      if((off < 0) || (len < 0) || (off + len > buf.length)) {
				throw new IOException("CephOutputStream.write: Indices out of bounds for write: "
																						+ "write length is " + len + ", buffer offset is " 
																						+ off +", and buffer size is " + buf.length);
      }

      // write!
      int result = ceph_write(fileHandle, buf, off, len);
      if (result < 0) {
				throw new IOException("CephOutputStream.write: Write of " + len + 
															"bytes to fd " + fileHandle + " failed");
      }
      if (result != len) {
				throw new IOException("CephOutputStream.write: Write of " + len + 
															"bytes to fd " + fileHandle + "was incomplete:  only "
															+ result + " of " + len + " bytes were written.");
      }
      return; 
    }
   
  /**
   * Flush the written data. It doesn't actually do anything; all writes are synchronous.
   * @throws IOException if you've closed the stream.
   */
  @Override
	public synchronized void flush() throws IOException {
			if (closed) {
				throw new IOException("Stream closed");
			}
			return;
		}
  
  /**
   * Close the CephOutputStream.
   * @throws IOException if Ceph somehow returns an error. In current code it can't.
   */
  @Override
	public synchronized void close() throws IOException {
      debug("CephOutputStream.close:enter");
      if (closed) {
				throw new IOException("Stream closed");
      }

      int result = ceph_close(fileHandle);
      if (result != 0) {
				throw new IOException("Close failed!");
      }
	
      closed = true;
      debug("CephOutputStream.close:exit");
    }

  private void debug(String out) {
    System.err.println(out);
  }
}
