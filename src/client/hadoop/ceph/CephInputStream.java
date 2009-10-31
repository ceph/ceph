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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;


/**
 * <p>
 * An {@link FSInputStream} for a CephFileSystem and corresponding
 * Ceph instance.
 */
public class CephInputStream extends FSInputStream {

  private boolean closed;

  private int fileHandle;

  private long fileLength;

	private CephFS ceph;

	private byte[] buffer;
	private int bufPos = 0;
	private int bufValid = 0;
	private long cephPos = 0;

  /**
   * Create a new CephInputStream.
   * @param conf The system configuration. Unused.
   * @param fh The filehandle provided by Ceph to reference.
   * @param flength The current length of the file. If the length changes
   * you will need to close and re-open it to access the new data.
   */
  public CephInputStream(Configuration conf, CephFS cephfs,
												 int fh, long flength, int bufferSize) {
    // Whoever's calling the constructor is responsible for doing the actual ceph_open
    // call and providing the file handle.
    fileLength = flength;
    fileHandle = fh;
    closed = false;
		ceph = cephfs;
		buffer = new byte[bufferSize];
    ceph.debug("CephInputStream constructor: initializing stream with fh "
										+ fh + " and file length " + flength, ceph.DEBUG);
      
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

	private synchronized boolean fillBuffer() throws IOException {
		bufValid = ceph.ceph_read(fileHandle, buffer, 0, buffer.length);
		bufPos = 0;
		if (bufValid < 0) {
			int err = bufValid;
			bufValid = 0;
			//attempt to reset to old position. If it fails, too bad.
			ceph.ceph_seek_from_start(fileHandle, cephPos);
			throw new IOException("Failed to fill read buffer! Error code:"
														+ err);
		}
		cephPos += bufValid;
		return (bufValid != 0);
	}

	/*
	 * Make sure this works!
	 */
  public synchronized long getPos() throws IOException {
		return cephPos - bufValid + bufPos;
  }

  /**
   * Find the number of bytes remaining in the file.
   */
  @Override
	public synchronized int available() throws IOException {
      return (int) (fileLength - getPos());
    }

  public synchronized void seek(long targetPos) throws IOException {
    ceph.debug("CephInputStream.seek: Seeking to position " + targetPos +
										" on fd " + fileHandle, ceph.TRACE);
    if (targetPos > fileLength) {
      throw new IOException("CephInputStream.seek: failed seek to position "
														+ targetPos + " on fd " + fileHandle
														+ ": Cannot seek after EOF " + fileLength);
    }
		long oldPos = cephPos;
		cephPos = ceph.ceph_seek_from_start(fileHandle, targetPos);
		bufValid = 0;
		bufPos = 0;
		if (cephPos < 0) {
			cephPos = oldPos;
			throw new IOException ("Ceph failed to seek to new position!");
		}
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
      ceph.debug("CephInputStream.read: Reading a single byte from fd " + fileHandle
											+ " by calling general read function", ceph.TRACE);

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
	public synchronized int read(byte buf[], int off, int len)
		throws IOException {
      ceph.debug("CephInputStream.read: Reading " + len  +
								 " bytes from fd " + fileHandle, ceph.TRACE);
      
      if (closed) {
				throw new IOException("CephInputStream.read: cannot read " + len  + 
															" bytes from fd " + fileHandle +
															": stream closed");
      }
			
      // ensure we're not past the end of the file
      if (getPos() >= fileLength) {
				ceph.debug("CephInputStream.read: cannot read " + len  + 
									 " bytes from fd " + fileHandle + ": current position is "
									 + getPos() + " and file length is " + fileLength,
									 ceph.WARN);
				
				return -1;
			}

			int totalRead = 0;
			int initialLen = len;
			int read;
			do {
				read = Math.min(len, bufValid - bufPos);
				try {
					System.arraycopy(buffer, bufPos, buf, off, read);
				}
				catch(IndexOutOfBoundsException ie) {
					throw new IOException("CephInputStream.read: Indices out of bounds:"
																+ "read length is " + len
																+ ", buffer offset is " + off
																+ ", and buffer size is " + buf.length);
				}
				catch (ArrayStoreException ae) {
					throw new IOException("Uh-oh, CephInputStream failed to do an array"
																+ "copy due to type mismatch...");
				}
				catch (NullPointerException ne) {
					throw new IOException("CephInputStream.read: cannot read "
																+ len + "bytes from fd:" + fileHandle
																+ ": buf is null");
				}
				bufPos += read;
				len -= read;
				off += read;
				totalRead += read;
			} while (len > 0 && fillBuffer());

      ceph.debug("CephInputStream.read: Reading " + initialLen
								 + " bytes from fd " + fileHandle
								 + ": succeeded in reading " + totalRead + " bytes",
								 ceph.TRACE);
      return totalRead;
	}

  /**
   * Close the CephInputStream and release the associated filehandle.
   */
  @Override
	public void close() throws IOException {
    ceph.debug("CephOutputStream.close:enter", ceph.TRACE);
    if (!closed) {
			int result = ceph.ceph_close(fileHandle);
			closed = true;
			if (result != 0) {
				throw new IOException("Close somehow failed!"
															+ "Don't try and use this stream again, though");
			}
			ceph.debug("CephOutputStream.close:exit", ceph.TRACE);
		}
	}
}
