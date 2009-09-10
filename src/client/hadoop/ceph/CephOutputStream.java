// -*- mode:Java; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
package org.apache.hadoop.fs.ceph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

class CephOutputStream extends OutputStream {

  private int bufferSize;

  private boolean closed;

  private int fileHandle;

  private static boolean debug;


  private native long ceph_seek_from_start(int fh, long pos);
  private native long ceph_getpos(int fh);
  private native int ceph_close(int fh);
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

  //Ceph likes things to be closed before it shuts down,
  //so closing the IOStream stuff voluntarily is good
  public void finalize () throws Throwable {
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
      if(debug) debug("CephOutputStream.write: writing a single byte to fd " + fileHandle);

      if (closed) {
	throw new IOException("CephOutputStream.write: cannot write " + 
			      "a byte to fd " + fileHandle + ": stream closed");
      }
      // Stick the byte in a buffer and write it
      byte buf[] = new byte[1];
      buf[0] = (byte) b;    
      int result = ceph_write(fileHandle, buf, 0, 1);
      if (1 != result)
	if(debug) debug("CephOutputStream.write: failed writing a single byte to fd "
	      + fileHandle + ": Ceph write() result = " + result);
      return;
    }

  /**
   * Write a byte buffer into the Ceph file.
   * @param buf[] the byte array to write from
   * @param off the position in the file to start writing at.
   * @param len The number of bytes to actually write.
   * @throws IOException if you have closed the CephOutputStream, or
   * the write fails.
   * @throws NullPointerException if buf is null.
   * @throws IndexOutOfBoundsException if len > buf.length.
   */
  @Override
  public synchronized void write(byte buf[], int off, int len) throws IOException {
     if(debug) debug("CephOutputStream.write: writing " + len + 
	   " bytes to fd " + fileHandle);
      // make sure stream is open
      if (closed) {
	throw new IOException("CephOutputStream.write: cannot write " + len + 
			      "bytes to fd " + fileHandle + ": stream closed");
      }

      // sanity check
      if (null == buf) {
	throw new NullPointerException("CephOutputStream.write: cannot write " + len + 
				       "bytes to fd " + fileHandle + ": write buffer is null");
      }

      // check for proper index bounds
      if((off < 0) || (len < 0) || (off + len > buf.length)) {
	throw new IndexOutOfBoundsException("CephOutputStream.write: Indices out of bounds for write: "
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
