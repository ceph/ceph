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

  static {
    System.loadLibrary("hadoopcephfs");
  }


  private int bufferSize;

  private long fileLength;

  //private FileSystemStore store;

  private Path path;

  private long blockSize;

  private File backupFile;

  private OutputStream backupStream;

  private Random r = new Random();

  private boolean closed;

  private int fileHandle;

  private int bytesWrittenToBlock = 0;

  private byte[] outBuf;

  //private List<Block> blocks = new ArrayList<Block>();

  //private Block nextBlock;

    

  private native long ceph_seek_from_start(int fh, long pos);
  private native long ceph_getpos(int fh);
  private native int ceph_close(int fh);
  private native int ceph_write(int fh, byte[] buffer, int buffer_offset, int length);


  /*  public CephOutputStream(Configuration conf, FileSystemStore store,
      Path path, long blockSize, Progressable progress) throws IOException {
    
      // basic pseudocode:
      // call ceph_open_for_write to open the file
      // store the file handle
      // store the client pointer
      // look up and store the block size while we're at it
      // the following code's old. kill it

      this.store = store;
      this.path = path;
      this.blockSize = blockSize;
      this.backupFile = newBackupFile();
      this.backupStream = new FileOutputStream(backupFile);
      this.bufferSize = conf.getInt("io.file.buffer.size", 4096);
      this.outBuf = new byte[bufferSize];

      }*/


  // The file handle 
  public CephOutputStream(Configuration conf,  int fh) {
    fileHandle = fh;
    //fileLength = flength;
    closed = false;
  }

  // possibly useful for the local copy, write later thing?
  // keep it around for now
  private File newBackupFile() throws IOException {
    File result = File.createTempFile("s3fs-out", "");
    result.deleteOnExit();
    return result;
  } 

    public long getPos() throws IOException {
    // change to get the position from Ceph client
    return ceph_getpos(fileHandle);
  }

  // writes a byte
  @Override
    public synchronized void write(int b) throws IOException {
      //System.out.println("CephOutputStream.write: writing a single byte to fd " + fileHandle);

      if (closed) {
	throw new IOException("CephOutputStream.write: cannot write " + 
			      "a byte to fd " + fileHandle + ": stream closed");
      }
      // Stick the byte in a buffer and write it
      byte buf[] = new byte[1];
      buf[0] = (byte) b;    
      int result = ceph_write(fileHandle, buf, 0, 1);
      if (1 != result)
	System.out.println("CephOutputStream.write: failed writing a single byte to fd "
			   + fileHandle + ": Ceph write() result = " + result);
      return;
    }

  @Override
    public synchronized void write(byte buf[], int off, int len) throws IOException {
      //System.out.println("CephOutputStream.write: writing " + len + 
      //		 " bytes to fd " + fileHandle);

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
   
  @Override
    public synchronized void flush() throws IOException {
      if (closed) {
	throw new IOException("Stream closed");
      }
      return;
    }
    
  @Override
    public synchronized void close() throws IOException {
      if (closed) {
	throw new IOException("Stream closed");
      }

      int result = ceph_close(fileHandle);
      if (result != 0) {
	throw new IOException("Close failed!");
      }
	
      closed = true;

    }
    

}


