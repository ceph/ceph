package org.apache.hadoop.fs.ceph;

/**
 * Thrown if something goes wrong with Ceph.
 */
public class CephException extends RuntimeException {

  public CephException(Throwable t) {
    super(t);
  }

}
