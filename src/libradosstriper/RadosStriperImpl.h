// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Sebastien Ponce <sebastien.ponce@cern.ch>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIBRADOSSTRIPER_RADOSSTRIPERIMPL_H
#define CEPH_LIBRADOSSTRIPER_RADOSSTRIPERIMPL_H

#include <string>

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/radosstriper/libradosstriper.h"
#include "include/radosstriper/libradosstriper.hpp"
#include "MultiAioCompletionImpl.h"

#include "librados/IoCtxImpl.h"
#include "librados/AioCompletionImpl.h"
#include "common/RefCountedObj.h"

namespace libradosstriper {

using MultiAioCompletionImplPtr =
    boost::intrusive_ptr<MultiAioCompletionImpl>;

struct RadosStriperImpl {

  /**
   * exception wrapper around an error code
   */
  struct ErrorCode {
    ErrorCode(int error) : m_code(error) {};
    int m_code;
  };

  /*
   * Constructor
   * @param cluster_name name of the cluster, can be NULL
   * @param client_name has 2 meanings depending on cluster_name
   *          - if cluster_name is null : this is the client id
   *          - else : this is the full client name in format type.id
   */
  RadosStriperImpl(librados::IoCtx& ioctx, librados::IoCtxImpl *ioctx_impl);
  /// Destructor
  ~RadosStriperImpl() {};

  // configuration
  int setObjectLayoutStripeUnit(unsigned int stripe_unit);
  int setObjectLayoutStripeCount(unsigned int stripe_count);
  int setObjectLayoutObjectSize(unsigned int object_size);

  // xattrs
  int getxattr(const object_t& soid, const char *name, bufferlist& bl);
  int setxattr(const object_t& soid, const char *name, bufferlist& bl);
  int getxattrs(const object_t& soid, map<string, bufferlist>& attrset);
  int rmxattr(const object_t& soid, const char *name);

  // io
  int write(const std::string& soid, const bufferlist& bl, size_t len, uint64_t off);
  int append(const std::string& soid, const bufferlist& bl, size_t len);
  int write_full(const std::string& soid, const bufferlist& bl);
  int read(const std::string& soid, bufferlist* pbl, size_t len, uint64_t off);

  // asynchronous io
  int aio_write(const std::string& soid, librados::AioCompletionImpl *c,
		const bufferlist& bl, size_t len, uint64_t off);
  int aio_append(const std::string& soid, librados::AioCompletionImpl *c,
		 const bufferlist& bl, size_t len);
  int aio_write_full(const std::string& soid, librados::AioCompletionImpl *c,
		     const bufferlist& bl);
  int aio_read(const std::string& soid, librados::AioCompletionImpl *c,
	       bufferlist* pbl, size_t len, uint64_t off);
  int aio_read(const std::string& soid, librados::AioCompletionImpl *c,
	       char* buf, size_t len, uint64_t off);
  int aio_flush();

  // stat, deletion and truncation
  int stat(const std::string& soid, uint64_t *psize, time_t *pmtime);
  int stat2(const std::string& soid, uint64_t *psize, struct timespec *pts);
  template<class TimeType>
  struct StatFunction {
    typedef int (librados::IoCtxImpl::*Type) (const object_t& oid,
					      librados::AioCompletionImpl *c,
					      uint64_t *psize, TimeType *pmtime);
  };
  template<class TimeType>
  int aio_generic_stat(const std::string& soid, librados::AioCompletionImpl *c,
		       uint64_t *psize, TimeType *pmtime,
		       typename StatFunction<TimeType>::Type statFunction);
  int aio_stat(const std::string& soid, librados::AioCompletionImpl *c,
	       uint64_t *psize, time_t *pmtime);
  int aio_stat2(const std::string& soid, librados::AioCompletionImpl *c,
		uint64_t *psize, struct timespec *pts);
  int remove(const std::string& soid, int flags=0);
  int trunc(const std::string& soid, uint64_t size);

  // asynchronous remove. Note that the removal is not 100% parallelized :
  // the removal of the first rados object of the striped object will be
  // done via a syncrhonous call after the completion of all other removals.
  // These are done asynchrounously and in parallel
  int aio_remove(const std::string& soid, librados::AioCompletionImpl *c, int flags=0);

  // reference counting
  void get() {
    lock.Lock();
    m_refCnt ++ ;
    lock.Unlock();
  }
  void put() {
    bool deleteme = false;
    lock.Lock();
    m_refCnt --;
    if (m_refCnt == 0)
      deleteme = true;
    cond.Signal();
    lock.Unlock();
    if (deleteme)
      delete this;
  }

  // objectid manipulation
  std::string getObjectId(const object_t& soid, long long unsigned objectno);

  // opening and closing of striped objects
  void unlockObject(const std::string& soid,
		    const std::string& lockCookie);
  void aio_unlockObject(const std::string& soid,
                        const std::string& lockCookie,
                        librados::AioCompletion *c);

  // internal versions of IO method
  int write_in_open_object(const std::string& soid,
			   const ceph_file_layout& layout,
			   const std::string& lockCookie,
			   const bufferlist& bl,
			   size_t len,
			   uint64_t off);
  int aio_write_in_open_object(const std::string& soid,
			       librados::AioCompletionImpl *c,
			       const ceph_file_layout& layout,
			       const std::string& lockCookie,
			       const bufferlist& bl,
			       size_t len,
			       uint64_t off);
  int internal_aio_write(const std::string& soid,
			 MultiAioCompletionImplPtr c,
			 const bufferlist& bl,
			 size_t len,
			 uint64_t off,
			 const ceph_file_layout& layout);

  int extract_uint32_attr(std::map<std::string, bufferlist> &attrs,
			  const std::string& key,
			  ceph_le32 *value);

  int extract_sizet_attr(std::map<std::string, bufferlist> &attrs,
			 const std::string& key,
			 size_t *value);

  int internal_get_layout_and_size(const std::string& oid,
				   ceph_file_layout *layout,
				   uint64_t *size);

  int internal_aio_remove(const std::string& soid,
			  MultiAioCompletionImplPtr multi_completion,
			  int flags=0);

  /**
   * opens an existing striped object and takes a shared lock on it
   * @return 0 if everything is ok and the lock was taken. -errcode otherwise
   * In particulae, if the striped object does not exists, -ENOENT is returned
   * In case the return code in not 0, no lock is taken
   */
  int openStripedObjectForRead(const std::string& soid,
			       ceph_file_layout *layout,
			       uint64_t *size,
			       std::string *lockCookie);

  /**
   * opens an existing striped object, takes a shared lock on it
   * and sets its size to the size it will have after the write.
   * In case the striped object does not exists, it will create it by
   * calling createOrOpenStripedObject.
   * @param layout this is filled with the layout of the file
   * @param size new size of the file (together with isFileSizeAbsolute)
   * In case of success, this is filled with the size of the file before the opening
   * @param isFileSizeAbsolute if false, this means that the given size should
   * be added to the current file size (append mode)
   * @return 0 if everything is ok and the lock was taken. -errcode otherwise
   * In case the return code in not 0, no lock is taken
   */
  int openStripedObjectForWrite(const std::string& soid,
				ceph_file_layout *layout,
				uint64_t *size,
				std::string *lockCookie,
				bool isFileSizeAbsolute);
  /**
   * creates an empty striped object with the given size and opens it calling
   * openStripedObjectForWrite, which implies taking a shared lock on it
   * Also deals with the cases where the object was created in the mean time
   * @param isFileSizeAbsolute if false, this means that the given size should
   * be added to the current file size (append mode). This of course only makes
   * sense in case the striped object already exists
   * @return 0 if everything is ok and the lock was taken. -errcode otherwise
   * In case the return code in not 0, no lock is taken
   */
  int createAndOpenStripedObject(const std::string& soid,
				 ceph_file_layout *layout,
				 uint64_t size,
				 std::string *lockCookie,
				 bool isFileSizeAbsolute);

  /**
   * truncates an object synchronously. Should only be called with size < original_size
   */
  int truncate(const std::string& soid,
	       uint64_t original_size,
	       uint64_t size,
	       ceph_file_layout &layout);

  /**
   * truncates an object asynchronously. Should only be called with size < original_size
   * note that the method is not 100% asynchronous, only the removal of rados objects
   * is, the (potential) truncation of the rados object residing just at the truncation
   * point is synchronous for lack of asynchronous truncation in the rados layer
   */
  int aio_truncate(const std::string& soid,
		   MultiAioCompletionImplPtr c,
		   uint64_t original_size,
		   uint64_t size,
		   ceph_file_layout &layout);

  /**
   * grows an object (adding 0s). Should only be called with size > original_size
   */
  int grow(const std::string& soid,
	   uint64_t original_size,
	   uint64_t size,
	   ceph_file_layout &layout);

  /**
   * creates a unique identifier
   */
  static std::string getUUID();

  CephContext *cct() {
    return (CephContext*)m_radosCluster.cct();
  }

  // reference counting
  Cond  cond;
  int m_refCnt;
  Mutex lock;


  // Context
  librados::Rados m_radosCluster;
  librados::IoCtx m_ioCtx;
  librados::IoCtxImpl *m_ioCtxImpl;

  // Default layout
  ceph_file_layout m_layout;
};
}
#endif
