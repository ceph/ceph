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

#include "include/atomic.h"

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/radosstriper/libradosstriper.h"
#include "include/radosstriper/libradosstriper.hpp"

#include "librados/IoCtxImpl.h"
#include "librados/AioCompletionImpl.h"
#include "common/RefCountedObj.h"

struct libradosstriper::RadosStriperImpl {

  /**
   * struct handling the data needed to pass to the call back
   * function in asynchronous operations
   */
  struct CompletionData : RefCountedObject {
    /// constructor
    CompletionData(libradosstriper::RadosStriperImpl * striper,
		   const std::string& soid,
		   const std::string& lockCookie,
		   librados::AioCompletionImpl *userCompletion = 0,
                   int n = 1);
    /// destructor
    ~CompletionData() override;
    /// complete method
    void complete(int r);
    /// striper to be used to handle the write completion
    libradosstriper::RadosStriperImpl *m_striper;
    /// striped object concerned by the write operation
    std::string m_soid;
    /// shared lock to be released at completion
    std::string m_lockCookie;
    /// completion handler
    librados::IoCtxImpl::C_aio_Complete *m_ack;
  };

  /**
   * struct handling the data needed to pass to the call back
   * function in asynchronous read operations
   */
  struct ReadCompletionData : CompletionData {
    /// bufferlist containing final result
    bufferlist* m_bl;
    /// extents that will be read
    std::vector<ObjectExtent>* m_extents;
    /// intermediate results
    std::vector<bufferlist>* m_resultbl;
    /// return code of read completion, to be remembered until unlocking happened
    int m_readRc;
    /// completion object for the unlocking of the striped object at the end of the read
    librados::AioCompletion *m_unlockCompletion;
    /// constructor
    ReadCompletionData(libradosstriper::RadosStriperImpl * striper,
		       const std::string& soid,
		       const std::string& lockCookie,
		       librados::AioCompletionImpl *userCompletion,
		       bufferlist* bl,
		       std::vector<ObjectExtent>* extents,
		       std::vector<bufferlist>* resultbl,
                       int n);
    /// destructor
    ~ReadCompletionData() override;
    /// complete method for when reading is over
    void complete_read(int r);
    /// complete method for when object is unlocked
    void complete_unlock(int r);
  };

  /**
   * struct handling the data needed to pass to the call back
   * function in asynchronous write operations
   */
  struct WriteCompletionData : CompletionData {
    /// safe completion handler
    librados::IoCtxImpl::C_aio_Complete *m_safe;
    /// return code of write completion, to be remembered until unlocking happened
    int m_writeRc;
    /// completion object for the unlocking of the striped object at the end of the write
    librados::AioCompletion *m_unlockCompletion;
    /// constructor
    WriteCompletionData(libradosstriper::RadosStriperImpl * striper,
			const std::string& soid,
			const std::string& lockCookie,
			librados::AioCompletionImpl *userCompletion,
                        int n);
    /// destructor
    ~WriteCompletionData() override;
    /// complete method for when writing is over
    void complete_write(int r);
    /// complete method for when object is unlocked
    void complete_unlock(int r);
    /// safe method
    void safe(int r);
  };

  /**
   * struct handling the data needed to pass to the call back
   * function in asynchronous read operations of a Rados File
   */
  struct RadosReadCompletionData : RefCountedObject {
    /// constructor
    RadosReadCompletionData(MultiAioCompletionImpl *multiAioCompl,
			    uint64_t expectedBytes,
			    bufferlist *bl,
			    CephContext *context,
			    int n = 1) :
      RefCountedObject(context, n),
      m_multiAioCompl(multiAioCompl), m_expectedBytes(expectedBytes), m_bl(bl) {};
    /// the multi asynch io completion object to be used
    MultiAioCompletionImpl *m_multiAioCompl;
    /// the expected number of bytes
    uint64_t m_expectedBytes;
    /// the bufferlist object where data have been written
    bufferlist *m_bl;
  };

  /**
   * struct handling (most of) the data needed to pass to the call back
   * function in asynchronous stat operations.
   * Inherited by the actual type for adding time information in different
   * versions (time_t or struct timespec)
   */
  struct BasicStatCompletionData : CompletionData {
    /// constructor
    BasicStatCompletionData(libradosstriper::RadosStriperImpl* striper,
			    const std::string& soid,
			    librados::AioCompletionImpl *userCompletion,
			    libradosstriper::MultiAioCompletionImpl *multiCompletion,
			    uint64_t *psize,
                            int n = 1) :
      CompletionData(striper, soid, "", userCompletion, n),
      m_multiCompletion(multiCompletion), m_psize(psize),
      m_statRC(0), m_getxattrRC(0) {};
    // MultiAioCompletionImpl used to handle the double aysnc
    // call in the back (stat + getxattr)
    libradosstriper::MultiAioCompletionImpl *m_multiCompletion;
    // where to store the size of first objct
    // this will be ignored but we need a place to store it when
    // async stat is called
    uint64_t m_objectSize;
    // where to store the file size
    uint64_t *m_psize;
    /// the bufferlist object used for the getxattr call
    bufferlist m_bl;
    /// return code of the stat
    int m_statRC;
    /// return code of the getxattr
    int m_getxattrRC;
  };

  /**
   * struct handling the data needed to pass to the call back
   * function in asynchronous stat operations.
   * Simple templated extension of BasicStatCompletionData.
   * The template parameter is the type of the time information
   * (used with time_t for stat and struct timespec for stat2)
   */
  template<class TimeType>
  struct StatCompletionData : BasicStatCompletionData {
    /// constructor
    StatCompletionData(libradosstriper::RadosStriperImpl* striper,
		       const std::string& soid,
		       librados::AioCompletionImpl *userCompletion,
		       libradosstriper::MultiAioCompletionImpl *multiCompletion,
		       uint64_t *psize,
		       TimeType *pmtime,
                       int n = 1) :
      BasicStatCompletionData(striper, soid, userCompletion, multiCompletion, psize, n),
      m_pmtime(pmtime) {};
    // where to store the file time
    TimeType *m_pmtime;
  };

  /**
   * struct handling the data needed to pass to the call back
   * function in asynchronous remove operations of a Rados File
   */
  struct RadosRemoveCompletionData : RefCountedObject {
    /// constructor
    RadosRemoveCompletionData(MultiAioCompletionImpl *multiAioCompl,
			      CephContext *context) :
      RefCountedObject(context, 2),
      m_multiAioCompl(multiAioCompl) {};
    /// the multi asynch io completion object to be used
    MultiAioCompletionImpl *m_multiAioCompl;
  };

  struct RemoveCompletionData : CompletionData {
    /// removal flags
    int flags;
    /**
     * constructor
     * note that the constructed object will take ownership of the lock
     */
    RemoveCompletionData(libradosstriper::RadosStriperImpl * striper,
			 const std::string& soid,
			 const std::string& lockCookie,
			 librados::AioCompletionImpl *userCompletion,
			 int flags = 0);
  };

  /**
   * struct handling the data needed to pass to the call back
   * function in asynchronous truncate operations
   */
  struct TruncateCompletionData : RefCountedObject {
    /// constructor
    TruncateCompletionData(libradosstriper::RadosStriperImpl* striper,
			   const std::string& soid,
			   uint64_t size);
    /// destructor
    ~TruncateCompletionData() override;
    /// striper to be used
    libradosstriper::RadosStriperImpl *m_striper;
    /// striped object concerned by the truncate operation
    std::string m_soid;
    /// the final size of the truncated object
    uint64_t m_size;
  };

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
			 libradosstriper::MultiAioCompletionImpl *c,
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
			  libradosstriper::MultiAioCompletionImpl *multi_completion,
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
		   libradosstriper::MultiAioCompletionImpl *c,
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

#endif
