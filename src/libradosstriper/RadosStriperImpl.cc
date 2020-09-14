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

#include <boost/algorithm/string/replace.hpp>

#include "libradosstriper/RadosStriperImpl.h"

#include <errno.h>

#include <sstream>
#include <iomanip>
#include <algorithm>

#include "include/types.h"
#include "include/uuid.h"
#include "include/ceph_fs.h"
#include "common/dout.h"
#include "common/strtol.h"
#include "common/RefCountedObj.h"
#include "osdc/Striper.h"
#include "librados/AioCompletionImpl.h"
#include <cls/lock/cls_lock_client.h>

/*
 * This file contents the actual implementation of the rados striped objects interface.
 *
 * Striped objects are stored in rados in a set of regular rados objects, after their
 * content has been striped using the osdc/Striper interface.
 *
 * The external attributes of the striped object are mapped to the attributes of the
 * first underlying object. This first object has a set of extra external attributes
 * storing the layout of the striped object for future read back. These attributes are :
 *  - striper.layout.object_size : the size of rados objects used.
 *                                 Must be a multiple of striper.layout.stripe_unit
 *  - striper.layout.stripe_unit : the size of a stripe unit
 *  - striper.layout.stripe_count : the number of stripes used
 *  - striper.size : total striped object size
 *
 * In general operations on striped objects are not atomic.
 * However, a certain number of safety guards have been put to make the interface closer
 * to atomicity :
 *  - each data operation takes a shared lock on the first rados object for the
 *    whole time of the operation
 *  - the remove and trunc operations take an exclusive lock on the first rados object
 *    for the whole time of the operation
 * This makes sure that no removal/truncation of a striped object occurs while
 * data operations are happening and vice versa. It thus makes sure that the layout
 * of a striped object does not change during data operation, which is essential for
 * data consistency.
 *
 * Still the writing to a striped object is not atomic. This means in particular that
 * the size of an object may not be in sync with its content at all times.
 * As the size is always guaranteed to be updated first and in an atomic way, and as
 * sparse striped objects are supported (see below), what will typically happen is
 * that a reader that comes too soon after a write will read 0s instead of the actual
 * data.
 *
 * Note that remove handles the pieces of the striped object in reverse order,
 * so that the head object is removed last, making the completion of the deletion atomic.
 *
 * Striped objects can be sparse, typically in case data was written at the end of the
 * striped object only. In such a case, some rados objects constituing the striped object
 * may be missing. Other can be partial (only the beginning will have data)
 * When dealing with such sparse striped files, missing objects are detected and
 * considered as full of 0s. They are however not created until real data is written
 * to them.
 *
 * There are a number of missing features/improvements that could be implemented.
 * Here are some ideas :
 *    - implementation of missing entry points (compared to rados)
 *      In particular : clone_range, sparse_read, exec, aio_flush_async, tmaps, omaps, ...
 *
 */

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "libradosstriper: "

/// size of xattr buffer
#define XATTR_BUFFER_SIZE 32

/// names of the different xattr entries
#define XATTR_LAYOUT_STRIPE_UNIT "striper.layout.stripe_unit"
#define XATTR_LAYOUT_STRIPE_COUNT "striper.layout.stripe_count"
#define XATTR_LAYOUT_OBJECT_SIZE "striper.layout.object_size"
#define XATTR_SIZE "striper.size"
#define LOCK_PREFIX "lock."

/// name of the lock used on objects to ensure layout stability during IO
#define RADOS_LOCK_NAME "striper.lock"

/// format of the extension of rados objects created for a given striped object
#define RADOS_OBJECT_EXTENSION_FORMAT ".%016llx"

/// default object layout
struct ceph_file_layout default_file_layout = {
  init_le32(1<<22),	// fl_stripe_unit
  init_le32(1),		// fl_stripe_count
  init_le32(1<<22),	// fl_object_size
  init_le32(0),		// fl_cas_hash
  init_le32(0),		// fl_object_stripe_unit
  init_le32(-1),	// fl_unused
  init_le32(-1),	// fl_pg_pool
};

using libradosstriper::MultiAioCompletionImplPtr;

namespace {

///////////////////////// CompletionData /////////////////////////////

/**
 * struct handling the data needed to pass to the call back
 * function in asynchronous operations
 */
struct CompletionData : RefCountedObject {
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
protected:
  CompletionData(libradosstriper::RadosStriperImpl * striper,
		 const std::string& soid,
		 const std::string& lockCookie,
		 librados::AioCompletionImpl *userCompletion = 0);
  ~CompletionData() override;

};

CompletionData::CompletionData
(libradosstriper::RadosStriperImpl* striper,
 const std::string& soid,
 const std::string& lockCookie,
 librados::AioCompletionImpl *userCompletion) :
  RefCountedObject(striper->cct()),
  m_striper(striper), m_soid(soid), m_lockCookie(lockCookie), m_ack(0) {
  m_striper->get();
  if (userCompletion) {
    m_ack = new librados::IoCtxImpl::C_aio_Complete(userCompletion);
    userCompletion->io = striper->m_ioCtxImpl;
  }
}

CompletionData::~CompletionData() {
  if (m_ack) delete m_ack;
  m_striper->put();
}

void CompletionData::complete(int r) {
  if (m_ack) m_ack->finish(r);
}

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
  /// complete method for when reading is over
  void complete_read(int r);
  /// complete method for when object is unlocked
  void complete_unlock(int r);

private:
  FRIEND_MAKE_REF(ReadCompletionData);
  ReadCompletionData(libradosstriper::RadosStriperImpl * striper,
		     const std::string& soid,
		     const std::string& lockCookie,
		     librados::AioCompletionImpl *userCompletion,
		     bufferlist* bl,
		     std::vector<ObjectExtent>* extents,
		     std::vector<bufferlist>* resultbl);
  ~ReadCompletionData() override;
};

ReadCompletionData::ReadCompletionData
(libradosstriper::RadosStriperImpl* striper,
 const std::string& soid,
 const std::string& lockCookie,
 librados::AioCompletionImpl *userCompletion,
 bufferlist* bl,
 std::vector<ObjectExtent>* extents,
 std::vector<bufferlist>* resultbl) :
  CompletionData(striper, soid, lockCookie, userCompletion),
  m_bl(bl), m_extents(extents), m_resultbl(resultbl), m_readRc(0),
  m_unlockCompletion(0) {}

ReadCompletionData::~ReadCompletionData() {
  m_unlockCompletion->release();
  delete m_extents;
  delete m_resultbl;
}

void ReadCompletionData::complete_read(int r) {
  // gather data into final buffer
  Striper::StripedReadResult readResult;
  vector<bufferlist>::iterator bit = m_resultbl->begin();
  for (vector<ObjectExtent>::iterator eit = m_extents->begin();
       eit != m_extents->end();
       ++eit, ++bit) {
    readResult.add_partial_result(m_striper->cct(), *bit, eit->buffer_extents);
  }
  m_bl->clear();
  readResult.assemble_result(m_striper->cct(), *m_bl, true);
  // Remember return code
  m_readRc = r;
}

void ReadCompletionData::complete_unlock(int r) {
  // call parent's completion method
  // Note that we ignore the return code of the unlock as we cannot do much about it
  CompletionData::complete(m_readRc?m_readRc:m_bl->length());
}

/**
 * struct handling the data needed to pass to the call back
 * function in asynchronous write operations
 */
struct WriteCompletionData : CompletionData {
  /// safe completion handler
  librados::IoCtxImpl::C_aio_Complete *m_safe;
  /// completion object for the unlocking of the striped object at the end of the write
  librados::AioCompletion *m_unlockCompletion;
  /// return code of write completion, to be remembered until unlocking happened
  int m_writeRc;
  /// complete method for when writing is over
  void complete_write(int r);
  /// complete method for when object is unlocked
  void complete_unlock(int r);
  /// safe method
  void safe(int r);
private:
  FRIEND_MAKE_REF(WriteCompletionData);
  /// constructor
  WriteCompletionData(libradosstriper::RadosStriperImpl * striper,
		      const std::string& soid,
		      const std::string& lockCookie,
		      librados::AioCompletionImpl *userCompletion);
  /// destructor
  ~WriteCompletionData() override;
};

WriteCompletionData::WriteCompletionData
(libradosstriper::RadosStriperImpl* striper,
 const std::string& soid,
 const std::string& lockCookie,
 librados::AioCompletionImpl *userCompletion) :
  CompletionData(striper, soid, lockCookie, userCompletion),
  m_safe(0), m_unlockCompletion(0), m_writeRc(0) {
  if (userCompletion) {
    m_safe = new librados::IoCtxImpl::C_aio_Complete(userCompletion);
  }
}

WriteCompletionData::~WriteCompletionData() {
  m_unlockCompletion->release();
  if (m_safe) delete m_safe;
}

void WriteCompletionData::complete_unlock(int r) {
  // call parent's completion method
  // Note that we ignore the return code of the unlock as we cannot do much about it
  CompletionData::complete(m_writeRc);
}

void WriteCompletionData::complete_write(int r) {
  // Remember return code
  m_writeRc = r;
}

void WriteCompletionData::safe(int r) {
  if (m_safe) m_safe->finish(r);
}

struct RemoveCompletionData : CompletionData {
  /// removal flags
  int flags;

private:
  FRIEND_MAKE_REF(RemoveCompletionData);
  /**
   * constructor
   * note that the constructed object will take ownership of the lock
   */
  RemoveCompletionData(libradosstriper::RadosStriperImpl * striper,
		       const std::string& soid,
		       const std::string& lockCookie,
		       librados::AioCompletionImpl *userCompletion,
		       int flags = 0) :
  CompletionData(striper, soid, lockCookie, userCompletion), flags(flags) {}
};

/**
 * struct handling the data needed to pass to the call back
 * function in asynchronous truncate operations
 */
struct TruncateCompletionData : RefCountedObject {
  /// striper to be used
  libradosstriper::RadosStriperImpl *m_striper;
  /// striped object concerned by the truncate operation
  std::string m_soid;
  /// the final size of the truncated object
  uint64_t m_size;

private:
  FRIEND_MAKE_REF(TruncateCompletionData);
  /// constructor
  TruncateCompletionData(libradosstriper::RadosStriperImpl* striper,
			 const std::string& soid,
			 uint64_t size) :
    RefCountedObject(striper->cct()),
    m_striper(striper), m_soid(soid), m_size(size) {
    m_striper->get();
  }
  /// destructor
  ~TruncateCompletionData() override {
    m_striper->put();
  }
};

/**
 * struct handling the data needed to pass to the call back
 * function in asynchronous read operations of a Rados File
 */
struct RadosReadCompletionData : RefCountedObject {
  /// the multi asynch io completion object to be used
  MultiAioCompletionImplPtr m_multiAioCompl;
  /// the expected number of bytes
  uint64_t m_expectedBytes;
  /// the bufferlist object where data have been written
  bufferlist *m_bl;

private:
  FRIEND_MAKE_REF(RadosReadCompletionData);
  /// constructor
  RadosReadCompletionData(MultiAioCompletionImplPtr multiAioCompl,
			  uint64_t expectedBytes,
			  bufferlist *bl,
			  CephContext *context) :
    RefCountedObject(context),
    m_multiAioCompl(multiAioCompl), m_expectedBytes(expectedBytes), m_bl(bl) {}
};

/**
 * struct handling (most of) the data needed to pass to the call back
 * function in asynchronous stat operations.
 * Inherited by the actual type for adding time information in different
 * versions (time_t or struct timespec)
 */
struct BasicStatCompletionData : CompletionData {
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

protected:
  /// constructor
  BasicStatCompletionData(libradosstriper::RadosStriperImpl* striper,
			  const std::string& soid,
			  librados::AioCompletionImpl *userCompletion,
			  libradosstriper::MultiAioCompletionImpl *multiCompletion,
			  uint64_t *psize) :
    CompletionData(striper, soid, "", userCompletion),
    m_multiCompletion(multiCompletion), m_psize(psize),
    m_statRC(0), m_getxattrRC(0) {};

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
  // where to store the file time
  TimeType *m_pmtime;
private:
  FRIEND_MAKE_REF(StatCompletionData);
  /// constructor
  StatCompletionData<TimeType>(libradosstriper::RadosStriperImpl* striper,
		     const std::string& soid,
		     librados::AioCompletionImpl *userCompletion,
		     libradosstriper::MultiAioCompletionImpl *multiCompletion,
		     uint64_t *psize,
		     TimeType *pmtime) :
    BasicStatCompletionData(striper, soid, userCompletion, multiCompletion, psize),
    m_pmtime(pmtime) {};
};

/**
 * struct handling the data needed to pass to the call back
 * function in asynchronous remove operations of a Rados File
 */
struct RadosRemoveCompletionData : RefCountedObject {
  /// the multi asynch io completion object to be used
  MultiAioCompletionImplPtr m_multiAioCompl;
private:
  FRIEND_MAKE_REF(RadosRemoveCompletionData);
  /// constructor
  RadosRemoveCompletionData(MultiAioCompletionImplPtr multiAioCompl,
			    CephContext *context) :
    RefCountedObject(context),
    m_multiAioCompl(multiAioCompl) {};
};


} // namespace {

///////////////////////// constructor /////////////////////////////

libradosstriper::RadosStriperImpl::RadosStriperImpl(librados::IoCtx& ioctx, librados::IoCtxImpl *ioctx_impl) :
  m_refCnt(0), m_radosCluster(ioctx), m_ioCtx(ioctx), m_ioCtxImpl(ioctx_impl),
  m_layout(default_file_layout) {}

///////////////////////// layout /////////////////////////////

int libradosstriper::RadosStriperImpl::setObjectLayoutStripeUnit
(unsigned int stripe_unit)
{
  /* stripe unit must be non-zero, 64k increment */
  if (!stripe_unit || (stripe_unit & (CEPH_MIN_STRIPE_UNIT-1)))
    return -EINVAL;
  m_layout.fl_stripe_unit = stripe_unit;
  return 0;
}

int libradosstriper::RadosStriperImpl::setObjectLayoutStripeCount
(unsigned int stripe_count)
{
  /* stripe count must be non-zero */
  if (!stripe_count)
    return -EINVAL;
  m_layout.fl_stripe_count = stripe_count;
  return 0;
}

int libradosstriper::RadosStriperImpl::setObjectLayoutObjectSize
(unsigned int object_size)
{
  /* object size must be non-zero, 64k increment */
  if (!object_size || (object_size & (CEPH_MIN_STRIPE_UNIT-1)))
    return -EINVAL;
  /* object size must be a multiple of stripe unit */
  if (object_size < m_layout.fl_stripe_unit ||
      object_size % m_layout.fl_stripe_unit)
    return -EINVAL;
  m_layout.fl_object_size = object_size;
  return 0;
}

///////////////////////// xattrs /////////////////////////////

int libradosstriper::RadosStriperImpl::getxattr(const object_t& soid,
                                                const char *name,
                                                bufferlist& bl)
{
  std::string firstObjOid = getObjectId(soid, 0);
  return m_ioCtx.getxattr(firstObjOid, name, bl);
}

int libradosstriper::RadosStriperImpl::setxattr(const object_t& soid,
                                                const char *name,
                                                bufferlist& bl)
{
  std::string firstObjOid = getObjectId(soid, 0);
  return m_ioCtx.setxattr(firstObjOid, name, bl);
}

int libradosstriper::RadosStriperImpl::getxattrs(const object_t& soid,
                                                 map<string, bufferlist>& attrset)
{
  std::string firstObjOid = getObjectId(soid, 0);
  int rc = m_ioCtx.getxattrs(firstObjOid, attrset);
  if (rc) return rc;
  // cleanup internal attributes dedicated to striping and locking
  attrset.erase(XATTR_LAYOUT_STRIPE_UNIT);
  attrset.erase(XATTR_LAYOUT_STRIPE_COUNT);
  attrset.erase(XATTR_LAYOUT_OBJECT_SIZE);
  attrset.erase(XATTR_SIZE);
  attrset.erase(std::string(LOCK_PREFIX) + RADOS_LOCK_NAME);
  return rc;
}

int libradosstriper::RadosStriperImpl::rmxattr(const object_t& soid,
                                               const char *name)
{
  std::string firstObjOid = getObjectId(soid, 0);
  return m_ioCtx.rmxattr(firstObjOid, name);
}

///////////////////////// io /////////////////////////////

int libradosstriper::RadosStriperImpl::write(const std::string& soid,
					     const bufferlist& bl,
					     size_t len,
					     uint64_t off) 
{
  // open the object. This will create it if needed, retrieve its layout
  // and size and take a shared lock on it
  ceph_file_layout layout;
  std::string lockCookie;
  int rc = createAndOpenStripedObject(soid, &layout, len+off, &lockCookie, true);
  if (rc) return rc;
  return write_in_open_object(soid, layout, lockCookie, bl, len, off);
}

int libradosstriper::RadosStriperImpl::append(const std::string& soid,
					      const bufferlist& bl,
					      size_t len) 
{
  // open the object. This will create it if needed, retrieve its layout
  // and size and take a shared lock on it
  ceph_file_layout layout;
  uint64_t size = len;
  std::string lockCookie;
  int rc = openStripedObjectForWrite(soid, &layout, &size, &lockCookie, false);
  if (rc) return rc;
  return write_in_open_object(soid, layout, lockCookie, bl, len, size);
}

int libradosstriper::RadosStriperImpl::write_full(const std::string& soid,
						  const bufferlist& bl) 
{
  int rc = trunc(soid, 0);
  if (rc && rc != -ENOENT) return rc; // ENOENT is obviously ok
  return write(soid, bl, bl.length(), 0);
}

int libradosstriper::RadosStriperImpl::read(const std::string& soid,
					    bufferlist* bl,
					    size_t len,
					    uint64_t off)
{
  // create a completion object
  librados::AioCompletionImpl c;
  // call asynchronous method
  int rc = aio_read(soid, &c, bl, len, off);
  // and wait for completion
  if (!rc) {
    // wait for completion
    c.wait_for_complete_and_cb();
    // return result
    rc = c.get_return_value();
  }
  return rc;
}

///////////////////////// asynchronous io /////////////////////////////

int libradosstriper::RadosStriperImpl::aio_write(const std::string& soid,
						 librados::AioCompletionImpl *c,
						 const bufferlist& bl,
						 size_t len,
						 uint64_t off)
{
  ceph_file_layout layout;
  std::string lockCookie;
  int rc = createAndOpenStripedObject(soid, &layout, len+off, &lockCookie, true);
  if (rc) return rc;
  return aio_write_in_open_object(soid, c, layout, lockCookie, bl, len, off);
}

int libradosstriper::RadosStriperImpl::aio_append(const std::string& soid,
						  librados::AioCompletionImpl *c,
						  const bufferlist& bl,
						  size_t len)
{
  ceph_file_layout layout;
  uint64_t size = len;
  std::string lockCookie;
  int rc = openStripedObjectForWrite(soid, &layout, &size, &lockCookie, false);
  if (rc) return rc;
  // create a completion object
  return aio_write_in_open_object(soid, c, layout, lockCookie, bl, len, size);
}

int libradosstriper::RadosStriperImpl::aio_write_full(const std::string& soid,
						      librados::AioCompletionImpl *c,
						      const bufferlist& bl)
{
  int rc = trunc(soid, 0);
  if (rc) return rc;
  return aio_write(soid, c, bl, bl.length(), 0);
}

static void rados_read_aio_unlock_complete(rados_striper_multi_completion_t c, void *arg)
{
  auto cdata = ceph::ref_t<ReadCompletionData>(static_cast<ReadCompletionData*>(arg), false);
  libradosstriper::MultiAioCompletionImpl *comp =
    reinterpret_cast<libradosstriper::MultiAioCompletionImpl*>(c);
  cdata->complete_unlock(comp->rval);
}

static void striper_read_aio_req_complete(rados_striper_multi_completion_t c, void *arg)
{
  auto cdata = static_cast<ReadCompletionData*>(arg);
  // launch the async unlocking of the object
  cdata->m_striper->aio_unlockObject(cdata->m_soid, cdata->m_lockCookie, cdata->m_unlockCompletion);
  // complete the read part in parallel
  libradosstriper::MultiAioCompletionImpl *comp =
    reinterpret_cast<libradosstriper::MultiAioCompletionImpl*>(c);
  cdata->complete_read(comp->rval);
}

static void rados_req_read_complete(rados_completion_t c, void *arg)
{
  auto data = static_cast<RadosReadCompletionData*>(arg);
  int rc = rados_aio_get_return_value(c);
  // We need to handle the case of sparse files here
  if (rc == -ENOENT) {
    // the object did not exist at all. This can happen for sparse files.
    // we consider we've read 0 bytes and it will fall into next case
    rc = 0;
  }
  ssize_t nread = rc;
  if (rc >= 0 && (((uint64_t)rc) < data->m_expectedBytes)) {
    // only partial data were present in the object (or the object did not
    // even exist if we've gone through previous case).
    // This is typical of sparse file and we need to complete with 0s.
    unsigned int lenOfZeros = data->m_expectedBytes-rc;
    unsigned int existingDataToZero = min(data->m_bl->length()-rc, lenOfZeros);
    if (existingDataToZero > 0) {
      data->m_bl->zero(rc, existingDataToZero);
    }
    if (lenOfZeros > existingDataToZero) {
      ceph::bufferptr zeros(ceph::buffer::create(lenOfZeros-existingDataToZero));
      zeros.zero();
      data->m_bl->push_back(zeros);
    }
    nread = data->m_expectedBytes;
  }
  auto multi_aio_comp = data->m_multiAioCompl;
  multi_aio_comp->complete_request(nread);
  multi_aio_comp->safe_request(rc);
}

int libradosstriper::RadosStriperImpl::aio_read(const std::string& soid,
						librados::AioCompletionImpl *c,
						bufferlist* bl,
						size_t len,
						uint64_t off)
{
  // open the object. This will retrieve its layout and size
  // and take a shared lock on it
  ceph_file_layout layout;
  uint64_t size;
  std::string lockCookie;
  int rc = openStripedObjectForRead(soid, &layout, &size, &lockCookie);
  if (rc) return rc;
  // find out the actual number of bytes we can read
  uint64_t read_len;
  if (off >= size) {
    // nothing to read ! We are done.
    read_len = 0;
  } else {
    read_len = min(len, (size_t)(size-off));
  }
  // get list of extents to be read from
  vector<ObjectExtent> *extents = new vector<ObjectExtent>();
  if (read_len > 0) {
    std::string format = soid;
    boost::replace_all(format, "%", "%%");
    format += RADOS_OBJECT_EXTENSION_FORMAT;
    file_layout_t l;
    l.from_legacy(layout);
    Striper::file_to_extents(cct(), format.c_str(), &l, off, read_len,
			     0, *extents);
  }

  // create a completion object and transfer ownership of extents and resultbl
  vector<bufferlist> *resultbl = new vector<bufferlist>(extents->size());
  auto cdata = ceph::make_ref<ReadCompletionData>(this, soid, lockCookie, c, bl, extents, resultbl);
  c->is_read = true;
  c->io = m_ioCtxImpl;
  // create a completion for the unlocking of the striped object at the end of the read
  librados::AioCompletion *unlock_completion =
    librados::Rados::aio_create_completion(cdata->get() /* create ref! */, rados_read_aio_unlock_complete);
  cdata->m_unlockCompletion = unlock_completion;
  // create the multiCompletion object handling the reads
  MultiAioCompletionImplPtr nc{new libradosstriper::MultiAioCompletionImpl,
			       false};
  nc->set_complete_callback(cdata.get(), striper_read_aio_req_complete);
  // go through the extents
  int r = 0, i = 0;
  for (vector<ObjectExtent>::iterator p = extents->begin(); p != extents->end(); ++p) {
    // create a buffer list describing where to place data read from current extend
    bufferlist *oid_bl = &((*resultbl)[i++]);
    for (vector<pair<uint64_t,uint64_t> >::iterator q = p->buffer_extents.begin();
        q != p->buffer_extents.end();
        ++q) {
      bufferlist buffer_bl;
      buffer_bl.substr_of(*bl, q->first, q->second);
      oid_bl->append(buffer_bl);
    }
    // read all extends of a given object in one go
    nc->add_request();
    // we need 2 references on data as both rados_req_read_safe and rados_req_read_complete
    // will release one
    auto data = ceph::make_ref<RadosReadCompletionData>(nc, p->length, oid_bl, cct());
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(data.detach(), rados_req_read_complete);
    r = m_ioCtx.aio_read(p->oid.name, rados_completion, oid_bl, p->length, p->offset);
    rados_completion->release();
    if (r < 0)
      break;
  }
  nc->finish_adding_requests();
  return r;
}

int libradosstriper::RadosStriperImpl::aio_read(const std::string& soid,
						librados::AioCompletionImpl *c,
						char* buf,
						size_t len,
						uint64_t off)
{
  // create a buffer list and store it inside the completion object
  c->bl.clear();
  c->bl.push_back(buffer::create_static(len, buf));
  // call the bufferlist version of this method
  return aio_read(soid, c, &c->bl, len, off);
}

int libradosstriper::RadosStriperImpl::aio_flush() 
{
  int ret;
  // pass to the rados level
  ret = m_ioCtx.aio_flush();
  if (ret < 0)
    return ret;
  //wait all CompletionData are released
  std::unique_lock l{lock};
  cond.wait(l, [this] {return m_refCnt <= 1;});
  return ret;
}

///////////////////////// stat and deletion /////////////////////////////

int libradosstriper::RadosStriperImpl::stat(const std::string& soid, uint64_t *psize, time_t *pmtime)
{
  // create a completion object
  librados::AioCompletionImpl c;
  // call asynchronous version of stat
  int rc = aio_stat(soid, &c, psize, pmtime);
  if (rc == 0) {
    // wait for completion of the remove
    c.wait_for_complete();
    // get result
    rc = c.get_return_value();
  }
  return rc;
}

static void striper_stat_aio_stat_complete(rados_completion_t c, void *arg) {
  auto data = ceph::ref_t<BasicStatCompletionData>(static_cast<BasicStatCompletionData*>(arg), false);
  int rc = rados_aio_get_return_value(c);
  if (rc == -ENOENT) {
    // remember this has failed
    data->m_statRC = rc;
  }
  data->m_multiCompletion->complete_request(rc);
}

static void striper_stat_aio_getxattr_complete(rados_completion_t c, void *arg) {
  auto data = ceph::ref_t<BasicStatCompletionData>(static_cast<BasicStatCompletionData*>(arg), false);
  int rc = rados_aio_get_return_value(c);
  // We need to handle the case of sparse files here
  if (rc < 0) {
    // remember this has failed
    data->m_getxattrRC = rc;
  } else {
    // this intermediate string allows to add a null terminator before calling strtol
    std::string err;
    std::string strsize(data->m_bl.c_str(), data->m_bl.length());
    *data->m_psize = strict_strtoll(strsize.c_str(), 10, &err);
    if (!err.empty()) {
      lderr(data->m_striper->cct()) << XATTR_SIZE << " : " << err << dendl;
      data->m_getxattrRC = -EINVAL;
    }
    rc = 0;
  }
  data->m_multiCompletion->complete_request(rc);
}

static void striper_stat_aio_req_complete(rados_striper_multi_completion_t c,
					  void *arg) {
  auto data = ceph::ref_t<BasicStatCompletionData>(static_cast<BasicStatCompletionData*>(arg), false);
  if (data->m_statRC) {
    data->complete(data->m_statRC);
  } else {
    if (data->m_getxattrRC < 0) {
      data->complete(data->m_getxattrRC);
    } else {
      data->complete(0);
    }
  }
}

template<class TimeType>
int libradosstriper::RadosStriperImpl::aio_generic_stat
(const std::string& soid,
 librados::AioCompletionImpl *c,
 uint64_t *psize,
 TimeType *pmtime,
 typename libradosstriper::RadosStriperImpl::StatFunction<TimeType>::Type statFunction)
{
  // use a MultiAioCompletion object for dealing with the fact
  // that we'll do 2 asynchronous calls in parallel
  MultiAioCompletionImplPtr multi_completion{
    new libradosstriper::MultiAioCompletionImpl, false};
  // Data object used for passing context to asynchronous calls
  std::string firstObjOid = getObjectId(soid, 0);
  auto cdata = ceph::make_ref<StatCompletionData<TimeType>>(this, firstObjOid, c, multi_completion.get(), psize, pmtime);
  multi_completion->set_complete_callback(cdata->get() /* create ref! */, striper_stat_aio_req_complete);
  // use a regular AioCompletion for the stat async call
  librados::AioCompletion *stat_completion =
    librados::Rados::aio_create_completion(cdata->get() /* create ref! */, striper_stat_aio_stat_complete);
  multi_completion->add_safe_request();
  object_t obj(firstObjOid);
  int rc = (m_ioCtxImpl->*statFunction)(obj, stat_completion->pc,
					&cdata->m_objectSize, cdata->m_pmtime);
  stat_completion->release();
  if (rc < 0) {
    // nothing is really started so cancel everything
    delete cdata.detach();
    return rc;
  }
  // use a regular AioCompletion for the getxattr async call
  librados::AioCompletion *getxattr_completion =
    librados::Rados::aio_create_completion(cdata->get() /* create ref! */, striper_stat_aio_getxattr_complete);
  multi_completion->add_safe_request();
  // in parallel, get the pmsize from the first object asynchronously
  rc = m_ioCtxImpl->aio_getxattr(obj, getxattr_completion->pc,
				 XATTR_SIZE, cdata->m_bl);
  getxattr_completion->release();
  multi_completion->finish_adding_requests();
  if (rc < 0) {
    // the async stat is ongoing, so we need to go on
    // we mark the getxattr as failed in the data object
    cdata->m_getxattrRC = rc;
    multi_completion->complete_request(rc);
    return rc;
  }
  return 0;
}

int libradosstriper::RadosStriperImpl::aio_stat(const std::string& soid,
						librados::AioCompletionImpl *c,
						uint64_t *psize,
						time_t *pmtime)
{
  return aio_generic_stat<time_t>(soid, c, psize, pmtime, &librados::IoCtxImpl::aio_stat);
}

int libradosstriper::RadosStriperImpl::stat2(const std::string& soid, uint64_t *psize, struct timespec *pts)
{
  // create a completion object
  librados::AioCompletionImpl c;
  // call asynchronous version of stat
  int rc = aio_stat2(soid, &c, psize, pts);
  if (rc == 0) {
    // wait for completion of the remove
    c.wait_for_complete_and_cb();
    // get result
    rc = c.get_return_value();
  }
  return rc;
}

int libradosstriper::RadosStriperImpl::aio_stat2(const std::string& soid,
						librados::AioCompletionImpl *c,
						uint64_t *psize,
						struct timespec *pts)
{
  return aio_generic_stat<struct timespec>(soid, c, psize, pts, &librados::IoCtxImpl::aio_stat2);
}

static void rados_req_remove_complete(rados_completion_t c, void *arg)
{
  auto cdata = static_cast<RadosRemoveCompletionData*>(arg);
  int rc = rados_aio_get_return_value(c);
  // in case the object did not exist, it means we had a sparse file, all is fine
  if (rc == -ENOENT) {
    rc = 0;
  }
  cdata->m_multiAioCompl->complete_request(rc);
  cdata->m_multiAioCompl->safe_request(rc);
}

static void striper_remove_aio_req_complete(rados_striper_multi_completion_t c, void *arg)
{
  auto cdata = ceph::ref_t<RemoveCompletionData>(static_cast<RemoveCompletionData*>(arg), false);
  libradosstriper::MultiAioCompletionImpl *comp =
    reinterpret_cast<libradosstriper::MultiAioCompletionImpl*>(c);
  ldout(cdata->m_striper->cct(), 10)
    << "RadosStriperImpl : striper_remove_aio_req_complete called for "
    << cdata->m_soid << dendl;
  int rc = comp->rval;
  if (rc == 0) {
    // All went fine, synchronously remove first object
    rc = cdata->m_striper->m_ioCtx.remove(cdata->m_striper->getObjectId(cdata->m_soid, 0),
					  cdata->flags);
  } else {
    lderr(cdata->m_striper->cct())
      << "RadosStriperImpl : deletion/truncation incomplete for " << cdata->m_soid
      << ", as errors were encountered. The file is left present but it's content "
      << " has been partially removed"
      << dendl;
  }
  cdata->complete(rc);
}

int libradosstriper::RadosStriperImpl::remove(const std::string& soid, int flags)
{
  // create a completion object
  librados::AioCompletionImpl c;
  // call asynchronous version of remove
  int rc = aio_remove(soid, &c, flags);
  if (rc == 0) {
    // wait for completion of the remove
    c.wait_for_complete_and_cb();
    // get result
    rc = c.get_return_value();
  }
  return rc;
}

int libradosstriper::RadosStriperImpl::aio_remove(const std::string& soid,
						  librados::AioCompletionImpl *c,
						  int flags)
{
  // the RemoveCompletionData object will lock the given soid for the duration
  // of the removal
  std::string lockCookie = getUUID();
  int rc = m_ioCtx.lock_exclusive(getObjectId(soid, 0), RADOS_LOCK_NAME, lockCookie, "", 0, 0);
  if (rc) return rc;
  // create CompletionData for the async remove call
  auto cdata = ceph::make_ref<RemoveCompletionData>(this, soid, lockCookie, c, flags);
  MultiAioCompletionImplPtr multi_completion{
    new libradosstriper::MultiAioCompletionImpl, false};
  multi_completion->set_complete_callback(cdata->get() /* create ref! */, striper_remove_aio_req_complete);
  // call asynchronous internal version of remove
  ldout(cct(), 10)
    << "RadosStriperImpl : Aio_remove starting for "
    << soid << dendl;
  rc = internal_aio_remove(soid, multi_completion);
  return rc;
}

int libradosstriper::RadosStriperImpl::internal_aio_remove(
 const std::string& soid,
 MultiAioCompletionImplPtr multi_completion,
 int flags)
{
  std::string firstObjOid = getObjectId(soid, 0);
  try {
    // check size and get number of rados objects to delete
    uint64_t nb_objects = 0;
    bufferlist bl2;
    int rc = getxattr(soid, XATTR_SIZE, bl2);
    if (rc < 0) {
      // no object size (or not able to get it)
      // try to find the number of object "by hand"
      uint64_t psize;
      time_t pmtime;
      while (!m_ioCtx.stat(getObjectId(soid, nb_objects), &psize, &pmtime)) {
        nb_objects++;
      }
    } else {
      // count total number of rados objects in the striped object
      std::string err;
      // this intermediate string allows to add a null terminator before calling strtol
      std::string strsize(bl2.c_str(), bl2.length());
      uint64_t size = strict_strtoll(strsize.c_str(), 10, &err);
      if (!err.empty()) {
        lderr(cct()) << XATTR_SIZE << " : " << err << dendl;
        
        return -EINVAL;
      }
      uint64_t object_size = m_layout.fl_object_size;
      uint64_t su = m_layout.fl_stripe_unit;
      uint64_t stripe_count = m_layout.fl_stripe_count;
      uint64_t nb_complete_sets = size / (object_size*stripe_count);
      uint64_t remaining_data = size % (object_size*stripe_count);
      uint64_t remaining_stripe_units = (remaining_data + su -1) / su;
      uint64_t remaining_objects = std::min(remaining_stripe_units, stripe_count);
      nb_objects = nb_complete_sets * stripe_count + remaining_objects;
    }
    // delete rados objects in reverse order
    // Note that we do not drop the first object. This one will only be dropped
    // if all other removals have been successful, and this is done in the
    // callback of the multi_completion object
    int rcr = 0;
    for (int i = nb_objects-1; i >= 1; i--) {
      multi_completion->add_request();
      auto data = ceph::make_ref<RadosRemoveCompletionData>(multi_completion, cct());
      librados::AioCompletion *rados_completion =
	librados::Rados::aio_create_completion(data->get() /* create ref! */,
					       rados_req_remove_complete);
      if (flags == 0) {
        rcr = m_ioCtx.aio_remove(getObjectId(soid, i), rados_completion);
      } else {
        rcr = m_ioCtx.aio_remove(getObjectId(soid, i), rados_completion, flags);
      }
      rados_completion->release();
      if (rcr < 0 and -ENOENT != rcr) {
        lderr(cct()) << "RadosStriperImpl::remove : deletion incomplete for " << soid
		     << ", as " << getObjectId(soid, i) << " could not be deleted (rc=" << rc << ")"
		     << dendl;
        break;
      }
    }
    // we are over adding requests to the multi_completion object
    multi_completion->finish_adding_requests();
    // return
    return rcr;
  } catch (ErrorCode &e) {
    // error caught when trying to take the exclusive lock
    return e.m_code;
  }

}

int libradosstriper::RadosStriperImpl::trunc(const std::string& soid, uint64_t size)
{
  // lock the object in exclusive mode
  std::string firstObjOid = getObjectId(soid, 0);
  librados::ObjectWriteOperation op;
  op.assert_exists();
  std::string lockCookie = RadosStriperImpl::getUUID();
  utime_t dur = utime_t();
  rados::cls::lock::lock(&op, RADOS_LOCK_NAME, ClsLockType::EXCLUSIVE, lockCookie, "", "", dur, 0);
  int rc = m_ioCtx.operate(firstObjOid, &op);
  if (rc) return rc;
  // load layout and size
  ceph_file_layout layout;
  uint64_t original_size;
  rc = internal_get_layout_and_size(firstObjOid, &layout, &original_size);
  if (!rc) {
    if (size < original_size) {
      rc = truncate(soid, original_size, size, layout);
    } else if (size > original_size) {
      rc = grow(soid, original_size, size, layout);
    }
  }
  // unlock object, ignore return code as we cannot do much
  m_ioCtx.unlock(firstObjOid, RADOS_LOCK_NAME, lockCookie);
  // final return
  return rc;
}


///////////////////////// private helpers /////////////////////////////

std::string libradosstriper::RadosStriperImpl::getObjectId(const object_t& soid,
                                                           long long unsigned objectno)
{
  std::ostringstream s;
  s << soid << '.' << std::setfill ('0') << std::setw(16) << std::hex << objectno;
  return s.str();
}

void libradosstriper::RadosStriperImpl::unlockObject(const std::string& soid,
						     const std::string& lockCookie)
{
  // unlock the shared lock on the first rados object
  std::string firstObjOid = getObjectId(soid, 0);
  m_ioCtx.unlock(firstObjOid, RADOS_LOCK_NAME, lockCookie);
}

void libradosstriper::RadosStriperImpl::aio_unlockObject(const std::string& soid,
						         const std::string& lockCookie,
                                                         librados::AioCompletion *c)
{
  // unlock the shared lock on the first rados object
  std::string firstObjOid = getObjectId(soid, 0);
  m_ioCtx.aio_unlock(firstObjOid, RADOS_LOCK_NAME, lockCookie, c);
}

static void rados_write_aio_unlock_complete(rados_striper_multi_completion_t c, void *arg)
{
  auto cdata = ceph::ref_t<WriteCompletionData>(static_cast<WriteCompletionData*>(arg), false);
  libradosstriper::MultiAioCompletionImpl *comp =
    reinterpret_cast<libradosstriper::MultiAioCompletionImpl*>(c);
  cdata->complete_unlock(comp->rval);
}

static void striper_write_aio_req_complete(rados_striper_multi_completion_t c, void *arg)
{
  auto cdata = ceph::ref_t<WriteCompletionData>(static_cast<WriteCompletionData*>(arg), false);
  // launch the async unlocking of the object
  cdata->m_striper->aio_unlockObject(cdata->m_soid, cdata->m_lockCookie, cdata->m_unlockCompletion);
  // complete the write part in parallel
  libradosstriper::MultiAioCompletionImpl *comp =
    reinterpret_cast<libradosstriper::MultiAioCompletionImpl*>(c);
  cdata->complete_write(comp->rval);
}

static void striper_write_aio_req_safe(rados_striper_multi_completion_t c, void *arg)
{
  auto cdata = ceph::ref_t<WriteCompletionData>(static_cast<WriteCompletionData*>(arg), false);
  libradosstriper::MultiAioCompletionImpl *comp =
    reinterpret_cast<libradosstriper::MultiAioCompletionImpl*>(c);
  cdata->safe(comp->rval);
}

int libradosstriper::RadosStriperImpl::write_in_open_object(const std::string& soid,
							    const ceph_file_layout& layout,
							    const std::string& lockCookie,
							    const bufferlist& bl,
							    size_t len,
							    uint64_t off) {
  // create a completion object to be passed to the callbacks of the multicompletion
  // we need 3 references as striper_write_aio_req_complete will release two and
  // striper_write_aio_req_safe will release one
  auto cdata = ceph::make_ref<WriteCompletionData>(this, soid, lockCookie, nullptr);
  // create a completion object for the unlocking of the striped object at the end of the write
  librados::AioCompletion *unlock_completion =
    librados::Rados::aio_create_completion(cdata->get() /* create ref! */, rados_write_aio_unlock_complete);
  cdata->m_unlockCompletion = unlock_completion;
  // create the multicompletion that will handle the write completion
  MultiAioCompletionImplPtr c{new libradosstriper::MultiAioCompletionImpl,
                              false};
  c->set_complete_callback(cdata->get() /* create ref! */, striper_write_aio_req_complete);
  c->set_safe_callback(cdata->get() /* create ref! */, striper_write_aio_req_safe);
  // call the asynchronous API
  int rc = internal_aio_write(soid, c, bl, len, off, layout);
  if (!rc) {
    // wait for completion and safety of data
    c->wait_for_complete_and_cb();
    c->wait_for_safe_and_cb();
    // wait for the unlocking
    unlock_completion->wait_for_complete();
    // return result
    rc = c->get_return_value();
  }
  return rc;
}

int libradosstriper::RadosStriperImpl::aio_write_in_open_object(const std::string& soid,
								librados::AioCompletionImpl *c,
								const ceph_file_layout& layout,
								const std::string& lockCookie,
								const bufferlist& bl,
								size_t len,
								uint64_t off) {
  // create a completion object to be passed to the callbacks of the multicompletion
  // we need 3 references as striper_write_aio_req_complete will release two and
  // striper_write_aio_req_safe will release one
  auto cdata = ceph::make_ref<WriteCompletionData>(this, soid, lockCookie, c);
  m_ioCtxImpl->get();
  c->io = m_ioCtxImpl;
  // create a completion object for the unlocking of the striped object at the end of the write
  librados::AioCompletion *unlock_completion =
    librados::Rados::aio_create_completion(cdata->get() /* create ref! */, rados_write_aio_unlock_complete);
  cdata->m_unlockCompletion = unlock_completion;
  // create the multicompletion that will handle the write completion
  libradosstriper::MultiAioCompletionImplPtr nc{
    new libradosstriper::MultiAioCompletionImpl, false};
  nc->set_complete_callback(cdata->get() /* create ref! */, striper_write_aio_req_complete);
  nc->set_safe_callback(cdata->get() /* create ref! */, striper_write_aio_req_safe);
  // internal asynchronous API
  int rc = internal_aio_write(soid, nc, bl, len, off, layout);
  return rc;
}

static void rados_req_write_complete(rados_completion_t c, void *arg)
{
  auto comp = reinterpret_cast<libradosstriper::MultiAioCompletionImpl*>(arg);
  comp->complete_request(rados_aio_get_return_value(c));
  comp->safe_request(rados_aio_get_return_value(c));
}

int
libradosstriper::RadosStriperImpl::internal_aio_write(const std::string& soid,
						      libradosstriper::MultiAioCompletionImplPtr c,
						      const bufferlist& bl,
						      size_t len,
						      uint64_t off,
						      const ceph_file_layout& layout)
{
  int r = 0;
  // Do not try anything if we are called with empty buffer,
  // file_to_extents would raise an exception
  if (len > 0) {
    // get list of extents to be written to
    vector<ObjectExtent> extents;
    std::string format = soid;
    boost::replace_all(format, "%", "%%");
    format += RADOS_OBJECT_EXTENSION_FORMAT;
    file_layout_t l;
    l.from_legacy(layout);
    Striper::file_to_extents(cct(), format.c_str(), &l, off, len, 0, extents);
    // go through the extents
    for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); ++p) {
      // assemble pieces of a given object into a single buffer list
      bufferlist oid_bl;
      for (vector<pair<uint64_t,uint64_t> >::iterator q = p->buffer_extents.begin();
	   q != p->buffer_extents.end();
	   ++q) {
        bufferlist buffer_bl;
        buffer_bl.substr_of(bl, q->first, q->second);
        oid_bl.append(buffer_bl);
      }
      // and write the object
      c->add_request();
      librados::AioCompletion *rados_completion =
        librados::Rados::aio_create_completion(c.get(),
					       rados_req_write_complete);
      r = m_ioCtx.aio_write(p->oid.name, rados_completion, oid_bl,
			    p->length, p->offset);
      rados_completion->release();
      if (r < 0)
        break;
    }
  }
  c->finish_adding_requests();
  return r;
}

int libradosstriper::RadosStriperImpl::extract_uint32_attr
(std::map<std::string, bufferlist> &attrs,
 const std::string& key,
 ceph_le32 *value)
{
  std::map<std::string, bufferlist>::iterator attrsIt = attrs.find(key);
  if (attrsIt != attrs.end()) {
    // this intermediate string allows to add a null terminator before calling strtol
    std::string strvalue(attrsIt->second.c_str(), attrsIt->second.length());
    std::string err;   
    *value = strict_strtol(strvalue.c_str(), 10, &err);
    if (!err.empty()) {
      lderr(cct()) << key << " : " << err << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }
  return 0;
}

int libradosstriper::RadosStriperImpl::extract_sizet_attr
(std::map<std::string, bufferlist> &attrs,
 const std::string& key,
 size_t *value)
{
  std::map<std::string, bufferlist>::iterator attrsIt = attrs.find(key);
  if (attrsIt != attrs.end()) {
    // this intermediate string allows to add a null terminator before calling strtol
    std::string strvalue(attrsIt->second.c_str(), attrsIt->second.length());
    std::string err;   
    *value = strict_strtoll(strvalue.c_str(), 10, &err);
    if (!err.empty()) {
      lderr(cct()) << key << " : " << err << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }
  return 0;
}

int libradosstriper::RadosStriperImpl::internal_get_layout_and_size(
  const std::string& oid,
  ceph_file_layout *layout,
  uint64_t *size)
{
  // get external attributes of the first rados object
  std::map<std::string, bufferlist> attrs;
  int rc = m_ioCtx.getxattrs(oid, attrs);
  if (rc) return rc;
  // deal with stripe_unit
  rc = extract_uint32_attr(attrs, XATTR_LAYOUT_STRIPE_UNIT, &layout->fl_stripe_unit);
  if (rc) return rc;
  // deal with stripe_count
  rc = extract_uint32_attr(attrs, XATTR_LAYOUT_STRIPE_COUNT, &layout->fl_stripe_count);
  if (rc) return rc;
  // deal with object_size
  rc = extract_uint32_attr(attrs, XATTR_LAYOUT_OBJECT_SIZE, &layout->fl_object_size);
  if (rc) return rc;
  // deal with size
  size_t ssize;
  rc = extract_sizet_attr(attrs, XATTR_SIZE, &ssize);
  if (rc) {
    return rc;
  }
  *size = ssize;
  // make valgrind happy by setting unused fl_pg_pool
  layout->fl_pg_pool = 0;
  return 0;
}

int libradosstriper::RadosStriperImpl::openStripedObjectForRead(
  const std::string& soid,
  ceph_file_layout *layout,
  uint64_t *size,
  std::string *lockCookie)
{
  // take a lock the first rados object, if it exists and gets its size
  // check, lock and size reading must be atomic and are thus done within a single operation
  librados::ObjectWriteOperation op;
  op.assert_exists();
  *lockCookie = getUUID();
  utime_t dur = utime_t();
  rados::cls::lock::lock(&op, RADOS_LOCK_NAME, ClsLockType::SHARED, *lockCookie, "Tag", "", dur, 0);
  std::string firstObjOid = getObjectId(soid, 0);
  int rc = m_ioCtx.operate(firstObjOid, &op);
  if (rc) {
    // error case (including -ENOENT)
    return rc;
  }
  rc = internal_get_layout_and_size(firstObjOid, layout, size);
  if (rc) {
    unlockObject(soid, *lockCookie);
    lderr(cct()) << "RadosStriperImpl::openStripedObjectForRead : "
		 << "could not load layout and size for "
		 << soid << " : rc = " << rc << dendl;
  }
  return rc;
}

int libradosstriper::RadosStriperImpl::openStripedObjectForWrite(const std::string& soid,
								 ceph_file_layout *layout,
								 uint64_t *size,
								 std::string *lockCookie,
								 bool isFileSizeAbsolute)
{
  // take a lock the first rados object, if it exists
  // check and lock must be atomic and are thus done within a single operation
  librados::ObjectWriteOperation op;
  op.assert_exists();
  *lockCookie = getUUID();
  utime_t dur = utime_t();
  rados::cls::lock::lock(&op, RADOS_LOCK_NAME, ClsLockType::SHARED, *lockCookie, "Tag", "", dur, 0);
  std::string firstObjOid = getObjectId(soid, 0);
  int rc = m_ioCtx.operate(firstObjOid, &op);
  if (rc) {
    if (rc == -ENOENT) {
      // object does not exist, delegate to createEmptyStripedObject
      int rc = createAndOpenStripedObject(soid, layout, *size, lockCookie, isFileSizeAbsolute);
      // return original size
      *size = 0;
      return rc; 
    } else {
      return rc;
    }
  }
  // all fine
  uint64_t curSize;
  rc = internal_get_layout_and_size(firstObjOid, layout, &curSize);
  if (rc) {
    unlockObject(soid, *lockCookie);
    lderr(cct()) << "RadosStriperImpl::openStripedObjectForWrite : "
		   << "could not load layout and size for "
		   << soid << " : rc = " << rc << dendl;
    return rc;
  }
  // atomically update object size, only if smaller than current one
  if (!isFileSizeAbsolute)
    *size += curSize;
  librados::ObjectWriteOperation writeOp;
  writeOp.cmpxattr(XATTR_SIZE, LIBRADOS_CMPXATTR_OP_GT, *size);
  std::ostringstream oss;
  oss << *size;
  bufferlist bl;
  bl.append(oss.str());
  writeOp.setxattr(XATTR_SIZE, bl);
  rc = m_ioCtx.operate(firstObjOid, &writeOp);
  // return current size
  *size = curSize;
  // handle case where objectsize is already bigger than size
  if (-ECANCELED == rc) 
    rc = 0;
  if (rc) {
    unlockObject(soid, *lockCookie);
    lderr(cct()) << "RadosStriperImpl::openStripedObjectForWrite : "
		   << "could not set new size for "
		   << soid << " : rc = " << rc << dendl;
  }
  return rc;
}

int libradosstriper::RadosStriperImpl::createAndOpenStripedObject(const std::string& soid,
								  ceph_file_layout *layout,
								  uint64_t size,
								  std::string *lockCookie,
								  bool isFileSizeAbsolute)
{
  // build atomic write operation
  librados::ObjectWriteOperation writeOp;
  writeOp.create(true);
  // object_size
  std::ostringstream oss_object_size;
  oss_object_size << m_layout.fl_object_size;
  bufferlist bl_object_size;
  bl_object_size.append(oss_object_size.str());
  writeOp.setxattr(XATTR_LAYOUT_OBJECT_SIZE, bl_object_size);
  // stripe unit
  std::ostringstream oss_stripe_unit;
  oss_stripe_unit << m_layout.fl_stripe_unit;
  bufferlist bl_stripe_unit;
  bl_stripe_unit.append(oss_stripe_unit.str());
  writeOp.setxattr(XATTR_LAYOUT_STRIPE_UNIT, bl_stripe_unit);
  // stripe count
  std::ostringstream oss_stripe_count;
  oss_stripe_count << m_layout.fl_stripe_count;
  bufferlist bl_stripe_count;
  bl_stripe_count.append(oss_stripe_count.str());
  writeOp.setxattr(XATTR_LAYOUT_STRIPE_COUNT, bl_stripe_count);
  // size
  std::ostringstream oss_size;
  oss_size << (isFileSizeAbsolute?size:0);
  bufferlist bl_size;
  bl_size.append(oss_size.str());
  writeOp.setxattr(XATTR_SIZE, bl_size);
  // effectively change attributes
  std::string firstObjOid = getObjectId(soid, 0);
  int rc = m_ioCtx.operate(firstObjOid, &writeOp);
  // in case of error (but no EEXIST which would mean the object existed), return
  if (rc && -EEXIST != rc) return rc;
  // Otherwise open the object
  uint64_t fileSize = size;
  return openStripedObjectForWrite(soid, layout, &fileSize, lockCookie, isFileSizeAbsolute);
}

static void striper_truncate_aio_req_complete(rados_striper_multi_completion_t c, void *arg)
{
  auto cdata = ceph::ref_t<TruncateCompletionData>(static_cast<TruncateCompletionData*>(arg), false);
  libradosstriper::MultiAioCompletionImpl *comp =
    reinterpret_cast<libradosstriper::MultiAioCompletionImpl*>(c);
  if (0 == comp->rval) {
    // all went fine, change size in the external attributes
    std::ostringstream oss;
    oss << cdata->m_size;
    bufferlist bl;
    bl.append(oss.str());
    cdata->m_striper->setxattr(cdata->m_soid, XATTR_SIZE, bl);
  }
}

int libradosstriper::RadosStriperImpl::truncate(const std::string& soid,
						uint64_t original_size,
						uint64_t size,
						ceph_file_layout &layout) 
{
  auto cdata = ceph::make_ref<TruncateCompletionData>(this, soid, size);
  libradosstriper::MultiAioCompletionImplPtr multi_completion{
    new libradosstriper::MultiAioCompletionImpl, false};
  multi_completion->set_complete_callback(cdata->get() /* create ref! */, striper_truncate_aio_req_complete);
  // call asynchrous version of truncate
  int rc = aio_truncate(soid, multi_completion, original_size, size, layout);
  // wait for completion of the truncation
  multi_completion->finish_adding_requests();
  multi_completion->wait_for_complete_and_cb();
  // return result
  if (rc == 0) {
    rc = multi_completion->get_return_value();
  }
  return rc;
}

int libradosstriper::RadosStriperImpl::aio_truncate
(const std::string& soid,
 libradosstriper::MultiAioCompletionImplPtr multi_completion,
 uint64_t original_size,
 uint64_t size,
 ceph_file_layout &layout)
{
  // handle the underlying rados objects. 3 cases here :
  //  -- the objects belonging to object sets entirely located
  //     before the truncation are unchanged
  //  -- the objects belonging to the object set where the
  //     truncation took place are truncated or removed
  //  -- the objects belonging to object sets entirely located
  //     after the truncation are removed
  // Note that we do it backward and that we change the size in
  // the external attributes only at the end. This make sure that
  // no rados object stays behind if we remove the striped object
  // after a truncation has failed
  uint64_t trunc_objectsetno = size / layout.fl_object_size / layout.fl_stripe_count;
  uint64_t last_objectsetno = original_size / layout.fl_object_size / layout.fl_stripe_count;
  bool exists = false;
  for (int64_t objectno = (last_objectsetno+1) * layout.fl_stripe_count-1;
       objectno >= (int64_t)((trunc_objectsetno + 1) * layout.fl_stripe_count);
       objectno--) {
    // if no object existed so far, check object existence
    if (!exists) {
      uint64_t nb_full_object_set = objectno / layout.fl_stripe_count;
      uint64_t object_index_in_set = objectno % layout.fl_stripe_count;
      uint64_t set_start_off = nb_full_object_set * layout.fl_object_size * layout.fl_stripe_count;
      uint64_t object_start_off = set_start_off + object_index_in_set * layout.fl_stripe_unit;
      exists = (original_size > object_start_off);
    }
    if (exists) {
      // remove asynchronously
      multi_completion->add_request();
      auto data = ceph::make_ref<RadosRemoveCompletionData>(multi_completion, cct());
      librados::AioCompletion *rados_completion =
	librados::Rados::aio_create_completion(data->get() /* create ref! */,
					       rados_req_remove_complete);
      int rc = m_ioCtx.aio_remove(getObjectId(soid, objectno), rados_completion);
      rados_completion->release();
      // in case the object did not exist, it means we had a sparse file, all is fine
      if (rc && rc != -ENOENT) return rc;
    }
  }
  for (int64_t objectno = ((trunc_objectsetno + 1) * layout.fl_stripe_count) -1;
       objectno >= (int64_t)(trunc_objectsetno * layout.fl_stripe_count);
       objectno--) {
    // if no object existed so far, check object existence
    if (!exists) {
      uint64_t object_start_off = ((objectno / layout.fl_stripe_count) * layout.fl_object_size) +
	((objectno % layout.fl_stripe_count) * layout.fl_stripe_unit);
      exists = (original_size > object_start_off);
    }
    if (exists) {
      // truncate
      file_layout_t l;
      l.from_legacy(layout);
      uint64_t new_object_size = Striper::object_truncate_size(cct(), &l, objectno, size);
      int rc;
      if (new_object_size > 0 or 0 == objectno) {
	// trunc is synchronous as there is no async version
	// but note that only a single object will be truncated
	// reducing the overload to a fixed amount
	rc = m_ioCtx.trunc(getObjectId(soid, objectno), new_object_size);
      } else {
	// removes are asynchronous in order to speed up truncations of big files
	multi_completion->add_request();
	auto data = ceph::make_ref<RadosRemoveCompletionData>(multi_completion, cct());
	librados::AioCompletion *rados_completion =
	  librados::Rados::aio_create_completion(data->get() /* create ref! */,
						 rados_req_remove_complete);
	rc = m_ioCtx.aio_remove(getObjectId(soid, objectno), rados_completion);
	rados_completion->release();
      }
      // in case the object did not exist, it means we had a sparse file, all is fine
      if (rc && rc != -ENOENT) return rc;
    }
  }
  return 0;
}  

int libradosstriper::RadosStriperImpl::grow(const std::string& soid,
					    uint64_t original_size,
					    uint64_t size,
					    ceph_file_layout &layout) 
{
  // handle the underlying rados objects. As we support sparse objects,
  // we only have to change the size in the external attributes
  std::ostringstream oss;
  oss << size;
  bufferlist bl;
  bl.append(oss.str());
  int rc = m_ioCtx.setxattr(getObjectId(soid, 0), XATTR_SIZE, bl);
  return rc;
}  

std::string libradosstriper::RadosStriperImpl::getUUID()
{
  struct uuid_d uuid;
  uuid.generate_random();
  char suuid[37];
  uuid.print(suuid);
  return std::string(suuid);
}
