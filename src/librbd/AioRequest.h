// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_AIOREQUEST_H
#define CEPH_LIBRBD_AIOREQUEST_H

#include <map>

#include "inttypes.h"

#include "common/snap_types.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"

namespace librbd {

  class AioCompletion;
  class ImageCtx;

  /**
   * This class represents an I/O operation to a single RBD data object.
   * Its subclasses encapsulate logic for dealing with special cases
   * for I/O due to layering.
   */
  class AioRequest
  {
  public:
    AioRequest();
    AioRequest(ImageCtx *ictx, const std::string &oid, uint64_t image_ofs,
	       size_t len, librados::snap_t snap_id, Context *completion,
	       bool hide_enoent);
    virtual ~AioRequest();

    uint64_t offset()
    {
      return m_block_ofs;
    }

    size_t length()
    {
      return m_len;
    }

    void complete(int r)
    {
      if (should_complete(r)) {
	if (m_hide_enoent && r == -ENOENT)
	  r = 0;
	m_completion->complete(r);
	delete this;
      }
    }

    virtual bool should_complete(int r) = 0;
    virtual int send() = 0;

  protected:
    void read_from_parent(uint64_t image_ofs, size_t len);

    ImageCtx *m_ictx;
    librados::IoCtx m_ioctx;
    std::string m_oid;
    uint64_t m_image_ofs;
    uint64_t m_block_ofs;
    size_t m_len;
    librados::snap_t m_snap_id;
    Context *m_completion;
    AioCompletion *m_parent_completion;
    ceph::bufferlist m_read_data;
    bool m_hide_enoent;
  };

  class AioRead : public AioRequest {
  public:
    AioRead(ImageCtx *ictx, const std::string &oid, uint64_t image_ofs,
	    size_t len, librados::snap_t snap_id, bool sparse,
	    Context *completion)
      : AioRequest(ictx, oid, image_ofs, len, snap_id, completion, false),
	m_tried_parent(false), m_sparse(sparse) {
      m_ioctx.snap_set_read(m_snap_id);
    }
    virtual ~AioRead() {}
    virtual bool should_complete(int r);
    virtual int send();

    ceph::bufferlist &data() {
      return m_read_data;
    }
    std::map<uint64_t, uint64_t> &ext_map() {
      return m_ext_map;
    }

  private:
    std::map<uint64_t, uint64_t> m_ext_map;
    bool m_tried_parent;
    bool m_sparse;
  };

  class AbstractWrite : public AioRequest {
  public:
    AbstractWrite();
    AbstractWrite(ImageCtx *ictx, const std::string &oid, uint64_t image_ofs,
		  size_t len, librados::snap_t snap_id, Context *completion,
		  bool has_parent, const ::SnapContext &snapc, bool hide_enoent);
    virtual ~AbstractWrite() {}
    virtual bool should_complete(int r);
    virtual int send();
    void guard_write();

  private:
    /**
     * Writes go through the following state machine to
     * deal with layering:
     *                           need copyup
     * LIBRBD_AIO_CHECK_EXISTS ---------------> LIBRBD_AIO_WRITE_COPYUP
     *           |                                       |
     *           | no overlap or object exists           | parent data read
     *           |                                       |
     *           v                                       |
     * LIBRBD_AIO_WRITE_FINAL <--------------------------/
     *
     * By default images start in LIBRBD_AIO_WRITE_FINAL.
     * If the write may need a copyup, it will start in
     * LIBRBD_AIO_WRITE_CHECK_EXISTS instead.
     */
    enum write_state_d {
      LIBRBD_AIO_WRITE_CHECK_EXISTS,
      LIBRBD_AIO_WRITE_COPYUP,
      LIBRBD_AIO_WRITE_FINAL
    };

  protected:
    virtual void add_copyup_ops() = 0;

    write_state_d m_state;
    bool m_has_parent;
    librados::ObjectReadOperation m_read;
    librados::ObjectWriteOperation m_write;
    librados::ObjectWriteOperation m_copyup;

  private:
    void send_copyup();
  };

  class AioWrite : public AbstractWrite {
  public:
    AioWrite(ImageCtx *ictx, const std::string &oid, uint64_t image_ofs,
	     const ceph::bufferlist &data, const ::SnapContext &snapc,
	     librados::snap_t snap_id, bool has_parent, Context *completion)
      : AbstractWrite(ictx, oid, image_ofs, data.length(), snap_id, completion,
		      has_parent, snapc, false),
	m_write_data(data) {
      guard_write();
      m_write.write(m_block_ofs, data);
    }
    virtual ~AioWrite() {}

  protected:
    virtual void add_copyup_ops() {
      m_copyup.write(m_block_ofs, m_write_data);
    }

  private:
    ceph::bufferlist m_write_data;
  };

  class AioRemove : public AbstractWrite {
  public:
    AioRemove(ImageCtx *ictx, const std::string &oid, uint64_t image_ofs,
	      const ::SnapContext &snapc, librados::snap_t snap_id,
	      bool has_parent, Context *completion)
      : AbstractWrite(ictx, oid, image_ofs, 0, snap_id, completion,
		      has_parent, snapc, true) {
      if (has_parent)
	m_write.truncate(0);
      else
	m_write.remove();
    }
    virtual ~AioRemove() {}

  protected:
    virtual void add_copyup_ops() {
      // removing an object never needs to copyup
      assert(0);
    }
  };

  class AioTruncate : public AbstractWrite {
  public:
    AioTruncate(ImageCtx *ictx, const std::string &oid, uint64_t image_ofs,
		const ::SnapContext &snapc, librados::snap_t snap_id,
		bool has_parent, Context *completion)
      : AbstractWrite(ictx, oid, image_ofs, 0, snap_id, completion,
		      has_parent, snapc, true) {
      guard_write();
      m_write.truncate(m_block_ofs);
    }
    virtual ~AioTruncate() {}

  protected:
    virtual void add_copyup_ops() {
      m_copyup.truncate(m_block_ofs);
    }
  };

  class AioZero : public AbstractWrite {
  public:
    AioZero(ImageCtx *ictx, const std::string &oid, uint64_t image_ofs,
	    size_t len, const ::SnapContext &snapc, librados::snap_t snap_id,
	    bool has_parent, Context *completion)
      : AbstractWrite(ictx, oid, image_ofs, len, snap_id, completion,
		      has_parent, snapc, true) {
      guard_write();
      m_write.zero(m_block_ofs, len);
    }
    virtual ~AioZero() {}

  protected:
    virtual void add_copyup_ops() {
      m_copyup.zero(m_block_ofs, m_len);
    }
  };

}

#endif
