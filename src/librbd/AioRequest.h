// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_AIOREQUEST_H
#define CEPH_LIBRBD_AIOREQUEST_H

#include "include/int_types.h"

#include <map>

#include "common/snap_types.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"

namespace librbd {

  struct AioCompletion;
  struct ImageCtx;

  /**
   * This class represents an I/O operation to a single RBD data object.
   * Its subclasses encapsulate logic for dealing with special cases
   * for I/O due to layering.
   */
  class AioRequest
  {
  public:
    AioRequest();
    AioRequest(ImageCtx *ictx, const std::string &oid,
	       uint64_t objectno, uint64_t off, uint64_t len,
	       librados::snap_t snap_id, Context *completion,
	       bool hide_enoent);
    virtual ~AioRequest();

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
    virtual void send() = 0;

  protected:
    void read_from_parent(vector<pair<uint64_t,uint64_t> >& image_extents);

    ImageCtx *m_ictx;
    librados::IoCtx *m_ioctx;
    std::string m_oid;
    uint64_t m_object_no, m_object_off, m_object_len;
    librados::snap_t m_snap_id;
    Context *m_completion;
    AioCompletion *m_parent_completion;
    ceph::bufferlist m_read_data;
    bool m_hide_enoent;
  };

  class AioRead : public AioRequest {
  public:
    AioRead(ImageCtx *ictx, const std::string &oid,
	    uint64_t objectno, uint64_t offset, uint64_t len,
	    vector<pair<uint64_t,uint64_t> >& be,
	    librados::snap_t snap_id, bool sparse,
	    Context *completion)
      : AioRequest(ictx, oid, objectno, offset, len, snap_id, completion,
		   false),
	m_buffer_extents(be),
	m_tried_parent(false), m_sparse(sparse) {
    }
    virtual ~AioRead() {}
    virtual bool should_complete(int r);
    virtual void send();

    ceph::bufferlist &data() {
      return m_read_data;
    }
    std::map<uint64_t, uint64_t> m_ext_map;

    friend class C_AioRead;

  private:
    vector<pair<uint64_t,uint64_t> > m_buffer_extents;
    bool m_tried_parent;
    bool m_sparse;
  };

  class AbstractWrite : public AioRequest {
  public:
    AbstractWrite();
    AbstractWrite(ImageCtx *ictx, const std::string &oid,
		  uint64_t object_no, uint64_t object_off, uint64_t len,
		  vector<pair<uint64_t,uint64_t> >& objectx, uint64_t object_overlap,
		  const ::SnapContext &snapc,
		  librados::snap_t snap_id,
		  Context *completion,
		  bool hide_enoent);
    virtual ~AbstractWrite() {}
    virtual bool should_complete(int r);
    virtual void send();
    void guard_write();

    bool has_parent() const {
      return !m_object_image_extents.empty();
    }

  private:
    /**
     * Writes go through the following state machine to deal with
     * layering:
     *
     *                           need copyup
     * LIBRBD_AIO_WRITE_GUARD ---------------> LIBRBD_AIO_WRITE_COPYUP
     *           |        ^                              |
     *           v        \------------------------------/
     *         done
     *           ^
     *           |
     * LIBRBD_AIO_WRITE_FLAT
     *
     * Writes start in LIBRBD_AIO_WRITE_GUARD or _FLAT, depending on whether
     * there is a parent or not.
     */
    enum write_state_d {
      LIBRBD_AIO_WRITE_GUARD,
      LIBRBD_AIO_WRITE_COPYUP,
      LIBRBD_AIO_WRITE_FLAT
    };

  protected:
    virtual void add_copyup_ops() = 0;

    write_state_d m_state;
    vector<pair<uint64_t,uint64_t> > m_object_image_extents;
    uint64_t m_parent_overlap;
    librados::ObjectWriteOperation m_write;
    librados::ObjectWriteOperation m_copyup;
    uint64_t m_snap_seq;
    std::vector<librados::snap_t> m_snaps;

  private:
    void send_copyup();
  };

  class AioWrite : public AbstractWrite {
  public:
    AioWrite(ImageCtx *ictx, const std::string &oid,
	     uint64_t object_no, uint64_t object_off,
	     vector<pair<uint64_t,uint64_t> >& objectx, uint64_t object_overlap,
	     const ceph::bufferlist &data, const ::SnapContext &snapc,
	     librados::snap_t snap_id,
	     Context *completion)
      : AbstractWrite(ictx, oid,
		      object_no, object_off, data.length(),
		      objectx, object_overlap,
		      snapc, snap_id,
		      completion, false),
	m_write_data(data) {
      guard_write();
      add_write_ops(m_write);
    }
    virtual ~AioWrite() {}

  protected:
    virtual void add_copyup_ops() {
      add_write_ops(m_copyup);
    }

  private:
    void add_write_ops(librados::ObjectWriteOperation &wr);
    ceph::bufferlist m_write_data;
  };

  class AioRemove : public AbstractWrite {
  public:
    AioRemove(ImageCtx *ictx, const std::string &oid,
	      uint64_t object_no,
	      vector<pair<uint64_t,uint64_t> >& objectx, uint64_t object_overlap,
	      const ::SnapContext &snapc, librados::snap_t snap_id,
	      Context *completion)
      : AbstractWrite(ictx, oid,
		      object_no, 0, 0,
		      objectx, object_overlap,
		      snapc, snap_id, completion,
		      true) {
      if (has_parent())
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
    AioTruncate(ImageCtx *ictx, const std::string &oid,
		uint64_t object_no, uint64_t object_off,
		vector<pair<uint64_t,uint64_t> >& objectx, uint64_t object_overlap,
		const ::SnapContext &snapc, librados::snap_t snap_id,
		Context *completion)
      : AbstractWrite(ictx, oid,
		      object_no, object_off, 0,
		      objectx, object_overlap,
		      snapc, snap_id, completion,
		      true) {
      guard_write();
      m_write.truncate(object_off);
    }
    virtual ~AioTruncate() {}

  protected:
    virtual void add_copyup_ops() {
      m_copyup.truncate(m_object_off);
    }
  };

  class AioZero : public AbstractWrite {
  public:
    AioZero(ImageCtx *ictx, const std::string &oid,
	    uint64_t object_no, uint64_t object_off, uint64_t object_len,
	    vector<pair<uint64_t,uint64_t> >& objectx, uint64_t object_overlap,
	    const ::SnapContext &snapc, librados::snap_t snap_id,
	    Context *completion)
      : AbstractWrite(ictx, oid,
		      object_no, object_off, object_len,
		      objectx, object_overlap,
		      snapc, snap_id, completion,
		      true) {
      guard_write();
      m_write.zero(object_off, object_len);
    }
    virtual ~AioZero() {}

  protected:
    virtual void add_copyup_ops() {
      m_copyup.zero(m_object_off, m_object_len);
    }
  };

}

#endif
