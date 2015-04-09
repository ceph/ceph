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
#include "librbd/ObjectMap.h"

namespace librbd {

  struct AioCompletion;
  struct ImageCtx;
  class CopyupRequest;

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
               const ::SnapContext &snapc, librados::snap_t snap_id,
               Context *completion, bool hide_enoent);
    virtual ~AioRequest();

    void complete(int r);

    virtual bool should_complete(int r) = 0;
    virtual void send() = 0;

  protected:
    void read_from_parent(vector<pair<uint64_t,uint64_t> >& image_extents,
                          bool block_completion);

    ImageCtx *m_ictx;
    std::string m_oid;
    uint64_t m_object_no, m_object_off, m_object_len;
    librados::snap_t m_snap_id;
    Context *m_completion;
    AioCompletion *m_parent_completion;
    ceph::bufferlist m_read_data;
    bool m_hide_enoent;
    std::vector<librados::snap_t> m_snaps;
  };

  class AioRead : public AioRequest {
  public:
    AioRead(ImageCtx *ictx, const std::string &oid,
	    uint64_t objectno, uint64_t offset, uint64_t len,
	    vector<pair<uint64_t,uint64_t> >& be, const ::SnapContext &snapc,
	    librados::snap_t snap_id, bool sparse,
	    Context *completion, int op_flags);
    virtual ~AioRead() {}
    virtual bool should_complete(int r);
    virtual void send();
    void guard_read();

    ceph::bufferlist &data() {
      return m_read_data;
    }

    std::map<uint64_t, uint64_t> m_ext_map;

    friend class C_AioRead;

  private:
    vector<pair<uint64_t,uint64_t> > m_buffer_extents;
    bool m_tried_parent;
    bool m_sparse;
    int m_op_flags;
    vector<pair<uint64_t,uint64_t> > m_image_extents;

    /**
     * Reads go through the following state machine to deal with
     * layering:
     *
     *                          need copyup
     * LIBRBD_AIO_READ_GUARD ---------------> LIBRBD_AIO_READ_COPYUP
     *           |                                       |
     *           v                                       |
     *         done <------------------------------------/
     *           ^
     *           |
     * LIBRBD_AIO_READ_FLAT
     *
     * Reads start in LIBRBD_AIO_READ_GUARD or _FLAT, depending on
     * whether there is a parent or not.
     */
    enum read_state_d {
      LIBRBD_AIO_READ_GUARD,
      LIBRBD_AIO_READ_COPYUP,
      LIBRBD_AIO_READ_FLAT
    };

    read_state_d m_state;
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

    bool has_parent() const {
      return !m_object_image_extents.empty();
    }

  private:
    /**
     * Writes go through the following state machine to deal with
     * layering and the object map:
     *
     * <start>
     *  .  |
     *  .  |
     *  .  \---> LIBRBD_AIO_WRITE_PRE
     *  .           |         |
     *  . . . . . . | . . . . | . . . . . . . . . . . 
     *      .       |   -or-  |                     .
     *      .       |         |                     v
     *      .       |         \----------------> LIBRBD_AIO_WRITE_FLAT . . .
     *      .       |                                               |      .
     *      v       v         need copyup                           |      .
     * LIBRBD_AIO_WRITE_GUARD -----------> LIBRBD_AIO_WRITE_COPYUP  |      .
     *  .       |   ^                           |                   |      .
     *  .       |   |                           |                   |      .
     *  .       |   \---------------------------/                   |      .
     *  .       |                                                   |      .
     *  .       \-------------------\           /-------------------/      .
     *  .                           |           |                          .
     *  .                       LIBRBD_AIO_WRITE_POST                      .
     *  .                                |                                 .
     *  .                                v                                 .
     *  . . . . . . . . . . . . . . > <finish> < . . . . . . . . . . . . . . 
     *
     * The _PRE_REMOVE/_POST_REMOVE states are skipped if the object map
     * is disabled.  The write starts in _WRITE_GUARD or _FLAT depending on
     * whether or not there is a parent overlap.
     */
    enum write_state_d {
      LIBRBD_AIO_WRITE_GUARD,
      LIBRBD_AIO_WRITE_COPYUP,
      LIBRBD_AIO_WRITE_FLAT,
      LIBRBD_AIO_WRITE_PRE,
      LIBRBD_AIO_WRITE_POST,
      LIBRBD_AIO_WRITE_ERROR
    };

  protected:
    write_state_d m_state;
    vector<pair<uint64_t,uint64_t> > m_object_image_extents;
    uint64_t m_parent_overlap;
    librados::ObjectWriteOperation m_write;
    uint64_t m_snap_seq;
    ceph::bufferlist *m_entire_object;

    virtual void add_write_ops(librados::ObjectWriteOperation *wr) = 0;
    virtual void guard_write();
    virtual void pre_object_map_update(uint8_t *new_state) = 0;
    virtual bool post_object_map_update() {
      return false;
    }

  private:
    bool send_pre();
    bool send_post();
    void send_write();
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
	m_write_data(data), m_op_flags(0) {
    }
    virtual ~AioWrite() {}

    void set_op_flags(int op_flags) {
      m_op_flags = op_flags;
    }
  protected:
    virtual void add_write_ops(librados::ObjectWriteOperation *wr);
    virtual void pre_object_map_update(uint8_t *new_state) {
      *new_state = OBJECT_EXISTS;
    }

  private:
    ceph::bufferlist m_write_data;
    int m_op_flags;
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
    }
    virtual ~AioRemove() {}

  protected:
    virtual void add_write_ops(librados::ObjectWriteOperation *wr) {
      if (has_parent()) {
	wr->truncate(0);
      } else {
	wr->remove();
      }
    }

    virtual void pre_object_map_update(uint8_t *new_state) {
      if (has_parent()) {
	m_object_state = OBJECT_EXISTS;
      } else {
	m_object_state = OBJECT_PENDING;
      }
      *new_state = m_object_state;
    }

    virtual bool post_object_map_update() {
      if (m_object_state == OBJECT_EXISTS) {
	return false;
      }
      return true;
    }

    virtual void guard_write() {
      // do nothing to disable write guard
    }

  private:
    uint8_t m_object_state;
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
    }
    virtual ~AioTruncate() {}

  protected:
    virtual void add_write_ops(librados::ObjectWriteOperation *wr) {
      wr->truncate(m_object_off);
    }

    virtual void pre_object_map_update(uint8_t *new_state) {
      *new_state = OBJECT_EXISTS;
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
    }
    virtual ~AioZero() {}

  protected:
    virtual void add_write_ops(librados::ObjectWriteOperation *wr) {
      wr->zero(m_object_off, m_object_len);
    }

    virtual void pre_object_map_update(uint8_t *new_state) {
      *new_state = OBJECT_EXISTS;
    }
  };

}

#endif
