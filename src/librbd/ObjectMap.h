// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OBJECT_MAP_H
#define CEPH_LIBRBD_OBJECT_MAP_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "include/rbd/object_map_types.h"
#include "common/bit_vector.hpp"
#include "librbd/AsyncRequest.h"
#include <boost/optional.hpp>

class Context;

namespace librbd {

class ImageCtx;

class ObjectMap {
public:

  ObjectMap(ImageCtx &image_ctx);

  static std::string object_map_name(const std::string &image_id,
				     uint64_t snap_id);

  int lock();
  int unlock();

  bool object_may_exist(uint64_t object_no) const;

  void aio_resize(uint64_t new_size, uint8_t default_object_state,
		  Context *on_finish);
  bool aio_update(uint64_t object_no, uint8_t new_state,
		  const boost::optional<uint8_t> &current_state,
		  Context *on_finish);
  bool aio_update(uint64_t start_object_no, uint64_t end_object_no,
		  uint8_t new_state,
		  const boost::optional<uint8_t> &current_state,
		  Context *on_finish);

  void refresh(uint64_t snap_id);
  void rollback(uint64_t snap_id);
  void snapshot(uint64_t snap_id);

  bool enabled() const;

private:

  class Request : public AsyncRequest {
  public:
    Request(ImageCtx &image_ctx, Context *on_finish)
      : AsyncRequest(image_ctx, on_finish), m_state(STATE_REQUEST)
    {
    }

  protected:
    virtual bool should_complete(int r);
    virtual int filter_return_code(int r) {
      // never propagate an error back to the caller
      return 0;
    }
    virtual void finish(ObjectMap *object_map) = 0;

  private:
    /**
     * <start> ---> STATE_REQUEST ---> <finish>
     *                   |                ^
     *                   v                |
     *            STATE_INVALIDATE -------/
     */
    enum State {
      STATE_REQUEST,
      STATE_INVALIDATE
    };

    State m_state;

    bool invalidate();
  };

  class ResizeRequest : public Request {
  public:
    ResizeRequest(ImageCtx &image_ctx, uint64_t new_size,
		  uint8_t default_object_state, Context *on_finish)
      : Request(image_ctx, on_finish), m_num_objs(0), m_new_size(new_size),
        m_default_object_state(default_object_state)
    {
    }

    virtual void send();
  protected:
    virtual void finish(ObjectMap *object_map);
  private:
    uint64_t m_num_objs;
    uint64_t m_new_size;
    uint8_t m_default_object_state;
  };

  class UpdateRequest : public Request {
  public:
    UpdateRequest(ImageCtx &image_ctx, uint64_t start_object_no,
		  uint64_t end_object_no, uint8_t new_state,
                  const boost::optional<uint8_t> &current_state,
		  Context *on_finish)
      : Request(image_ctx, on_finish), m_start_object_no(start_object_no),
	m_end_object_no(end_object_no), m_new_state(new_state),
	m_current_state(current_state)
    {
    }

    virtual void send();
  protected:
    virtual void finish(ObjectMap *object_map);
  private:
    uint64_t m_start_object_no;
    uint64_t m_end_object_no;
    uint8_t m_new_state;
    boost::optional<uint8_t> m_current_state;
  };

  ImageCtx &m_image_ctx;

  ceph::BitVector<2> m_object_map;

  bool m_enabled;

  void invalidate();

};

} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_H
