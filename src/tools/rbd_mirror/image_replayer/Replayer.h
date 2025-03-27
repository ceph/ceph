// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_H
#define RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_H

#include <cstdint>
#include <string>

struct Context;

namespace rbd {
namespace mirror {
namespace image_replayer {

struct Replayer {
  virtual ~Replayer() {}

  virtual void destroy() = 0;

  virtual void init(Context* on_finish) = 0;
  virtual void shut_down(Context* on_finish) = 0;

  virtual void flush(Context* on_finish) = 0;

  virtual bool get_replay_status(std::string* description,
                                 Context* on_finish) = 0;

  virtual bool is_replaying() const = 0;
  virtual bool is_resync_requested() const = 0;

  virtual int get_error_code() const = 0;
  virtual std::string get_error_description() const = 0;

  virtual void prune_snapshot(uint64_t) = 0;
  virtual void set_remote_snap_id_end_limit(uint64_t) = 0;
  virtual uint64_t get_remote_snap_id_end_limit() = 0;

  virtual uint64_t get_last_snapshot_bytes() const {
    return 0;
  }
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_H
