// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_H
#define RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_H

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
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_IMAGE_REPLAYER_REPLAYER_H
