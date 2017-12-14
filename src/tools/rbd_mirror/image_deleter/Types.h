// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_DELETER_TYPES_H
#define CEPH_RBD_MIRROR_IMAGE_DELETER_TYPES_H

#include "include/Context.h"
#include "librbd/journal/Policy.h"
#include <string>

struct utime_t;

namespace rbd {
namespace mirror {
namespace image_deleter {

enum ErrorResult {
  ERROR_RESULT_COMPLETE,
  ERROR_RESULT_RETRY,
  ERROR_RESULT_RETRY_IMMEDIATELY
};

struct TrashListener {
  TrashListener() {
  }
  TrashListener(const TrashListener&) = delete;
  TrashListener& operator=(const TrashListener&) = delete;

  virtual ~TrashListener() {
  }

  virtual void handle_trash_image(const std::string& image_id,
                                  const utime_t& deferment_end_time) = 0;

};

struct JournalPolicy : public librbd::journal::Policy {
  bool append_disabled() const override {
    return true;
  }
  bool journal_disabled() const override {
    return true;
  }

  void allocate_tag_on_lock(Context *on_finish) override {
    on_finish->complete(0);
  }
};

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_DELETER_TYPES_H
