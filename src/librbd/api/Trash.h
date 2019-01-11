// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_TRASH_H
#define LIBRBD_API_TRASH_H

#include "include/rbd/librbd.hpp"
#include <string>
#include <vector>

namespace librados { struct IoCtx; }

namespace librbd {

class ProgressContext;

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Trash {

  static int move(librados::IoCtx &io_ctx, rbd_trash_image_source_t source,
                  const std::string &image_name, uint64_t delay);
  static int get(librados::IoCtx &io_ctx, const std::string &id,
                 trash_image_info_t *info);
  static int list(librados::IoCtx &io_ctx,
                  std::vector<trash_image_info_t> &entries);
  static int purge(IoCtx& io_ctx, time_t expire_ts,
                   float threshold, ProgressContext& pctx);
  static int remove(librados::IoCtx &io_ctx, const std::string &image_id,
                    bool force, ProgressContext& prog_ctx);
  static int restore(librados::IoCtx &io_ctx, rbd_trash_image_source_t source,
                     const std::string &image_id,
                     const std::string &image_new_name);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Trash<librbd::ImageCtx>;

#endif // LIBRBD_API_TRASH_H
