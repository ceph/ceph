// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/Request.h"

namespace librbd {
namespace operation {

Request::Request(ImageCtx &image_ctx, Context *on_finish)
  : AsyncRequest(image_ctx, on_finish) {
}

void Request::send() {
  // TODO: record op start in journal
  send_op();
}

void Request::finish(int r) {
  // TODO: record op finish in journal
}

} // namespace operation
} // namespace librbd
