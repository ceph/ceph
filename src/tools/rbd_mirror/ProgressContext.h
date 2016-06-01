// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_PROGRESS_CONTEXT_H
#define RBD_MIRROR_PROGRESS_CONTEXT_H

namespace rbd {
namespace mirror {

class ProgressContext
{
public:
  virtual ~ProgressContext() {}
  virtual void update_progress(const std::string &description,
			       bool flush = true) = 0;
};

} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_PROGRESS_CONTEXT_H
