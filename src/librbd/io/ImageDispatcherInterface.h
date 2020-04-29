// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_DISPATCHER_INTERFACE_H
#define CEPH_LIBRBD_IO_IMAGE_DISPATCHER_INTERFACE_H

#include "include/int_types.h"
#include "librbd/io/DispatcherInterface.h"
#include "librbd/io/ImageDispatchInterface.h"

struct Context;

namespace librbd {
namespace io {

struct ImageDispatcherInterface
  : public DispatcherInterface<ImageDispatchInterface> {
public:
};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_IMAGE_DISPATCHER_INTERFACE_H
