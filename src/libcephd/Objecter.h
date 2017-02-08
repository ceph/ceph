// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBOSD_OBJECTER_H
#define CEPH_LIBOSD_OBJECTER_H

#include <memory>

#include "ceph_osd.h"
#include "include/types.h" // epoch_t

namespace ceph
{
namespace osd
{
class Dispatcher;

class Objecter {
 private:
  int read_sync(const char *object, const uint8_t volume[16],
                uint64_t offset, uint64_t length, char *data, int flags);
  int write_sync(const char *object, const uint8_t volume[16],
                 uint64_t offset, uint64_t length, char *data, int flags);
  int truncate_sync(const char *object, int64_t pool_id, OSDMapRef &omap,
                    uint64_t offset, int flags);

 public:
  std::unique_ptr<Dispatcher> dispatcher;

  virtual ~Objecter() {}

  virtual bool wait_for_active(epoch_t *epoch) = 0;

  int read(const char *object, const uint8_t volume[16],
	   uint64_t offset, uint64_t length, char *data,
	   int flags, libosd_io_completion_fn cb, void *user);
  int write(const char *object, const uint8_t volume[16],
	    uint64_t offset, uint64_t length, char *data,
	    int flags, libosd_io_completion_fn cb, void *user);
  int truncate(const char *object, int64_t pool_id, OSDMapRef &omap, uint64_t offset,
	       int flags, libosd_io_completion_fn cb, void *user);
};

} // namespace osd
} // namespace ceph

#endif // CEPH_LIBOSD_OBJECTER_H
