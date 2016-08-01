#ifndef CEPH_MEMORY_H
#define CEPH_MEMORY_H

#include <memory>

namespace ceph {
  using std::shared_ptr;
  using std::weak_ptr;
  using std::unique_ptr;
  using std::static_pointer_cast;
}

#endif
