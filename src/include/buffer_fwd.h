#ifndef BUFFER_FWD_H
#define BUFFER_FWD_H

namespace ceph {
  namespace buffer {
    inline namespace v15_2_0 {
      class ptr;
      class list;
    }
    class hash;
  }

  using bufferptr = buffer::ptr;
  using bufferlist = buffer::list;
  using bufferhash = buffer::hash;
}

#endif

