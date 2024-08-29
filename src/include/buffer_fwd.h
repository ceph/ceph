#ifndef BUFFER_FWD_H
#define BUFFER_FWD_H

namespace ceph {
  namespace buffer {
    inline namespace v15_2_0 {
      class ptr;
      class ptr_rw;
      class list;
    }
    class hash;
  }

  using bufferptr = buffer::ptr;
  using bufferptr_rw = buffer::ptr_rw;
  using bufferlist = buffer::list;
  using bufferhash = buffer::hash;
}

#endif

