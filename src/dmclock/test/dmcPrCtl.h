// essentially the same as ceph's PrCtl.h, copied into the dmclock library

#include <dmtest-config.h>
#ifdef HAVE_SYS_PRCTL_H
#include <iostream>
#include <sys/prctl.h>
#include <errno.h>

struct PrCtl {
  int saved_state = -1;
  int set_dumpable(int new_state) {
    int r = prctl(PR_SET_DUMPABLE, new_state);
    if (r) {
      r = -errno;
      std::cerr << "warning: unable to " << (new_state ? "set" : "unset")
                << " dumpable flag: " << strerror(r)
                << std::endl;
    }
    return r;
  }
  PrCtl(int new_state = 0) {
    int r = prctl(PR_GET_DUMPABLE);
    if (r == -1) {
      r = errno;
      std::cerr << "warning: unable to get dumpable flag: " << strerror(r)
                << std::endl;
    } else if (r != new_state) {
      if (!set_dumpable(new_state)) {
        saved_state = r;
      }
    }
  }
  ~PrCtl() {
    if (saved_state < 0) {
      return;
    }
    set_dumpable(saved_state);
  }
};
#else
struct PrCtl {};
#endif
