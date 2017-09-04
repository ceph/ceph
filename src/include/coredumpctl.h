#pragma once

#include "acconfig.h"

#ifdef HAVE_SYS_PRCTL_H
#include <iostream>
#include <sys/prctl.h>
#include "common/errno.h"

class PrCtl {
  int saved_state = -1;
  static int get_dumpable() {
    int r = prctl(PR_GET_DUMPABLE);
    if (r == -1) {
      r = errno;
      std::cerr << "warning: unable to get dumpable flag: " << cpp_strerror(r)
                << std::endl;
    }
    return r;
  }
  static int set_dumpable(bool new_state) {
    int r = prctl(PR_SET_DUMPABLE, new_state);
    if (r) {
      r = -errno;
      std::cerr << "warning: unable to " << (new_state ? "set" : "unset")
                << " dumpable flag: " << cpp_strerror(r)
                << std::endl;
    }
    return r;
  }
public:
  PrCtl(int new_state = 0) {
    int r = get_dumpable();
    if (r == -1) {
      return;
    }
    if (r != new_state) {
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
#include <sys/resource.h>
#ifdef RLIMIT_CORE
#include <iostream>
#include <sys/resource.h>
#include "common/errno.h"

class PrCtl {
  rlimit saved_lim;
  static int get_dumpable(rlimit* saved) {
    int r = getrlimit(RLIMIT_CORE, saved);
    if (r) {
      r = errno;
      std::cerr << "warning: unable to getrlimit(): " << cpp_strerror(r)
                << std::endl;
    }
    return r;
  }
  static void set_dumpable(const rlimit& rlim) {
    int r = setrlimit(RLIMIT_CORE, &rlim);
    if (r) {
      r = -errno;
      std::cerr << "warning: unable to setrlimit(): " << cpp_strerror(r)
                << std::endl;
    }
  }
public:
  PrCtl(int new_state = 0) {
    int r = get_dumpable(&saved_lim);
    if (r == -1) {
      return;
    }
    rlimit new_lim;
    if (new_state) {
      new_lim.rlim_cur = saved_lim.rlim_max;
    } else {
      new_lim.rlim_cur = new_lim.rlim_max = 0;
    }
    if (new_lim.rlim_cur == saved_lim.rlim_cur) {
      return;
    }
    set_dumpable(new_lim);
  }
  ~PrCtl() {
    set_dumpable(saved_lim);
  }
};
#else
struct PrCtl {
  // to silence the Wunused-variable warning
  PrCtl() {}
};

#endif  // RLIMIT_CORE
#endif
