// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_COMMON_PREFORKER_H
#define CEPH_COMMON_PREFORKER_H

#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sstream>

#include "common/errno.h"
#include "common/safe_io.h"
#include "include/ceph_assert.h"
#include "include/compat.h"
#include "include/sock_compat.h"

/**
 * pre-fork fork/daemonize helper class
 *
 * Hide the details of letting a process fork early, do a bunch of
 * initialization work that may spam stdout or exit with an error, and
 * then daemonize.  The exit() method will either exit directly (if we
 * haven't forked) or pass a message to the parent with the error if
 * we have.
 */
class Preforker {
  pid_t childpid;
  bool forked;
  int fd[2];  // parent's, child's

public:
  Preforker()
    : childpid(0),
      forked(false)
  {}

  int prefork(std::string &err) {
    ceph_assert(!forked);
    std::ostringstream oss;
    int r = socketpair_cloexec(AF_UNIX, SOCK_STREAM, 0, fd);
    if (r < 0) {
      int e = errno;
      oss << "[" << getpid() << "]: unable to create socketpair: " << cpp_strerror(e);
      err = oss.str();
      return (errno = e, -1);
    }

    struct sigaction sa;
    sa.sa_handler = SIG_IGN;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGHUP, &sa, nullptr) != 0) {
      int e = errno;
      oss << "[" << getpid() << "]: unable to ignore SIGHUP: " << cpp_strerror(e);
      err = oss.str();
      return (errno = e, -1);
    }

    forked = true;

    childpid = fork();
    if (childpid < 0) {
      int e = errno;
      oss << "[" << getpid() << "]: unable to fork: " << cpp_strerror(e);
      err = oss.str();
      return (errno = e, -1);
    }
    if (is_child()) {
      ::close(fd[0]);
    } else {
      ::close(fd[1]);
    }
    return 0;
  }

  int get_signal_fd() const {
    return forked ? fd[1] : 0;
  }

  bool is_child() {
    return childpid == 0;
  }

  bool is_parent() {
    return childpid != 0;
  }

  int parent_wait(std::string &err_msg) {
    ceph_assert(forked);

    int r = -1;
    std::ostringstream oss;
    int err = safe_read_exact(fd[0], &r, sizeof(r));
    if (err == 0 && r == -1) {
      // daemonize
      ::close(0);
      ::close(1);
      ::close(2);
    } else if (err) {
      oss << "[" << getpid() << "]: " << cpp_strerror(err);
    } else {
      // wait for child to exit
      int status;
      err = waitpid(childpid, &status, 0);
      if (err < 0) {
        oss << "[" << getpid() << "]" << " waitpid error: " << cpp_strerror(err);
      } else if (WIFSIGNALED(status)) {
        oss << "[" << getpid() << "]" << " exited with a signal";
      } else if (!WIFEXITED(status)) {
        oss << "[" << getpid() << "]" << " did not exit normally";
      } else {
        err = WEXITSTATUS(status);
        if (err != 0)
         oss << "[" << getpid() << "]" << " returned exit_status " << cpp_strerror(err);
      }
    }
    err_msg = oss.str();
    return err;
  }

  int signal_exit(int r) {
    if (forked) {
      /* If we get an error here, it's too late to do anything reasonable about it. */
      [[maybe_unused]] auto n = safe_write(fd[1], &r, sizeof(r));
    }
    return r;
  }
  void exit(int r) {
    if (is_child())
        signal_exit(r);
    ::exit(r);
  }

  void daemonize() {
    ceph_assert(forked);
    static int r = -1;
    int r2 = ::write(fd[1], &r, sizeof(r));
    r += r2;  // make the compiler shut up about the unused return code from ::write(2).
  }
  
};

#endif
