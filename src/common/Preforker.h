// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_COMMON_PREFORKER_H
#define CEPH_COMMON_PREFORKER_H

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <errno.h>
#include <unistd.h>
#include "common/safe_io.h"

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

  void prefork() {
    assert(!forked);
    int r = socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
    if (r < 0) {
      cerr << "[" << getpid() << "]: unable to create socketpair: " << cpp_strerror(errno) << std::endl;
      exit(errno);
    }

    forked = true;

    childpid = fork();
    if (childpid == 0) {
      ::close(fd[0]);
    } else {
      ::close(fd[1]);
    }
  }

  bool is_child() {
    return childpid == 0;
  }

  bool is_parent() {
    return childpid != 0;
  }

  int parent_wait() {
    assert(forked);

    int r = -1;
    int err = safe_read_exact(fd[0], &r, sizeof(r));
    if (err == 0 && r == -1) {
      // daemonize
      ::close(0);
      ::close(1);
      ::close(2);
      r = 0;
    } else if (err) {
      cerr << "[" << getpid() << "]: " << cpp_strerror(-err) << std::endl;
    } else {
      // wait for child to exit
      waitpid(childpid, NULL, 0);
    }
    return r;
  }

  int exit(int r) {
    if (forked) {
      // tell parent
      (void)::write(fd[1], &r, sizeof(r));
    }
    return r;
  }

  void daemonize() {
    assert(forked);
    static int r = -1;
    (void)::write(fd[1], &r, sizeof(r));
  }
  
};

#endif
