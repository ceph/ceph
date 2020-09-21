// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

// Run a function in a forked child, with a timeout.

#pragma once

#include <functional>
#include <iostream>
#include <ostream>

#include <signal.h>
#ifndef _WIN32
#include <sys/wait.h>
#endif
#include <sys/types.h>

#include "include/ceph_assert.h"
#include "common/errno.h"

static void _fork_function_dummy_sighandler(int sig) {}

// Run a function post-fork, with a timeout.  Function can return
// int8_t only due to unix exit code limitations.  Returns -ETIMEDOUT
// if timeout is reached.

#ifndef _WIN32
static inline int fork_function(
  int timeout,
  std::ostream& errstr,
  std::function<int8_t(void)> f)
{
  // first fork the forker.
  pid_t forker_pid = fork();
  if (forker_pid) {
    // just wait
    int status;
    while (waitpid(forker_pid, &status, 0) == -1) {
      ceph_assert(errno == EINTR);
    }
    if (WIFSIGNALED(status)) {
      errstr << ": got signal: " << WTERMSIG(status) << "\n";
      return 128 + WTERMSIG(status);
    }
    if (WIFEXITED(status)) {
      int8_t r = WEXITSTATUS(status);
      errstr << ": exit status: " << (int)r << "\n";
      return r;
    }
    errstr << ": waitpid: unknown status returned\n";
    return -1;
  }

  // we are forker (first child)

  // close all fds
  int maxfd = sysconf(_SC_OPEN_MAX);
  if (maxfd == -1)
    maxfd = 16384;
  for (int fd = 0; fd <= maxfd; fd++) {
    if (fd == STDIN_FILENO)
      continue;
    if (fd == STDOUT_FILENO)
      continue;
    if (fd == STDERR_FILENO)
      continue;
    ::close(fd);
  }

  sigset_t mask, oldmask;
  int pid;

  // Restore default action for SIGTERM in case the parent process decided
  // to ignore it.
  if (signal(SIGTERM, SIG_DFL) == SIG_ERR) {
    std::cerr << ": signal failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }
  // Because SIGCHLD is ignored by default, setup dummy handler for it,
  // so we can mask it.
  if (signal(SIGCHLD, _fork_function_dummy_sighandler) == SIG_ERR) {
    std::cerr << ": signal failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }
  // Setup timeout handler.
  if (signal(SIGALRM, timeout_sighandler) == SIG_ERR) {
    std::cerr << ": signal failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }
  // Block interesting signals.
  sigemptyset(&mask);
  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  sigaddset(&mask, SIGCHLD);
  sigaddset(&mask, SIGALRM);
  if (sigprocmask(SIG_SETMASK, &mask, &oldmask) == -1) {
    std::cerr << ": sigprocmask failed: "
	      << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }

  pid = fork();

  if (pid == -1) {
    std::cerr << ": fork failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }

  if (pid == 0) { // we are second child
    // Restore old sigmask.
    if (sigprocmask(SIG_SETMASK, &oldmask, NULL) == -1) {
      std::cerr << ": sigprocmask failed: "
		<< cpp_strerror(errno) << "\n";
      goto fail_exit;
    }
    (void)setpgid(0, 0); // Become process group leader.
    int8_t r = f();
    _exit((uint8_t)r);
  }

  // Parent
  (void)alarm(timeout);

  for (;;) {
    int signo;
    if (sigwait(&mask, &signo) == -1) {
      std::cerr << ": sigwait failed: " << cpp_strerror(errno) << "\n";
      goto fail_exit;
    }
    switch (signo) {
    case SIGCHLD:
      int status;
      if (waitpid(pid, &status, WNOHANG) == -1) {
	std::cerr << ": waitpid failed: " << cpp_strerror(errno) << "\n";
	goto fail_exit;
      }
      if (WIFEXITED(status))
	_exit(WEXITSTATUS(status));
      if (WIFSIGNALED(status))
	_exit(128 + WTERMSIG(status));
      std::cerr << ": unknown status returned\n";
      goto fail_exit;
    case SIGINT:
    case SIGTERM:
      // Pass SIGINT and SIGTERM, which are usually used to terminate
      // a process, to the child.
      if (::kill(pid, signo) == -1) {
	std::cerr << ": kill failed: " << cpp_strerror(errno) << "\n";
	goto fail_exit;
      }
      continue;
    case SIGALRM:
      std::cerr << ": timed out (" << timeout << " sec)\n";
      if (::killpg(pid, SIGKILL) == -1) {
	std::cerr << ": kill failed: " << cpp_strerror(errno) << "\n";
	goto fail_exit;
      }
      _exit(-ETIMEDOUT);
    default:
      std::cerr << ": sigwait: invalid signal: " << signo << "\n";
      goto fail_exit;
    }
  }
  return 0;
fail_exit:
  _exit(EXIT_FAILURE);
}
#else
static inline int fork_function(
  int timeout,
  std::ostream& errstr,
  std::function<int8_t(void)> f)
{
  errstr << "Forking is not available on Windows.\n";
  return -1;
}
#endif
