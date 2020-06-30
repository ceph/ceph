// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 Mirantis Inc
 *
 * Author: Mykola Golub <mgolub@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef SUB_PROCESS_H
#define SUB_PROCESS_H

#if defined(__FreeBSD__) || defined(__APPLE__)
#include <signal.h>
#endif

#ifndef _WIN32
#include <sys/wait.h>
#endif
#include <sstream>
#include <vector>

/**
 * SubProcess:
 * A helper class to spawn a subprocess.
 *
 * Example:
 *
 *   SubProcess cat("cat", SubProcess::PIPE, SubProcess::PIPE);
 *   if (cat.spawn() != 0) {
 *     std::cerr << "cat failed: " << cat.err() << std::endl;
 *     return false;
 *   }
 *   write_to_fd(cat.get_stdout(), "hello world!\n");
 *   cat.close_stdout();
 *   read_from_fd(cat.get_stdin(), buf);
 *   if (cat.join() != 0) {
 *     std::cerr << cat.err() << std::endl;
 *     return false;
 *   }
 */

class SubProcess {
public:
  enum std_fd_op{
    KEEP,
    CLOSE,
    PIPE
  };
public:
  SubProcess(const char *cmd,
             std_fd_op stdin_op = CLOSE,
             std_fd_op stdout_op = CLOSE,
             std_fd_op stderr_op = CLOSE);
  virtual ~SubProcess();

  void add_cmd_args(const char *arg, ...);
  void add_cmd_arg(const char *arg);

  virtual int spawn(); // Returns 0 on success or -errno on failure.
  virtual int join();  // Returns exit code (0 on success).

  bool is_spawned() const { return pid > 0; }

  int get_stdin() const;
  int get_stdout() const;
  int get_stderr() const;

  void close_stdin();
  void close_stdout();
  void close_stderr();

  void kill(int signo = SIGTERM) const;

  const std::string err() const;

protected:
  bool is_child() const { return pid == 0; }
  virtual void exec();

private:
  void close(int &fd);

protected:
  std::string cmd;
  std::vector<std::string> cmd_args;
  std_fd_op stdin_op;
  std_fd_op stdout_op;
  std_fd_op stderr_op;
  int stdin_pipe_out_fd;
  int stdout_pipe_in_fd;
  int stderr_pipe_in_fd;
  int pid;
  std::ostringstream errstr;
};

class SubProcessTimed : public SubProcess {
public:
  SubProcessTimed(const char *cmd, std_fd_op stdin_op = CLOSE,
		  std_fd_op stdout_op = CLOSE, std_fd_op stderr_op = CLOSE,
		  int timeout = 0, int sigkill = SIGKILL);

protected:
  void exec() override;

private:
  int timeout;
  int sigkill;
};

void timeout_sighandler(int sig);

#endif
