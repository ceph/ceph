/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdarg.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <iomanip>

#include "common/SubProcess.h"
#include "common/errno.h"
#include "common/win32/wstring.h"
#include "include/ceph_assert.h"
#include "include/compat.h"

SubProcess::SubProcess(const char *cmd_, std_fd_op stdin_op_, std_fd_op stdout_op_, std_fd_op stderr_op_) :
  cmd(cmd_),
  cmd_args(),
  stdin_op(stdin_op_),
  stdout_op(stdout_op_),
  stderr_op(stderr_op_),
  stdin_pipe_out_fd(-1),
  stdout_pipe_in_fd(-1),
  stderr_pipe_in_fd(-1),
  pid(0),
  errstr() {
}

SubProcess::~SubProcess() {
  ceph_assert(!is_spawned());
  ceph_assert(stdin_pipe_out_fd == -1);
  ceph_assert(stdout_pipe_in_fd == -1);
  ceph_assert(stderr_pipe_in_fd == -1);
}

void SubProcess::add_cmd_args(const char *arg, ...) {
  ceph_assert(!is_spawned());

  va_list ap;
  va_start(ap, arg);
  const char *p = arg;
  do {
    add_cmd_arg(p);
    p = va_arg(ap, const char*);
  } while (p != NULL);
  va_end(ap);
}

void SubProcess::add_cmd_arg(const char *arg) {
  ceph_assert(!is_spawned());

  cmd_args.push_back(arg);
}

int SubProcess::get_stdin() const {
  ceph_assert(is_spawned());
  ceph_assert(stdin_op == PIPE);

  return stdin_pipe_out_fd;
}

int SubProcess::get_stdout() const {
  ceph_assert(is_spawned());
  ceph_assert(stdout_op == PIPE);

  return stdout_pipe_in_fd;
}

int SubProcess::get_stderr() const {
  ceph_assert(is_spawned());
  ceph_assert(stderr_op == PIPE);

  return stderr_pipe_in_fd;
}

void SubProcess::close(int &fd) {
  if (fd == -1)
    return;

  ::close(fd);
  fd = -1;
}

void SubProcess::close_stdin() {
  ceph_assert(is_spawned());
  ceph_assert(stdin_op == PIPE);

  close(stdin_pipe_out_fd);
}

void SubProcess::close_stdout() {
  ceph_assert(is_spawned());
  ceph_assert(stdout_op == PIPE);

  close(stdout_pipe_in_fd);
}

void SubProcess::close_stderr() {
  ceph_assert(is_spawned());
  ceph_assert(stderr_op == PIPE);

  close(stderr_pipe_in_fd);
}

const std::string SubProcess::err() const {
  return errstr.str();
}

SubProcessTimed::SubProcessTimed(const char *cmd, std_fd_op stdin_op,
                 std_fd_op stdout_op, std_fd_op stderr_op,
                 int timeout_, int sigkill_) :
  SubProcess(cmd, stdin_op, stdout_op, stderr_op),
  timeout(timeout_),
  sigkill(sigkill_) {
}

static bool timedout = false;
void timeout_sighandler(int sig) {
  timedout = true;
}

void SubProcess::close_h(HANDLE &handle) {
  if (handle == INVALID_HANDLE_VALUE)
    return;

  CloseHandle(handle);
  handle = INVALID_HANDLE_VALUE;
}

int SubProcess::join() {
  ceph_assert(is_spawned());

  close(stdin_pipe_out_fd);
  close(stdout_pipe_in_fd);
  close(stderr_pipe_in_fd);

  int status = 0;

  if (WaitForSingleObject(proc_handle, INFINITE) != WAIT_FAILED) {
    if (!GetExitCodeProcess(proc_handle, (DWORD*)&status)) {
      errstr << cmd << ": Could not get exit code: " << pid
             << ". Error code: " << GetLastError();
      status = -ECHILD;
    } else if (status) {
      errstr << cmd << ": exit status: " << status;
    }
  } else {
    errstr << cmd << ": Waiting for child process failed: " << pid
           << ". Error code: " << GetLastError();
    status = -ECHILD;
  }

  close_h(proc_handle);
  pid = 0;
  return status;
}

void SubProcess::kill(int signo) const {
  ceph_assert(is_spawned());
  ceph_assert(TerminateProcess(proc_handle, 128 + SIGTERM));
}

int SubProcess::spawn() {
  std::ostringstream cmdline;
  cmdline << cmd;
  for (auto& arg : cmd_args) {
    cmdline << " " << std::quoted(arg);
  }
  std::wstring cmdline_w = to_wstring(cmdline.str());

  STARTUPINFOW si = {0};
  PROCESS_INFORMATION pi = {0};
  SECURITY_ATTRIBUTES sa = {0};

  sa.nLength = sizeof(SECURITY_ATTRIBUTES);
  sa.bInheritHandle = TRUE;
  sa.lpSecurityDescriptor = NULL;

  HANDLE stdin_r = INVALID_HANDLE_VALUE, stdin_w = INVALID_HANDLE_VALUE,
         stdout_r = INVALID_HANDLE_VALUE, stdout_w = INVALID_HANDLE_VALUE,
         stderr_r = INVALID_HANDLE_VALUE, stderr_w = INVALID_HANDLE_VALUE;

  if ((stdin_op == PIPE && !CreatePipe(&stdin_r, &stdin_w, &sa, 0)) ||
      (stdout_op == PIPE && !CreatePipe(&stdout_r, &stdout_w, &sa, 0)) ||
      (stderr_op == PIPE && !CreatePipe(&stderr_r, &stderr_w, &sa, 0))) {
    errstr << cmd << ": CreatePipe failed: " << GetLastError();
    return -1;
  }

  // The following handles will be used by the parent process and
  // must be marked as non-inheritable.
  if ((stdin_op == PIPE && !SetHandleInformation(stdin_w, HANDLE_FLAG_INHERIT, 0)) ||
      (stdout_op == PIPE && !SetHandleInformation(stdout_r, HANDLE_FLAG_INHERIT, 0)) ||
      (stderr_op == PIPE && !SetHandleInformation(stderr_r, HANDLE_FLAG_INHERIT, 0))) {
    errstr << cmd << ": SetHandleInformation failed: "
           << GetLastError();
    goto fail;
  }

  si.cb = sizeof(STARTUPINFO);
  si.hStdInput = stdin_op == KEEP ? GetStdHandle(STD_INPUT_HANDLE) : stdin_r;
  si.hStdOutput = stdout_op == KEEP ? GetStdHandle(STD_OUTPUT_HANDLE) : stdout_w;
  si.hStdError = stderr_op == KEEP ? GetStdHandle(STD_ERROR_HANDLE) : stderr_w;
  si.dwFlags |= STARTF_USESTDHANDLES;

  stdin_pipe_out_fd = stdin_op == PIPE ? _open_osfhandle((intptr_t)stdin_w, 0) : -1;
  stdout_pipe_in_fd = stdout_op == PIPE ? _open_osfhandle((intptr_t)stdout_r, _O_RDONLY) : - 1;
  stderr_pipe_in_fd = stderr_op == PIPE ? _open_osfhandle((intptr_t)stderr_r, _O_RDONLY) : -1;

  if (stdin_op == PIPE && stdin_pipe_out_fd == -1 ||
      stdout_op == PIPE && stdout_pipe_in_fd == -1 ||
      stderr_op == PIPE && stderr_pipe_in_fd == -1) {
    errstr << cmd << ": _open_osfhandle failed: " << GetLastError();
    goto fail;
  }

  // We've transfered ownership from those handles.
  stdin_w = stdout_r = stderr_r = INVALID_HANDLE_VALUE;

  if (!CreateProcessW(
      NULL, const_cast<wchar_t*>(cmdline_w.c_str()),
      NULL, NULL, /* No special security attributes */
      1, /* Inherit handles marked as inheritable */
      0, /* No special flags */
      NULL, /* Use the same environment variables */
      NULL, /* use the same cwd */
      &si, &pi)) {
    errstr << cmd << ": CreateProcess failed: " << GetLastError();
    goto fail;
  }

  proc_handle = pi.hProcess;
  pid = GetProcessId(proc_handle);
  if (!pid) {
    errstr << cmd << ": Could not get child process id.";
    goto fail;
  }

  // The following are used by the subprocess.
  CloseHandle(stdin_r);
  CloseHandle(stdout_w);
  CloseHandle(stderr_w);
  CloseHandle(pi.hThread);
  return 0;

fail:
  // fd copies
  close(stdin_pipe_out_fd);
  close(stdout_pipe_in_fd);
  close(stderr_pipe_in_fd);

  // the original handles
  close_h(stdin_r);
  close_h(stdin_w);
  close_h(stdout_r);
  close_h(stdout_w);
  close_h(stderr_r);
  close_h(stderr_w);

  // We may consider mapping some of the Windows errors.
  return -1;
}

void SubProcess::exec() {
}

int SubProcessTimed::spawn() {
  if (auto ret = SubProcess::spawn(); ret < 0) {
    return ret;
  }

  if (timeout > 0) {
    waiter = std::thread([&](){
      DWORD wait_status = WaitForSingleObject(proc_handle, timeout * 1000);
      ceph_assert(wait_status != WAIT_FAILED);
      if (wait_status == WAIT_TIMEOUT) {
        // 128 + sigkill is just the return code, which is expected by
        // the unit tests and possibly by other code. We can't pick a
        // termination signal unless we use window events.
        ceph_assert(TerminateProcess(proc_handle, 128 + sigkill));
        timedout = 1;
      }
    });
  }
  return 0;
}

int SubProcessTimed::join() {
  ceph_assert(is_spawned());

  if (waiter.joinable()) {
    waiter.join();
  }

  return SubProcess::join();;
}

void SubProcessTimed::exec() {
}
