// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#include <unistd.h>

#include <iostream>

#include "common/SubProcess.h"
#include "common/safe_io.h"
#include "gtest/gtest.h"

bool read_from_fd(int fd, std::string &out) {
  out.clear();
  char buf[1024];
  ssize_t n = safe_read(fd, buf, sizeof(buf) - 1);
  if (n < 0)
    return false;
  buf[n] = '\0';
  out = buf;
  return true;
}

TEST(SubProcess, True)
{
  SubProcess p("true");
  ASSERT_EQ(p.spawn(), 0);
  ASSERT_EQ(p.join(), 0);
  ASSERT_TRUE(p.err()[0] == '\0');
}

TEST(SubProcess, False)
{
  SubProcess p("false");
  ASSERT_EQ(p.spawn(), 0);
  ASSERT_EQ(p.join(), 1);
  ASSERT_FALSE(p.err()[0] == '\0');
}

TEST(SubProcess, NotFound)
{
  SubProcess p("NOTEXISTENTBINARY", false, false, true);
  ASSERT_EQ(p.spawn(), 0);
  std::string buf;
  ASSERT_TRUE(read_from_fd(p.stderr(), buf));
  std::cerr << "stderr: " << buf;
  ASSERT_EQ(p.join(), 1);
  std::cerr << "err: " << p.err() << std::endl;
  ASSERT_FALSE(p.err()[0] == '\0');
}

TEST(SubProcess, Echo)
{
  SubProcess echo("echo", false, true);
  echo.add_cmd_args("1", "2", "3", NULL);

  ASSERT_EQ(echo.spawn(), 0);
  std::string buf;
  ASSERT_TRUE(read_from_fd(echo.stdout(), buf));
  std::cerr << "stdout: " << buf;
  ASSERT_EQ(buf, "1 2 3\n");
  ASSERT_EQ(echo.join(), 0);
  ASSERT_TRUE(echo.err()[0] == '\0');
}

TEST(SubProcess, Cat)
{
  SubProcess cat("cat", true, true, true);

  ASSERT_EQ(cat.spawn(), 0);
  std::string msg("to my, trociny!");
  int n = write(cat.stdin(), msg.c_str(), msg.size());
  ASSERT_EQ(n, (int)msg.size());
  cat.close_stdin();
  std::string buf;
  ASSERT_TRUE(read_from_fd(cat.stdout(), buf));
  std::cerr << "stdout: " << buf << std::endl;
  ASSERT_EQ(buf, msg);
  ASSERT_TRUE(read_from_fd(cat.stderr(), buf));
  ASSERT_EQ(buf, "");
  ASSERT_EQ(cat.join(), 0);
  ASSERT_TRUE(cat.err()[0] == '\0');
}

TEST(SubProcess, CatDevNull)
{
  SubProcess cat("cat", true, true, true);
  cat.add_cmd_arg("/dev/null");

  ASSERT_EQ(cat.spawn(), 0);
  std::string buf;
  ASSERT_TRUE(read_from_fd(cat.stdout(), buf));
  ASSERT_EQ(buf, "");
  ASSERT_TRUE(read_from_fd(cat.stderr(), buf));
  ASSERT_EQ(buf, "");
  ASSERT_EQ(cat.join(), 0);
  ASSERT_TRUE(cat.err()[0] == '\0');
}

TEST(SubProcess, Killed)
{
  SubProcessTimed cat("cat", true, true);

  ASSERT_EQ(cat.spawn(), 0);
  cat.kill();
  ASSERT_EQ(cat.join(), 128 + SIGTERM);
  std::cerr << "err: " << cat.err() << std::endl;
  ASSERT_FALSE(cat.err()[0] == '\0');
}

TEST(SubProcess, CatWithArgs)
{
  SubProcess cat("cat", true, true, true);
  cat.add_cmd_args("/dev/stdin", "/dev/null", "/NOTEXIST", NULL);

  ASSERT_EQ(cat.spawn(), 0);
  std::string msg("Hello, Word!");
  int n = write(cat.stdin(), msg.c_str(), msg.size());
  ASSERT_EQ(n, (int)msg.size());
  cat.close_stdin();
  std::string buf;
  ASSERT_TRUE(read_from_fd(cat.stdout(), buf));
  std::cerr << "stdout: " << buf << std::endl;
  ASSERT_EQ(buf, msg);
  ASSERT_TRUE(read_from_fd(cat.stderr(), buf));
  std::cerr << "stderr: " << buf;
  ASSERT_FALSE(buf.empty());
  ASSERT_EQ(cat.join(), 1);
  std::cerr << "err: " << cat.err() << std::endl;
  ASSERT_FALSE(cat.err()[0] == '\0');
}

TEST(SubProcess, Subshell)
{
  SubProcess sh("/bin/sh", true, true, true);
  sh.add_cmd_args("-c",
      "sleep 0; "
      "cat; "
      "echo 'error from subshell' >&2; "
      "/bin/sh -c 'exit 13'", NULL);
  ASSERT_EQ(sh.spawn(), 0);
  std::string msg("hello via subshell");
  int n = write(sh.stdin(), msg.c_str(), msg.size());
  ASSERT_EQ(n, (int)msg.size());
  sh.close_stdin();
  std::string buf;
  ASSERT_TRUE(read_from_fd(sh.stdout(), buf));
  std::cerr << "stdout: " << buf << std::endl;
  ASSERT_EQ(buf, msg);
  ASSERT_TRUE(read_from_fd(sh.stderr(), buf));
  std::cerr << "stderr: " << buf;
  ASSERT_EQ(buf, "error from subshell\n");
  ASSERT_EQ(sh.join(), 13);
  std::cerr << "err: " << sh.err() << std::endl;
  ASSERT_FALSE(sh.err()[0] == '\0');
}
