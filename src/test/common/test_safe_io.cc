// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include "common/safe_io.h"

#include "gtest/gtest.h"


TEST(SafeIO, safe_read_file) {
  const char *fname = "safe_read_testfile";
  ::unlink(fname);
  int fd = ::open(fname, O_RDWR|O_CREAT|O_TRUNC, 0600);
  ASSERT_NE(fd, -1);
  const char buf[] = "0123456789";
  for (int i = 0; i < 8; i++) {
    ASSERT_EQ((ssize_t)sizeof(buf), write(fd, buf, sizeof(buf)));
  }
  ::close(fd);
  char rdata[80];
  ASSERT_EQ((int)sizeof(rdata),
	    safe_read_file(".", fname, rdata, sizeof(rdata)));
  for (char *p = rdata, *end = rdata+sizeof(rdata); p < end; p+=sizeof(buf)) {
    ASSERT_EQ(0, std::memcmp(p, buf, std::min(size_t(end-p), sizeof(buf))));
  }
  ::unlink(fname);
}

// Local Variables:
// compile-command: "cd ../.. ;
//   make unittest_safe_io &&
//   ./unittest_safe_io"
// End:
