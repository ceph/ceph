#include "config.h"
#include "common/run_cmd.h"

#include "gtest/gtest.h"

#include <stdlib.h>
#include <unistd.h>

TEST(RunCommand, StringSimple)
{
  char temp_file_name[] = "run_cmd_temp_file_XXXXXX";

  int fd = ::mkstemps(temp_file_name, 0);
  ASSERT_GE(fd, 0);
  ::close(fd);

  int ret = run_cmd("touch", temp_file_name, NULL);
  ASSERT_EQ(ret, 0);

  ASSERT_EQ(access(temp_file_name, R_OK), 0);

  ret = run_cmd("rm", "-f", temp_file_name, NULL);
  ASSERT_EQ(ret, 0);

  ASSERT_NE(access(temp_file_name, R_OK), 0);
}
