#include "common/config.h"
#include "common/run_cmd.h"

#include "gtest/gtest.h"

#include <stdlib.h>
#include <unistd.h>

TEST(RunCommand, StringSimple)
{
  char temp_file_name[] = "run_cmd_temp_file_XXXXXX";

  int fd = ::mkstemp(temp_file_name);
  ASSERT_GE(fd, 0);
  ::close(fd);

  std::string ret = run_cmd("touch", temp_file_name, (char*)NULL);
  ASSERT_EQ(ret, "");

  ASSERT_EQ(access(temp_file_name, R_OK), 0);

  ret = run_cmd("rm", "-f", temp_file_name, (char*)NULL);
  ASSERT_EQ(ret, "");

  ASSERT_NE(access(temp_file_name, R_OK), 0);
}
