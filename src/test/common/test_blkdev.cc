// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include "include/types.h"
#include "common/blkdev.h"

#include "gtest/gtest.h"
#include <iostream>

  using namespace std;

TEST(blkdev, get_block_device_base) {
  char buf[PATH_MAX*2];
  char buf3[PATH_MAX*2];

  ASSERT_EQ(-EINVAL, get_block_device_base("/etc/notindev", buf, 100));

  for (int i=0; i<2; ++i) {
    string root;
    if (i == 0) {
      const char* env = getenv("CEPH_ROOT");
      ASSERT_NE(env, nullptr) << "Environment Variable CEPH_ROOT not found!";
      root = string(env) + "/src/test/common/test_blkdev_sys_block";
    }
    set_block_device_sandbox_dir(root.c_str());

    // work backwards
    sprintf(buf, "%s/sys/block", root.c_str());
    DIR *dir = opendir(buf);
    ASSERT_NE(dir, nullptr);
    struct dirent *de = nullptr;
    while ((de = ::readdir(dir))) {
      if (de->d_name[0] == '.')
	continue;

      char base[PATH_MAX];
      sprintf(base, "/dev/%s", de->d_name);
      printf("base %s (%s)\n", base, de->d_name);
      for (char *p = base; *p; ++p)
	if (*p == '!')
	  *p = '/';

      ASSERT_EQ(-ERANGE, get_block_device_base(base, buf3, 1));
      ASSERT_EQ(0, get_block_device_base(base, buf3, sizeof(buf3)));
      printf("  got '%s' expected '%s'\n", buf3, de->d_name);
      ASSERT_EQ(0, strcmp(de->d_name, buf3));
      printf("  discard granularity = %lld .. supported = %d\n",
	     (long long)get_block_device_int_property(base, "queue/discard_granularity"),
	     (int)block_device_support_discard(base));

      char subdirfn[PATH_MAX];
      sprintf(subdirfn, "%s/sys/block/%s", root.c_str(), de->d_name);
      DIR *subdir = opendir(subdirfn);
      ASSERT_TRUE(subdir);
      struct dirent *de2 = nullptr;
      while ((de2 = ::readdir(subdir))) {
	if (de2->d_name[0] == '.')
	  continue;
	// partiions will be prefixed with the base name
	if (strncmp(de2->d_name, de->d_name, strlen(de->d_name))) {
	  //printf("skipping %s\n", de2->d_name);
	  continue;
	}
	char part[PATH_MAX];
	sprintf(part, "/dev/%s", de2->d_name);
	for (char *p = part; *p; ++p)
	  if (*p == '!')
	    *p = '/';
	printf(" part %s (%s %s)\n", part, de->d_name, de2->d_name);

	ASSERT_EQ(0, get_block_device_base(part, buf3, sizeof(buf3)));
	printf("  got '%s' expected '%s'\n", buf3, de->d_name);
	ASSERT_EQ(0, strcmp(buf3, de->d_name));
	printf("  discard granularity = %lld .. supported = %d\n",
	       (long long)get_block_device_int_property(part, "queue/discard_granularity"),
	       (int)block_device_support_discard(part));
      }

      closedir(subdir);
    }
    closedir(dir);
  }
}

TEST(blkdev, device_model)
{
  const char* env = getenv("CEPH_ROOT");
  ASSERT_NE(env, nullptr) << "Environment Variable CEPH_ROOT not found!";
  string root = string(env) + "/src/test/common/test_blkdev_sys_block";
  set_block_device_sandbox_dir(root.c_str());

  char model[1000] = {0};
  int rc = block_device_model("sda", model, sizeof(model));
  ASSERT_EQ(0, rc);

  printf("model '%s'\n", model);
  ASSERT_EQ(strcmp(model, "myfancymodel"), 0);
}

TEST(blkdev, get_block_device_string_property)
{
  const char* env = getenv("CEPH_ROOT");
  ASSERT_NE(env, nullptr) << "Environment Variable CEPH_ROOT not found!";
  string root = string(env) + "/src/test/common/test_blkdev_sys_block";
  set_block_device_sandbox_dir(root.c_str());

  char val[1000] = {0};
  int rc = get_block_device_string_property("sda", "device/model",
					    val, sizeof(val));
  ASSERT_EQ(0, rc);
  printf("val '%s'\n", val);
  ASSERT_EQ(strcmp(val, "myfancymodel"), 0);
}

TEST(blkdev, is_rotational)
{
  const char* env = getenv("CEPH_ROOT");
  ASSERT_NE(env, nullptr) << "Environment Variable CEPH_ROOT not found!";
  string root = string(env) + "/src/test/common/test_blkdev_sys_block";
  set_block_device_sandbox_dir(root.c_str());

  ASSERT_FALSE(block_device_is_rotational("sda"));
  ASSERT_TRUE(block_device_is_rotational("sdb"));
}


