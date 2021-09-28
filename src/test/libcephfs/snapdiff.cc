// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "include/stat.h"
#include "include/ceph_assert.h"
#include "include/object.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <algorithm>
#include <limits.h>
#include <dirent.h>

using namespace std;
class TestMount {
  ceph_mount_info* cmount = nullptr;
  char dir_path[64];

public:
  TestMount( const char* root_dir_name = "dir0") {
    ceph_create(&cmount, NULL);
    ceph_conf_read_file(cmount, NULL);
    ceph_conf_parse_env(cmount, NULL);
    ceph_assert(0 == ceph_mount(cmount, NULL));

    sprintf(dir_path, "/%s_%d", root_dir_name, getpid());
    ceph_assert(0 == ceph_mkdir(cmount, dir_path, 0777));
  }
  ~TestMount()
  {
    if (cmount) {
      ceph_assert(0 == purge_dir(""));
    }
    ceph_rmdir(cmount, dir_path);
    ceph_shutdown(cmount);
  }

  string make_file_path(const char* relpath) {
    char path[PATH_MAX];
    sprintf(path, "%s/%s", dir_path, relpath);
    return path;
  }

  string make_snap_name(const char* name) {
    char snap_name[64];
    sprintf(snap_name, "%s_%d", name, getpid());
    return snap_name;
  }
  string make_snap_path(const char* sname, const char* subdir = nullptr) {
    char snap_path[PATH_MAX];
    string snap_name = subdir ?
      concat_path(make_snap_name(sname), subdir) :
      make_snap_name(sname);
    sprintf(snap_path, ".snap/%s", snap_name.c_str());
    return snap_path;
  }
  string make_snapdiff_relpath(const char* name1, const char* name2,
    const char* relpath = nullptr) {
    char diff_path[PATH_MAX];
    string snap_name1 = make_snap_name(name1);
    string snap_name2 = make_snap_name(name2);
    if (relpath) {
      sprintf(diff_path, ".snap/.~diff=%s.~diff=%s/%s",
        snap_name1.c_str(), snap_name2.c_str(), relpath);
    } else {
      sprintf(diff_path, ".snap/.~diff=%s.~diff=%s",
        snap_name1.c_str(), snap_name2.c_str());
    }
    return diff_path;
  }

  int mksnap(const char* name) {
    string snap_name = make_snap_name(name);
    return ceph_mksnap(cmount, dir_path, snap_name.c_str(),
      0755, nullptr, 0);
  }
  int rmsnap(const char* name) {
    string snap_name = make_snap_name(name);
    return ceph_rmsnap(cmount, dir_path, snap_name.c_str());
  }
  int get_snapid(const char* name, uint64_t* res)
  {
    ceph_assert(res);
    snap_info snap_info;

    char snap_path[PATH_MAX];
    string snap_name = make_snap_name(name);
    sprintf(snap_path, "%s/.snap/%s", dir_path, snap_name.c_str());
    int r = ceph_get_snap_info(cmount, snap_path, &snap_info);
    if (r >= 0) {
      *res = snap_info.id;
      r = 0;
    }
    return r;
  }

  int write_full(const char* relpath, const string& data)
  {
    auto file_path = make_file_path(relpath);
    int fd = ceph_open(cmount, file_path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (fd < 0) {
      return -EACCES;
    }
    int r = ceph_write(cmount, fd, data.c_str(), data.size(), 0);
    if (r >= 0) {
      ceph_fsync(cmount, fd, 0);
    }
    ceph_close(cmount, fd);
    return r;
  }
  string concat_path(string_view path, string_view name) {
    string s(path);
    if (s.empty() || s.back() != '/') {
      s += '/';
    }
    s += name;
    return s;
  }
  int readfull_and_compare(string_view path,
                           string_view name,
    const string_view expected)
  {
    string s = concat_path(path, name);
    return readfull_and_compare(s.c_str(), expected);
  }
  int readfull_and_compare(const char* relpath,
    const string_view expected)
  {
    auto file_path = make_file_path(relpath);
    int fd = ceph_open(cmount, file_path.c_str(), O_RDONLY, 0);
    if (fd < 0) {
      return -EACCES;
    }
    std::string s;
    s.resize(expected.length() + 1);

    int ret = ceph_read(cmount, fd, s.data(), s.length(), 0);
    ceph_close(cmount, fd);

    if (ret < 0) {
      return -EIO;
    }
    if (ret != int(expected.length())) {
      return -ERANGE;
    }
    s.resize(ret);
    if (s != expected) {
      return -EINVAL;
    }
    return 0;
  }
  int unlink(const char* relpath)
  {
    auto file_path = make_file_path(relpath);
    return ceph_unlink(cmount, file_path.c_str());
  }

  int for_each_readdir(const char* relpath,
    std::function<bool(const dirent* dire)> fn)
  {
    auto subdir_path = make_file_path(relpath);
    struct ceph_dir_result* ls_dir;
    int r = ceph_opendir(cmount, subdir_path.c_str(), &ls_dir);
    if (r != 0) {
      return r;
    }
    struct dirent* result;
    while( nullptr != (result = ceph_readdir(cmount, ls_dir))) {
      if (strcmp(result->d_name, ".") == 0 ||
          strcmp(result->d_name, "..") == 0) {
        continue;
      }
      if (!fn(result)) {
        r = -EINTR;
        break;
      }
    }
    ceph_assert(0 == ceph_closedir(cmount, ls_dir));
    return r;
  }
  int readdir_and_compare(const char* relpath,
    const vector<string>& expected0)
  {
    vector<string> expected(expected0);
    auto end = expected.end();
    int r = for_each_readdir(relpath,
      [&](const dirent* dire) {

        std::string name(dire->d_name);
        auto it = std::find(expected.begin(), end, name);
        if (it == end) {
          return false;
        }
        expected.erase(it);
        return true;
      });
    if (r == 0 && !expected.empty()) {
      r = -ENOTEMPTY;
    }
    return r;
  }
  int for_each_readdir_snapdiff(const char* snap1_relpath,
    uint64_t snap_other,
    std::function<bool(const dirent*, uint64_t)> fn)
  {
    auto snap1_path = make_file_path(snap1_relpath);
    struct ceph_dir_result* ls_dir;
    int r = ceph_opendir(cmount, snap1_path.c_str(), &ls_dir);
    if (r != 0) {
    std::cout << snap1_path << " failed to open, ret:" << r << std::endl;
      return r;
    }
    dirent res_de;
    uint64_t res_snapid;
    while (0 < (r = ceph_readdir_snapdiff(cmount,
                                          ls_dir,
                                          snap_other,
                                          &res_de,
                                          &res_snapid))) {
      if (strcmp(res_de.d_name, ".") == 0 ||
        strcmp(res_de.d_name, "..") == 0) {
        continue;
      }
      if (!fn(&res_de, res_snapid)) {
        r = -EINTR;
        break;
      }
    }
    ceph_assert(0 == ceph_closedir(cmount, ls_dir));
    return r;
  }
  int readdir_snapdiff_and_compare(const char* snap1_relpath,
    uint64_t snap_other,
    const vector<pair<string, uint64_t>>& expected0)
  {
    vector<pair<string, uint64_t>> expected(expected0);
    auto end = expected.end();
    int r = for_each_readdir_snapdiff(snap1_relpath,
      snap_other,
      [&](const dirent* dire, uint64_t snapid) {

        pair<string, uint64_t> p = std::make_pair(dire->d_name, snapid);
        auto it = std::find(expected.begin(), end, p);
        if (it == end) {
          return false;
        }
        expected.erase(it);
        return true;
      });
    if (r == 0 && !expected.empty()) {
      r = -ENOTEMPTY;
    }
    return r;
  }

  int mkdir(const char* relpath)
  {
    auto path = make_file_path(relpath);
    return ceph_mkdir(cmount, path.c_str(), 0777);
  }
  int rmdir(const char* relpath)
  {
    auto path = make_file_path(relpath);
    return ceph_rmdir(cmount, path.c_str());
  }
  int purge_dir(const char* relpath0, bool inclusive = true)
  {
    int r =
      for_each_readdir(relpath0,
        [&] (const dirent* dire) {
          string relpath = concat_path(relpath0, dire->d_name);
          if (dire->d_type == DT_REG) {
            unlink(relpath.c_str());
          } else if (dire->d_type == DT_DIR) {
            purge_dir(relpath.c_str());
            rmdir(relpath.c_str());
          }
          return true;
        });
    if (r != 0) {
      return r;
    }
    if (*relpath0 != 0) {
      r = rmdir(relpath0);
    }
    return r;
  }

  void remove_all() {
    purge_dir("/", false);
  }

  ceph_mount_info* get_cmount() {
    return cmount;
  }
};

TEST(LibCephFS, SnapDiffSimple)
{
  TestMount test_mount;

  ASSERT_LT(0, test_mount.write_full("fileA", "hello world"));
  ASSERT_LT(0, test_mount.write_full("fileC", "hello world in another file"));
  ASSERT_LT(0, test_mount.write_full("fileD", "hello world unmodified"));

  ASSERT_EQ(0, test_mount.mksnap("snap1"));

  std::cout << "---------snap1 listing---------" << std::endl;
  ASSERT_EQ(0, test_mount.for_each_readdir("/",
    [&](const dirent* dire) {
      std::cout << dire->d_name<< std::endl;
      return true;
    }));
  {
    vector<string> expected;
    expected.push_back("fileA");
    expected.push_back("fileC");
    expected.push_back("fileD");
    ASSERT_EQ(0, test_mount.readdir_and_compare("/", expected));
  }
  ASSERT_EQ(0, test_mount.readfull_and_compare("fileA", "hello world"));
  ASSERT_EQ(-ERANGE, test_mount.readfull_and_compare("fileC", "hello world"));

  ASSERT_LT(0, test_mount.write_full("fileA", "hello world again"));
  ASSERT_LT(0, test_mount.write_full("fileB", "hello world again in B"));
  ASSERT_EQ(0, test_mount.unlink("fileC"));

  ASSERT_EQ(0, test_mount.mksnap("snap2"));
  std::cout << "---------snap2 listing---------" << std::endl;
  ASSERT_EQ(0, test_mount.for_each_readdir("/",
    [&](const dirent* dire) {
      std::cout << dire->d_name << std::endl;
      return true;
    }));

  std::cout << "---------invalid snapdiff path, the same snaps---------" << std::endl;
  {
    auto snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap1");
    ASSERT_EQ(-ENOENT, test_mount.for_each_readdir(snapdiff_path.c_str(),
      [&](const dirent* dire) {
        return true;
      }));
  }
  std::cout << "---------snap1 vs. snap2 diff listing---------" << std::endl;
  auto snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2");
  ASSERT_EQ(0, test_mount.for_each_readdir(snapdiff_path.c_str(),
    [&](const dirent* dire) {
      std::cout << dire->d_name << std::endl;
      return true;
    }));
  std::cout << "---------reading from snapdiff results---------" << std::endl;
  {
    vector<string> expected;
    expected.push_back("fileA");
    expected.push_back("~fileC");
    expected.push_back("fileB");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(),expected));
  }

  ASSERT_EQ(0, test_mount.readfull_and_compare("fileA", "hello world again"));
  ASSERT_EQ(-EACCES, test_mount.readfull_and_compare("fileC", "hello world"));

  ASSERT_EQ(0, test_mount.readfull_and_compare(snapdiff_path, "fileA", "hello world again"));
  ASSERT_EQ(-EINVAL, test_mount.readfull_and_compare(snapdiff_path, "fileA", "hello world AGAIN"));
  ASSERT_EQ(0, test_mount.readfull_and_compare(snapdiff_path, "fileB", "hello world again in B"));
  ASSERT_EQ(0, test_mount.readfull_and_compare(snapdiff_path, "~fileC", "hello world in another file"));
  std::cout << "---------invalid snapdiff path, no snap2 ---------" << std::endl;
  {
    // invalid file path - no slash between snapdiff and file names
    string s = snapdiff_path;
    s += "fileA";
    ASSERT_EQ(-EACCES, test_mount.readfull_and_compare(s.c_str(), "hello world again"));
  }
  std::cout << "------------- closing -------------" << std::endl;

  ASSERT_EQ(0, test_mount.unlink("fileA"));
  ASSERT_EQ(0, test_mount.unlink("fileB"));
  ASSERT_EQ(0, test_mount.unlink("fileD"));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
}

/* The following method creates the following layout of files/folders/snapshots,
* where:
  - xN denotes file 'x' version N.
  - X denotes folder name
  - * denotes no/removed file/folder

#     snap1        snap2      snap3      head
# a1     |     a1     |    a3    |    a4
# b1     |     b2     |    b3    |    b3
# c1     |     *      |    *     |    *
# *      |     d2     |    d3    |    d3
# f1     |     f2     |    *     |    *
# ff1    |     ff1    |    *     |    *
# g1     |     *      |    g3    |    g3
# *      |     *      |    *     |    h4
# i1     |     i1     |    i1    |    i1
# S      |     S      |    S     |    S
# S/sa1  |     S/sa2  |    S/sa3 |    S/sa3
# *      |     *      |    *     |    S/sh4
# *      |     T      |    T     |    T
# *      |     T/td2  |    T/td3 |    T/td3
# C      |     *      |    *     |    *
# C/cc1  |     *      |    *     |    *
# C/C1   |     *      |    *     |    *
# C/C1/c1|     *      |    *     |    *
# G      |     *      |    G     |    G
# G/gg1  |     *      |    G/gg3 |    G/gg3
# *      |     k2     |    *     |    *
# *      |     l2     |    l2    |    *
# *      |     K      |    *     |    *
# *      |     K/kk2  |    *     |    *
# *      |     *      |    H     |    H
# *      |     *      |    H/hh3 |    H/hh3
# I      |     I      |    I     |    *
# I/ii1  |     I/ii2  |    I/ii3 |    *
# I/iii1 |     I/iii1 |    I/iii3|    *
# *      |     *      |   I/iiii3|    *
# *      |    I/J     |  I/J     |    *
# *      |   I/J/i2   |  I/J/i3  |    *
# *      |   I/J/j2   |  I/J/j2  |    *
# *      |   I/J/k2   |    *     |    *
# *      |     *      |  I/J/l3  |    *
# L      |     L      |    L     |    L
# L/ll1  |    L/ll1   |   L/ll3  |    L/ll3
# L/LL   |    L/LL    |  L/LL    |    L/LL
# *      |    L/LL/ll2|  L/LL/ll3|    L/LL/ll4
# *      |    L/LM    |    *     |    *
# *      |    L/LM/lm2|    *     |    *
# *      |    L/LN    |    L/LN  |    *
*/
void prepareSnapDiffCases(TestMount& test_mount)
{
  //************ snap1 *************
  ceph_assert(0 < test_mount.write_full("a", "file 'a' v1"));
  ceph_assert(0 < test_mount.write_full("b", "file 'b' v1"));
  ceph_assert(0 < test_mount.write_full("c", "file 'c' v1"));
  ceph_assert(0 < test_mount.write_full("f", "file 'f' v1"));
  ceph_assert(0 < test_mount.write_full("ff", "file 'ff' v1"));
  ceph_assert(0 < test_mount.write_full("g", "file 'g' v1"));
  ceph_assert(0 < test_mount.write_full("i", "file 'i' v1"));

  ceph_assert(0 == test_mount.mkdir("S"));
  ceph_assert(0 < test_mount.write_full("S/sa", "file 'S/sa' v1"));

  ceph_assert(0 == test_mount.mkdir("C"));
  ceph_assert(0 < test_mount.write_full("C/cc", "file 'C/cc' v1"));

  ceph_assert(0 == test_mount.mkdir("C/C1"));
  ceph_assert(0 < test_mount.write_full("C/C1/c", "file 'C/C1/c' v1"));

  ceph_assert(0 == test_mount.mkdir("G"));
  ceph_assert(0 < test_mount.write_full("G/gg", "file 'G/gg' v1"));

  ceph_assert(0 == test_mount.mkdir("I"));
  ceph_assert(0 < test_mount.write_full("I/ii", "file 'I/ii' v1"));
  ceph_assert(0 < test_mount.write_full("I/iii", "file 'I/iii' v1"));

  ceph_assert(0 == test_mount.mkdir("L"));
  ceph_assert(0 < test_mount.write_full("L/ll", "file 'L/ll' v1"));
  ceph_assert(0 == test_mount.mkdir("L/LL"));

  ceph_assert(0 == test_mount.mksnap("snap1"));
  //************ snap2 *************

  ceph_assert(0 < test_mount.write_full("b", "file 'b' v2"));
  ceph_assert(0 == test_mount.unlink("c"));
  ceph_assert(0 < test_mount.write_full("d", "file 'd' v2"));
  ceph_assert(0 < test_mount.write_full("f", "file 'f' v2"));
  ceph_assert(0 == test_mount.unlink("g"));

  ceph_assert(0 < test_mount.write_full("S/sa", "file 'S/sa' v2"));

  ceph_assert(0 == test_mount.mkdir("T"));
  ceph_assert(0 < test_mount.write_full("T/td", "file 'T/td' v2"));

  ceph_assert(0 == test_mount.purge_dir("C"));
  ceph_assert(0 == test_mount.purge_dir("G"));

  ceph_assert(0 < test_mount.write_full("k", "file 'k' v2"));
  ceph_assert(0 < test_mount.write_full("l", "file 'l' v2"));

  ceph_assert(0 == test_mount.mkdir("K"));
  ceph_assert(0 < test_mount.write_full("K/kk", "file 'K/kk' v2"));

  ceph_assert(0 < test_mount.write_full("I/ii", "file 'I/ii' v2"));

  ceph_assert(0 == test_mount.mkdir("I/J"));
  ceph_assert(0 < test_mount.write_full("I/J/i", "file 'I/J/i' v2"));
  ceph_assert(0 < test_mount.write_full("I/J/j", "file 'I/J/j' v2"));
  ceph_assert(0 < test_mount.write_full("I/J/k", "file 'I/J/k' v2"));

  ceph_assert(0 < test_mount.write_full("L/LL/ll", "file 'L/LL/ll' v2"));

  ceph_assert(0 == test_mount.mkdir("L/LM"));
  ceph_assert(0 < test_mount.write_full("L/LM/lm", "file 'L/LM/lm' v2"));

  ceph_assert(0 == test_mount.mkdir("L/LN"));

  ceph_assert(0 == test_mount.mksnap("snap2"));
    //************ snap3 *************

  ceph_assert(0 < test_mount.write_full("a", "file 'a' v3"));
  ceph_assert(0 < test_mount.write_full("b", "file 'b' v3"));
  ceph_assert(0 < test_mount.write_full("d", "file 'd' v3"));
  ceph_assert(0 == test_mount.unlink("f"));
  ceph_assert(0 == test_mount.unlink("ff"));
  ceph_assert(0 < test_mount.write_full("g", "file 'g' v3"));

  ceph_assert(0 < test_mount.write_full("S/sa", "file 'S/sa' v3"));

  ceph_assert(0 < test_mount.write_full("T/td", "file 'T/td' v3"));

  ceph_assert(0 == test_mount.mkdir("G"));
  ceph_assert(0 < test_mount.write_full("G/gg", "file 'G/gg' v3"));

  ceph_assert(0 == test_mount.unlink("k"));

  ceph_assert(0 == test_mount.purge_dir("K"));

  ceph_assert(0 == test_mount.mkdir("H"));
  ceph_assert(0 < test_mount.write_full("H/hh", "file 'H/hh' v3"));

  ceph_assert(0 < test_mount.write_full("I/ii", "file 'I/ii' v3"));
  ceph_assert(0 < test_mount.write_full("I/iii", "file 'I/iii' v3"));
  ceph_assert(0 < test_mount.write_full("I/iiii", "file 'I/iiii' v3"));

  ceph_assert(0 < test_mount.write_full("I/J/i", "file 'I/J/i' v3"));
  ceph_assert(0 == test_mount.unlink("I/J/k"));
  ceph_assert(0 < test_mount.write_full("I/J/l", "file 'I/J/l' v3"));

  ceph_assert(0 < test_mount.write_full("L/ll", "file 'L/ll' v3"));

  ceph_assert(0 < test_mount.write_full("L/LL/ll", "file 'L/LL/ll' v3"));

  ceph_assert(0 == test_mount.purge_dir("L/LM"));

  ceph_assert(0 == test_mount.mksnap("snap3"));
  //************ head *************
  ceph_assert(0 < test_mount.write_full("a", "file 'a' head"));

  ceph_assert(0 < test_mount.write_full("h", "file 'h' head"));

  ceph_assert(0 < test_mount.write_full("S/sh", "file 'S/sh' head"));

  ceph_assert(0 == test_mount.unlink("l"));

  ceph_assert(0 == test_mount.purge_dir("I"));

  ceph_assert(0 < test_mount.write_full("L/LL/ll", "file 'L/LL/ll' head"));

  ceph_assert(0 == test_mount.purge_dir("L/LN"));
}

TEST(LibCephFS, SnapDiffVariousCases)
{
  TestMount test_mount;

  prepareSnapDiffCases(test_mount);

  string snapdiff_path;

  {
    std::cout << "---------snap1 vs. snap2 diff listing---------" << std::endl;
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2");
    ASSERT_EQ(0, test_mount.for_each_readdir(snapdiff_path.c_str(),
      [&](const dirent* dire) {
        std::cout << dire->d_name << " ";
        return true;
      }));
    std::cout << std::endl;

    vector<string> expected;
    expected.push_back("b");
    expected.push_back("~c");
    expected.push_back("d");
    expected.push_back("f");
    expected.push_back("~g");
    expected.push_back("S");
    expected.push_back("T");
    expected.push_back("~C");
    expected.push_back("~G");
    expected.push_back("k");
    expected.push_back("l");
    expected.push_back("K");
    expected.push_back("I");
    expected.push_back("L");

    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "b", "file 'b' v2"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~c", "file 'c' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "d", "file 'd' v2"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~g", "file 'g' v1"));

    expected.clear();
    expected.push_back("sa");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "S");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "sa", "file 'S/sa' v2"));

    expected.clear();
    expected.push_back("td");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "T");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "td", "file 'T/td' v2"));

    expected.clear();
    expected.push_back("~cc");
    expected.push_back("~C1");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "~C");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~cc", "file 'C/cc' v1"));

    expected.clear();
    expected.push_back("~c");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "~C/~C1");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~c", "file 'C/C1/c' v1"));

    expected.clear();
    expected.push_back("ii");
    expected.push_back("J");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "I");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ii", "file 'I/ii' v2"));

    expected.clear();
    expected.push_back("i");
    expected.push_back("j");
    expected.push_back("k");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "I/J");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "k", "file 'I/J/k' v2"));

    expected.clear();
    expected.push_back("LL");
    expected.push_back("LM");
    expected.push_back("LN");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "L");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    expected.clear();
    expected.push_back("ll");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "L/LL");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/LL/ll' v2"));

    expected.clear();
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "L/LN");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
  }

  {
    std::cout << "---------snap2 vs. snap3 diff listing---------" << std::endl;
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3");
    ASSERT_EQ(0, test_mount.for_each_readdir(snapdiff_path.c_str(),
      [&](const dirent* dire) {
        std::cout << dire->d_name << " ";
        return true;
      }));
    std::cout << std::endl;

    vector<string> expected;
    expected.push_back("a");
    expected.push_back("b");
    expected.push_back("d");
    expected.push_back("~f");
    expected.push_back("~ff");
    expected.push_back("g");
    expected.push_back("S");
    expected.push_back("T");
    expected.push_back("G");
    expected.push_back("~k");
    expected.push_back("~K");
    expected.push_back("H");
    expected.push_back("I");
    expected.push_back("L");

    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "a", "file 'a' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "b", "file 'b' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "d", "file 'd' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~f", "file 'f' v2"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~ff", "file 'ff' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "g", "file 'g' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~k", "file 'k' v2"));

    expected.clear();
    expected.push_back("sa");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "S");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "sa", "file 'S/sa' v3"));

    expected.clear();
    expected.push_back("td");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "T");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "td", "file 'T/td' v3"));

    expected.clear();
    expected.push_back("gg");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "G");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "gg", "file 'G/gg' v3"));

    expected.clear();
    expected.push_back("~kk");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "~K");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~kk", "file 'K/kk' v2"));

    expected.clear();
    expected.push_back("hh");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "H");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "hh", "file 'H/hh' v3"));

    expected.clear();
    expected.push_back("ii");
    expected.push_back("iii");
    expected.push_back("iiii");
    expected.push_back("J");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "I");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ii", "file 'I/ii' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "iii", "file 'I/iii' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "iiii", "file 'I/iiii' v3"));

    expected.clear();
    expected.push_back("i");
    expected.push_back("~k");
    expected.push_back("l");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "I/J");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "i", "file 'I/J/i' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~k", "file 'I/J/k' v2"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "l", "file 'I/J/l' v3"));

    expected.clear();
    expected.push_back("ll");
    expected.push_back("LL");
    expected.push_back("~LM");
    expected.push_back("LN");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "L");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/ll' v3"));

    expected.clear();
    expected.push_back("ll");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "L/LL");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/LL/ll' v3"));

    expected.clear();
    expected.push_back("~lm");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "L/~LM");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~lm", "file 'L/LM/lm' v2"));

    expected.clear();
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "L/LN");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
  }
  {
    std::cout << "---------snap1 vs. snap3 diff listing---------" << std::endl;
    snapdiff_path = test_mount.make_snapdiff_relpath("snap3", "snap1");
    ASSERT_EQ(0, test_mount.for_each_readdir(snapdiff_path.c_str(),
      [&](const dirent* dire) {
        std::cout << dire->d_name << " ";
        return true;
      }));
    std::cout << std::endl;
    vector<string> expected;
    expected.push_back("a");
    expected.push_back("b");
    expected.push_back("~c");
    expected.push_back("d");
    expected.push_back("~f");
    expected.push_back("~ff");
    expected.push_back("g");
    expected.push_back("S");
    expected.push_back("T");
    expected.push_back("~C");
    expected.push_back("G");
    expected.push_back("l");
    expected.push_back("~G");
    expected.push_back("H");
    expected.push_back("I");
    expected.push_back("L");

    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "a", "file 'a' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "b", "file 'b' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~c", "file 'c' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "d", "file 'd' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~f", "file 'f' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~ff", "file 'ff' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "g", "file 'g' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "l", "file 'l' v2"));

    expected.clear();
    expected.push_back("sa");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "S");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "sa", "file 'S/sa' v3"));

    expected.clear();
    expected.push_back("td");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "T");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "td", "file 'T/td' v3"));

    expected.clear();
    expected.push_back("~cc");
    expected.push_back("~C1");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "~C");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~cc", "file 'C/cc' v1"));

    expected.clear();
    expected.push_back("~c");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "~C/~C1");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~c", "file 'C/C1/c' v1"));

    expected.clear();
    expected.push_back("~gg");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "~G");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~gg", "file 'G/gg' v1"));

    expected.clear();
    expected.push_back("gg");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "G");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "gg", "file 'G/gg' v3"));

    expected.clear();
    expected.push_back("hh");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "H");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "hh", "file 'H/hh' v3"));

    expected.clear();
    expected.push_back("ii");
    expected.push_back("iii");
    expected.push_back("iiii");
    expected.push_back("J");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "I");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "iii", "file 'I/iii' v3"));

    expected.clear();
    expected.push_back("i");
    expected.push_back("j");
    expected.push_back("l");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "I/J");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "i", "file 'I/J/i' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "l", "file 'I/J/l' v3"));

    expected.clear();
    expected.push_back("ll");
    expected.push_back("LL");
    expected.push_back("LN");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "L");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/ll' v3"));

    expected.clear();
    expected.push_back("ll");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "L/LL");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/LL/ll' v3"));

    expected.clear();
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "L/LN");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
  }
  std::cout << "-------------" << std::endl;

  test_mount.remove_all();
  test_mount.rmsnap("snap1");
  test_mount.rmsnap("snap2");
  test_mount.rmsnap("snap3");
}

TEST(LibCephFS, SnapDiffLib)
{
  TestMount test_mount;

  ASSERT_LT(0, test_mount.write_full("fileA", "hello world"));
  ASSERT_LT(0, test_mount.write_full("fileC", "hello world to be removed"));
  ASSERT_LT(0, test_mount.write_full("fileD", "hello world unmodified"));
  ASSERT_EQ(0, test_mount.mkdir("dirA"));
  ASSERT_LT(0, test_mount.write_full("dirA/fileA", "file 'A/a' v1"));
  ASSERT_EQ(0, test_mount.mkdir("dirC"));
  ASSERT_LT(0, test_mount.write_full("dirC/filec", "file 'C/c' v1"));
  ASSERT_EQ(0, test_mount.mkdir("dirD"));
  ASSERT_LT(0, test_mount.write_full("dirD/filed", "file 'D/d' v1"));

  ASSERT_EQ(0, test_mount.mksnap("snap1"));
  ASSERT_LT(0, test_mount.write_full("fileA", "hello world again in A"));
  ASSERT_LT(0, test_mount.write_full("fileB", "hello world in B"));
  ASSERT_EQ(0, test_mount.unlink("fileC"));

  ASSERT_LT(0, test_mount.write_full("dirA/fileA", "file 'A/a' v2"));
  ASSERT_EQ(0, test_mount.purge_dir("dirC"));
  ASSERT_EQ(0, test_mount.mkdir("dirB"));
  ASSERT_LT(0, test_mount.write_full("dirB/fileb", "file 'B/b' v2"));
  ASSERT_EQ(0, test_mount.mksnap("snap2"));

  uint64_t snapid1;
  uint64_t snapid2;
  ASSERT_EQ(0, test_mount.get_snapid("snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("snap2", &snapid2));
  std::cout << snapid1 << " vs. " << snapid2 << std::endl;
  ASSERT_GT(snapid1, 0);
  ASSERT_GT(snapid2, 0);
  ASSERT_GT(snapid2, snapid1);

  {
    string snap_path = test_mount.make_snap_path("snap2");
    std::cout << "---------snap1 vs. snap2 diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirA");
    std::cout << "---------snap1/dirA vs. snap2/dirA diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirB");
    std::cout << "---------snap1/dirB vs. snap2/dirB diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirD");
    std::cout << "---------snap1/dirD vs. snap2/dirD diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }
  {
    std::cout << "---------snap1 listing verification---------" << std::endl;
    string snap_path = test_mount.make_snap_path("snap1");
    vector<string> expected;
    expected.push_back("fileA");
    expected.push_back("fileC");
    expected.push_back("fileD");
    expected.push_back("dirA");
    expected.push_back("dirC");
    expected.push_back("dirD");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snap_path.c_str(), expected));
  }
  {
    std::cout << "---------snap2 listing verification---------" << std::endl;
    string snap_path = test_mount.make_snap_path("snap2");
    vector<string> expected;
    expected.push_back("fileA");
    expected.push_back("fileB");
    expected.push_back("fileD");
    expected.push_back("dirA");
    expected.push_back("dirB");
    expected.push_back("dirD");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snap_path.c_str(), expected));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2");
    std::cout << "---------snap1 vs. snap2 diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2");
    std::cout << "---------snap1 vs. snap2 diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileA", snapid2);
    expected.emplace_back("fileB", snapid2);
    expected.emplace_back("fileC", snapid1);
    expected.emplace_back("dirA", snapid2);
    expected.emplace_back("dirB", snapid2);
    expected.emplace_back("dirC", snapid1);
    expected.emplace_back("dirD", snapid2);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snapid1,
        expected));
  }
  {
    string snap_path = test_mount.make_snap_path("snap1");
    std::cout << "---------snap2 vs. snap1 diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileA", snapid2);
    expected.emplace_back("fileB", snapid2);
    expected.emplace_back("fileC", snapid1);
    expected.emplace_back("dirA", snapid2);
    expected.emplace_back("dirB", snapid2);
    expected.emplace_back("dirC", snapid1);
    expected.emplace_back("dirD", snapid2);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snapid2,
        expected));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirB");
    std::cout << "---------snap1/dirB vs. snap2/dirB diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileb", snapid2);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snapid1,
        expected));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirD");
    std::cout << "---------snap1/dirD vs. snap2/dirD diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snapid1,
        expected));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2");
    std::cout << "---------invalid snapdiff params, the same snaps---------" << std::endl;
    ASSERT_EQ(-EINVAL, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid2,
      [&](const dirent* dire, uint64_t snapid) {
        return true;
      }));
  }
  {
    std::cout << "---------invalid snapdiff params, no snap_other ---------" << std::endl;
    string snap_path = test_mount.make_snap_path("snap2");
    ASSERT_EQ(-EINVAL, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      CEPH_NOSNAP,
      [&](const dirent* dire, uint64_t snapid) {
        return true;
      }));
  }

  std::cout << "------------- closing -------------" << std::endl;
  ASSERT_EQ(0, test_mount.purge_dir(""));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
}

#ifdef __NEVER_DEFINED__
//FIXME: this test case is broken for now - second snap1 listing after snapdiff call
// returns incomplete list of entries from cache. Proper clear_dir_complete_and_ordered() 
// call or something is needed.
//
TEST(LibCephFS, SnapDiffLib2)
{
  TestMount test_mount;

  ASSERT_LT(0, test_mount.write_full("fileA", "hello world"));
  ASSERT_LT(0, test_mount.write_full("fileC", "hello world to be removed"));
  ASSERT_LT(0, test_mount.write_full("fileD", "hello world unmodified"));
  ASSERT_EQ(0, test_mount.mkdir("dirA"));
  ASSERT_LT(0, test_mount.write_full("dirA/fileA", "file 'A/a' v1"));
  ASSERT_EQ(0, test_mount.mkdir("dirC"));
  ASSERT_LT(0, test_mount.write_full("dirC/filec", "file 'C/c' v1"));
  ASSERT_EQ(0, test_mount.mkdir("dirD"));
  ASSERT_LT(0, test_mount.write_full("dirD/filed", "file 'D/d' v1"));

  ASSERT_EQ(0, test_mount.mksnap("snap1"));
  ASSERT_LT(0, test_mount.write_full("fileA", "hello world again in A"));
  ASSERT_LT(0, test_mount.write_full("fileB", "hello world in B"));
  ASSERT_EQ(0, test_mount.unlink("fileC"));

  ASSERT_LT(0, test_mount.write_full("dirA/fileA", "file 'A/a' v2"));
  ASSERT_EQ(0, test_mount.purge_dir("dirC"));
  ASSERT_EQ(0, test_mount.mkdir("dirB"));
  ASSERT_LT(0, test_mount.write_full("dirB/fileb", "file 'B/b' v2"));
  ASSERT_EQ(0, test_mount.mksnap("snap2"));

  ASSERT_LT(0, test_mount.write_full("fileC", "hello world in C recovered"));
  ASSERT_LT(0, test_mount.write_full("fileD", "hello world in D now modified"));
  ASSERT_LT(0, test_mount.write_full("fileE", "file 'E' created at snap3"));
  ASSERT_EQ(0, test_mount.purge_dir("dirA"));
  ASSERT_EQ(0, test_mount.purge_dir("dirB"));
  ASSERT_EQ(0, test_mount.mksnap("snap3"));

  uint64_t snapid1;
  uint64_t snapid2;
  uint64_t snapid3;
  ASSERT_EQ(0, test_mount.get_snapid("snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("snap2", &snapid2));
  ASSERT_EQ(0, test_mount.get_snapid("snap3", &snapid3));
  std::cout << snapid1 << " vs. " << snapid2 << " vs. " << snapid3 << std::endl;
  ASSERT_GT(snapid1, 0);
  ASSERT_GT(snapid2, 0);
  ASSERT_GT(snapid3, 0);
  ASSERT_GT(snapid2, snapid1);
  ASSERT_GT(snapid3, snapid2);

  auto verify_snap_listing = [&]()
  {
    {
      std::cout << "---------snap1 listing verification---------" << std::endl;
      string snap_path = test_mount.make_snap_path("snap1");

      ASSERT_EQ(0, test_mount.for_each_readdir(
        snap_path.c_str(),
        [&](const dirent* dire) {
          std::cout << dire->d_name << std::endl;
          return true;
        }));


      vector<string> expected;
      expected.push_back("fileA");
      expected.push_back("fileC");
      expected.push_back("fileD");
      expected.push_back("dirA");
      expected.push_back("dirC");
      expected.push_back("dirD");
      ASSERT_EQ(0,
        test_mount.readdir_and_compare(snap_path.c_str(), expected));
    }
    {
      std::cout << "---------snap2 listing verification---------" << std::endl;
      string snap_path = test_mount.make_snap_path("snap2");
      vector<string> expected;
      expected.push_back("fileA");
      expected.push_back("fileB");
      expected.push_back("fileD");
      expected.push_back("dirA");
      expected.push_back("dirB");
      expected.push_back("dirD");
      ASSERT_EQ(0,
        test_mount.readdir_and_compare(snap_path.c_str(), expected));
    }
    {
      std::cout << "---------snap3 listing verification---------" << std::endl;
      string snap_path = test_mount.make_snap_path("snap3");
      vector<string> expected;
      expected.push_back("fileA");
      expected.push_back("fileB");
      expected.push_back("fileC");
      expected.push_back("fileD");
      expected.push_back("fileE");
      expected.push_back("dirD");
      ASSERT_EQ(0,
        test_mount.readdir_and_compare(snap_path.c_str(), expected));
    }
  };

  verify_snap_listing();
  {
    string snap_path = test_mount.make_snap_path("snap2");
    std::cout << "---------snap1 vs. snap2 diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }
  /*{
    string snap_path = test_mount.make_snap_path("snap2", "dirA");
    std::cout << "---------snap1/dirA vs. snap2/dirA diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirB");
    std::cout << "---------snap1/dirB vs. snap2/dirB diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirD");
    std::cout << "---------snap1/dirD vs. snap2/dirD diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }*/
  verify_snap_listing();
  /*{
    string snap_path = test_mount.make_snap_path("snap2");
    std::cout << "---------snap1 vs. snap2 diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid1,
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2");
    std::cout << "---------snap1 vs. snap2 diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileA", snapid2);
    expected.emplace_back("fileB", snapid2);
    expected.emplace_back("fileC", snapid1);
    expected.emplace_back("dirA", snapid2);
    expected.emplace_back("dirB", snapid2);
    expected.emplace_back("dirC", snapid1);
    expected.emplace_back("dirD", snapid2);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snapid1,
        expected));
  }
  {
    string snap_path = test_mount.make_snap_path("snap1");
    std::cout << "---------snap2 vs. snap1 diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileA", snapid2);
    expected.emplace_back("fileB", snapid2);
    expected.emplace_back("fileC", snapid1);
    expected.emplace_back("dirA", snapid2);
    expected.emplace_back("dirB", snapid2);
    expected.emplace_back("dirC", snapid1);
    expected.emplace_back("dirD", snapid2);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snapid2,
        expected));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirB");
    std::cout << "---------snap1/dirB vs. snap2/dirB diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileb", snapid2);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snapid1,
        expected));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirD");
    std::cout << "---------snap1/dirD vs. snap2/dirD diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snapid1,
        expected));
  }
  {
    string snap_path = test_mount.make_snap_path("snap2");
    std::cout << "---------invalid snapdiff params, the same snaps---------" << std::endl;
    ASSERT_EQ(-EINVAL, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snapid2,
      [&](const dirent* dire, uint64_t snapid) {
        return true;
      }));
  }
  {
    std::cout << "---------invalid snapdiff params, no snap_other ---------" << std::endl;
    string snap_path = test_mount.make_snap_path("snap2");
    ASSERT_EQ(-EINVAL, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      CEPH_NOSNAP,
      [&](const dirent* dire, uint64_t snapid) {
        return true;
      }));
  }

  //FIXME: add a test case where removed entry has got its last snapshot id within (A..B) range
  // resulted snapid to be equal to snapA!

  */
  std::cout << "------------- closing -------------" << std::endl;
  ASSERT_EQ(0, test_mount.purge_dir(""));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
  ASSERT_EQ(0, test_mount.rmsnap("snap3"));
}
#endif