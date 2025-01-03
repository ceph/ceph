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
#include "include/stringify.h"
#include "common/ceph_context.h"
#include "common/config_proxy.h"
#include "json_spirit/json_spirit.h"
#include "boost/format/alt_sstream.hpp"
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
#include <optional>

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

  int conf_get(const char *option, char *buf, size_t len) {
    return ceph_conf_get(cmount, option, buf, len);
  }

  json_spirit::mValue tell_rank0(const std::string& prefix, cmdmap_t&& cmdmap = {}) {
    cmdmap["prefix"] = prefix;
    cmdmap["format"] = std::string("json");

    JSONFormatter jf;
    jf.open_object_section("");
    ceph::common::cmdmap_dump(cmdmap, &jf);
    jf.close_section();

    boost::io::basic_oaltstringstream<char> oss;
    jf.flush(oss);

    const char *cmdv[] = {oss.begin()};

    char *outb, *outs;
    size_t outb_len, outs_len;
    int status = ceph_mds_command(cmount, "0", cmdv, sizeof(cmdv)/sizeof(cmdv[0]), nullptr, 0, &outb, &outb_len, &outs, &outs_len);
    if (status < 0)
    {
      outs[outs_len] = 0;
      std::cout << "couldn't tell rank 0 '" << oss.begin() << "'\n" << strerror(-status) << ": " << outs << std::endl;
      return json_spirit::mValue::null;
    }

    json_spirit::mValue dump;
    if (!json_spirit::read(outb, dump))
    {
      std::cout << "couldn't parse '" << prefix << "'response json" << std::endl;
      return json_spirit::mValue::null;
    }
    return dump;
  }

  bool tell_rank0_config(const std::string &var, const std::optional<const std::string> val = {}) {
    cmdmap_t cmdmap;
    std::string prefix;
    cmdmap["var"] = var;

    if (val.has_value()) {
      cmdmap["val"] = std::vector{val.value()};
      prefix = "config set";
    }
    else {
      prefix = "config unset";
    }

    return !tell_rank0(prefix, std::move(cmdmap)).is_null();
  }

  string make_file_path(const char *relpath) {
    char path[PATH_MAX];
    sprintf(path, "%s/%s", dir_path, relpath);
    return path;
  }

  string make_snap_name(const char* name) {
    char snap_name[64];
    if (name && *name) {
      sprintf(snap_name, "%s_%d", name, getpid());
    } else {
      // just simulate empty snapname
      snap_name[0] = 0;
    }
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
      ceph_truncate(cmount, file_path.c_str(), data.size());
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
  int unlink(const char* relpath)
  {
    auto file_path = make_file_path(relpath);
    return ceph_unlink(cmount, file_path.c_str());
  }

  int test_open(const char* relpath)
  {
    auto subdir_path = make_file_path(relpath);
    struct ceph_dir_result* ls_dir;
    int r = ceph_opendir(cmount, subdir_path.c_str(), &ls_dir);
    if (r != 0) {
      return r;
    }
    ceph_assert(0 == ceph_closedir(cmount, ls_dir));
    return r;
  }

  int for_each_readdir(const char* relpath,
    std::function<bool(const dirent*, const struct ceph_statx*)> fn)
  {
    auto subdir_path = make_file_path(relpath);
    struct ceph_dir_result* ls_dir;
    int r = ceph_opendir(cmount, subdir_path.c_str(), &ls_dir);
    if (r != 0) {
      return r;
    }

    while (1) {
      struct dirent result;
      struct ceph_statx stx;

      r = ceph_readdirplus_r(
        cmount, ls_dir, &result, &stx, CEPH_STATX_BASIC_STATS,
        0,
        NULL);
      if (!r)
        break;
      if (r < 0) {
        std::cerr << "ceph_readdirplus_r failed, error: "
                  << r << std::endl;
        return r;
      }

      if (strcmp(result.d_name, ".") == 0 ||
          strcmp(result.d_name, "..") == 0) {
        continue;
      }
      if (!fn(&result, &stx)) {
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
      [&](const dirent* dire, const struct ceph_statx* stx) {

        std::string name(dire->d_name);
        auto it = std::find(expected.begin(), end, name);
        if (it == end) {
          std::cerr << "readdir_and_compare error: unexpected name:"
                    << name << std::endl;
          return false;
        }
        expected.erase(it);
        return true;
      });
    if (r == 0 && !expected.empty()) {
      std::cerr << __func__ << " error: left entries:" << std::endl;
      for (auto& e : expected) {
        std::cerr << e << std::endl;
      }
      std::cerr << __func__ << " ************" << std::endl;
      r = -ENOTEMPTY;
    }
    return r;
  }
  int for_each_readdir_snapdiff(const char* relpath,
    const char* snap1,
    const char* snap2,
    std::function<bool(const dirent*, uint64_t)> fn)
  {
    auto s1 = make_snap_name(snap1);
    auto s2 = make_snap_name(snap2);
    ceph_snapdiff_info info;
    ceph_snapdiff_entry_t res_entry;
    int r = ceph_open_snapdiff(cmount,
                               dir_path,
                               relpath,
                               s1.c_str(),
                               s2.c_str(),
                               &info);
    if (r != 0) {
      std::cerr << " Failed to open snapdiff, ret:" << r << std::endl;
      return r;
    }
    while (0 < (r = ceph_readdir_snapdiff(&info,
                                          &res_entry))) {
      if (strcmp(res_entry.dir_entry.d_name, ".") == 0 ||
        strcmp(res_entry.dir_entry.d_name, "..") == 0) {
        continue;
      }
      if (!fn(&res_entry.dir_entry, res_entry.snapid)) {
        r = -EINTR;
        break;
      }
    }
    ceph_assert(0 == ceph_close_snapdiff(&info));
    if (r != 0) {
      std::cerr << " Failed to readdir snapdiff, ret:" << r
                << " " << relpath << ", " << snap1 << " vs. " << snap2
                << std::endl;
    }
    return r;
  }
  int readdir_snapdiff_and_compare(const char* relpath,
    const char* snap1,
    const char* snap2,
    const vector<pair<string, uint64_t>>& expected0)
  {
    vector<pair<string, uint64_t>> expected(expected0);
    auto end = expected.end();
    int r = for_each_readdir_snapdiff(relpath, snap1, snap2,
      [&](const dirent* dire, uint64_t snapid) {

        pair<string, uint64_t> p = std::make_pair(dire->d_name, snapid);
        auto it = std::find(expected.begin(), end, p);
        if (it == end) {
          std::cerr << "readdir_snapdiff_and_compare error: unexpected name:"
            << dire->d_name << "/" << snapid << std::endl;
          return false;
        }
        expected.erase(it);
        return true;
      });
    if (r == 0 && !expected.empty()) {
      std::cerr << __func__ << " error: left entries:" << std::endl;
      for (auto& e : expected) {
        std::cerr << e.first << "/" << e.second << std::endl;
      }
      std::cerr << __func__ << " ************" << std::endl;
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
        [&](const dirent* dire, const struct ceph_statx* stx) {
          string relpath = concat_path(relpath0, dire->d_name);
	  if (S_ISDIR(stx->stx_mode)) {
            purge_dir(relpath.c_str());
            rmdir(relpath.c_str());
          } else {
            unlink(relpath.c_str());
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

  void verify_snap_diff(vector<pair<string, uint64_t>>& expected,
                        const char* relpath,
                        const char* snap1,
                        const char* snap2);
  void print_snap_diff(const char* relpath,
		       const char* snap1,
                       const char* snap2);

  void prepareSnapDiffLib1Cases();
  void prepareSnapDiffLib2Cases();
  void prepareSnapDiffLib3Cases();
  void prepareHugeSnapDiff(const std::string& name_prefix_start,
                           const std::string& name_prefix_bulk,
                           const std::string& name_prefix_end,
                           size_t file_count,
                           bool bulk_diff);
};

// Helper function to verify readdir_snapdiff returns expected results
void TestMount::verify_snap_diff(vector<pair<string, uint64_t>>& expected,
                                 const char* relpath,
                                 const char* snap1,
                                 const char* snap2)
{
  std::cout << "---------" << snap1 << " vs. " << snap2
            << " diff listing verification for /" << (relpath ? relpath : "")
            << std::endl;
  ASSERT_EQ(0,
    readdir_snapdiff_and_compare(relpath, snap1, snap2, expected));
};

// Helper function to print readdir_snapdiff results
void TestMount::print_snap_diff(const char* relpath,
				const char* snap1,
                                const char* snap2)
{
  std::cout << "---------" << snap1 << " vs. " << snap2
            << " diff listing for /" << (relpath ? relpath : "")
            << std::endl;
  ASSERT_EQ(0, for_each_readdir_snapdiff(relpath, snap1, snap2,
    [&](const dirent* dire, uint64_t snapid) {
      std::cout << dire->d_name << " snap " << snapid << std::endl;
      return true;
    }));
};

/* The following method creates some files/folders/snapshots layout,
   described in the sheet below.
   We're to test SnapDiff readdir API against that structure.

* where:
  - xN denotes file 'x' version N.
  - X denotes folder name
  - * denotes no/removed file/folder

#    snap1         snap2
# fileA1      | fileA2      |
# *           | fileB2      |
# fileC1      | *           |
# fileD1      | fileD1      |
# dirA        | dirA        |
# dirA/fileA1 | dirA/fileA2 |
# *           | dirB        |
# *           | dirB/fileb2 |
# dirC        | *           |
# dirC/filec1 | *           |
# dirD        | dirD        |
# dirD/fileD1 | dirD/fileD1 |
*/
void TestMount::prepareSnapDiffLib1Cases()
{
  //************ snap1 *************
  ASSERT_LE(0, write_full("fileA", "hello world"));
  ASSERT_LE(0, write_full("fileC", "hello world to be removed"));
  ASSERT_LE(0, write_full("fileD", "hello world unmodified"));
  ASSERT_EQ(0, mkdir("dirA"));
  ASSERT_LE(0, write_full("dirA/fileA", "file 'A/a' v1"));
  ASSERT_EQ(0, mkdir("dirC"));
  ASSERT_LE(0, write_full("dirC/filec", "file 'C/c' v1"));
  ASSERT_EQ(0, mkdir("dirD"));
  ASSERT_LE(0, write_full("dirD/filed", "file 'D/d' v1"));

  ASSERT_EQ(0, mksnap("snap1"));

  //************ snap2 *************
  ASSERT_LE(0, write_full("fileA", "hello world again in A"));
  ASSERT_LE(0, write_full("fileB", "hello world in B"));
  ASSERT_EQ(0, unlink("fileC"));

  ASSERT_LE(0, write_full("dirA/fileA", "file 'A/a' v2"));
  ASSERT_EQ(0, purge_dir("dirC"));
  ASSERT_EQ(0, mkdir("dirB"));
  ASSERT_LE(0, write_full("dirB/fileb", "file 'B/b' v2"));

  ASSERT_EQ(0, mksnap("snap2"));
}

/*
* Basic functionality testing for the SnapDiff readdir API
*/
TEST(LibCephFS, SnapDiffLib)
{
  TestMount test_mount;

  // Create simple directory tree with a couple of snapshots
  // to test against
  test_mount.prepareSnapDiffLib1Cases();

  uint64_t snapid1;
  uint64_t snapid2;

  // learn snapshot ids and do basic verification
  ASSERT_EQ(0, test_mount.get_snapid("snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("snap2", &snapid2));
  ASSERT_GT(snapid1, 0);
  ASSERT_GT(snapid2, 0);
  ASSERT_GT(snapid2, snapid1);
  std::cout << snapid1 << " vs. " << snapid2 << std::endl;

  //
  // Make sure root listing for snapshot snap1 is as expected
  //
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

  //
  // Make sure root listing for snapshot snap2 is as expected
  //
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

  //
  // Print snap1 vs. snap2 delta for the root
  //
  test_mount.print_snap_diff("", "snap1", "snap2");

  //
  // Make sure snap1 vs. snap2 delta for the root is as expected
  //
  {
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileA", snapid2);
    expected.emplace_back("fileB", snapid2);
    expected.emplace_back("fileC", snapid1);
    expected.emplace_back("dirA", snapid2);
    expected.emplace_back("dirB", snapid2);
    expected.emplace_back("dirC", snapid1);
    expected.emplace_back("dirD", snapid2);
    test_mount.verify_snap_diff(expected, "", "snap1", "snap2");
  }

  json_spirit::mValue dump;
  {
    struct Cleanup
    {
      TestMount &test_mount;
      ~Cleanup()
      {
        // make sure to restore the default settings before leaving this block
        test_mount.tell_rank0_config("mds_max_caps_per_client");
        test_mount.tell_rank0_config("mds_session_cap_acquisition_throttle");
        test_mount.tell_rank0_config("mds_session_cap_acquisition_decay_rate");
        test_mount.tell_rank0_config("mds_op_history_size");
        test_mount.tell_rank0_config("mds_op_history_duration");
      }
    } cleanup {test_mount};
    // the following commands will be run with cap_acquisition_throttle triggered
    // to verify that such event is logged on the operations
    ASSERT_TRUE(test_mount.tell_rank0_config("mds_max_caps_per_client", "1"));
    ASSERT_TRUE(test_mount.tell_rank0_config("mds_session_cap_acquisition_throttle", "1"));
    ASSERT_TRUE(test_mount.tell_rank0_config("mds_session_cap_acquisition_decay_rate", "1"));
    ASSERT_TRUE(test_mount.tell_rank0_config("mds_op_history_size", "100"));
    ASSERT_TRUE(test_mount.tell_rank0_config("mds_op_history_duration", "600"));

    //
    // Make sure snap1 vs. snap2 delta for /dirA is as expected
    //
    {
      vector<pair<string, uint64_t>> expected;
      expected.emplace_back("fileA", snapid2);
      test_mount.verify_snap_diff(expected, "dirA", "snap1", "snap2");
    }

    //
    // Make sure snap1 vs. snap2 delta for /dirB is as expected
    //
    {
      vector<pair<string, uint64_t>> expected;
      expected.emplace_back("fileb", snapid2);
      test_mount.verify_snap_diff(expected, "dirB", "snap1", "snap2");
    }

    //
    // Make sure snap1 vs. snap2 delta for /dirC is as expected
    //
    {
      vector<pair<string, uint64_t>> expected;
      expected.emplace_back("filec", snapid1);
      test_mount.verify_snap_diff(expected, "dirC", "snap2", "snap1");
    }

    //
    // Make sure snap1 vs. snap2 delta for /dirD is as expected
    //
    {
      vector<pair<string, uint64_t>> expected;
      test_mount.verify_snap_diff(expected, "dirD", "snap1", "snap2");
    }

    // Make sure SnapDiff returns an error when provided with the same
    // snapshot name for both parties A and B.
    {
      string snap_path = test_mount.make_snap_path("snap2");
      string snap_other_path = snap_path;
      std::cout << "---------invalid snapdiff params, the same snaps---------" << std::endl;
      ASSERT_EQ(-EINVAL, test_mount.for_each_readdir_snapdiff(
        "",
        "snap2",
        "snap2",
        [&](const dirent* dire, uint64_t snapid) {
          return true;
        }));
    }
    // Make sure SnapDiff returns an error when provided with an empty
    // snapshot name for one of the parties
    {
      std::cout << "---------invalid snapdiff params, no snap_other ---------" << std::endl;
      string snap_path = test_mount.make_snap_path("snap2");
      string snap_other_path;
      ASSERT_EQ(-EINVAL, test_mount.for_each_readdir_snapdiff(
        "",
        "snap2",
        "",
        [&](const dirent* dire, uint64_t snapid) {
          return true;
        }));
    }

    // do this before the scope ends and cleanup is run
    dump = test_mount.tell_rank0("dump_historic_ops");
  }

  ASSERT_FALSE(dump.is_null());
  bool seen_cap_throttle_in_recent_op_events = false;
  try {
    for (const auto& op: dump.get_obj().at("ops").get_array()) {
      for (const auto& ev: op.get_obj().at("type_data").get_obj().at("events").get_array()) {
        if (ev.get_obj().at("event") == "cap_acquisition_throttle") {
	  seen_cap_throttle_in_recent_op_events = true;
	  goto done;
	}
      }
    }
    done:;
  }
  catch (const std::runtime_error &e) {
    std::cout << "error while parsing dump_historic_ops: " << e.what() << std::endl;
  }

  if (!seen_cap_throttle_in_recent_op_events) {
    std::cout << "couldn't find 'cap_acquisition_throttle' event in:" << std::endl;
    json_spirit::write(dump, std::cout, json_spirit::pretty_print);
  }

  ASSERT_TRUE(seen_cap_throttle_in_recent_op_events);

  std::cout << "------------- closing -------------" << std::endl;

  ASSERT_EQ(0, test_mount.purge_dir(""));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
}

/* The following method creates some files/folders/snapshots layout,
   described in the sheet below.
   We're to test SnapDiff readdir API against that structure.

* where:
  - xN denotes file 'x' version N.
  - X denotes folder name
  - * denotes no/removed file/folder

#     snap1        snap2      snap3      head
# fileA1      | fileA2      | fileA2
# *           | fileB2      | fileB2
# fileC1      | *           | fileC3
# fileD1      | fileD1      | fileD3
# *           | *           | fileE3
# fileF1      | *           | *
# fileG1      | fileG2      | *
# dirA        | dirA        | *
# dirA/fileA1 | dirA/fileA2 | *
# *           | dirB        | *
# *           | dirB/fileb2 | *
# dirC        | *           | *
# dirC/filec1 | *           | *
# dirD        | dirD        | dirD
# dirD/filed1 | dirD/filed1 | dirD/filed1
*/
void TestMount::prepareSnapDiffLib2Cases()
{
  //************ snap1 *************
  ASSERT_LE(0, write_full("fileA", "hello world"));
  ASSERT_LE(0, write_full("fileC", "hello world to be removed temporarily"));
  ASSERT_LE(0, write_full("fileD", "hello world unmodified"));
  ASSERT_LE(0, write_full("fileF", "hello world to be removed completely"));
  ASSERT_LE(0, write_full("fileG", "hello world to be overwritten at snap2"));
  ASSERT_EQ(0, mkdir("dirA"));
  ASSERT_LE(0, write_full("dirA/fileA", "file 'A/a' v1"));
  ASSERT_EQ(0, mkdir("dirC"));
  ASSERT_LE(0, write_full("dirC/filec", "file 'C/c' v1"));
  ASSERT_EQ(0, mkdir("dirD"));
  ASSERT_LE(0, write_full("dirD/filed", "file 'D/d' v1"));

  ASSERT_EQ(0, mksnap("snap1"));

  //************ snap2 *************
  ASSERT_LE(0, write_full("fileA", "hello world again in A"));
  ASSERT_LE(0, write_full("fileB", "hello world in B"));
  ASSERT_LE(0, write_full("fileG", "hello world to be removed at snap3"));
  ASSERT_EQ(0, unlink("fileC"));
  ASSERT_EQ(0, unlink("fileF"));

  ASSERT_LE(0, write_full("dirA/fileA", "file 'A/a' v2"));
  ASSERT_EQ(0, mkdir("dirB"));
  ASSERT_LE(0, write_full("dirB/fileb", "file 'B/b' v2"));
  ASSERT_EQ(0, purge_dir("dirC"));

  ASSERT_EQ(0, mksnap("snap2"));

  //************ snap3 *************
  ASSERT_LE(0, write_full("fileC", "hello world in C recovered"));
  ASSERT_LE(0, write_full("fileD", "hello world in D now modified"));
  ASSERT_LE(0, write_full("fileE", "file 'E' created at snap3"));
  ASSERT_EQ(0, unlink("fileG"));
  ASSERT_EQ(0, purge_dir("dirA"));
  ASSERT_EQ(0, purge_dir("dirB"));
  ASSERT_EQ(0, mksnap("snap3"));
}

/* The following method creates a folder with tons of file
   updated between two snapshots
   We're to test SnapDiff readdir API against that structure.

* where:
  - xN denotes file 'x' version N.
  - X denotes folder name
  - * denotes no/removed file/folder

#    snap1         snap2
* aaaaA1     | aaaaA1    |
* aaaaB1     |    *      |
* *          | aaaaC2    |
* aaaaD1     | aaaaD2    |
# file<NNN>1 | file<NNN>2|
* fileZ1     | fileA1    |
* zzzzA1     | zzzzA1    |
* zzzzB1     |    *      |
* *          | zzzzC2    |
* zzzzD1     | zzzzD2    |
*/

void TestMount::prepareHugeSnapDiff(const std::string& name_prefix_start,
                                    const std::string& name_prefix_bulk,
                                    const std::string& name_prefix_end,
                                    size_t file_count,
                                    bool bulk_diff)
{
  //************ snap1 *************
  std::string startA = name_prefix_start + "A";
  std::string startB = name_prefix_start + "B";
  std::string startC = name_prefix_start + "C";
  std::string startD = name_prefix_start + "D";
  std::string endA = name_prefix_end + "A";
  std::string endB = name_prefix_end + "B";
  std::string endC = name_prefix_end + "C";
  std::string endD = name_prefix_end + "D";

  ASSERT_LE(0, write_full(startA.c_str(), "hello world"));
  ASSERT_LE(0, write_full(startB.c_str(), "hello world"));
  ASSERT_LE(0, write_full(startD.c_str(), "hello world"));
  for(size_t i = 0; i < file_count; i++) {
    auto s = name_prefix_bulk + stringify(i);
    ASSERT_LE(0, write_full(s.c_str(), "hello world"));
  }
  ASSERT_LE(0, write_full(endA.c_str(), "hello world"));
  ASSERT_LE(0, write_full(endB.c_str(), "hello world"));
  ASSERT_LE(0, write_full(endD.c_str(), "hello world"));

  ASSERT_EQ(0, mksnap("snap1"));

  ASSERT_LE(0, unlink(startB.c_str()));
  ASSERT_LE(0, write_full(startC.c_str(), "hello world2"));
  ASSERT_LE(0, write_full(startD.c_str(), "hello world2"));
  if (bulk_diff) {
    for(size_t i = 0; i < file_count; i++) {
      auto s = std::string(name_prefix_bulk) + stringify(i);
      ASSERT_LE(0, write_full(s.c_str(), "hello world2"));
    }
  }
  ASSERT_LE(0, unlink(endB.c_str()));
  ASSERT_LE(0, write_full(endC.c_str(), "hello world2"));
  ASSERT_LE(0, write_full(endD.c_str(), "hello world2"));
  ASSERT_EQ(0, mksnap("snap2"));
}

/*
* More versatile SnapDiff readdir API verification,
* includes 3 different snapshots and interleaving/repetitive calls to make sure
* the results aren't spoiled due to caching.
*/
TEST(LibCephFS, SnapDiffLib2)
{
  TestMount test_mount;

  test_mount.prepareSnapDiffLib2Cases();

  // Create simple directory tree with a couple of snapshots to test against
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

  // define a labda which verifies snap1/snap2/snap3 listings
  auto verify_snap_listing = [&]()
  {
    {
      string snap_path = test_mount.make_snap_path("snap1");

      std::cout << "---------snap1 listing verification---------" << std::endl;
      vector<string> expected;
      expected.push_back("fileA");
      expected.push_back("fileC");
      expected.push_back("fileD");
      expected.push_back("fileF");
      expected.push_back("fileG");
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
      expected.push_back("fileG");
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
  // Prepare expected delta for snap1 vs. snap2
  vector<pair<string, uint64_t>> snap1_2_diff_expected;
  snap1_2_diff_expected.emplace_back("fileA", snapid2);
  snap1_2_diff_expected.emplace_back("fileB", snapid2);
  snap1_2_diff_expected.emplace_back("fileC", snapid1);
  snap1_2_diff_expected.emplace_back("fileF", snapid1);
  snap1_2_diff_expected.emplace_back("fileG", snapid2);
  snap1_2_diff_expected.emplace_back("dirA", snapid2);
  snap1_2_diff_expected.emplace_back("dirB", snapid2);
  snap1_2_diff_expected.emplace_back("dirC", snapid1);
  snap1_2_diff_expected.emplace_back("dirD", snapid2);

  // Prepare expected delta for snap1 vs. snap3
  vector<pair<string, uint64_t>> snap1_3_diff_expected;
  snap1_3_diff_expected.emplace_back("fileA", snapid3);
  snap1_3_diff_expected.emplace_back("fileB", snapid3);
  snap1_3_diff_expected.emplace_back("fileC", snapid3);
  snap1_3_diff_expected.emplace_back("fileD", snapid3);
  snap1_3_diff_expected.emplace_back("fileE", snapid3);
  snap1_3_diff_expected.emplace_back("fileF", snapid1);
  snap1_3_diff_expected.emplace_back("fileG", snapid1);
  snap1_3_diff_expected.emplace_back("dirA", snapid1);
  snap1_3_diff_expected.emplace_back("dirC", snapid1);
  snap1_3_diff_expected.emplace_back("dirD", snapid3);

  // Prepare expected delta for snap2 vs. snap3
  vector<pair<string, uint64_t>> snap2_3_diff_expected;
  snap2_3_diff_expected.emplace_back("fileC", snapid3);
  snap2_3_diff_expected.emplace_back("fileD", snapid3);
  snap2_3_diff_expected.emplace_back("fileE", snapid3);
  snap2_3_diff_expected.emplace_back("fileG", snapid2);
  snap2_3_diff_expected.emplace_back("dirA", snapid2);
  snap2_3_diff_expected.emplace_back("dirB", snapid2);
  snap2_3_diff_expected.emplace_back("dirD", snapid3);

  // Check snapshot listings on a cold cache
  verify_snap_listing();

  // Check snapshot listings on a warm cache
  verify_snap_listing(); // served from cache

  // Print snap1 vs. snap2 delta against the root folder
  test_mount.print_snap_diff("", "snap1", "snap2");

  // Verify snap1 vs. snap2 delta for the root
  test_mount.verify_snap_diff(snap1_2_diff_expected, "", "snap1", "snap2");

  // Check snapshot listings on a warm cache once again
  // to make sure it wasn't spoiled by SnapDiff
  verify_snap_listing(); // served from cache

  // Verify snap2 vs. snap1 delta
  test_mount.verify_snap_diff(snap1_2_diff_expected, "", "snap2", "snap1");

  // Check snapshot listings on a warm cache once again
  // to make sure it wasn't spoiled by SnapDiff
  verify_snap_listing(); // served from cache

  // Verify snap1 vs. snap3 delta for the root
  test_mount.verify_snap_diff(snap1_3_diff_expected, "", "snap1", "snap3");

  // Verify snap2 vs. snap3 delta for the root
  test_mount.verify_snap_diff(snap2_3_diff_expected, "", "snap2", "snap3");

  // Check snapshot listings on a warm cache once again
  // to make sure it wasn't spoiled by SnapDiff
  verify_snap_listing(); // served from cache

  // Print snap1 vs. snap2 delta against /dirA folder
  test_mount.print_snap_diff("dirA", "snap1", "snap2");

  // Verify snap1 vs. snap2 delta for /dirA
  {
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileA", snapid2);
    test_mount.verify_snap_diff(expected, "dirA", "snap1", "snap2");
  }

  // Print snap1 vs. snap2 delta against /dirB folder
  test_mount.print_snap_diff("dirB", "snap1", "snap2");

  // Verify snap1 vs. snap2 delta for /dirB
  {
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileb", snapid2);
    test_mount.verify_snap_diff(expected, "dirB", "snap1", "snap2");
  }

  // Print snap1 vs. snap2 delta against /dirD folder
  test_mount.print_snap_diff("dirD", "snap1", "snap2");

  // Verify snap1 vs. snap2 delta for /dirD
  {
    vector<pair<string, uint64_t>> expected;
    test_mount.verify_snap_diff(expected, "dirD", "snap1", "snap2");
  }

  // Check snapshot listings on a warm cache once again
  // to make sure it wasn't spoiled by SnapDiff
  verify_snap_listing(); // served from cache

  // Verify snap1 vs. snap2 delta for the root once again
  test_mount.verify_snap_diff(snap1_2_diff_expected, "", "snap1", "snap2");

  // Verify snap2 vs. snap3 delta for the root once again
  test_mount.verify_snap_diff(snap2_3_diff_expected, "", "snap3", "snap2");

  // Verify snap1 vs. snap3 delta for the root once again
  test_mount.verify_snap_diff(snap1_3_diff_expected, "", "snap1", "snap3");

  std::cout << "------------- closing -------------" << std::endl;
  ASSERT_EQ(0, test_mount.purge_dir(""));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
  ASSERT_EQ(0, test_mount.rmsnap("snap3"));
}

/* The following method creates some files/folders/snapshots layout,
   described in the sheet below.
   We're to test SnapDiff against that structure.

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
void TestMount::prepareSnapDiffLib3Cases()
{
  //************ snap1 *************
  ASSERT_LE(0, write_full("a", "file 'a' v1"));
  ASSERT_LE(0, write_full("b", "file 'b' v1"));
  ASSERT_LE(0, write_full("c", "file 'c' v1"));
  ASSERT_LE(0, write_full("e", "file 'e' v1"));
  ASSERT_LE(0, write_full("~e", "file '~e' v1"));
  ASSERT_LE(0, write_full("f", "file 'f' v1"));
  ASSERT_LE(0, write_full("ff", "file 'ff' v1"));
  ASSERT_LE(0, write_full("g", "file 'g' v1"));
  ASSERT_LE(0, write_full("i", "file 'i' v1"));

  ASSERT_EQ(0, mkdir("S"));
  ASSERT_LE(0, write_full("S/sa", "file 'S/sa' v1"));

  ASSERT_EQ(0, mkdir("C"));
  ASSERT_LE(0, write_full("C/cc", "file 'C/cc' v1"));

  ASSERT_EQ(0, mkdir("C/CC"));
  ASSERT_LE(0, write_full("C/CC/c", "file 'C/CC/c' v1"));

  ASSERT_EQ(0, mkdir("G"));
  ASSERT_LE(0, write_full("G/gg", "file 'G/gg' v1"));

  ASSERT_EQ(0, mkdir("I"));
  ASSERT_LE(0, write_full("I/ii", "file 'I/ii' v1"));
  ASSERT_LE(0, write_full("I/iii", "file 'I/iii' v1"));

  ASSERT_EQ(0, mkdir("L"));
  ASSERT_LE(0, write_full("L/ll", "file 'L/ll' v1"));
  ASSERT_EQ(0, mkdir("L/LL"));

  ASSERT_EQ(0, mksnap("snap1"));
  //************ snap2 *************

  ASSERT_LE(0, write_full("b", "file 'b' v2"));
  ASSERT_EQ(0, unlink("c"));
  ASSERT_LE(0, write_full("d", "file 'd' v2"));
  ASSERT_LE(0, write_full("e", "file 'e' v2"));
  ASSERT_LE(0, write_full("~e", "file '~e' v2"));
  ASSERT_LE(0, write_full("f", "file 'f' v2"));
  ASSERT_EQ(0, unlink("g"));

  ASSERT_LE(0, write_full("S/sa", "file 'S/sa' v2"));

  ASSERT_EQ(0, mkdir("T"));
  ASSERT_LE(0, write_full("T/td", "file 'T/td' v2"));

  ASSERT_EQ(0, purge_dir("C"));
  ASSERT_EQ(0, purge_dir("G"));

  ASSERT_LE(0, write_full("k", "file 'k' v2"));
  ASSERT_LE(0, write_full("l", "file 'l' v2"));

  ASSERT_EQ(0, mkdir("K"));
  ASSERT_LE(0, write_full("K/kk", "file 'K/kk' v2"));

  ASSERT_LE(0, write_full("I/ii", "file 'I/ii' v2"));

  ASSERT_EQ(0, mkdir("I/J"));
  ASSERT_LE(0, write_full("I/J/i", "file 'I/J/i' v2"));
  ASSERT_LE(0, write_full("I/J/j", "file 'I/J/j' v2"));
  ASSERT_LE(0, write_full("I/J/k", "file 'I/J/k' v2"));

  ASSERT_LE(0, write_full("L/LL/ll", "file 'L/LL/ll' v2"));

  ASSERT_EQ(0, mkdir("L/LM"));
  ASSERT_LE(0, write_full("L/LM/lm", "file 'L/LM/lm' v2"));

  ASSERT_EQ(0, mkdir("L/LN"));

  ASSERT_EQ(0, mksnap("snap2"));
    //************ snap3 *************

  ASSERT_LE(0, write_full("a", "file 'a' v3"));
  ASSERT_LE(0, write_full("b", "file 'b' v3"));
  ASSERT_LE(0, write_full("d", "file 'd' v3"));
  ASSERT_EQ(0, unlink("e"));
  ASSERT_EQ(0, unlink("~e"));
  ASSERT_EQ(0, unlink("f"));
  ASSERT_EQ(0, unlink("ff"));
  ASSERT_LE(0, write_full("g", "file 'g' v3"));

  ASSERT_LE(0, write_full("S/sa", "file 'S/sa' v3"));

  ASSERT_LE(0, write_full("T/td", "file 'T/td' v3"));

  ASSERT_EQ(0, mkdir("G"));
  ASSERT_LE(0, write_full("G/gg", "file 'G/gg' v3"));

  ASSERT_EQ(0, unlink("k"));

  ASSERT_EQ(0, purge_dir("K"));

  ASSERT_EQ(0, mkdir("H"));
  ASSERT_LE(0, write_full("H/hh", "file 'H/hh' v3"));

  ASSERT_LE(0, write_full("I/ii", "file 'I/ii' v3"));
  ASSERT_LE(0, write_full("I/iii", "file 'I/iii' v3"));
  ASSERT_LE(0, write_full("I/iiii", "file 'I/iiii' v3"));

  ASSERT_LE(0, write_full("I/J/i", "file 'I/J/i' v3"));
  ASSERT_EQ(0, unlink("I/J/k"));
  ASSERT_LE(0, write_full("I/J/l", "file 'I/J/l' v3"));

  ASSERT_LE(0, write_full("L/ll", "file 'L/ll' v3"));

  ASSERT_LE(0, write_full("L/LL/ll", "file 'L/LL/ll' v3"));

  ASSERT_EQ(0, purge_dir("L/LM"));

  ASSERT_EQ(0, mksnap("snap3"));
  //************ head *************
  ASSERT_LE(0, write_full("a", "file 'a' head"));

  ASSERT_LE(0, write_full("h", "file 'h' head"));

  ASSERT_LE(0, write_full("S/sh", "file 'S/sh' head"));

  ASSERT_EQ(0, unlink("l"));

  ASSERT_EQ(0, purge_dir("I"));

  ASSERT_LE(0, write_full("L/LL/ll", "file 'L/LL/ll' head"));

  ASSERT_EQ(0, purge_dir("L/LN"));
}

//
// This case tests SnapDiff functionality for snap1/snap2 snapshot delta
// It operates against FS layout created by prepareSnapDiffCases() method,
// see relevant table before that function for FS state overview.
//
TEST(LibCephFS, SnapDiffCases1_2)
{
  TestMount test_mount;

  // Create directory tree evolving through a bunch of snapshots
  test_mount.prepareSnapDiffLib3Cases();

  uint64_t snapid1;
  uint64_t snapid2;
  ASSERT_EQ(0, test_mount.get_snapid("snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("snap2", &snapid2));
  std::cout << snapid1 << " vs. " << snapid2 << std::endl;
  ASSERT_GT(snapid1, 0);
  ASSERT_GT(snapid2, 0);
  ASSERT_GT(snapid2, snapid1);

  // Print snapshot delta (snap1 vs. snap2) results for root in a
  // human-readable form.
  test_mount.print_snap_diff("", "snap1", "snap2");

  {
    // Make sure the root delta is as expected
    // One should use columns snap1 and snap2 from
    // the table preceeding prepareSnapDiffCases() function
    // to learn which names to expect in the delta.
    //
    //  - file 'a' is unchanged hence not present in delta
    //  - file 'ff' is unchanged hence not present in delta
    //  - file 'i' is unchanged hence not present in delta
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("b", snapid2);  // file 'b' is updated in snap2
    expected.emplace_back("c", snapid1);  // file 'c' is removed in snap2
    expected.emplace_back("d", snapid2);  // file 'd' is created in snap2
    expected.emplace_back("e", snapid2);  // file 'e' is updated in snap2
    expected.emplace_back("~e", snapid2); // file '~e' is updated in snap2
    expected.emplace_back("f", snapid2);  // file 'f' is updated in snap2
    expected.emplace_back("g", snapid1);  // file 'g' is removed in snap2
    expected.emplace_back("S", snapid2);  // folder 'S' is present in snap2 hence reported
    expected.emplace_back("T", snapid2);  // folder 'T' is created in snap2
    expected.emplace_back("C", snapid1);  // folder 'C' is removed in snap2
    expected.emplace_back("G", snapid1);  // folder 'G' is removed in snap2
    expected.emplace_back("k", snapid2);  // file 'k' is created in snap2
    expected.emplace_back("l", snapid2);  // file 'l' is created in snap2
    expected.emplace_back("K", snapid2);  // folder 'K' is created in snap2
    expected.emplace_back("I", snapid2);  // folder 'I' is created in snap2
    expected.emplace_back("L", snapid2);  // folder 'L' is present in snap2 but got more
                                       // subfolders
    test_mount.verify_snap_diff(expected, "", "snap1", "snap2");
  }
  {

    //
    // Make sure snapshot delta for /S (existed at both snap1 and snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("sa", snapid2);
    test_mount.verify_snap_diff(expected, "S", "snap1", "snap2");
  }
  {
    //
    // Make sure snapshot delta for /T (created at snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("td", snapid2);
    test_mount.verify_snap_diff(expected, "T", "snap1", "snap2");
  }
  {
    //
    // Make sure snapshot delta for /C (removed at snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("cc", snapid1);
    expected.emplace_back("CC", snapid1);
    test_mount.verify_snap_diff(expected, "C", "snap2", "snap1");
  }
  {
    //
    // Make sure snapshot delta for /C/CC (removed at snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("c", snapid1);
    test_mount.verify_snap_diff(expected, "C/CC", "snap2", "snap1");
  }
  {
    //
    // Make sure snapshot delta for /I (created at snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("ii", snapid2);
    expected.emplace_back("J", snapid2);
    test_mount.verify_snap_diff(expected, "I", "snap1", "snap2");
  }
  {
    //
    // Make sure snapshot delta for /I/J (created at snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("i", snapid2);
    expected.emplace_back("j", snapid2);
    expected.emplace_back("k", snapid2);
    test_mount.verify_snap_diff(expected, "I/J", "snap1", "snap2");
  }
  {
    //
    // Make sure snapshot delta for /L (extended at snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("LL", snapid2);
    expected.emplace_back("LM", snapid2);
    expected.emplace_back("LN", snapid2);
    test_mount.verify_snap_diff(expected, "L", "snap1", "snap2");
  }
  {
    //
    // Make sure snapshot delta for /L/LL (updated at snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("ll", snapid2);
    test_mount.verify_snap_diff(expected, "L/LL", "snap1", "snap2");
  }
  {
    //
    // Make sure snapshot delta for /L/LN (created empty at snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    test_mount.verify_snap_diff(expected, "L/LN", "snap1", "snap2");
  }

  {
    // Make sure snapshot delta for /L/LM (created at snap2)
    // is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("lm", snapid2);
    test_mount.verify_snap_diff(expected, "L/LM", "snap1", "snap2");
  }
  std::cout << "-------------" << std::endl;

  test_mount.remove_all();
  test_mount.rmsnap("snap1");
  test_mount.rmsnap("snap2");
  test_mount.rmsnap("snap3");
}

//
// This case tests SnapDiff functionality for snap2/snap3 snapshot delta
// retrieved through .snap path-based query API.
// It operates against FS layout created by prepareSnapDiffCases() method,
// see relevant table before that function for FS state overview.
//
TEST(LibCephFS, SnapDiffCases2_3)
{
  TestMount test_mount;

  // Create directory tree evolving through a bunch of snapshots
  test_mount.prepareSnapDiffLib3Cases();

  uint64_t snapid2;
  uint64_t snapid3;
  ASSERT_EQ(0, test_mount.get_snapid("snap2", &snapid2));
  ASSERT_EQ(0, test_mount.get_snapid("snap3", &snapid3));
  std::cout << snapid2 << " vs. " << snapid3 << std::endl;
  ASSERT_GT(snapid3, 0);
  ASSERT_GT(snapid3, 0);
  ASSERT_GT(snapid3, snapid2);

  // Print snapshot delta (snap2 vs. snap3) results for root in a
  // human-readable form.
  test_mount.print_snap_diff("", "snap2", "snap3");

  {
    // Make sure the root delta is as expected
    // One should use columns snap1 and snap2 from
    // the table preceeding prepareSnapDiffCases() function
    // to learn which names to expect in the delta.
    //
    //  - file 'c' is removed since snap1 hence not present in delta
    //  - file 'l' is unchanged hence not present in delta
    //  - file 'i' is unchanged hence not present in delta
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("a", snapid3);   // file 'a' is updated in snap3
    expected.emplace_back("b", snapid3);   // file 'b' is updated in snap3
    expected.emplace_back("d", snapid3);   // file 'd' is updated in snap3
    expected.emplace_back("~e", snapid2);  // file '~e' is removed in snap3
    expected.emplace_back("e", snapid2);   // file 'e' is removed in snap3
    expected.emplace_back("f", snapid2);   // file 'f' is removed in snap3
    expected.emplace_back("ff", snapid2);  // file 'ff' is removed in snap3
    expected.emplace_back("g", snapid3);   // file 'g' re-appeared in snap3
    expected.emplace_back("S", snapid3);   // folder 'S' is present in snap3 hence reported
    expected.emplace_back("T", snapid3);   // folder 'T' is present in snap3 hence reported
    expected.emplace_back("G", snapid3);   // folder 'G' re-appeared in snap3 hence reported
    expected.emplace_back("k", snapid2);   // file 'k' is removed in snap3
    expected.emplace_back("K", snapid2);   // folder 'K' is removed in snap3
    expected.emplace_back("H", snapid3);   // folder 'H' is created in snap3 hence reported
    expected.emplace_back("I", snapid3);   // folder 'I' is present in snap3 hence reported
    expected.emplace_back("L", snapid3);   // folder 'L' is present in snap3 hence reported
    test_mount.verify_snap_diff(expected, "", "snap2", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /S (children updated) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("sa", snapid3);
    test_mount.verify_snap_diff(expected, "S", "snap2", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /T (children updated) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("td", snapid3);
    test_mount.verify_snap_diff(expected, "T", "snap2", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /G (re-appeared) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("gg", snapid3);
    test_mount.verify_snap_diff(expected, "G", "snap2", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /K (removed) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("kk", snapid2);
    test_mount.verify_snap_diff(expected, "K", "snap3", "snap2");
  }
  {
    //
    // Make sure snapshot delta for /H (created) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("hh", snapid3);
    test_mount.verify_snap_diff(expected, "H", "snap2", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /I (children updated) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("ii", snapid3);
    expected.emplace_back("iii", snapid3);
    expected.emplace_back("iiii", snapid3);
    expected.emplace_back("J", snapid3);
    test_mount.verify_snap_diff(expected, "I", "snap2", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /I/J (children updated/removed) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("i", snapid3);
    expected.emplace_back("k", snapid2);
        expected.emplace_back("l", snapid3);
    test_mount.verify_snap_diff(expected, "I/J", "snap2", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /L (children updated/removed) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("ll", snapid3);
    expected.emplace_back("LL", snapid3);
    expected.emplace_back("LM", snapid2);
    expected.emplace_back("LN", snapid3);
    test_mount.verify_snap_diff(expected, "L", "snap2", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /L/LL (children updated) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("ll", snapid3);
    test_mount.verify_snap_diff(expected, "L/LL", "snap2", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /L/LM (removed) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("lm", snapid2);
    test_mount.verify_snap_diff(expected, "L/LM", "snap3", "snap2");
  }
  {
    //
    // Make sure snapshot delta for /L/LN (created empty) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    test_mount.verify_snap_diff(expected, "L/LN", "snap2", "snap3");
  }
  test_mount.remove_all();
  test_mount.rmsnap("snap1");
  test_mount.rmsnap("snap2");
  test_mount.rmsnap("snap3");
}

//
// This case tests SnapDiff functionality for snap1/snap3 snapshot delta
// retrieved through .snap path-based query API.
// It operates against FS layout created by prepareSnapDiffCases() method,
// see relevant table before that function for FS state overview.
//
TEST(LibCephFS, SnapDiffCases1_3)
{
  TestMount test_mount;

  // Create directory tree evolving through a bunch of snapshots
  test_mount.prepareSnapDiffLib3Cases();

  uint64_t snapid1;
  uint64_t snapid3;
  ASSERT_EQ(0, test_mount.get_snapid("snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("snap3", &snapid3));
  std::cout << snapid1 << " vs. " << snapid3 << std::endl;
  ASSERT_GT(snapid3, 0);
  ASSERT_GT(snapid3, 0);
  ASSERT_GT(snapid3, snapid1);

  // Print snapshot delta (snap2 vs. snap3) results for root in a
  // human-readable form.
  test_mount.print_snap_diff("", "snap1", "snap3");

  {
    // Make sure the root delta is as expected
    // One should use columns snap1 and snap3 from
    // the table preceeding prepareSnapDiffCases() function
    // to learn which names to expect in the delta.
    //
    //  - file 'i' is unchanged hence not present in delta
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("a", snapid3);  // file 'a' is updated in snap3
    expected.emplace_back("b", snapid3);  // file 'b' is updated in snap3
    expected.emplace_back("c", snapid1); // file 'c' is removed in snap2
    expected.emplace_back("d", snapid3);  // file 'd' is updated in snap3
    expected.emplace_back("~e", snapid1); // file '~e' is removed in snap3
    expected.emplace_back("e", snapid1);  // file 'e' is removed in snap3
    expected.emplace_back("f", snapid1);  // file 'f' is removed in snap3
    expected.emplace_back("ff", snapid1); // file 'ff' is removed in snap3
    expected.emplace_back("g", snapid3);  // file 'g' removed in snap2 and
                                          // re-appeared in snap3
    expected.emplace_back("S", snapid3);  // folder 'S' is present in snap3 hence reported
    expected.emplace_back("T", snapid3);  // folder 'T' is present in snap3 hence reported
    expected.emplace_back("C", snapid1);  // folder 'C' is removed in snap2

    // folder 'G' is removed in snap2 and re-appeared in snap3
    // hence reporting it twice under different snapid
    expected.emplace_back("G", snapid1);
    expected.emplace_back("G", snapid3);

    expected.emplace_back("l", snapid3);   // file 'l' is created in snap2
    expected.emplace_back("H", snapid3);   // folder 'H' is created in snap3 hence reported
    expected.emplace_back("I", snapid3);   // folder 'I' is created in snap3 hence reported
    expected.emplace_back("L", snapid3);   // folder 'L' is created in snap3 hence reported
    test_mount.verify_snap_diff(expected, "", "snap3", "snap1");
  }
  {
    //
    // Make sure snapshot delta for /S (children updated) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("sa", snapid3);
    test_mount.verify_snap_diff(expected, "S", "snap3", "snap1");
  }
  {
    //
    // Make sure snapshot delta for /T (created and children updated) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("td", snapid3);
    test_mount.verify_snap_diff(expected, "T", "snap3", "snap1");
  }
  {
    //
    // Make sure snapshot delta for /C (removed) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("cc", snapid1);
    expected.emplace_back("CC", snapid1);
    test_mount.verify_snap_diff(expected, "C", "snap3", "snap1");
  }
  {
    //
    // Make sure snapshot delta for /C/CC (removed) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("c", snapid1);
    test_mount.verify_snap_diff(expected, "C/CC", "snap3", "snap1");
  }
  {
    //
    // Make sure snapshot delta for /G (removed) is as expected
    // For this case (G@snap1 and G@snap3 are different entries)
    // the order in which snapshot names are provided is crucial.
    // Making  G@snap1 vs. snap3 delta returns everything from G@snap1
    // but omits any entries from G/snap3 (since it's a different entry).
    // And making  G@snap3 vs. snap1 delta returns everything from G@snap3
    // but nothing from snap1,

    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("gg", snapid1);
    test_mount.verify_snap_diff(expected, "G", "snap1", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /G (re-created) is as expected
    // The snapshot names order is important, see above.
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("gg", snapid3);
    test_mount.verify_snap_diff(expected, "G", "snap3", "snap1");
  }
  {
    //
    // Make sure snapshot delta for /H (created) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("hh", snapid3);
    test_mount.verify_snap_diff(expected, "H", "snap1", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /I (chinldren updated) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("ii", snapid3);
    expected.emplace_back("iii", snapid3);
    expected.emplace_back("iiii", snapid3);
    expected.emplace_back("J", snapid3);
    test_mount.verify_snap_diff(expected, "I", "snap1", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /I/J (created at snap2) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("i", snapid3);
    expected.emplace_back("j", snapid3);
    expected.emplace_back("l", snapid3);
    test_mount.verify_snap_diff(expected, "I/J", "snap1", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /L is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("ll", snapid3);
    expected.emplace_back("LL", snapid3);
    expected.emplace_back("LN", snapid3);
    test_mount.verify_snap_diff(expected, "L", "snap1", "snap3");
  }
  {
    //
    // Make sure snapshot delta for /L/LL (children updated) is as expected
    //
    vector<std::pair<string, uint64_t>> expected;
    expected.emplace_back("ll", snapid3);
    test_mount.verify_snap_diff(expected, "L/LL", "snap1", "snap3");
  }
  {
    vector<std::pair<string, uint64_t>> expected;
    test_mount.verify_snap_diff(expected, "L/LN", "snap1", "snap3");
  }
  std::cout << "-------------" << std::endl;

  test_mount.remove_all();
  test_mount.rmsnap("snap1");
  test_mount.rmsnap("snap2");
  test_mount.rmsnap("snap3");
}

/*
* SnapDiff readdir API testing for huge dir
* when delta is minor.
*/
TEST(LibCephFS, HugeSnapDiffSmallDelta)
{
  TestMount test_mount;

  long int file_count = 10000;
  printf("Seeding %ld files...\n", file_count);

  // Create simple directory tree with a couple of snapshots
  // to test against.
  string name_prefix_start = "aaaa";
  string name_prefix_bulk = "file";
  string name_prefix_end = "zzzz";
  test_mount.prepareHugeSnapDiff(name_prefix_start,
                                 name_prefix_bulk,
                                 name_prefix_end,
                                 file_count,
                                 false);

  uint64_t snapid1;
  uint64_t snapid2;

  // learn snapshot ids and do basic verification
  ASSERT_EQ(0, test_mount.get_snapid("snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("snap2", &snapid2));
  ASSERT_GT(snapid1, 0);
  ASSERT_GT(snapid2, 0);
  ASSERT_GT(snapid2, snapid1);
  std::cout << snapid1 << " vs. " << snapid2 << std::endl;

  //
  // Make sure snap1 vs. snap2 delta for the root is as expected
  //
  {
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back(name_prefix_start + "B", snapid1);
    expected.emplace_back(name_prefix_start + "C", snapid2);
    expected.emplace_back(name_prefix_start + "D", snapid2);

    expected.emplace_back(name_prefix_end + "B", snapid1);
    expected.emplace_back(name_prefix_end + "C", snapid2);
    expected.emplace_back(name_prefix_end + "D", snapid2);
    test_mount.verify_snap_diff(expected, "", "snap1", "snap2");
  }

  std::cout << "------------- closing -------------" << std::endl;
  ASSERT_EQ(0, test_mount.purge_dir(""));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
}

/*
* SnapDiff readdir API testing for huge dir
* when delta is large
*/
TEST(LibCephFS, HugeSnapDiffLargeDelta)
{
  TestMount test_mount;

  // Calculate amount of files required to have multiple directory fragments
  // using relevant config parameters.
  // file_count = mds_bal_spli_size * mds_bal_fragment_fast_factor + 100
  char buf[256];
  int r = test_mount.conf_get("mds_bal_split_size", buf, sizeof(buf));
  ASSERT_TRUE(r >= 0);
  long int file_count = strtol(buf, nullptr, 10);
  r = test_mount.conf_get("mds_bal_fragment_fast_factor ", buf, sizeof(buf));
  ASSERT_TRUE(r >= 0);
  double factor = strtod(buf, nullptr);
  file_count *= factor;
  file_count += 100;
  printf("Seeding %ld files...\n", file_count);

  // Create simple directory tree with a couple of snapshots
  // to test against.

  string name_prefix_start = "aaaa";
  string name_prefix_bulk = "file";
  string name_prefix_end = "zzzz";

  test_mount.prepareHugeSnapDiff(name_prefix_start,
                                 name_prefix_bulk,
                                 name_prefix_end,
                                 file_count,
                                 true);
  uint64_t snapid1;
  uint64_t snapid2;

  // learn snapshot ids and do basic verification
  ASSERT_EQ(0, test_mount.get_snapid("snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("snap2", &snapid2));
  ASSERT_GT(snapid1, 0);
  ASSERT_GT(snapid2, 0);
  ASSERT_GT(snapid2, snapid1);
  std::cout << snapid1 << " vs. " << snapid2 << std::endl;

  //
  // Make sure snap1 vs. snap2 delta for the root is as expected
  //
  {
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back(name_prefix_start + "B", snapid1);
    expected.emplace_back(name_prefix_start + "C", snapid2);
    expected.emplace_back(name_prefix_start + "D", snapid2);
    for (size_t i = 0; i < (size_t)file_count; i++) {
      expected.emplace_back(name_prefix_bulk + stringify(i), snapid2);
    }
    expected.emplace_back(name_prefix_end + "B", snapid1);
    expected.emplace_back(name_prefix_end + "C", snapid2);
    expected.emplace_back(name_prefix_end + "D", snapid2);
    test_mount.verify_snap_diff(expected, "", "snap1", "snap2");
  }

  std::cout << "------------- closing -------------" << std::endl;
  ASSERT_EQ(0, test_mount.purge_dir(""));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
}
