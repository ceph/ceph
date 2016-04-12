// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"

#include "os/bluestore/BlueFS.h"

void usage(char **argv)
{
  cout << argv[0] << " <outdir> <bdev[0..2]>" << std::endl;;
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf->apply_changes(NULL);

  BlueFS fs;

  if (args.size() != 4) {
    usage(argv);
    exit(-1);
  }

  cout << "args " << args << std::endl;
  string outdir = args[0];
  for (unsigned i = 1; i < args.size(); ++i) {
    fs.add_block_device(i-1, args[i]);
  }

  int r = fs.mount();
  assert(r == 0);

  vector<string> dirs;
  r = fs.readdir("", &dirs);
  assert(r == 0);
  for (auto& dir : dirs) {
    if (dir[0] == '.')
      continue;
    cout << dir << "/" << std::endl;
    vector<string> ls;
    r = fs.readdir(dir, &ls);
    assert(r == 0);
    string cmd = "mkdir -p " + outdir + "/" + dir;
    r = system(cmd.c_str());
    assert(r == 0);
    for (auto& file : ls) {
      if (file[0] == '.')
	continue;
      cout << dir << "/" << file << std::endl;
      uint64_t size;
      utime_t mtime;
      r = fs.stat(dir, file, &size, &mtime);
      assert(r == 0);
      string path = outdir + "/" + dir + "/" + file;
      int fd = ::open(path.c_str(), O_CREAT|O_WRONLY|O_TRUNC, 0644);
      assert(fd >= 0);
      if (size > 0) {
	BlueFS::FileReader *h;
	r = fs.open_for_read(dir, file, &h, false);
	assert(r == 0);
	int pos = 0;
	int left = size;
	while (left) {
	  bufferlist bl;
	  r = fs.read(h, &h->buf, pos, left, &bl, NULL);
	  assert(r > 0);
	  int rc = bl.write_fd(fd);
	  assert(rc == 0);
	  pos += r;
	  left -= r;
	}
	delete h;
      }
      ::close(fd);
    }
  }
}
