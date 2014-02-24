// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <sys/socket.h>
#include <linux/un.h>
#include <unistd.h>
#include <string.h>

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/config.h"
#include "tools/common.h"

#include "include/compat.h"
#include "include/assert.h"

using std::vector;

void do_status(CephToolCtx *ctx, bool shutdown = false);

static void usage()
{
  cout << "usage:\n";
  cout << " ceph [options] [command]\n";
  cout << " ceph -s     cluster status summary\n";
  cout << " ceph -w     running cluster summary and events\n";
  cout << "\n";
  cout << "If no commands are specified, enter interactive mode.\n";
  cout << "\n";
  cout << "CLUSTER COMMANDS\n";
  cout << "  ceph health [detail]\n";
  cout << "  ceph quorum_status\n";
  cout << "  ceph df [detail]\n";
  cout << "  ceph -m <mon-ip-or-host> mon_status\n";
  cout << "\n";
  cout << "AUTHENTICATION (AUTH) COMMANDS\n";
  cout << "  ceph auth get-or-create[-key] <name> [capsys1 capval1 [...]]\n";
  cout << "  ceph auth del <name>\n";
  cout << "  ceph auth list\n";
  cout << "\n";
  cout << "METADATA SERVER (MDS) COMMANDS\n";
  cout << "  ceph mds stat\n";
  cout << "  ceph mds tell <mds-id or *> injectargs '--<switch> <value> [--<switch> <value>...]'\n";
  cout << "  ceph mds add_data_pool <pool-id>\n";
  cout << "\n";
  cout << "MONITOR (MON) COMMANDS\n";
  cout << "  ceph mon add <name> <ip>[:<port>]\n";
  cout << "  ceph mon remove <name>\n";
  cout << "  ceph mon stat\n";
  cout << "  ceph mon tell <mon-id or *> injectargs '--<switch> <value> [--<switch> <value>...]'\n";
  cout << "\n";
  cout << "OBJECT STORAGE DEVICE (OSD) COMMANDS\n";
  cout << "  ceph osd dump [--format=json]\n";
  cout << "  ceph osd ls [--format=json]\n";
  cout << "  ceph osd tree\n";
  cout << "  ceph osd map <pool-name> <object-name>\n";
  cout << "  ceph osd down <osd-id>\n";
  cout << "  ceph osd in <osd-id>\n";
  cout << "  ceph osd out <osd-id>\n";
  cout << "  ceph osd set <noout|noin|nodown|noup|noscrub|nodeep-scrub>\n";
  cout << "  ceph osd unset <noout|noin|nodown|noup|noscrub|nodeep-scrub>\n";
  cout << "  ceph osd pause\n";
  cout << "  ceph osd unpause\n";
  cout << "  ceph osd tell <osd-id or *> injectargs '--<switch> <value> [--<switch> <value>...]'\n";
  cout << "  ceph osd getcrushmap -o <file>\n";
  cout << "  ceph osd getmap -o <file>\n";
  cout << "  ceph osd crush set <osd-id> <weight> <loc1> [<loc2> ...]\n";
  cout << "  ceph osd crush add <osd-id> <weight> <loc1> [<loc2> ...]\n";
  cout << "  ceph osd crush create-or-move <osd-id> <initial-weight> <loc1> [<loc2> ...]\n";
  cout << "  ceph osd crush rm <name> [ancestor]\n";
  cout << "  ceph osd crush move <bucketname> <loc1> [<loc2> ...]\n";
  cout << "  ceph osd crush link <bucketname> <loc1> [<loc2> ...]\n";
  cout << "  ceph osd crush unlink <bucketname> [ancestor]\n";
  cout << "  ceph osd crush add-bucket <bucketname> <type>\n";
  cout << "  ceph osd crush reweight <name> <weight>\n";
  cout << "  ceph osd crush tunables <legacy|argonaut|bobtail|optimal|default>\n";
  cout << "  ceph osd crush rule list\n";
  cout << "  ceph osd crush rule dump\n";
  cout << "  ceph osd crush rule create-simple <name> <root> <failure-domain>\n";
  cout << "  ceph osd create [<uuid>]\n";
  cout << "  ceph osd rm <osd-id> [<osd-id>...]\n";
  cout << "  ceph osd lost <osd-id> [--yes-i-really-mean-it]\n";
  cout << "  ceph osd reweight <osd-id> <weight>\n";
  cout << "  ceph osd blacklist add <address>[:source_port] [time]\n";
  cout << "  ceph osd blacklist rm <address>[:source_port]\n";
  cout << "  ceph osd blacklist ls\n";
  cout << "  ceph osd pool mksnap <pool> <snapname>\n";
  cout << "  ceph osd pool rmsnap <pool> <snapname>\n";
  cout << "  ceph osd pool create <pool> <pg_num> [<pgp_num>]\n";
  cout << "  ceph osd pool delete <pool> [<pool> --yes-i-really-really-mean-it]\n";
  cout << "  ceph osd pool rename <pool> <new pool name>\n";
  cout << "  ceph osd pool set <pool> <field> <value>\n";
  cout << "  ceph osd pool set-quota <pool> (max_bytes|max_objects) <value>\n";
  cout << "  ceph osd scrub <osd-id>\n";
  cout << "  ceph osd deep-scrub <osd-id>\n";
  cout << "  ceph osd repair <osd-id>\n";
  cout << "  ceph osd tell <osd-id or *> bench [bytes per write] [total bytes]\n";
  cout << "\n";
  cout << "PLACEMENT GROUP (PG) COMMANDS\n";
  cout << "  ceph pg dump\n";
  cout << "  ceph pg <pg-id> query\n";
  cout << "  ceph pg scrub <pg-id>\n";
  cout << "  ceph pg deep-scrub <pg-id>\n";
  cout << "  ceph pg map <pg-id>\n";
  cout << "\n";
  cout << "OPTIONS\n";
  cout << "  -o <file>        Write out to <file>\n";
  cout << "  -i <file>        Read input from <file> (for some commands)\n";
  generic_client_usage(); // Will exit()
}

static void parse_cmd_args(vector<const char*> &args,
		std::string *in_file, std::string *out_file,
			   ceph_tool_mode_t *mode, bool *concise,
			   string *admin_socket, string *admin_socket_cmd,
			   string *watch_level)
{
  std::vector<const char*>::iterator i;
  std::string val;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--in-file", (char*)NULL)) {
      *in_file = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-o", "--out-file", (char*)NULL)) {
      *out_file = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--admin-daemon", (char*)NULL)) {
      *admin_socket = val;
      if (i == args.end())
	usage();
      *admin_socket_cmd = *i++;
    } else if (ceph_argparse_flag(args, i, "-s", "--status", (char*)NULL)) {
      *mode = CEPH_TOOL_MODE_STATUS;
    } else if (ceph_argparse_flag(args, i, "-w", "--watch", (char*)NULL)) {
      *mode = CEPH_TOOL_MODE_WATCH;
    } else if (ceph_argparse_flag(args, i, "--watch-debug", (char*) NULL)) {
      *watch_level = "log-debug";
    } else if (ceph_argparse_flag(args, i, "--watch-info", (char*) NULL)) {
      *watch_level = "log-info";
    } else if (ceph_argparse_flag(args, i, "--watch-sec", (char*) NULL)) {
      *watch_level = "log-sec";
    } else if (ceph_argparse_flag(args, i, "--watch-warn", (char*) NULL)) {
      *watch_level = "log-warn";
    } else if (ceph_argparse_flag(args, i, "--watch-error", (char*) NULL)) {
      *watch_level = "log-error";
    } else if (ceph_argparse_flag(args, i, "--concise", (char*)NULL)) {
      *concise = true;
    } else if (ceph_argparse_flag(args, i, "--verbose", (char*)NULL)) {
      *concise = false;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
    } else {
      if (admin_socket_cmd && admin_socket_cmd->length()) {
	*admin_socket_cmd += " " + string(*i);
      }
      ++i;
    }
  }
}

static int get_indata(const char *in_file, bufferlist &indata)
{
  int fd = VOID_TEMP_FAILURE_RETRY(::open(in_file, O_RDONLY));
  if (fd < 0) {
    int err = errno;
    derr << "error opening in_file '" << in_file << "': "
	 << cpp_strerror(err) << dendl;
    return 1;
  }
  struct stat st;
  if (::fstat(fd, &st)) {
    int err = errno;
    derr << "error getting size of in_file '" << in_file << "': "
	 << cpp_strerror(err) << dendl;
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return 1;
  }

  indata.push_back(buffer::create(st.st_size));
  indata.zero();
  int ret = safe_read_exact(fd, indata.c_str(), st.st_size);
  if (ret) {
    derr << "error reading in_file '" << in_file << "': "
	 << cpp_strerror(ret) << dendl;
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return 1;
  }

  VOID_TEMP_FAILURE_RETRY(::close(fd));
  derr << "read " << st.st_size << " bytes from " << in_file << dendl;
  return 0;
}

int do_admin_socket(string path, string cmd)
{
  struct sockaddr_un address;
  int fd;
  int r;
  
  fd = socket(PF_UNIX, SOCK_STREAM, 0);
  if(fd < 0) {
    cerr << "socket failed with " << cpp_strerror(errno) << std::endl;
    return -1;
  }

  memset(&address, 0, sizeof(struct sockaddr_un));
  address.sun_family = AF_UNIX;
  snprintf(address.sun_path, UNIX_PATH_MAX, "%s", path.c_str());

  if (connect(fd, (struct sockaddr *) &address, 
	      sizeof(struct sockaddr_un)) != 0) {
    cerr << "connect to " << path << " failed with " << cpp_strerror(errno) << std::endl;
    ::close(fd);
    return -1;
  }
  
  char *buf = NULL;
  uint32_t len;
  r = safe_write(fd, cmd.c_str(), cmd.length() + 1);
  if (r < 0) {
    cerr << "write to " << path << " failed with " << cpp_strerror(errno) << std::endl;
    goto out;
  }
  
  r = safe_read(fd, &len, sizeof(len));
  if (r < 0) {
    cerr << "read " << len << " length from " << path << " failed with " << cpp_strerror(errno) << std::endl;
    goto out;
  }
  if (r < 4) {
    cerr << "read only got " << r << " bytes of 4 expected for response length; invalid command?" << std::endl;
    r = -1;
    goto out;
  }
  len = ntohl(len);

  buf = new char[len+1];
  r = safe_read(fd, buf, len);
  if (r < 0) {
    cerr << "read " << len << " bytes from " << path << " failed with " << cpp_strerror(errno) << std::endl;
    goto out;
  }
  buf[len] = '\0';

  cout << buf << std::endl;
  r = 0;

 out:
  if (buf)
    delete[] buf;
  ::close(fd);
  return r;
}


int main(int argc, const char **argv)
{
  std::string in_file, out_file;
  enum ceph_tool_mode_t mode = CEPH_TOOL_MODE_CLI_INPUT;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  // initialize globals
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // parse user input
  bool concise = true;
  string admin_socket;
  string admin_socket_cmd;
  string watch_level = "log-info";
  parse_cmd_args(args, &in_file, &out_file, &mode, &concise, 
		 &admin_socket, &admin_socket_cmd, &watch_level);

  // daemon admin socket?
  if (admin_socket.length()) {
    return do_admin_socket(admin_socket, admin_socket_cmd);
  }

  // input
  bufferlist indata;
  if (!in_file.empty()) {
    if (get_indata(in_file.c_str(), indata)) {
      derr << "failed to get data from '" << in_file << "'" << dendl;
      return 1;
    }
  }

  CephToolCtx *ctx = ceph_tool_common_init(mode, concise);
  if (!ctx) {
    derr << "ceph_tool_common_init failed." << dendl;
    return 1;
  }
  signal(SIGINT, SIG_DFL);
  signal(SIGTERM, SIG_DFL);

  bufferlist outbl;
  int ret = 0;
  switch (mode) {
    case CEPH_TOOL_MODE_STATUS:
      do_status(ctx, true);
      break;
    case CEPH_TOOL_MODE_WATCH: {
      do_status(ctx);
      ctx->lock.Lock();
      ctx->dispatcher->subs.name = watch_level;
      ctx->dispatcher->subs.last_known_version = 0;
      ctx->mc.sub_want(watch_level, 0, 0);
      ctx->mc.renew_subs();
      ctx->lock.Unlock();
      break;
    }

    case CEPH_TOOL_MODE_CLI_INPUT: {
      if (args.empty()) {
	if (ceph_tool_do_cli(ctx))
	  ret = 1;
      } else {
	while (!args.empty()) {
	  vector<string> cmd;
	  for (vector<const char*>::iterator n = args.begin();
	       n != args.end(); ) {
	    std::string np(*n);
	    n = args.erase(n);
	    if (np == ";")
	      break;
	    cmd.push_back(np);
	  }

	  bufferlist obl;
	  ret = do_command(ctx, cmd, indata, obl);
	  if (ret < 0) {
	    ret = -ret;
	    break;
	  }
	  outbl.claim(obl);
	}
      }
      if (ceph_tool_messenger_shutdown())
	ret = 1;
      break;
    }

    default: {
      derr << "logic error: illegal ceph command mode " << mode << dendl;
      ret = 1;
      break;
    }
  }
 
  if (ret == 0 && outbl.length()) {
    // output
    int err;
    if (out_file.empty() || out_file == "-") {
      err = outbl.write_fd(STDOUT_FILENO);
    } else {
      int out_fd = VOID_TEMP_FAILURE_RETRY(::open(out_file.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644));
      if (out_fd < 0) {
	int ret = errno;
	derr << " failed to create file '" << out_file << "': "
	     << cpp_strerror(ret) << dendl;
	return 1;
      }
      err = outbl.write_fd(out_fd);
      ::close(out_fd);
    }
    if (err) {
      derr << " failed to write " << outbl.length() << " bytes to " << out_file << ": "
	   << cpp_strerror(err) << dendl;
      ret = 1;
    } else if (!concise && !out_file.empty())
      cerr << " wrote " << outbl.length() << " byte payload to " << out_file << std::endl;
  }

  if (ceph_tool_common_shutdown(ctx))
    ret = 1;
  return ret;
}
