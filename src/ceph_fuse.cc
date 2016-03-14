// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sys/stat.h>
#include <sys/utsname.h>
#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"
#include "common/errno.h"

#include "client/Client.h"
#include "client/fuse_ll.h"

#include "msg/Messenger.h"

#include "mon/MonClient.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#if defined(__linux__)
#include "common/linux_version.h"
#endif
#include "global/global_init.h"
#include "common/safe_io.h"
       
#include <sys/types.h>
#include <fcntl.h>

#include <fuse.h>

static void fuse_usage()
{
  const char **argv = (const char **) malloc((2) * sizeof(char *));
  argv[0] = "ceph-fuse";
  argv[1] = "-h";
  struct fuse_args args = FUSE_ARGS_INIT(2, (char**)argv);
  if (fuse_parse_cmdline(&args, NULL, NULL, NULL) == -1) {
    derr << "fuse_parse_cmdline failed." << dendl;
    fuse_opt_free_args(&args);
  }

  assert(args.allocated);  // Checking fuse has realloc'd args so we can free newargv
  free(argv);
}
void usage()
{
  cout <<
"usage: ceph-fuse [-m mon-ip-addr:mon-port] <mount point> [OPTIONS]\n"
"  --client_mountpoint/-r <root_directory>\n"
"                    use root_directory as the mounted root, rather than the full Ceph tree.\n"
"\n";
  fuse_usage();
  generic_client_usage();
}

int main(int argc, const char **argv, const char *envp[]) {
  int filer_flags = 0;
  //cerr << "ceph-fuse starting " << myrank << "/" << world << std::endl;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    usage();
    return 0;
  }
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "--localize-reads", (char*)NULL)) {
      cerr << "setting CEPH_OSD_FLAG_LOCALIZE_READS" << std::endl;
      filer_flags |= CEPH_OSD_FLAG_LOCALIZE_READS;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      assert(0);
    } else {
      ++i;
    }
  }

  // args for fuse
  const char **newargv;
  int newargc;
  vec_to_argv(argv[0], args, &newargc, &newargv);

  // FUSE will chdir("/"); be ready.
  g_ceph_context->_conf->set_val("chdir", "/");
  g_ceph_context->_conf->apply_changes(NULL);

  // check for 32-bit arch
  if (sizeof(long) == 4) {
    cerr << std::endl;
    cerr << "WARNING: Ceph inode numbers are 64 bits wide, and FUSE on 32-bit kernels does" << std::endl;
    cerr << "         not cope well with that situation.  Expect to crash shortly." << std::endl;
    cerr << std::endl;
  }

  // we need to handle the forking ourselves.
  int fd[2] = {0, 0};  // parent's, child's
  pid_t childpid = 0;
  int tester_r = 0;
  void *tester_rp = NULL;
  bool restart_log = false;
  if (g_conf->daemonize) {
    int r = socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
    if (r < 0) {
      cerr << "ceph-fuse[" << getpid() << "]: unable to create socketpair: " << cpp_strerror(errno) << std::endl;
      exit(1);
    }

    g_ceph_context->_log->stop();
    restart_log = true;

    childpid = fork();
  }

  if (childpid == 0) {
    if (restart_log)
      g_ceph_context->_log->start();
    common_init_finish(g_ceph_context);

    //cout << "child, mounting" << std::endl;
    ::close(fd[0]);

    class RemountTest : public Thread {
    public:
      CephFuse *cfuse;
      Client *client;
      RemountTest() : cfuse(NULL), client(NULL) {}
      void init(CephFuse *cf, Client *cl) {
	cfuse = cf;
	client = cl;
      }
      virtual ~RemountTest() {}
      virtual void *entry() {
#if defined(__linux__)
	int ver = get_linux_version();
	assert(ver != 0);
	bool can_invalidate_dentries = g_conf->client_try_dentry_invalidate &&
				       ver < KERNEL_VERSION(3, 18, 0);
	int tr = client->test_dentry_handling(can_invalidate_dentries);
	if (tr != 0) {
	  cerr << "ceph-fuse[" << getpid()
	       << "]: fuse failed dentry invalidate/remount test with error "
	       << cpp_strerror(tr) << ", stopping" << std::endl;

	  char buf[5050];
	  string mountpoint = cfuse->get_mount_point();
	  snprintf(buf, 5049, "fusermount -u -z %s", mountpoint.c_str());
	  int umount_r = system(buf);
	  if (umount_r) {
	    if (umount_r != -1) {
	      if (WIFEXITED(umount_r)) {
		umount_r = WEXITSTATUS(umount_r);
		cerr << "got error " << umount_r
		     << " when unmounting Ceph on failed remount test!" << std::endl;
	      } else {
		cerr << "attempt to umount on failed remount test failed (on a signal?)" << std::endl;
	      }
	    } else {
	      cerr << "system() invocation failed during remount test" << std::endl;
	    }
	  }
	}
	return reinterpret_cast<void*>(tr);
#else
	return reinterpret_cast<void*>(0);
#endif
      }
    } tester;


    // get monmap
    Messenger *messenger = NULL;
    Client *client;
    CephFuse *cfuse;

    MonClient *mc = new MonClient(g_ceph_context);
    int r = mc->build_initial_monmap();
    if (r == -EINVAL)
      usage();
    if (r < 0)
      goto out_mc_start_failed;

    // start up network
    messenger = Messenger::create_client_messenger(g_ceph_context, "client");
    messenger->set_default_policy(Messenger::Policy::lossy_client(0, 0));
    messenger->set_policy(entity_name_t::TYPE_MDS,
			  Messenger::Policy::lossless_client(0, 0));

    client = new Client(messenger, mc);
    if (filer_flags) {
      client->set_filer_flags(filer_flags);
    }

    cfuse = new CephFuse(client, fd[1]);

    r = cfuse->init(newargc, newargv);
    if (r != 0) {
      cerr << "ceph-fuse[" << getpid() << "]: fuse failed to initialize" << std::endl;
      goto out_messenger_start_failed;
    }

    cout << "ceph-fuse[" << getpid() << "]: starting ceph client" << std::endl;
    r = messenger->start();
    if (r < 0) {
      cerr << "ceph-fuse[" << getpid() << "]: ceph mount failed with " << cpp_strerror(-r) << std::endl;
      goto out_messenger_start_failed;
    }

    // start client
    r = client->init();
    if (r < 0) {
      cerr << "ceph-fuse[" << getpid() << "]: ceph mount failed with " << cpp_strerror(-r) << std::endl;
      goto out_init_failed;
    }
    
    client->update_metadata("mount_point", cfuse->get_mount_point());

    // start up fuse
    // use my argc, argv (make sure you pass a mount point!)
    r = client->mount(g_conf->client_mountpoint.c_str(), g_ceph_context->_conf->fuse_require_active_mds);
    if (r < 0) {
      if (r == CEPH_FUSE_NO_MDS_UP)
        cerr << "ceph-fuse[" << getpid() << "]: probably no MDS server is up?" << std::endl;
      cerr << "ceph-fuse[" << getpid() << "]: ceph mount failed with " << cpp_strerror(-r) << std::endl;
      goto out_shutdown;
    }

    r = cfuse->start();
    if (r != 0) {
      cerr << "ceph-fuse[" << getpid() << "]: fuse failed to start" << std::endl;
      goto out_client_unmount;
    }

    cerr << "ceph-fuse[" << getpid() << "]: starting fuse" << std::endl;
    tester.init(cfuse, client);
    tester.create("tester");
    r = cfuse->loop();
    tester.join(&tester_rp);
    tester_r = static_cast<int>(reinterpret_cast<uint64_t>(tester_rp));
    cerr << "ceph-fuse[" << getpid() << "]: fuse finished with error " << r
	 << " and tester_r " << tester_r <<std::endl;
    
    
  out_client_unmount:
    client->unmount();
    //cout << "unmounted" << std::endl;
    
    cfuse->finalize();
    delete cfuse;
    
  out_shutdown:
    client->shutdown();
  out_init_failed:
    // wait for messenger to finish
    messenger->shutdown();
    messenger->wait();
  out_messenger_start_failed:
    delete client;
  out_mc_start_failed:
    
    if (g_conf->daemonize) {
      //cout << "child signalling parent with " << r << std::endl;
      static int foo = 0;
      foo += ::write(fd[1], &r, sizeof(r));
    }
    
    delete messenger;
    g_ceph_context->put();
    free(newargv);
    
    delete mc;
    
    //cout << "child done" << std::endl;
    return r;
  } else {
    // i am the parent
    //cout << "parent, waiting for signal" << std::endl;
    ::close(fd[1]);

    int r = -1;
    int err = safe_read_exact(fd[0], &r, sizeof(r));
    if (err == 0 && r == 0) {
      // close stdout, etc.
      //cout << "success" << std::endl;
      ::close(0);
      ::close(1);
      ::close(2);
    } else if (err) {
      cerr << "ceph-fuse[" << getpid() << "]: mount failed: " << cpp_strerror(-err) << std::endl;
    } else {
      cerr << "ceph-fuse[" << getpid() << "]: mount failed: " << cpp_strerror(-r) << std::endl;
    }
    return r;
  }
}

