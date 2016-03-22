// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/Threads.h"

#include <string>
#include <vector>

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror-image-replay: "

rbd::mirror::ImageReplayer<> *replayer = nullptr;

void usage() {
  std::cout << "usage: ceph_test_rbd_mirror_image_replay [options...] \\" << std::endl;
  std::cout << "           <client-id> <local-pool> <remote-pool> <image>" << std::endl;
  std::cout << std::endl;
  std::cout << "  client-id     client ID to register in remote journal" << std::endl;
  std::cout << "  local-pool    local (secondary, destination) pool" << std::endl;
  std::cout << "  remote-pool   remote (primary, source) pool" << std::endl;
  std::cout << "  image         image to replay (mirror)" << std::endl;
  std::cout << std::endl;
  std::cout << "options:\n";
  std::cout << "  -m monaddress[:port]      connect to specified monitor\n";
  std::cout << "  --keyring=<path>          path to keyring for local cluster\n";
  std::cout << "  --log-file=<logfile>      file to log debug output\n";
  std::cout << "  --debug-rbd-mirror=<log-level>/<memory-level>  set rbd-mirror debug level\n";
  generic_server_usage();
}

static atomic_t g_stopping;

static void handle_signal(int signum)
{
  g_stopping.set(1);
}

int get_image_id(rbd::mirror::RadosRef cluster, int64_t pool_id,
		 const std::string &image_name, std::string *image_id)
{
  librados::IoCtx ioctx;

  int r = cluster->ioctx_create2(pool_id, ioctx);
  if (r < 0) {
    derr << "error opening ioctx for pool " << pool_id
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  librbd::ImageCtx *image_ctx = new librbd::ImageCtx(image_name, "", NULL,
						     ioctx, true);
  r = image_ctx->state->open();
  if (r < 0) {
    derr << "error opening remote image " << image_name
	 << ": " << cpp_strerror(r) << dendl;
    delete image_ctx;
    return r;
  }

  *image_id = image_ctx->id;
  image_ctx->state->close();
  return 0;
}

int main(int argc, const char **argv)
{
  std::vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
	      CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  for (auto i = args.begin(); i != args.end(); ++i) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return EXIT_SUCCESS;
    }
  }

  if (args.size() < 4) {
    usage();
    return EXIT_FAILURE;
  }

  std::string client_id = args[0];
  std::string local_pool_name = args[1];
  std::string remote_pool_name = args[2];
  std::string image_name = args[3];

  dout(1) << "client_id=" << client_id << ", local_pool_name="
	  << local_pool_name << ", remote_pool_name=" << remote_pool_name
	  << ", image_name=" << image_name << dendl;

  rbd::mirror::ImageReplayer<>::BootstrapParams bootstap_params(image_name);
  int64_t local_pool_id;
  int64_t remote_pool_id;
  std::string remote_image_id;

  if (local_pool_name == remote_pool_name) {
    std::cerr << "local and remote pools can't be the same" << std::endl;
    return EXIT_FAILURE;
  }

  if (g_conf->daemonize) {
    global_init_daemonize(g_ceph_context);
  }
  g_ceph_context->enable_perf_counter();

  common_init_finish(g_ceph_context);

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  dout(5) << "connecting to cluster" << dendl;

  rbd::mirror::RadosRef local(new librados::Rados());
  rbd::mirror::RadosRef remote(new librados::Rados());
  rbd::mirror::Threads *threads = nullptr;

  C_SaferCond start_cond, stop_cond;

  int r = local->init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "could not initialize rados handle" << dendl;
    goto cleanup;
  }

  r = local->connect();
  if (r < 0) {
    derr << "error connecting to local cluster" << dendl;
    goto cleanup;
  }

  r = local->pool_lookup(local_pool_name.c_str());
  if (r < 0) {
    derr << "error finding local pool " << local_pool_name
	 << ": " << cpp_strerror(r) << dendl;
    goto cleanup;
  }
  local_pool_id = r;

  r = remote->init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "could not initialize rados handle" << dendl;
    goto cleanup;
  }

  r = remote->connect();
  if (r < 0) {
    derr << "error connecting to local cluster" << dendl;
    goto cleanup;
  }

  r = remote->pool_lookup(remote_pool_name.c_str());
  if (r < 0) {
    derr << "error finding remote pool " << remote_pool_name
	 << ": " << cpp_strerror(r) << dendl;
    goto cleanup;
  }
  remote_pool_id = r;

  r = get_image_id(remote, remote_pool_id, image_name, &remote_image_id);
  if (r < 0) {
    derr << "error resolving ID for remote image " << image_name
	 << ": " << cpp_strerror(r) << dendl;
    goto cleanup;
  }

  dout(5) << "starting replay" << dendl;

  threads = new rbd::mirror::Threads(reinterpret_cast<CephContext*>(
    local->cct()));
  replayer = new rbd::mirror::ImageReplayer<>(threads, local, remote, client_id,
					      local_pool_id, remote_pool_id,
					      remote_image_id);

  replayer->start(&start_cond, &bootstap_params);
  r = start_cond.wait();
  if (r < 0) {
    derr << "failed to start: " << cpp_strerror(r) << dendl;
    goto cleanup;
  }

  dout(5) << "replay started" << dendl;

  while (!g_stopping.read()) {
    usleep(200000);
  }

  dout(1) << "termination signal received, stopping replay" << dendl;

  replayer->stop(&stop_cond);
  r = stop_cond.wait();
  assert(r == 0);

  dout(1) << "shutdown" << dendl;

 cleanup:
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();

  delete replayer;
  delete threads;
  g_ceph_context->put();

  return r < 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
